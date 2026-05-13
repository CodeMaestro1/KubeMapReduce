package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"

	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/shuffle"
)

const outputBucket = "mapreduce-outputs"

func (w *Worker) downloadShuffleInputs(ctx context.Context, a *pb.TaskAssignment, taskTempDir string) (readers []io.Reader, closers []io.Closer, err error) {
	if w.shuffleClient != nil {
		return w.downloadShuffleInputsFromService(ctx, a, taskTempDir)
	}
	// Legacy / test fallback: download from object storage.
	return w.downloadShuffleInputsFromStorage(ctx, a, taskTempDir)
}

func (w *Worker) downloadShuffleInputsFromService(ctx context.Context, a *pb.TaskAssignment, taskTempDir string) (readers []io.Reader, closers []io.Closer, err error) {
	stream, err := w.shuffleClient.GetShuffleData(ctx, &pb.ShuffleDataRequest{
		JobId:       a.JobId,
		PartitionId: a.PartitionId,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open shuffle read stream: %w", err)
	}

	openSegment := func(idx int) (*os.File, error) {
		// Precondition: taskTempDir is created by runReduce and writable.
		return os.Create(filepath.Join(taskTempDir, fmt.Sprintf("shuffle-input-%d.jsonl", idx)))
	}
	segmentIdx := 0
	tmpFile, err := openSegment(segmentIdx)
	if err != nil {
		return nil, nil, fmt.Errorf("create temp shuffle input file: %w", err)
	}
	segmentHasData := false
	segmentHasher := sha256.New()
	expectedSegmentChecksums := make([]string, 0, len(a.DataLocations))
	for _, loc := range a.DataLocations {
		_, checksum := splitChecksumURI(loc)
		expectedSegmentChecksums = append(expectedSegmentChecksums, checksum)
	}
	resolveExpectedChecksum := func(idx int, chunkChecksum string) (string, error) {
		var expectedFromAssignment string
		if idx < len(expectedSegmentChecksums) {
			expectedFromAssignment = expectedSegmentChecksums[idx]
		}
		chunkChecksum = strings.TrimSpace(chunkChecksum)
		if expectedFromAssignment != "" && chunkChecksum != "" && expectedFromAssignment != chunkChecksum {
			return "", fmt.Errorf("segment %d checksum metadata mismatch: assignment=%s stream=%s", idx, expectedFromAssignment, chunkChecksum)
		}
		if expectedFromAssignment != "" {
			return expectedFromAssignment, nil
		}
		return chunkChecksum, nil
	}
	finalizeSegment := func(idx int, expected string) error {
		if err := validateChecksumHex(segmentHasher, expected); err != nil {
			return fmt.Errorf("segment %d checksum mismatch: %w", idx, err)
		}
		if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek temp shuffle input file: %w", err)
		}
		readers = append(readers, tmpFile)
		closers = append(closers, tmpFile)
		segmentIdx++
		next, err := openSegment(segmentIdx)
		if err != nil {
			return fmt.Errorf("create temp shuffle input file: %w", err)
		}
		tmpFile = next
		segmentHasData = false
		segmentHasher = sha256.New()
		return nil
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			tmpFile.Close()
			return nil, nil, fmt.Errorf("receive shuffle chunk: %w", err)
		}
		if len(chunk.Data) == 0 {
			// Empty chunk signals a segment boundary (end of one map output stream).
			if segmentHasData {
				expected, err := resolveExpectedChecksum(segmentIdx, chunk.GetSegmentChecksum())
				if err != nil {
					tmpFile.Close()
					return nil, nil, err
				}
				if err := finalizeSegment(segmentIdx, expected); err != nil {
					tmpFile.Close()
					return nil, nil, err
				}
			}
			continue
		}
		segmentHasData = true
		if _, err := segmentHasher.Write(chunk.Data); err != nil {
			tmpFile.Close()
			return nil, nil, fmt.Errorf("hash shuffle chunk: %w", err)
		}
		if _, err := tmpFile.Write(chunk.Data); err != nil {
			tmpFile.Close()
			return nil, nil, fmt.Errorf("write shuffle chunk: %w", err)
		}
	}

	if segmentHasData {
		expected, err := resolveExpectedChecksum(segmentIdx, "")
		if err != nil {
			tmpFile.Close()
			return nil, nil, err
		}
		if err := validateChecksumHex(segmentHasher, expected); err != nil {
			tmpFile.Close()
			return nil, nil, fmt.Errorf("segment %d checksum mismatch: %w", segmentIdx, err)
		}
		if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
			tmpFile.Close()
			return nil, nil, fmt.Errorf("seek temp shuffle input file: %w", err)
		}
		readers = append(readers, tmpFile)
		closers = append(closers, tmpFile)
	} else {
		_ = tmpFile.Close()
	}

	return readers, closers, nil
}

func (w *Worker) downloadShuffleInputsFromStorage(ctx context.Context, a *pb.TaskAssignment, taskTempDir string) (readers []io.Reader, closers []io.Closer, err error) {
	for i, loc := range a.DataLocations {
		bareURI, expectedChecksum := splitChecksumURI(loc)
		bucket, key, parseErr := parseS3URI(bareURI)
		if parseErr != nil {
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, fmt.Errorf("parse input URI %d (%s): %w", i, loc, parseErr)
		}
		rc, err := w.storage.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		if err != nil {
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, fmt.Errorf("download input %d (%s): %w", i, loc, err)
		}
		tmpPath := filepath.Join(taskTempDir, fmt.Sprintf("input-%d.jsonl", i))
		tmpFile, err := os.Create(tmpPath)
		if err != nil {
			rc.Close()
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, fmt.Errorf("create temp input file %d: %w", i, err)
		}
		hasher := sha256.New()
		if _, err := io.Copy(tmpFile, io.TeeReader(rc, hasher)); err != nil {
			_ = rc.Close()
			_ = tmpFile.Close()
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, fmt.Errorf("stream input %d to temp file: %w", i, err)
		}
		if err := rc.Close(); err != nil {
			_ = tmpFile.Close()
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, fmt.Errorf("close input %d: %w", i, err)
		}
		if err := validateChecksumHex(hasher, expectedChecksum); err != nil {
			_ = tmpFile.Close()
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, fmt.Errorf("input %d checksum mismatch: %w", i, err)
		}
		if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
			_ = tmpFile.Close()
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, fmt.Errorf("seek temp input file %d: %w", i, err)
		}
		readers = append(readers, tmpFile)
		closers = append(closers, tmpFile)
	}
	return readers, closers, nil
}

func validateChecksumHex(hasher hash.Hash, expectedChecksum string) error {
	if expectedChecksum == "" {
		return nil
	}
	got := hex.EncodeToString(hasher.Sum(nil))
	if got != expectedChecksum {
		return fmt.Errorf("SHA-256 mismatch: expected %s, got %s", expectedChecksum, got)
	}
	return nil
}

func (w *Worker) mergeShuffleInputs(a *pb.TaskAssignment, taskTempDir string, readers []io.Reader) (io.Reader, io.Closer, error) {
	// External k-way merge sort over pre-sorted mapper output files.
	// Write merged output to a temp file instead of a bytes.Buffer to prevent OOM.
	mergedFile, err := os.Create(filepath.Join(taskTempDir, "merged.jsonl"))
	if err != nil {
		return nil, nil, fmt.Errorf("create merged file: %w", err)
	}

	mergeCfg := shuffle.DefaultMergeConfig()
	mergeCfg.TempDir = taskTempDir
	if w.cfg.ShuffleBatchSize > 0 {
		mergeCfg.BatchSize = w.cfg.ShuffleBatchSize
	}
	if w.cfg.ShuffleMaxRecordBytes > 0 {
		mergeCfg.MaxRecordBytes = w.cfg.ShuffleMaxRecordBytes
	}
	mergeStats, mergeErr := shuffle.MergeInputs(readers, mergedFile, mergeCfg)
	if mergeErr != nil {
		mergedFile.Close()
		return nil, nil, fmt.Errorf("merge: %w", mergeErr)
	}

	if _, err := mergedFile.Seek(0, 0); err != nil {
		mergedFile.Close()
		return nil, nil, fmt.Errorf("seek merged output: %w", err)
	}
	log.Printf("[reduce] merge stats task=%s passes=%d spills=%d peak_streams=%d records=%d",
		a.TaskId, mergeStats.TotalPasses, mergeStats.SpillCount, mergeStats.PeakOpenStreams, mergeStats.TotalRecords)

	if _, err := mergedFile.Seek(0, io.SeekStart); err != nil {
		mergedFile.Close()
		return nil, nil, fmt.Errorf("seek merged file: %w", err)
	}
	return mergedFile, mergedFile, nil
}

// uploadEmptyReduceOutput writes a zero-byte partition object so the
// downstream contract (one output URI per reduce task) is preserved without
// spawning the reducer subprocess or running an empty merge pass.
func (w *Worker) uploadEmptyReduceOutput(ctx context.Context, a *pb.TaskAssignment) (uri, checksum string, err error) {
	key := fmt.Sprintf("%s/partition-%d.jsonl", a.JobId, a.PartitionId)
	if _, err := w.storage.PutObject(ctx, outputBucket, key,
		bytes.NewReader(nil), 0,
		minio.PutObjectOptions{ContentType: "application/x-ndjson"}); err != nil {
		return "", "", fmt.Errorf("upload empty output: %w", err)
	}
	h := sha256.Sum256(nil)
	return fmt.Sprintf("s3://%s/%s", outputBucket, key), hex.EncodeToString(h[:]), nil
}

// streamReduceOutput pipes the reducer's stdout reader directly into MinIO
// while computing the SHA-256 incrementally. This avoids buffering the full
// reducer output in worker RAM (issue: large reductions OOMed under the
// previous []byte-based path). Returns the resulting object URI and checksum.
func (w *Worker) streamReduceOutput(ctx context.Context, a *pb.TaskAssignment, stdout io.Reader) (uri, checksum string, err error) {
	key := fmt.Sprintf("%s/partition-%d.jsonl", a.JobId, a.PartitionId)
	h := sha256.New()
	tee := io.TeeReader(stdout, h)
	if _, err := w.storage.PutObject(ctx, outputBucket, key,
		tee, -1,
		minio.PutObjectOptions{ContentType: "application/x-ndjson"}); err != nil {
		return "", "", fmt.Errorf("upload output: %w", err)
	}
	uri = fmt.Sprintf("s3://%s/%s", outputBucket, key)
	checksum = hex.EncodeToString(h.Sum(nil))
	log.Printf("[reduce] done task=%s output=%s", a.TaskId, uri)
	return uri, checksum, nil
}

// reducerStream returns a streaming stdout reader for the reducer process.
// When execCodeStream is nil (older test wiring) it falls back to execCode and
// wraps the buffered []byte result as an io.ReadCloser so the streaming path
// remains the single code path in runReduce.
func (w *Worker) reducerStream(ctx context.Context, codePath, runtimeEnv string, stdin io.Reader) (io.ReadCloser, func() error, error) {
	if w.execCodeStream != nil {
		return w.execCodeStream(ctx, codePath, runtimeEnv, stdin)
	}
	out, err := w.execCode(ctx, codePath, runtimeEnv, stdin)
	if err != nil {
		return nil, nil, err
	}
	return io.NopCloser(bytes.NewReader(out)), func() error { return nil }, nil
}

func (w *Worker) runReduce(ctx context.Context, a *pb.TaskAssignment) (outputURIs, outputChecksums []string, err error) {
	log.Printf("[reduce] task=%s partition=%d inputs=%d", a.TaskId, a.PartitionId, len(a.DataLocations))

	// Create a dedicated temp directory for this reduce task to ensure cleanup.
	taskTempDir, err := os.MkdirTemp(w.cfg.TempDir, fmt.Sprintf("reduce-%s-*", a.TaskId))
	if err != nil {
		return nil, nil, fmt.Errorf("create task temp dir: %w", err)
	}
	defer os.RemoveAll(taskTempDir)

	// Prepare user code.
	codePath, cleanup, err := w.prepareCode(ctx, w.storage, a.CodeLocation, taskTempDir)
	if err != nil {
		return nil, nil, fmt.Errorf("prepare code: %w", err)
	}
	defer cleanup()

	// Download all shuffle-input files for this partition as streaming readers.
	readers, closers, err := w.downloadShuffleInputs(ctx, a, taskTempDir)
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()
	if err != nil {
		return nil, nil, err
	}

	// Empty-partition short-circuit: skip merge + reducer subprocess + tee
	// upload when no shuffle inputs arrived (zero map outputs hashed into
	// this partition). Still write an empty output object so downstream
	// consumers see a 1:1 partition→object mapping.
	if len(readers) == 0 {
		uri, chk, err := w.uploadEmptyReduceOutput(ctx, a)
		if err != nil {
			return nil, nil, err
		}
		log.Printf("[reduce] empty-partition short-circuit task=%s output=%s", a.TaskId, uri)
		return []string{uri}, []string{chk}, nil
	}

	// External k-way merge sort over pre-sorted mapper output files.
	mergedReader, mergedCloser, err := w.mergeShuffleInputs(a, taskTempDir, readers)
	if err != nil {
		return nil, nil, err
	}
	defer mergedCloser.Close()

	// Execute reducer with globally sorted JSONL on stdin and stream stdout
	// directly into object storage to keep peak memory bounded by the user
	// code's own buffering, not the full reducer output size.
	stdout, wait, err := w.reducerStream(ctx, codePath, a.RuntimeEnv, mergedReader)
	if err != nil {
		return nil, nil, fmt.Errorf("reducer: %w", err)
	}
	defer stdout.Close()

	uri, chk, putErr := w.streamReduceOutput(ctx, a, stdout)
	// Always wait for the reducer to exit so we surface its error and avoid
	// leaking a zombie process when PutObject succeeds but the user code
	// itself crashed mid-stream.
	if waitErr := wait(); waitErr != nil {
		return nil, nil, fmt.Errorf("reducer: %w", waitErr)
	}
	if putErr != nil {
		return nil, nil, putErr
	}
	return []string{uri}, []string{chk}, nil
}
