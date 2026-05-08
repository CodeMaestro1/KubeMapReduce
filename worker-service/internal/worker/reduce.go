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
				if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
					tmpFile.Close()
					return nil, nil, fmt.Errorf("seek temp shuffle input file: %w", err)
				}
				readers = append(readers, tmpFile)
				closers = append(closers, tmpFile)
				segmentIdx++
				tmpFile, err = openSegment(segmentIdx)
				if err != nil {
					return nil, nil, fmt.Errorf("create temp shuffle input file: %w", err)
				}
				segmentHasData = false
			}
			continue
		}
		segmentHasData = true
		if _, err := tmpFile.Write(chunk.Data); err != nil {
			tmpFile.Close()
			return nil, nil, fmt.Errorf("write shuffle chunk: %w", err)
		}
	}

	if segmentHasData {
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

func (w *Worker) uploadReduceOutput(ctx context.Context, a *pb.TaskAssignment, reduceOut []byte) (uri, checksum string, err error) {
	// Upload reducer output.
	key := fmt.Sprintf("%s/partition-%d.jsonl", a.JobId, a.PartitionId)
	h := sha256.New()
	h.Write(reduceOut)
	_, err = w.storage.PutObject(ctx, outputBucket, key,
		bytes.NewReader(reduceOut), int64(len(reduceOut)),
		minio.PutObjectOptions{ContentType: "application/x-ndjson"})
	if err != nil {
		return "", "", fmt.Errorf("upload output: %w", err)
	}

	uri = fmt.Sprintf("s3://%s/%s", outputBucket, key)
	checksum = hex.EncodeToString(h.Sum(nil))
	log.Printf("[reduce] done task=%s output=%s", a.TaskId, uri)
	return uri, checksum, nil
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

	// External k-way merge sort over pre-sorted mapper output files.
	mergedReader, mergedCloser, err := w.mergeShuffleInputs(a, taskTempDir, readers)
	if err != nil {
		return nil, nil, err
	}
	defer mergedCloser.Close()

	// Execute reducer with globally sorted JSONL on stdin.
	reduceOut, err := w.execCode(ctx, codePath, a.RuntimeEnv, mergedReader)
	if err != nil {
		return nil, nil, fmt.Errorf("reducer: %w", err)
	}

	// Upload reducer output.
	uri, chk, err := w.uploadReduceOutput(ctx, a, reduceOut)
	if err != nil {
		return nil, nil, err
	}

	return []string{uri}, []string{chk}, nil
}
