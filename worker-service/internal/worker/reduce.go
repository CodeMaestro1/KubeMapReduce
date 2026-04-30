package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7"

	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/shuffle"
)

const outputBucket = "mapreduce-outputs"

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
	readers := make([]io.Reader, 0, len(a.DataLocations))
	closers := make([]io.Closer, 0, len(a.DataLocations))
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()

	for i, rawURI := range a.DataLocations {
		dataURI, expectedChecksum := splitChecksumURI(rawURI)
		bucket, key, parseErr := parseS3URI(dataURI)
		if parseErr != nil {
			return nil, nil, parseErr
		}
		obj, getErr := w.storage.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		if getErr != nil {
			return nil, nil, fmt.Errorf("GetObject %s: %w", dataURI, getErr)
		}

		// Write to temp file instead of memory to prevent OOM.
		tmpPath := filepath.Join(taskTempDir, fmt.Sprintf("input-%d.jsonl", i))
		tmpFile, err := os.Create(tmpPath)
		if err != nil {
			obj.Close()
			return nil, nil, fmt.Errorf("create temp input file: %w", err)
		}
		closers = append(closers, tmpFile)

		h := sha256.New()
		if _, copyErr := io.Copy(io.MultiWriter(tmpFile, h), obj); copyErr != nil {
			obj.Close()
			return nil, nil, fmt.Errorf("reading shuffle input %s: %w", dataURI, copyErr)
		}
		obj.Close()

		if expectedChecksum != "" {
			got := hex.EncodeToString(h.Sum(nil))
			if got != expectedChecksum {
				return nil, nil, fmt.Errorf("checksum mismatch for shuffle input %s: want %s got %s", dataURI, expectedChecksum, got)
			}
		}

		if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
			return nil, nil, fmt.Errorf("seek temp input file: %w", err)
		}
		readers = append(readers, tmpFile)
	}

	// External k-way merge sort over pre-sorted mapper output files.
	// Write merged output to a temp file instead of a bytes.Buffer to prevent OOM.
	mergedFile, err := os.Create(filepath.Join(taskTempDir, "merged.jsonl"))
	if err != nil {
		return nil, nil, fmt.Errorf("create merged file: %w", err)
	}
	closers = append(closers, mergedFile)

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
		return nil, nil, fmt.Errorf("merge: %w", mergeErr)
	}

	if _, err := mergedFile.Seek(0, 0); err != nil {
		return nil, nil, fmt.Errorf("seek merged output: %w", err)
	}
	log.Printf("[reduce] merge stats task=%s passes=%d spills=%d peak_streams=%d records=%d",
		a.TaskId, mergeStats.TotalPasses, mergeStats.SpillCount, mergeStats.PeakOpenStreams, mergeStats.TotalRecords)

	if _, err := mergedFile.Seek(0, io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("seek merged file: %w", err)
	}

	// Execute reducer with globally sorted JSONL on stdin.
	reduceOut, err := w.execCode(ctx, codePath, a.RuntimeEnv, mergedFile)
	if err != nil {
		return nil, nil, fmt.Errorf("reducer: %w", err)
	}

	// Upload reducer output.
	key := fmt.Sprintf("%s/partition-%d.jsonl", a.JobId, a.PartitionId)
	h := sha256.New()
	h.Write(reduceOut)
	_, err = w.storage.PutObject(ctx, outputBucket, key,
		bytes.NewReader(reduceOut), int64(len(reduceOut)),
		minio.PutObjectOptions{ContentType: "application/x-ndjson"})
	if err != nil {
		return nil, nil, fmt.Errorf("upload output: %w", err)
	}

	uri := fmt.Sprintf("s3://%s/%s", outputBucket, key)
	chk := hex.EncodeToString(h.Sum(nil))
	log.Printf("[reduce] done task=%s output=%s", a.TaskId, uri)
	return []string{uri}, []string{chk}, nil
}
