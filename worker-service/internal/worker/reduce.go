package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"

	"github.com/minio/minio-go/v7"

	"kubemapreduce/pkg/shuffle"
	pb "kubemapreduce/proto"
)

const outputBucket = "mapreduce-outputs"

func (w *Worker) runReduce(ctx context.Context, a *pb.TaskAssignment) (outputURIs, outputChecksums []string, err error) {
	log.Printf("[reduce] task=%s partition=%d inputs=%d", a.TaskId, a.PartitionId, len(a.DataLocations))

	// Prepare user code.
	codePath, cleanup, err := w.prepareCode(ctx, w.storage, a.CodeLocation, w.cfg.TempDir)
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

	for _, uri := range a.DataLocations {
		bucket, key, parseErr := parseS3URI(uri)
		if parseErr != nil {
			return nil, nil, parseErr
		}
		obj, getErr := w.storage.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		if getErr != nil {
			return nil, nil, fmt.Errorf("GetObject %s: %w", uri, getErr)
		}
		readers = append(readers, obj)
		closers = append(closers, obj)
	}

	// External k-way merge sort over pre-sorted mapper output files.
	var merged bytes.Buffer
	mergeCfg := shuffle.DefaultMergeConfig()
	mergeCfg.TempDir = w.cfg.TempDir
	if _, mergeErr := shuffle.MergeInputs(readers, &merged, mergeCfg); mergeErr != nil {
		return nil, nil, fmt.Errorf("merge: %w", mergeErr)
	}

	// Execute reducer with globally sorted JSONL on stdin.
	reduceOut, err := w.execCode(ctx, codePath, a.RuntimeEnv, &merged)
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
