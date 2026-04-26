package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"sort"

	"github.com/minio/minio-go/v7"

	"kubemapreduce/pkg/shuffle"
	pb "kubemapreduce/proto"
)

const stagingBucket = "mapreduce-staging"

func (w *Worker) runMap(ctx context.Context, a *pb.TaskAssignment) (outputURIs, outputChecksums []string, err error) {
	log.Printf("[map] task=%s partition=%d locations=%d", a.TaskId, a.PartitionId, len(a.DataLocations))

	// Prepare user code.
	codePath, cleanup, err := w.prepareCode(ctx, w.storage, a.CodeLocation, w.cfg.TempDir)
	if err != nil {
		return nil, nil, fmt.Errorf("prepare code: %w", err)
	}
	defer cleanup()

	// Collect input JSONL lines from all data locations.
	// ByteStart/ByteEnd/SplitChecksum come from the first (and usually only) split.
	var inputLines [][]byte
	for i, uri := range a.DataLocations {
		byteStart, byteEnd, chk := a.ByteStart, a.ByteEnd, a.SplitChecksum
		if i > 0 {
			byteStart, byteEnd, chk = 0, 0, ""
		}
		lines, readErr := readSplitRecords(ctx, w.storage, uri, byteStart, byteEnd, chk)
		if readErr != nil {
			return nil, nil, fmt.Errorf("read split %s: %w", uri, readErr)
		}
		inputLines = append(inputLines, lines...)
	}

	// Execute mapper: JSONL in, JSONL out.
	mapOut, err := w.execCode(ctx, codePath, a.RuntimeEnv, jsonlReader(inputLines))
	if err != nil {
		return nil, nil, fmt.Errorf("mapper: %w", err)
	}

	records, err := parseJSONLRecords(mapOut)
	if err != nil {
		return nil, nil, fmt.Errorf("parse mapper output: %w", err)
	}

	// Sort records by key.
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })

	// Optional combiner pass (input is already sorted).
	if a.CombinerLocation != "" {
		cPath, cCleanup, cErr := w.prepareCode(ctx, w.storage, a.CombinerLocation, w.cfg.TempDir)
		if cErr != nil {
			return nil, nil, fmt.Errorf("prepare combiner: %w", cErr)
		}
		defer cCleanup()

		combinedOut, cExecErr := w.execCode(ctx, cPath, a.RuntimeEnv, jsonlReader(marshalRecords(records)))
		if cExecErr != nil {
			return nil, nil, fmt.Errorf("combiner: %w", cExecErr)
		}
		records, err = parseJSONLRecords(combinedOut)
		if err != nil {
			return nil, nil, fmt.Errorf("parse combiner output: %w", err)
		}
		sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	}

	// Hash-partition into R buckets.
	R := int(a.TotalReducers)
	if R <= 0 {
		R = 1
	}
	partitions := make([][]shuffle.Record, R)
	for _, rec := range records {
		p := hashPartition(rec.Key, R)
		partitions[p] = append(partitions[p], rec)
	}

	// Upload each partition to staging.
	for i, part := range partitions {
		uri, chk, upErr := uploadStagingPartition(ctx, w.storage, a.JobId, a.TaskId, a.AttemptId, i, part)
		if upErr != nil {
			return nil, nil, fmt.Errorf("upload partition %d: %w", i, upErr)
		}
		outputURIs = append(outputURIs, uri)
		outputChecksums = append(outputChecksums, chk)
	}

	log.Printf("[map] done task=%s records=%d partitions=%d", a.TaskId, len(records), R)
	return outputURIs, outputChecksums, nil
}

// hashPartition assigns a record key to partition [0, R) using FNV-32a.
func hashPartition(key string, R int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % R
}

// parseJSONLRecords decodes newline-delimited JSON objects into shuffle.Record values.
func parseJSONLRecords(data []byte) ([]shuffle.Record, error) {
	var records []shuffle.Record
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64*1024), shuffle.DefaultMaxRecordBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec shuffle.Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("record %q: %w", line, err)
		}
		records = append(records, rec)
	}
	return records, sc.Err()
}

// marshalRecords serialises a slice of shuffle.Record to raw JSONL lines.
func marshalRecords(records []shuffle.Record) [][]byte {
	lines := make([][]byte, 0, len(records))
	for _, r := range records {
		b, _ := json.Marshal(r)
		lines = append(lines, b)
	}
	return lines
}

// jsonlReader builds an io.Reader that emits each line followed by a newline.
func jsonlReader(lines [][]byte) io.Reader {
	var buf bytes.Buffer
	for _, l := range lines {
		buf.Write(l)
		if !bytes.HasSuffix(l, []byte{'\n'}) {
			buf.WriteByte('\n')
		}
	}
	return &buf
}

// uploadStagingPartition serialises records as JSONL, uploads to staging, and returns
// the s3 URI and SHA-256 checksum of the uploaded content.
func uploadStagingPartition(ctx context.Context, storage objectStorage, jobID, taskID, attemptID string, partIdx int, records []shuffle.Record) (uri, checksum string, err error) {
	key := fmt.Sprintf("%s/%s/%s/partition-%d.jsonl", jobID, taskID, attemptID, partIdx)

	var buf bytes.Buffer
	h := sha256.New()
	enc := json.NewEncoder(io.MultiWriter(&buf, h))
	for _, rec := range records {
		if encErr := enc.Encode(rec); encErr != nil {
			return "", "", encErr
		}
	}
	data := buf.Bytes()

	_, err = storage.PutObject(ctx, stagingBucket, key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/x-ndjson"})
	if err != nil {
		return "", "", fmt.Errorf("PutObject staging %s: %w", key, err)
	}

	return fmt.Sprintf("s3://%s/%s", stagingBucket, key), hex.EncodeToString(h.Sum(nil)), nil
}
