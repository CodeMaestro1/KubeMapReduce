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

	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/shuffle"
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

	// Collect input JSONL lines from all input splits. Newer task assignments
	// carry per-split metadata; legacy assignments fall back to the single
	// byte-range fields on TaskAssignment.
	var inputLines [][]byte
	for _, split := range taskInputSplits(a) {
		lines, readErr := readSplitRecords(ctx, w.storage, split.uri, split.byteStart, split.byteEnd, split.checksum)
		if readErr != nil {
			return nil, nil, fmt.Errorf("read split %s: %w", split.uri, readErr)
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

	// Sort records by key with an in-memory budget; spill sorted runs to disk
	// when the budget is exceeded so very large mapper outputs cannot OOM the
	// worker (issue #152). The returned reader yields the merged sorted JSONL
	// stream and MUST be closed to release any spill files.
	sortedReader, err := sortRecordsSpilling(records, w.spillThresholdBytes(), w.cfg.TempDir)
	if err != nil {
		return nil, nil, fmt.Errorf("sort map output: %w", err)
	}
	records = nil // help GC: data now lives in sortedReader / on disk
	defer func() {
		if sortedReader != nil {
			sortedReader.Close()
		}
	}()

	// Optional combiner pass (input is already sorted).
	if a.CombinerLocation != "" {
		cPath, cCleanup, cErr := w.prepareCode(ctx, w.storage, a.CombinerLocation, w.cfg.TempDir)
		if cErr != nil {
			return nil, nil, fmt.Errorf("prepare combiner: %w", cErr)
		}
		defer cCleanup()

		combinedOut, cExecErr := w.execCode(ctx, cPath, a.RuntimeEnv, sortedReader)
		// Combiner has fully consumed the sorted stream; release spill files now.
		_ = sortedReader.Close()
		sortedReader = nil
		if cExecErr != nil {
			return nil, nil, fmt.Errorf("combiner: %w", cExecErr)
		}
		combinedRecords, parseErr := parseJSONLRecords(combinedOut)
		if parseErr != nil {
			return nil, nil, fmt.Errorf("parse combiner output: %w", parseErr)
		}
		// Combiner output is not guaranteed sorted; re-sort with the same budget.
		// (Spill-aware re-sort is wired in a follow-up commit; for now, in-memory.)
		sort.Slice(combinedRecords, func(i, j int) bool { return combinedRecords[i].Key < combinedRecords[j].Key })
		var encBuf bytes.Buffer
		enc := json.NewEncoder(&encBuf)
		for _, rec := range combinedRecords {
			if err := enc.Encode(rec); err != nil {
				return nil, nil, fmt.Errorf("encode combiner output: %w", err)
			}
		}
		sortedReader = io.NopCloser(&encBuf)
	}

	// Hash-partition into R buckets by streaming the sorted JSONL output.
	R := int(a.TotalReducers)
	if R <= 0 {
		R = 1
	}
	partitions := make([][]shuffle.Record, R)
	totalRecords := 0
	sc := bufio.NewScanner(sortedReader)
	sc.Buffer(make([]byte, 64*1024), shuffle.DefaultMaxRecordBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec shuffle.Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, nil, fmt.Errorf("decode sorted record: %w", err)
		}
		p := hashPartition(rec.Key, R)
		partitions[p] = append(partitions[p], rec)
		totalRecords++
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("read sorted stream: %w", err)
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

	log.Printf("[map] done task=%s records=%d partitions=%d", a.TaskId, totalRecords, R)
	return outputURIs, outputChecksums, nil
}

// spillThresholdBytes returns the configured map sort spill threshold in bytes,
// falling back to the spillingSorter default when unset or invalid.
func (w *Worker) spillThresholdBytes() int64 {
	if w == nil || w.cfg == nil {
		return defaultSpillThresholdBytes
	}
	if mb := w.cfg.MapSortSpillThresholdMB; mb > 0 {
		return int64(mb) * 1024 * 1024
	}
	return defaultSpillThresholdBytes
}

// sortRecordsSpilling sorts records by key with a memory budget, spilling
// sorted runs to tempDir when the budget is exceeded. The returned ReadCloser
// yields the merged sorted JSONL stream; callers must Close it to release
// spill files.
func sortRecordsSpilling(records []shuffle.Record, thresholdBytes int64, tempDir string) (io.ReadCloser, error) {
	s := newSpillingSorter(thresholdBytes, tempDir)
	if err := s.AddAll(records); err != nil {
		s.cleanup()
		return nil, err
	}
	return s.Finalize()
}

type taskInputSplit struct {
	uri       string
	byteStart int64
	byteEnd   int64
	checksum  string
}

func taskInputSplits(a *pb.TaskAssignment) []taskInputSplit {
	if len(a.GetInputSplits()) > 0 {
		splits := make([]taskInputSplit, 0, len(a.GetInputSplits()))
		for _, split := range a.GetInputSplits() {
			if split == nil {
				continue
			}
			splits = append(splits, taskInputSplit{
				uri:       split.InputUri,
				byteStart: split.ByteStart,
				byteEnd:   split.ByteEnd,
				checksum:  split.SplitChecksum,
			})
		}
		return splits
	}

	if len(a.DataLocations) == 0 {
		return nil
	}

	splits := make([]taskInputSplit, 0, len(a.DataLocations))
	for i, uri := range a.DataLocations {
		byteStart, byteEnd, chk := a.ByteStart, a.ByteEnd, a.SplitChecksum
		if i > 0 {
			byteStart, byteEnd, chk = 0, 0, ""
		}
		splits = append(splits, taskInputSplit{
			uri:       uri,
			byteStart: byteStart,
			byteEnd:   byteEnd,
			checksum:  chk,
		})
	}
	return splits
}

// hashPartition assigns a record key to partition [0, R) using FNV-32a.
func hashPartition(key string, R int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7FFFFFFF) % R
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
