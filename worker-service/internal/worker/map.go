package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"io"
	"log"

	"github.com/minio/minio-go/v7"

	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/shuffle"
)

const stagingBucket = "mapreduce-staging"
const shuffleChunkSizeBytes = 1 << 20 // 1 MiB

func (w *Worker) runMap(ctx context.Context, a *pb.TaskAssignment) (outputURIs, outputChecksums []string, err error) {
	log.Printf("[map] task=%s partition=%d locations=%d", a.TaskId, a.PartitionId, len(a.DataLocations))

	// Prepare user code.
	codePath, cleanup, err := w.prepareCode(ctx, w.storage, a.CodeLocation, w.cfg.TempDir)
	if err != nil {
		return nil, nil, fmt.Errorf("prepare code: %w", err)
	}
	defer cleanup()

	// Create a streaming reader for all input splits.
	var readers []io.Reader
	var closers []io.Closer
	for _, split := range taskInputSplits(a) {
		rc, readErr := readSplitRecordsStreaming(ctx, w.storage, split.uri, split.byteStart, split.byteEnd, split.checksum)
		if readErr != nil {
			// Cleanup already opened readers
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, fmt.Errorf("read split %s: %w", split.uri, readErr)
		}
		readers = append(readers, rc)
		closers = append(closers, rc)
	}
	inputReader := io.MultiReader(readers...)
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()

	// Execute mapper streaming: JSONL in, JSONL out.
	stdout, wait, err := w.execCodeStream(ctx, codePath, a.RuntimeEnv, inputReader)
	if err != nil {
		return nil, nil, fmt.Errorf("mapper: %w", err)
	}
	defer stdout.Close()

	// Pipe mapper output directly into the spilling sorter.
	sorter := newSpillingSorter(w.spillThresholdBytes(), w.cfg.TempDir)
	defer sorter.cleanup()

	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 64*1024), shuffle.DefaultMaxRecordBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec shuffle.Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, nil, fmt.Errorf("decode mapper record: %w", err)
		}
		if err := sorter.Add(rec); err != nil {
			return nil, nil, fmt.Errorf("sorter add: %w", err)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("read mapper output: %w", err)
	}

	if err := wait(); err != nil {
		return nil, nil, fmt.Errorf("mapper wait: %w", err)
	}

	// Finalize sorting (merges spilled runs if any).
	sortedReader, err := sorter.Finalize()
	if err != nil {
		return nil, nil, fmt.Errorf("sort map output: %w", err)
	}
	defer sortedReader.Close()

	// Optional combiner pass (input is already sorted).
	if a.CombinerLocation != "" {
		cPath, cCleanup, cErr := w.prepareCode(ctx, w.storage, a.CombinerLocation, w.cfg.TempDir)
		if cErr != nil {
			return nil, nil, fmt.Errorf("prepare combiner: %w", cErr)
		}
		defer cCleanup()

		combinedStdout, cWait, cExecErr := w.execCodeStream(ctx, cPath, a.RuntimeEnv, sortedReader)
		if cExecErr != nil {
			return nil, nil, fmt.Errorf("combiner: %w", cExecErr)
		}
		defer combinedStdout.Close()

		// Re-sort combiner output if needed, or pipe directly to partitioning.
		// Since combiners are usually used for reduction, we spill again to be safe.
		cSorter := newSpillingSorter(w.spillThresholdBytes(), w.cfg.TempDir)
		defer cSorter.cleanup()

		csc := bufio.NewScanner(combinedStdout)
		csc.Buffer(make([]byte, 64*1024), shuffle.DefaultMaxRecordBytes)
		for csc.Scan() {
			line := csc.Bytes()
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var rec shuffle.Record
			if err := json.Unmarshal(line, &rec); err != nil {
				return nil, nil, fmt.Errorf("decode combiner record: %w", err)
			}
			if err := cSorter.Add(rec); err != nil {
				return nil, nil, fmt.Errorf("combiner sorter add: %w", err)
			}
		}
		if err := csc.Err(); err != nil {
			return nil, nil, fmt.Errorf("read combiner output: %w", err)
		}
		if err := cWait(); err != nil {
			return nil, nil, fmt.Errorf("combiner wait: %w", err)
		}

		// Replace sortedReader with combined stream.
		_ = sortedReader.Close()
		sortedReader, err = cSorter.Finalize()
		if err != nil {
			return nil, nil, fmt.Errorf("finalize combiner sort: %w", err)
		}
	}

	// Hash-partition into R buckets by streaming the sorted JSONL output
	// directly to per-partition writers — no [][]shuffle.Record buffer, no
	// per-partition bytes.Buffer. Peak RAM is bounded by one chunk per open
	// writer (~1 MiB) instead of the full mapper output.
	R := int(a.TotalReducers)
	if R <= 0 {
		R = 1
	}
	writers := make(map[int]partitionWriter)
	openOrder := make([]int, 0, R)
	defer func() {
		for _, pw := range writers {
			pw.Abort()
		}
	}()

	totalRecords := 0
	sc = bufio.NewScanner(sortedReader)
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
		pw, ok := writers[p]
		if !ok {
			pw, err = w.openPartitionWriter(ctx, a.JobId, a.TaskId, p)
			if err != nil {
				return nil, nil, err
			}
			writers[p] = pw
			openOrder = append(openOrder, p)
		}
		// Write the record line followed by a newline. The sorted reader
		// emits valid JSONL records, so no re-marshal is needed.
		if _, err := pw.Write(line); err != nil {
			return nil, nil, fmt.Errorf("write partition %d: %w", p, err)
		}
		if _, err := pw.Write(newlineBytes); err != nil {
			return nil, nil, fmt.Errorf("write partition %d newline: %w", p, err)
		}
		totalRecords++
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("read sorted stream: %w", err)
	}

	// Close partitions in their original ascending index order so output
	// URIs / checksums remain deterministic for downstream consumers.
	sortInts(openOrder)
	for _, i := range openOrder {
		pw := writers[i]
		uri, chk, err := pw.Close()
		delete(writers, i)
		if err != nil {
			return nil, nil, fmt.Errorf("close partition %d: %w", i, err)
		}
		outputURIs = append(outputURIs, uri)
		outputChecksums = append(outputChecksums, chk)
	}

	log.Printf("[map] done task=%s records=%d partitions=%d", a.TaskId, totalRecords, R)
	return outputURIs, outputChecksums, nil
}

var newlineBytes = []byte{'\n'}

func sortInts(xs []int) {
	// Tiny insertion sort: partition counts are small (R, typically <100)
	// and we want to avoid pulling in sort just for this.
	for i := 1; i < len(xs); i++ {
		v := xs[i]
		j := i - 1
		for j >= 0 && xs[j] > v {
			xs[j+1] = xs[j]
			j--
		}
		xs[j+1] = v
	}
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
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h&0x7FFFFFFF) % R
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
