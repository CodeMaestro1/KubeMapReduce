//go:build integration

package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// streamingMockStorage serves objects via generator functions so that the full
// dataset never needs to be allocated before runReduce is called. getCount
// records how many times each object was fetched, enabling early-exit assertions.
type streamingMockStorage struct {
	mu         sync.Mutex
	generators map[string]func() io.ReadCloser
	uploaded   map[string][]byte
	getCount   map[string]int
}

func newStreamingMockStorage() *streamingMockStorage {
	return &streamingMockStorage{
		generators: make(map[string]func() io.ReadCloser),
		uploaded:   make(map[string][]byte),
		getCount:   make(map[string]int),
	}
}

func (s *streamingMockStorage) registerGenerator(bucket, key string, gen func() io.ReadCloser) {
	s.generators[bucket+"/"+key] = gen
}

func (s *streamingMockStorage) registerStatic(bucket, key string, data []byte) {
	s.generators[bucket+"/"+key] = func() io.ReadCloser {
		return io.NopCloser(bytes.NewReader(data))
	}
}

func (s *streamingMockStorage) GetObject(_ context.Context, bucket, key string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	k := bucket + "/" + key
	s.mu.Lock()
	gen, ok := s.generators[k]
	s.getCount[k]++
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("object not found: %s", k)
	}
	return gen(), nil
}

func (s *streamingMockStorage) PutObject(_ context.Context, bucket, key string, reader io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	s.mu.Lock()
	s.uploaded[bucket+"/"+key] = data
	s.mu.Unlock()
	return minio.UploadInfo{Bucket: bucket, Key: key}, nil
}

// newSortedJSONLReadCloser returns n pre-sorted JSONL records as an
// io.ReadCloser. Keys are zero-padded for correct lexicographic order.
// Uses io.Pipe so no full dataset allocation occurs before reading.
func newSortedJSONLReadCloser(startKey, n, valuePad int) io.ReadCloser {
	pr, pw := io.Pipe()
	padding := strings.Repeat("x", valuePad)
	go func() {
		for i := startKey; i < startKey+n; i++ {
			line := fmt.Sprintf(`{"key":"%010d","value":"%s"}`+"\n", i, padding)
			if _, err := pw.Write([]byte(line)); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()
	return pr
}

// samplePeakHeapDuring runs fn and returns the peak HeapAlloc observed during
// the call, sampled every 50 ms by a background goroutine.
func samplePeakHeapDuring(fn func()) uint64 {
	var peak uint64
	done := make(chan struct{})
	go func() {
		var ms runtime.MemStats
		for {
			select {
			case <-done:
				return
			default:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > peak {
					peak = ms.HeapAlloc
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	fn()
	close(done)
	return peak
}

// countFilesInDir returns the count of regular files directly inside dir.
func countFilesInDir(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			n++
		}
	}
	return n
}

// TestRunReduce_LargeInputCompletesWithoutOOM runs runReduce with 50 streaming
// shuffle inputs totaling ~2.5 MB of logical data and a BatchSize that forces
// multi-pass spill. Asserts heap stays under 30 MB and temp dir is clean.
func TestRunReduce_LargeInputCompletesWithoutOOM(t *testing.T) {
	const (
		numInputs      = 50
		recordsPerInput = 200
		valuePad       = 200
		batchSize      = 10
	)

	store := newStreamingMockStorage()

	var dataLocations []string
	for i := 0; i < numInputs; i++ {
		bucket := "mapreduce-staging"
		key := fmt.Sprintf("job-inttest/part-%04d.jsonl", i)
		idx := i
		store.registerGenerator(bucket, key, func() io.ReadCloser {
			return newSortedJSONLReadCloser(idx*recordsPerInput, recordsPerInput, valuePad)
		})
		dataLocations = append(dataLocations, fmt.Sprintf("s3://%s/%s", bucket, key))
	}

	w := newTestWorker(t, nil, store)
	w.cfg.ShuffleBatchSize = batchSize

	runtime.GC()

	var (
		uris      []string
		checksums []string
		runErr    error
	)
	peakHeap := samplePeakHeapDuring(func() {
		uris, checksums, runErr = w.runReduce(context.Background(), reduceAssignment(dataLocations))
	})

	if runErr != nil {
		t.Fatalf("runReduce: %v", runErr)
	}
	if len(uris) != 1 || len(checksums) != 1 {
		t.Errorf("want 1 output URI and checksum, got %d URIs %d checksums", len(uris), len(checksums))
	}

	const heapCeiling = 30 * 1024 * 1024
	if peakHeap > heapCeiling {
		t.Errorf("peak HeapAlloc %d bytes exceeds %d byte ceiling — possible OOM regression",
			peakHeap, heapCeiling)
	}

	if n := countFilesInDir(t, w.cfg.TempDir); n != 0 {
		t.Errorf("temp files leaked: %d file(s) remain in %s", n, w.cfg.TempDir)
	}
}

// TestRunReduce_CorruptChecksumOnFirstInputFailsEarly verifies that a bad
// checksum on input[0] causes runReduce to return immediately without ever
// fetching input[1] or input[2].
func TestRunReduce_CorruptChecksumOnFirstInputFailsEarly(t *testing.T) {
	inputs := [][]byte{
		[]byte(`{"key":"aaa","value":"1"}` + "\n"),
		[]byte(`{"key":"bbb","value":"2"}` + "\n"),
		[]byte(`{"key":"ccc","value":"3"}` + "\n"),
	}

	store := newStreamingMockStorage()
	for i, data := range inputs {
		store.registerStatic("mapreduce-staging", fmt.Sprintf("job-cksum0/part-%d.jsonl", i), data)
	}

	var dataLocations []string
	for i, data := range inputs {
		sum := sha256.Sum256(data)
		checksum := hex.EncodeToString(sum[:])
		if i == 0 {
			checksum = corruptHex(checksum)
		}
		dataLocations = append(dataLocations,
			fmt.Sprintf("s3://mapreduce-staging/job-cksum0/part-%d.jsonl#sha256=%s", i, checksum))
	}

	w := newTestWorker(t, nil, store)
	_, _, err := w.runReduce(context.Background(), reduceAssignment(dataLocations))

	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("want 'checksum mismatch' in error, got: %v", err)
	}

	store.mu.Lock()
	count1 := store.getCount["mapreduce-staging/job-cksum0/part-1.jsonl"]
	count2 := store.getCount["mapreduce-staging/job-cksum0/part-2.jsonl"]
	store.mu.Unlock()

	if count1 != 0 {
		t.Errorf("part-1 fetched %d time(s): want 0 (early exit after part-0 checksum failure)", count1)
	}
	if count2 != 0 {
		t.Errorf("part-2 fetched %d time(s): want 0 (early exit after part-0 checksum failure)", count2)
	}
}

// TestRunReduce_CorruptChecksumOnSecondInputFailsEarly verifies that a bad
// checksum on input[1] causes runReduce to fail after fetching input[1] but
// before fetching input[2].
func TestRunReduce_CorruptChecksumOnSecondInputFailsEarly(t *testing.T) {
	inputs := [][]byte{
		[]byte(`{"key":"aaa","value":"1"}` + "\n"),
		[]byte(`{"key":"bbb","value":"2"}` + "\n"),
		[]byte(`{"key":"ccc","value":"3"}` + "\n"),
	}

	store := newStreamingMockStorage()
	for i, data := range inputs {
		store.registerStatic("mapreduce-staging", fmt.Sprintf("job-cksum1/part-%d.jsonl", i), data)
	}

	var dataLocations []string
	for i, data := range inputs {
		sum := sha256.Sum256(data)
		checksum := hex.EncodeToString(sum[:])
		if i == 1 {
			checksum = corruptHex(checksum)
		}
		dataLocations = append(dataLocations,
			fmt.Sprintf("s3://mapreduce-staging/job-cksum1/part-%d.jsonl#sha256=%s", i, checksum))
	}

	w := newTestWorker(t, nil, store)
	_, _, err := w.runReduce(context.Background(), reduceAssignment(dataLocations))

	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("want 'checksum mismatch' in error, got: %v", err)
	}

	store.mu.Lock()
	count0 := store.getCount["mapreduce-staging/job-cksum1/part-0.jsonl"]
	count1 := store.getCount["mapreduce-staging/job-cksum1/part-1.jsonl"]
	count2 := store.getCount["mapreduce-staging/job-cksum1/part-2.jsonl"]
	store.mu.Unlock()

	if count0 != 1 {
		t.Errorf("part-0 fetched %d time(s): want 1 (valid checksum, should succeed)", count0)
	}
	if count1 != 1 {
		t.Errorf("part-1 fetched %d time(s): want 1 (fetched, checksum fails)", count1)
	}
	if count2 != 0 {
		t.Errorf("part-2 fetched %d time(s): want 0 (early exit after part-1 failure)", count2)
	}
}

// TestRunReduce_SpillFilesCleanedUp verifies that all shuffle-input-*.jsonl
// and merged-*.jsonl temp files are removed after runReduce returns.
// Skipped on Windows: os.Remove on an open *os.File silently fails there,
// but the production worker runs on Linux where unlink-while-open works.
func TestRunReduce_SpillFilesCleanedUp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("temp file cleanup relies on POSIX unlink-while-open semantics unavailable on Windows")
	}

	const numInputs = 5

	store := newStreamingMockStorage()

	var dataLocations []string
	for i := 0; i < numInputs; i++ {
		bucket := "mapreduce-staging"
		key := fmt.Sprintf("job-spill/part-%d.jsonl", i)
		idx := i
		store.registerGenerator(bucket, key, func() io.ReadCloser {
			return newSortedJSONLReadCloser(idx*10, 10, 0)
		})
		dataLocations = append(dataLocations, fmt.Sprintf("s3://%s/%s", bucket, key))
	}

	w := newTestWorker(t, nil, store)
	w.cfg.ShuffleBatchSize = 2 // 5 inputs with BatchSize=2 forces multi-pass spill

	countBefore := countFilesInDir(t, w.cfg.TempDir)

	_, _, err := w.runReduce(context.Background(), reduceAssignment(dataLocations))
	if err != nil {
		t.Fatalf("runReduce: %v", err)
	}

	countAfter := countFilesInDir(t, w.cfg.TempDir)
	if countAfter != countBefore {
		entries, _ := os.ReadDir(w.cfg.TempDir)
		var leaked []string
		for _, e := range entries {
			if !e.IsDir() {
				leaked = append(leaked, e.Name())
			}
		}
		t.Errorf("temp files leaked: %d before, %d after, leaked files: %v",
			countBefore, countAfter, leaked)
	}
}

// corruptHex flips the last hex digit of s.
func corruptHex(s string) string {
	if len(s) == 0 {
		return s
	}
	last := s[len(s)-1]
	var replacement byte
	if last == 'f' {
		replacement = '0'
	} else {
		replacement = last + 1
	}
	return s[:len(s)-1] + string(replacement)
}
