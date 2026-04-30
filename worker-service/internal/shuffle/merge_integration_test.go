//go:build integration

package shuffle

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// newSortedJSONLReader returns an io.Reader that streams n pre-sorted JSONL
// records. Keys are zero-padded so lexicographic order equals numeric order.
// valuePad bytes are appended to each value field to control record size.
// Uses io.Pipe so no full-dataset allocation occurs before reading begins.
func newSortedJSONLReader(startKey, n, valuePad int) io.Reader {
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

// TestMergeInputs_LargeStreamingInputNoOOM verifies that merging ~18 MB of
// logical data stays within a generous heap ceiling. Readers are io.Pipe so
// the full dataset is never in RAM at once.
func TestMergeInputs_LargeStreamingInputNoOOM(t *testing.T) {
	const (
		numReaders      = 200
		recordsPerReader = 500
		valuePad        = 150
		batchSize       = 50
	)

	tmpDir := t.TempDir()
	cfg := MergeConfig{
		BatchSize:      batchSize,
		TempDir:        tmpDir,
		MaxRecordBytes: DefaultMaxRecordBytes,
	}

	readers := make([]io.Reader, numReaders)
	for i := range readers {
		readers[i] = newSortedJSONLReader(i*recordsPerReader, recordsPerReader, valuePad)
	}

	runtime.GC()

	var (
		stats  MergeStats
		merErr error
	)
	peakHeap := samplePeakHeapDuring(func() {
		stats, merErr = MergeInputs(readers, io.Discard, cfg)
	})

	if merErr != nil {
		t.Fatalf("MergeInputs: %v", merErr)
	}
	if stats.SpillCount == 0 {
		t.Errorf("SpillCount=0: expected spill files with BatchSize=%d and %d readers", batchSize, numReaders)
	}
	if stats.TotalRecords != int64(numReaders*recordsPerReader) {
		t.Errorf("TotalRecords: got %d, want %d", stats.TotalRecords, numReaders*recordsPerReader)
	}

	const heapCeiling = 50 * 1024 * 1024
	if peakHeap > heapCeiling {
		t.Errorf("peak HeapAlloc %d bytes exceeds %d byte ceiling — possible OOM regression",
			peakHeap, heapCeiling)
	}

	if n := countFilesInDir(t, tmpDir); n != 0 {
		t.Errorf("spill files leaked: %d file(s) remain in %s after MergeInputs returned", n, tmpDir)
	}
}

// TestMergeInputs_SpillTriggeredByBatchSize confirms that BatchSize=2 with 5
// readers produces spill files and that all of them are cleaned up on return.
func TestMergeInputs_SpillTriggeredByBatchSize(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := MergeConfig{
		BatchSize:      2,
		TempDir:        tmpDir,
		MaxRecordBytes: DefaultMaxRecordBytes,
	}

	readers := make([]io.Reader, 5)
	for i := range readers {
		readers[i] = newSortedJSONLReader(i*10, 10, 0)
	}

	countBefore := countFilesInDir(t, tmpDir)

	stats, err := MergeInputs(readers, io.Discard, cfg)
	if err != nil {
		t.Fatalf("MergeInputs: %v", err)
	}
	if stats.SpillCount == 0 {
		t.Errorf("SpillCount=0: expected multi-pass spill with BatchSize=2 and 5 readers")
	}
	if stats.TotalRecords != 50 {
		t.Errorf("TotalRecords: got %d, want 50", stats.TotalRecords)
	}

	countAfter := countFilesInDir(t, tmpDir)
	if countAfter != countBefore {
		t.Errorf("spill files leaked: %d before, %d after in %s", countBefore, countAfter, tmpDir)
	}
}
