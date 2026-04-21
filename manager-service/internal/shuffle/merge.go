package shuffle

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Record represents a single JSONL key-value pair processed during the shuffle phase.
//
// The [Key] is used as the primary sorting and partitioning criterion.
type Record struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// MergeConfig holds tunable parameters for the multi-pass external merge operation.
//
// These limits prevent the Manager from exhausting file descriptors or memory
// when merging intermediate data from thousands of mapper tasks.
type MergeConfig struct {
	// BatchSize is the maximum number of concurrent open streams per merge pass.
	BatchSize int
	// TempDir is the directory where intermediate spill files are stored.
	TempDir string
}

// MergeStats provides observability into the complexity of a shuffle operation.
type MergeStats struct {
	// TotalPasses is the number of recursive merge rounds executed.
	TotalPasses int
	// PeakOpenStreams is the maximum number of file descriptors held simultaneously.
	PeakOpenStreams int
	// SpillCount is the number of temporary files created on disk.
	SpillCount int
	// TotalRecords is the count of key-value pairs written to the final output.
	TotalRecords int64
}

// DefaultMergeConfig returns a configuration with safe defaults for production clusters.
//
// The BatchSize of 500 is chosen to stay well below the typical 1024 soft limit for
// file descriptors on Linux containers.
func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		BatchSize: 500,
		TempDir:   os.TempDir(),
	}
}

// MergeInputs performs a bounded multi-pass external merge sort on pre-sorted
// JSONL input streams.
//
// This function implements a standard k-way merge using a min-heap. If the number
// of input streams exceeds [MergeConfig.BatchSize], it recursively spills partial
// results to disk to stay within resource limits.
//
// Callers must ensure that all input readers provide data that is already sorted
// by [Record.Key].
func MergeInputs(readers []io.Reader, w io.Writer, cfg MergeConfig) (MergeStats, error) {
	if len(readers) == 0 {
		return MergeStats{}, nil
	}

	if cfg.BatchSize <= 1 {
		cfg.BatchSize = 500
	}
	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}

	stats := MergeStats{}

	// Single-pass optimization
	if len(readers) <= cfg.BatchSize {
		stats.TotalPasses = 1
		stats.PeakOpenStreams = len(readers)
		total, err := heapMerge(readers, w)
		if err != nil {
			return stats, err
		}
		stats.TotalRecords = total
		return stats, nil
	}

	// Multi-pass merge
	currentInputs := readers
	var intermediateFiles []*os.File

	// Cleanup intermediate files on exit
	defer func() {
		for _, f := range intermediateFiles {
			f.Close()
			os.Remove(f.Name())
		}
	}()

	for len(currentInputs) > cfg.BatchSize {
		stats.TotalPasses++
		var nextPassInputs []io.Reader

		for i := 0; i < len(currentInputs); i += cfg.BatchSize {
			end := i + cfg.BatchSize
			if end > len(currentInputs) {
				end = len(currentInputs)
			}
			batch := currentInputs[i:end]

			if len(batch) > stats.PeakOpenStreams {
				stats.PeakOpenStreams = len(batch)
			}

			// Create spill file
			spillFile, err := os.CreateTemp(cfg.TempDir, "shuffle-spill-*.jsonl")
			if err != nil {
				return stats, fmt.Errorf("failed to create spill file: %v", err)
			}
			intermediateFiles = append(intermediateFiles, spillFile)
			stats.SpillCount++

			_, err = heapMerge(batch, spillFile)
			if err != nil {
				return stats, fmt.Errorf("failed merge to spill file: %v", err)
			}

			// Rewind for reading in next pass
			if _, err := spillFile.Seek(0, 0); err != nil {
				return stats, fmt.Errorf("failed to seek spill file: %v", err)
			}
			nextPassInputs = append(nextPassInputs, spillFile)
		}

		currentInputs = nextPassInputs
	}

	// Final pass
	stats.TotalPasses++
	if len(currentInputs) > stats.PeakOpenStreams {
		stats.PeakOpenStreams = len(currentInputs)
	}
	total, err := heapMerge(currentInputs, w)
	if err != nil {
		return stats, err
	}
	stats.TotalRecords = total

	return stats, nil
}

type heapItem struct {
	record  Record
	scanner *bufio.Scanner
	index   int // To ensure stable merge for duplicate keys
}

type recordHeap []heapItem

func (h recordHeap) Len() int { return len(h) }
func (h recordHeap) Less(i, j int) bool {
	if h[i].record.Key == h[j].record.Key {
		return h[i].index < h[j].index
	}
	return h[i].record.Key < h[j].record.Key
}
func (h recordHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *recordHeap) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}

func (h *recordHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// heapMerge performs a single-pass k-way merge using a min-heap.
func heapMerge(readers []io.Reader, w io.Writer) (int64, error) {
	h := &recordHeap{}
	heap.Init(h)

	for i, r := range readers {
		scanner := bufio.NewScanner(r)
		if scanner.Scan() {
			var rec Record
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
				return 0, fmt.Errorf("malformed JSON in stream %d: %v", i, err)
			}
			heap.Push(h, heapItem{
				record:  rec,
				scanner: scanner,
				index:   i,
			})
		}
		if err := scanner.Err(); err != nil {
			return 0, fmt.Errorf("error reading stream %d: %v", i, err)
		}
	}

	var totalRecords int64
	encoder := json.NewEncoder(w)

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		if err := encoder.Encode(item.record); err != nil {
			return totalRecords, fmt.Errorf("failed to encode output: %v", err)
		}
		totalRecords++

		if item.scanner.Scan() {
			var rec Record
			if err := json.Unmarshal(item.scanner.Bytes(), &rec); err != nil {
				return totalRecords, fmt.Errorf("malformed JSON in stream %d: %v", item.index, err)
			}
			item.record = rec
			heap.Push(h, item)
		}
		if err := item.scanner.Err(); err != nil {
			return totalRecords, fmt.Errorf("error reading stream %d: %v", item.index, err)
		}
	}

	return totalRecords, nil
}
