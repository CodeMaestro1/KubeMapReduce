package shuffle

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Record represents a single JSONL key-value pair.
type Record struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// MergeConfig holds tunable parameters for the multi-pass merge.
type MergeConfig struct {
	BatchSize int    // Max concurrent open streams per pass
	TempDir   string // Directory for intermediate spill files
}

// MergeStats holds metrics about the merge operation.
type MergeStats struct {
	TotalPasses     int   // Number of merge passes executed
	PeakOpenStreams int   // Maximum concurrent streams in any single pass
	SpillCount      int   // Number of intermediate spill files created
	TotalRecords    int64 // Total records written to final output
}

// DefaultMergeConfig returns a config with safe production defaults.
func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		BatchSize: 500,
		TempDir:   os.TempDir(),
	}
}

// MergeInputs performs a bounded multi-pass external merge sort on
// pre-sorted JSONL input streams. It writes the fully merged, sorted
// output to the provided writer.
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
