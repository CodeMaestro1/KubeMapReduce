package shuffle

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

const (
	// MaxBatchSize is the upper limit for concurrent open streams to prevent
	// file descriptor exhaustion. Most Linux containers have a soft limit of 1024.
	MaxBatchSize = 900

	// DefaultMaxRecordBytes is the default buffer limit for the JSONL scanner.
	// 1MB is sufficient for typical MapReduce key-value pairs while preventing
	// a single oversized record from causing an OOM.
	DefaultMaxRecordBytes = 1 * 1024 * 1024
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
	// MaxRecordBytes is the maximum allowed size for a single JSONL line.
	MaxRecordBytes int
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
func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		BatchSize:      500,
		TempDir:        os.TempDir(),
		MaxRecordBytes: DefaultMaxRecordBytes,
	}
}

// MergeInputs performs a bounded multi-pass external merge sort on pre-sorted
// JSONL input streams.
func MergeInputs(readers []io.Reader, w io.Writer, cfg MergeConfig) (MergeStats, error) {
	if len(readers) == 0 {
		return MergeStats{}, nil
	}

	// Validate and clamp tunables
	if cfg.BatchSize <= 1 {
		cfg.BatchSize = 500
	}
	if cfg.BatchSize > MaxBatchSize {
		cfg.BatchSize = MaxBatchSize
	}
	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}
	if cfg.MaxRecordBytes <= 0 {
		cfg.MaxRecordBytes = DefaultMaxRecordBytes
	}

	log.Printf("[shuffle] Starting merge of %d streams (batch_size=%d, temp_dir=%s)",
		len(readers), cfg.BatchSize, cfg.TempDir)

	stats := MergeStats{}

	// Single-pass optimization
	if len(readers) <= cfg.BatchSize {
		stats.TotalPasses = 1
		stats.PeakOpenStreams = len(readers)
		total, err := heapMerge(readers, w, cfg.MaxRecordBytes)
		if err != nil {
			return stats, err
		}
		stats.TotalRecords = total
		log.Printf("[shuffle] Merge completed in 1 pass. Total records: %d", total)
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

			// Use buffered writer for efficient spilling
			bw := bufio.NewWriter(spillFile)
			_, err = heapMerge(batch, bw, cfg.MaxRecordBytes)
			if err != nil {
				return stats, fmt.Errorf("failed merge to spill file: %v", err)
			}
			if err := bw.Flush(); err != nil {
				return stats, fmt.Errorf("failed to flush spill file: %v", err)
			}

			// Close input readers from this batch to prevent FD exhaustion
			// during high fan-in merges across multiple passes.
			for _, input := range batch {
				if rc, ok := input.(io.ReadCloser); ok {
					rc.Close()
				}
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
	total, err := heapMerge(currentInputs, w, cfg.MaxRecordBytes)
	if err != nil {
		return stats, err
	}
	stats.TotalRecords = total

	log.Printf("[shuffle] Multi-pass merge completed. Passes: %d, Spills: %d, Peak Streams: %d, Records: %d",
		stats.TotalPasses, stats.SpillCount, stats.PeakOpenStreams, total)

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

func (h *recordHeap) Push(x any) {
	*h = append(*h, x.(heapItem))
}

func (h *recordHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// heapMerge performs a single-pass k-way merge using a min-heap.
func heapMerge(readers []io.Reader, w io.Writer, maxRecordBytes int) (int64, error) {
	h := &recordHeap{}
	heap.Init(h)

	for i, r := range readers {
		scanner := bufio.NewScanner(r)
		// Set buffer limit to prevent OOM on oversized records.
		// The max token size is the larger of the initial buffer capacity and maxRecordBytes.
		initialBufSize := 64 * 1024
		if initialBufSize > maxRecordBytes {
			initialBufSize = maxRecordBytes
		}
		buf := make([]byte, initialBufSize)
		scanner.Buffer(buf, maxRecordBytes)

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
