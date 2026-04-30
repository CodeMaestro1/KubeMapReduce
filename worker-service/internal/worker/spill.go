package worker

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"

	"kubemapreduce/worker-service/internal/shuffle"
)

// recordOverheadBytes accounts for JSON encoding overhead per record (quotes,
// braces, commas) when estimating the in-memory footprint of a record. The
// number is intentionally a slight over-estimate so the spill threshold is
// reached before the real heap allocation grows past the configured budget.
const recordOverheadBytes = 32

// defaultSpillThresholdBytes is the fallback in-memory budget when the caller
// passes a non-positive threshold. It mirrors the production default of
// MAP_SORT_SPILL_THRESHOLD_MB=256.
const defaultSpillThresholdBytes int64 = 256 * 1024 * 1024

// spillingSorter sorts a stream of [shuffle.Record] values by key while
// enforcing an in-memory byte budget.
//
// Records are buffered in memory until their estimated combined size reaches
// thresholdBytes; at that point the buffered slice is sorted and written to a
// temporary JSONL file on disk in sorted order ("spilled"). [Finalize] sorts
// any remaining buffer, spills it if other on-disk runs already exist, and
// returns an [io.ReadCloser] that streams the merged sorted JSONL output. The
// returned reader's Close method removes every spill file.
//
// This guarantees that the Map sort phase never holds more than roughly
// thresholdBytes of records in memory at once, preventing OOM on large map
// outputs (issue #152).
type spillingSorter struct {
	thresholdBytes int64
	tempDir        string

	buffer      []shuffle.Record
	bufferBytes int64
	spillFiles  []*os.File
}

// newSpillingSorter constructs a spillingSorter with the given memory budget
// (in bytes) and temporary directory for spill files. Non-positive thresholds
// fall back to [defaultSpillThresholdBytes]; an empty tempDir falls back to
// [os.TempDir].
func newSpillingSorter(thresholdBytes int64, tempDir string) *spillingSorter {
	if thresholdBytes <= 0 {
		thresholdBytes = defaultSpillThresholdBytes
	}
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	return &spillingSorter{
		thresholdBytes: thresholdBytes,
		tempDir:        tempDir,
	}
}

// AddAll appends every record to the sorter, spilling intermediate runs to
// disk whenever the in-memory budget is exceeded.
func (s *spillingSorter) AddAll(records []shuffle.Record) error {
	for _, rec := range records {
		if err := s.Add(rec); err != nil {
			return err
		}
	}
	return nil
}

// Add appends a single record to the sorter and spills the in-memory buffer
// to a temporary JSONL file when the byte budget is exceeded.
func (s *spillingSorter) Add(rec shuffle.Record) error {
	s.buffer = append(s.buffer, rec)
	s.bufferBytes += int64(len(rec.Key)) + int64(len(rec.Value)) + recordOverheadBytes
	if s.bufferBytes >= s.thresholdBytes {
		return s.spill()
	}
	return nil
}

// SpillCount reports the number of spill files written so far. Useful for
// observability and tests.
func (s *spillingSorter) SpillCount() int {
	return len(s.spillFiles)
}

// spill sorts the in-memory buffer by key and writes it to a new temporary
// JSONL file, leaving the file open and rewound so subsequent merge passes
// can read it as a sorted stream.
func (s *spillingSorter) spill() error {
	if len(s.buffer) == 0 {
		return nil
	}
	sort.Slice(s.buffer, func(i, j int) bool { return s.buffer[i].Key < s.buffer[j].Key })

	f, err := os.CreateTemp(s.tempDir, "map-spill-*.jsonl")
	if err != nil {
		return fmt.Errorf("create spill file: %w", err)
	}

	bw := bufio.NewWriter(f)
	enc := json.NewEncoder(bw)
	for _, rec := range s.buffer {
		if err := enc.Encode(rec); err != nil {
			f.Close()
			os.Remove(f.Name())
			return fmt.Errorf("write spill record: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("flush spill file: %w", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("rewind spill file: %w", err)
	}

	s.spillFiles = append(s.spillFiles, f)
	s.buffer = s.buffer[:0]
	s.bufferBytes = 0
	return nil
}

// Finalize closes the input phase and returns an [io.ReadCloser] that emits
// the fully merged, sorted JSONL stream of every record previously added.
//
// When no records ever spilled, the in-memory buffer is sorted and returned
// directly. Otherwise the trailing buffer is spilled too and all on-disk runs
// are merged via [shuffle.MergeInputs]. The returned reader's Close method
// MUST be called by the caller; it removes every spill file from disk.
func (s *spillingSorter) Finalize() (io.ReadCloser, error) {
	// Fast path: never spilled, return in-memory sorted JSONL.
	if len(s.spillFiles) == 0 {
		sort.Slice(s.buffer, func(i, j int) bool { return s.buffer[i].Key < s.buffer[j].Key })
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for _, rec := range s.buffer {
			if err := enc.Encode(rec); err != nil {
				return nil, fmt.Errorf("encode buffered record: %w", err)
			}
		}
		s.buffer = nil
		s.bufferBytes = 0
		return io.NopCloser(&buf), nil
	}

	// Mixed path: spill the trailing buffer so all data sits in sorted runs
	// on disk, then merge every run into a single sorted stream.
	if err := s.spill(); err != nil {
		s.cleanup()
		return nil, err
	}

	readers := make([]io.Reader, 0, len(s.spillFiles))
	for _, f := range s.spillFiles {
		readers = append(readers, f)
	}

	pr, pw := io.Pipe()
	go func() {
		_, err := shuffle.MergeInputs(readers, pw, shuffle.MergeConfig{TempDir: s.tempDir})
		_ = pw.CloseWithError(err)
	}()

	return &spillReadCloser{reader: pr, sorter: s}, nil
}

// cleanup closes and removes every spill file. Safe to call multiple times.
func (s *spillingSorter) cleanup() {
	for _, f := range s.spillFiles {
		f.Close()
		os.Remove(f.Name())
	}
	s.spillFiles = nil
}

// spillReadCloser wraps the merge pipe so closing it both shuts down the
// upstream merge goroutine and removes every spill file from disk.
type spillReadCloser struct {
	reader *io.PipeReader
	sorter *spillingSorter
	closed bool
}

func (r *spillReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *spillReadCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.reader.Close()
	r.sorter.cleanup()
	return err
}
