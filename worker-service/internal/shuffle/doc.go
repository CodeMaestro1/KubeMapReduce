// Package shuffle implements the k-way merge logic used to aggregate intermediate mapper outputs.
//
// # Overview
// This package is responsible for the "Shuffle" phase of the MapReduce paradigm. It takes
// multiple sorted JSONL streams (produced by Mapper tasks) and merges them into a single,
// globally sorted stream for a Reducer task.
//
// # Design Rationale
// The core algorithm is an external k-way merge sort using a min-heap (implemented via the
// [container/heap] package). This approach was chosen to allow the system to merge data
// that is much larger than the available RAM. By spilling intermediate batches to disk
// (multi-pass merge), the Worker maintains a constant memory footprint regardless of
// the input size.
//
// # Key Types
//   - [Record]: Represents a single key-value pair in a JSONL stream.
//   - [MergeConfig]: Tunables for controlling file descriptor usage and temp storage.
//   - [MergeStats]: Observability metrics for the merge operation.
//
// # Thread Safety
// The [MergeInputs] function is self-contained and does not share state. It is safe to
// call concurrently from multiple goroutines (e.g., when merging data for multiple
// Reducer partitions simultaneously).
//
// # Error Handling
// Errors are wrapped and returned if JSON unmarshalling fails or if disk I/O errors occur
// during spilling. Callers are responsible for cleaning up the final output writer.
package shuffle
