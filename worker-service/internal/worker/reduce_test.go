package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"

	pb "kubemapreduce/proto"
)

// ── shared helpers ────────────────────────────────────────────────────────────

// reduceAssignment builds a minimal REDUCE TaskAssignment for direct runReduce calls.
func reduceAssignment(dataLocations []string) *pb.TaskAssignment {
	return &pb.TaskAssignment{
		TaskId:        "task-2",
		AttemptId:     "attempt-1",
		JobId:         "job-1",
		Type:          pb.TaskType_REDUCE,
		LeaseId:       "lease-2",
		CodeLocation:  "s3://code/reducer.py",
		DataLocations: dataLocations,
		PartitionId:   0,
	}
}

// failingPutStorage delegates GetObject to mockStorage but always fails PutObject.
type failingPutStorage struct{ *mockStorage }

func (f *failingPutStorage) PutObject(_ context.Context, _, _ string, _ io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, fmt.Errorf("storage unavailable")
}

// ── splitChecksumURI ──────────────────────────────────────────────────────────

func TestSplitChecksumURI_NoFragment(t *testing.T) {
	uri, chk := splitChecksumURI("s3://bucket/key/path.jsonl")
	if uri != "s3://bucket/key/path.jsonl" {
		t.Errorf("uri: %q", uri)
	}
	if chk != "" {
		t.Errorf("checksum: %q, want empty", chk)
	}
}

func TestSplitChecksumURI_WithSHA256Fragment(t *testing.T) {
	uri, chk := splitChecksumURI("s3://bucket/key.jsonl#sha256=abc123def")
	if uri != "s3://bucket/key.jsonl" {
		t.Errorf("uri: %q", uri)
	}
	if chk != "abc123def" {
		t.Errorf("checksum: %q", chk)
	}
}

func TestSplitChecksumURI_UnknownFragment(t *testing.T) {
	raw := "s3://bucket/key.jsonl#etag=xyz"
	uri, chk := splitChecksumURI(raw)
	if uri != raw {
		t.Errorf("uri should be unchanged: %q", uri)
	}
	if chk != "" {
		t.Errorf("checksum: %q, want empty for unknown fragment", chk)
	}
}

func TestSplitChecksumURI_EmptySHA256Value(t *testing.T) {
	uri, chk := splitChecksumURI("s3://bucket/key.jsonl#sha256=")
	if uri != "s3://bucket/key.jsonl" {
		t.Errorf("uri: %q", uri)
	}
	if chk != "" {
		t.Errorf("checksum: %q, want empty string for blank value", chk)
	}
}

// ── reduce checksum validation ────────────────────────────────────────────────

func TestWorker_ReduceAcceptsCorrectShuffleChecksum(t *testing.T) {
	store := newMockStorage()

	part0 := []byte(`{"key":"apple","value":"1"}` + "\n" + `{"key":"cherry","value":"3"}` + "\n")
	part1 := []byte(`{"key":"banana","value":"2"}` + "\n" + `{"key":"date","value":"4"}` + "\n")

	store.put("mapreduce-staging", "job-1/map-0/att-0/partition-0.jsonl", part0)
	store.put("mapreduce-staging", "job-1/map-1/att-0/partition-0.jsonl", part1)
	store.put("code", "reducer.py", []byte("# mock"))

	sum0 := sha256.Sum256(part0)
	sum1 := sha256.Sum256(part1)
	chk0 := hex.EncodeToString(sum0[:])
	chk1 := hex.EncodeToString(sum1[:])

	assignment := &pb.TaskAssignment{
		TaskId:       "task-2",
		AttemptId:    "attempt-1",
		JobId:        "job-1",
		Type:         pb.TaskType_REDUCE,
		LeaseId:      "lease-2",
		CodeLocation: "s3://code/reducer.py",
		DataLocations: []string{
			"s3://mapreduce-staging/job-1/map-0/att-0/partition-0.jsonl#sha256=" + chk0,
			"s3://mapreduce-staging/job-1/map-1/att-0/partition-0.jsonl#sha256=" + chk1,
		},
		PartitionId: 0,
	}

	w := newTestWorker(t, &mockGRPCClient{}, store)
	outputURIs, _, err := w.runReduce(context.Background(), assignment)
	if err != nil {
		t.Fatalf("runReduce: %v", err)
	}
	if len(outputURIs) != 1 {
		t.Errorf("want 1 output, got %d", len(outputURIs))
	}
}

func TestWorker_ReduceRejectsBadShuffleChecksum(t *testing.T) {
	store := newMockStorage()

	part0 := []byte(`{"key":"apple","value":"1"}` + "\n")
	part1 := []byte(`{"key":"banana","value":"2"}` + "\n")

	store.put("mapreduce-staging", "job-1/map-0/att-0/partition-0.jsonl", part0)
	store.put("mapreduce-staging", "job-1/map-1/att-0/partition-0.jsonl", part1)
	store.put("code", "reducer.py", []byte("# mock"))

	sum0 := sha256.Sum256(part0)
	chk0 := hex.EncodeToString(sum0[:])

	assignment := &pb.TaskAssignment{
		TaskId:       "task-2",
		AttemptId:    "attempt-1",
		JobId:        "job-1",
		Type:         pb.TaskType_REDUCE,
		LeaseId:      "lease-2",
		CodeLocation: "s3://code/reducer.py",
		DataLocations: []string{
			"s3://mapreduce-staging/job-1/map-0/att-0/partition-0.jsonl#sha256=" + chk0,
			"s3://mapreduce-staging/job-1/map-1/att-0/partition-0.jsonl#sha256=deadbeefdeadbeef",
		},
		PartitionId: 0,
	}

	w := newTestWorker(t, &mockGRPCClient{}, store)
	_, _, err := w.runReduce(context.Background(), assignment)
	if err == nil {
		t.Fatal("expected error for bad shuffle checksum")
	}
	if !strings.Contains(err.Error(), "SHA-256 mismatch") && !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch in error, got: %v", err)
	}
}

func TestWorker_ReduceNoChecksumStillSucceeds(t *testing.T) {
	store := newMockStorage()

	part0 := []byte(`{"key":"apple","value":"1"}` + "\n")
	store.put("mapreduce-staging", "job-1/map-0/att-0/partition-0.jsonl", part0)
	store.put("code", "reducer.py", []byte("# mock"))

	assignment := &pb.TaskAssignment{
		TaskId:       "task-2",
		AttemptId:    "attempt-1",
		JobId:        "job-1",
		Type:         pb.TaskType_REDUCE,
		LeaseId:      "lease-2",
		CodeLocation: "s3://code/reducer.py",
		DataLocations: []string{
			"s3://mapreduce-staging/job-1/map-0/att-0/partition-0.jsonl",
		},
		PartitionId: 0,
	}

	w := newTestWorker(t, &mockGRPCClient{}, store)
	_, _, err := w.runReduce(context.Background(), assignment)
	if err != nil {
		t.Fatalf("runReduce: %v", err)
	}
}

// ── direct runReduce unit tests ───────────────────────────────────────────────

func TestRunReduce_HappyPath(t *testing.T) {
	store := newMockStorage()
	part0 := []byte(`{"key":"apple","value":"1"}` + "\n" + `{"key":"cherry","value":"3"}` + "\n")
	part1 := []byte(`{"key":"banana","value":"2"}` + "\n" + `{"key":"date","value":"4"}` + "\n")
	store.put("mapreduce-staging", "job-1/map-0/partition-0.jsonl", part0)
	store.put("mapreduce-staging", "job-1/map-1/partition-0.jsonl", part1)
	store.put("code", "reducer.py", []byte("# mock"))

	w := newTestWorker(t, nil, store)
	a := reduceAssignment([]string{
		"s3://mapreduce-staging/job-1/map-0/partition-0.jsonl",
		"s3://mapreduce-staging/job-1/map-1/partition-0.jsonl",
	})

	uris, checksums, err := w.runReduce(context.Background(), a)
	if err != nil {
		t.Fatalf("runReduce: %v", err)
	}
	wantURI := "s3://mapreduce-outputs/job-1/partition-0.jsonl"
	if len(uris) != 1 || uris[0] != wantURI {
		t.Errorf("uris = %v, want [%s]", uris, wantURI)
	}
	if len(checksums) != 1 || checksums[0] == "" {
		t.Errorf("checksums = %v, want one non-empty entry", checksums)
	}
	if _, ok := store.uploaded[outputBucket+"/job-1/partition-0.jsonl"]; !ok {
		t.Errorf("output not found in uploaded map")
	}
}

func TestRunReduce_EmptyDataLocations(t *testing.T) {
	store := newMockStorage()
	store.put("code", "reducer.py", []byte("# mock"))

	w := newTestWorker(t, nil, store)
	a := reduceAssignment(nil)

	uris, checksums, err := w.runReduce(context.Background(), a)
	if err != nil {
		t.Fatalf("runReduce with zero inputs: %v", err)
	}
	if len(uris) != 1 {
		t.Errorf("want 1 output URI, got %d: %v", len(uris), uris)
	}
	if len(checksums) != 1 || checksums[0] == "" {
		t.Errorf("want 1 non-empty checksum, got %v", checksums)
	}
}

func TestRunReduce_GetObjectFailure(t *testing.T) {
	store := newMockStorage()
	store.put("code", "reducer.py", []byte("# mock"))
	// Input URI is not seeded — GetObject will fail.

	w := newTestWorker(t, nil, store)
	a := reduceAssignment([]string{"s3://mapreduce-staging/missing/file.jsonl"})

	_, _, err := w.runReduce(context.Background(), a)
	if err == nil {
		t.Fatal("expected GetObject error, got nil")
	}
	if !strings.Contains(err.Error(), "download input") && !strings.Contains(err.Error(), "object not found") {
		t.Errorf("expected download/object error, got: %v", err)
	}
}

func TestRunReduce_ChecksumMismatchOnShuffleInput(t *testing.T) {
	store := newMockStorage()
	store.put("mapreduce-staging", "job-1/part.jsonl", []byte(`{"key":"k","value":"v"}`+"\n"))
	store.put("code", "reducer.py", []byte("# mock"))

	w := newTestWorker(t, nil, store)
	a := reduceAssignment([]string{
		"s3://mapreduce-staging/job-1/part.jsonl#sha256=deadbeefdeadbeef",
	})

	_, _, err := w.runReduce(context.Background(), a)
	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") && !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Errorf("expected checksum mismatch in error, got: %v", err)
	}
}

func TestRunReduce_ExecCodeFailure(t *testing.T) {
	store := newMockStorage()
	store.put("mapreduce-staging", "job-1/part.jsonl", []byte(`{"key":"k","value":"v"}`+"\n"))
	store.put("code", "reducer.py", []byte("# mock"))

	w := newTestWorker(t, nil, store)
	w.execCode = func(_ context.Context, _ string, _ string, _ io.Reader) ([]byte, error) {
		return nil, fmt.Errorf("exec failed")
	}
	a := reduceAssignment([]string{"s3://mapreduce-staging/job-1/part.jsonl"})

	_, _, err := w.runReduce(context.Background(), a)
	if err == nil {
		t.Fatal("expected exec error, got nil")
	}
	if !strings.Contains(err.Error(), "reducer") {
		t.Errorf("expected 'reducer' in error, got: %v", err)
	}
}

func TestRunReduce_PutObjectFailure(t *testing.T) {
	inner := newMockStorage()
	inner.put("mapreduce-staging", "job-1/part.jsonl", []byte(`{"key":"k","value":"v"}`+"\n"))
	inner.put("code", "reducer.py", []byte("# mock"))
	store := &failingPutStorage{inner}

	w := newTestWorker(t, nil, store)
	a := reduceAssignment([]string{"s3://mapreduce-staging/job-1/part.jsonl"})

	_, _, err := w.runReduce(context.Background(), a)
	if err == nil {
		t.Fatal("expected upload error, got nil")
	}
	if !strings.Contains(err.Error(), "upload output") {
		t.Errorf("expected 'upload output' in error, got: %v", err)
	}
}

func TestRunReduce_ChecksumCorrectness(t *testing.T) {
	input := []byte(`{"key":"hello","value":"world"}` + "\n")
	store := newMockStorage()
	store.put("mapreduce-staging", "job-1/part.jsonl", input)
	store.put("code", "reducer.py", []byte("# mock"))

	// Identity execCode: reducer emits exactly what it receives.
	w := newTestWorker(t, nil, store)
	a := reduceAssignment([]string{"s3://mapreduce-staging/job-1/part.jsonl"})

	_, checksums, err := w.runReduce(context.Background(), a)
	if err != nil {
		t.Fatalf("runReduce: %v", err)
	}
	if len(checksums) != 1 {
		t.Fatalf("want 1 checksum, got %d", len(checksums))
	}

	// The reducer output is whatever execCode (identity) returns; compute its SHA-256.
	uploaded := store.uploaded[outputBucket+"/job-1/partition-0.jsonl"]
	h := sha256.Sum256(uploaded)
	want := hex.EncodeToString(h[:])
	if checksums[0] != want {
		t.Errorf("checksum = %q, want %q", checksums[0], want)
	}
}

func TestRunReduce_MergeFailure(t *testing.T) {
	store := newMockStorage()
	// A 200-byte record: exceeds MaxRecordBytes=1, causing scanner buffer overflow.
	longLine := `{"key":"apple","value":"` + strings.Repeat("x", 200) + `"}` + "\n"
	store.put("mapreduce-staging", "job-1/big.jsonl", []byte(longLine))
	store.put("code", "reducer.py", []byte("# mock"))

	w := newTestWorker(t, nil, store)
	w.cfg.ShuffleMaxRecordBytes = 1

	a := reduceAssignment([]string{"s3://mapreduce-staging/job-1/big.jsonl"})
	_, _, err := w.runReduce(context.Background(), a)
	if err == nil {
		t.Fatal("expected merge error, got nil")
	}
	if !strings.Contains(err.Error(), "merge") {
		t.Errorf("expected 'merge' in error, got: %v", err)
	}
}
