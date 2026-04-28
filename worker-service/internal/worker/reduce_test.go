package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"google.golang.org/grpc"

	pb "kubemapreduce/proto"
)

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

	var completedReq *pb.TaskCompleteRequest
	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return &pb.TaskAssignment{
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
			}, nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
		},
		taskCompleteFn: func(_ context.Context, req *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			completedReq = req
			return &pb.Ack{Success: true}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Errorf("unexpected TaskFailed: %s", req.ErrorMessage)
			return &pb.Ack{}, nil
		},
	}

	w := newTestWorker(t, grpcClient, store)
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if completedReq == nil {
		t.Fatal("TaskComplete was not called")
	}
	if len(completedReq.OutputLocations) != 1 {
		t.Errorf("want 1 output, got %d", len(completedReq.OutputLocations))
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

	var failedReq *pb.TaskFailedRequest
	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return &pb.TaskAssignment{
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
			}, nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
		},
		taskCompleteFn: func(_ context.Context, _ *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Fatal("TaskComplete should not be called when shuffle checksum fails")
			return &pb.Ack{}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			failedReq = req
			return &pb.Ack{Success: true}, nil
		},
	}

	w := newTestWorker(t, grpcClient, store)
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("expected error for bad shuffle checksum")
	}
	if failedReq == nil {
		t.Fatal("TaskFailed was not called")
	}
	if !strings.Contains(failedReq.ErrorMessage, "checksum mismatch") {
		t.Fatalf("expected checksum mismatch in error, got: %q", failedReq.ErrorMessage)
	}
}

func TestWorker_ReduceNoChecksumStillSucceeds(t *testing.T) {
	store := newMockStorage()

	part0 := []byte(`{"key":"apple","value":"1"}` + "\n")
	store.put("mapreduce-staging", "job-1/map-0/att-0/partition-0.jsonl", part0)
	store.put("code", "reducer.py", []byte("# mock"))

	var completedReq *pb.TaskCompleteRequest
	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return &pb.TaskAssignment{
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
			}, nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
		},
		taskCompleteFn: func(_ context.Context, req *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			completedReq = req
			return &pb.Ack{Success: true}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Errorf("unexpected TaskFailed: %s", req.ErrorMessage)
			return &pb.Ack{}, nil
		},
	}

	w := newTestWorker(t, grpcClient, store)
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if completedReq == nil {
		t.Fatal("TaskComplete was not called")
	}
}
