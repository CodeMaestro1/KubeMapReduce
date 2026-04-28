package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc"

	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/config"
)

// ── mock gRPC client ──────────────────────────────────────────────────────────

type mockGRPCClient struct {
	registerFn     func(context.Context, *pb.RegisterRequest, ...grpc.CallOption) (*pb.TaskAssignment, error)
	heartbeatFn    func(context.Context, *pb.HeartbeatRequest, ...grpc.CallOption) (*pb.HeartbeatResponse, error)
	taskCompleteFn func(context.Context, *pb.TaskCompleteRequest, ...grpc.CallOption) (*pb.Ack, error)
	taskFailedFn   func(context.Context, *pb.TaskFailedRequest, ...grpc.CallOption) (*pb.Ack, error)
}

func (m *mockGRPCClient) Register(ctx context.Context, req *pb.RegisterRequest, opts ...grpc.CallOption) (*pb.TaskAssignment, error) {
	return m.registerFn(ctx, req, opts...)
}
func (m *mockGRPCClient) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest, opts ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
	return m.heartbeatFn(ctx, req, opts...)
}
func (m *mockGRPCClient) TaskComplete(ctx context.Context, req *pb.TaskCompleteRequest, opts ...grpc.CallOption) (*pb.Ack, error) {
	return m.taskCompleteFn(ctx, req, opts...)
}
func (m *mockGRPCClient) TaskFailed(ctx context.Context, req *pb.TaskFailedRequest, opts ...grpc.CallOption) (*pb.Ack, error) {
	return m.taskFailedFn(ctx, req, opts...)
}

// ── mock object storage ───────────────────────────────────────────────────────

type mockStorage struct {
	objects  map[string][]byte // bucket+"/"+key → content
	uploaded map[string][]byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		objects:  make(map[string][]byte),
		uploaded: make(map[string][]byte),
	}
}

func (m *mockStorage) put(bucket, key string, data []byte) {
	m.objects[bucket+"/"+key] = data
}

func (m *mockStorage) GetObject(_ context.Context, bucket, key string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	data, ok := m.objects[bucket+"/"+key]
	if !ok {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockStorage) PutObject(_ context.Context, bucket, key string, reader io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	m.uploaded[bucket+"/"+key] = data
	return minio.UploadInfo{Bucket: bucket, Key: key}, nil
}

// ── test helpers ──────────────────────────────────────────────────────────────

func newTestWorker(t *testing.T, grpcClient pb.WorkerServiceClient, store objectStorage) *Worker {
	t.Helper()
	cfg := &config.Config{
		TaskID:               "task-1",
		AttemptID:            "attempt-1",
		ManagerAddr:          "localhost:50051",
		HeartbeatIntervalSec: 60,
		TempDir:              t.TempDir(),
	}
	return &Worker{
		cfg:     cfg,
		client:  grpcClient,
		storage: store,
		prepareCode: func(_ context.Context, _ objectStorage, _ string, _ string) (string, func(), error) {
			return "/fake/code", func() {}, nil
		},
		execCode: func(_ context.Context, _ string, _ string, stdin io.Reader) ([]byte, error) {
			// Echo stdin back as output (identity mapper/reducer).
			return io.ReadAll(stdin)
		},
	}
}

// mapAssignment returns a minimal MAP TaskAssignment for tests.
func mapAssignment(dataURI string) *pb.TaskAssignment {
	return &pb.TaskAssignment{
		TaskId:        "task-1",
		AttemptId:     "attempt-1",
		JobId:         "job-1",
		Type:          pb.TaskType_MAP,
		LeaseId:       "lease-1",
		CodeLocation:  "s3://code/mapper.py",
		DataLocations: []string{dataURI},
		TotalReducers: 2,
	}
}

// ── MAP success ───────────────────────────────────────────────────────────────

func TestWorker_MapSuccess(t *testing.T) {
	store := newMockStorage()

	// Pre-load input data and code.
	inputData := `{"key":"apple","value":"1"}` + "\n" + `{"key":"banana","value":"2"}` + "\n"
	store.put("inputs", "data.jsonl", []byte(inputData))
	store.put("code", "mapper.py", []byte("# mock"))

	var completedReq *pb.TaskCompleteRequest
	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return mapAssignment("s3://inputs/data.jsonl"), nil
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
	if completedReq.TaskId != "task-1" {
		t.Errorf("TaskId: %s", completedReq.TaskId)
	}
	// With 2 reducers, there should be 2 output URIs.
	if len(completedReq.OutputLocations) != 2 {
		t.Errorf("want 2 output locations, got %d: %v", len(completedReq.OutputLocations), completedReq.OutputLocations)
	}
	// Verify staging uploads happened.
	if len(store.uploaded) == 0 {
		t.Error("expected staging uploads, got none")
	}
}

// ── MAP: records partitioned correctly ───────────────────────────────────────

func TestWorker_MapPartitionsRecords(t *testing.T) {
	store := newMockStorage()

	// 4 records that should partition deterministically across R=2 buckets.
	records := []struct{ K, V string }{
		{"alpha", "1"}, {"beta", "2"}, {"gamma", "3"}, {"delta", "4"},
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range records {
		enc.Encode(map[string]string{"key": r.K, "value": r.V})
	}
	store.put("inputs", "data.jsonl", buf.Bytes())
	store.put("code", "mapper.py", []byte("# mock"))

	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return mapAssignment("s3://inputs/data.jsonl"), nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
		},
		taskCompleteFn: func(_ context.Context, _ *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			return &pb.Ack{Success: true}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Errorf("unexpected failure: %s", req.ErrorMessage)
			return &pb.Ack{}, nil
		},
	}

	w := newTestWorker(t, grpcClient, store)
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Each key should hash to exactly one of the 2 partitions.
	totalRecords := 0
	for k, data := range store.uploaded {
		_ = k
		sc := newLineScanner(bytes.NewReader(data))
		for sc.Scan() {
			if len(bytes.TrimSpace(sc.Bytes())) > 0 {
				totalRecords++
			}
		}
	}
	if totalRecords != len(records) {
		t.Errorf("total records in staging: got %d, want %d", totalRecords, len(records))
	}
}

// ── REDUCE success ────────────────────────────────────────────────────────────

func TestWorker_ReduceSuccess(t *testing.T) {
	store := newMockStorage()

	// Two pre-sorted MAP output files for partition 0.
	part0 := `{"key":"apple","value":"1"}` + "\n" + `{"key":"cherry","value":"3"}` + "\n"
	part1 := `{"key":"banana","value":"2"}` + "\n" + `{"key":"date","value":"4"}` + "\n"
	store.put("mapreduce-staging", "job-1/map-0/att-0/partition-0.jsonl", []byte(part0))
	store.put("mapreduce-staging", "job-1/map-1/att-0/partition-0.jsonl", []byte(part1))
	store.put("code", "reducer.py", []byte("# mock"))

	reduceAssignment := &pb.TaskAssignment{
		TaskId:       "task-2",
		AttemptId:    "attempt-1",
		JobId:        "job-1",
		Type:         pb.TaskType_REDUCE,
		LeaseId:      "lease-2",
		CodeLocation: "s3://code/reducer.py",
		DataLocations: []string{
			"s3://mapreduce-staging/job-1/map-0/att-0/partition-0.jsonl",
			"s3://mapreduce-staging/job-1/map-1/att-0/partition-0.jsonl",
		},
		PartitionId: 0,
	}

	var completedReq *pb.TaskCompleteRequest
	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return reduceAssignment, nil
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

	// Output should exist in the mock storage.
	outKey := "job-1/partition-0.jsonl"
	if _, ok := store.uploaded[outputBucket+"/"+outKey]; !ok {
		t.Errorf("output not uploaded to %s/%s", outputBucket, outKey)
	}
}

// ── SIGTERM → TaskFailed ──────────────────────────────────────────────────────

func TestWorker_SIGTERMCausesTaskFailed(t *testing.T) {
	store := newMockStorage()
	store.put("inputs", "data.jsonl", []byte(`{"key":"k","value":"v"}`+"\n"))
	store.put("code", "mapper.py", []byte("# mock"))

	ctx, cancel := context.WithCancel(context.Background())

	var failedReq *pb.TaskFailedRequest
	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return mapAssignment("s3://inputs/data.jsonl"), nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
		},
		taskCompleteFn: func(_ context.Context, _ *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Error("TaskComplete should not be called after SIGTERM")
			return &pb.Ack{}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			failedReq = req
			return &pb.Ack{Success: true}, nil
		},
	}

	cfg := &config.Config{
		TaskID:               "task-1",
		AttemptID:            "attempt-1",
		ManagerAddr:          "localhost:50051",
		HeartbeatIntervalSec: 1, // short so heartbeat goroutine starts quickly
		TempDir:              t.TempDir(),
	}
	w := &Worker{
		cfg:     cfg,
		client:  grpcClient,
		storage: store,
		prepareCode: func(_ context.Context, _ objectStorage, _ string, _ string) (string, func(), error) {
			return "/fake/code", func() {}, nil
		},
		// Block until ctx is cancelled to simulate long-running work.
		execCode: func(execCtx context.Context, _ string, _ string, _ io.Reader) ([]byte, error) {
			cancel() // simulate SIGTERM during execution
			<-execCtx.Done()
			return nil, execCtx.Err()
		},
	}

	err := w.Run(ctx)
	if err == nil {
		t.Fatal("expected error after SIGTERM")
	}

	// Allow heartbeat goroutine to deliver terminated signal.
	deadline := time.Now().Add(2 * time.Second)
	for failedReq == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if failedReq == nil {
		t.Fatal("TaskFailed was not called after SIGTERM")
	}
	if failedReq.TaskId != "task-1" {
		t.Errorf("TaskFailed TaskId: %s", failedReq.TaskId)
	}
}

// ── TERMINATE from heartbeat ──────────────────────────────────────────────────

func TestWorker_HeartbeatTerminateCausesTaskFailed(t *testing.T) {
	store := newMockStorage()
	store.put("inputs", "data.jsonl", []byte(`{"key":"k","value":"v"}`+"\n"))
	store.put("code", "mapper.py", []byte("# mock"))

	terminated := make(chan struct{})
	var failedReq *pb.TaskFailedRequest

	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return &pb.TaskAssignment{
				TaskId: "task-1", AttemptId: "attempt-1", JobId: "job-1",
				Type: pb.TaskType_MAP, LeaseId: "lease-1",
				CodeLocation:  "s3://code/mapper.py",
				DataLocations: []string{"s3://inputs/data.jsonl"},
				TotalReducers: 1,
			}, nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_TERMINATE}, nil
		},
		taskCompleteFn: func(_ context.Context, _ *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Error("TaskComplete should not be called after TERMINATE")
			return &pb.Ack{}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			failedReq = req
			close(terminated)
			return &pb.Ack{Success: true}, nil
		},
	}

	cfg := &config.Config{
		TaskID: "task-1", AttemptID: "attempt-1", ManagerAddr: "localhost:50051",
		HeartbeatIntervalSec: 1,
		TempDir:              t.TempDir(),
	}
	w := &Worker{
		cfg:     cfg,
		client:  grpcClient,
		storage: store,
		prepareCode: func(_ context.Context, _ objectStorage, _ string, _ string) (string, func(), error) {
			return "/fake/code", func() {}, nil
		},
		execCode: func(execCtx context.Context, _ string, _ string, _ io.Reader) ([]byte, error) {
			<-execCtx.Done()
			return nil, execCtx.Err()
		},
	}

	err := w.Run(context.Background())
	if err == nil {
		t.Fatal("expected error after TERMINATE")
	}

	select {
	case <-terminated:
	case <-time.After(3 * time.Second):
		t.Fatal("TaskFailed was not called within timeout")
	}

	if failedReq == nil || failedReq.TaskId != "task-1" {
		t.Errorf("unexpected failedReq: %+v", failedReq)
	}
}

// ── Register failure ──────────────────────────────────────────────────────────

func TestWorker_RegisterFailureReturnsError(t *testing.T) {
	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return nil, fmt.Errorf("no such task")
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return nil, nil
		},
		taskCompleteFn: func(_ context.Context, _ *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			return nil, nil
		},
		taskFailedFn: func(_ context.Context, _ *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			return nil, nil
		},
	}
	w := newTestWorker(t, grpcClient, newMockStorage())
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("expected error from failed Register")
	}
}

func TestWorker_MissingStorageReportsTaskFailed(t *testing.T) {
	var failedReq *pb.TaskFailedRequest
	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return &pb.TaskAssignment{
				TaskId:        "task-1",
				AttemptId:     "attempt-1",
				JobId:         "job-1",
				Type:          pb.TaskType_MAP,
				LeaseId:       "lease-1",
				CodeLocation:  "s3://code/mapper.py",
				DataLocations: []string{"s3://inputs/data.jsonl"},
				TotalReducers: 1,
			}, nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
		},
		taskCompleteFn: func(_ context.Context, _ *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Fatal("TaskComplete should not be called when storage is unavailable")
			return &pb.Ack{}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			failedReq = req
			return &pb.Ack{Success: true}, nil
		},
	}

	cfg := &config.Config{
		TaskID:               "task-1",
		AttemptID:            "attempt-1",
		ManagerAddr:          "localhost:50051",
		HeartbeatIntervalSec: 60,
		TempDir:              t.TempDir(),
	}
	w := New(cfg, grpcClient, nil)

	if err := w.Run(context.Background()); err == nil {
		t.Fatal("expected error when storage is unavailable")
	}
	if failedReq == nil {
		t.Fatal("TaskFailed was not called")
	}
	if !strings.Contains(failedReq.ErrorMessage, "object storage is not configured") {
		t.Fatalf("unexpected error message: %q", failedReq.ErrorMessage)
	}
}

func TestWorker_EmptyManifestReportsTaskFailed(t *testing.T) {
	store := newMockStorage()

	var failedReq *pb.TaskFailedRequest
	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			return &pb.TaskAssignment{
				TaskId:        "task-1",
				AttemptId:     "attempt-1",
				JobId:         "job-1",
				Type:          pb.TaskType_MAP,
				LeaseId:       "lease-1",
				IsManifest:    true,
				TotalReducers: 1,
			}, nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
		},
		taskCompleteFn: func(_ context.Context, _ *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Fatal("TaskComplete should not be called for an empty manifest")
			return &pb.Ack{}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			failedReq = req
			return &pb.Ack{Success: true}, nil
		},
	}

	w := newTestWorker(t, grpcClient, store)
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("expected error for empty manifest data_locations")
	}
	if failedReq == nil {
		t.Fatal("TaskFailed was not called")
	}
	if !strings.Contains(failedReq.ErrorMessage, "manifest assignment missing data locations") {
		t.Fatalf("unexpected error message: %q", failedReq.ErrorMessage)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{r: r, buf: make([]byte, 0, 4096)}
}

type lineScanner struct {
	r    io.Reader
	buf  []byte
	line []byte
	done bool
}

func (s *lineScanner) Scan() bool {
	if s.done {
		return false
	}
	tmp := make([]byte, 256)
	for {
		if i := bytes.IndexByte(s.buf, '\n'); i >= 0 {
			s.line = s.buf[:i]
			s.buf = s.buf[i+1:]
			return true
		}
		n, err := s.r.Read(tmp)
		s.buf = append(s.buf, tmp[:n]...)
		if err != nil {
			s.done = true
			if len(s.buf) > 0 {
				s.line = s.buf
				s.buf = nil
				return true
			}
			return false
		}
	}
}

func (s *lineScanner) Bytes() []byte { return s.line }
