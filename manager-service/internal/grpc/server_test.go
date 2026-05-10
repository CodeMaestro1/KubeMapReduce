package grpc

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"kubemapreduce/manager-service/internal/manager"
	pb "kubemapreduce/proto"
)

// splitManifestURIForTest parses a manifest URI of the form
// "<uri>#sha256=<hex>" and returns the URI, hex checksum, and whether the
// fragment was present.
func splitManifestURIForTest(s string) (string, string, bool) {
	idx := strings.Index(s, "#sha256=")
	if idx < 0 {
		return s, "", false
	}
	return s[:idx], s[idx+len("#sha256="):], true
}

// noOpDispatcher is a test-only TaskDispatcher for grpc server tests.
type noOpDispatcher struct{}

func (n *noOpDispatcher) DispatchTask(_ context.Context, _ *manager.Task) error { return nil }

func setupMockServer(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *WorkerServer) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	scheduler, err := manager.NewScheduler(db, 0, 1, &manager.MockOrchestrator{}, &noOpDispatcher{}, "manager-0:50051", 30, nil)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	return db, mock, NewWorkerServer(scheduler, nil, 0)
}

func TestWorkerServer_PruneStaleTaskQueues(t *testing.T) {
	now := time.Now()
	s := &WorkerServer{
		taskQueues:       make(map[string]*taskQueue),
		taskQueueIdleTTL: 10 * time.Minute,
	}

	staleEmpty := make(chan *pb.TaskAssignment, 1)
	freshEmpty := make(chan *pb.TaskAssignment, 1)
	staleNonEmpty := make(chan *pb.TaskAssignment, 1)
	staleNonEmpty <- &pb.TaskAssignment{TaskId: "t1"}

	s.taskQueues["stale-empty"] = &taskQueue{ch: staleEmpty, lastTouched: now.Add(-11 * time.Minute)}
	s.taskQueues["fresh-empty"] = &taskQueue{ch: freshEmpty, lastTouched: now.Add(-5 * time.Minute)}
	s.taskQueues["stale-non-empty"] = &taskQueue{ch: staleNonEmpty, lastTouched: now.Add(-11 * time.Minute)}

	s.pruneStaleTaskQueues(now)

	if _, ok := s.taskQueues["stale-empty"]; ok {
		t.Fatal("expected stale empty queue to be evicted")
	}
	if _, ok := s.taskQueues["fresh-empty"]; !ok {
		t.Fatal("expected fresh empty queue to remain")
	}
	if _, ok := s.taskQueues["stale-non-empty"]; !ok {
		t.Fatal("expected non-empty queue to remain")
	}
}

func TestWorkerServer_GetOrCreateTaskQueue_ReusesAndTouches(t *testing.T) {
	fixedNow := time.Now()
	nowVal := fixedNow
	s := &WorkerServer{
		taskQueues:       make(map[string]*taskQueue),
		taskQueueIdleTTL: 10 * time.Minute,
		taskQueueNow: func() time.Time {
			return nowVal
		},
	}

	q1 := s.getOrCreateTaskQueue("job-1")
	entry1 := s.taskQueues["job-1"]
	if entry1 == nil {
		t.Fatal("expected queue entry to be created")
	}
	if entry1.lastTouched != fixedNow {
		t.Fatalf("expected created queue lastTouched %v, got %v", fixedNow, entry1.lastTouched)
	}

	nowVal = fixedNow.Add(2 * time.Minute)
	q2 := s.getOrCreateTaskQueue("job-1")
	entry2 := s.taskQueues["job-1"]

	if q1 != q2 {
		t.Fatal("expected same queue channel to be reused")
	}
	if entry2.lastTouched != nowVal {
		t.Fatalf("expected reused queue lastTouched %v, got %v", nowVal, entry2.lastTouched)
	}
}

type fakeManifestUploader struct {
	uri     string
	err     error
	payload []byte
}

func (f *fakeManifestUploader) UploadManifest(ctx context.Context, bucketName, objectName string, payload []byte) (string, error) {
	f.payload = append([]byte(nil), payload...)
	if f.err != nil {
		return "", f.err
	}
	if f.uri != "" {
		return f.uri, nil
	}
	return fmt.Sprintf("s3://%s/%s", bucketName, objectName), nil
}

func TestNewWorkerServer(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("unexpected error creating sqlmock: %v", err)
	}
	defer db.Close()

	scheduler, err := manager.NewScheduler(db, 0, 1, &manager.MockOrchestrator{}, &noOpDispatcher{}, "manager-0:50051", 30, nil)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	t.Run("without minioClient", func(t *testing.T) {
		server := NewWorkerServer(scheduler, nil, 0)
		if server == nil {
			t.Fatal("expected non-nil server")
		}
		if server.scheduler != scheduler {
			t.Errorf("expected scheduler %v, got %v", scheduler, server.scheduler)
		}
		if server.minioClient != nil {
			t.Errorf("expected nil minioClient, got %v", server.minioClient)
		}
		if server.uploader != nil {
			t.Errorf("expected nil uploader, got %v", server.uploader)
		}
		if server.manifestThresholdBytes != maxTaskAssignmentSizeBytes {
			t.Errorf("expected default threshold %d, got %d", maxTaskAssignmentSizeBytes, server.manifestThresholdBytes)
		}
	})

	t.Run("with minioClient and threshold", func(t *testing.T) {
		dummyClient := &minio.Client{}
		threshold := 1024

		server := NewWorkerServer(scheduler, dummyClient, threshold)
		if server == nil {
			t.Fatal("expected non-nil server")
		}
		if server.minioClient != dummyClient {
			t.Errorf("expected minioClient %v, got %v", dummyClient, server.minioClient)
		}
		if server.uploader == nil {
			t.Fatal("expected non-nil uploader")
		}
		if _, ok := server.uploader.(*minioManifestUploader); !ok {
			t.Errorf("expected uploader to be *minioManifestUploader, got %T", server.uploader)
		}
		if server.manifestThresholdBytes != threshold {
			t.Errorf("expected threshold %d, got %d", threshold, server.manifestThresholdBytes)
		}
	})
}

func TestWorkerServer_Register_MissingArgs(t *testing.T) {
	db, _, server := setupMockServer(t)
	defer db.Close()

	cases := []struct {
		name string
		req  *pb.RegisterRequest
	}{
		{
			name: "missing task_id",
			req:  &pb.RegisterRequest{AttemptId: "attempt-1"},
		},
		{
			name: "missing attempt_id",
			req:  &pb.RegisterRequest{TaskId: "task-1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.Register(context.Background(), tc.req)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", err)
			}
		})
	}
}

func TestWorkerServer_Register_Success(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "In-Progress", attemptID, 0))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "s3://code/combiner.py", 3, "sha256-input"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow("s3://mapreduce-inputs/split-0.jsonl", 0, 128, "sha256-split-0"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	req := &pb.RegisterRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
	}

	resp, err := server.Register(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.TaskId != taskID {
		t.Errorf("expected task_id %s, got %s", taskID, resp.TaskId)
	}
	if resp.LeaseId != "lease123" {
		t.Errorf("expected lease_id lease123, got %s", resp.LeaseId)
	}
	if resp.ByteStart != 0 || resp.ByteEnd != 128 {
		t.Errorf("expected byte range [0,128], got [%d,%d]", resp.ByteStart, resp.ByteEnd)
	}
	if resp.SplitChecksum != "sha256-split-0" {
		t.Errorf("expected split checksum sha256-split-0, got %s", resp.SplitChecksum)
	}
}

func TestWorkerServer_Register_RuntimeEnvInferredFromExtension(t *testing.T) {
	cases := []struct {
		mapperURI   string
		wantRuntime string
	}{
		{"s3://code/mapper.py", "python"},
		{"s3://code/mapper.jar", "java"},
		{"s3://code/mapper.c", "c"},
		{"s3://code/mapper.cpp", "cpp"},
		{"s3://code/mapper.cc", "cpp"},
		{"s3://code/mapper.cxx", "cpp"},
		{"s3://code/mapper", ""},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.mapperURI, func(t *testing.T) {
			db, mock, server := setupMockServer(t)
			defer db.Close()

			taskID := uuid.New().String()
			jobID := uuid.New().String()
			attemptID := uuid.New().String()

			mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
				WithArgs(taskID).
				WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
					AddRow(taskID, jobID, "Map", "In-Progress", attemptID, 0))

			mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
				WithArgs(taskID).
				WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
					AddRow(tc.mapperURI, "s3://code/reducer.py", "", 1, ""))

			mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
				WithArgs(taskID).
				WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
					AddRow("s3://mapreduce-inputs/data.jsonl", 0, 0, ""))

			mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
				WithArgs(attemptID).
				WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
					AddRow("worker-1", "lease-abc", time.Now(), time.Now()))

			resp, err := server.Register(context.Background(), &pb.RegisterRequest{
				TaskId:    taskID,
				AttemptId: attemptID,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.RuntimeEnv != tc.wantRuntime {
				t.Errorf("mapper=%s: want RuntimeEnv=%q, got %q", tc.mapperURI, tc.wantRuntime, resp.RuntimeEnv)
			}
		})
	}
}

func TestWorkerServer_Register_PermissionDenied(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()
	wrongAttempt := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "In-Progress", attemptID, 0))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "s3://code/combiner.py", 3, "sha256-input"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow("s3://mapreduce-inputs/split-0.jsonl", 0, 128, "sha256-split-0"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	req := &pb.RegisterRequest{
		TaskId:    taskID,
		AttemptId: wrongAttempt,
	}

	_, err := server.Register(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", err)
	}
	if strings.Contains(st.Message(), attemptID) {
		t.Fatalf("permission denied message must not leak expected attempt_id")
	}
}

func TestWorkerServer_Heartbeat_MissingArguments(t *testing.T) {
	_, _, server := setupMockServer(t)

	tests := []struct {
		name      string
		taskID    string
		attemptID string
		leaseID   string
	}{
		{"Missing TaskId", "", "attempt123", "lease123"},
		{"Missing AttemptId", "task123", "", "lease123"},
		{"Missing LeaseId", "task123", "attempt123", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pb.HeartbeatRequest{
				TaskId:    tt.taskID,
				AttemptId: tt.attemptID,
				LeaseId:   tt.leaseID,
			}
			_, err := server.Heartbeat(context.Background(), req)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected gRPC status error, got: %v", err)
			}
			if st.Code() != codes.InvalidArgument {
				t.Errorf("expected InvalidArgument, got %v", st.Code())
			}
		})
	}
}

func TestWorkerServer_Heartbeat_TaskNotFound(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := "lease123"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnError(sql.ErrNoRows)

	mock.ExpectRollback()

	req := &pb.HeartbeatRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
		LeaseId:   leaseID,
	}

	resp, err := server.Heartbeat(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error, should return TERMINATE cleanly: %v", err)
	}
	if resp.Action != pb.HeartbeatResponse_TERMINATE {
		t.Errorf("expected TERMINATE, got %v", resp.Action)
	}
}

func TestWorkerServer_Heartbeat_InternalError(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := "lease123"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnError(errors.New("db connection failed"))

	mock.ExpectRollback()

	req := &pb.HeartbeatRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
		LeaseId:   leaseID,
	}

	_, err := server.Heartbeat(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Internal {
		t.Errorf("expected Internal error, got %v", st.Code())
	}
}

func TestWorkerServer_Heartbeat_Success(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := "lease123"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id", "job_id"}).AddRow("In-Progress", attemptID, uuid.New().String()))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, leaseID, 5).
		WillReturnRows(sqlmock.NewRows([]string{"lease_valid"}).AddRow(true))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryRenewLease)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	req := &pb.HeartbeatRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
		LeaseId:   leaseID,
	}

	resp, err := server.Heartbeat(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Action != pb.HeartbeatResponse_CONTINUE {
		t.Errorf("expected CONTINUE, got %v", resp.Action)
	}
}

func TestWorkerServer_Heartbeat_Expired(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := "lease123"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id", "job_id"}).AddRow("In-Progress", attemptID, uuid.New().String()))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, leaseID, 5).
		WillReturnRows(sqlmock.NewRows([]string{"lease_valid"}).AddRow(false))

	mock.ExpectRollback()

	req := &pb.HeartbeatRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
		LeaseId:   leaseID,
	}

	resp, err := server.Heartbeat(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error, should return TERMINATE cleanly: %v", err)
	}
	if resp.Action != pb.HeartbeatResponse_TERMINATE {
		t.Errorf("expected TERMINATE, got %v", resp.Action)
	}
}

func TestWorkerServer_Register_ReduceUsesReplicaPartition(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Reduce", "In-Progress", attemptID, 3))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "s3://code/combiner.py", 3, "sha256-input"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetReduceTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"partition_index", "output_uri", "checksum"}).
			AddRow(3, "s3://shuffle/part-3-0.jsonl", "sha256-output"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-2", "lease456", time.Now(), time.Now()))

	resp, err := server.Register(context.Background(), &pb.RegisterRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.PartitionId != 3 {
		t.Errorf("expected partition_id 3, got %d", resp.PartitionId)
	}
	if resp.SplitChecksum != "" {
		t.Errorf("expected empty split checksum for reduce assignment, got %q", resp.SplitChecksum)
	}
}

// TestWorkerServer_Register_DirectModeUnderThreshold verifies that small
// assignments bypass the manifest fallback entirely: IsManifest stays false,
// the original data_locations are sent inline, and the uploader is never
// invoked. This is the "direct mode" branch of the manifest protocol.
func TestWorkerServer_Register_DirectModeUnderThreshold(t *testing.T) {
	db, mock, baseServer := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "In-Progress", attemptID, 0))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "s3://code/combiner.py", 3, "sha256-input"))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow("s3://mapreduce-inputs/split-0.jsonl", 0, 128, "sha256-split-0"))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	uploader := &fakeManifestUploader{uri: "s3://mapreduce-manifests/SHOULD-NOT-BE-USED"}
	server := newWorkerServerWithManifestUploader(baseServer.scheduler, nil, uploader, 1<<20)

	resp, err := server.Register(context.Background(), &pb.RegisterRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.IsManifest {
		t.Fatalf("expected direct mode, got IsManifest=true")
	}
	if len(resp.DataLocations) != 1 || resp.DataLocations[0] != "s3://mapreduce-inputs/split-0.jsonl" {
		t.Fatalf("expected inline data_locations, got %+v", resp.DataLocations)
	}
	if len(uploader.payload) != 0 {
		t.Fatalf("expected uploader to not be invoked in direct mode")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}

func TestWorkerServer_Register_ManifestFallback(t *testing.T) {
	db, mock, baseServer := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "In-Progress", attemptID, 0))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "s3://code/combiner.py", 3, "sha256-input"))

	// A single split with a URI long enough to push the proto over the 500-byte threshold.
	longURI := "s3://mapreduce-inputs/" + strings.Repeat("a", 600) + ".jsonl"
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow(longURI, 0, 128, "sha256-split-0"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	uploader := &fakeManifestUploader{uri: "s3://mapreduce-manifests/test-manifest.json"}
	server := newWorkerServerWithManifestUploader(baseServer.scheduler, nil, uploader, 500)
	resp, err := server.Register(context.Background(), &pb.RegisterRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.IsManifest {
		t.Fatalf("expected manifest mode")
	}
	if len(resp.DataLocations) != 1 {
		t.Fatalf("expected exactly one manifest entry, got %+v", resp.DataLocations)
	}
	gotURI, gotChecksum, ok := splitManifestURIForTest(resp.DataLocations[0])
	if !ok {
		t.Fatalf("expected manifest URI with sha256 fragment, got %q", resp.DataLocations[0])
	}
	if gotURI != uploader.uri {
		t.Fatalf("expected manifest URI %q, got %q", uploader.uri, gotURI)
	}
	wantDigest := sha256.Sum256(uploader.payload)
	if gotChecksum != hex.EncodeToString(wantDigest[:]) {
		t.Fatalf("expected sha256 fragment %x, got %s", wantDigest, gotChecksum)
	}
	if len(uploader.payload) == 0 {
		t.Fatalf("expected manifest payload to be uploaded")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}

func TestWorkerServer_Register_ManifestUploadFailureReturnsError(t *testing.T) {
	db, mock, baseServer := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "In-Progress", attemptID, 0))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "s3://code/combiner.py", 3, "sha256-input"))
	longURI := "s3://mapreduce-inputs/" + strings.Repeat("b", 600) + ".jsonl"
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow(longURI, 0, 128, "sha256-split-0"))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	uploader := &fakeManifestUploader{err: errors.New("upload failed")}
	server := newWorkerServerWithManifestUploader(baseServer.scheduler, nil, uploader, 500)
	_, err := server.Register(context.Background(), &pb.RegisterRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}

func TestWorkerServer_Register_ManifestTooLargeReturnsResourceExhausted(t *testing.T) {
	db, mock, baseServer := setupMockServer(t)
	defer db.Close()

	origThreshold := maxTaskAssignmentSizeBytes
	maxTaskAssignmentSizeBytes = 100
	defer func() { maxTaskAssignmentSizeBytes = origThreshold }()
	origManifestThreshold := maxManifestPayloadSizeBytes
	maxManifestPayloadSizeBytes = 100
	defer func() { maxManifestPayloadSizeBytes = origManifestThreshold }()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "In-Progress", attemptID, 0))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "s3://code/combiner.py", 3, "sha256-input"))
	longURI := "s3://mapreduce-inputs/" + strings.Repeat("c", 600) + ".jsonl"
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow(longURI, 0, 128, "sha256-split-0"))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	uploader := &fakeManifestUploader{uri: "s3://mapreduce-manifests/test-manifest.json"}
	server := newWorkerServerWithManifestUploader(baseServer.scheduler, nil, uploader, 0)
	_, err := server.Register(context.Background(), &pb.RegisterRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
	if len(uploader.payload) != 0 {
		t.Fatalf("expected no manifest upload when marshaled manifest exceeds threshold")
	}
}

func TestWorkerServer_TaskComplete_MissingArgumentsReturnsInvalidArgument(t *testing.T) {
	_, _, server := setupMockServer(t)

	cases := []struct {
		name      string
		taskID    string
		attemptID string
		leaseID   string
	}{
		{"missing_task_id", "", "attempt123", "lease123"},
		{"missing_attempt_id", "task123", "", "lease123"},
		{"missing_lease_id", "task123", "attempt123", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.TaskComplete(context.Background(), &pb.TaskCompleteRequest{
				TaskId:          tc.taskID,
				AttemptId:       tc.attemptID,
				LeaseId:         tc.leaseID,
				OutputLocations: []string{"s3://outputs/reduce-0.jsonl"},
				OutputChecksums: []string{"sha256-output"},
			})

			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}

			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for %s, got %v", tc.name, err)
			}
		})
	}
}

func TestWorkerServer_TaskComplete_StaleAttemptReturnsPermissionDenied(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	staleAttemptID := uuid.New().String()
	currentAttemptID := uuid.New().String()

	jobID := uuid.New().String()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"job_id", "status"}).AddRow(jobID, "Running"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id", "job_id"}).AddRow("In-Progress", currentAttemptID, uuid.New().String()))
	mock.ExpectRollback()

	_, err := server.TaskComplete(context.Background(), &pb.TaskCompleteRequest{
		TaskId:          taskID,
		AttemptId:       staleAttemptID,
		LeaseId:         "lease123",
		OutputLocations: []string{"s3://outputs/reduce-0.jsonl"},
		OutputChecksums: []string{"sha256-output"},
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestWorkerServer_TaskComplete_ExpiredLeaseReturnsPermissionDenied(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	jobID := uuid.New().String()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"job_id", "status"}).AddRow(jobID, "Running"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id", "job_id"}).AddRow("In-Progress", attemptID, uuid.New().String()))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, "lease123", 5).
		WillReturnRows(sqlmock.NewRows([]string{"lease_valid"}).AddRow(false))
	mock.ExpectRollback()

	_, err := server.TaskComplete(context.Background(), &pb.TaskCompleteRequest{
		TaskId:          taskID,
		AttemptId:       attemptID,
		LeaseId:         "lease123",
		OutputLocations: []string{"s3://outputs/reduce-0.jsonl"},
		OutputChecksums: []string{"sha256-output"},
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestWorkerServer_TaskComplete_OutputMismatchReturnsInvalidArgument(t *testing.T) {
	_, _, server := setupMockServer(t)

	_, err := server.TaskComplete(context.Background(), &pb.TaskCompleteRequest{
		TaskId:          uuid.New().String(),
		AttemptId:       uuid.New().String(),
		LeaseId:         "lease123",
		OutputLocations: []string{"s3://outputs/reduce-0.jsonl"},
		OutputChecksums: nil,
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestWorkerServer_TaskComplete_TaskNotFoundReturnsNotFound(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err := server.TaskComplete(context.Background(), &pb.TaskCompleteRequest{
		TaskId:          taskID,
		AttemptId:       attemptID,
		LeaseId:         "lease123",
		OutputLocations: []string{"s3://outputs/reduce-0.jsonl"},
		OutputChecksums: []string{"sha256-output"},
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestWorkerServer_TaskComplete_InternalErrorReturnsInternal(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	_, err := server.TaskComplete(context.Background(), &pb.TaskCompleteRequest{
		TaskId:          taskID,
		AttemptId:       attemptID,
		LeaseId:         "lease123",
		OutputLocations: []string{"s3://outputs/reduce-0.jsonl"},
		OutputChecksums: []string{"sha256-output"},
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}

func TestWorkerServer_TaskComplete_SuccessReturnsAck(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"job_id", "status"}).AddRow(jobID, "Running"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id", "job_id"}).AddRow("In-Progress", attemptID, uuid.New().String()))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, "lease123", 5).
		WillReturnRows(sqlmock.NewRows([]string{"lease_valid"}).AddRow(true))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryCompleteTask)).
		WithArgs(taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(manager.QuerySucceedAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryInsertOutputBulkBase)).
		WithArgs(taskID, 0, "s3://outputs/reduce-0.jsonl", "sha256-output").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCountAllPendingTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectCommit()

	resp, err := server.TaskComplete(context.Background(), &pb.TaskCompleteRequest{
		TaskId:          taskID,
		AttemptId:       attemptID,
		LeaseId:         "lease123",
		OutputLocations: []string{"s3://outputs/reduce-0.jsonl"},
		OutputChecksums: []string{"sha256-output"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success ack")
	}
}

func TestWorkerServer_TaskFailed_MissingArgsReturnsInvalidArgument(t *testing.T) {
	_, _, server := setupMockServer(t)

	tests := []struct {
		name      string
		taskId    string
		attemptId string
		leaseId   string
	}{
		{"missing task_id", "", "attempt123", "lease123"},
		{"missing attempt_id", "task123", "", "lease123"},
		{"missing lease_id", "task123", "attempt123", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.TaskFailed(context.Background(), &pb.TaskFailedRequest{
				TaskId:       tc.taskId,
				AttemptId:    tc.attemptId,
				LeaseId:      tc.leaseId,
				ErrorMessage: "worker crashed",
			})
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", err)
			}
		})
	}
}

func TestWorkerServer_TaskFailed_TaskNotFoundReturnsNotFound(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err := server.TaskFailed(context.Background(), &pb.TaskFailedRequest{
		TaskId:       taskID,
		AttemptId:    attemptID,
		LeaseId:      "lease123",
		ErrorMessage: "worker crashed",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestWorkerServer_TaskFailed_InternalErrorReturnsInternal(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnError(errors.New("database connection failed"))
	mock.ExpectRollback()

	_, err := server.TaskFailed(context.Background(), &pb.TaskFailedRequest{
		TaskId:       taskID,
		AttemptId:    attemptID,
		LeaseId:      "lease123",
		ErrorMessage: "worker crashed",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}
func TestWorkerServer_TaskFailed_StaleAttemptReturnsPermissionDenied(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	staleAttemptID := uuid.New().String()
	currentAttemptID := uuid.New().String()

	jobID := uuid.New().String()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"job_id", "status"}).AddRow(jobID, "Running"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id", "job_id"}).AddRow("In-Progress", currentAttemptID, uuid.New().String()))
	mock.ExpectRollback()

	_, err := server.TaskFailed(context.Background(), &pb.TaskFailedRequest{
		TaskId:       taskID,
		AttemptId:    staleAttemptID,
		LeaseId:      "lease123",
		ErrorMessage: "worker crashed",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

// TestWorkerServer_Register_MapTaskSingleSplitURI verifies that a map task with
// one input split sends exactly that split's URI in DataLocations with the
// correct byte range.
func TestWorkerServer_Register_MapTaskSingleSplitURI(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "In-Progress", attemptID, 0))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "", 3, "sha256-input"))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow("s3://mapreduce-inputs/split-0.jsonl", int64(100), int64(200), "sha256-0"))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease-x", time.Now(), time.Now()))

	resp, err := server.Register(context.Background(), &pb.RegisterRequest{TaskId: taskID, AttemptId: attemptID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.DataLocations) != 1 || resp.DataLocations[0] != "s3://mapreduce-inputs/split-0.jsonl" {
		t.Errorf("expected exactly 1 URI 's3://mapreduce-inputs/split-0.jsonl', got %v", resp.DataLocations)
	}
	if resp.ByteStart != 100 || resp.ByteEnd != 200 {
		t.Errorf("expected byte range [100,200], got [%d,%d]", resp.ByteStart, resp.ByteEnd)
	}
	if resp.SplitChecksum != "sha256-0" {
		t.Errorf("expected checksum sha256-0, got %s", resp.SplitChecksum)
	}
	if len(resp.InputSplits) != 1 {
		t.Fatalf("expected 1 input split, got %d", len(resp.InputSplits))
	}
	if resp.InputSplits[0].InputUri != "s3://mapreduce-inputs/split-0.jsonl" {
		t.Fatalf("expected split URI s3://mapreduce-inputs/split-0.jsonl, got %q", resp.InputSplits[0].InputUri)
	}
}

// TestWorkerServer_Register_MapTaskMultipleSplitsPreservesAll verifies that
// map tasks carrying multiple input splits expose all split metadata to the
// worker while still keeping the first split in the legacy top-level fields.
func TestWorkerServer_Register_MapTaskMultipleSplitsPreservesAll(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "In-Progress", attemptID, 0))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "", 3, "sha256-input"))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow("s3://mapreduce-inputs/split-0.jsonl", int64(0), int64(128), "sha256-0").
			AddRow("s3://mapreduce-inputs/split-1.jsonl", int64(128), int64(256), "sha256-1"))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease-y", time.Now(), time.Now()))

	resp, err := server.Register(context.Background(), &pb.RegisterRequest{TaskId: taskID, AttemptId: attemptID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.DataLocations) != 2 || resp.DataLocations[0] != "s3://mapreduce-inputs/split-0.jsonl" || resp.DataLocations[1] != "s3://mapreduce-inputs/split-1.jsonl" {
		t.Errorf("expected both split URIs in data locations, got %v", resp.DataLocations)
	}
	if resp.ByteStart != 0 || resp.ByteEnd != 128 {
		t.Errorf("expected first split byte range [0,128], got [%d,%d]", resp.ByteStart, resp.ByteEnd)
	}
	if resp.SplitChecksum != "sha256-0" {
		t.Errorf("expected first split checksum sha256-0, got %s", resp.SplitChecksum)
	}
	if len(resp.InputSplits) != 2 {
		t.Fatalf("expected 2 input splits, got %d", len(resp.InputSplits))
	}
	if resp.InputSplits[0].InputUri != "s3://mapreduce-inputs/split-0.jsonl" || resp.InputSplits[1].InputUri != "s3://mapreduce-inputs/split-1.jsonl" {
		t.Fatalf("unexpected split URIs: %+v", resp.InputSplits)
	}
	if resp.InputSplits[1].ByteStart != 128 || resp.InputSplits[1].ByteEnd != 256 {
		t.Fatalf("expected second split byte range [128,256], got [%d,%d]", resp.InputSplits[1].ByteStart, resp.InputSplits[1].ByteEnd)
	}
	if resp.InputSplits[1].SplitChecksum != "sha256-1" {
		t.Fatalf("expected second split checksum sha256-1, got %q", resp.InputSplits[1].SplitChecksum)
	}
}

func TestWorkerServer_TaskFailed_ExpiredLeaseReturnsPermissionDenied(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	jobID := uuid.New().String()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"job_id", "status"}).AddRow(jobID, "Running"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id", "job_id"}).AddRow("In-Progress", attemptID, uuid.New().String()))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, "lease123", 5).
		WillReturnRows(sqlmock.NewRows([]string{"lease_valid"}).AddRow(false))
	mock.ExpectRollback()

	_, err := server.TaskFailed(context.Background(), &pb.TaskFailedRequest{
		TaskId:       taskID,
		AttemptId:    attemptID,
		LeaseId:      "lease123",
		ErrorMessage: "worker crashed",
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestWorkerServer_TaskFailed_SuccessReturnsAck(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	jobID := uuid.New().String()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"job_id", "status"}).AddRow(jobID, "Running"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id", "job_id"}).AddRow("In-Progress", attemptID, uuid.New().String()))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, "lease123", 5).
		WillReturnRows(sqlmock.NewRows([]string{"lease_valid"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCountAttemptsByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryUpdateTaskStatus)).
		WithArgs("Idle", taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryFailAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryUpdateTaskInProgress)).
		WithArgs(sqlmock.AnyArg(), taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryInsertAttempt)).
		WithArgs(sqlmock.AnyArg(), taskID, "system-recovery", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Metadata queries called by GetTaskByID during redispatch
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "In-Progress", "new-attempt", 0))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("m", "r", "", 1, "c"))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow("u", 0, 1, "c"))

	resp, err := server.TaskFailed(context.Background(), &pb.TaskFailedRequest{
		TaskId:       taskID,
		AttemptId:    attemptID,
		LeaseId:      "lease123",
		ErrorMessage: "worker crashed",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success ack")
	}
}
