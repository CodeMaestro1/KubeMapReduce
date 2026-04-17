package grpc

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"kubemapreduce/manager-service/internal/manager"
	pb "kubemapreduce/proto"
)

func setupMockServer(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *WorkerServer) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	scheduler, err := manager.NewScheduler(db, 0)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	return db, mock, NewWorkerServer(scheduler)
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
			AddRow("s3://inputs/split-0.jsonl", 0, 128, "sha256-split-0"))

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
			AddRow("s3://inputs/split-0.jsonl", 0, 128, "sha256-split-0"))

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
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, leaseID).
		WillReturnRows(sqlmock.NewRows([]string{"lease_valid"}).AddRow(true))

	mock.ExpectExec(regexp.QuoteMeta(manager.QueryRenewLease)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

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
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, leaseID).
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
