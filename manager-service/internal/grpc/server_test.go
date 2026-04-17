package grpc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

	scheduler, err := manager.NewScheduler(db, 0, 1, &manager.MockOrchestrator{}, "manager-0:50051", 30)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	return db, mock, NewWorkerServer(scheduler, nil)
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
	if resp.ByteStart != 0 || resp.ByteEnd != 128 {
		t.Errorf("expected byte range [0,128], got [%d,%d]", resp.ByteStart, resp.ByteEnd)
	}
	if resp.SplitChecksum != "sha256-split-0" {
		t.Errorf("expected split checksum sha256-split-0, got %s", resp.SplitChecksum)
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

func TestWorkerServer_Register_ManifestFallback(t *testing.T) {
	db, mock, baseServer := setupMockServer(t)
	defer db.Close()

	origThreshold := maxTaskAssignmentSizeBytes
	maxTaskAssignmentSizeBytes = 500
	defer func() { maxTaskAssignmentSizeBytes = origThreshold }()

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

	inputRows := sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"})
	for i := 0; i < 40; i++ {
		inputRows.AddRow(fmt.Sprintf("s3://inputs/split-%d.jsonl", i), int64(i*128), int64((i+1)*128), fmt.Sprintf("sha256-split-%d", i))
	}
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(inputRows)

	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	uploader := &fakeManifestUploader{uri: "s3://mapreduce-manifests/test-manifest.json"}
	server := newWorkerServerWithManifestUploader(baseServer.scheduler, nil, uploader)
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
	if len(resp.DataLocations) != 1 || resp.DataLocations[0] != uploader.uri {
		t.Fatalf("expected manifest URI %q, got %+v", uploader.uri, resp.DataLocations)
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

	origThreshold := maxTaskAssignmentSizeBytes
	maxTaskAssignmentSizeBytes = 500
	defer func() { maxTaskAssignmentSizeBytes = origThreshold }()

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
		WillReturnRows(func() *sqlmock.Rows {
			rows := sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"})
			for i := 0; i < 15; i++ {
				rows.AddRow(fmt.Sprintf("s3://inputs/split-%d.jsonl", i), int64(i*128), int64((i+1)*128), fmt.Sprintf("sha256-split-%d", i))
			}
			return rows
		}())
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	uploader := &fakeManifestUploader{err: errors.New("upload failed")}
	server := newWorkerServerWithManifestUploader(baseServer.scheduler, nil, uploader)
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

func TestWorkerServer_TaskComplete_StaleAttemptReturnsPermissionDenied(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	staleAttemptID := uuid.New().String()
	currentAttemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", currentAttemptID))
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

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, "lease123").
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

func TestWorkerServer_TaskComplete_SuccessReturnsAck(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, "lease123").
		WillReturnRows(sqlmock.NewRows([]string{"lease_valid"}).AddRow(true))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryCompleteTask)).
		WithArgs(taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(manager.QuerySucceedAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryInsertOutput)).
		WithArgs(taskID, 0, "s3://outputs/reduce-0.jsonl", "sha256-output").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskJobID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"job_id"}).AddRow(jobID))
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

func TestWorkerServer_TaskFailed_StaleAttemptReturnsPermissionDenied(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	staleAttemptID := uuid.New().String()
	currentAttemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", currentAttemptID))
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

func TestWorkerServer_TaskFailed_ExpiredLeaseReturnsPermissionDenied(t *testing.T) {
	db, mock, server := setupMockServer(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, "lease123").
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

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(manager.QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryCheckLeaseValid)).
		WithArgs(attemptID, "lease123").
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
	mock.ExpectQuery(regexp.QuoteMeta(manager.QueryGetTaskJobID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"job_id"}).AddRow("job123"))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryUpdateTaskInProgress)).
		WithArgs(sqlmock.AnyArg(), taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(manager.QueryInsertAttempt)).
		WithArgs(sqlmock.AnyArg(), taskID, "system-recovery", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

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
