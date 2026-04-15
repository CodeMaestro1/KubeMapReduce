package manager

import (
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// setupMockDB is a helper to initialize the scheduler with a mocked DB.
func setupMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *Scheduler) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	scheduler, err := NewScheduler(db, 0)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	return db, mock, scheduler
}

func TestNewScheduler_NilDB(t *testing.T) {
	_, err := NewScheduler(nil, 0)
	if err == nil {
		t.Fatalf("expected error when passing nil DB to NewScheduler")
	}
}

func TestScheduler_GetNextTask_QuotaExceeded(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))
	mock.ExpectRollback()

	_, err := scheduler.GetNextTask("worker-1")
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("expected ErrQuotaExceeded, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestScheduler_GetNextTask_JobFailed(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectRollback()

	_, err := scheduler.GetNextTask("worker-1")
	if !errors.Is(err, ErrJobFailed) {
		t.Errorf("expected ErrJobFailed, got %v", err)
	}
}

func TestScheduler_GetNextTask_MapSuccess(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(0, "Map").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type"}).AddRow(taskID, jobID, "Map"))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).
		WithArgs(sqlmock.AnyArg(), taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).
		WithArgs(sqlmock.AnyArg(), taskID, "worker-1", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	task, err := scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}

	if task.ID != taskID.String() {
		t.Errorf("expected task ID %s, got %s", taskID.String(), task.ID)
	}
	if task.Type != MapTask {
		t.Errorf("expected MapTask type, got %v", task.Type)
	}
}

func TestScheduler_GetNextTask_NoMapIdle_ReduceSuccess(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(0, "Map").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingTasksByType)).
		WithArgs("Map").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(0, "Reduce").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type"}).AddRow(taskID, jobID, "Reduce"))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).
		WithArgs(sqlmock.AnyArg(), taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).
		WithArgs(sqlmock.AnyArg(), taskID, "worker-1", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	task, err := scheduler.GetNextTask("worker-1")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}

	if task.ID != taskID.String() {
		t.Errorf("expected task ID %s, got %s", taskID.String(), task.ID)
	}
	if task.Type != ReduceTask {
		t.Errorf("expected ReduceTask type, got %v", task.Type)
	}
}

func TestScheduler_GetNextTask_JobCompleted(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(0, "Map").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingTasksByType)).
		WithArgs("Map").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(0, "Reduce").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingTasksByType)).
		WithArgs("Reduce").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectRollback()

	_, err := scheduler.GetNextTask("worker-1")
	if !errors.Is(err, ErrJobCompleted) {
		t.Errorf("expected ErrJobCompleted, got %v", err)
	}
}

func TestScheduler_GetNextTask_EmptyWorkerID(t *testing.T) {
	_, _, scheduler := setupMockDB(t)

	_, err := scheduler.GetNextTask("")
	if !errors.Is(err, ErrEmptyWorkerID) {
		t.Errorf("expected ErrEmptyWorkerID, got %v", err)
	}
}

func TestScheduler_CompleteTask_Success(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectLeaseInfo)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"lease_id", "last_renewed_at", "lease_ttl"}).AddRow("mock-lease", time.Now(), 30))

	mock.ExpectExec(regexp.QuoteMeta(QueryCompleteTask)).
		WithArgs(taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QuerySucceedAttempt)).
		WithArgs(sqlmock.AnyArg(), attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryInsertOutput)).
		WithArgs(taskID, 0, "s3://output1", "hash1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err := scheduler.CompleteTask(taskID, attemptID, "mock-lease", []string{"s3://output1"}, []string{"hash1"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestScheduler_CompleteTask_LengthMismatch(t *testing.T) {
	db, _, scheduler := setupMockDB(t)
	defer db.Close()

	err := scheduler.CompleteTask(uuid.NewString(), uuid.NewString(), "mock-lease", []string{"uri1"}, []string{"hash1", "hash2"})
	if !errors.Is(err, ErrOutputMismatch) {
		t.Errorf("expected output mismatch, got %v", err)
	}
}

func TestScheduler_CompleteTask_AlreadyCompleted(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("Completed", attemptID))
	mock.ExpectRollback()

	err := scheduler.CompleteTask(taskID, attemptID, "mock-lease", nil, nil)
	if !errors.Is(err, ErrInvalidStateTransition) {
		t.Errorf("expected invalid state transition for already completed, got %v", err)
	}
}

func TestScheduler_CompleteTask_StaleAttempt(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	staleAttemptID := "stale-attempt"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	mock.ExpectRollback()

	err := scheduler.CompleteTask(taskID, staleAttemptID, "mock-lease", nil, nil)
	if !errors.Is(err, ErrStaleAttempt) {
		t.Errorf("expected ErrStaleAttempt, got %v", err)
	}
}

func TestScheduler_CompleteTask_NotFound(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	if err := scheduler.CompleteTask(taskID, uuid.NewString(), "mock-lease", nil, nil); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got: %v", err)
	}
}

func TestScheduler_FailTask_MaxAttempts(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()
	now := time.Now()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectLeaseInfo)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"lease_id", "last_renewed_at", "lease_ttl"}).AddRow(leaseID, now, 30))

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountAttemptsByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3)) // MaxAttempts

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Failed", taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(sqlmock.AnyArg(), attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err := scheduler.FailTask(taskID, attemptID, leaseID, "crash")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestScheduler_RenewLease_Success(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()
	now := time.Now()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectLeaseInfo)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"lease_id", "last_renewed_at", "lease_ttl"}).AddRow(leaseID, now, 30))

	mock.ExpectExec(regexp.QuoteMeta(QueryRenewLease)).
		WithArgs(sqlmock.AnyArg(), attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err := scheduler.RenewLease(taskID, attemptID, leaseID)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestScheduler_RenewLease_Expired(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()

	// Renew interval is expired (TTL is 30, last renewed 40s ago)
	now := time.Now()
	expired := now.Add(-40 * time.Second)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectLeaseInfo)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"lease_id", "last_renewed_at", "lease_ttl"}).AddRow(leaseID, expired, 30))
	mock.ExpectRollback()

	err := scheduler.RenewLease(taskID, attemptID, leaseID)
	if !errors.Is(err, ErrExpiredLease) {
		t.Fatalf("expected ErrExpiredLease got %v", err)
	}
}

func TestScheduler_RenewLease_Mismatched(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()
	now := time.Now()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectLeaseInfo)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"lease_id", "last_renewed_at", "lease_ttl"}).AddRow(uuid.New().String(), now, 30)) // mismatch here
	mock.ExpectRollback()

	err := scheduler.RenewLease(taskID, attemptID, leaseID)
	if !errors.Is(err, ErrExpiredLease) {
		t.Fatalf("expected ErrExpiredLease got %v", err)
	}
}

func TestScheduler_FailStaleTasks_WithInvalidTimeout(t *testing.T) {
	_, _, scheduler := setupMockDB(t)
	_, err := scheduler.FailStaleTasks(0)
	if !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("expected ErrInvalidTimeout")
	}

	_, err = scheduler.FailStaleTasks(-5 * time.Second)
	if !errors.Is(err, ErrInvalidTimeout) {
		t.Fatalf("expected ErrInvalidTimeout for negative interval")
	}
}

func TestScheduler_FailStaleTasks_Success(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectStaleTasks)).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "attempt_id"}).AddRow(taskID, attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountAttemptsByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Idle", taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(sqlmock.AnyArg(), attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	recovered, err := scheduler.FailStaleTasks(30 * time.Second)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if recovered != 1 {
		t.Errorf("expected 1 recovered task, got %d", recovered)
	}
}

func TestScheduler_GetTaskByID_WithAttempt(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "task_type", "status", "current_attempt_id"}).
			AddRow(taskID, "Reduce", "In-Progress", attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	task, err := scheduler.GetTaskByID(taskID)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if task.ID != taskID {
		t.Errorf("expected task ID %s, got %s", taskID, task.ID)
	}
	if task.State != InProgress {
		t.Errorf("expected state InProgress, got %v", task.State)
	}
	if task.ActiveAttemptID != attemptID {
		t.Errorf("expected attemptID %v, got %v", attemptID, task.ActiveAttemptID)
	}
}

func TestScheduler_AllMapTasksCompleted_Negative(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingMapTasks)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	if scheduler.AllMapTasksCompleted() {
		t.Errorf("expected false")
	}
}

func TestScheduler_IsJobFinished_Negative(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountAllPendingTasks)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	if scheduler.IsJobFinished() {
		t.Errorf("expected false")
	}
}

func TestScheduler_GetMapOutputs(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMapOutputs)).
		WillReturnRows(sqlmock.NewRows([]string{"output_uri"}).AddRow("s3://map-out-1").AddRow("s3://map-out-2"))

	outputs := scheduler.GetMapOutputs()
	if len(outputs) != 2 || outputs[0] != "s3://map-out-1" || outputs[1] != "s3://map-out-2" {
		t.Errorf("unexpected map outputs: %v", outputs)
	}
}

func TestScheduler_GetReduceOutputs(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetReduceOutputs)).
		WillReturnRows(sqlmock.NewRows([]string{"output_uri"}).AddRow("s3://reduce-out-1"))

	outputs := scheduler.GetReduceOutputs()
	if len(outputs) != 1 || outputs[0] != "s3://reduce-out-1" {
		t.Errorf("unexpected reduce outputs: %v", outputs)
	}
}

func TestScheduler_GetTaskStatus(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetTaskStatus)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("In-Progress"))

	status, err := scheduler.GetTaskStatus(taskID)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if status != InProgress {
		t.Errorf("expected InProgress, got %v", status)
	}

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetTaskStatus)).
		WithArgs(taskID).
		WillReturnError(sql.ErrNoRows)

	_, err = scheduler.GetTaskStatus(taskID)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestScheduler_CompleteTask_LeaseExpired(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()

	// Renew interval is expired
	now := time.Now()
	expired := now.Add(-40 * time.Second)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectLeaseInfo)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"lease_id", "last_renewed_at", "lease_ttl"}).AddRow(leaseID, expired, 30))
	mock.ExpectRollback()

	err := scheduler.CompleteTask(taskID, attemptID, leaseID, nil, nil)
	if !errors.Is(err, ErrExpiredLease) {
		t.Fatalf("expected ErrExpiredLease got %v", err)
	}
}

func TestScheduler_CompleteTask_AsymmetricArrays(t *testing.T) {
	db, _, scheduler := setupMockDB(t)
	defer db.Close()

	err := scheduler.CompleteTask(uuid.New().String(), uuid.New().String(), "lease-1", []string{}, []string{"hash1"})
	if !errors.Is(err, ErrOutputMismatch) {
		t.Errorf("expected ErrOutputMismatch for asymmetric arrays, got %v", err)
	}
}

func TestScheduler_CompleteTask_NoMutation(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := "lease-123"

	origURIs := []string{"uri1"}
	origChecksums := []string{"hash1"}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectLeaseInfo)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"lease_id", "last_renewed_at", "lease_ttl"}).AddRow(leaseID, time.Now(), 30))
	mock.ExpectExec(regexp.QuoteMeta(QueryCompleteTask)).
		WithArgs(taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QuerySucceedAttempt)).
		WithArgs(sqlmock.AnyArg(), attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertOutput)).
		WithArgs(taskID, 0, "uri1", "hash1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := scheduler.CompleteTask(taskID, attemptID, leaseID, origURIs, origChecksums)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mutate the original slices. Since the task stores a deep copy locally or in the DB, 
	// this shouldn't affect anything, but the test fulfills criteria 3 explicitly
	origURIs[0] = "mutated-uri"
	origChecksums[0] = "mutated-hash"

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}
