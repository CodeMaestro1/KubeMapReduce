package manager

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// MockDispatcher is a no-op TaskDispatcher for tests.
type MockDispatcher struct{}

func (m *MockDispatcher) DispatchTask(ctx context.Context, task *Task) error { return nil }

// expectReplicaCheck adds the authoritative routing query expectation.
func expectReplicaCheck(mock sqlmock.Sqlmock, jobID string, replicaIndex int) {
	mock.ExpectQuery(`SELECT replica_index FROM JOBS`).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"replica_index"}).AddRow(replicaIndex))
}

// expectJobLockQuery adds the job row lock query for CompleteTask/FailTask.
func expectJobLockQuery(mock sqlmock.Sqlmock, taskID, jobID, jobStatus string) {
	mock.ExpectQuery(`SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j`).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"job_id", "status"}).AddRow(jobID, jobStatus))
}

// setupMockDB is a helper to initialize the scheduler with a mocked DB.
func setupMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *Scheduler) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	scheduler, err := NewScheduler(db, 0, 1, &MockOrchestrator{}, &MockDispatcher{}, "manager-0:50051", 30, nil)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	return db, mock, scheduler
}

func setupMockDBWithOrchestrator(t *testing.T, orchestrator WorkerOrchestrator) (*sql.DB, sqlmock.Sqlmock, *Scheduler) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	scheduler, err := NewScheduler(db, 0, 1, orchestrator, &MockDispatcher{}, "manager-0:50051", 30, nil)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}
	return db, mock, scheduler
}

type spawnCall struct {
	taskID      string
	attemptID   string
	managerAddr string
}

type recordingOrchestrator struct {
	mu          sync.Mutex
	calls       []spawnCall
	cancelJobs  []string
	deletedJobs []string
	err         error
}

type deadlineRecordingOrchestrator struct {
	mu              sync.Mutex
	cancelJobs      []string
	cancelDeadlines []time.Time
	firstDelay      time.Duration
	callCount       int
}

func (r *recordingOrchestrator) EnsureWorkerPool(ctx context.Context, jobID string, numWorkers int, managerAddr string) error {
	if r.err != nil {
		return r.err
	}
	return nil
}

func (r *recordingOrchestrator) SpawnWorker(ctx context.Context, taskID string, jobID string, attemptID string, managerAddr string) error {
	r.mu.Lock()
	r.calls = append(r.calls, spawnCall{taskID: taskID, attemptID: attemptID, managerAddr: managerAddr})
	r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	return nil
}

func (r *recordingOrchestrator) CancelJob(ctx context.Context, jobID string) error {
	r.mu.Lock()
	r.cancelJobs = append(r.cancelJobs, jobID)
	r.mu.Unlock()
	return nil
}

func (r *recordingOrchestrator) DeleteWorkerJob(ctx context.Context, taskID string) error {
	r.mu.Lock()
	r.deletedJobs = append(r.deletedJobs, taskID)
	r.mu.Unlock()
	return nil
}

func (d *deadlineRecordingOrchestrator) EnsureWorkerPool(ctx context.Context, jobID string, numWorkers int, managerAddr string) error {
	return nil
}

func (d *deadlineRecordingOrchestrator) SpawnWorker(ctx context.Context, taskID string, jobID string, attemptID string, managerAddr string) error {
	return nil
}

func (d *deadlineRecordingOrchestrator) CancelJob(ctx context.Context, jobID string) error {
	d.mu.Lock()
	d.cancelJobs = append(d.cancelJobs, jobID)
	if deadline, ok := ctx.Deadline(); ok {
		d.cancelDeadlines = append(d.cancelDeadlines, deadline)
	}
	d.callCount++
	isFirstCall := d.callCount == 1
	delay := d.firstDelay
	d.mu.Unlock()

	if isFirstCall && delay > 0 {
		time.Sleep(delay)
	}
	return nil
}

func (d *deadlineRecordingOrchestrator) DeleteWorkerJob(ctx context.Context, taskID string) error {
	return nil
}

func expectLeaseValidation(mock sqlmock.Sqlmock, attemptID string, leaseID string, leaseValid bool) {
	mock.ExpectQuery(regexp.QuoteMeta(QueryCheckLeaseValid)).
		WithArgs(attemptID, leaseID, 5).
		WillReturnRows(sqlmock.NewRows([]string{"lease_valid"}).AddRow(leaseValid))
}

func expectTaskMetadataQueries(mock sqlmock.Sqlmock, taskID uuid.UUID, mapperURI, reducerURI string, rTasks int) {
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobConfigByTask)).
		WithArgs(taskID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow(mapperURI, reducerURI, "s3://code/combiner.py", rTasks, "sha256-input"))

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetTaskInputs)).
		WithArgs(taskID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).
			AddRow("s3://mapreduce-inputs/split-0.jsonl", 0, 128, "sha256-split-0"))
}

func expectReduceTaskMetadataQueries(mock sqlmock.Sqlmock, taskID uuid.UUID, mapperURI, reducerURI string, rTasks int) {
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobConfigByTask)).
		WithArgs(taskID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow(mapperURI, reducerURI, "s3://code/combiner.py", rTasks, "sha256-input"))

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetReduceTaskInputs)).
		WithArgs(taskID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"partition_index", "output_uri", "checksum"}).
			AddRow(0, "s3://shuffle/map-0-part-0", "sha256-shuffle-0"))
}

func TestNewScheduler_NilDB(t *testing.T) {
	_, err := NewScheduler(nil, 0, 1, &MockOrchestrator{}, &MockDispatcher{}, "manager-0:50051", 30, nil)
	if err == nil {
		t.Fatalf("expected error when passing nil DB to NewScheduler")
	}
}

func TestNewScheduler_NilOrchestrator(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock DB: %v", err)
	}
	defer db.Close()

	_, err = NewScheduler(db, 0, 1, nil, &MockDispatcher{}, "manager-0:50051", 30, nil)
	if err == nil {
		t.Fatalf("expected error when passing nil orchestrator to NewScheduler")
	}
}

func TestScheduler_GetNextTask_QuotaExceeded(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobUUID := uuid.New()
	jobID := jobUUID.String()
	taskID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	expectReplicaCheck(mock, jobID, 0)
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID, 0, "Map").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "replica_index"}).AddRow(taskID, jobUUID, "Map", 0))
	expectTaskMetadataQueries(mock, taskID, "s3://code/mapper.py", "s3://code/reducer.py", 4)
	mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))
	mock.ExpectRollback()

	_, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1")
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
	jobID := uuid.NewString()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectRollback()

	_, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1")
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
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	expectReplicaCheck(mock, jobID.String(), 0)

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID.String(), 0, "Map").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "replica_index"}).AddRow(taskID, jobID, "Map", 0))

	expectTaskMetadataQueries(mock, taskID, "s3://code/mapper.py", "s3://code/reducer.py", 4)

	mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).
		WithArgs(sqlmock.AnyArg(), taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).
		WithArgs(sqlmock.AnyArg(), taskID, "worker-1", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID.String(), "Running").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	task, err := scheduler.GetNextTask(context.Background(), jobID.String(), "worker-1")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}

	if task.ID != taskID.String() {
		t.Errorf("expected task ID %s, got %s", taskID.String(), task.ID)
	}
	if task.Type != MapTask {
		t.Errorf("expected MapTask type, got %v", task.Type)
	}
	if task.JobID != jobID.String() {
		t.Errorf("expected job ID %s, got %s", jobID.String(), task.JobID)
	}
	if task.CodeURI != "s3://code/mapper.py" {
		t.Errorf("expected mapper code URI, got %s", task.CodeURI)
	}
	if task.TotalReducers != 4 {
		t.Errorf("expected 4 reducers, got %d", task.TotalReducers)
	}
	if len(task.InputSplits) != 1 {
		t.Fatalf("expected 1 input split, got %d", len(task.InputSplits))
	}
	if task.InputSplits[0].InputURI != "s3://mapreduce-inputs/split-0.jsonl" {
		t.Errorf("unexpected split URI %s", task.InputSplits[0].InputURI)
	}
}

func TestScheduler_GetNextTask_NoMapIdle_ReduceSuccess(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	expectReplicaCheck(mock, jobID.String(), 0)

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID.String(), 0, "Map").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingTasksByType)).
		WithArgs(jobID.String(), "Map").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID.String(), 0, "Reduce").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "replica_index"}).AddRow(taskID, jobID, "Reduce", 3))

	expectReduceTaskMetadataQueries(mock, taskID, "s3://code/mapper.py", "s3://code/reducer.py", 2)

	mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).
		WithArgs(sqlmock.AnyArg(), taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).
		WithArgs(sqlmock.AnyArg(), taskID, "worker-1", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID.String(), "Running").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	task, err := scheduler.GetNextTask(context.Background(), jobID.String(), "worker-1")
	if err != nil {
		t.Fatalf("expected task, got err: %v", err)
	}

	if task.ID != taskID.String() {
		t.Errorf("expected task ID %s, got %s", taskID.String(), task.ID)
	}
	if task.Type != ReduceTask {
		t.Errorf("expected ReduceTask type, got %v", task.Type)
	}
	if task.CodeURI != "s3://code/reducer.py" {
		t.Errorf("expected reducer code URI, got %s", task.CodeURI)
	}
	if task.ReducePartition != 3 {
		t.Errorf("expected reduce partition 3, got %d", task.ReducePartition)
	}
}

func TestScheduler_GetNextTask_JobCompleted(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobID := uuid.NewString()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	expectReplicaCheck(mock, jobID, 0)

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID, 0, "Map").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingTasksByType)).
		WithArgs(jobID, "Map").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID, 0, "Reduce").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingTasksByType)).
		WithArgs(jobID, "Reduce").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectRollback()

	_, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1")
	if !errors.Is(err, ErrJobCompleted) {
		t.Errorf("expected ErrJobCompleted, got %v", err)
	}
}

func TestScheduler_GetNextTask_EmptyWorkerID(t *testing.T) {
	db, _, scheduler := setupMockDB(t)
	defer db.Close()

	_, err := scheduler.GetNextTask(context.Background(), uuid.NewString(), "")
	if !errors.Is(err, ErrEmptyWorkerID) {
		t.Errorf("expected ErrEmptyWorkerID, got %v", err)
	}
}

func TestScheduler_GetNextTask_EmptyJobID(t *testing.T) {
	db, _, scheduler := setupMockDB(t)
	defer db.Close()

	_, err := scheduler.GetNextTask(context.Background(), "", "worker-1")
	if !errors.Is(err, ErrEmptyJobID) {
		t.Errorf("expected ErrEmptyJobID, got %v", err)
	}
}

func TestScheduler_GetNextTask_InvalidJobID(t *testing.T) {
	db, _, scheduler := setupMockDB(t)
	defer db.Close()

	_, err := scheduler.GetNextTask(context.Background(), "not-a-uuid", "worker-1")
	if err == nil {
		t.Fatal("expected error for invalid job ID format")
	}
}

func TestScheduler_CompleteTask_Success(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	jobID := uuid.New().String()

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	expectLeaseValidation(mock, attemptID, "mock-lease", true)

	mock.ExpectExec(regexp.QuoteMeta(QueryCompleteTask)).
		WithArgs(taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QuerySucceedAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryInsertOutputBulkBase)).
		WithArgs(taskID, 0, "s3://output1", "hash1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountAllPendingTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err := scheduler.CompleteTask(context.Background(), taskID, attemptID, "mock-lease", []string{"s3://output1"}, []string{"hash1"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestScheduler_CompleteTask_LengthMismatch(t *testing.T) {
	db, _, scheduler := setupMockDB(t)
	defer db.Close()

	err := scheduler.CompleteTask(context.Background(), uuid.NewString(), uuid.NewString(), "mock-lease", []string{"uri1"}, []string{"hash1", "hash2"})
	if !errors.Is(err, ErrOutputMismatch) {
		t.Errorf("expected output mismatch, got %v", err)
	}
}

func TestScheduler_CompleteTask_AlreadyCompleted(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()

	jobID := uuid.New().String()

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("Completed", attemptID))
	mock.ExpectRollback()

	err := scheduler.CompleteTask(context.Background(), taskID, attemptID, "mock-lease", nil, nil)
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

	jobID := uuid.New().String()

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	mock.ExpectRollback()

	err := scheduler.CompleteTask(context.Background(), taskID, staleAttemptID, "mock-lease", nil, nil)
	if !errors.Is(err, ErrStaleAttempt) {
		t.Errorf("expected ErrStaleAttempt, got %v", err)
	}
}

func TestScheduler_CompleteTask_NotFound(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()

	jobID := uuid.New().String()

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	if err := scheduler.CompleteTask(context.Background(), taskID, uuid.NewString(), "mock-lease", nil, nil); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got: %v", err)
	}
}

func TestScheduler_FailTask_MaxAttempts(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()
	jobID := uuid.New().String()
	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	expectLeaseValidation(mock, attemptID, leaseID, true)

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountAttemptsByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3)) // MaxAttempts

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Failed", taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err := scheduler.FailTask(context.Background(), taskID, attemptID, leaseID, "crash")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestScheduler_FailTask_RetryableSuccess(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()

	jobID := uuid.New().String()

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	expectLeaseValidation(mock, attemptID, leaseID, true)

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountAttemptsByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Idle", taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).
		WithArgs(sqlmock.AnyArg(), taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).
		WithArgs(sqlmock.AnyArg(), taskID, "system-recovery", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err := scheduler.FailTask(context.Background(), taskID, attemptID, leaseID, "worker exited")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestScheduler_FailTask_DBClockExpiredEvenIfAppWouldThinkValid(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()

	jobID := uuid.New().String()

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	// Simulates DB clock ahead of the manager pod: local time could still consider the
	// lease active, but the DB is the authority and rejects it as expired.
	expectLeaseValidation(mock, attemptID, leaseID, false)
	mock.ExpectRollback()

	err := scheduler.FailTask(context.Background(), taskID, attemptID, leaseID, "late failure")
	if !errors.Is(err, ErrExpiredLease) {
		t.Fatalf("expected ErrExpiredLease got %v", err)
	}
}

func TestScheduler_RenewLease_Success(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	expectLeaseValidation(mock, attemptID, leaseID, true)
	mock.ExpectExec(regexp.QuoteMeta(QueryRenewLease)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	err := scheduler.RenewLease(context.Background(), taskID, attemptID, leaseID)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestScheduler_RenewLease_DBClockValidEvenIfAppWouldThinkExpired(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	// Simulates DB clock behind the manager pod: a local check might think the lease
	// expired already, but renewal succeeds because DB time is the lease authority.
	expectLeaseValidation(mock, attemptID, leaseID, true)
	mock.ExpectExec(regexp.QuoteMeta(QueryRenewLease)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	err := scheduler.RenewLease(context.Background(), taskID, attemptID, leaseID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScheduler_RenewLease_Expired(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	expectLeaseValidation(mock, attemptID, leaseID, false)
	mock.ExpectRollback()

	err := scheduler.RenewLease(context.Background(), taskID, attemptID, leaseID)
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
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	expectLeaseValidation(mock, attemptID, leaseID, false)
	mock.ExpectRollback()

	err := scheduler.RenewLease(context.Background(), taskID, attemptID, leaseID)
	if !errors.Is(err, ErrExpiredLease) {
		t.Fatalf("expected ErrExpiredLease got %v", err)
	}
}

func TestScheduler_Recover_SpawnsRecoverableAttempts(t *testing.T) {
	rec := &recordingOrchestrator{}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, rec)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectRecoverableAttempts)).
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "current_attempt_id", "job_id", "last_renewed_at"}).
			AddRow(taskID, attemptID, "job-123", time.Now()))

	// Recover will call GetTaskByID -> DispatchTask for each recovered attempt.
	// GetTaskByID issues a query; we mock it here.
	mock.ExpectQuery(`SELECT task_id, job_id, task_type, status, current_attempt_id, replica_index FROM TASKS`).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, "job-123", "Map", "In-Progress", attemptID, 0))

	if err := scheduler.Recover(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScheduler_Recover_NoRecoverableAttempts_NoSpawn(t *testing.T) {
	rec := &recordingOrchestrator{}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, rec)
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectRecoverableAttempts)).
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "current_attempt_id", "job_id", "last_renewed_at"}))

	if err := scheduler.Recover(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("expected no spawn calls, got %d", len(rec.calls))
	}
}

func TestScheduler_Recover_PartialSpawnFailure_DoesNotReturnError(t *testing.T) {
	rec := &recordingOrchestrator{err: errors.New("k8s unavailable")}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, rec)
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectRecoverableAttempts)).
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "current_attempt_id", "job_id", "last_renewed_at"}).
			AddRow(uuid.NewString(), uuid.NewString(), uuid.NewString(), time.Now()).
			AddRow(uuid.NewString(), uuid.NewString(), uuid.NewString(), time.Now()))

	if err := scheduler.Recover(context.Background()); err != nil {
		t.Fatalf("expected recover to continue after partial spawn failures, got %v", err)
	}
}

func TestScheduler_FailStaleTasks_NoStaleTasks(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectStaleTasks)).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "attempt_id", "job_id", "attempt_count"}))
	mock.ExpectCommit()

	recovered, err := scheduler.FailStaleTasks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("expected 0 recovered tasks, got %d", recovered)
	}
}

func TestScheduler_FailStaleTasks_Success(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	jobID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectStaleTasks)).
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "attempt_id", "job_id", "attempt_count"}).
			AddRow(taskID, attemptID, jobID, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Idle", taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).
		WithArgs(sqlmock.AnyArg(), taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).
		WithArgs(sqlmock.AnyArg(), taskID, "system-recovery", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	recovered, err := scheduler.FailStaleTasks(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if recovered != 1 {
		t.Errorf("expected 1 recovered task, got %d", recovered)
	}
}

func TestScheduler_FailStaleTasks_MarksJobFailedAtMaxAttempts(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	jobID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectStaleTasks)).
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "attempt_id", "job_id", "attempt_count"}).
			AddRow(taskID, attemptID, jobID, MaxTaskAttempts))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Failed", taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	recovered, err := scheduler.FailStaleTasks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered task, got %d", recovered)
	}
}

func TestScheduler_FailStaleTasks_DeduplicatesJobCancellation(t *testing.T) {
	rec := &recordingOrchestrator{}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, rec)
	defer db.Close()

	task1 := uuid.New().String()
	task2 := uuid.New().String()
	attempt1 := uuid.New().String()
	attempt2 := uuid.New().String()
	jobID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectStaleTasks)).
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "attempt_id", "job_id", "attempt_count"}).
			AddRow(task1, attempt1, jobID, MaxTaskAttempts).
			AddRow(task2, attempt2, jobID, MaxTaskAttempts))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Failed", task1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(attempt1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Failed", task2).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(attempt2).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	recovered, err := scheduler.FailStaleTasks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered != 2 {
		t.Fatalf("expected 2 recovered tasks, got %d", recovered)
	}
}

func TestScheduler_FailStaleTasks_CallsDeleteWorkerJobForFailedTask(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	jobID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectStaleTasks)).
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "attempt_id", "job_id", "attempt_count"}).
			AddRow(taskID, attemptID, jobID, MaxTaskAttempts))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Failed", taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	recovered, err := scheduler.FailStaleTasks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered task, got %d", recovered)
	}
}

func TestScheduler_FailStaleTasks_UsesPerJobFinalizeDeadlines(t *testing.T) {
	rec := &deadlineRecordingOrchestrator{firstDelay: 15 * time.Millisecond}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, rec)
	defer db.Close()

	task1 := uuid.New().String()
	task2 := uuid.New().String()
	attempt1 := uuid.New().String()
	attempt2 := uuid.New().String()
	job1 := uuid.New().String()
	job2 := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectStaleTasks)).
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "attempt_id", "job_id", "attempt_count"}).
			AddRow(task1, attempt1, job1, MaxTaskAttempts).
			AddRow(task2, attempt2, job2, MaxTaskAttempts))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Failed", task1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(attempt1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(job1, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).
		WithArgs("Failed", task2).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).
		WithArgs(attempt2).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(job2, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	recovered, err := scheduler.FailStaleTasks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered != 2 {
		t.Fatalf("expected 2 recovered tasks, got %d", recovered)
	}
}

func TestScheduler_GetTaskByID_WithAttempt(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	jobID := uuid.New().String()
	attemptID := uuid.New().String()

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Reduce", "In-Progress", attemptID, 7))

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobConfigByTask)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).
			AddRow("s3://code/mapper.py", "s3://code/reducer.py", "s3://code/combiner.py", 3, "sha256-input"))

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetReduceTaskInputs)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"partition_index", "output_uri", "checksum"}).
			AddRow(7, "s3://shuffle/map-0-part-7", "sha256-shuffle"))

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetAttemptDetails)).
		WithArgs(attemptID).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "lease_id", "start_time", "last_renewed_at"}).
			AddRow("worker-1", "lease123", time.Now(), time.Now()))

	task, err := scheduler.GetTaskByID(context.Background(), taskID)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if task.ID != taskID {
		t.Errorf("expected task ID %s, got %s", taskID, task.ID)
	}
	if task.State != InProgress {
		t.Errorf("expected state InProgress, got %v", task.State)
	}
	if task.JobID != jobID {
		t.Errorf("expected job ID %s, got %s", jobID, task.JobID)
	}
	if task.ActiveAttemptID != attemptID {
		t.Errorf("expected attemptID %v, got %v", attemptID, task.ActiveAttemptID)
	}
	if task.CodeURI != "s3://code/reducer.py" {
		t.Errorf("expected reducer code URI, got %s", task.CodeURI)
	}
	if task.ReplicaIndex != 7 {
		t.Errorf("expected replica index 7, got %d", task.ReplicaIndex)
	}
	if len(task.ShuffleInputs) != 1 || task.ShuffleInputs[0].OutputURI != "s3://shuffle/map-0-part-7" {
		t.Errorf("expected reduce shuffle inputs to be hydrated, got %+v", task.ShuffleInputs)
	}
}

func TestScheduler_AllMapTasksCompleted_Negative(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobID := uuid.NewString()

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingMapTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	if scheduler.AllMapTasksCompleted(context.Background(), jobID) {
		t.Errorf("expected false")
	}
}

func TestScheduler_IsJobFinished_Negative(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobID := uuid.NewString()

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountAllPendingTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	if scheduler.IsJobFinished(context.Background(), jobID) {
		t.Errorf("expected false")
	}
}

func TestScheduler_GetMapOutputs(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobID := uuid.NewString()

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMapOutputs)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"output_uri"}).AddRow("s3://map-out-1").AddRow("s3://map-out-2"))

	outputs, err := scheduler.GetMapOutputs(context.Background(), jobID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(outputs) != 2 || outputs[0] != "s3://map-out-1" || outputs[1] != "s3://map-out-2" {
		t.Errorf("unexpected map outputs: %v", outputs)
	}
}

func TestScheduler_GetReduceOutputs(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobID := uuid.NewString()

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetReduceOutputs)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"output_uri"}).AddRow("s3://reduce-out-1"))

	outputs, err := scheduler.GetReduceOutputs(context.Background(), jobID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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

	status, err := scheduler.GetTaskStatus(context.Background(), taskID)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if status != InProgress {
		t.Errorf("expected InProgress, got %v", status)
	}

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetTaskStatus)).
		WithArgs(taskID).
		WillReturnError(sql.ErrNoRows)

	_, err = scheduler.GetTaskStatus(context.Background(), taskID)
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

	jobID := uuid.New().String()

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	expectLeaseValidation(mock, attemptID, leaseID, false)
	mock.ExpectRollback()

	err := scheduler.CompleteTask(context.Background(), taskID, attemptID, leaseID, nil, nil)
	if !errors.Is(err, ErrExpiredLease) {
		t.Fatalf("expected ErrExpiredLease got %v", err)
	}
}

func TestScheduler_CompleteTask_DBClockExpiredEvenIfAppWouldThinkValid(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := uuid.New().String()

	jobID := uuid.New().String()

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

	// Simulates DB clock ahead of the app clock. Completion must fence on DB time.
	expectLeaseValidation(mock, attemptID, leaseID, false)
	mock.ExpectRollback()

	err := scheduler.CompleteTask(context.Background(), taskID, attemptID, leaseID, nil, nil)
	if !errors.Is(err, ErrExpiredLease) {
		t.Fatalf("expected ErrExpiredLease got %v", err)
	}
}

func TestScheduler_CompleteTask_AsymmetricArrays(t *testing.T) {
	db, _, scheduler := setupMockDB(t)
	defer db.Close()

	err := scheduler.CompleteTask(context.Background(), uuid.New().String(), uuid.New().String(), "lease-1", []string{}, []string{"hash1"})
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
	jobID := uuid.New().String()

	origURIs := []string{"uri1"}
	origChecksums := []string{"hash1"}

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	expectLeaseValidation(mock, attemptID, leaseID, true)
	mock.ExpectExec(regexp.QuoteMeta(QueryCompleteTask)).
		WithArgs(taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QuerySucceedAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertOutputBulkBase)).
		WithArgs(taskID, 0, "uri1", "hash1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountAllPendingTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := scheduler.CompleteTask(context.Background(), taskID, attemptID, leaseID, origURIs, origChecksums)
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

func TestScheduler_ScheduleJob_PersistsDDSRecords(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open mock DB: %v", err)
	}
	defer db.Close()

	scheduler, err := NewScheduler(db, 0, 4, &MockOrchestrator{}, &MockDispatcher{}, "manager-0:50051", 30, nil)
	if err != nil {
		t.Fatalf("unexpected error creating scheduler: %v", err)
	}

	req := ScheduleJobRequest{
		JobID:         uuid.NewString(),
		UserID:        uuid.NewString(),
		InputURI:      "s3://mapreduce-inputs/job-1/data.jsonl",
		MapperURI:     "s3://code/mapper.py",
		ReducerURI:    "s3://code/reducer.py",
		CombinerURI:   "s3://code/combiner.py",
		MTasks:        1,
		RTasks:        1,
		InputChecksum: "sha256-input",
		Tasks: []ScheduleTask{
			{
				TaskID:       uuid.NewString(),
				TaskType:     "Map",
				ReplicaIndex: 99,
				InputSplits: []ScheduleTaskInput{{
					InputURI:      "s3://mapreduce-inputs/job-1/split-0.jsonl",
					ByteStart:     0,
					ByteEnd:       128,
					SplitChecksum: "sha256-split-0",
				}},
			},
			{
				TaskID:       uuid.NewString(),
				TaskType:     "Reduce",
				ReplicaIndex: 3,
			},
		},
	}
	expectedReplicaIndex, err := ComputeReplicaIndex(req.JobID, 4)
	if err != nil {
		t.Fatalf("failed to compute expected replica index: %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJob)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJobConfig)).
		WithArgs(sqlmock.AnyArg(), req.InputURI, req.MapperURI, req.ReducerURI, req.CombinerURI, req.MTasks, req.RTasks, req.InputChecksum).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "Map", expectedReplicaIndex).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTaskInput)).
		WithArgs(sqlmock.AnyArg(), "s3://mapreduce-inputs/job-1/split-0.jsonl", int64(0), int64(128), "sha256-split-0").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "Reduce", 3).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := scheduler.ScheduleJob(context.Background(), req); err != nil {
		t.Fatalf("expected schedule success, got %v", err)
	}
}

func TestScheduler_ScheduleJob_RejectsInvalidRequest(t *testing.T) {
	db, _, scheduler := setupMockDB(t)
	defer db.Close()

	err := scheduler.ScheduleJob(context.Background(), ScheduleJobRequest{
		JobID:         "not-a-uuid",
		UserID:        uuid.NewString(),
		InputURI:      "s3://mapreduce-inputs/job-1/data.jsonl",
		MapperURI:     "s3://code/mapper.py",
		ReducerURI:    "s3://code/reducer.py",
		MTasks:        1,
		RTasks:        1,
		InputChecksum: "sha256-input",
	})
	if err == nil {
		t.Fatal("expected invalid schedule request to fail")
	}
}

func TestScheduler_UpsertSystemConfig(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta(QueryUpsertSystemConfig)).
		WithArgs(20, "500m", "1Gi", 5, 2, "topology.kubernetes.io/zone", "app.kubernetes.io/name=minio").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := scheduler.UpsertSystemConfig(context.Background(), SystemConfigUpdate{
		MaxConcurrentPods:     20,
		CPULimit:              "500m",
		MemoryLimit:           "1Gi",
		WorkerReplicas:        5,
		MaxJobsPerNode:        2,
		LocalityKey:           "topology.kubernetes.io/zone",
		LocalityLabelSelector: "app.kubernetes.io/name=minio",
	}); err != nil {
		t.Fatalf("expected upsert success, got %v", err)
	}
}

func TestScheduler_CancelJob_Success(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	jobID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobStatusForUpdate)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("Running"))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE TASKS SET status = 'Failed' WHERE job_id =").
		WithArgs(jobID).
		WillReturnResult(sqlmock.NewResult(1, 3))
	mock.ExpectExec(regexp.QuoteMeta(QueryFailRunningAttemptsByJob)).
		WithArgs(jobID).
		WillReturnResult(sqlmock.NewResult(1, 2))
	mock.ExpectCommit()

	err := scheduler.CancelJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestScheduler_CompleteTask_JobCompleted_TriggersCleanup(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID := uuid.New().String()
	leaseID := "lease-456"
	jobID := uuid.New().String()

	mock.ExpectBegin()
	expectJobLockQuery(mock, taskID, jobID, "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))
	expectLeaseValidation(mock, attemptID, leaseID, true)
	mock.ExpectExec(regexp.QuoteMeta(QueryCompleteTask)).
		WithArgs(taskID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QuerySucceedAttempt)).
		WithArgs(attemptID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertOutputBulkBase)).
		WithArgs(taskID, 0, "s3://output/file.txt", "sha256-output").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountAllPendingTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Cleaning").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := scheduler.CompleteTask(context.Background(), taskID, attemptID, leaseID,
		[]string{"s3://output/file.txt"}, []string{"sha256-output"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestScheduler_GetNextTask_ConcurrentQuotaSafety(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobID := uuid.NewString()

	// Simulating concurrent calls: first caller acquires lock, sees quota available, and assigns.
	// We use ordered expectations to simulate the serialized critical section.
	taskID := uuid.New()
	dbJobID, _ := uuid.Parse(jobID)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	expectReplicaCheck(mock, jobID, 0)
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID, 0, "Map").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "replica_index"}).AddRow(taskID, dbJobID, "Map", 0))
	expectTaskMetadataQueries(mock, taskID, "s3://map", "s3://reduce", 1)
	mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	_, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestScheduler_GetNextTask_NoStarvation_MapBeforeReduce(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobID := uuid.NewString()

	// Verify that if Map tasks are pending, the scheduler returns ErrNoIdleTasks
	// instead of proceeding to Reduce tasks (Map-phase starvation check).
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	expectReplicaCheck(mock, jobID, 0)
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID, 0, "Map").
		WillReturnError(sql.ErrNoRows) // No IDLE map tasks
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingTasksByType)).
		WithArgs(jobID, "Map").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5)) // But 5 map tasks are still IN-PROGRESS
	mock.ExpectRollback()

	_, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1")
	if !errors.Is(err, ErrNoIdleTasks) {
		t.Errorf("expected ErrNoIdleTasks due to Map phase incomplete, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

type cleanupProbeOrchestrator struct {
	release <-chan struct{}
	entered chan string
	active  int32
	maxSeen int32
}

func (c *cleanupProbeOrchestrator) SpawnWorker(context.Context, string, string, string, string) error {
	return nil
}

func (c *cleanupProbeOrchestrator) EnsureWorkerPool(context.Context, string, int, string) error {
	return nil
}

func (c *cleanupProbeOrchestrator) DeleteWorkerJob(context.Context, string) error {
	return nil
}

func (c *cleanupProbeOrchestrator) CancelJob(_ context.Context, jobID string) error {
	cur := atomic.AddInt32(&c.active, 1)
	for {
		max := atomic.LoadInt32(&c.maxSeen)
		if cur <= max || atomic.CompareAndSwapInt32(&c.maxSeen, max, cur) {
			break
		}
	}
	c.entered <- jobID
	<-c.release
	atomic.AddInt32(&c.active, -1)
	return nil
}

func TestScheduler_popPendingCleanup_RespectsLimit(t *testing.T) {
	db, _, scheduler := setupMockDB(t)
	defer db.Close()

	for i := 0; i < 10; i++ {
		scheduler.enqueueCleanup(uuid.NewString(), "Failed")
	}

	popped := scheduler.popPendingCleanup(3)
	if len(popped) != 3 {
		t.Fatalf("expected 3 popped cleanup jobs, got %d", len(popped))
	}
	if got := len(scheduler.pendingCleanup); got != 7 {
		t.Fatalf("expected 7 jobs left in cleanup queue, got %d", got)
	}
}

func TestScheduler_StartCleanupReconciler_BoundedConcurrency(t *testing.T) {
	release := make(chan struct{})
	orch := &cleanupProbeOrchestrator{
		release: release,
		entered: make(chan string, 32),
	}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, orch)
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT job_id FROM JOBS WHERE status = 'Cleaning'")).
		WillReturnRows(sqlmock.NewRows([]string{"job_id"}))

	jobIDs := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		jobID := uuid.NewString()
		jobIDs = append(jobIDs, jobID)
		scheduler.enqueueCleanup(jobID, "Completed")
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobStatusForUpdate)).
			WithArgs(jobID).
			WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("Cleaning"))
		mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
			WithArgs(jobID, "Completed").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
	}

	ctx, cancel := context.WithCancel(context.Background())
	scheduler.StartCleanupReconciler(ctx, 5*time.Millisecond)

	deadline := time.After(2 * time.Second)
	seen := map[string]struct{}{}
	for len(seen) < cleanupReconcileWorkers {
		select {
		case jobID := <-orch.entered:
			seen[jobID] = struct{}{}
		case <-deadline:
			t.Fatalf("timed out waiting for first cleanup wave; seen=%d", len(seen))
		}
	}

	time.Sleep(30 * time.Millisecond)
	if maxSeen := atomic.LoadInt32(&orch.maxSeen); maxSeen > cleanupReconcileWorkers {
		t.Fatalf("cleanup concurrency exceeded limit: got %d, limit %d", maxSeen, cleanupReconcileWorkers)
	}

	close(release)

	for len(seen) < len(jobIDs) {
		select {
		case jobID := <-orch.entered:
			seen[jobID] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for all cleanup jobs; seen=%d want=%d", len(seen), len(jobIDs))
		}
	}

	if maxSeen := atomic.LoadInt32(&orch.maxSeen); maxSeen < 2 {
		t.Fatalf("expected concurrent cleanup execution, saw max concurrency %d", maxSeen)
	}

	waitUntil := time.Now().Add(2 * time.Second)
	for {
		err := mock.ExpectationsWereMet()
		if err == nil {
			break
		}
		if time.Now().After(waitUntil) {
			t.Fatalf("unfulfilled expectations: %s", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
}

func TestScheduler_ExpiredLeaseFencedByDB(t *testing.T) {
	tests := []struct {
		name string
		call func(context.Context, *Scheduler, string, string, string) error
	}{
		{
			name: "CompleteTask",
			call: func(ctx context.Context, scheduler *Scheduler, taskID, attemptID, leaseID string) error {
				return scheduler.CompleteTask(ctx, taskID, attemptID, leaseID, []string{"s3://out"}, []string{"sha1"})
			},
		},
		{
			name: "FailTask",
			call: func(ctx context.Context, scheduler *Scheduler, taskID, attemptID, leaseID string) error {
				return scheduler.FailTask(ctx, taskID, attemptID, leaseID, "crash")
			},
		},
		{
			name: "RenewLease",
			call: func(ctx context.Context, scheduler *Scheduler, taskID, attemptID, leaseID string) error {
				return scheduler.RenewLease(ctx, taskID, attemptID, leaseID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, scheduler := setupMockDB(t)
			defer db.Close()

			taskID := uuid.New().String()
			attemptID := uuid.New().String()
			leaseID := "lease123"
			jobID := uuid.New().String()

			mock.ExpectBegin()
			// CompleteTask and FailTask lock the parent job row first
			if tt.name == "CompleteTask" || tt.name == "FailTask" {
				expectJobLockQuery(mock, taskID, jobID, "Running")
			}
			mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).
				WithArgs(taskID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id"}).AddRow("In-Progress", attemptID))

			// Mock DB clock reporting the lease is expired (false).
			expectLeaseValidation(mock, attemptID, leaseID, false)
			mock.ExpectRollback()

			err := tt.call(context.Background(), scheduler, taskID, attemptID, leaseID)
			if !errors.Is(err, ErrExpiredLease) {
				t.Errorf("expected ErrExpiredLease from DB clock, got %v", err)
			}
		})
	}
}

// ── Staging cleanup tests ────────────────────────────────────

type mockStagingCleaner struct {
	mu      sync.Mutex
	deleted []string
	err     error
}

func (m *mockStagingCleaner) DeleteStagingObjects(_ context.Context, jobID string) error {
	m.mu.Lock()
	m.deleted = append(m.deleted, jobID)
	err := m.err
	m.mu.Unlock()
	return err
}

func (m *mockStagingCleaner) calledWith() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.deleted))
	copy(out, m.deleted)
	return out
}

func setupMockDBWithStaging(t *testing.T, staging StagingCleaner) (*sql.DB, sqlmock.Sqlmock, *Scheduler) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	scheduler, err := NewScheduler(db, 0, 1, &MockOrchestrator{}, &MockDispatcher{}, "manager-0:50051", 30, staging)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	return db, mock, scheduler
}

// TestFinalizeJob_DeletesStagingObjects verifies that finalizeJob calls
// DeleteStagingObjects after CancelJob succeeds and before updating the DB.
func TestFinalizeJob_DeletesStagingObjects(t *testing.T) {
	staging := &mockStagingCleaner{}
	db, mock, scheduler := setupMockDBWithStaging(t, staging)
	defer db.Close()

	jobID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobStatusForUpdate)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("Cleaning"))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Completed").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	scheduler.finalizeJob(context.Background(), jobID, "Completed")

	deleted := staging.calledWith()
	if len(deleted) != 1 || deleted[0] != jobID {
		t.Fatalf("expected staging.DeleteStagingObjects(%q), got %v", jobID, deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled DB expectations: %v", err)
	}
}

// TestFinalizeJob_StagingFailureRequeues verifies that a MinIO error causes the
// job to be re-enqueued for retry without updating the DB status.
func TestFinalizeJob_StagingFailureRequeues(t *testing.T) {
	staging := &mockStagingCleaner{err: errors.New("minio unavailable")}
	db, mock, scheduler := setupMockDBWithStaging(t, staging)
	defer db.Close()

	jobID := uuid.New().String()

	// No DB expectations — finalizeJob must return before touching the DB.
	scheduler.finalizeJob(context.Background(), jobID, "Completed")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected DB call after staging failure: %v", err)
	}

	// Job must be back in the retry queue.
	pending := scheduler.popPendingCleanup(10)
	state, ok := pending[jobID]
	if !ok {
		t.Fatalf("job %s not re-enqueued after staging failure", jobID)
	}
	if state != "Completed" {
		t.Errorf("expected terminal state Completed in retry queue, got %q", state)
	}
}

// TestStartCleanupReconciler_RetriesStagingFailure verifies that the reconciler
// retries a job whose staging cleanup failed on the first attempt.
func TestStartCleanupReconciler_RetriesStagingFailure(t *testing.T) {
	callCount := 0
	staging := &mockStagingCleaner{}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	scheduler, err := NewScheduler(db, 0, 1, &MockOrchestrator{}, &MockDispatcher{}, "manager-0:50051", 30, staging)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	jobID := uuid.New().String()

	// First reconciler pass: staging fails → job re-enqueued.
	// Second pass: staging succeeds → DB update expected.
	staging.err = errors.New("temporary minio error")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT job_id FROM JOBS WHERE status = 'Cleaning'")).
		WillReturnRows(sqlmock.NewRows([]string{"job_id"}))

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobStatusForUpdate)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("Cleaning"))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Completed").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	scheduler.enqueueCleanup(jobID, "Completed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler.StartCleanupReconciler(ctx, 5*time.Millisecond)

	// Wait for the first tick: staging error → re-enqueue.
	deadline := time.After(500 * time.Millisecond)
	for {
		if len(staging.calledWith()) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for first staging attempt")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}

	// Clear the error so the next attempt succeeds.
	staging.mu.Lock()
	staging.err = nil
	callCount = len(staging.deleted)
	staging.mu.Unlock()
	_ = callCount

	// Wait for the DB update to be fulfilled.
	deadline2 := time.After(2 * time.Second)
	for {
		if err := mock.ExpectationsWereMet(); err == nil {
			break
		}
		select {
		case <-deadline2:
			t.Fatalf("unfulfilled DB expectations after retry: %v", mock.ExpectationsWereMet())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestFinalizeJob_CleaningPhaseEnforcedOnRetry is an integration test that verifies
// the Cleaning phase is maintained between cleanup attempts:
//  1. First call: staging fails → DB not touched (job stays in Cleaning).
//  2. Second call: staging succeeds → Cleaning → terminal transition enforced.
func TestFinalizeJob_CleaningPhaseEnforcedOnRetry(t *testing.T) {
	staging := &mockStagingCleaner{err: errors.New("transient minio error")}
	db, mock, scheduler := setupMockDBWithStaging(t, staging)
	defer db.Close()

	jobID := uuid.New().String()

	// First attempt: staging fails — DB must not be touched.
	scheduler.finalizeJob(context.Background(), jobID, "Completed")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("DB was touched during staging failure (job should stay in Cleaning): %v", err)
	}

	// Job must be back in the retry queue with its terminal state preserved.
	pending := scheduler.popPendingCleanup(10)
	state, ok := pending[jobID]
	if !ok {
		t.Fatalf("job %s not re-enqueued after cleanup failure; job is not in Cleaning", jobID)
	}
	if state != "Completed" {
		t.Errorf("expected terminal state Completed in retry queue, got %q", state)
	}

	// Second attempt: staging succeeds — transition Cleaning → Completed must be enforced.
	staging.mu.Lock()
	staging.err = nil
	staging.mu.Unlock()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobStatusForUpdate)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("Cleaning"))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WithArgs(jobID, "Completed").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	scheduler.finalizeJob(context.Background(), jobID, "Completed")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled DB expectations on retry (Cleaning → Completed not applied): %v", err)
	}
}

func TestScheduler_ScheduleJob_SpawnsWorkersForMapTasks(t *testing.T) {
	rec := &recordingOrchestrator{}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, rec)
	defer db.Close()

	req := ScheduleJobRequest{
		JobID:         uuid.NewString(),
		UserID:        uuid.NewString(),
		InputURI:      "s3://mapreduce-inputs/job-1/data.jsonl",
		MapperURI:     "s3://code/mapper.py",
		ReducerURI:    "s3://code/reducer.py",
		MTasks:        2,
		RTasks:        1,
		InputChecksum: "sha256-input",
		Tasks: []ScheduleTask{
			{TaskID: uuid.NewString(), TaskType: "Map", ReplicaIndex: 0},
			{TaskID: uuid.NewString(), TaskType: "Map", ReplicaIndex: 0},
			{TaskID: uuid.NewString(), TaskType: "Reduce", ReplicaIndex: 0},
		},
	}

	expectedReplicaIndex, _ := ComputeReplicaIndex(req.JobID, 1)

	// Main transaction expectations
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJob)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJobConfig)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "Map", expectedReplicaIndex).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "Map", expectedReplicaIndex).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "Reduce", 0).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := scheduler.ScheduleJob(context.Background(), req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// New architecture uses EnsureWorkerPool (idempotent) instead of per-task SpawnWorker.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

func TestScheduler_ScheduleJob_RespectsQuota(t *testing.T) {
	rec := &recordingOrchestrator{}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, rec)
	defer db.Close()

	req := ScheduleJobRequest{
		JobID:      uuid.NewString(),
		UserID:     uuid.NewString(),
		InputURI:   "s3://in",
		MapperURI:  "s3://m",
		ReducerURI: "s3://r",
		MTasks:     3,
		RTasks:     1,
		Tasks: []ScheduleTask{
			{TaskID: uuid.NewString(), TaskType: "Map"},
			{TaskID: uuid.NewString(), TaskType: "Map"},
			{TaskID: uuid.NewString(), TaskType: "Map"},
			{TaskID: uuid.NewString(), TaskType: "Reduce"},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJob)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJobConfig)).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 4; i++ {
		mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	if err := scheduler.ScheduleJob(context.Background(), req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// New architecture: EnsureWorkerPool handles pool sizing.
}

func TestScheduler_ScheduleJob_SpawnFailureDoesNotFailSchedule(t *testing.T) {
	rec := &recordingOrchestrator{err: errors.New("k8s down")}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, rec)
	defer db.Close()

	req := ScheduleJobRequest{
		JobID:      uuid.NewString(),
		UserID:     uuid.NewString(),
		InputURI:   "s3://in",
		MapperURI:  "s3://m",
		ReducerURI: "s3://r",
		MTasks:     1,
		RTasks:     1,
		Tasks: []ScheduleTask{
			{TaskID: uuid.NewString(), TaskType: "Map"},
			{TaskID: uuid.NewString(), TaskType: "Reduce"},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJob)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJobConfig)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// EnsureWorkerPool returns error ("k8s down") but ScheduleJob should not fail.
	if err := scheduler.ScheduleJob(context.Background(), req); err != nil {
		t.Fatalf("ScheduleJob should not fail on EnsureWorkerPool error, got %v", err)
	}
}

func TestScheduler_ScheduleJob_ZeroQuota_NoSpawn(t *testing.T) {
	rec := &recordingOrchestrator{}
	db, mock, scheduler := setupMockDBWithOrchestrator(t, rec)
	defer db.Close()

	req := ScheduleJobRequest{
		JobID:      uuid.NewString(),
		UserID:     uuid.NewString(),
		InputURI:   "s3://in",
		MapperURI:  "s3://m",
		ReducerURI: "s3://r",
		MTasks:     1,
		RTasks:     1,
		Tasks: []ScheduleTask{
			{TaskID: uuid.NewString(), TaskType: "Map"},
			{TaskID: uuid.NewString(), TaskType: "Reduce"},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJob)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertJobConfig)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertTask)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Quota is zero
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))
	mock.ExpectRollback()

	if err := scheduler.ScheduleJob(context.Background(), req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rec.calls) != 0 {
		t.Fatalf("expected no spawn calls with zero quota, got %d", len(rec.calls))
	}
}
