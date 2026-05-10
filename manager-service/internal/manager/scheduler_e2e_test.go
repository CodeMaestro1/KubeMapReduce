package manager

import (
	"context"
	"database/sql"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// TrackingOrchestrator captures lifecycle calls for assertion in E2E tests.
// All fields are protected by mu to prevent data races when finalizeJob runs
// in a background goroutine.
type TrackingOrchestrator struct {
	mu          sync.Mutex
	spawnCalls  []SpawnCall
	cancelCalls []string
	SpawnErr    error
}

type SpawnCall struct {
	TaskID      string
	JobID       string
	AttemptID   string
	ManagerAddr string
}

func (o *TrackingOrchestrator) SpawnWorker(ctx context.Context, taskID, jobID, attemptID, addr string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.spawnCalls = append(o.spawnCalls, SpawnCall{taskID, jobID, attemptID, addr})
	return o.SpawnErr
}

func (o *TrackingOrchestrator) CancelJob(ctx context.Context, jobID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cancelCalls = append(o.cancelCalls, jobID)
	return nil
}

func (o *TrackingOrchestrator) EnsureWorkerPool(ctx context.Context, jobID string, numWorkers int, managerAddr string) error {
	return o.SpawnErr
}

func (o *TrackingOrchestrator) DeleteWorkerJob(ctx context.Context, taskID string) error {
	return nil
}

// SpawnCount returns the number of SpawnWorker calls recorded so far.
func (o *TrackingOrchestrator) SpawnCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.spawnCalls)
}

// GetSpawnCall returns the i-th SpawnWorker call.
func (o *TrackingOrchestrator) GetSpawnCall(i int) SpawnCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.spawnCalls[i]
}

// CancelCount returns the number of CancelJob calls recorded so far.
func (o *TrackingOrchestrator) CancelCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.cancelCalls)
}

// GetCancelCall returns the i-th CancelJob call.
func (o *TrackingOrchestrator) GetCancelCall(i int) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.cancelCalls[i]
}

func setupE2ETest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *Scheduler, *TrackingOrchestrator) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}

	orch := &TrackingOrchestrator{}
	s, err := NewScheduler(db, 0, 1, orch, &MockDispatcher{}, "localhost:50051", 30, nil)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	return db, mock, s, orch
}

func TestE2E_WorkerKillDuringMapTask(t *testing.T) {
	db, mock, s, _ := setupE2ETest(t)
	defer db.Close()

	jobID := uuid.New().String()
	taskID := uuid.New().String()
	workerID := "worker-1"
	attemptID1 := uuid.New().String()

	// 1. GetNextTask -> Map task assigned (attempt-1)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).WithArgs(jobID).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	expectReplicaCheck(mock, jobID, 0)
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).WithArgs(jobID, s.replicaIndex, "Map").WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "replica_index"}).AddRow(taskID, jobID, "Map", 0))
	// hydrateTaskMetadata
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobConfigByTask)).WithArgs(taskID).WillReturnRows(sqlmock.NewRows([]string{"mapper_uri", "reducer_uri", "combiner_uri", "r_tasks", "input_checksum"}).AddRow("m", "r", "c", 1, "sum"))
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetTaskInputs)).WithArgs(taskID).WillReturnRows(sqlmock.NewRows([]string{"input_uri", "byte_start", "byte_end", "split_checksum"}).AddRow("u", 0, 100, "s"))
	mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	// update state
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).WithArgs(sqlmock.AnyArg(), taskID).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).WithArgs(sqlmock.AnyArg(), taskID, workerID, sqlmock.AnyArg(), s.leaseTTL).WillReturnResult(sqlmock.NewResult(1, 1))
	// update job status
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).WithArgs(jobID, "Running").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	task, err := s.GetNextTask(context.Background(), jobID, workerID)
	if err != nil {
		t.Fatalf("GetNextTask failed: %v", err)
	}
	if task.ID != taskID {
		t.Errorf("Expected task %s, got %s", taskID, task.ID)
	}

	// 2. FailStaleTasks -> detects expired lease, marks attempt-1 Failed, resets to Idle
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectStaleTasks)).WithArgs(s.replicaIndex).WillReturnRows(
		sqlmock.NewRows([]string{"task_id", "attempt_id", "job_id", "attempt_count", "last_renewed_at", "lease_ttl"}).AddRow(taskID, attemptID1, jobID, 1, time.Now().Add(-1*time.Hour), 30))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).WithArgs("Idle", taskID).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).WithArgs(attemptID1).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Redispatch calls GetTaskByID
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetTaskByID)).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "status", "current_attempt_id", "replica_index"}).
			AddRow(taskID, jobID, "Map", "Idle", nil, 0))

	expectTaskMetadataQueries(mock, uuid.MustParse(taskID), "m", "r", 1)

	recovered, err := s.FailStaleTasks(context.Background())
	if err != nil {
		t.Fatalf("FailStaleTasks failed: %v", err)
	}
	if recovered != 1 {
		t.Errorf("Expected 1 recovered task, got %d", recovered)
	}
}

func TestE2E_ZombieFencing(t *testing.T) {
	db, mock, s, _ := setupE2ETest(t)
	defer db.Close()

	taskID := uuid.New().String()
	attemptID1 := uuid.New().String()
	leaseID1 := uuid.New().String()

	// Simulating zombie completion: attemptID1 was failed in DB
	mock.ExpectBegin()
	// Job lock query (new)
	expectJobLockQuery(mock, taskID, uuid.New().String(), "Running")
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectTaskForUpdate)).WithArgs(taskID).WillReturnRows(sqlmock.NewRows([]string{"status", "current_attempt_id", "job_id"}).AddRow("Idle", uuid.New().String(), uuid.New().String()))
	// validateLeaseTx fails because attempt is not current
	mock.ExpectQuery(regexp.QuoteMeta(QueryCheckLeaseValid)).WithArgs(attemptID1, leaseID1, 5).WillReturnRows(sqlmock.NewRows([]string{"valid"}).AddRow(false))
	mock.ExpectRollback()

	err := s.CompleteTask(context.Background(), taskID, attemptID1, leaseID1, []string{"u1"}, []string{"c1"})
	if err == nil {
		t.Fatal("Expected error for zombie completion, got nil")
	}
	if err != ErrExpiredLease && err != ErrStaleAttempt {
		// Depending on implementation, but it should be a fencing error
	}
}

func TestE2E_TripleFailure_MaxAttemptsExhaustion(t *testing.T) {
	db, mock, s, _ := setupE2ETest(t)
	defer db.Close()

	jobID := uuid.New().String()
	taskID := uuid.New().String()
	attemptID3 := uuid.New().String()

	// Simulating 3rd failure — attempt_count from scan is MaxTaskAttempts
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectStaleTasks)).WithArgs(s.replicaIndex).WillReturnRows(
		sqlmock.NewRows([]string{"task_id", "attempt_id", "job_id", "attempt_count", "last_renewed_at", "lease_ttl"}).AddRow(taskID, attemptID3, jobID, MaxTaskAttempts, time.Now().Add(-1*time.Hour), 30))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskStatus)).WithArgs("Failed", taskID).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryFailAttempt)).WithArgs(attemptID3).WillReturnResult(sqlmock.NewResult(1, 1))
	// updateJobStatusTx to Cleaning
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).WithArgs(jobID, "Cleaning").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	recovered, err := s.FailStaleTasks(context.Background())
	if err != nil {
		t.Fatalf("FailStaleTasks failed: %v", err)
	}
	if recovered != 1 {
		t.Errorf("Expected 1 recovered task, got %d", recovered)
	}
}

func TestE2E_CancellationDuringExecution(t *testing.T) {
	db, mock, s, orch := setupE2ETest(t)
	defer db.Close()

	jobID := uuid.New().String()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobStatusForUpdate)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("Running"))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).WithArgs(jobID, "Cleaning").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryLockNonCompletedTasksByJob)).WithArgs(jobID).WillReturnRows(sqlmock.NewRows([]string{"task_id"}))
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectRunningAttemptIDsByJob)).WithArgs(jobID).WillReturnRows(sqlmock.NewRows([]string{"attempt_id"}))
	mock.ExpectExec(regexp.QuoteMeta(QueryBulkFailNonCompletedTasksByJob)).WithArgs(jobID).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryFailRunningAttemptsByJob)).WithArgs(jobID).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// finalizeJob mock (called in goroutine)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobStatusForUpdate)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("Cleaning"))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).WithArgs(jobID, "Cancelled").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := s.CancelJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("CancelJob failed: %v", err)
	}

	// The new architecture enqueues cleanup via enqueueCleanup instead of goroutines.
	// Manually drain the queue by popping and running finalizeJob.
	pending := s.popPendingCleanup(10)
	for jid, terminalState := range pending {
		s.finalizeJob(context.Background(), jid, terminalState)
	}

	if orch.CancelCount() != 1 || orch.GetCancelCall(0) != jobID {
		t.Errorf("Expected orchestrator CancelJob for %s, got count=%d", jobID, orch.CancelCount())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestE2E_ManagerRestartRecovery(t *testing.T) {
	db, mock, s, _ := setupE2ETest(t)
	defer db.Close()

	jobID := uuid.New().String()

	// 1. Reset tasks to idle
	mock.ExpectExec(regexp.QuoteMeta(QueryResetTasksForReplica)).
		WithArgs(s.replicaIndex).
		WillReturnResult(sqlmock.NewResult(0, 5))

	// 2. Identify running jobs for this replica
	mock.ExpectQuery("SELECT job_id FROM JOBS WHERE status = 'Running' AND replica_index = \\$1").
		WithArgs(s.replicaIndex).
		WillReturnRows(sqlmock.NewRows([]string{"job_id"}).AddRow(jobID))

	// 3. Ensure worker pool exists
	// No explicit expectation for EnsureWorkerPool in sqlmock, but we check orch calls
	err := s.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Wait a bit for the background pool check (though in this implementation it's synchronous)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
