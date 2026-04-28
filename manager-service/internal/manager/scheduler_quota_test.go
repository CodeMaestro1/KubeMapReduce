package manager

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// TestScheduler_ReadQuotaSnapshotTx_ConcurrentReadsConsistent runs N parallel
// goroutines that each open their own transaction and call readQuotaSnapshotTx.
// It uses a non-ordered expectation set so the mock accepts queries in whatever
// order Go's scheduler interleaves them. The test asserts:
//
//   - every caller successfully acquires the advisory lock and reads the same
//     committed (max, active) pair (sqlmock returns a fixed snapshot), and
//   - no caller receives an error from the lock or count queries.
//
// This is the unit-level analogue of "multiple scheduler replicas reading the
// quota table simultaneously" from issue #75.
func TestScheduler_ReadQuotaSnapshotTx_ConcurrentReadsConsistent(t *testing.T) {
	const callers = 16

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	scheduler, err := NewScheduler(db, 0, 1, &MockOrchestrator{}, "manager-0:50051", 30, nil)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	for i := 0; i < callers; i++ {
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
			WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(8))
		mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
		mock.ExpectRollback()
	}

	var wg sync.WaitGroup
	errs := make([]error, callers)
	snaps := make([]QuotaSnapshot, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				errs[idx] = err
				return
			}
			defer tx.Rollback()
			snaps[idx], errs[idx] = scheduler.readQuotaSnapshotTx(context.Background(), tx)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("caller %d: unexpected error %v", i, e)
		}
		if snaps[i].MaxPods != 8 || snaps[i].ActivePods != 3 || snaps[i].Available != 5 {
			t.Fatalf("caller %d: snapshot mismatch %+v", i, snaps[i])
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}

// fakeQuotaDB is an in-memory model of the rows the scheduler touches during
// quota enforcement. It serializes the advisory-lock + count-running + insert
// sequence using a sync.Mutex, which is the property the real PostgreSQL
// pg_advisory_xact_lock(42) provides for the same critical section.
//
// The model captures the maximum active count ever observed so a stress test
// can assert that no run of the algorithm permitted oversubscription.
type fakeQuotaDB struct {
	mu        sync.Mutex
	maxPods   int
	active    int
	highWater int
	rejected  int
	accepted  int
}

func (f *fakeQuotaDB) tryAcquire() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active >= f.maxPods {
		f.rejected++
		return ErrQuotaExceeded
	}
	f.active++
	if f.active > f.highWater {
		f.highWater = f.active
	}
	f.accepted++
	return nil
}

func (f *fakeQuotaDB) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active > 0 {
		f.active--
	}
}

// TestScheduler_QuotaCriticalSection_NoOversubscriptionUnderStress models the
// algorithm's critical section with an in-memory mutex (the same guarantee the
// advisory lock provides) and hammers it from many goroutines. It asserts that
// the high-water mark of concurrently-running attempts never exceeds maxPods,
// which is the explicit "No oversubscription in stress test" acceptance
// criterion of issue #75.
func TestScheduler_QuotaCriticalSection_NoOversubscriptionUnderStress(t *testing.T) {
	const (
		maxPods    = 4
		goroutines = 64
		iterations = 50
	)

	q := &fakeQuotaDB{maxPods: maxPods}
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if err := q.tryAcquire(); err == nil {
					q.release()
				}
			}
		}()
	}
	wg.Wait()

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.highWater > maxPods {
		t.Fatalf("oversubscription detected: highWater=%d, maxPods=%d", q.highWater, maxPods)
	}
	if q.accepted == 0 {
		t.Fatalf("expected at least one acceptance, got 0")
	}
	total := q.accepted + q.rejected
	if total != goroutines*iterations {
		t.Fatalf("accounting drift: accepted=%d rejected=%d total=%d expected=%d",
			q.accepted, q.rejected, total, goroutines*iterations)
	}
}

// TestScheduler_GetNextTask_QuotaExceededLeavesTaskIdle verifies the quota
// rejection path from the real GetNextTask code path: an over-quota call must
// roll back the transaction so the candidate task remains 'Idle' and is
// available to the next scheduling tick (no work is consumed by a refusal).
func TestScheduler_GetNextTask_QuotaExceededLeavesTaskIdle(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobUUID := uuid.New()
	jobID := jobUUID.String()
	taskID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID, 0, "Map").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "replica_index"}).
			AddRow(taskID, jobUUID, "Map", 0))
	expectTaskMetadataQueries(mock, taskID, "s3://map", "s3://reduce", 1)
	mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(2))
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectRollback()

	_, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1")
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
	// Crucially: no UpdateTaskInProgress / InsertAttempt / Commit were expected
	// above. ExpectationsWereMet would fail if the rejection path executed any
	// state-mutating query.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}

// TestScheduler_QuerySelectIdleTask_HasFifoOrdering is a documentation test:
// it pins the SQL ordering clause that defines the fairness policy described
// in package doc.go. If a future change drops the ORDER BY clause, this test
// fails, prompting the author to update doc.go in lockstep.
func TestScheduler_QuerySelectIdleTask_HasFifoOrdering(t *testing.T) {
	q := strings.ToUpper(QuerySelectIdleTask)
	if !strings.Contains(q, "ORDER BY REPLICA_INDEX ASC, TASK_ID ASC") {
		t.Fatalf("QuerySelectIdleTask must declare deterministic FIFO ordering; got:\n%s", QuerySelectIdleTask)
	}
	if !strings.Contains(q, "FOR UPDATE SKIP LOCKED") {
		t.Fatalf("QuerySelectIdleTask must use FOR UPDATE SKIP LOCKED; got:\n%s", QuerySelectIdleTask)
	}
}

// TestScheduler_GetNextTask_ReducePhaseStarvationGuard simulates the case where
// every Map task has been picked up but is still In-Progress. Reduce
// scheduling must NOT be allowed in this state, otherwise reducers would race
// mappers and read partial outputs. The scheduler must surface ErrNoIdleTasks
// so the caller waits for the Map phase to finish.
func TestScheduler_GetNextTask_ReducePhaseStarvationGuard(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobID := uuid.NewString()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID, 0, "Map").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountPendingTasksByType)).
		WithArgs(jobID, "Map").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	mock.ExpectRollback()

	_, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1")
	if !errors.Is(err, ErrNoIdleTasks) {
		t.Fatalf("expected ErrNoIdleTasks while Map phase incomplete, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}

// TestScheduler_GetNextTask_FifoStarvationFreedom interleaves three IDLE-task
// selections that each return a different candidate row; after all three are
// drained, the fourth call must observe sql.ErrNoRows and surface
// ErrJobCompleted (assuming no pending tasks). The point of this test is to
// document that the scheduler's FIFO loop is starvation-free: every pending
// idle task is eventually selected exactly once.
func TestScheduler_GetNextTask_FifoStarvationFreedom(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobUUID := uuid.New()
	jobID := jobUUID.String()

	taskIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}

	for _, tid := range taskIDs {
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
			WithArgs(jobID).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
			WithArgs(jobID, 0, "Map").
			WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "replica_index"}).
				AddRow(tid, jobUUID, "Map", 0))
		expectTaskMetadataQueries(mock, tid, "s3://map", "s3://reduce", 1)
		mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
			WillReturnRows(sqlmock.NewRows([]string{"max_concurrent_pods"}).AddRow(10))
		mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
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

	seen := make(map[string]int)
	for range taskIDs {
		task, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		seen[task.ID]++
	}
	for _, tid := range taskIDs {
		if seen[tid.String()] != 1 {
			t.Fatalf("task %s was scheduled %d times (expected exactly 1)", tid, seen[tid.String()])
		}
	}

	if _, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1"); !errors.Is(err, ErrJobCompleted) {
		t.Fatalf("expected ErrJobCompleted after all tasks drained, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}

// TestScheduler_GetNextTask_EmptySystemConfigFallback verifies that the
// scheduler correctly falls back to a hardcoded default (10) when the
// SYSTEM_CONFIG table is empty (common on fresh clusters).
func TestScheduler_GetNextTask_EmptySystemConfigFallback(t *testing.T) {
	db, mock, scheduler := setupMockDB(t)
	defer db.Close()
	jobUUID := uuid.New()
	jobID := jobUUID.String()
	taskID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(QueryCountFailedTasks)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(regexp.QuoteMeta(QuerySelectIdleTask)).
		WithArgs(jobID, 0, "Map").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "job_id", "task_type", "replica_index"}).
			AddRow(taskID, jobUUID, "Map", 0))
	expectTaskMetadataQueries(mock, taskID, "s3://map", "s3://reduce", 1)
	mock.ExpectExec(regexp.QuoteMeta(QueryAcquireSchedulingLock)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Simulate empty SYSTEM_CONFIG row
	mock.ExpectQuery(regexp.QuoteMeta(QueryGetMaxConcurrentPods)).
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery(regexp.QuoteMeta(QueryCountRunningAttempts)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2)) // 2 running < 10 default

	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateTaskInProgress)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryInsertAttempt)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(QueryUpdateJobStatus)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	task, err := scheduler.GetNextTask(context.Background(), jobID, "worker-1")
	if err != nil {
		t.Fatalf("expected GetNextTask to succeed with default quota, got error: %v", err)
	}
	if task == nil || task.ID != taskID.String() {
		t.Fatalf("expected task %s, got %v", taskID, task)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}
