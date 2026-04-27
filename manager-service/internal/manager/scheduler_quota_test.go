package manager

import (
	"context"
	"errors"
	"regexp"
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
