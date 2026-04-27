package manager

import (
	"context"
	"regexp"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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
