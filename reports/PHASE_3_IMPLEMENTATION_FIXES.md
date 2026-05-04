# Phase 3 Logic Flaws — Implementation Fixes

This document provides concrete code changes required to fix the 3 logic flaws identified in Phase 3 audit.

---

## Fix 1: logic-cancel (HIGH) — Job Cancellation Race Condition

**Issue:** Task results committed to DB while pods deleted asynchronously, creating inconsistent state.

**Files to Modify:**
- `manager-service/internal/manager/scheduler.go`

### Change 1a: Validate job status in CompleteTask

**Location:** `scheduler.go` — `CompleteTask()` method, after task status validation (around line 765)

**Before:**
```go
if state != "In-Progress" {
    return ErrInvalidStateTransition
}
if !currAttempt.Valid || currAttempt.String != attemptID {
    return ErrStaleAttempt
}
// Immediately proceeds to insert output
```

**After:**
```go
if state != "In-Progress" {
    return ErrInvalidStateTransition
}
if !currAttempt.Valid || currAttempt.String != attemptID {
    return ErrStaleAttempt
}

// ADD: Check job is not already terminal
var jobStatus string
if err := tx.QueryRowContext(ctx, 
    `SELECT status FROM JOBS WHERE job_id = $1 FOR UPDATE`, 
    jobID).Scan(&jobStatus); err != nil {
    return err
}
if jobStatus != "Running" && jobStatus != "Pending" {
    // Job is Cleaning/Cancelled/Failed — don't accept more results
    return fmt.Errorf("job is in terminal state %s, rejecting task completion", jobStatus)
}
```

### Change 1b: Reorder finalizeJob operations

**Location:** `scheduler.go` — `finalizeJob()` method (around lines 1302-1350)

**Before:**
```go
func (s *Scheduler) finalizeJob(ctx context.Context, jobID, terminalState string) {
    // ... setup code ...
    
    // PODS DELETED FIRST
    if err := s.orchestrator.CancelJob(ctx, jobID); err != nil {
        slog.ErrorContext(ctx, "orchestrator.CancelJob failed", 
            slog.String("job_id", jobID), 
            slog.Any("err", err))
        s.enqueueCleanup(jobID, terminalState)
        return
    }
    
    // ... staging cleanup ...
    
    // DATABASE UPDATED LATER
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        slog.ErrorContext(ctx, "BeginTx failed", slog.Any("err", err))
        s.enqueueCleanup(jobID, terminalState)
        return
    }
    defer tx.Rollback()
    
    if err := s.applyJobTransitionTx(ctx, tx, jobID, terminalState); err != nil {
        slog.ErrorContext(ctx, "applyJobTransitionTx failed", slog.Any("err", err))
        s.enqueueCleanup(jobID, terminalState)
        return
    }
    
    if err := tx.Commit(); err != nil {
        slog.ErrorContext(ctx, "tx.Commit failed", slog.Any("err", err))
        s.enqueueCleanup(jobID, terminalState)
        return
    }
}
```

**After:**
```go
func (s *Scheduler) finalizeJob(ctx context.Context, jobID, terminalState string) {
    // ... setup code ...
    
    // DATABASE UPDATED FIRST
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        slog.ErrorContext(ctx, "BeginTx failed", slog.Any("err", err))
        s.enqueueCleanup(jobID, terminalState)
        return
    }
    defer tx.Rollback()
    
    if err := s.applyJobTransitionTx(ctx, tx, jobID, terminalState); err != nil {
        slog.ErrorContext(ctx, "applyJobTransitionTx failed", slog.Any("err", err))
        s.enqueueCleanup(jobID, terminalState)
        return
    }
    
    if err := tx.Commit(); err != nil {
        slog.ErrorContext(ctx, "tx.Commit failed", slog.Any("err", err))
        s.enqueueCleanup(jobID, terminalState)
        return
    }
    
    // PODS DELETED ONLY AFTER DATABASE COMMITTED
    if err := s.orchestrator.CancelJob(ctx, jobID); err != nil {
        slog.ErrorContext(ctx, "orchestrator.CancelJob failed", 
            slog.String("job_id", jobID), 
            slog.Any("err", err))
        // Note: Job status already committed as terminal in DB
        // Failing to delete pods is not fatal, but should be retried
        s.enqueueCleanup(jobID, terminalState)
        return
    }
    
    // ... staging cleanup ...
}
```

**Rationale:** Database transaction must commit before any side effects (pod deletion). This ensures if the pod deletion fails, at least the job is already marked terminal in the DB.

---

## Fix 2: logic-lease (HIGH) — Clock Skew in Lease Expiry

**Issue:** If Manager process clock is ahead of PostgreSQL server, leases expire prematurely.

**Files to Modify:**
- `manager-service/internal/manager/queries.go`

### Change 2a: Update QueryCheckLeaseValid

**Location:** `queries.go` — `QueryCheckLeaseValid` constant (around line 127)

**Before:**
```go
const QueryCheckLeaseValid = `
    SELECT lease_id = $2
        AND last_renewed_at + lease_ttl * INTERVAL '1 second' >= NOW() AS lease_valid
    FROM TASK_ATTEMPTS
    WHERE attempt_id = $1
`
```

**After:**
```go
const QueryCheckLeaseValid = `
    SELECT lease_id = $2
        AND last_renewed_at + lease_ttl * INTERVAL '1 second' + INTERVAL '5 seconds' >= NOW() AS lease_valid
    FROM TASK_ATTEMPTS
    WHERE attempt_id = $1
`
```

### Change 2b: Update QuerySelectStaleTasks

**Location:** `queries.go` — `QuerySelectStaleTasks` constant (around line 164)

**Before:**
```go
const QuerySelectStaleTasks = `
    SELECT t.id, t.job_id, t.task_type, t.replica_index, t.status, 
           t.current_attempt_id, a.id, a.lease_id, a.lease_ttl
    FROM TASKS t
    JOIN TASK_ATTEMPTS a ON t.current_attempt_id = a.id
    WHERE t.status = 'In-Progress' 
        AND a.status = 'Running'
        AND a.last_renewed_at + a.lease_ttl * INTERVAL '1 second' < NOW()
    FOR UPDATE OF t SKIP LOCKED
`
```

**After:**
```go
const QuerySelectStaleTasks = `
    SELECT t.id, t.job_id, t.task_type, t.replica_index, t.status, 
           t.current_attempt_id, a.id, a.lease_id, a.lease_ttl
    FROM TASKS t
    JOIN TASK_ATTEMPTS a ON t.current_attempt_id = a.id
    WHERE t.status = 'In-Progress' 
        AND a.status = 'Running'
        AND a.last_renewed_at + a.lease_ttl * INTERVAL '1 second' - INTERVAL '5 seconds' < NOW()
    FOR UPDATE OF t SKIP LOCKED
`
```

Note: Subtracting 5 seconds from the tolerance window makes the query more conservative — it marks tasks as stale 5 seconds earlier, which provides safety margin.

### Change 2c: Add validation in validateLeaseTx (optional but recommended)

**Location:** `scheduler.go` — `validateLeaseTx()` method (around line 711)

**Before:**
```go
func (s *Scheduler) validateLeaseTx(ctx context.Context, tx *sql.Tx, leaseID uuid.UUID, ttl int) (bool, error) {
    var isValid bool
    if err := tx.QueryRowContext(ctx, QueryCheckLeaseValid, leaseID, leaseID).Scan(&isValid); err != nil {
        return false, err
    }
    return isValid, nil
}
```

**After:** (No change needed if 5-second tolerance added to QueryCheckLeaseValid)

**Comment:** The constant change in queries.go is sufficient. No method-level change required.

**Alternative Approach (if strict NTP is enforced):**

Add startup validation instead of query tolerance:
```go
// In manager/main.go:main() after database connection
clockSkew := checkPostgresClockSkew(db)
if clockSkew > 5*time.Second {
    log.Fatalf("Manager clock differs from PostgreSQL by %v; please synchronize via NTP", clockSkew)
}

func checkPostgresClockSkew(db *sql.DB) time.Duration {
    var dbTime time.Time
    if err := db.QueryRow(`SELECT NOW()`).Scan(&dbTime); err != nil {
        return 0
    }
    localTime := time.Now()
    return localTime.Sub(dbTime)
}
```

---

## Fix 3: logic-jsonl (MEDIUM) — JSONL EOF Record Handling

**Issue:** Records without trailing newlines at end of split rejected as incomplete.

**Files to Modify:**
- `worker-service/internal/worker/split.go`

### Change: Update record reading loop

**Location:** `split.go` — `readSplitRecords()` method (around lines 88-92)

**Before:**
```go
func readSplitRecords(br *bufio.Reader, byteStart, byteEnd int64) ([][]byte, error) {
    var records [][]byte
    var offset int64

    for {
        line, err := br.ReadBytes('\n')
        if len(line) > 0 {
            if err == io.EOF {
                // REJECTS last record if no newline
                return nil, fmt.Errorf("incomplete JSONL record at byte offset %d", byteStart+offset)
            }
            trimmed := bytes.TrimRight(line, "\r\n")
            if len(trimmed) > 0 {
                records = append(records, append([]byte(nil), trimmed...))
            }
            offset += int64(len(line))
        }
        if err != nil {
            break
        }
    }
    return records, nil
}
```

**After:**
```go
func readSplitRecords(br *bufio.Reader, byteStart, byteEnd int64) ([][]byte, error) {
    var records [][]byte
    var offset int64

    for {
        line, err := br.ReadBytes('\n')
        if len(line) > 0 {
            // Accept lines even if no trailing newline (valid at EOF)
            // Only error if we're mid-split and hit EOF unexpectedly
            if err == io.EOF {
                // This is the last line — accept it even without newline
                trimmed := bytes.TrimRight(line, "\r\n")
                if len(trimmed) > 0 {
                    records = append(records, append([]byte(nil), trimmed...))
                }
                return records, nil  // Exit gracefully
            }
            if err != nil {
                // Actual error reading
                return nil, err
            }
            
            trimmed := bytes.TrimRight(line, "\r\n")
            if len(trimmed) > 0 {
                records = append(records, append([]byte(nil), trimmed...))
            }
            offset += int64(len(line))
        } else if err != nil {
            // Empty line at EOF or error
            if err == io.EOF {
                return records, nil
            }
            return nil, err
        }
    }
}
```

**Alternative (Simpler):**

Replace the entire loop with:
```go
func readSplitRecords(br *bufio.Reader, byteStart, byteEnd int64) ([][]byte, error) {
    var records [][]byte

    for {
        line, err := br.ReadBytes('\n')
        if len(line) > 0 {
            trimmed := bytes.TrimRight(line, "\r\n")
            if len(trimmed) > 0 {
                records = append(records, append([]byte(nil), trimmed...))
            }
        }
        
        if err == io.EOF {
            // Graceful end of split — this is normal
            return records, nil
        }
        if err != nil {
            // Actual error
            return nil, err
        }
    }
}
```

**Rationale:** The loop should accept any valid JSONL record, including those without trailing newlines. io.EOF at the end of a split is normal and indicates the split was fully read.

---

## Testing These Fixes

### Test 1: Cancel/Complete Race (Fix 1)

```go
func TestScheduler_CancelJobDuringCompleteTask(t *testing.T) {
    // Setup scheduler and submit job
    // Goroutine 1: Simulate CompleteTask RPC (sleep midway)
    // Goroutine 2: Call CancelJob while Goroutine 1 is in progress
    // Verify: Database is consistent, no orphaned results
    // Verify: Pods are deleted only after job status committed
}
```

### Test 2: Clock Skew Lease Expiry (Fix 2)

```go
func TestScheduler_LeaseExpiryWithClockSkew(t *testing.T) {
    // Setup scheduler with PostgreSQL
    // Manually set lease_ttl to 10 seconds
    // Artificially move database time forward by 7 seconds (simulating skew)
    // Verify: Lease is still valid (not expired prematurely)
    // Verify: After 15+ seconds passes, lease is finally expired
}
```

### Test 3: JSONL EOF Handling (Fix 3)

```go
func TestSplitRecords_NoTrailingNewline(t *testing.T) {
    // Create JSONL: `{"id":"1"}\n{"id":"2"}` (no trailing newline on last record)
    // Call readSplitRecords
    // Verify: Both records returned successfully
    // No error thrown
}
```

---

## Deployment Checklist

- [ ] Apply Fix 1: logic-cancel (scheduler.go changes)
- [ ] Apply Fix 2: logic-lease (queries.go constants)
- [ ] Apply Fix 3: logic-jsonl (split.go loop)
- [ ] Run unit tests: `go test -v ./...`
- [ ] Run race detector: `go test -race ./...`
- [ ] Code formatting: `go fmt ./...`
- [ ] Static analysis: `go vet ./... && golangci-lint run`
- [ ] Dependency check: `go mod tidy && govulncheck ./...`
- [ ] Integration tests with scenarios above
- [ ] Deploy to staging
- [ ] Smoke tests on staging
- [ ] Production deployment

---

## Estimated Effort

- **Development:** 1 hour (all 3 fixes are straightforward)
- **Testing:** 1 hour (unit tests + integration scenarios)
- **Code Review:** 30 minutes
- **Deployment:** 30 minutes (including smoke tests)

**Total:** ~3 hours to production-ready state
