package manager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

const MaxTaskAttempts = 3

var (
	ErrNoIdleTasks            = errors.New("no idle tasks available right now, please wait")
	ErrJobCompleted           = errors.New("all tasks completed, job is done")
	ErrTaskNotFound           = errors.New("task not found")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrEmptyJobID             = errors.New("jobID cannot be empty")
	ErrEmptyWorkerID          = errors.New("workerID cannot be empty")
	ErrInvalidTimeout         = errors.New("timeout must be strictly positive")
	ErrStaleAttempt           = errors.New("stale commit attempt rejected to prevent split-brain")
	ErrExpiredLease           = errors.New("lease expired or mismatched")
	ErrOutputMismatch         = errors.New("outputURIs and outputChecksums must have the same length")
	ErrJobFailed              = errors.New("job failed: one or more tasks reached maximum attempts")
	ErrQuotaExceeded          = errors.New("global worker pod quota exceeded")
)

type Scheduler struct {
	db           *sql.DB
	replicaIndex int
}

type taskMetadataQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func NewScheduler(db *sql.DB, replicaIndex int) (*Scheduler, error) {
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}
	return &Scheduler{
		db:           db,
		replicaIndex: replicaIndex,
	}, nil
}

func (s *Scheduler) GetNextTask(jobID string, workerID string) (*Task, error) {
	if jobID == "" {
		return nil, ErrEmptyJobID
	}
	if workerID == "" {
		return nil, ErrEmptyWorkerID
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// 1. Quota Enforcement
	var maxPods, activePods int
	err = tx.QueryRowContext(ctx, QueryGetMaxConcurrentPods).Scan(&maxPods)
	if err != nil {
		return nil, err
	}
	err = tx.QueryRowContext(ctx, QueryCountRunningAttempts).Scan(&activePods)
	if err != nil {
		return nil, err
	}
	if activePods >= maxPods {
		return nil, ErrQuotaExceeded
	}

	// 2. Check for failed job tasks
	var failedCount int
	err = tx.QueryRowContext(ctx, QueryCountFailedTasks, jobID).Scan(&failedCount)
	if err == nil && failedCount > 0 {
		return nil, ErrJobFailed
	}

	// 3. Try to schedule Map tasks first
	task, err := s.tryAssignTask(ctx, tx, jobID, workerID, "Map")
	if err == nil {
		if errCommit := tx.Commit(); errCommit != nil {
			return nil, errCommit
		}
		return task, nil
	}
	if !errors.Is(err, ErrNoIdleTasks) {
		return nil, err
	}

	// 4. Check if Map phase is completed
	var pendingMapTasks int
	err = tx.QueryRowContext(ctx, QueryCountPendingTasksByType, jobID, "Map").Scan(&pendingMapTasks)
	if err != nil {
		return nil, err
	}
	if pendingMapTasks > 0 {
		return nil, ErrNoIdleTasks
	}

	// 5. Try to schedule Reduce tasks
	task, err = s.tryAssignTask(ctx, tx, jobID, workerID, "Reduce")
	if err == nil {
		if errCommit := tx.Commit(); errCommit != nil {
			return nil, errCommit
		}
		return task, nil
	}
	if !errors.Is(err, ErrNoIdleTasks) {
		return nil, err
	}

	// 6. Check if Reduce phase is completed
	var pendingReduceTasks int
	err = tx.QueryRowContext(ctx, QueryCountPendingTasksByType, jobID, "Reduce").Scan(&pendingReduceTasks)
	if err != nil {
		return nil, err
	}
	if pendingReduceTasks > 0 {
		return nil, ErrNoIdleTasks
	}

	return nil, ErrJobCompleted
}

func (s *Scheduler) tryAssignTask(ctx context.Context, tx *sql.Tx, requestedJobID string, workerID string, taskType string) (*Task, error) {
	row := tx.QueryRowContext(ctx, QuerySelectIdleTask, requestedJobID, s.replicaIndex, taskType)

	var t Task
	var tType string
	var dbJobID uuid.UUID
	var taskID uuid.UUID

	err := row.Scan(&taskID, &dbJobID, &tType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoIdleTasks
		}
		return nil, err
	}

	t.ID = taskID.String()
	t.JobID = dbJobID.String()
	if tType == "Map" {
		t.Type = MapTask
	} else {
		t.Type = ReduceTask
	}

	attemptID := uuid.New()
	leaseID := uuid.New()
	t.ActiveAttemptID = attemptID.String()
	t.LeaseID = leaseID.String()
	t.State = InProgress
	now := time.Now()
	t.startTime = now
	t.LastHeartbeat = now
	if t.JobID != requestedJobID {
		return nil, fmt.Errorf("scheduled task %s belongs to unexpected job %s", t.ID, t.JobID)
	}
	if err := s.hydrateTaskMetadata(ctx, tx, &t); err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(ctx, QueryUpdateTaskInProgress, attemptID, taskID)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(ctx, QueryInsertAttempt,
		attemptID, taskID, workerID, leaseID, now, now)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (s *Scheduler) hydrateTaskMetadata(ctx context.Context, q taskMetadataQuerier, t *Task) error {
	var mapperURI, reducerURI, combinerURI, inputChecksum string
	if err := q.QueryRowContext(ctx, QueryGetJobConfigByTask, t.ID).
		Scan(&mapperURI, &reducerURI, &combinerURI, &t.TotalReducers, &inputChecksum); err != nil {
		return err
	}

	t.CombinerURI = combinerURI
	t.InputChecksum = inputChecksum
	if t.Type == MapTask {
		t.CodeURI = mapperURI
	} else {
		t.CodeURI = reducerURI
	}

	rows, err := q.QueryContext(ctx, QueryGetTaskInputs, t.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var splits []TaskInputSplit
	for rows.Next() {
		var split TaskInputSplit
		if err := rows.Scan(&split.InputURI, &split.ByteStart, &split.ByteEnd, &split.SplitChecksum); err != nil {
			return err
		}
		splits = append(splits, split)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	t.InputSplits = splits
	if len(splits) > 0 {
		t.InputFile = splits[0].InputURI
		t.ByteStart = splits[0].ByteStart
		t.ByteEnd = splits[0].ByteEnd
	}

	return nil
}

func (s *Scheduler) CompleteTask(taskID string, attemptID string, leaseID string, outputURIs []string, outputChecksums []string) error {
	if len(outputURIs) != len(outputChecksums) {
		return ErrOutputMismatch
	}

	// Deep copy to prevent caller mutation from affecting internal db logic, fulfilling problem 3
	safeURIs := make([]string, len(outputURIs))
	copy(safeURIs, outputURIs)
	safeChecksums := make([]string, len(outputChecksums))
	copy(safeChecksums, outputChecksums)

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var state string
	var currAttempt sql.NullString
	err = tx.QueryRowContext(ctx, QuerySelectTaskForUpdate, taskID).Scan(&state, &currAttempt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskNotFound
		}
		return err
	}

	if state != "In-Progress" {
		return ErrInvalidStateTransition
	}
	if !currAttempt.Valid || currAttempt.String != attemptID {
		return ErrStaleAttempt
	}

	var dbLeaseID string
	var lastRenewed time.Time
	var ttl int
	err = tx.QueryRowContext(ctx, QuerySelectLeaseInfo, attemptID).Scan(&dbLeaseID, &lastRenewed, &ttl)
	if err != nil {
		return err
	}

	if dbLeaseID != leaseID || time.Now().After(lastRenewed.Add(time.Duration(ttl)*time.Second)) {
		return ErrExpiredLease
	}

	_, err = tx.ExecContext(ctx, QueryCompleteTask, taskID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, QuerySucceedAttempt, time.Now(), attemptID)
	if err != nil {
		return err
	}

	for i, uri := range safeURIs {
		_, err = tx.ExecContext(ctx, QueryInsertOutput, taskID, i, uri, safeChecksums[i])
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Scheduler) RenewLease(taskID string, attemptID string, leaseID string) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var state string
	var currAttempt sql.NullString
	err = tx.QueryRowContext(ctx, QuerySelectTaskForUpdate, taskID).Scan(&state, &currAttempt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskNotFound
		}
		return err
	}

	if state != "In-Progress" {
		return ErrInvalidStateTransition
	}
	if !currAttempt.Valid || currAttempt.String != attemptID {
		return ErrStaleAttempt
	}

	var dbLeaseID string
	var lastRenewed time.Time
	var ttl int
	err = tx.QueryRowContext(ctx, QuerySelectLeaseInfo, attemptID).Scan(&dbLeaseID, &lastRenewed, &ttl)
	if err != nil {
		return err
	}

	if dbLeaseID != leaseID || time.Now().After(lastRenewed.Add(time.Duration(ttl)*time.Second)) {
		return ErrExpiredLease
	}

	_, err = tx.ExecContext(ctx, QueryRenewLease, time.Now(), attemptID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Scheduler) FailTask(taskID string, attemptID string, leaseID string, reason string) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var state string
	var currAttempt sql.NullString
	err = tx.QueryRowContext(ctx, QuerySelectTaskForUpdate, taskID).Scan(&state, &currAttempt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskNotFound
		}
		return err
	}

	if state != "In-Progress" {
		return ErrInvalidStateTransition
	}
	if !currAttempt.Valid || currAttempt.String != attemptID {
		return ErrStaleAttempt
	}

	var dbLeaseID string
	var lastRenewed time.Time
	var ttl int
	err = tx.QueryRowContext(ctx, QuerySelectLeaseInfo, attemptID).Scan(&dbLeaseID, &lastRenewed, &ttl)
	if err != nil {
		return err
	}

	if dbLeaseID != leaseID || time.Now().After(lastRenewed.Add(time.Duration(ttl)*time.Second)) {
		return ErrExpiredLease
	}

	var attemptCount int
	err = tx.QueryRowContext(ctx, QueryCountAttemptsByTask, taskID).Scan(&attemptCount)
	if err != nil {
		return err
	}

	newState := "Idle"
	if attemptCount >= MaxTaskAttempts {
		newState = "Failed"
	}

	_, err = tx.ExecContext(ctx, QueryUpdateTaskStatus, newState, taskID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, QueryFailAttempt, time.Now(), attemptID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Scheduler) FailStaleTasks(timeout time.Duration) (int, error) {
	if timeout <= 0 {
		return 0, ErrInvalidTimeout
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, QuerySelectStaleTasks, time.Now().Add(-timeout))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type staleRec struct {
		taskID    string
		attemptID string
	}
	var stales []staleRec
	for rows.Next() {
		var t, a string
		if err := rows.Scan(&t, &a); err != nil {
			return 0, fmt.Errorf("scanning stale task row: %w", err)
		}
		stales = append(stales, staleRec{t, a})
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterating stale task rows: %w", err)
	}

	recoveredCount := 0
	for _, rec := range stales {
		var attemptCount int
		err = tx.QueryRowContext(ctx, QueryCountAttemptsByTask, rec.taskID).Scan(&attemptCount)
		if err != nil {
			return 0, fmt.Errorf("counting attempts for task %s: %w", rec.taskID, err)
		}

		newState := "Idle"
		if attemptCount >= MaxTaskAttempts {
			newState = "Failed"
		}

		_, err = tx.ExecContext(ctx, QueryUpdateTaskStatus, newState, rec.taskID)
		if err != nil {
			return 0, fmt.Errorf("updating status for task %s: %w", rec.taskID, err)
		}

		_, err = tx.ExecContext(ctx, QueryFailAttempt, time.Now(), rec.attemptID)
		if err != nil {
			return 0, fmt.Errorf("failing attempt %s: %w", rec.attemptID, err)
		}
		recoveredCount++
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit stale task cleanup: %v", err)
		return 0, err
	}
	return recoveredCount, nil
}

func (s *Scheduler) GetMapOutputs(jobID string) []string {
	ctx := context.Background()
	rows, err := s.db.QueryContext(ctx, QueryGetMapOutputs, jobID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var outputs []string
	for rows.Next() {
		var uri string
		if err := rows.Scan(&uri); err == nil {
			outputs = append(outputs, uri)
		}
	}
	return outputs
}

func (s *Scheduler) GetReduceOutputs(jobID string) []string {
	ctx := context.Background()
	rows, err := s.db.QueryContext(ctx, QueryGetReduceOutputs, jobID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var outputs []string
	for rows.Next() {
		var uri string
		if err := rows.Scan(&uri); err == nil {
			outputs = append(outputs, uri)
		}
	}
	return outputs
}

func (s *Scheduler) AllMapTasksCompleted(jobID string) bool {
	ctx := context.Background()
	var pending int
	err := s.db.QueryRowContext(ctx, QueryCountPendingMapTasks, jobID).Scan(&pending)
	return err == nil && pending == 0
}

func (s *Scheduler) IsJobFinished(jobID string) bool {
	ctx := context.Background()
	var pending int
	err := s.db.QueryRowContext(ctx, QueryCountAllPendingTasks, jobID).Scan(&pending)
	return err == nil && pending == 0
}

func (s *Scheduler) GetTaskStatus(taskID string) (TaskState, error) {
	ctx := context.Background()
	var status string
	err := s.db.QueryRowContext(ctx, QueryGetTaskStatus, taskID).Scan(&status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrTaskNotFound
		}
		return 0, err
	}
	switch status {
	case "Idle":
		return Idle, nil
	case "In-Progress":
		return InProgress, nil
	case "Completed":
		return Completed, nil
	case "Failed":
		return Failed, nil
	default:
		return 0, fmt.Errorf("unrecognized task status %q", status)
	}
}

func (s *Scheduler) GetTaskByID(taskID string) (*Task, error) {
	ctx := context.Background()
	var t Task
	var jobID uuid.UUID
	var dbType string
	var dbStatus string
	var attemptID sql.NullString
	err := s.db.QueryRowContext(ctx, QueryGetTaskByID, taskID).Scan(&t.ID, &jobID, &dbType, &dbStatus, &attemptID, &t.ReplicaIndex)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}

	if dbType == "Map" {
		t.Type = MapTask
	} else {
		t.Type = ReduceTask
	}
	t.JobID = jobID.String()
	switch dbStatus {
	case "Idle":
		t.State = Idle
	case "In-Progress":
		t.State = InProgress
	case "Completed":
		t.State = Completed
	case "Failed":
		t.State = Failed
	}
	if err := s.hydrateTaskMetadata(ctx, s.db, &t); err != nil {
		return nil, err
	}
	if attemptID.Valid {
		t.ActiveAttemptID = attemptID.String
		err = s.db.QueryRowContext(ctx, QueryGetAttemptDetails, attemptID.String).Scan(&t.workerID, &t.LeaseID, &t.startTime, &t.LastHeartbeat)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("attempt %s referenced by task %s not found: %w", attemptID.String, taskID, err)
			}
			return nil, err
		}
	}

	t.OutputURIs = []string{}
	t.OutputChecksums = []string{}
	return &t, nil
}
