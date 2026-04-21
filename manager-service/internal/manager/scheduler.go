package manager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const MaxTaskAttempts = 3
const (
	cleanupReconcileWorkers   = 4
	cleanupReconcileBatchSize = 64
)

var (
	ErrNoIdleTasks            = errors.New("no idle tasks available right now, please wait")
	ErrJobCompleted           = errors.New("all tasks completed, job is done")
	ErrTaskNotFound           = errors.New("task not found")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrEmptyJobID             = errors.New("jobID cannot be empty")
	ErrEmptyWorkerID          = errors.New("workerID cannot be empty")
	ErrStaleAttempt           = errors.New("stale commit attempt rejected to prevent split-brain")
	ErrExpiredLease           = errors.New("lease expired or mismatched")
	ErrOutputMismatch         = errors.New("outputURIs and outputChecksums must have the same length")
	ErrJobFailed              = errors.New("job failed: one or more tasks reached maximum attempts")
	ErrQuotaExceeded          = errors.New("global worker pod quota exceeded")
)

type Scheduler struct {
	db             *sql.DB
	replicaIndex   int
	totalReplicas  int
	orchestrator   WorkerOrchestrator
	managerAddr    string
	leaseTTL       int
	cleanupMu      sync.Mutex
	pendingCleanup map[string]string
}

type taskMetadataQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func NewScheduler(db *sql.DB, replicaIndex int, totalReplicas int, orchestrator WorkerOrchestrator, managerAddr string, leaseTTL int) (*Scheduler, error) {
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}
	if totalReplicas <= 0 {
		totalReplicas = 1
	}
	if orchestrator == nil {
		return nil, errors.New("orchestrator cannot be nil")
	}
	if leaseTTL <= 0 {
		leaseTTL = 30
	}
	return &Scheduler{
		db:             db,
		replicaIndex:   replicaIndex,
		totalReplicas:  totalReplicas,
		orchestrator:   orchestrator,
		managerAddr:    managerAddr,
		leaseTTL:       leaseTTL,
		pendingCleanup: make(map[string]string),
	}, nil
}

// Recover reconciles active attempts assigned to this replica and re-spawns workers.
// It should be called on startup to resume orchestration after a crash.
func (s *Scheduler) Recover(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, QuerySelectRecoverableAttempts, s.replicaIndex)
	if err != nil {
		return fmt.Errorf("failed to query recoverable attempts: %w", err)
	}
	defer rows.Close()

	type recoverableAttempt struct {
		taskID    string
		attemptID string
		jobID     string
	}
	var attemptsToSpawn []recoverableAttempt
	for rows.Next() {
		var rec recoverableAttempt
		if err := rows.Scan(&rec.taskID, &rec.attemptID, &rec.jobID); err != nil {
			return fmt.Errorf("failed to scan recoverable attempt during recovery: %w", err)
		}
		attemptsToSpawn = append(attemptsToSpawn, rec)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row error during recovery: %w", err)
	}

	const recoverSpawnTimeout = 20 * time.Second
	spawnFailures := 0
	for _, rec := range attemptsToSpawn {
		spawnCtx, cancel := context.WithTimeout(ctx, recoverSpawnTimeout)
		err := s.orchestrator.SpawnWorker(spawnCtx, rec.taskID, rec.jobID, rec.attemptID, s.managerAddr)
		cancel()
		if err != nil {
			log.Printf("Failed to spawn worker for recovered task %s (attempt %s): %v", rec.taskID, rec.attemptID, err)
			spawnFailures++
		}
	}
	if spawnFailures > 0 {
		log.Printf("Recovery finished with %d/%d spawn failures; service will continue and retry via reaper", spawnFailures, len(attemptsToSpawn))
	}
	return nil
}

// prepareRetryAttemptTx creates a fresh attempt and lease for a retryable task and
// atomically rebinds TASKS.current_attempt_id to the new attempt within the same transaction.
func (s *Scheduler) prepareRetryAttemptTx(ctx context.Context, tx *sql.Tx, taskID string) (string, error) {
	attemptID := uuid.New()
	leaseID := uuid.New()
	_, err := tx.ExecContext(ctx, QueryUpdateTaskInProgress, attemptID, taskID)
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, QueryInsertAttempt,
		attemptID,
		taskID,
		"system-recovery",
		leaseID,
		s.leaseTTL,
	)
	if err != nil {
		return "", err
	}
	return attemptID.String(), nil
}

func (s *Scheduler) GetNextTask(ctx context.Context, jobID string, workerID string) (*Task, error) {
	if jobID == "" {
		return nil, ErrEmptyJobID
	}
	if _, err := uuid.Parse(jobID); err != nil {
		return nil, fmt.Errorf("invalid job ID %q: %w", jobID, err)
	}
	if workerID == "" {
		return nil, ErrEmptyWorkerID
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// 1. Check for failed job tasks
	var failedCount int
	err = tx.QueryRowContext(ctx, QueryCountFailedTasks, jobID).Scan(&failedCount)
	if err != nil {
		return nil, err
	}
	if failedCount > 0 {
		return nil, ErrJobFailed
	}

	// 2. Try to schedule Map tasks first
	task, err := s.tryAssignTask(ctx, tx, jobID, workerID, "Map")
	if err == nil {
		if err := s.updateJobStatusTx(ctx, tx, jobID, "Running"); err != nil {
			return nil, err
		}
		if errCommit := tx.Commit(); errCommit != nil {
			return nil, errCommit
		}
		return task, nil
	}
	if !errors.Is(err, ErrNoIdleTasks) {
		return nil, err
	}

	// 3. Check if Map phase is completed
	var pendingMapTasks int
	err = tx.QueryRowContext(ctx, QueryCountPendingTasksByType, jobID, "Map").Scan(&pendingMapTasks)
	if err != nil {
		return nil, err
	}
	if pendingMapTasks > 0 {
		return nil, ErrNoIdleTasks
	}

	// 4. Try to schedule Reduce tasks
	task, err = s.tryAssignTask(ctx, tx, jobID, workerID, "Reduce")
	if err == nil {
		if err := s.updateJobStatusTx(ctx, tx, jobID, "Running"); err != nil {
			return nil, err
		}
		if errCommit := tx.Commit(); errCommit != nil {
			return nil, errCommit
		}
		return task, nil
	}
	if !errors.Is(err, ErrNoIdleTasks) {
		return nil, err
	}

	// 5. Check if Reduce phase is completed
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
	var replicaIndex int

	err := row.Scan(&taskID, &dbJobID, &tType, &replicaIndex)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoIdleTasks
		}
		return nil, err
	}

	t.ID = taskID.String()
	t.JobID = dbJobID.String()
	t.ReplicaIndex = replicaIndex
	switch tType {
	case "Map":
		t.Type = MapTask
	case "Reduce":
		t.Type = ReduceTask
		t.ReducePartition = replicaIndex
	default:
		return nil, fmt.Errorf("unexpected task type %q for task %s", tType, taskID.String())
	}

	attemptID := uuid.New()
	leaseID := uuid.New()
	t.ActiveAttemptID = attemptID.String()
	t.LeaseID = leaseID.String()
	t.State = InProgress
	now := time.Now()
	t.startTime = now
	t.LastHeartbeat = now
	requestedUUID, err := uuid.Parse(requestedJobID)
	if err != nil {
		return nil, fmt.Errorf("invalid requested job ID %q: %w", requestedJobID, err)
	}
	if dbJobID != requestedUUID {
		return nil, fmt.Errorf("scheduled task %s belongs to unexpected job %s", t.ID, t.JobID)
	}
	if err := s.hydrateTaskMetadata(ctx, tx, &t); err != nil {
		return nil, err
	}
	if err := s.enforceQuotaTx(ctx, tx); err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(ctx, QueryUpdateTaskInProgress, attemptID, taskID)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(ctx, QueryInsertAttempt,
		attemptID,
		taskID,
		workerID,
		leaseID,
		s.leaseTTL,
	)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (s *Scheduler) enforceQuotaTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, QueryAcquireSchedulingLock); err != nil {
		return err
	}

	var maxPods int
	if err := tx.QueryRowContext(ctx, QueryGetMaxConcurrentPods).Scan(&maxPods); err != nil {
		return err
	}

	var activePods int
	if err := tx.QueryRowContext(ctx, QueryCountRunningAttempts).Scan(&activePods); err != nil {
		return err
	}

	if activePods >= maxPods {
		return ErrQuotaExceeded
	}
	return nil
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

	if t.Type == ReduceTask {
		rows, err := q.QueryContext(ctx, QueryGetReduceTaskInputs, t.ID)
		if err != nil {
			return err
		}
		defer rows.Close()

		var shuffleInputs []TaskOutputRef
		for rows.Next() {
			var input TaskOutputRef
			if err := rows.Scan(&input.PartitionIndex, &input.OutputURI, &input.Checksum); err != nil {
				return err
			}
			shuffleInputs = append(shuffleInputs, input)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		t.ShuffleInputs = shuffleInputs
		return nil
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

func (s *Scheduler) ScheduleJob(ctx context.Context, req ScheduleJobRequest) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := uuid.Parse(req.JobID); err != nil {
		return fmt.Errorf("invalid job id: %w", err)
	}
	if _, err := uuid.Parse(req.UserID); err != nil {
		return fmt.Errorf("invalid user id: %w", err)
	}
	if strings.TrimSpace(req.InputURI) == "" {
		return errors.New("input URI cannot be empty")
	}
	if strings.TrimSpace(req.MapperURI) == "" {
		return errors.New("mapper URI cannot be empty")
	}
	if strings.TrimSpace(req.ReducerURI) == "" {
		return errors.New("reducer URI cannot be empty")
	}
	if req.MTasks < 1 || req.RTasks < 1 {
		return errors.New("task counts must be strictly positive")
	}
	if len(req.Tasks) != req.MTasks+req.RTasks {
		return errors.New("tasks must contain exactly mTasks + rTasks entries")
	}
	var mapCount, reduceCount int
	for _, task := range req.Tasks {
		switch task.TaskType {
		case "Map":
			mapCount++
		case "Reduce":
			reduceCount++
		}
	}
	if mapCount != req.MTasks {
		return fmt.Errorf("expected %d Map tasks but got %d", req.MTasks, mapCount)
	}
	if reduceCount != req.RTasks {
		return fmt.Errorf("expected %d Reduce tasks but got %d", req.RTasks, reduceCount)
	}

	jobID, err := uuid.Parse(req.JobID)
	if err != nil {
		return err
	}
	userID, err := uuid.Parse(req.UserID)
	if err != nil {
		return err
	}
	now := time.Now()

	if _, err := tx.ExecContext(ctx, QueryInsertJob, jobID, userID, now, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, QueryInsertJobConfig,
		jobID,
		req.InputURI,
		req.MapperURI,
		req.ReducerURI,
		req.CombinerURI,
		req.MTasks,
		req.RTasks,
		req.InputChecksum,
	); err != nil {
		return err
	}

	expectedReplicaIndex, err := ComputeReplicaIndex(req.JobID, s.totalReplicas)
	if err != nil {
		return fmt.Errorf("failed to compute replica index: %w", err)
	}

	var scheduledTasks []string
	for _, task := range req.Tasks {
		taskID, err := uuid.Parse(task.TaskID)
		if err != nil {
			return err
		}
		if strings.TrimSpace(task.TaskType) == "" {
			return errors.New("task type cannot be empty")
		}
		if task.TaskType != "Map" && task.TaskType != "Reduce" {
			return fmt.Errorf("invalid task type %q: must be Map or Reduce", task.TaskType)
		}

		// Keep reduce partition semantics intact; only map tasks are normalized
		// to the manager replica selected for this job.
		if task.TaskType == "Map" {
			task.ReplicaIndex = expectedReplicaIndex
		}

		if _, err := tx.ExecContext(ctx, QueryInsertTask, taskID, jobID, task.TaskType, task.ReplicaIndex); err != nil {
			return err
		}
		scheduledTasks = append(scheduledTasks, taskID.String())

		for _, split := range task.InputSplits {
			if _, err := tx.ExecContext(ctx, QueryInsertTaskInput,
				taskID,
				split.InputURI,
				split.ByteStart,
				split.ByteEnd,
				split.SplitChecksum,
			); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Scheduled job %s with %d tasks; worker orchestration deferred to attempt assignment", jobID, len(scheduledTasks))

	return nil
}

func (s *Scheduler) UpsertSystemConfig(ctx context.Context, req SystemConfigUpdate) error {
	_, err := s.db.ExecContext(ctx, QueryUpsertSystemConfig, req.MaxConcurrentPods, req.CPULimit, req.MemoryLimit, time.Now())
	return err
}

func (s *Scheduler) updateJobStatusTx(ctx context.Context, tx *sql.Tx, jobID string, status string) error {
	_, err := tx.ExecContext(ctx, QueryUpdateJobStatus, jobID, status, time.Now())
	return err
}

func (s *Scheduler) validateLeaseTx(ctx context.Context, tx *sql.Tx, attemptID string, leaseID string) error {
	var leaseValid bool
	err := tx.QueryRowContext(ctx, QueryCheckLeaseValid, attemptID, leaseID).Scan(&leaseValid)
	if err != nil {
		return err
	}
	if !leaseValid {
		return ErrExpiredLease
	}
	return nil
}

func (s *Scheduler) CompleteTask(ctx context.Context, taskID string, attemptID string, leaseID string, outputURIs []string, outputChecksums []string) error {
	if len(outputURIs) != len(outputChecksums) {
		return ErrOutputMismatch
	}

	// Deep copy to prevent caller mutation from affecting internal db logic, fulfilling problem 3
	safeURIs := make([]string, len(outputURIs))
	copy(safeURIs, outputURIs)
	safeChecksums := make([]string, len(outputChecksums))
	copy(safeChecksums, outputChecksums)

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

	if err := s.validateLeaseTx(ctx, tx, attemptID, leaseID); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, QueryCompleteTask, taskID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, QuerySucceedAttempt, attemptID)
	if err != nil {
		return err
	}

	for i, uri := range safeURIs {
		_, err = tx.ExecContext(ctx, QueryInsertOutput, taskID, i, uri, safeChecksums[i])
		if err != nil {
			return err
		}
	}

	var jobID string
	if err := tx.QueryRowContext(ctx, QueryGetTaskJobID, taskID).Scan(&jobID); err != nil {
		return err
	}
	var pendingTasks int
	if err := tx.QueryRowContext(ctx, QueryCountAllPendingTasks, jobID).Scan(&pendingTasks); err != nil {
		return err
	}
	jobCompleted := false
	if pendingTasks == 0 {
		if err := s.updateJobStatusTx(ctx, tx, jobID, "Cleaning"); err != nil {
			return err
		}
		jobCompleted = true
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if jobCompleted {
		go func() {
			cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			defer cancel()
			s.finalizeJob(cancelCtx, jobID, "Completed")
		}()
	}

	return nil
}

func (s *Scheduler) RenewLease(ctx context.Context, taskID string, attemptID string, leaseID string) error {
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

	if err := s.validateLeaseTx(ctx, tx, attemptID, leaseID); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, QueryRenewLease, attemptID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Scheduler) CancelJob(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := s.updateJobStatusTx(ctx, tx, jobID, "Cleaning"); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "UPDATE TASKS SET status = 'Failed' WHERE job_id = $1 AND status != 'Completed'", jobID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, QueryFailRunningAttemptsByJob, jobID)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	go func() {
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		s.finalizeJob(cancelCtx, jobID, "Cancelled")
	}()

	return nil
}

func (s *Scheduler) FailTask(ctx context.Context, taskID string, attemptID string, leaseID string, reason string) error {
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

	if err := s.validateLeaseTx(ctx, tx, attemptID, leaseID); err != nil {
		return err
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

	_, err = tx.ExecContext(ctx, QueryFailAttempt, attemptID)
	if err != nil {
		return err
	}

	var jobID string
	if err := tx.QueryRowContext(ctx, QueryGetTaskJobID, taskID).Scan(&jobID); err != nil {
		return err
	}

	var retryAttemptID string
	if newState == "Failed" {
		if err := s.updateJobStatusTx(ctx, tx, jobID, "Cleaning"); err != nil {
			return err
		}
	} else {
		retryAttemptID, err = s.prepareRetryAttemptTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if newState == "Failed" {
		go func() {
			cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			defer cancel()
			s.finalizeJob(cancelCtx, jobID, "Failed")
		}()
	} else if newState == "Idle" {
		spawnCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err := s.orchestrator.SpawnWorker(spawnCtx, taskID, jobID, retryAttemptID, s.managerAddr); err != nil {
			log.Printf("Failed to respawn worker for failed task %s (attempt %s): %v", taskID, retryAttemptID, err)
		}
	}

	return nil
}

func (s *Scheduler) FailStaleTasks(ctx context.Context) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, QuerySelectStaleTasks)
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
	type retrySpawn struct {
		taskID    string
		attemptID string
		jobID     string
	}
	var respawnTasks []retrySpawn
	failedJobs := make(map[string]struct{})
	for _, rec := range stales {
		var jobID string
		if err := tx.QueryRowContext(ctx, QueryGetTaskJobID, rec.taskID).Scan(&jobID); err != nil {
			return 0, fmt.Errorf("loading job for task %s: %w", rec.taskID, err)
		}

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

		_, err = tx.ExecContext(ctx, QueryFailAttempt, rec.attemptID)
		if err != nil {
			return 0, fmt.Errorf("failing attempt %s: %w", rec.attemptID, err)
		}
		if newState == "Failed" {
			if err := s.updateJobStatusTx(ctx, tx, jobID, "Cleaning"); err != nil {
				return 0, fmt.Errorf("marking job %s failed: %w", jobID, err)
			}
			failedJobs[jobID] = struct{}{}
		} else if newState == "Idle" {
			retryAttemptID, err := s.prepareRetryAttemptTx(ctx, tx, rec.taskID)
			if err != nil {
				return 0, fmt.Errorf("creating retry attempt for task %s: %w", rec.taskID, err)
			}
			respawnTasks = append(respawnTasks, retrySpawn{taskID: rec.taskID, attemptID: retryAttemptID, jobID: jobID})
		}
		recoveredCount++
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit stale task cleanup: %v", err)
		return 0, err
	}

	for jobID := range failedJobs {
		finalizeCtx, finalizeCancel := context.WithTimeout(ctx, 2*time.Minute)
		s.finalizeJob(finalizeCtx, jobID, "Failed")
		finalizeCancel()
	}

	for _, rec := range respawnTasks {
		spawnCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := s.orchestrator.SpawnWorker(spawnCtx, rec.taskID, rec.jobID, rec.attemptID, s.managerAddr); err != nil {
			log.Printf("Failed to respawn worker for stale task %s (attempt %s): %v", rec.taskID, rec.attemptID, err)
		}
		cancel()
	}

	return recoveredCount, nil
}

func (s *Scheduler) GetMapOutputs(ctx context.Context, jobID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, QueryGetMapOutputs, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs []string
	for rows.Next() {
		var uri string
		if err := rows.Scan(&uri); err != nil {
			return nil, err
		}
		outputs = append(outputs, uri)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return outputs, nil
}

func (s *Scheduler) GetReduceOutputs(ctx context.Context, jobID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, QueryGetReduceOutputs, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs []string
	for rows.Next() {
		var uri string
		if err := rows.Scan(&uri); err != nil {
			return nil, err
		}
		outputs = append(outputs, uri)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return outputs, nil
}

func (s *Scheduler) AllMapTasksCompleted(ctx context.Context, jobID string) bool {
	var pending int
	err := s.db.QueryRowContext(ctx, QueryCountPendingMapTasks, jobID).Scan(&pending)
	return err == nil && pending == 0
}

func (s *Scheduler) IsJobFinished(ctx context.Context, jobID string) bool {
	var pending int
	err := s.db.QueryRowContext(ctx, QueryCountAllPendingTasks, jobID).Scan(&pending)
	return err == nil && pending == 0
}

func (s *Scheduler) GetTaskStatus(ctx context.Context, taskID string) (TaskState, error) {
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

func (s *Scheduler) GetTaskByID(ctx context.Context, taskID string) (*Task, error) {
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

	switch dbType {
	case "Map":
		t.Type = MapTask
	case "Reduce":
		t.Type = ReduceTask
		t.ReducePartition = t.ReplicaIndex
	default:
		return nil, fmt.Errorf("unknown task type %q for task %s", dbType, taskID)
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
	default:
		return nil, fmt.Errorf("unrecognized task status %q for task %s", dbStatus, taskID)
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

func (s *Scheduler) enqueueCleanup(jobID, terminalState string) {
	if strings.TrimSpace(jobID) == "" {
		return
	}
	s.cleanupMu.Lock()
	s.pendingCleanup[jobID] = terminalState
	s.cleanupMu.Unlock()
}

func (s *Scheduler) popPendingCleanup(limit int) map[string]string {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	if len(s.pendingCleanup) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(s.pendingCleanup) {
		limit = len(s.pendingCleanup)
	}
	jobs := make(map[string]string, limit)
	popped := 0
	for jobID, terminalState := range s.pendingCleanup {
		jobs[jobID] = terminalState
		delete(s.pendingCleanup, jobID)
		popped++
		if popped >= limit {
			break
		}
	}
	return jobs
}

func (s *Scheduler) finalizeJob(ctx context.Context, jobID, terminalState string) {
	if err := s.orchestrator.CancelJob(ctx, jobID); err != nil {
		log.Printf("Failed to cleanup K8s worker jobs for job %s: %v", jobID, err)
		s.enqueueCleanup(jobID, terminalState)
		return
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin tx for terminal status update of job %s: %v", jobID, err)
		s.enqueueCleanup(jobID, terminalState)
		return
	}
	defer tx.Rollback()

	if err := s.updateJobStatusTx(ctx, tx, jobID, terminalState); err != nil {
		log.Printf("Failed to update terminal status of job %s: %v", jobID, err)
		s.enqueueCleanup(jobID, terminalState)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit terminal status update of job %s: %v", jobID, err)
		s.enqueueCleanup(jobID, terminalState)
		return
	}
}

// StartCleanupReconciler retries failed Kubernetes worker cleanup requests.
// It should run for the process lifetime with a cancellable context.
func (s *Scheduler) StartCleanupReconciler(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}

	// Recovery: Deduce terminal state for jobs stuck in Cleaning
	rows, err := s.db.QueryContext(ctx, "SELECT job_id FROM JOBS WHERE status = 'Cleaning'")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var jobID string
			if err := rows.Scan(&jobID); err != nil {
				log.Printf("Warning: failed to scan Cleaning job row during recovery: %v", err)
				continue
			}
			terminalState, deriveErr := s.determineCleaningTerminalState(ctx, jobID)
			if deriveErr != nil {
				log.Printf("Warning: failed to determine terminal state for Cleaning job %s during recovery: %v", jobID, deriveErr)
				s.enqueueCleanup(jobID, "")
				continue
			}
			s.enqueueCleanup(jobID, terminalState)
		}
		if err := rows.Err(); err != nil {
			log.Printf("Warning: failed iterating Cleaning jobs during recovery: %v", err)
		}
	} else {
		log.Printf("Warning: failed to recover Cleaning jobs: %v", err)
	}

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				jobs := s.popPendingCleanup(cleanupReconcileBatchSize)
				if len(jobs) == 0 {
					continue
				}

				sem := make(chan struct{}, cleanupReconcileWorkers)
				var wg sync.WaitGroup
				for jobID, terminalState := range jobs {
					wg.Add(1)
					sem <- struct{}{}
					go func(jobID, terminalState string) {
						defer wg.Done()
						defer func() { <-sem }()

						retryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						if strings.TrimSpace(terminalState) == "" {
							var deriveErr error
							terminalState, deriveErr = s.determineCleaningTerminalState(retryCtx, jobID)
							if deriveErr != nil {
								log.Printf("Warning: retrying cleanup for job %s after terminal-state recovery error: %v", jobID, deriveErr)
								s.enqueueCleanup(jobID, "")
								return
							}
						}
						s.finalizeJob(retryCtx, jobID, terminalState)
					}(jobID, terminalState)
				}
				wg.Wait()
			}
		}
	}()
}

func (s *Scheduler) determineCleaningTerminalState(ctx context.Context, jobID string) (string, error) {
	var failedCount int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM TASKS WHERE job_id = $1 AND status = 'Failed'", jobID).Scan(&failedCount); err != nil {
		return "", fmt.Errorf("query failed task count for job %s: %w", jobID, err)
	}
	if failedCount > 0 {
		return "Failed", nil
	}

	var pending int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM TASKS WHERE job_id = $1 AND status != 'Completed'", jobID).Scan(&pending); err != nil {
		return "", fmt.Errorf("query pending task count for job %s: %w", jobID, err)
	}
	if pending > 0 {
		return "Failed", nil
	}
	return "Completed", nil
}
