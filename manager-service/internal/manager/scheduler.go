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
	stagingCleaner StagingCleaner
	managerAddr    string
	leaseTTL       int
	cleanupMu      sync.Mutex
	pendingCleanup map[string]struct{}
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
		pendingCleanup: make(map[string]struct{}),
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

func (s *Scheduler) GetNextTask(jobID string, workerID string) (*Task, error) {
	if jobID == "" {
		return nil, ErrEmptyJobID
	}
	if _, err := uuid.Parse(jobID); err != nil {
		return nil, fmt.Errorf("invalid job ID %q: %w", jobID, err)
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
	if err != nil {
		return nil, err
	}
	if failedCount > 0 {
		return nil, ErrJobFailed
	}

	// 3. Try to schedule Map tasks first
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

func (s *Scheduler) ScheduleJob(req ScheduleJobRequest) error {
	ctx := context.Background()
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

func (s *Scheduler) UpsertSystemConfig(req SystemConfigUpdate) error {
	ctx := context.Background()
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
		s.cleanStagingAndFinalizeJob(jobID, "Completed")
		go func() {
			cancelCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			s.tryCancelJob(cancelCtx, jobID, "completed")
		}()
	}

	return nil
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

	if err := s.validateLeaseTx(ctx, tx, attemptID, leaseID); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, QueryRenewLease, attemptID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Scheduler) CancelJob(jobID string) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := s.updateJobStatusTx(ctx, tx, jobID, "Cancelled"); err != nil {
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
		cancelCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		s.tryCancelJob(cancelCtx, jobID, "cancelled")
	}()

	return nil
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
		s.cleanStagingAndFinalizeJob(jobID, "Failed")
		go func() {
			cancelCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			s.tryCancelJob(cancelCtx, jobID, "failed")
		}()
	} else if newState == "Idle" {
		spawnCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := s.orchestrator.SpawnWorker(spawnCtx, taskID, jobID, retryAttemptID, s.managerAddr); err != nil {
			log.Printf("Failed to respawn worker for failed task %s (attempt %s): %v", taskID, retryAttemptID, err)
		}
	}

	return nil
}

func (s *Scheduler) FailStaleTasks() (int, error) {
	ctx := context.Background()
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
	type zombieWorker struct {
		taskID    string
		attemptID string
	}
	var respawnTasks []retrySpawn
	var zombies []zombieWorker
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
		zombies = append(zombies, zombieWorker{taskID: rec.taskID, attemptID: rec.attemptID})
		if newState == "Failed" {
			if _, alreadyCleaning := failedJobs[jobID]; !alreadyCleaning {
				if err := s.updateJobStatusTx(ctx, tx, jobID, "Cleaning"); err != nil {
					return 0, fmt.Errorf("marking job %s Cleaning: %w", jobID, err)
				}
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

	// Reap zombie worker pods/jobs for each stale attempt. The DDS row is already
	// authoritative (attempt marked Failed, task reset or failed), so any lingering
	// pod's RPCs would be rejected by the fencing check; deleting the K8s Job here
	// prevents zombie compute from consuming cluster resources. Deletes are best
	// effort and idempotent (already-gone jobs are treated as success).
	reapCtx, reapCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer reapCancel()
	for _, z := range zombies {
		if err := s.orchestrator.DeleteWorker(reapCtx, z.taskID, z.attemptID); err != nil {
			log.Printf("Failed to delete zombie worker for task %s (attempt %s): %v", z.taskID, z.attemptID, err)
		}
	}

	// Delete staging data and finalize each failed job through the Cleaning phase
	// before marking it Failed. This is done before K8s cancellation so that
	// any lingering workers RPCs will be rejected by fencing.
	for jobID := range failedJobs {
		s.cleanStagingAndFinalizeJob(jobID, "Failed")
	}

	cancelCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for jobID := range failedJobs {
		s.tryCancelJob(cancelCtx, jobID, "failed")
	}

	for _, rec := range respawnTasks {
		spawnCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := s.orchestrator.SpawnWorker(spawnCtx, rec.taskID, rec.jobID, rec.attemptID, s.managerAddr); err != nil {
			log.Printf("Failed to respawn worker for stale task %s (attempt %s): %v", rec.taskID, rec.attemptID, err)
		}
		cancel()
	}

	return recoveredCount, nil
}

func (s *Scheduler) GetMapOutputs(jobID string) ([]string, error) {
	ctx := context.Background()
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

func (s *Scheduler) GetReduceOutputs(jobID string) ([]string, error) {
	ctx := context.Background()
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

func (s *Scheduler) enqueueCleanup(jobID string) {
	if strings.TrimSpace(jobID) == "" {
		return
	}
	s.cleanupMu.Lock()
	s.pendingCleanup[jobID] = struct{}{}
	s.cleanupMu.Unlock()
}

// SetStagingCleaner registers a StagingCleaner implementation that will be
// invoked during the Cleaning phase of a job before it is transitioned to
// a terminal state (Completed or Failed). Calling this is optional; when not
// set the Cleaning phase still transitions correctly but no staging objects
// are removed (relying on the bucket lifecycle policy instead).
func (s *Scheduler) SetStagingCleaner(c StagingCleaner) {
	s.stagingCleaner = c
}

// cleanStagingAndFinalizeJob runs the Cleaning phase for a job: it bulk-deletes
// the shuffle staging prefix (staging/<jobID>/) then marks the job with the
// given terminal status. If staging deletion fails the error is logged and
// finalization continues to prevent the job from being stuck in Cleaning.
func (s *Scheduler) cleanStagingAndFinalizeJob(jobID, terminalStatus string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if s.stagingCleaner != nil {
		if err := s.stagingCleaner.DeleteStagingPrefix(ctx, jobID); err != nil {
			log.Printf("staging cleanup failed for job %s (finalizing as %s anyway): %v", jobID, terminalStatus, err)
		}
	}

	if _, err := s.db.ExecContext(ctx, QueryUpdateJobStatus, jobID, terminalStatus, time.Now()); err != nil {
		log.Printf("failed to finalize job %s as %s: %v", jobID, terminalStatus, err)
	}
}

func (s *Scheduler) popPendingCleanup() []string {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	if len(s.pendingCleanup) == 0 {
		return nil
	}
	jobs := make([]string, 0, len(s.pendingCleanup))
	for jobID := range s.pendingCleanup {
		jobs = append(jobs, jobID)
		delete(s.pendingCleanup, jobID)
	}
	return jobs
}

func (s *Scheduler) tryCancelJob(ctx context.Context, jobID, reason string) {
	if err := s.orchestrator.CancelJob(ctx, jobID); err != nil {
		log.Printf("Failed to cleanup K8s worker jobs for %s job %s: %v", reason, jobID, err)
		s.enqueueCleanup(jobID)
	}
}

// StartCleanupReconciler retries failed Kubernetes worker cleanup requests.
// It should run for the process lifetime with a cancellable context.
func (s *Scheduler) StartCleanupReconciler(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, jobID := range s.popPendingCleanup() {
					retryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					s.tryCancelJob(retryCtx, jobID, "retry")
					cancel()
				}
			}
		}
	}()
}
