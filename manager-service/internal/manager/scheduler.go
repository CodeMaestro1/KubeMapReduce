package manager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"kubemapreduce/manager-service/pkg/observability"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MaxTaskAttempts defines the maximum number of times a single task will be retried
// before the entire job is marked as Failed.
const MaxTaskAttempts = 3

// DefaultMaxConcurrentPods is the fallback pod concurrency ceiling used when the
// SYSTEM_CONFIG table has no seed row (e.g. a fresh cluster). It is intentionally
// a single authoritative constant so that every subsystem that needs this default
// (quota enforcement, GetSystemConfig) references the same value.
const DefaultMaxConcurrentPods = 10

const (
	cleanupReconcileWorkers   = 4
	cleanupReconcileBatchSize = 64
)

var (
	ErrNoIdleTasks            = errors.New("no idle tasks available right now, please wait")
	ErrJobCompleted           = errors.New("all tasks completed, job is done")
	ErrTaskNotFound           = errors.New("task not found")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrJobCancelling          = errors.New("job is being cancelled; worker output discarded")
	ErrEmptyJobID             = errors.New("jobID cannot be empty")
	ErrEmptyWorkerID          = errors.New("workerID cannot be empty")
	ErrStaleAttempt           = errors.New("stale commit attempt rejected to prevent split-brain")
	ErrExpiredLease           = errors.New("lease expired or mismatched")
	ErrOutputMismatch         = errors.New("outputURIs and outputChecksums must have the same length")
	ErrJobFailed              = errors.New("job failed: one or more tasks reached maximum attempts")
	ErrQuotaExceeded          = errors.New("global worker pod quota exceeded")
)

type TaskDispatcher interface {
	DispatchTask(ctx context.Context, task *Task) error
}

// Scheduler manages the lifecycle of MapReduce jobs and their constituent tasks.
//
// It is the central authority for task assignment, state transitions, and lease-based
// fencing. The Scheduler uses a Distributed Data Store (DDS) to maintain consistency
// across multiple Manager replicas.
type Scheduler struct {
	db                    *sql.DB
	replicaIndex          int
	totalReplicas         int
	orchestrator          WorkerOrchestrator
	dispatcher            TaskDispatcher
	managerAddr           string
	leaseTTL              int
	leaseClockSkewSeconds int
	staging               StagingCleaner
	cleanupMu             sync.Mutex
	pendingCleanup        map[string]string
	heartbeats            sync.Map
}

type taskMetadataQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// NewScheduler initializes a new Scheduler instance.
//
// Callers must provide a valid *sql.DB connection and an implementation of WorkerOrchestrator.
// The leaseTTL (in seconds) determines how long a worker can go without a heartbeat before
// being considered stale by the Active Reaper. staging may be nil to disable MinIO cleanup.
func NewScheduler(db *sql.DB, replicaIndex int, totalReplicas int, orchestrator WorkerOrchestrator, dispatcher TaskDispatcher, managerAddr string, leaseTTL int, staging StagingCleaner) (*Scheduler, error) {
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}
	if totalReplicas <= 0 {
		totalReplicas = 1
	}
	if orchestrator == nil {
		return nil, errors.New("orchestrator cannot be nil")
	}
	if dispatcher == nil {
		return nil, errors.New("dispatcher cannot be nil")
	}
	if leaseTTL <= 0 {
		leaseTTL = 30
	}
	return &Scheduler{
		db:                    db,
		replicaIndex:          replicaIndex,
		totalReplicas:         totalReplicas,
		orchestrator:          orchestrator,
		dispatcher:            dispatcher,
		managerAddr:           managerAddr,
		leaseTTL:              leaseTTL,
		leaseClockSkewSeconds: 5,
		staging:               staging,
		pendingCleanup:        make(map[string]string),
		heartbeats:            sync.Map{},
	}, nil
}

// Recover reconciles active attempts assigned to this replica and re-spawns workers.
//
// This method should be called immediately after Manager startup. it queries the DDS
// authoritativeManagerAddr returns the gRPC address of the replica that owns jobID.
// Workers must connect to that replica — any other replica will reject GetNextTask
// with a routing mismatch error. If the address cannot be computed (e.g. totalReplicas
// is 0), this manager's own address is returned as a safe fallback.
func (s *Scheduler) authoritativeManagerAddr(jobID string) string {
	idx, err := ComputeReplicaIndex(jobID, s.totalReplicas)
	if err != nil || idx == s.replicaIndex {
		return s.managerAddr
	}
	// Rewrite the ordinal in the stable DNS name:
	// "manager-0.manager-headless...." → "manager-N.manager-headless...."
	old := fmt.Sprintf("manager-%d.", s.replicaIndex)
	replacement := fmt.Sprintf("manager-%d.", idx)
	return strings.Replace(s.managerAddr, old, replacement, 1)
}

// for tasks that are "In-Progress" and bound to this replica index, then uses the
// orchestrator to ensure a physical worker process exists for each. This ensures
// continuity across Manager crashes without losing progress.
func (s *Scheduler) Recover(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, QuerySelectRecoverableAttempts, s.replicaIndex)
	if err != nil {
		return fmt.Errorf("failed to query recoverable attempts: %w", err)
	}
	defer rows.Close()

	type recoverableAttempt struct {
		taskID        string
		attemptID     string
		jobID         string
		lastRenewedAt time.Time
	}
	var attemptsToSpawn []recoverableAttempt
	for rows.Next() {
		var rec recoverableAttempt
		if err := rows.Scan(&rec.taskID, &rec.attemptID, &rec.jobID, &rec.lastRenewedAt); err != nil {
			return fmt.Errorf("failed to scan recoverable attempt during recovery: %w", err)
		}
		attemptsToSpawn = append(attemptsToSpawn, rec)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row error during recovery: %w", err)
	}

	// Warm the heartbeat map from DB so the reaper does not immediately evict
	// tasks that were alive before this Manager restart.
	for _, rec := range attemptsToSpawn {
		s.heartbeats.Store(rec.attemptID, rec.lastRenewedAt)
	}

	const recoverSpawnTimeout = 20 * time.Second

	// Deduplicate job IDs for worker pool ensuring
	uniqueJobs := make(map[string]struct{})
	for _, rec := range attemptsToSpawn {
		uniqueJobs[rec.jobID] = struct{}{}
	}

	spawnFailures := 0
	for jobID := range uniqueJobs {
		spawnCtx, cancel := context.WithTimeout(ctx, recoverSpawnTimeout)
		err := s.orchestrator.EnsureWorkerPool(spawnCtx, jobID, 4, s.managerAddr)
		cancel()
		if err != nil {
			slog.ErrorContext(ctx, "failed to ensure worker pool during recovery",
				slog.String("job_id", jobID),
				slog.Any("err", err),
			)
			spawnFailures++
		}
	}

	// Also need to re-populate the task queues for the WorkerServer (dispatcher)
	// so workers can pull them.
	for _, rec := range attemptsToSpawn {
		spawnCtx, cancel := context.WithTimeout(ctx, recoverSpawnTimeout)
		task, err := s.GetTaskByID(spawnCtx, rec.taskID)
		if err == nil {
			err = s.dispatcher.DispatchTask(spawnCtx, task)
		}
		cancel()
		if err != nil {
			slog.ErrorContext(ctx, "failed to redispatch recovered task",
				slog.String("task_id", rec.taskID),
				slog.Any("err", err),
			)
		}
	}
	if spawnFailures > 0 {
		slog.WarnContext(ctx, "worker recovery completed with spawn failures; reaper will retry",
			slog.Int("spawn_failures", spawnFailures),
			slog.Int("total_attempts", len(attemptsToSpawn)),
		)
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

// GetNextTask atomically selects and assigns the next schedulable task for a job.
//
// It enforces phase sequencing: all Map tasks must complete before any Reduce tasks
// are scheduled. It also performs Resource Quota Enforcement by checking against the
// global max_concurrent_pods limit before assigning a new task.
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

	// 1b. Enforce Authoritative Routing
	var assignedReplica int
	err = tx.QueryRowContext(ctx, "SELECT replica_index FROM JOBS WHERE job_id = $1", jobID).Scan(&assignedReplica)
	if err != nil {
		return nil, err
	}
	if assignedReplica != s.replicaIndex {
		return nil, fmt.Errorf("job assigned to replica %d, but this is replica %d", assignedReplica, s.replicaIndex)
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

	// 3.5. Auto-complete Reduce tasks for empty partitions before any
	// dispatch. Saves a worker pod + image pull + reducer artifact download
	// + temp-dir churn per empty partition. Idempotent: only Idle rows
	// match, so subsequent calls become no-ops once the map phase output
	// distribution has stabilised.
	autoCompletedJobNowDone, err := s.autoCompleteEmptyReducesTx(ctx, tx, jobID)
	if err != nil {
		return nil, err
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
		// No idle work right now, but the auto-complete pass may have
		// transitioned tasks; commit so those updates persist.
		if errCommit := tx.Commit(); errCommit != nil {
			return nil, errCommit
		}
		if autoCompletedJobNowDone {
			s.enqueueCleanup(jobID, "Completed")
		}
		return nil, ErrNoIdleTasks
	}

	if errCommit := tx.Commit(); errCommit != nil {
		return nil, errCommit
	}
	if autoCompletedJobNowDone {
		s.enqueueCleanup(jobID, "Completed")
	}
	return nil, ErrJobCompleted
}

// autoCompleteEmptyReducesTx marks Idle Reduce tasks with no shuffle inputs
// as Completed inside the supplied transaction. Returns whether the job
// reached zero pending tasks as a result, so the caller can enqueue cleanup
// once the transaction commits.
func (s *Scheduler) autoCompleteEmptyReducesTx(ctx context.Context, tx *sql.Tx, jobID string) (jobNowDone bool, err error) {
	rows, err := tx.QueryContext(ctx, QueryCompleteEmptyReduceTasks, jobID)
	if err != nil {
		return false, fmt.Errorf("auto-complete empty reduces: %w", err)
	}
	defer rows.Close()

	completed := 0
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			return false, err
		}
		completed++
		slog.InfoContext(ctx, "auto-completed empty reduce partition",
			slog.String("task_id", taskID),
			slog.String("job_id", jobID),
		)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if completed == 0 {
		return false, nil
	}

	if m := observability.DefaultMetrics(); m != nil {
		for i := 0; i < completed; i++ {
			m.TasksCompleted.Inc()
		}
	}

	var pending int
	if err := tx.QueryRowContext(ctx, QueryCountAllPendingTasks, jobID).Scan(&pending); err != nil {
		return false, err
	}
	if pending == 0 {
		if err := s.updateJobStatusTx(ctx, tx, jobID, "Cleaning"); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
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
	// NOTE: These are in-memory values for the caller's convenience (gRPC response).
	// The DB-authoritative timestamps are set via NOW() in QueryInsertAttempt.
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

	s.heartbeats.Store(attemptID.String(), time.Now())

	return &t, nil
}

// QuotaSnapshot is the consistent view of cluster-wide pod accounting captured
// inside the scheduling transaction's advisory-lock critical section.
//
// MaxPods is the operator-configured ceiling from SYSTEM_CONFIG.max_concurrent_pods
// and ActivePods is the count of TASK_ATTEMPTS whose status is 'Running' at the
// moment the lock was held. Available is provided as a convenience and equals
// max(0, MaxPods-ActivePods).
type QuotaSnapshot struct {
	MaxPods    int
	ActivePods int
	Available  int
}

// readQuotaSnapshotTx reads the cluster-wide pod accounting under the
// transaction-scoped advisory lock. It is the single source of truth for both
// quota enforcement and observability.
func (s *Scheduler) readQuotaSnapshotTx(ctx context.Context, tx *sql.Tx) (QuotaSnapshot, error) {
	if _, err := tx.ExecContext(ctx, QueryAcquireSchedulingLock); err != nil {
		return QuotaSnapshot{}, err
	}

	var snap QuotaSnapshot
	if err := tx.QueryRowContext(ctx, QueryGetMaxConcurrentPods).Scan(&snap.MaxPods); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			snap.MaxPods = DefaultMaxConcurrentPods
		} else {
			return QuotaSnapshot{}, err
		}
	}
	if err := tx.QueryRowContext(ctx, QueryCountRunningAttempts).Scan(&snap.ActivePods); err != nil {
		return QuotaSnapshot{}, err
	}
	if snap.MaxPods > snap.ActivePods {
		snap.Available = snap.MaxPods - snap.ActivePods
	}
	return snap, nil
}

func (s *Scheduler) enforceQuotaTx(ctx context.Context, tx *sql.Tx) error {
	snap, err := s.readQuotaSnapshotTx(ctx, tx)
	if err != nil {
		return err
	}
	if snap.Available <= 0 {
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

// ScheduleJob initializes a new MapReduce job in the DDS.
//
// It validates the job request, persists the job configuration, and creates the
// set of "Idle" tasks ready for assignment. This method ensures that all tasks for
// a specific job are assigned to a consistent Manager replica based on the jobID hash.
// ValidateScheduleJobRequest performs pre-flight validation on the incoming scheduling request.
func ValidateScheduleJobRequest(req ScheduleJobRequest) error {
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

	return nil
}

// ExtractMapTaskIDs filters a list of ScheduleTasks and returns the task IDs for those typed as "Map".
func ExtractMapTaskIDs(tasks []ScheduleTask) []string {
	var mapTaskIDs []string
	for _, task := range tasks {
		if task.TaskType == "Map" {
			mapTaskIDs = append(mapTaskIDs, task.TaskID)
		}
	}
	return mapTaskIDs
}

func (s *Scheduler) ScheduleJob(ctx context.Context, req ScheduleJobRequest) error {
	if err := ValidateScheduleJobRequest(req); err != nil {
		return err
	}

	jobID, _ := uuid.Parse(req.JobID)
	userID, _ := uuid.Parse(req.UserID)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	expectedReplicaIndex, err := ComputeReplicaIndex(req.JobID, s.totalReplicas)
	if err != nil {
		return fmt.Errorf("failed to compute replica index: %w", err)
	}

	if _, err := tx.ExecContext(ctx, QueryInsertJob, jobID, userID, expectedReplicaIndex); err != nil {
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
		if m := observability.DefaultMetrics(); m != nil {
			m.TasksScheduled.WithLabelValues(task.TaskType).Inc()
		}

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

	// After tx.Commit(), ensure worker pool is running.
	// We use the worker_replicas from SYSTEM_CONFIG (default 1) or a reasonable pool size.
	// For simplicity, let's fetch worker count from orchestrator/config provider.
	// Using 4 as a default pool size per job for now.
	if err := s.orchestrator.EnsureWorkerPool(ctx, req.JobID, 4, s.authoritativeManagerAddr(req.JobID)); err != nil {
		slog.ErrorContext(ctx, "failed to ensure worker pool",
			slog.String("job_id", req.JobID), slog.Any("err", err))
		// Note: we don't return error here because the job IS scheduled in DB.
		// The reaper or subsequent calls will retry EnsureWorkerPool.
	}

	return nil
}

// GetSystemConfig retrieves the current cluster configuration.
func (s *Scheduler) GetSystemConfig(ctx context.Context) (SystemConfigUpdate, error) {
	var cfg SystemConfigUpdate
	err := s.db.QueryRowContext(ctx, QueryGetSystemConfig).Scan(
		&cfg.MaxConcurrentPods,
		&cfg.CPULimit,
		&cfg.MemoryLimit,
		&cfg.WorkerReplicas,
		&cfg.MaxJobsPerNode,
		&cfg.LocalityKey,
		&cfg.LocalityLabelSelector,
	)
	if err == sql.ErrNoRows {
		// Return defaults if not configured
		return SystemConfigUpdate{
			MaxConcurrentPods:     DefaultMaxConcurrentPods,
			CPULimit:              "500m",
			MemoryLimit:           "1Gi",
			WorkerReplicas:        1,
			MaxJobsPerNode:        1,
			LocalityKey:           "topology.kubernetes.io/zone",
			LocalityLabelSelector: "app.kubernetes.io/name=minio",
		}, nil
	}
	return cfg, err
}

// UpsertSystemConfig updates cluster-wide operational parameters.
func (s *Scheduler) UpsertSystemConfig(ctx context.Context, req SystemConfigUpdate) error {
	_, err := s.db.ExecContext(ctx, QueryUpsertSystemConfig,
		req.MaxConcurrentPods,
		req.CPULimit,
		req.MemoryLimit,
		req.WorkerReplicas,
		req.MaxJobsPerNode,
		req.LocalityKey,
		req.LocalityLabelSelector,
	)
	return err
}

func (s *Scheduler) updateJobStatusTx(ctx context.Context, tx *sql.Tx, jobID string, status string) error {
	_, err := tx.ExecContext(ctx, QueryUpdateJobStatus, jobID, status)
	return err
}

// applyJobTransitionTx reads the current job status with FOR UPDATE, validates the
// transition via ValidateJobTransition, and applies QueryUpdateJobStatus.
// If the current status already equals to, it returns nil (idempotent no-op).
func (s *Scheduler) applyJobTransitionTx(ctx context.Context, tx *sql.Tx, jobID string, to string) error {
	var current string
	err := tx.QueryRowContext(ctx, QueryGetJobStatusForUpdate, jobID).Scan(&current)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskNotFound
		}
		return err
	}
	if current == to {
		return nil
	}
	if err := ValidateJobTransition(current, to); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, QueryUpdateJobStatus, jobID, to)
	return err
}

func (s *Scheduler) validateLeaseTx(ctx context.Context, tx *sql.Tx, attemptID string, leaseID string) error {
	var leaseValid bool
	err := tx.QueryRowContext(ctx, QueryCheckLeaseValid, attemptID, leaseID, s.leaseClockSkewSeconds).Scan(&leaseValid)
	if err != nil {
		return err
	}
	if !leaseValid {
		return ErrExpiredLease
	}
	return nil
}

// CompleteTask transitions a task to the "Completed" state and persists its output references.
//
// This is a fenced operation: it validates that the attemptID and leaseID are still valid
// before committing. This prevents a "zombie" worker (one whose lease has expired) from
// overwriting the work of a newer attempt.
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

	// Lock the parent job row early to prevent split-brain race conditions.
	// Two concurrent CompleteTask/FailTask calls for tasks in the same job
	// must be serialized to avoid both committing terminal transitions.
	// This intentionally trades some terminal-path throughput for correctness.
	var lockedJobID string
	var jobStatus string
	if err := tx.QueryRowContext(ctx, "SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j ON t.job_id = j.job_id WHERE t.task_id = $1 FOR UPDATE OF j", taskID).Scan(&lockedJobID, &jobStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskNotFound
		}
		return err
	}
	if jobStatus == "Cleaning" {
		// Job is being cancelled. Discard this output; staging cleanup handles MinIO objects.
		return ErrJobCancelling
	}
	if jobStatus != "Running" && jobStatus != "Pending" {
		return ErrInvalidStateTransition
	}

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

	if len(safeURIs) > 0 {
		placeholders := make([]string, 0, len(safeURIs))
		args := make([]any, 0, len(safeURIs)*4)
		for i, uri := range safeURIs {
			base := i * 4
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4))
			args = append(args, taskID, i, uri, safeChecksums[i])
		}
		query := QueryInsertOutputBulkBase + strings.Join(placeholders, ", ")
		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
	}

	jobID := lockedJobID
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

	s.heartbeats.Delete(attemptID)

	if m := observability.DefaultMetrics(); m != nil {
		m.TasksCompleted.Inc()
	}

	if jobCompleted {
		s.enqueueCleanup(jobID, "Completed")
	}

	return nil
}

// RenewLease extends the validity window of a task attempt.
//
// Workers must call this periodically (via Heartbeat) to signal they are still alive.
// Failure to renew the lease allows the Active Reaper to reclaim the task for a new attempt.
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

	if _, err := tx.ExecContext(ctx, QueryRenewLease, attemptID); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	s.heartbeats.Store(attemptID, time.Now())
	return nil
}

// CancelJob marks a job as "Cancelled" and terminates all associated worker processes.
func (s *Scheduler) CancelJob(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := s.applyJobTransitionTx(ctx, tx, jobID, "Cleaning"); err != nil {
		if errors.Is(err, ErrForbiddenTransition) {
			// Job already in a terminal state — idempotent cancel is a no-op.
			return nil
		}
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

	s.enqueueCleanup(jobID, "Cancelled")

	return nil
}

// FailTask marks a task attempt as "Failed" and decides whether to retry or abort.
//
// If the task has not exceeded MaxTaskAttempts, it is returned to the "Idle" state for
// another worker to pick up. Otherwise, the entire job is transitioned to "Failed".
func (s *Scheduler) FailTask(ctx context.Context, taskID string, attemptID string, leaseID string, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Lock the parent job row early to prevent split-brain race conditions.
	var lockedJobID string
	var jobStatus string
	if err := tx.QueryRowContext(ctx, "SELECT t.job_id, j.status FROM TASKS t JOIN JOBS j ON t.job_id = j.job_id WHERE t.task_id = $1 FOR UPDATE OF j", taskID).Scan(&lockedJobID, &jobStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTaskNotFound
		}
		return err
	}
	if jobStatus != "Running" && jobStatus != "Pending" {
		return ErrInvalidStateTransition
	}

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

	jobID := lockedJobID

	if newState == "Failed" {
		if err := s.updateJobStatusTx(ctx, tx, jobID, "Cleaning"); err != nil {
			return err
		}
	} else {
		_, err = s.prepareRetryAttemptTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	s.heartbeats.Delete(attemptID)

	if m := observability.DefaultMetrics(); m != nil {
		m.TasksFailed.Inc()
	}

	if newState == "Failed" {
		s.enqueueCleanup(jobID, "Failed")
	} else if newState == "Idle" {
		spawnCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		// Ensure worker pool is healthy (idempotent)
		if err := s.orchestrator.EnsureWorkerPool(spawnCtx, jobID, 4, s.managerAddr); err != nil {
			slog.WarnContext(ctx, "failed to ensure worker pool after task failure; reaper will retry",
				slog.String("job_id", jobID),
				slog.Any("err", err),
			)
		}

		// Redispatch for pull-based workers
		task, err := s.GetTaskByID(spawnCtx, taskID)
		if err == nil {
			err = s.dispatcher.DispatchTask(spawnCtx, task)
		}

		if err != nil {
			slog.ErrorContext(ctx, "failed to redispatch task after failure",
				slog.String("task_id", taskID),
				slog.String("job_id", jobID),
				slog.Any("err", err),
			)
		}
	}

	return nil
}

// FailStaleTasks identifies and reclaims tasks whose worker leases have expired.
//
// This is the implementation of the "Active Reaper" (Section 5.1). It runs as a background
// process, scanning for "In-Progress" tasks that haven't sent a heartbeat within the
// leaseTTL window. Stale tasks are either returned to "Idle" or marked as "Failed".
func (s *Scheduler) FailStaleTasks(ctx context.Context) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, QuerySelectStaleTasks, s.replicaIndex)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type staleRec struct {
		taskID       string
		attemptID    string
		jobID        string
		attemptCount int
	}
	var stales []staleRec
	for rows.Next() {
		var t, a, j string
		var c int
		if err := rows.Scan(&t, &a, &j, &c); err != nil {
			return 0, fmt.Errorf("scanning stale task row: %w", err)
		}
		stales = append(stales, staleRec{t, a, j, c})
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
	var failedTaskIDs []string
	failedJobs := make(map[string]struct{})
	for _, rec := range stales {
		// IN-MEMORY FILTER: skip if heartbeat is fresh
		if val, ok := s.heartbeats.Load(rec.attemptID); ok {
			if last, ok := val.(time.Time); ok {
				if time.Since(last) < time.Duration(s.leaseTTL)*time.Second {
					continue
				}
			}
		}

		newState := "Idle"
		if rec.attemptCount >= MaxTaskAttempts {
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
			if err := s.updateJobStatusTx(ctx, tx, rec.jobID, "Cleaning"); err != nil {
				return 0, fmt.Errorf("marking job %s failed: %w", rec.jobID, err)
			}
			failedTaskIDs = append(failedTaskIDs, rec.taskID)
			failedJobs[rec.jobID] = struct{}{}
		} else if newState == "Idle" {
			retryAttemptID, err := s.prepareRetryAttemptTx(ctx, tx, rec.taskID)
			if err != nil {
				return 0, fmt.Errorf("creating retry attempt for task %s: %w", rec.taskID, err)
			}
			respawnTasks = append(respawnTasks, retrySpawn{taskID: rec.taskID, attemptID: retryAttemptID, jobID: rec.jobID})
		}
		recoveredCount++
	}

	if err := tx.Commit(); err != nil {
		slog.ErrorContext(ctx, "failed to commit stale task cleanup", slog.Any("err", err))
		return 0, err
	}

	for jobID := range failedJobs {
		s.enqueueCleanup(jobID, "Failed")
	}

	// Ensure worker pool for jobs that have tasks to respawn
	uniqueRespawnJobs := make(map[string]struct{})
	for _, rec := range respawnTasks {
		uniqueRespawnJobs[rec.jobID] = struct{}{}
	}
	for jobID := range uniqueRespawnJobs {
		spawnCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := s.orchestrator.EnsureWorkerPool(spawnCtx, jobID, 4, s.managerAddr); err != nil {
			slog.ErrorContext(ctx, "failed to ensure worker pool during reaper retry",
				slog.String("job_id", jobID),
				slog.Any("err", err),
			)
		}
		cancel()
	}

	for _, rec := range respawnTasks {
		spawnCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		// Hydrate and Dispatch
		task, err := s.GetTaskByID(spawnCtx, rec.taskID)
		if err == nil {
			err = s.dispatcher.DispatchTask(spawnCtx, task)
		}

		if err != nil {
			slog.ErrorContext(ctx, "failed to redispatch stale task",
				slog.String("task_id", rec.taskID),
				slog.String("job_id", rec.jobID),
				slog.String("attempt_id", rec.attemptID),
				slog.Any("err", err),
			)
		}
		cancel()
	}

	return recoveredCount, nil
}

// GetMapOutputs retrieves output URIs from all successfully completed Map tasks for a job.
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

// GetReduceOutputs retrieves output URIs from all successfully completed Reduce tasks for a job.
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

// AllMapTasksCompleted returns true if every Map task associated with the job is in the "Completed" state.
func (s *Scheduler) AllMapTasksCompleted(ctx context.Context, jobID string) bool {
	var pending int
	err := s.db.QueryRowContext(ctx, QueryCountPendingMapTasks, jobID).Scan(&pending)
	return err == nil && pending == 0
}

// IsJobFinished returns true if all tasks (Map and Reduce) for a job have reached the "Completed" state.
func (s *Scheduler) IsJobFinished(ctx context.Context, jobID string) bool {
	var pending int
	err := s.db.QueryRowContext(ctx, QueryCountAllPendingTasks, jobID).Scan(&pending)
	return err == nil && pending == 0
}

// GetTaskStatus retrieves the current state of a specific task.
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

// GetTaskByID retrieves full metadata for a task, including its current attempt details if active.
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
	// Stage 1: Delete staging objects first (cleanup work independent of DB state).
	if s.staging != nil {
		if err := s.staging.DeleteStagingObjects(ctx, jobID); err != nil {
			slog.ErrorContext(ctx, "failed to delete staging objects",
				slog.String("job_id", jobID),
				slog.String("terminal_state", terminalState),
				slog.Any("err", err),
			)
			s.enqueueCleanup(jobID, terminalState)
			return
		}
	}

	// Stage 2: Atomically update DB status (commit this first to prevent split-brain).
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.ErrorContext(ctx, "failed to begin tx for terminal status update",
			slog.String("job_id", jobID),
			slog.Any("err", err),
		)
		s.enqueueCleanup(jobID, terminalState)
		return
	}
	defer tx.Rollback()

	if err := s.applyJobTransitionTx(ctx, tx, jobID, terminalState); err != nil {
		slog.ErrorContext(ctx, "failed to update terminal status",
			slog.String("job_id", jobID),
			slog.String("terminal_state", terminalState),
			slog.Any("err", err),
		)
		s.enqueueCleanup(jobID, terminalState)
		return
	}
	if err := tx.Commit(); err != nil {
		slog.ErrorContext(ctx, "failed to commit terminal status update",
			slog.String("job_id", jobID),
			slog.Any("err", err),
		)
		s.enqueueCleanup(jobID, terminalState)
		return
	}

	// Stage 3: Delete Kubernetes worker jobs (now that DB state is committed).
	// If this fails, pods still exist but DB is terminal; enqueue for cleanup reconciliation.
	if err := s.orchestrator.CancelJob(ctx, jobID); err != nil {
		slog.ErrorContext(ctx, "failed to cleanup K8s worker jobs",
			slog.String("job_id", jobID),
			slog.String("terminal_state", terminalState),
			slog.Any("err", err),
		)
		s.enqueueCleanup(jobID, terminalState)
		return
	}
}

// StartCleanupReconciler retries failed Kubernetes worker cleanup requests.
//
// It should run for the process lifetime with a cancellable context. It periodically
// pops failed cleanup requests from an internal queue and retries the finalizeJob flow.
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
				slog.WarnContext(ctx, "failed to scan Cleaning job row during recovery", slog.Any("err", err))
				continue
			}
			terminalState, deriveErr := s.determineCleaningTerminalState(ctx, jobID)
			if deriveErr != nil {
				slog.WarnContext(ctx, "failed to determine terminal state for Cleaning job during recovery",
					slog.String("job_id", jobID),
					slog.Any("err", deriveErr),
				)
				s.enqueueCleanup(jobID, "")
				continue
			}
			s.enqueueCleanup(jobID, terminalState)
		}
		if err := rows.Err(); err != nil {
			slog.WarnContext(ctx, "failed iterating Cleaning jobs during recovery", slog.Any("err", err))
		}
	} else {
		slog.WarnContext(ctx, "failed to recover Cleaning jobs", slog.Any("err", err))
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
			launched:
				for jobID, terminalState := range jobs {
					wg.Add(1)
					select {
					case sem <- struct{}{}:
					case <-ctx.Done():
						wg.Done()
						break launched
					}
					go func(jobID, terminalState string) {
						defer wg.Done()
						defer func() { <-sem }()

						retryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						if strings.TrimSpace(terminalState) == "" {
							var deriveErr error
							terminalState, deriveErr = s.determineCleaningTerminalState(retryCtx, jobID)
							if deriveErr != nil {
								slog.WarnContext(retryCtx, "retrying cleanup after terminal-state recovery error",
									slog.String("job_id", jobID),
									slog.Any("err", deriveErr),
								)
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
