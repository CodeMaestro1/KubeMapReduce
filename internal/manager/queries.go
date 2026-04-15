// Package manager provides named SQL query constants for the MapReduce scheduler.
// Centralizing queries here prevents duplication, makes maintenance easier,
// and ensures the scheduler's DDS interactions are auditable at a glance.
package manager

// ---------------------------------------------------------------------------
// GetNextTask queries
// ---------------------------------------------------------------------------

// QueryGetMaxConcurrentPods reads the cluster-wide pod limit from SYSTEM_CONFIG.
// Used by GetNextTask for Resource Quota Enforcement (Section 4.3 of the Design Document).
const QueryGetMaxConcurrentPods = `SELECT max_concurrent_pods FROM SYSTEM_CONFIG LIMIT 1`

// QueryCountRunningAttempts counts globally active worker attempts.
// Compared against max_concurrent_pods to enforce cluster-wide scheduling limits.
const QueryCountRunningAttempts = `SELECT COUNT(*) FROM TASK_ATTEMPTS WHERE status = 'Running'`

// QueryCountFailedTasks checks if any task in the job has reached the Failed state.
// A non-zero result means the job is irrecoverable and GetNextTask returns ErrJobFailed.
const QueryCountFailedTasks = `SELECT COUNT(*) FROM TASKS WHERE status = 'Failed'`

// QuerySelectIdleTask atomically locks and retrieves the next schedulable task.
// Uses PostgreSQL's FOR UPDATE SKIP LOCKED to prevent double-scheduling across
// concurrent Manager replicas (Section 5.2: "Concurrency Control via Row-Level Locking").
const QuerySelectIdleTask = `
	SELECT task_id, job_id, task_type FROM TASKS
	WHERE status = 'Idle' AND replica_index = $1 AND task_type = $2
	FOR UPDATE SKIP LOCKED LIMIT 1`

// QueryCountPendingTasksByType counts non-completed tasks of a given type.
// Used to determine whether the Map phase is fully finished before allowing
// the scheduler to transition into the Reduce phase.
const QueryCountPendingTasksByType = `SELECT COUNT(*) FROM TASKS WHERE task_type = $1 AND status != 'Completed'`

// QueryCountAllPendingTasks counts all non-completed tasks regardless of type.
// Returns 0 when every map AND reduce task has completed (job is done).
const QueryCountAllPendingTasks = `SELECT COUNT(*) FROM TASKS WHERE status != 'Completed'`

// ---------------------------------------------------------------------------
// Task state mutation queries
// ---------------------------------------------------------------------------

// QueryUpdateTaskInProgress transitions a task from Idle to In-Progress and
// binds it to a specific attempt. Part of the atomic assignment transaction.
const QueryUpdateTaskInProgress = `UPDATE TASKS SET status = 'In-Progress', current_attempt_id = $1 WHERE task_id = $2`

// QueryInsertAttempt creates a new TASK_ATTEMPTS record when a worker is assigned.
// The lease_ttl of 30 seconds matches the 3-heartbeat timeout window from Section 5.
const QueryInsertAttempt = `
	INSERT INTO TASK_ATTEMPTS (attempt_id, task_id, worker_id, lease_id, last_renewed_at, lease_ttl, start_time, status)
	VALUES ($1, $2, $3, $4, $5, 30, $6, 'Running')`

// QuerySelectTaskForUpdate locks a task row for safe state transition.
// Used by CompleteTask, FailTask, and RenewLease to enforce serializable access.
const QuerySelectTaskForUpdate = `SELECT status, current_attempt_id FROM TASKS WHERE task_id = $1 FOR UPDATE`

// QueryCompleteTask marks a task as Completed and clears the attempt binding.
const QueryCompleteTask = `UPDATE TASKS SET status = 'Completed', current_attempt_id = NULL WHERE task_id = $1`

// QuerySucceedAttempt marks the current attempt as successful with an end timestamp.
const QuerySucceedAttempt = `UPDATE TASK_ATTEMPTS SET status = 'Success', end_time = $1 WHERE attempt_id = $2`

// QueryInsertOutput persists a single output shard to TASK_OUTPUTS (1NF compliant).
const QueryInsertOutput = `INSERT INTO TASK_OUTPUTS (task_id, partition_index, output_uri, checksum) VALUES ($1, $2, $3, $4)`

// ---------------------------------------------------------------------------
// Lease management queries
// ---------------------------------------------------------------------------

// QuerySelectLeaseInfo fetches lease credentials for fencing validation.
// Lease expiry is computed at runtime as last_renewed_at + lease_ttl
// (Section 5.1: "Lease-Based Locking for Split-Brain Prevention").
const QuerySelectLeaseInfo = `SELECT lease_id, last_renewed_at, lease_ttl FROM TASK_ATTEMPTS WHERE attempt_id = $1`

// QueryRenewLease updates the heartbeat timestamp, extending the lease TTL window.
const QueryRenewLease = `UPDATE TASK_ATTEMPTS SET last_renewed_at = $1 WHERE attempt_id = $2`

// ---------------------------------------------------------------------------
// Failure and recovery queries
// ---------------------------------------------------------------------------

// QueryCountAttemptsByTask counts all attempts (Running, Success, Failed) for a task.
// Compared against MaxTaskAttempts to decide between Idle retry and permanent Failed.
const QueryCountAttemptsByTask = `SELECT COUNT(*) FROM TASK_ATTEMPTS WHERE task_id = $1`

// QueryUpdateTaskStatus transitions a task to a new status and clears the attempt binding.
// Used by both FailTask (→ Idle or Failed) and FailStaleTasks.
const QueryUpdateTaskStatus = `UPDATE TASKS SET status = $1, current_attempt_id = NULL WHERE task_id = $2`

// QueryFailAttempt marks an attempt as Failed with an end timestamp.
const QueryFailAttempt = `UPDATE TASK_ATTEMPTS SET status = 'Failed', end_time = $1 WHERE attempt_id = $2`

// QuerySelectStaleTasks finds in-progress tasks whose lease has expired.
// The Manager's Active Reaper uses this to reclaim zombie workers (Section 5.1).
const QuerySelectStaleTasks = `
	SELECT t.task_id, a.attempt_id
	FROM TASKS t
	JOIN TASK_ATTEMPTS a ON t.current_attempt_id = a.attempt_id
	WHERE t.status = 'In-Progress' AND a.status = 'Running' AND a.last_renewed_at < $1
	FOR UPDATE OF t`

// ---------------------------------------------------------------------------
// Read-only output queries
// ---------------------------------------------------------------------------

// QueryGetMapOutputs collects output URIs from all completed Map tasks.
// Used by the Manager to locate intermediate shuffle data for the Reduce phase.
const QueryGetMapOutputs = `
	SELECT o.output_uri FROM TASK_OUTPUTS o
	JOIN TASKS t ON o.task_id = t.task_id
	WHERE t.task_type = 'Map' AND t.status = 'Completed'`

// QueryGetReduceOutputs collects output URIs from all completed Reduce tasks.
// Used by the Manager to build the final result set for the Retrieve Result flow.
const QueryGetReduceOutputs = `
	SELECT o.output_uri FROM TASK_OUTPUTS o
	JOIN TASKS t ON o.task_id = t.task_id
	WHERE t.task_type = 'Reduce' AND t.status = 'Completed'`

// ---------------------------------------------------------------------------
// Read-only status queries
// ---------------------------------------------------------------------------

// QueryGetTaskStatus retrieves the current status string for a single task.
// Supports the UI Service's CQRS-style read path for the jobs status CLI command.
const QueryGetTaskStatus = `SELECT status FROM TASKS WHERE task_id = $1`

// QueryGetTaskByID retrieves full task metadata including the current attempt binding.
const QueryGetTaskByID = `SELECT task_id, task_type, status, current_attempt_id FROM TASKS WHERE task_id = $1`

// QueryGetAttemptDetails fetches worker and lease info for an active attempt.
// Used by GetTaskByID to hydrate the full Task struct when an attempt is active.
const QueryGetAttemptDetails = `SELECT worker_id, lease_id, start_time, last_renewed_at FROM TASK_ATTEMPTS WHERE attempt_id = $1`

// QueryCountPendingMapTasks counts Map tasks that haven't reached Completed.
// Used by AllMapTasksCompleted to determine Map→Reduce phase transition readiness.
const QueryCountPendingMapTasks = `SELECT COUNT(*) FROM TASKS WHERE task_type = 'Map' AND status != 'Completed'`
