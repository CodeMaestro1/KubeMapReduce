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
const QueryCountFailedTasks = `SELECT COUNT(*) FROM TASKS WHERE job_id = $1 AND status = 'Failed'`

// QuerySelectIdleTask atomically locks and retrieves the next schedulable task.
// Uses PostgreSQL's FOR UPDATE SKIP LOCKED to prevent double-scheduling across
// concurrent Manager replicas (Section 5.2: "Concurrency Control via Row-Level Locking").
const QuerySelectIdleTask = `
	SELECT task_id, job_id, task_type, replica_index FROM TASKS
	WHERE job_id = $1 AND status = 'Idle' AND replica_index = $2 AND task_type = $3
	FOR UPDATE SKIP LOCKED LIMIT 1`

// QueryCountPendingTasksByType counts non-completed tasks of a given type.
// Used to determine whether the Map phase is fully finished before allowing
// the scheduler to transition into the Reduce phase.
const QueryCountPendingTasksByType = `SELECT COUNT(*) FROM TASKS WHERE job_id = $1 AND task_type = $2 AND status != 'Completed'`

// QueryCountAllPendingTasks counts all non-completed tasks regardless of type.
// Returns 0 when every map AND reduce task has completed (job is done).
const QueryCountAllPendingTasks = `SELECT COUNT(*) FROM TASKS WHERE job_id = $1 AND status != 'Completed'`

// ---------------------------------------------------------------------------
// Task state mutation queries
// ---------------------------------------------------------------------------

// QueryUpdateTaskInProgress transitions a task from Idle to In-Progress and
// binds it to a specific attempt. Part of the atomic assignment transaction.
const QueryUpdateTaskInProgress = `UPDATE TASKS SET status = 'In-Progress', current_attempt_id = $1 WHERE task_id = $2`

// QueryInsertAttempt creates a new TASK_ATTEMPTS record when a worker is assigned.
// lease_ttl is caller-provided from scheduler config (HeartbeatInterval * MaxMissedHeartbeats).
const QueryInsertAttempt = `
	INSERT INTO TASK_ATTEMPTS (attempt_id, task_id, worker_id, lease_id, last_renewed_at, lease_ttl, start_time, status)
	VALUES ($1, $2, $3, $4, NOW(), $5, NOW(), 'Running')`

// QueryGetJobConfigByTask loads immutable job configuration needed to build
// a worker-facing task assignment.
const QueryGetJobConfigByTask = `
	SELECT jc.mapper_uri, jc.reducer_uri, jc.combiner_uri, jc.r_tasks, jc.input_checksum
	FROM JOB_CONFIGS jc
	JOIN TASKS t ON t.job_id = jc.job_id
	WHERE t.task_id = $1`

// QueryGetTaskInputs loads all logical input splits assigned to a task.
const QueryGetTaskInputs = `
	SELECT input_uri, byte_start, byte_end, split_checksum
	FROM TASK_INPUTS
	WHERE task_id = $1
	ORDER BY input_assignment_id`

// QueryGetReduceTaskInputs loads completed map outputs that belong to the reduce
// partition represented by the reduce task's replica_index.
const QueryGetReduceTaskInputs = `
	SELECT COALESCE(o.partition_index, 0), o.output_uri, o.checksum
	FROM TASK_OUTPUTS o
	JOIN TASKS map_tasks ON map_tasks.task_id = o.task_id
	JOIN TASKS reduce_task ON reduce_task.task_id = $1
	WHERE map_tasks.job_id = reduce_task.job_id
	  AND map_tasks.task_type = 'Map'
	  AND map_tasks.status = 'Completed'
	  AND (o.partition_index IS NULL OR o.partition_index = reduce_task.replica_index)
	ORDER BY o.output_id`

// QuerySelectTaskForUpdate locks a task row for safe state transition.
// Used by CompleteTask, FailTask, and RenewLease to enforce serializable access.
const QuerySelectTaskForUpdate = `SELECT status, current_attempt_id FROM TASKS WHERE task_id = $1 FOR UPDATE`

// QueryCompleteTask marks a task as Completed and clears the attempt binding.
const QueryCompleteTask = `UPDATE TASKS SET status = 'Completed', current_attempt_id = NULL WHERE task_id = $1`

// QuerySucceedAttempt marks the current attempt as successful with an end timestamp.
const QuerySucceedAttempt = `UPDATE TASK_ATTEMPTS SET status = 'Success', end_time = NOW() WHERE attempt_id = $1`

// QueryInsertOutput persists a single output shard to TASK_OUTPUTS (1NF compliant).
const QueryInsertOutput = `INSERT INTO TASK_OUTPUTS (task_id, partition_index, output_uri, checksum) VALUES ($1, $2, $3, $4)`

// ---------------------------------------------------------------------------
// Lease management queries
// ---------------------------------------------------------------------------

// QueryCheckLeaseValid validates both the fence token and expiry against the DB clock.
// This keeps commit/renew/fail lease checks aligned with stale-task reaping.
const QueryCheckLeaseValid = `
	SELECT lease_id = $2
	   AND last_renewed_at + lease_ttl * INTERVAL '1 second' >= NOW() AS lease_valid
	FROM TASK_ATTEMPTS
	WHERE attempt_id = $1`

// QueryRenewLease updates the heartbeat timestamp, extending the lease TTL window.
const QueryRenewLease = `UPDATE TASK_ATTEMPTS SET last_renewed_at = NOW() WHERE attempt_id = $1`

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
const QueryFailAttempt = `UPDATE TASK_ATTEMPTS SET status = 'Failed', end_time = NOW() WHERE attempt_id = $1`

// QuerySelectStaleTasks finds in-progress tasks whose lease has expired.
// The Manager's Active Reaper uses this to reclaim zombie workers (Section 5.1).
// Expiry is computed using lease_ttl so correctness does not depend on the caller's timeout value.
const QuerySelectStaleTasks = `
	SELECT t.task_id, a.attempt_id
	FROM TASKS t
	JOIN TASK_ATTEMPTS a ON t.current_attempt_id = a.attempt_id
	WHERE t.status = 'In-Progress' AND a.status = 'Running' AND a.last_renewed_at + a.lease_ttl * INTERVAL '1 second' < NOW()
	FOR UPDATE OF t`

// QuerySelectRecoverableAttempts returns active attempts that belong to this manager replica.
// Recovery uses these rows to re-spawn workers with the existing attempt_id fence token.
const QuerySelectRecoverableAttempts = `
	SELECT task_id, current_attempt_id, job_id
	FROM TASKS
	WHERE status = 'In-Progress' AND replica_index = $1 AND current_attempt_id IS NOT NULL`

// ---------------------------------------------------------------------------
// Read-only output queries
// ---------------------------------------------------------------------------

// QueryGetMapOutputs collects output URIs from all completed Map tasks.
// Used by the Manager to locate intermediate shuffle data for the Reduce phase.
const QueryGetMapOutputs = `
	SELECT o.output_uri FROM TASK_OUTPUTS o
	JOIN TASKS t ON o.task_id = t.task_id
	WHERE t.job_id = $1 AND t.task_type = 'Map' AND t.status = 'Completed'`

// QueryGetReduceOutputs collects output URIs from all completed Reduce tasks.
// Used by the Manager to build the final result set for the Retrieve Result flow.
const QueryGetReduceOutputs = `
	SELECT o.output_uri FROM TASK_OUTPUTS o
	JOIN TASKS t ON o.task_id = t.task_id
	WHERE t.job_id = $1 AND t.task_type = 'Reduce' AND t.status = 'Completed'`

// ---------------------------------------------------------------------------
// Read-only status queries
// ---------------------------------------------------------------------------

// QueryGetTaskStatus retrieves the current status string for a single task.
// Supports the UI Service's CQRS-style read path for the jobs status CLI command.
const QueryGetTaskStatus = `SELECT status FROM TASKS WHERE task_id = $1`

const QueryGetTaskJobID = `SELECT job_id FROM TASKS WHERE task_id = $1`

// QueryGetTaskByID retrieves full task metadata including the current attempt binding.
const QueryGetTaskByID = `SELECT task_id, job_id, task_type, status, current_attempt_id, replica_index FROM TASKS WHERE task_id = $1`

// QueryGetAttemptDetails fetches worker and lease info for an active attempt.
// Used by GetTaskByID to hydrate the full Task struct when an attempt is active.
const QueryGetAttemptDetails = `SELECT worker_id, lease_id, start_time, last_renewed_at FROM TASK_ATTEMPTS WHERE attempt_id = $1`

// QueryCountPendingMapTasks counts Map tasks that haven't reached Completed.
// Used by AllMapTasksCompleted to determine Map→Reduce phase transition readiness.
const QueryCountPendingMapTasks = `SELECT COUNT(*) FROM TASKS WHERE job_id = $1 AND task_type = 'Map' AND status != 'Completed'`

// ---------------------------------------------------------------------------
// Job lifecycle and bootstrap queries
// ---------------------------------------------------------------------------

const QueryInsertJob = `
	INSERT INTO JOBS (job_id, user_id, status, created_at, updated_at)
	VALUES ($1, $2, 'Pending', $3, $4)`

const QueryInsertJobConfig = `
	INSERT INTO JOB_CONFIGS (job_id, input_uri, mapper_uri, reducer_uri, combiner_uri, m_tasks, r_tasks, input_checksum)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

const QueryInsertTask = `
	INSERT INTO TASKS (task_id, job_id, current_attempt_id, task_type, status, replica_index)
	VALUES ($1, $2, NULL, $3, 'Idle', $4)`

const QueryInsertTaskInput = `
	INSERT INTO TASK_INPUTS (task_id, input_uri, byte_start, byte_end, split_checksum)
	VALUES ($1, $2, $3, $4, $5)`

const QueryUpdateJobStatus = `UPDATE JOBS SET status = $2, updated_at = $3 WHERE job_id = $1`

const QueryUpsertSystemConfig = `
	INSERT INTO SYSTEM_CONFIG (config_id, max_concurrent_pods, cpu_limit, memory_limit, updated_at)
	VALUES (1, $1, $2, $3, $4)
	ON CONFLICT (config_id) DO UPDATE
	SET max_concurrent_pods = EXCLUDED.max_concurrent_pods,
	    cpu_limit = EXCLUDED.cpu_limit,
	    memory_limit = EXCLUDED.memory_limit,
	    updated_at = EXCLUDED.updated_at`
