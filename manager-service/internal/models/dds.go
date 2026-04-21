package models

import (
	"time"

	"github.com/google/uuid"
)

// JobStatus represents the high-level lifecycle state of a MapReduce job.
//
// These states are used by the Manager to determine whether a job is eligible for
// scheduling, cleaning, or reporting to the user. Transitions are typically
// one-way toward terminal states ([JobCompleted], [JobFailed], [JobCancelled]).
type JobStatus string

const (
	// JobPending indicates the job has been accepted and stored but not yet
	// picked up by the orchestrator.
	JobPending JobStatus = "Pending"
	// JobRunning indicates the orchestrator is actively managing the job lifecycle.
	JobRunning JobStatus = "Running"
	// JobCompleted indicates the job finished successfully and all output is ready.
	JobCompleted JobStatus = "Completed"
	// JobCancelled indicates the job was stopped by user intervention.
	JobCancelled JobStatus = "Cancelled"
	// JobFailed indicates the job stopped due to an unrecoverable error.
	JobFailed JobStatus = "Failed"
	// JobCleaning indicates the job has finished and the system is reclaiming
	// resources (e.g. temporary MinIO buckets).
	JobCleaning JobStatus = "Cleaning"
)

// TaskType distinguishes between the two primary phases of a MapReduce job.
type TaskType string

const (
	// TaskTypeMap represents a mapper task that processes input splits.
	TaskTypeMap TaskType = "Map"
	// TaskTypeReduce represents a reducer task that aggregates intermediate results.
	TaskTypeReduce TaskType = "Reduce"
)

// TaskStatus represents the state of an individual unit of work.
//
// While a [Task] might have multiple [TaskAttempt]s, this status reflects the
// aggregate state. A task is only "Completed" if at least one attempt succeeded.
type TaskStatus string

const (
	// TaskIdle indicates the task is ready to be assigned to a worker.
	TaskIdle TaskStatus = "Idle"
	// TaskInProgress indicates a worker has claimed the task and a lease is active.
	TaskInProgress TaskStatus = "In-Progress"
	// TaskCompleted indicates the task output has been verified and committed.
	TaskCompleted TaskStatus = "Completed"
	// TaskFailed indicates the task reached its maximum retry limit.
	TaskFailed TaskStatus = "Failed"
)

// AttemptStatus tracks the status of a specific worker's execution of a task.
//
// This level of granularity is necessary to handle worker failures and "zombie"
// workers. The Manager only accepts output from an attempt that matches the
// task's [CurrentAttemptID].
type AttemptStatus string

const (
	// AttemptRunning indicates the worker is actively heartbeating.
	AttemptRunning AttemptStatus = "Running"
	// AttemptSuccess indicates the worker reported success before the lease expired.
	AttemptSuccess AttemptStatus = "Success"
	// AttemptFailed indicates the worker reported a failure or the lease timed out.
	AttemptFailed AttemptStatus = "Failed"
)

// Job corresponds to the JOBS table in the Distributed Data Store (DDS).
//
// It serves as the root entity for all MapReduce operations, linking a user to
// their submitted work.
type Job struct {
	JobID     uuid.UUID `db:"job_id" json:"jobId"`
	UserID    uuid.UUID `db:"user_id" json:"userId"`
	Status    JobStatus `db:"status" json:"status"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`
	UpdatedAt time.Time `db:"updated_at" json:"updatedAt"`
}

// JobConfig corresponds to the JOB_CONFIGS table.
//
// This struct stores the immutable parameters provided by the user at submission.
// Keeping these in a separate table from [Job] maintains a clean audit trail and
// prevents the main JOBS table from becoming bloated with large URI strings.
type JobConfig struct {
	JobID         uuid.UUID `db:"job_id" json:"jobId"`
	InputURI      string    `db:"input_uri" json:"inputUri"`
	MapperURI     string    `db:"mapper_uri" json:"mapperUri"`
	ReducerURI    string    `db:"reducer_uri" json:"reducerUri"`
	CombinerURI   string    `db:"combiner_uri" json:"combinerUri"`
	MTasks        int       `db:"m_tasks" json:"mTasks"`
	RTasks        int       `db:"r_tasks" json:"rTasks"`
	InputChecksum string    `db:"input_checksum" json:"inputChecksum"`
}

// Task corresponds to the TASKS table.
//
// A Task represents a logical partition of a [Job]. Fencing is managed via
// the [CurrentAttemptID], which ensures that only the most recently assigned
// worker can commit results to the DDS.
type Task struct {
	TaskID           uuid.UUID  `db:"task_id" json:"taskId"`
	JobID            uuid.UUID  `db:"job_id" json:"jobId"`
	TaskType         TaskType   `db:"task_type" json:"taskType"`
	Status           TaskStatus `db:"status" json:"status"`
	ReplicaIndex     int        `db:"replica_index" json:"replicaIndex"`
	CurrentAttemptID *uuid.UUID `db:"current_attempt_id" json:"currentAttemptId,omitempty"`
}

// TaskInput corresponds to the TASK_INPUTS table.
//
// It defines exactly which part of a file or intermediate data a task should
// process. The [ByteStart] and [ByteEnd] fields allow for efficient range-based
// reads from shared storage (MinIO).
type TaskInput struct {
	InputAssignmentID int64     `db:"input_assignment_id" json:"inputAssignmentId"`
	TaskID            uuid.UUID `db:"task_id" json:"taskId"`
	InputURI          string    `db:"input_uri" json:"inputUri"`
	ByteStart         int64     `db:"byte_start" json:"byteStart"`
	ByteEnd           int64     `db:"byte_end" json:"byteEnd"`
	SplitChecksum     string    `db:"split_checksum" json:"splitChecksum"`
}

// TaskAttempt corresponds to the TASK_ATTEMPTS table.
//
// Each attempt represents one execution instance of a [Task]. The [LeaseID] and
// [LastRenewedAt] fields are critical for the Active Reaper to identify and
// reclaim tasks from crashed workers.
type TaskAttempt struct {
	AttemptID     uuid.UUID     `db:"attempt_id" json:"attemptId"`
	TaskID        uuid.UUID     `db:"task_id" json:"taskId"`
	WorkerID      string        `db:"worker_id" json:"workerId"`
	LeaseID       uuid.UUID     `db:"lease_id" json:"leaseId"`
	LastRenewedAt time.Time     `db:"last_renewed_at" json:"lastRenewedAt"`
	LeaseTTL      int           `db:"lease_ttl" json:"leaseTtl"`
	StartTime     time.Time     `db:"start_time" json:"startTime"`
	EndTime       *time.Time    `db:"end_time" json:"endTime,omitempty"`
	Status        AttemptStatus `db:"status" json:"status"`
}

// TaskOutput corresponds to the TASK_OUTPUTS table.
//
// It records the location and integrity [Checksum] of data produced by a
// [Task]. These records are consumed by the Shuffle phase to prepare inputs for
// Reducer tasks.
type TaskOutput struct {
	OutputID       int64     `db:"output_id" json:"outputId"`
	TaskID         uuid.UUID `db:"task_id" json:"taskId"`
	PartitionIndex *int      `db:"partition_index" json:"partitionIndex,omitempty"`
	OutputURI      string    `db:"output_uri" json:"outputUri"`
	Checksum       string    `db:"checksum" json:"checksum"`
}

// IsTerminal returns true if the job has reached a final state
// (Completed, Failed, or Cancelled) and will not transition further.
//
// This is used by the Manager to trigger the Cleaning phase and garbage collection
// of ephemeral K8s resources.
func (j *Job) IsTerminal() bool {
	return j.Status == JobCompleted || j.Status == JobFailed || j.Status == JobCancelled
}

// SystemConfig corresponds to the SYSTEM_CONFIG table.
//
// It stores global platform limits that are applied to all jobs. These values
// are typically managed by the Admin CLI to perform live tuning of the cluster.
type SystemConfig struct {
	ConfigID          int       `db:"config_id" json:"configId"`
	MaxConcurrentPods int       `db:"max_concurrent_pods" json:"maxConcurrentPods"`
	CPULimit          string    `db:"cpu_limit" json:"cpuLimit"`
	MemoryLimit       string    `db:"memory_limit" json:"memoryLimit"`
	UpdatedAt         time.Time `db:"updated_at" json:"updatedAt"`
}
