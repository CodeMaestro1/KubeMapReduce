package models

import (
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	JobPending   JobStatus = "Pending"
	JobRunning   JobStatus = "Running"
	JobCompleted JobStatus = "Completed"
	JobCancelled JobStatus = "Cancelled"
	JobFailed    JobStatus = "Failed"
	JobCleaning  JobStatus = "Cleaning"
)

type TaskType string

const (
	TaskTypeMap    TaskType = "Map"
	TaskTypeReduce TaskType = "Reduce"
)

type TaskStatus string

const (
	TaskIdle       TaskStatus = "Idle"
	TaskInProgress TaskStatus = "In-Progress"
	TaskCompleted  TaskStatus = "Completed"
)

type AttemptStatus string

const (
	AttemptRunning AttemptStatus = "Running"
	AttemptSuccess AttemptStatus = "Success"
	AttemptFailed  AttemptStatus = "Failed"
)

// Job corresponds to the JOBS table.
type Job struct {
	JobID     uuid.UUID `db:"job_id" json:"jobId"`
	UserID    uuid.UUID `db:"user_id" json:"userId"`
	Status    JobStatus `db:"status" json:"status"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`
	UpdatedAt time.Time `db:"updated_at" json:"updatedAt"`
}

// JobConfig corresponds to the JOB_CONFIGS table.
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
type Task struct {
	TaskID           uuid.UUID  `db:"task_id" json:"taskId"`
	JobID            uuid.UUID  `db:"job_id" json:"jobId"`
	TaskType         TaskType   `db:"task_type" json:"taskType"`
	Status           TaskStatus `db:"status" json:"status"`
	ReplicaIndex     int        `db:"replica_index" json:"replicaIndex"`
	CurrentAttemptID *uuid.UUID `db:"current_attempt_id" json:"currentAttemptId,omitempty"`
}

// TaskInput corresponds to the TASK_INPUTS table.
type TaskInput struct {
	InputAssignmentID int64     `db:"input_assignment_id" json:"inputAssignmentId"`
	TaskID            uuid.UUID `db:"task_id" json:"taskId"`
	InputURI          string    `db:"input_uri" json:"inputUri"`
	ByteStart         int64     `db:"byte_start" json:"byteStart"`
	ByteEnd           int64     `db:"byte_end" json:"byteEnd"`
	SplitChecksum     string    `db:"split_checksum" json:"splitChecksum"`
}

// TaskAttempt corresponds to the TASK_ATTEMPTS table.
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
type TaskOutput struct {
	OutputID       int64     `db:"output_id" json:"outputId"`
	TaskID         uuid.UUID `db:"task_id" json:"taskId"`
	PartitionIndex *int      `db:"partition_index" json:"partitionIndex,omitempty"`
	OutputURI      string    `db:"output_uri" json:"outputUri"`
	Checksum       string    `db:"checksum" json:"checksum"`
}

// LeaseExpired checks if this attempt's lease has expired by computing
// last_renewed_at + lease_ttl and comparing against the current time.
// This follows the design document's runtime lease expiry computation
// (Section 5.1: "Lease expiry is computed at runtime as last_renewed_at + lease_ttl").
func (ta *TaskAttempt) LeaseExpired() bool {
	expiry := ta.LastRenewedAt.Add(time.Duration(ta.LeaseTTL) * time.Second)
	return time.Now().After(expiry)
}

// IsTerminal returns true if the job has reached a final state
// (Completed, Failed, or Cancelled) and will not transition further.
// Used by the Manager to trigger the Cleaning phase and garbage collection.
func (j *Job) IsTerminal() bool {
	return j.Status == JobCompleted || j.Status == JobFailed || j.Status == JobCancelled
}

// SystemConfig corresponds to the SYSTEM_CONFIG table.
type SystemConfig struct {
	ConfigID          int       `db:"config_id" json:"configId"`
	MaxConcurrentPods int       `db:"max_concurrent_pods" json:"maxConcurrentPods"`
	CPULimit          string    `db:"cpu_limit" json:"cpuLimit"`
	MemoryLimit       string    `db:"memory_limit" json:"memoryLimit"`
	UpdatedAt         time.Time `db:"updated_at" json:"updatedAt"`
}
