package models

import "time"

// FunctionSpec defines the execution environment and entrypoint for a user-provided
// Map or Reduce function.
//
// This decoupled specification allows the system to support multiple runtimes
// (e.g. Go, Python) by simply swapping the worker container image while keeping
// the same entrypoint logic.
type FunctionSpec struct {
	Language   string `json:"language"`
	Artifact   string `json:"artifact"`
	Entrypoint string `json:"entrypoint"`
	Interface  string `json:"interface"`
}

// JobSubmissionRequest contains the parameters for starting a new MapReduce job.
//
// This is the primary entrypoint for users of the platform. The [Mapper] and
// [Reducer] fields are required, while [Combiner] is an optional optimization.
type JobSubmissionRequest struct {
	Filename string        `json:"filename"`
	Mapper   FunctionSpec  `json:"mapper"`
	Reducer  FunctionSpec  `json:"reducer"`
	Combiner *FunctionSpec `json:"combiner,omitempty"`
	Reducers int           `json:"reducers,omitempty"`
}

// JobSubmissionResponse is returned to the user immediately after a job is queued.
//
// The [JobID] should be used in subsequent calls to [JobStatusResponse] to track
// progress.
type JobSubmissionResponse struct {
	JobID   string `json:"jobId"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// JobStatusResponse provides a snapshot of a job's progress and metadata.
type JobStatusResponse struct {
	JobID     string    `json:"jobId"`
	Status    string    `json:"status"`
	Message   string    `json:"message"`
	Filename  string    `json:"filename"`
	Reducers  int       `json:"reducers,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

// CreateUserRequest is used by the Admin CLI to provision new users.
type CreateUserRequest struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

// WorkerConfigRequest mirrors the admin worker-config CLI command.
//
// It allows for dynamic scaling of the worker pool without restarting the Manager.
type WorkerConfigRequest struct {
	WorkerReplicas int `json:"workerReplicas"`
	MaxJobsPerNode int `json:"maxJobsPerNode"`
}

// NodeConfigRequest defines the resource constraints for individual worker pods.
//
// These values are injected into the K8s Pod specification by the Manager.
type NodeConfigRequest struct {
	MaxPods     int    `json:"maxPods"`
	CPULimit    string `json:"cpuLimit"`
	MemoryLimit string `json:"memoryLimit"`
}

// DeleteUserRequest is used by the Admin CLI to deprovision users.
type DeleteUserRequest struct {
	Username string `json:"username"`
}

// PresignRequest is used to request a temporary URL for object storage access.
type PresignRequest struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// PresignResponse contains the generated presigned URL.
type PresignResponse struct {
	URL string `json:"url"`
}
