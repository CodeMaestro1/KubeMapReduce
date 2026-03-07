package models

import "time"

type FunctionSpec struct {
	Language   string `json:"language"`
	Artifact   string `json:"artifact"`
	Entrypoint string `json:"entrypoint"`
	Interface  string `json:"interface"`
}

type JobSubmissionRequest struct {
	Filename string        `json:"filename"`
	Mapper   FunctionSpec  `json:"mapper"`
	Reducer  FunctionSpec  `json:"reducer"`
	Combiner *FunctionSpec `json:"combiner,omitempty"`
	Reducers int           `json:"reducers,omitempty"`
}

type JobSubmissionResponse struct {
	JobID   string `json:"jobId"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type JobStatusResponse struct {
	JobID     string    `json:"jobId"`
	Status    string    `json:"status"`
	Message   string    `json:"message"`
	Filename  string    `json:"filename"`
	Reducers  int       `json:"reducers,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

type CreateUserRequest struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

// WorkerConfigRequest mirrors the admin configure-nodes CLI command
// and maps directly to the SYSTEM_CONFIG table in the DDS schema.
type WorkerConfigRequest struct {
	MaxConcurrentPods int    `json:"maxConcurrentPods"`
	CPULimit          string `json:"cpuLimit"`
	MemoryLimit       string `json:"memoryLimit"`
}

type NodeConfigRequest struct {
	MaxPods     int    `json:"maxPods"`
	CPULimit    string `json:"cpuLimit"`
	MemoryLimit string `json:"memoryLimit"`
}

type DeleteUserRequest struct {
	Username string `json:"username"`
}

type DeleteUserRequest struct {
	Username string `json:"username"`
}
