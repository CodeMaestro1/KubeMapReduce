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

type JobStatus struct {
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

type WorkerConfigRequest struct {
	WorkerReplicas int `json:"workerReplicas"`
	MaxJobsPerNode int `json:"maxJobsPerNode"`
}

type NodeConfigRequest struct {
	MaxPods     int    `json:"maxPods"`
	CPULimit    string `json:"cpuLimit"`
	MemoryLimit string `json:"memoryLimit"`
}

type DeleteUserRequest struct {
	Username string `json:"username"`
}
