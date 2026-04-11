package models

type FunctionSpec struct {
	Language   string `json:"language"`
	Artifact   string `json:"artifact"`
	Entrypoint string `json:"entrypoint"`
	Interface  string `json:"interface"`
}

type JobSubmissionRequest struct {
	Filename string       `json:"filename"`
	Mapper   FunctionSpec `json:"mapper"`
	Reducer  FunctionSpec `json:"reducer"`
}

type JobSubmissionResponse struct {
	JobID   string `json:"jobId"`
	Status  string `json:"status"`
	Message string `json:"message"`
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
