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

type WorkerConfigRequest struct {
	WorkerReplicas int `json:"workerReplicas"`
	MaxJobsPerNode int `json:"maxJobsPerNode"`
}

type DeleteUserRequest struct {
	Username string `json:"username"`
}
