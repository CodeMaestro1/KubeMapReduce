package models

import (
	"encoding/json"
	"testing"
)

func TestJobSubmissionRequest_JSON(t *testing.T) {
	input := `{
		"filename": "s3://inputs/job-123/input-data.csv",
		"inputChecksum": "sha256-123",
		"mapper": {
			"language": "python",
			"artifact": "mapper.py",
			"entrypoint": "map_func",
			"interface": "stdin"
		},
		"reducer": {
			"language": "python",
			"artifact": "reducer.py",
			"entrypoint": "reduce_func",
			"interface": "stdin"
		}
	}`

	var req JobSubmissionRequest
	if err := json.Unmarshal([]byte(input), &req); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if req.Filename != "s3://inputs/job-123/input-data.csv" {
		t.Errorf("Filename = %q, want %q", req.Filename, "s3://inputs/job-123/input-data.csv")
	}
	if req.InputChecksum != "sha256-123" {
		t.Errorf("InputChecksum = %q, want %q", req.InputChecksum, "sha256-123")
	}
	if req.Mapper.Language != "python" {
		t.Errorf("Mapper.Language = %q, want %q", req.Mapper.Language, "python")
	}
	if req.Mapper.Artifact != "mapper.py" {
		t.Errorf("Mapper.Artifact = %q, want %q", req.Mapper.Artifact, "mapper.py")
	}
	if req.Mapper.Entrypoint != "map_func" {
		t.Errorf("Mapper.Entrypoint = %q, want %q", req.Mapper.Entrypoint, "map_func")
	}
	if req.Reducer.Language != "python" {
		t.Errorf("Reducer.Language = %q, want %q", req.Reducer.Language, "python")
	}
}

func TestJobSubmissionResponse_JSON(t *testing.T) {
	resp := JobSubmissionResponse{
		JobID:   "job-abc123",
		Status:  "accepted",
		Message: "ok",
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded JobSubmissionResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.JobID != resp.JobID {
		t.Errorf("JobID = %q, want %q", decoded.JobID, resp.JobID)
	}
	if decoded.Status != resp.Status {
		t.Errorf("Status = %q, want %q", decoded.Status, resp.Status)
	}
	if decoded.Message != resp.Message {
		t.Errorf("Message = %q, want %q", decoded.Message, resp.Message)
	}
}

func TestCreateUserRequest_JSON(t *testing.T) {
	input := `{"username":"alice","email":"alice@example.com","password":"pw","role":"ADMIN"}`

	var req CreateUserRequest
	if err := json.Unmarshal([]byte(input), &req); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if req.Username != "alice" {
		t.Errorf("Username = %q, want %q", req.Username, "alice")
	}
	if req.Email != "alice@example.com" {
		t.Errorf("Email = %q, want %q", req.Email, "alice@example.com")
	}
	if req.Password != "pw" {
		t.Errorf("Password = %q, want %q", req.Password, "pw")
	}
	if req.Role != "ADMIN" {
		t.Errorf("Role = %q, want %q", req.Role, "ADMIN")
	}
}

func TestWorkerConfigRequest_JSON(t *testing.T) {
	input := `{"workerReplicas":4,"maxJobsPerNode":8}`

	var req WorkerConfigRequest
	if err := json.Unmarshal([]byte(input), &req); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if req.WorkerReplicas != 4 {
		t.Errorf("WorkerReplicas = %d, want %d", req.WorkerReplicas, 4)
	}
	if req.MaxJobsPerNode != 8 {
		t.Errorf("MaxJobsPerNode = %d, want %d", req.MaxJobsPerNode, 8)
	}
}

func TestFunctionSpec_EmptyFields(t *testing.T) {
	var spec FunctionSpec
	if err := json.Unmarshal([]byte(`{}`), &spec); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if spec.Language != "" || spec.Artifact != "" || spec.Entrypoint != "" || spec.Interface != "" {
		t.Errorf("expected all empty fields, got %+v", spec)
	}
}

func TestWorkerConfigRequest_ZeroValues(t *testing.T) {
	var req WorkerConfigRequest
	if err := json.Unmarshal([]byte(`{}`), &req); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if req.WorkerReplicas != 0 || req.MaxJobsPerNode != 0 {
		t.Errorf("expected zero values, got %+v", req)
	}
}

func TestJobSubmissionRequest_MarshalRoundTrip(t *testing.T) {
	original := JobSubmissionRequest{
		Filename:      "s3://inputs/job-123/input.txt",
		InputChecksum: "sha256-abc",
		Mapper: FunctionSpec{
			Language:   "go",
			Artifact:   "mapper.wasm",
			Entrypoint: "Map",
			Interface:  "grpc",
		},
		Reducer: FunctionSpec{
			Language:   "go",
			Artifact:   "reducer.wasm",
			Entrypoint: "Reduce",
			Interface:  "grpc",
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded JobSubmissionRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.Filename != original.Filename {
		t.Errorf("Filename = %q, want %q", decoded.Filename, original.Filename)
	}
	if decoded.InputChecksum != original.InputChecksum {
		t.Errorf("InputChecksum = %q, want %q", decoded.InputChecksum, original.InputChecksum)
	}
	if decoded.Mapper != original.Mapper {
		t.Errorf("Mapper = %+v, want %+v", decoded.Mapper, original.Mapper)
	}
	if decoded.Reducer != original.Reducer {
		t.Errorf("Reducer = %+v, want %+v", decoded.Reducer, original.Reducer)
	}
}

func TestDeleteUserRequest_JSON(t *testing.T) {
	input := `{"username":"alice"}`

	var req DeleteUserRequest
	if err := json.Unmarshal([]byte(input), &req); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if req.Username != "alice" {
		t.Errorf("Username = %q, want %q", req.Username, "alice")
	}
}
