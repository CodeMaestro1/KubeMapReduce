package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSystemConfig_Serialization(t *testing.T) {
	config := SystemConfig{
		ConfigID:              1,
		MaxConcurrentPods:     10,
		CPULimit:              "500m",
		MemoryLimit:           "1Gi",
		WorkerReplicas:        3,
		MaxJobsPerNode:        5,
		LocalityKey:           "topology.kubernetes.io/zone",
		LocalityLabelSelector: "app=minio",
		UpdatedAt:             time.Now().Truncate(time.Second), // Truncate so JSON marshal/unmarshal comparisons are stable
	}

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal SystemConfig: %v", err)
	}

	var unmarshaledConfig SystemConfig
	if err := json.Unmarshal(data, &unmarshaledConfig); err != nil {
		t.Fatalf("Failed to unmarshal SystemConfig: %v", err)
	}

	if config.MaxConcurrentPods != unmarshaledConfig.MaxConcurrentPods {
		t.Errorf("Expected MaxConcurrentPods %d, got %d", config.MaxConcurrentPods, unmarshaledConfig.MaxConcurrentPods)
	}
	if config.WorkerReplicas != unmarshaledConfig.WorkerReplicas {
		t.Errorf("Expected WorkerReplicas %d, got %d", config.WorkerReplicas, unmarshaledConfig.WorkerReplicas)
	}
	if config.MaxJobsPerNode != unmarshaledConfig.MaxJobsPerNode {
		t.Errorf("Expected MaxJobsPerNode %d, got %d", config.MaxJobsPerNode, unmarshaledConfig.MaxJobsPerNode)
	}
	if config.LocalityKey != unmarshaledConfig.LocalityKey {
		t.Errorf("Expected LocalityKey %q, got %q", config.LocalityKey, unmarshaledConfig.LocalityKey)
	}
	if config.LocalityLabelSelector != unmarshaledConfig.LocalityLabelSelector {
		t.Errorf("Expected LocalityLabelSelector %q, got %q", config.LocalityLabelSelector, unmarshaledConfig.LocalityLabelSelector)
	}
}

func TestTaskOutput_1NFStructure(t *testing.T) {
	partitionIndex := 2
	output := TaskOutput{
		OutputID:       100,
		TaskID:         uuid.New(),
		PartitionIndex: &partitionIndex,
		OutputURI:      "s3://bucket/path/to/output",
		Checksum:       "abcdef123456",
	}

	data, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("Failed to marshal TaskOutput: %v", err)
	}

	var jsonParsed map[string]interface{}
	if err := json.Unmarshal(data, &jsonParsed); err != nil {
		t.Fatalf("Failed to unmarshal TaskOutput into map: %v", err)
	}

	if jsonParsed["outputId"].(float64) != float64(output.OutputID) {
		t.Errorf("Expected outputId %d in json, got %v", output.OutputID, jsonParsed["outputId"])
	}
	if jsonParsed["partitionIndex"].(float64) != float64(*output.PartitionIndex) {
		t.Errorf("Expected partitionIndex %d in json, got %v", *output.PartitionIndex, jsonParsed["partitionIndex"])
	}
}

func TestTaskAttempt_AttemptStatus(t *testing.T) {
	attempt := TaskAttempt{
		AttemptID:     uuid.New(),
		TaskID:        uuid.New(),
		WorkerID:      "worker-1",
		LeaseID:       uuid.New(),
		LastRenewedAt: time.Now(),
		LeaseTTL:      30,
		StartTime:     time.Now(),
		Status:        AttemptRunning,
	}

	if attempt.Status != AttemptRunning {
		t.Errorf("Expected attempt status %s, got %s", AttemptRunning, attempt.Status)
	}

	attempt.Status = AttemptSuccess
	if attempt.Status != AttemptSuccess {
		t.Errorf("Expected status change to %s, got %s", AttemptSuccess, attempt.Status)
	}
}

func TestJob_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		status   JobStatus
		terminal bool
	}{
		{"Pending is not terminal", JobPending, false},
		{"Running is not terminal", JobRunning, false},
		{"Cleaning is not terminal", JobCleaning, false},
		{"Completed is terminal", JobCompleted, true},
		{"Failed is terminal", JobFailed, true},
		{"Cancelled is terminal", JobCancelled, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := Job{
				JobID:  uuid.New(),
				UserID: uuid.New(),
				Status: tc.status,
			}

			if got := job.IsTerminal(); got != tc.terminal {
				t.Errorf("Job.IsTerminal() for status %q = %v, want %v", tc.status, got, tc.terminal)
			}
		})
	}
}

func TestNodeConfigRequest_Serialization(t *testing.T) {
	req := NodeConfigRequest{
		MaxPods:     20,
		CPULimit:    "500m",
		MemoryLimit: "1Gi",
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal WorkerConfigRequest: %v", err)
	}

	var unmarshaled NodeConfigRequest
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal NodeConfigRequest: %v", err)
	}

	if unmarshaled.MaxPods != 20 {
		t.Errorf("Expected MaxPods 20, got %d", unmarshaled.MaxPods)
	}
	if unmarshaled.CPULimit != "500m" {
		t.Errorf("Expected CPULimit '500m', got %q", unmarshaled.CPULimit)
	}
	if unmarshaled.MemoryLimit != "1Gi" {
		t.Errorf("Expected MemoryLimit '1Gi', got %q", unmarshaled.MemoryLimit)
	}

	// Verify JSON keys match the DDS schema naming convention
	var jsonMap map[string]interface{}
	if err := json.Unmarshal(data, &jsonMap); err != nil {
		t.Fatalf("Failed to unmarshal into map: %v", err)
	}

	expectedKeys := []string{"maxPods", "cpuLimit", "memoryLimit"}
	for _, key := range expectedKeys {
		if _, ok := jsonMap[key]; !ok {
			t.Errorf("Expected JSON key %q to be present", key)
		}
	}
}

func TestTaskInput_Serialization(t *testing.T) {
	taskID := uuid.New()
	input := TaskInput{
		InputAssignmentID: 42,
		TaskID:            taskID,
		InputURI:          "s3://bucket/mapreduce-inputs/split-0.jsonl",
		ByteStart:         0,
		ByteEnd:           65536,
		SplitChecksum:     "sha256-abc123",
	}

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("Failed to marshal TaskInput: %v", err)
	}

	var unmarshaled TaskInput
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal TaskInput: %v", err)
	}

	if unmarshaled.InputAssignmentID != 42 {
		t.Errorf("Expected InputAssignmentID 42, got %d", unmarshaled.InputAssignmentID)
	}
	if unmarshaled.TaskID != taskID {
		t.Errorf("Expected TaskID %s, got %s", taskID, unmarshaled.TaskID)
	}
	if unmarshaled.InputURI != "s3://bucket/mapreduce-inputs/split-0.jsonl" {
		t.Errorf("Expected InputURI 's3://bucket/mapreduce-inputs/split-0.jsonl', got %q", unmarshaled.InputURI)
	}
	if unmarshaled.ByteStart != 0 {
		t.Errorf("Expected ByteStart 0, got %d", unmarshaled.ByteStart)
	}
	if unmarshaled.ByteEnd != 65536 {
		t.Errorf("Expected ByteEnd 65536, got %d", unmarshaled.ByteEnd)
	}
	if unmarshaled.SplitChecksum != "sha256-abc123" {
		t.Errorf("Expected SplitChecksum 'sha256-abc123', got %q", unmarshaled.SplitChecksum)
	}

	// Verify JSON keys match the camelCase convention
	var jsonMap map[string]interface{}
	if err := json.Unmarshal(data, &jsonMap); err != nil {
		t.Fatalf("Failed to unmarshal into map: %v", err)
	}

	expectedKeys := []string{"inputAssignmentId", "taskId", "inputUri", "byteStart", "byteEnd", "splitChecksum"}
	for _, key := range expectedKeys {
		if _, ok := jsonMap[key]; !ok {
			t.Errorf("Expected JSON key %q to be present", key)
		}
	}
}

func TestJobConfig_Serialization(t *testing.T) {
	jobID := uuid.New()
	config := JobConfig{
		JobID:         jobID,
		InputURI:      "s3://bucket/mapreduce-inputs/data.jsonl",
		MapperURI:     "s3://code/mapper.py",
		ReducerURI:    "s3://code/reducer.py",
		CombinerURI:   "s3://code/combiner.py",
		MTasks:        10,
		RTasks:        5,
		InputChecksum: "sha256-xyz789",
	}

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to marshal JobConfig: %v", err)
	}

	var unmarshaled JobConfig
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal JobConfig: %v", err)
	}

	if unmarshaled.JobID != jobID {
		t.Errorf("Expected JobID %s, got %s", jobID, unmarshaled.JobID)
	}
	if unmarshaled.MapperURI != "s3://code/mapper.py" {
		t.Errorf("Expected MapperURI 's3://code/mapper.py', got %q", unmarshaled.MapperURI)
	}
	if unmarshaled.CombinerURI != "s3://code/combiner.py" {
		t.Errorf("Expected CombinerURI 's3://code/combiner.py', got %q", unmarshaled.CombinerURI)
	}
	if unmarshaled.MTasks != 10 {
		t.Errorf("Expected MTasks 10, got %d", unmarshaled.MTasks)
	}
	if unmarshaled.RTasks != 5 {
		t.Errorf("Expected RTasks 5, got %d", unmarshaled.RTasks)
	}
	if unmarshaled.InputChecksum != "sha256-xyz789" {
		t.Errorf("Expected InputChecksum 'sha256-xyz789', got %q", unmarshaled.InputChecksum)
	}

	// Verify JSON keys match the camelCase convention from the DDS schema
	var jsonMap map[string]interface{}
	if err := json.Unmarshal(data, &jsonMap); err != nil {
		t.Fatalf("Failed to unmarshal into map: %v", err)
	}

	expectedKeys := []string{"jobId", "inputUri", "mapperUri", "reducerUri", "combinerUri", "mTasks", "rTasks", "inputChecksum"}
	for _, key := range expectedKeys {
		if _, ok := jsonMap[key]; !ok {
			t.Errorf("Expected JSON key %q to be present", key)
		}
	}
}
