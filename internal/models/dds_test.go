package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSystemConfig_Serialization(t *testing.T) {
	config := SystemConfig{
		ConfigID:          1,
		MaxConcurrentPods: 10,
		CPULimit:          "500m",
		MemoryLimit:       "1Gi",
		UpdatedAt:         time.Now().Truncate(time.Second), // Truncate so JSON marshal/unmarshal comparisons are stable
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

func TestTaskAttempt_LeaseExpired(t *testing.T) {
	// Active lease: renewed just now with a 30-second TTL
	active := TaskAttempt{
		AttemptID:     uuid.New(),
		TaskID:        uuid.New(),
		WorkerID:      "worker-1",
		LeaseID:       uuid.New(),
		LastRenewedAt: time.Now(),
		LeaseTTL:      30,
		StartTime:     time.Now(),
		Status:        AttemptRunning,
	}

	if active.LeaseExpired() {
		t.Fatal("expected active lease (renewed just now with 30s TTL) to NOT be expired")
	}

	// Expired lease: renewed 60 seconds ago with a 30-second TTL
	expired := TaskAttempt{
		AttemptID:     uuid.New(),
		TaskID:        uuid.New(),
		WorkerID:      "worker-2",
		LeaseID:       uuid.New(),
		LastRenewedAt: time.Now().Add(-60 * time.Second),
		LeaseTTL:      30,
		StartTime:     time.Now().Add(-60 * time.Second),
		Status:        AttemptRunning,
	}

	if !expired.LeaseExpired() {
		t.Fatal("expected lease (renewed 60s ago with 30s TTL) to be expired")
	}

	// Edge case: zero TTL means the lease expires immediately
	zeroTTL := TaskAttempt{
		AttemptID:     uuid.New(),
		TaskID:        uuid.New(),
		WorkerID:      "worker-3",
		LeaseID:       uuid.New(),
		LastRenewedAt: time.Now().Add(-1 * time.Millisecond),
		LeaseTTL:      0,
		StartTime:     time.Now(),
		Status:        AttemptRunning,
	}

	if !zeroTTL.LeaseExpired() {
		t.Fatal("expected zero-TTL lease to be expired")
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

func TestWorkerConfigRequest_Serialization(t *testing.T) {
	req := WorkerConfigRequest{
		MaxConcurrentPods: 20,
		CPULimit:          "500m",
		MemoryLimit:       "1Gi",
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal WorkerConfigRequest: %v", err)
	}

	var unmarshaled WorkerConfigRequest
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal WorkerConfigRequest: %v", err)
	}

	if unmarshaled.MaxConcurrentPods != 20 {
		t.Errorf("Expected MaxConcurrentPods 20, got %d", unmarshaled.MaxConcurrentPods)
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

	expectedKeys := []string{"maxConcurrentPods", "cpuLimit", "memoryLimit"}
	for _, key := range expectedKeys {
		if _, ok := jsonMap[key]; !ok {
			t.Errorf("Expected JSON key %q to be present", key)
		}
	}
}
