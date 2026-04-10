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
