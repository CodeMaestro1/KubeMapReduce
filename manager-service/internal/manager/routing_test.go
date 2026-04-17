package manager

import (
	"testing"
)

func TestComputeReplicaIndex(t *testing.T) {
	tests := []struct {
		jobID         string
		totalReplicas int
		expected      int
		expectErr     bool
	}{
		{"c9c8e88e-6c0b-4b2a-8d1e-2b5f7e6f8a9d", 3, 1, false}, // Deterministic test 1
		{"a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d", 5, 0, false}, // Deterministic test 2
		{"00000000-0000-0000-0000-000000000000", 1, 0, false},
		{"some-job-id", 0, 0, true},
		{"some-job-id", -1, 0, true},
	}

	for _, tt := range tests {
		got, err := ComputeReplicaIndex(tt.jobID, tt.totalReplicas)
		if tt.expectErr {
			if err == nil {
				t.Errorf("expected error for totalReplicas %d, got nil", tt.totalReplicas)
			}
			continue
		}
		if err != nil {
			t.Errorf("unexpected error for %s: %v", tt.jobID, err)
		}
		if got != tt.expected {
			t.Errorf("ComputeReplicaIndex(%s, %d) = %d; want %d", tt.jobID, tt.totalReplicas, got, tt.expected)
		}
	}
}
