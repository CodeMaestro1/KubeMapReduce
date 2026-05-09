package manager

import (
	"testing"
)

// authoritativeScheduler builds a minimal Scheduler for authoritativeManagerAddr tests.
func authoritativeScheduler(replicaIndex, totalReplicas int, managerAddr string) *Scheduler {
	return &Scheduler{
		replicaIndex:  replicaIndex,
		totalReplicas: totalReplicas,
		managerAddr:   managerAddr,
	}
}

func TestAuthoritativeManagerAddr(t *testing.T) {
	// Known hash values from TestComputeReplicaIndex:
	//   "c9c8e88e-6c0b-4b2a-8d1e-2b5f7e6f8a9d" % 3 = 1
	//   "00000000-0000-0000-0000-000000000000" % 1 = 0
	jobTo1 := "c9c8e88e-6c0b-4b2a-8d1e-2b5f7e6f8a9d" // hashes to replica 1 with 3 replicas
	jobTo0 := "00000000-0000-0000-0000-000000000000" // hashes to replica 0 with 1 replica

	tests := []struct {
		name          string
		replicaIndex  int
		totalReplicas int
		managerAddr   string
		jobID         string
		want          string
	}{
		{
			name:          "job routes to different replica — substitutes ordinal",
			replicaIndex:  0,
			totalReplicas: 3,
			managerAddr:   "manager-0.manager-headless.mapreduce.svc.cluster.local:50051",
			jobID:         jobTo1,
			want:          "manager-1.manager-headless.mapreduce.svc.cluster.local:50051",
		},
		{
			name:          "job routes to same replica — returns own address",
			replicaIndex:  1,
			totalReplicas: 3,
			managerAddr:   "manager-1.manager-headless.mapreduce.svc.cluster.local:50051",
			jobID:         jobTo1,
			want:          "manager-1.manager-headless.mapreduce.svc.cluster.local:50051",
		},
		{
			name:          "job routes to replica 1 from replica 2",
			replicaIndex:  2,
			totalReplicas: 3,
			managerAddr:   "manager-2.manager-headless.mapreduce.svc.cluster.local:50051",
			jobID:         jobTo1,
			want:          "manager-1.manager-headless.mapreduce.svc.cluster.local:50051",
		},
		{
			name:          "single replica — always returns own address",
			replicaIndex:  0,
			totalReplicas: 1,
			managerAddr:   "manager-0.manager-headless.mapreduce.svc.cluster.local:50051",
			jobID:         jobTo0,
			want:          "manager-0.manager-headless.mapreduce.svc.cluster.local:50051",
		},
		{
			name:          "bad totalReplicas — falls back to own address",
			replicaIndex:  0,
			totalReplicas: 0,
			managerAddr:   "manager-0.manager-headless.mapreduce.svc.cluster.local:50051",
			jobID:         jobTo1,
			want:          "manager-0.manager-headless.mapreduce.svc.cluster.local:50051",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authoritativeScheduler(tt.replicaIndex, tt.totalReplicas, tt.managerAddr)
			got := s.authoritativeManagerAddr(tt.jobID)
			if got != tt.want {
				t.Errorf("authoritativeManagerAddr() = %q, want %q", got, tt.want)
			}
		})
	}
}

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
