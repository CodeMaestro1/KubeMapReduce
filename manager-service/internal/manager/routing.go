package manager

import (
	"errors"
	"hash/fnv"
)

// ComputeReplicaIndex computes the FNV-1a hash of the jobID to determine
// the authoritative Manager replica for the job.
//
// In a multi-replica Manager deployment (StatefulSet), this function ensures
// that all tasks for a specific job are handled by the same Manager instance.
// This simplifies state management and prevents race conditions between replicas
// trying to schedule the same job's tasks.
func ComputeReplicaIndex(jobID string, totalReplicas int) (int, error) {
	if totalReplicas <= 0 {
		return 0, errors.New("totalReplicas must be greater than zero")
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(jobID))
	hashValue := h.Sum32()
	return int(hashValue % uint32(totalReplicas)), nil
}
