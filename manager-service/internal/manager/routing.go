package manager

import (
	"errors"
	"hash/fnv"
)

// ComputeReplicaIndex computes the FNV-1a hash of the jobID to determine
// the authoritative Manager replica for the job.
func ComputeReplicaIndex(jobID string, totalReplicas int) (int, error) {
	if totalReplicas <= 0 {
		return 0, errors.New("totalReplicas must be greater than zero")
	}
	h := fnv.New32a()
	h.Write([]byte(jobID))
	hashValue := h.Sum32()
	return int(hashValue % uint32(totalReplicas)), nil
}
