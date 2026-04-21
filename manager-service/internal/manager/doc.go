// Package manager provides the core scheduling and orchestration logic for MapReduce jobs.
//
// # Overview
// This package is responsible for the entire lifecycle of a MapReduce job, from initial
// task decomposition and assignment to final result aggregation and resource cleanup.
// It manages a fleet of workers running on Kubernetes, ensuring that work is distributed
// efficiently while maintaining strict fault tolerance and consistency.
//
// # Design Rationale
// The manager is designed around a "lease-based fencing" architecture. Every task assignment
// is bound to a unique attempt ID and a time-limited lease. This design prevents "split-brain"
// scenarios where a slow or partitioned worker might attempt to commit results after a
// replacement worker has already been assigned. All state transitions are persisted in a
// Distributed Data Store (DDS) using atomic transactions to ensure linearizability.
//
// # Key Types
// - [Scheduler]: The central coordinator that handles task assignment, heartbeats, and failures.
// - [WorkerOrchestrator]: An interface for spawning and terminating physical worker processes.
// - [Task]: A struct representing a single unit of work (Map or Reduce) and its current state.
// - [TaskState]: An enumeration of the possible lifecycle stages of a task.
//
// # Thread Safety
// The [Scheduler] is safe for concurrent use. It relies on PostgreSQL's row-level locking
// (FOR UPDATE SKIP LOCKED) and transaction-scoped advisory locks to synchronize actions
// across multiple Manager replicas.
//
// # Error Handling
// Callers should expect errors like [ErrStaleAttempt] when a worker's lease has expired,
// or [ErrQuotaExceeded] when the cluster cannot accommodate more workers. Most errors
// are sentinel values that can be checked with errors.Is.
package manager
