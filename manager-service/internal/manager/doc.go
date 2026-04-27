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
//
// # Scheduling and Quota Algorithm
//
// [Scheduler.GetNextTask] is the single entry point for assigning work. It is
// safe to call concurrently from any number of Manager replicas or goroutines
// because every step runs inside a single PostgreSQL transaction with explicit
// row-level and advisory locks. The algorithm is:
//
//  1. BEGIN transaction (default isolation: READ COMMITTED, sufficient because
//     all contended rows are taken under FOR UPDATE).
//  2. Verify the job has no Failed tasks via [QueryCountFailedTasks]. If it
//     does, the job is irrecoverable and the call returns [ErrJobFailed].
//  3. Phase ordering: Map tasks are always preferred over Reduce tasks. Reduce
//     scheduling is gated on every Map task being Completed. This is the
//     starvation-prevention mechanism for the Map phase: Reduce tasks cannot
//     overtake an unfinished Map phase.
//  4. Idle-task selection ([QuerySelectIdleTask]) uses
//     "FOR UPDATE SKIP LOCKED" so concurrent callers never observe the same
//     candidate row. Rows are returned in deterministic FIFO order by
//     (replica_index, task_id). Because FIFO is enforced by the SQL ordering
//     clause, no client-side fairness logic is needed and starvation of any
//     individual idle task is bounded by the number of pending sibling tasks.
//  5. Quota enforcement ([Scheduler.enforceQuotaTx]) acquires the
//     transaction-scoped advisory lock 42 ([QueryAcquireSchedulingLock]) so
//     all replicas serialize their max_concurrent_pods check. Inside the
//     critical section it reads SYSTEM_CONFIG.max_concurrent_pods
//     ([QueryGetMaxConcurrentPods]) and the current count of Running attempts
//     ([QueryCountRunningAttempts]). If active_pods >= max_pods the
//     transaction is rolled back with [ErrQuotaExceeded] and the candidate
//     task remains Idle (it will be picked up by a later scheduling tick).
//  6. On success the task row is flipped to In-Progress and a fresh
//     TASK_ATTEMPTS row is inserted, both inside the same transaction so the
//     attempt count visible to step 5 is exact.
//  7. COMMIT.
//
// # Fairness Policy
//
// The fairness policy is FIFO with phase ordering:
//   - All Map tasks of a job complete before any Reduce task is scheduled.
//   - Within a phase, idle tasks are returned in ascending (replica_index,
//     task_id) order. Because task_id is a v4 UUID assigned at job submission
//     time, the ordering is stable across replicas and reproducible in tests.
//   - Quota waiters never block: an over-quota call returns [ErrQuotaExceeded]
//     immediately and is expected to retry. There is no in-process queue,
//     which keeps the policy stateless and survives Manager crashes.
//
// # Concurrency Guarantees
//
//   - No double-scheduling: FOR UPDATE SKIP LOCKED guarantees a row is visible
//     to at most one transaction at a time.
//   - No oversubscription: pg_advisory_xact_lock(42) serializes the read of
//     max_concurrent_pods and the insert of the new TASK_ATTEMPTS row, so the
//     Running count is always observed as a consistent snapshot.
//   - No phase inversion: the pendingMapTasks check is performed inside the
//     same transaction that would assign a Reduce task, so a Map task that
//     transitions Idle -> In-Progress concurrently is observed by the Reduce
//     branch.
package manager
