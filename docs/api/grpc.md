# gRPC API Reference

This document describes the gRPC contract between the **Worker Service** (implemented by the Manager) and the **Worker Pods**.

## Overview of Fencing

KubeMapReduce uses a two-level fencing strategy to protect against split-brain scenarios caused by stale or partitioned workers:

1.  **`attempt_id` (Task-Level):** When a worker is assigned a task, it receives a unique `attempt_id` (a UUID). This ID is stored in the DDS as `TASKS.current_attempt_id`. The Manager will reject any RPC (`Heartbeat`, `TaskComplete`, `TaskFailed`) where the provided `attempt_id` does not match the one currently in the DDS.
2.  **`lease_id` (Attempt-Level):** Each attempt is also assigned a `lease_id`. This ID is used to validate that the worker's session is still active and has not been timed out by the Active Reaper.

---

## WorkerService

### Register

```protobuf
rpc Register(RegisterRequest) returns (TaskAssignment)
```

Called by a Worker immediately after startup. The Worker identifies itself using the `task_id` and `attempt_id` it received from its environment variables.

The Manager uses this call to assign the Worker its specific task metadata, returning a `TaskAssignment` that includes the `lease_id` and the locations of the code and data to process.

#### Fields: RegisterRequest
| Field | Type | Description |
|---|---|---|
| task_id | string (UUID) | The task the worker is claiming. |
| attempt_id | string (UUID) | The specific attempt identifier received from the environment. |

#### Fields: TaskAssignment
| Field | Type | Description |
|---|---|---|
| task_id | string | Echoes the assigned task ID. |
| attempt_id | string | Echoes the assigned attempt ID. |
| lease_id | string | The identifier for the worker's active lease. |
| type | TaskType | Whether this is a `MAP` or `REDUCE` task. |
| data_locations | repeated string | URIs for the input data (input files for Map, intermediate shards for Reduce). |
| code_location | string | URI for the mapper or reducer binary/script. |
| total_reducers | int32 | Total number of partitions (R), used by Map workers for hashing. |

---

### Heartbeat

```protobuf
rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse)
```

Called periodically by the Worker to signal it is still alive and making progress.

**Fencing check:**
The Manager performs a transactional check:
1.  Does `TASKS.current_attempt_id` match the request's `attempt_id`?
2.  Does the `lease_id` match the one stored for that attempt?
3.  Is the lease still within its TTL window in the DDS?

If any check fails, the Manager returns a `TERMINATE` action, and the Worker must immediately shut down.

#### Fields: HeartbeatRequest
| Field | Type | Description |
|---|---|---|
| task_id | string | The ID of the task being performed. |
| attempt_id | string | The unique attempt ID for fencing. |
| lease_id | string | The active lease identifier. |

#### Fields: HeartbeatResponse
| Field | Type | Description |
|---|---|---|
| action | Action | `CONTINUE` (keep working) or `TERMINATE` (stop immediately). |

---

### TaskComplete

```protobuf
rpc TaskComplete(TaskCompleteRequest) returns (Ack)
```

Called by the Worker upon successful completion of its assigned task.

**Fencing check:**
The Manager will only commit the results to the DDS if the `attempt_id` and `lease_id` are still valid. If they match, the task status is set to `Completed`, and its output URIs are persisted for use in the next phase.

#### Fields: TaskCompleteRequest
| Field | Type | Description |
|---|---|---|
| task_id | string | The ID of the completed task. |
| attempt_id | string | The unique attempt ID for fencing. |
| lease_id | string | The active lease identifier. |
| output_locations | repeated string | URIs of the generated output files (e.g., intermediate shards). |
| output_checksums | repeated string | SHA-256 hashes of the output files. |

---

### TaskFailed

```protobuf
rpc TaskFailed(TaskFailedRequest) returns (Ack)
```

Called by the Worker if it encounters an unrecoverable error during execution.

**Fencing check:**
Similar to `TaskComplete`, the failure is only recorded if the `attempt_id` and `lease_id` are current. This prevents a stale worker's failure from triggering a premature job-wide abort if a replacement worker is already successfully running.

#### Fields: TaskFailedRequest
| Field | Type | Description |
|---|---|---|
| task_id | string | The ID of the failed task. |
| attempt_id | string | The unique attempt ID for fencing. |
| lease_id | string | The active lease identifier. |
| error_message | string | A description of why the task failed. |
