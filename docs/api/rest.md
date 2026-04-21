# REST API Reference

> Detailed documentation for the KubeMapReduce UI/Manager Service REST API.

---

## GET /

**Auth:** None  
**Response:** 200 OK

Basic discovery endpoint to verify the API is running and reachable.

### Response Body
| Field | Type | Description |
|---|---|---|
| name | string | Service name ("KubeMapReduce API") |
| status | string | Current service health ("running") |
| message | string | High-level instruction for users |

---

## GET /health

**Auth:** None  
**Response:** 200 OK

Shallow health check for monitoring systems and the CLI.

### Response Body
| Field | Type | Description |
|---|---|---|
| status | string | Service health ("ok") |

---

## POST /jobs

**Auth:** User JWT (Bearer)  
**Response:** 202 Accepted

Submits a new MapReduce job. This is a metadata-only submission: the system assumes the input data and code artifacts are already present in shared storage.

### Request Body
| Field | Type | Required | Description |
|---|---|---|---|
| filename | string | ✓ | Name of the input data file in shared storage |
| mapper | object | ✓ | [FunctionSpec] for the mapping phase |
| reducer | object | ✓ | [FunctionSpec] for the reduction phase |
| combiner | object | | [FunctionSpec] for optional local combining |
| reducers | int | | Number of reducer tasks (default: 1) |

#### FunctionSpec
| Field | Type | Description |
|---|---|---|
| language | string | Runtime language (e.g., "python", "java") |
| artifact | string | Filename of the executable code |
| entrypoint | string | Name of the function to invoke |
| interface | string | Human-readable signature of the function |

### Response Body
| Field | Type | Description |
|---|---|---|
| jobId | UUID | Unique identifier for tracking progress |
| status | string | Initial job status ("accepted") |
| message | string | Confirmation message |

### Error Responses
| Status | Code | When |
|---|---|---|
| 400 | validation_error | Missing required field or invalid language |
| 401 | unauthorized | Missing or expired JWT |

---

## GET /jobs

**Auth:** User JWT (Bearer)  
**Response:** 200 OK

Retrieves a paginated list of all MapReduce jobs in the system, ordered by creation time (newest first).

### Query Parameters
| Parameter | Type | Default | Description |
|---|---|---|---|
| limit | int | 100 | Number of records to return (max 500) |
| offset | int | 0 | Number of records to skip |

### Response Body (Array)
| Field | Type | Description |
|---|---|---|
| jobId | UUID | Unique job identifier |
| status | string | Current status (e.g., Pending, Running, Completed) |
| message | string | Human-readable status description |
| filename | string | Input filename |
| reducers | int | Configured reducer count |
| createdAt | datetime | ISO-8601 creation timestamp |

---

## GET /jobs/{job_id}

**Auth:** User JWT (Bearer)  
**Response:** 200 OK

Retrieves detailed status and metadata for a specific job.

### Response Body
*Same fields as the items in [GET /jobs].*

### Error Responses
| Status | Code | When |
|---|---|---|
| 404 | not_found | Job ID does not exist |

---

## GET /jobs/{job_id}/results

**Auth:** User JWT (Bearer)  
**Response:** 200 OK (currently 501 Not Implemented)

Streams the aggregated results of a completed job.

### Error Responses
| Status | Code | When |
|---|---|---|
| 501 | not_implemented | Backend streaming is pending integration |
| 404 | not_found | Job ID does not exist |

---

## PUT /admin/workers/config

**Auth:** Admin JWT (Bearer)  
**Response:** 202 Accepted

Updates the global configuration for the worker fleet.

### Request Body
| Field | Type | Required | Description |
|---|---|---|---|
| workerReplicas | int | ✓ | Number of workers to maintain per job phase |
| maxJobsPerNode | int | ✓ | Maximum number of worker pods per K8s node |

---

## PUT /admin/nodes/config

**Auth:** Admin JWT (Bearer)  
**Response:** 200 OK (currently 501 Not Implemented)

Updates per-node resource limits for the compute cluster.

### Request Body
| Field | Type | Required | Description |
|---|---|---|---|
| maxPods | int | ✓ | Maximum pod density allowed on any node |
| cpuLimit | string | ✓ | CPU quota (e.g., "500m", "1.0") |
| memoryLimit | string | ✓ | Memory quota (e.g., "1Gi", "512Mi") |

---

## POST /admin/users

**Auth:** Admin JWT (Bearer)  
**Response:** 201 Created

Provisions a new user in the Keycloak identity provider.

### Request Body
| Field | Type | Required | Description |
|---|---|---|---|
| username | string | ✓ | Unique username |
| email | string | ✓ | User's email address |
| password | string | ✓ | Initial password |
| role | string | ✓ | System role ("USER" or "ADMIN") |

---

## DELETE /admin/users/{username}

**Auth:** Admin JWT (Bearer)  
**Response:** 204 No Content

Removes a user identity from the system.
