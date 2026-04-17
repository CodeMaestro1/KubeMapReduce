# GitHub Copilot Instructions — KubeMapReduce Platform

> **INF-419 Distributed Systems Project**  
> Technical University of Crete — School of ECE  
> Supervisor: Prof. Vasilios Samoladas

---

## Project Overview

This is a **microservice-based, distributed MapReduce computing platform** deployed on Kubernetes. The system follows a three-layer architecture (UI → Manager → Workers) designed for fault tolerance and horizontal scalability. All core services are written in **Go (Golang 1.24+)**.

---

## Repository Structure

```
kubemapreduce/
├── cli-service/
│   └── cmd/cli/                     # UI service entrypoint and CLI logic
├── manager-service/
│   ├── cmd/api/                     # Manager/API service entrypoint
│   ├── internal/{api,config,manager,models,validation}
│   └── pkg/httputil/                # Manager shared HTTP helpers
├── auth-service/
│   ├── cmd/setup/                   # Keycloak bootstrap/setup entrypoint
│   └── pkg/auth/                    # Auth/token/JWT/Keycloak helpers
├── k8s/                             # Kubernetes manifests
├── infra/                           # Infra/config/deploy assets
└── .github/workflows/               # CI/CD workflows
```

---

## Build & Test Commands

**Build all services:**
```bash
go build -o bin/cli ./cli-service/cmd/cli
go build -o bin/api ./manager-service/cmd/api
go build -o bin/setup ./auth-service/cmd/setup
```

**Run all tests with coverage:**
```bash
go test -v -race -coverprofile=coverage.out ./...
```

**Run tests for a specific service:**
```bash
go test -v ./cli-service/...
go test -v ./manager-service/...
go test -v ./auth-service/...
```

**Run a single test by pattern:**
```bash
go test -v -run TestNameOrPattern ./...
```

**Lint and vet:**
```bash
go vet ./...
go fmt ./...
```

**Regenerate Protobuf stubs (after editing proto/mapreduce.proto):**
```bash
protoc --go_out=. --go-grpc_out=. proto/mapreduce.proto
```

---

## Core Technology Stack

| Component | Technology | Notes |
|-----------|-----------|-------|
| Language | Go 1.24+ | All services: UI, Manager, Worker, CLI |
| Auth / SSO | Keycloak | OAuth2 Password Grant; JWT via `golang-jwt/jwt/v5` + `MicahParks/keyfunc/v3` |
| State Database | PostgreSQL | Row-level locking (`SELECT … FOR UPDATE SKIP LOCKED`) |
| Shared Storage | MinIO | S3-compatible; pre-signed PUT/GET URLs for zero-trust access |
| Internal RPC | gRPC + Protobuf (proto3) | HTTP/2 multiplexed streams for heartbeats |
| External API | REST / HTTP | Standard `net/http` + `http.ServeMux` — **no Gin or Echo** |
| Orchestration | Kubernetes (K8s) | StatefulSet (Manager), Deployment (UI), Jobs (Workers) |
| External Access | Kubernetes Gateway API | HTTPRoutes for `api.`, `storage.`, `auth.` domains |
| Containerization | Docker | Distroless for UI/Manager; Ubuntu + JRE + Python3 + GCC for Worker |

---

## Coding Standards

### General Go Practices
- Use **idiomatic Go**: prefer explicit error handling over panics; use `errors.Is` / `errors.As` for error wrapping.
- All exported identifiers must have **GoDoc comments**.
- Use **`context.Context`** as the first argument in every function that performs I/O or calls an external service.
- Prefer **table-driven tests** (`t.Run`) for unit tests.
- No global mutable state; inject dependencies via struct constructors.
- Use **`os/signal`** for graceful shutdown; always handle `SIGTERM` and `SIGINT`.

### HTTP (UI Service)
- Use the standard `net/http` stack and `http.ServeMux` — **do not use Gin, Echo, or Chi**.
- Every handler must validate **Bearer JWT** via the shared `auth` middleware before processing.
- Return structured JSON error bodies: `{"error": "<message>", "code": "<HTTP status>"}`
- Use **HTTP status codes precisely** as specified in API contract (202, 200, 201, 204, etc.).

### gRPC (Manager ↔ Worker)
- All RPC definitions live in `proto/mapreduce.proto`. **Do not hand-write gRPC service code** — regenerate from proto.
- Always pass `lease_id` and `attempt_id` in heartbeat and completion RPCs for fencing validation.
- Manager must reject `TaskComplete` or `TaskFailed` RPC if `attempt_id != TASKS.current_attempt_id` in the DDS.
- Missing **3 consecutive heartbeats** within a 30-second window triggers the Active Reaper (K8s pod DELETE + task reset).

### PostgreSQL / DDS
- All task-assignment queries **must use** `SELECT … FOR UPDATE SKIP LOCKED` to prevent double-scheduling.
- Schema changes go in numbered SQL migration files under `migrations/`; never alter schema in application code.
- `TASKS.current_attempt_id` is a **deliberate denormalization** — update atomically alongside `TASK_ATTEMPTS` inside a single transaction.
- Lease expiry is computed at runtime as `last_renewed_at + lease_ttl`; do not pre-compute.

### MinIO / Storage
- Client code must **never embed** MinIO credentials. Inject via Kubernetes Secrets / env vars (`S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_ENDPOINT`).
- External uploads/downloads use **pre-signed URLs only**.
- File structure: `temp/` (temporary uploads), `inputs/` (input files), `staging/<job_id>/` (shuffle), `outputs/<job_id>/` (final results).
- After job terminal state, Manager **must** bulk-delete `staging/<job_id>/` before finalizing.

---

## Key Architectural Patterns

### Manager Load Balancing (FNV-1a Hashing)
```go
replicaIndex := fnv1a(jobID) % numReplicas
managerAddr := fmt.Sprintf("manager-%d.manager-headless.%s.svc.cluster.local", replicaIndex, namespace)
```
**Do not change the hash function.**

### Attempt-Based Commit Protocol
- Every Worker receives unique `attempt_id` at startup.
- Worker writes output to: `s3://bucket/staging/<job_id>/<task_id>/<attempt_id>.json`
- Manager validates `attempt_id` against `TASKS.current_attempt_id` before committing.
- **Stale attempts are silently rejected** — never overwrite newer attempt output.

### Lease-Based Locking (Zombie Fencing)
- On task assignment, Manager writes row to `TASK_ATTEMPTS` with `lease_id` (UUID) and `lease_ttl`.
- Workers renew lease on every `Heartbeat` RPC.
- Expired leases cause DDS to reject commit — even if zombie worker reconnects later.

### Record Boundary Handling (JSONL Splits)
1. If `byte_start > 0`, read byte at `byte_start - 1`. If not `\n`, skip to next `\n` before processing.
2. Always read **past** `byte_end` until next `\n` to avoid truncating the last record.

### Resource Quota Enforcement
- Before spawning K8s Job, Manager queries `SYSTEM_CONFIG.max_concurrent_pods` and counts `Running` tasks.
- If new pod exceeds limit, leave task `Idle` and retry next scheduling tick.

### gRPC Message Scalability (Manifest Pattern)
If serialized `data_locations` list for Reducer exceeds **2 MB**:
1. Serialize list to JSON manifest object.
2. Upload manifest to MinIO.
3. Set `data_locations` to single manifest URI; set `is_manifest = true`.
4. Worker detects `is_manifest` and fetches full list via HTTP GET.

---

## DDS Schema Reference

**Key tables** (keep in sync with `migrations/`):

```sql
JOBS            (job_id UUID PK, user_id UUID, status ENUM, created_at, updated_at)
JOB_CONFIGS     (job_id UUID FK PK, input_uri, mapper_uri, reducer_uri, combiner_uri,
                 m_tasks INT, r_tasks INT, input_checksum)
TASKS           (task_id UUID PK, job_id UUID FK, task_type ENUM, status ENUM,
                 replica_index INT, current_attempt_id UUID FK nullable)
TASK_INPUTS     (input_assignment_id BIGINT PK, task_id UUID FK, input_uri,
                 byte_start BIGINT, byte_end BIGINT, split_checksum)
TASK_ATTEMPTS   (attempt_id UUID PK, task_id UUID FK, worker_id, lease_id UUID,
                 last_renewed_at TIMESTAMP, lease_ttl INT, start_time, end_time, status ENUM)
TASK_OUTPUTS    (output_id BIGINT PK, task_id UUID FK, partition_index INT,
                 output_uri, checksum)
SYSTEM_CONFIG   (config_id INT PK, max_concurrent_pods INT, cpu_limit, memory_limit,
                 updated_at)
```

**Status enumerations:**
- `JOBS.status`: `Pending | Running | Completed | Cancelled | Failed | Cleaning`
- `TASKS.status`: `Idle | In-Progress | Completed | Failed`
- `TASK_ATTEMPTS.status`: `Running | Success | Failed`

---

## gRPC Contract

Source: `proto/mapreduce.proto`

```protobuf
service WorkerService {
  rpc Register     (RegisterRequest)     returns (TaskAssignment);
  rpc Heartbeat    (HeartbeatRequest)    returns (HeartbeatResponse);
  rpc TaskComplete (TaskCompleteRequest) returns (Ack);
  rpc TaskFailed   (TaskFailedRequest)   returns (Ack);
}
```

**Key messages:** `TaskAssignment` carries `task_id`, `attempt_id`, `job_id`, `type` (MAP/REDUCE), `code_location`, `data_locations[]`, `byte_start`, `byte_end`, `partition_id`, `is_manifest`, `total_reducers`, `split_checksum`.

**HeartbeatResponse.Action**: `CONTINUE | TERMINATE`

**All RPCs must include `lease_id` for fencing.**

---

## REST API Reference

### UI Service (public-facing)

| Method | Endpoint | Status | Auth |
|--------|----------|--------|------|
| POST | `/api/v1/jobs` | 202 | User JWT |
| GET | `/api/v1/jobs` | 200 | User JWT |
| GET | `/api/v1/jobs/{job_id}` | 200 | User JWT |
| DELETE | `/api/v1/jobs/{job_id}` | 204 | User JWT |
| POST | `/api/v1/uploads/presigned` | 200 | User JWT |
| POST | `/api/v1/downloads/presigned` | 200 | User JWT |
| POST | `/api/v1/admin/users` | 201 | Admin JWT |
| DELETE | `/api/v1/admin/users/{username}` | 204 | Admin JWT |
| POST | `/api/v1/admin/config/workers` | 200 | Admin JWT |

### Manager Service (internal)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/internal/schedule` | Trigger orchestration for new job |
| DELETE | `/internal/jobs/{job_id}` | Cancel job, delete K8s pods immediately |

---

## Data Format

All user-facing input/output is **JSON Lines (JSONL)**. Each line is a single JSON object:

```jsonl
{"key": "document_1", "value": "hello world"}
```

- Workers pipe JSONL into user code via **stdin**.
- User code emits JSONL to **stdout**.
- Exit code `0` = success; non-zero = application-level failure.
- **Supported code:** `.py` (python3), `.jar` (java -jar), `.c`/`.cpp` (compiled with GCC/G++).

---

## Worker Execution Flow

```
1. gRPC Register(task_id, attempt_id) → receive TaskAssignment
2. Download code from MinIO → /tmp/execution/
3. If C/C++: compile with g++ -O3; chmod +x
4. Download input split (byte-range GET from MinIO)
5. Validate SHA-256 against split_checksum from TaskAssignment
6. Spawn user code as child process via os/exec
7. Pipe JSONL stream → stdin; capture stdout
8. [MAP] Sort output by key in memory (spill to disk if > threshold)
9. [MAP + Combiner] Pipe sorted output into combiner process stdin
10. [MAP] Hash-partition output into R buckets; upload to staging/<job_id>/<task_id>/<attempt_id>/
11. [REDUCE] External Merge Sort via min-heap; batch 500 streams at a time
12. Upload final result to outputs/<job_id>/
13. gRPC TaskComplete(task_id, attempt_id, lease_id, output_locations[], output_checksums[])
```

---

## Fault Tolerance Checklist

When implementing any component, ensure:

- [ ] Worker catches `SIGTERM` via `os/signal` and calls `gRPC TaskFailed` before exiting.
- [ ] Manager's Active Reaper deletes K8s Job + Pod after 3 missed heartbeats.
- [ ] All task queries use `SELECT … FOR UPDATE SKIP LOCKED`.
- [ ] `TaskComplete` validates `attempt_id == TASKS.current_attempt_id`.
- [ ] Lease TTL check happens inside same DB transaction as commit.
- [ ] Manager enters `Cleaning` phase before marking terminal state; bulk-deletes `staging/<job_id>/`.
- [ ] Orphaned `temp/` objects handled by bucket lifecycle policy (24h TTL) — not by application.
- [ ] Worker reads past `byte_end` to finish current JSONL record.
- [ ] Manager injects `backoffLimit: 0` and `restartPolicy: Never` into all Worker Job manifests.
- [ ] SHA-256 checksums validated at Map-read phase and again at Shuffle-merge phase.

---

## Authentication

- **CLI** handles all token refresh logic; UI Service is **stateless** — validates Bearer tokens per request.
- Token refresh uses `grant_type=refresh_token` with 30-second skew window.
- UI Service validates Admin role from JWT claims before delegating to Keycloak Admin API.
- Internal services (Manager, MinIO) **never** receive or validate user JWTs. Trust is delegated (pre-signed URLs, K8s Secrets).
- Tokens stored locally at `.mapreduce/config.json` with `chmod 600` permissions.

---

## Kubernetes Deployment

- **Manager**: `StatefulSet`, headless service, 3 replicas by default. DNS: `manager-{i}.manager-headless.<namespace>.svc.cluster.local`
- **UI**: `Deployment` (stateless, horizontally scalable)
- **Workers**: Dynamic `Job` resources with `backoffLimit: 0`, `restartPolicy: Never`
- **PostgreSQL**: `ReadWriteOnce` PVC
- **MinIO**: `ReadWriteMany` PVC if multi-node
- **Probes**: `GET /healthz` (liveness); `GET /readyz` (readiness) on UI and Manager
- **External routing**: Kubernetes Gateway API with three `HTTPRoute` resources for `api.`, `storage.`, `auth.mapreduce.local`
- **TLS termination**: At the Gateway
- **MinIO credentials**: Injected via `secretKeyRef` from `minio-creds` Secret — never hardcoded

---

## What Copilot Should NOT Do

- Do not use web frameworks (Gin, Echo, Fiber) — use `net/http` only.
- Do not use `database/sql` directly for schema changes — use migration files.
- Do not store MinIO credentials anywhere except Kubernetes Secrets.
- Do not implement token refresh on server side — it belongs in CLI.
- Do not use Kubernetes native retry (`backoffLimit > 0`) — Manager owns fault tolerance.
- Do not use JSON arrays for file locations in DDS — use `TASK_OUTPUTS` table (1NF).
- Do not change the FNV-1a hashing algorithm for replica selection.
- Do not add new gRPC RPCs without updating `proto/mapreduce.proto` first.
