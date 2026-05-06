# KubeMapReduce Test Audit Report

As requested, here is the comprehensive analysis and audit of the testing strategy for the KubeMapReduce platform, analyzing gaps across multiple dimensions of distributed system testing, and providing a concrete path forward.

## 1. Missing Test Matrix

| Category | Component/Area | Current State | Gap/Deficiency | Severity |
| :--- | :--- | :--- | :--- | :--- |
| **Unit Test** | `worker-service/internal/worker/storage.go` | Mock storage is tested, but `GetObject` / `PutObject` implementations have 0% coverage. | Missing test coverage for MinIO storage interactions and their error paths. | High |
| **Unit Test** | `worker-service/internal/worker/map.go` | Overall coverage is ~80%, but `sortRecordsSpilling` is at 60%. | Edge cases around large dataset spilling thresholds are not fully covered. | Medium |
| **Unit Test** | `auth-service` / `cli-service` | Commands have basic tests (~40% coverage). | Insufficient mock coverage for CLI edge cases (e.g., token refresh logic under intermittent network failure). | Medium |
| **Integration Test**| `manager-service` <-> `worker-service` | Tested primarily via mocked gRPC clients. | No real in-memory/localhost gRPC integration test combining actual manager and worker networking layers. | High |
| **Integration Test**| `k8s.io/client-go` orchestration | Uses `fake.NewSimpleClientset()` extensively. | Lack of integration tests against a real or `envtest` Kubernetes API server. | Medium |
| **Distributed Failures**| General System Resilience | Covered by `e2e_failure_injection.sh` (Worker Kill, Manager Restart, Zombie Fencing). | No automated injection of network partitions, delayed packets, or dropped gRPC connections (e.g., via Linkerd fault injection). | High |
| **Concurrency / Race**| Manager `Scheduler` & `Orchestrator` | Unit tests exist with standard assertions, and CI runs `go test -race`. | Tests do not simulate highly concurrent scenarios (e.g., hundreds of simultaneous submissions/assignments) needed to effectively trigger race conditions in the Scheduler. | High |
| **MinIO Failure** | Storage layer | Happy path MinIO usage in E2E. | What happens if MinIO returns 503s mid-upload, or drops connections during `PutObject`? Missing retry/backoff assertion tests. | High |
| **Auth / Security** | JWT Validation & Keycloak | Tested via `net/http/httptest` JWKS mocks. | No tests simulating Keycloak downtime, JWT tampering, or token expiry exactly mid-flight during long-running streaming RPCs. | High |
| **Load / Stress** | System throughput | `benchmarks` dir exists but no formalized stress testing. | Manager performance degradation with 1000+ concurrent workers and massive DDS queue buildup is untested. | Critical |

---

## 2. Prioritized Test Plan

**Phase 1: High-ROI Unit & Race Condition Fixes (Weeks 1-2)**
1. **Fix missing unit coverage**: Write tests for `storage.go` MinIO error paths, `sortRecordsSpilling` edge cases, and CLI token refresh logic.
2. **Add Concurrent Test Scenarios**: Write tests that spawn 100+ concurrent goroutines against the Manager and Scheduler APIs. This ensures the existing `-race` CI check actually encounters contention and validates state transitions properly.

**Phase 2: Integration & MinIO Fault Tolerance (Weeks 3-4)**
1. **MinIO Fault Injection Tests**: Implement integration tests using `toxiproxy` or custom `http.RoundTripper` mocks to simulate slow uploads, timeouts, and connection resets to MinIO, ensuring the worker handles backoff/retries gracefully.
2. **API Server `envtest`**: Replace `fake.NewSimpleClientset()` in critical tests with `sigs.k8s.io/controller-runtime/pkg/envtest` for accurate Kubernetes API behavior.

**Phase 3: Formalized E2E & Load Testing (Weeks 5-6)**
1. **Migrate Bash E2E to Go**: Port `e2e_failure_injection.sh` into a formalized Go-based E2E framework (e.g., Ginkgo or KUTTL) to enable parallel execution and better failure reporting.
2. **Implement Load Testing**: Create a `k6` or `locust` script that submits 100s of MapReduce jobs simultaneously, measuring API latency, Manager resource utilization, and DDS row lock contention.

**Phase 4: Advanced Chaos Engineering (Weeks 7-8)**
1. **Network Partitions & Security**: Use Linkerd fault injection or Chaos Mesh to simulate network partitions between Keycloak, the Manager, and Workers. Assert that security policies (JWT expiries, mTLS) are enforced correctly under duress.

---

## 3. Concrete Test Cases/Examples

### Test Case 1: MinIO Connection Reset Mid-Upload (Integration)
**Description:** Verify that a worker retries a failed output chunk upload if MinIO drops the connection.
**Implementation:**
- Spin up MinIO in a test container.
- Place a proxy (like Toxiproxy) in front of MinIO.
- Start worker `Reduce` task.
- Instruct proxy to reset the TCP connection after 50% of the bytes are sent.
- **Assertion:** Worker logs a retry, successfully reconnects, finishes the upload, and does *not* send a `TaskFailed` RPC to the Manager.

### Test Case 2: Concurrency Race on Scheduler Assignment
**Description:** Verify that 100 concurrent task requests don't cause double-assignment of the same chunk.
**Implementation:**
- Initialize `Manager` with an in-memory SQLite/Postgres container.
- Spawn 100 goroutines that simultaneously hit the `Register` gRPC endpoint.
- **Assertion:** Run with `go test -race`. Assert that the database row locks (`SELECT ... FOR UPDATE SKIP LOCKED`) successfully dole out exactly 100 unique tasks, with 0 duplicates.

### Test Case 3: JWT Expiry Mid-Flight (Security)
**Description:** Verify that if a user's CLI token expires while the CLI is streaming a large input file to the pre-signed MinIO URL, the job submission fails gracefully or refreshes.
**Implementation:**
- Mock the Keycloak JWKS server to dispense a token expiring in 1 second.
- Throttle the CLI's upload speed.
- Wait for 2 seconds.
- Attempt to call `POST /api/v1/jobs`.
- **Assertion:** The API rejects the request with 401 Unauthorized. The CLI automatically attempts to use its refresh token, obtains a new JWT, and completes the submission successfully.

### Test Case 4: Load Testing the DDS (Stress)
**Description:** Determine the maximum job ingestion rate before HTTP 503/429 limits are hit or the Scheduler stalls.
**Implementation:**
- Use `k6` to simulate 500 active CLI users.
- Each user authenticates and continuously submits sleep jobs (e.g., `sleep_job.json`).
- **Assertion:** The API p99 latency remains under 500ms. Keycloak does not crash. PostgreSQL connection pool limits are respected.

---

## 4. Suggested Testing Tooling/Frameworks

| Tool | Purpose | Recommendation Rationale |
| :--- | :--- | :--- |
| **Testcontainers-Go** | Integration Tests | Replaces Docker Compose for integration testing. Allows spinning up ephemeral Postgres, Keycloak, and MinIO instances dynamically from within the Go tests. |
| **Ginkgo & Gomega** | BDD / E2E Testing | Ideal for replacing `e2e_failure_injection.sh`. Provides rich matchers, asynchronous assertions (`Eventually()`), and clean test structuring for K8s E2E tests. |
| **Toxiproxy** | Failure Simulation | A TCP proxy developed by Shopify. Excellent for simulating MinIO network degradation, Keycloak latency, or Postgres connection drops programmatically from tests. |
| **k6 (by Grafana)** | Load & Stress Testing | Scriptable in JavaScript, very fast, integrates well with CI. Perfect for simulating thousands of concurrent API requests or gRPC calls to the Manager. |
| **Chaos Mesh** / **Litmus** | Kubernetes Chaos | For advanced infrastructure testing. Can automatically delete manager pods, introduce network delays between worker nodes, and test Linkerd circuit breakers in a live cluster. |
| **sigs.k8s.io/controller-runtime/pkg/envtest** | K8s API Mocking | Superior to `fake.NewSimpleClientset()` because it runs a real API server and etcd locally without a full kubelet. Captures complex behaviors like Finalizers and Server-Side Apply. |
