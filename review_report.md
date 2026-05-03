# KubeMapReduce Security and Logic Code Review


[SEVERITY: CRITICAL] Task Allocation Row Lock Contention
File: manager-service/internal/manager/queries.go:110
Type: Logic
Issue: `QuerySelectTaskForUpdate` uses `FOR UPDATE`, but lacks the `SKIP LOCKED` clause required for concurrent task schedulers.
Impact: Concurrent managers contending for the same task attempt will experience deadlocks or excessive lock waits, degrading cluster throughput.
Fix: Add `SKIP LOCKED` to the SQL query.

[SEVERITY: HIGH] Env var leakage to child process
File: worker-service/internal/worker/exec.go:21
Type: Security
Issue: The `credentialEnvPrefixes` checks block `MINIO_ACCESS` and `MINIO_SECRET` but leaves out `MINIO_ENDPOINT`, which is passed to workers in the Kubernetes manifests. Furthermore, `DATABASE_DSN` is not completely blocked if renamed.
Impact: A user-supplied map/reduce script can read the internal `MINIO_ENDPOINT` environment variable and use it for network reconnaissance.
Fix: Add `MINIO_ENDPOINT` to `credentialEnvPrefixes`.

[SEVERITY: HIGH] JWT Skew Window
File: auth-service/pkg/auth/jwt.go:80
Type: Security
Issue: The JWT parser configuration is missing the `jwt.WithLeeway()` option to account for clock skew.
Impact: Tokens generated immediately by Keycloak might be rejected as invalid if the auth-service clock is slightly behind.
Fix: Add `jwt.WithLeeway(5 * time.Minute)` to the parsing options.

[SEVERITY: HIGH] MinIO pre-signed URLs Path Traversal
File: manager-service/internal/api/handlers.go:1037
Type: Security
Issue: The `validateUploadKey` checks for `.` and `..` by doing equality checking (`filename == "." || filename == ".."`). However, if someone passes `...` or `a..b`, it bypasses the check.
Impact: Attackers can potentially write to arbitrary keys in the input bucket.
Fix: Strictly validate that `filename` conforms to a safe regex pattern instead of relying on limited string checks.

[SEVERITY: HIGH] FNV-1a Routing Affinity Loss
File: manager-service/internal/manager/routing.go:15
Type: Logic
Issue: `ComputeReplicaIndex` hashes the jobID with FNV-1a modulo `totalReplicas`. If `totalReplicas` changes mid-job (e.g. StatefulSet scales up/down), the `jobID` hashes to a different replica.
Impact: State loss for jobs actively in progress, leading to hung jobs and incorrect results.
Fix: Implement consistent hashing or persist the assigned replica index at job creation.

[SEVERITY: MEDIUM] gRPC mTLS Unauthenticated Flag
File: worker-service/cmd/worker/main.go:36
Type: Security
Issue: The worker allows skipping RPC tokens using `GRPC_INSECURE=true`.
Impact: A misconfigured environment may bypass authentication, allowing unauthenticated RPCs from arbitrary workers.
Fix: Remove or secure the `GRPC_INSECURE` override path.

[SEVERITY: LOW] `http.Client` explicit timeouts
File: cli-service/cmd/cli/jobs.go:420
Type: BestPractice
Issue: `downloadShard` uses `http.Get(rawURL)` which instantiates a default HTTP client without an explicit timeout.
Impact: If the object storage server hangs or drops packets, the CLI goroutine will leak and hang indefinitely.
Fix: Use a configured `http.Client` with an explicit timeout.

## Summary of Findings


| Severity | Type         | File                                              | Issue                                                       |
|----------|--------------|---------------------------------------------------|-------------------------------------------------------------|
| CRITICAL | Logic        | `manager-service/internal/manager/queries.go`     | `SELECT FOR UPDATE SKIP LOCKED` missing on task allocation |
| HIGH     | Security     | `worker-service/internal/worker/exec.go`          | Env var leakage (Missing MINIO_ENDPOINT)                   |
| HIGH     | Security     | `auth-service/pkg/auth/jwt.go`                    | Missing skew window (`jwt.WithLeeway`)                     |
| HIGH     | Security     | `manager-service/internal/api/handlers.go`        | MinIO pre-signed URLs path traversal / bucket scoping      |
| HIGH     | Logic        | `manager-service/internal/manager/routing.go`     | FNV-1a routing failure when replica count changes          |
| MEDIUM   | Security     | `worker-service/cmd/worker/main.go`               | gRPC mTLS `GRPC_INSECURE` flag allowed unauthenticated     |
| LOW      | BestPractice | `cli-service/cmd/cli/jobs.go`                     | `http.Client` explicit timeouts missing (`http.Get`)       |
