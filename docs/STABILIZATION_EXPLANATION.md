# KubeMapReduce Stabilization: Minikube E2E & Hardening

This document explains the rationale behind the changes made to stabilize the KubeMapReduce platform for local development on Minikube and ensure a reliable E2E failure injection test suite.

## 📦 Kubernetes Manifests (k8s/)

### 10-postgres.yaml
- **Change**: Replaced `command: ["postgres", ...]` with `args: [...]`.
- **Rationale**: The official PostgreSQL Docker image uses an entrypoint script (`docker-entrypoint.sh`) to initialize the database (run `initdb`) if the data directory is empty. Overriding `command` skips this script, causing the container to crash because it can't find its own data directory. Using `args` allows the entrypoint to run correctly.

### 20-minio.yaml
- **Change**: Changed Service type to `NodePort` and explicitly set `nodePort: 30900`.
- **Rationale**: For local development on Minikube, using a static `NodePort` makes MinIO reachable from the host machine (CLI) without requiring a persistent `kubectl port-forward` session. It also matches the expectations of the `create-secrets.sh` script, which calculates the S3 endpoint based on the Minikube IP and this port.

### 25-keycloak.yaml
- **Change**: Updated readiness and liveness probes to be more lenient.
- **Rationale**: Keycloak can take significant time to warm up and initialize its internal caches and database connections. Lenient probes prevent the container from being prematurely killed during a slow startup on resource-constrained local environments.

### 30-manager.yaml & 35-api.yaml
- **Change**: Updated `image` to `kubemapreduce/manager:latest` (and `api:latest`) and set `imagePullPolicy: IfNotPresent`.
- **Rationale**: Ensures the cluster uses the local images built within the Minikube Docker daemon (`eval $(minikube docker-env)`) rather than trying to pull stale or non-existent images from a public registry.
- **Change (Manager)**: Set `HEARTBEAT_INTERVAL_SEC: "2"` and `MAX_MISSED_HEARTBEATS: "5"`.
- **Rationale**: Reduces the time the manager takes to detect a dead or partitioned worker from 40s down to 12s. This is critical for keeping E2E tests fast and responsive.

### 51-manager-cluster-rbac.yaml
- **Change**: Added `ClusterRole` and `ClusterRoleBinding` for `nodes` access.
- **Rationale**: The Manager requires permission to list cluster nodes to implement Data Locality scheduling (pairing workers with MinIO pods based on topology labels). Since nodes are cluster-scoped, a regular `Role` is insufficient.

---

## 🗄️ Database Migrations (migrations/)

### 0002_add_locality_config.sql
- **Change**: Added `locality_label_selector` column to `SYSTEM_CONFIG`.
- **Rationale**: The Manager service logic was refactored to support dynamic discovery of data nodes (MinIO) via label selectors. The SQL migration was missing this specific column, causing the Manager to crash on startup when attempting to load the system configuration.

---

## ⚙️ Service Logic & E2E Tests

### e2e/failure_injection_test.go
- **Change**: Implemented `waitForWorkerPodsReady` helper.
- **Rationale**: Prevents race conditions where tests attempt to inject failures (like `SIGSTOP`) into worker pods that are still in `ContainerCreating` or `Pending` status.
- **Change**: Reduced `ManagerRestartScenario` and `ZombieFencingScenario` wait times.
- **Rationale**: By tuning the heartbeat parameters in the manifests, we can safely reduce the test sleep timers, bringing the total E2E suite duration from 10 minutes down to 3 minutes.

### worker-service/internal/worker/worker.go
- **Change**: Added a retry loop for the initial gRPC stream connection.
- **Rationale**: During scale-up, workers might attempt to connect to the Manager before its Service is fully discoverable via DNS. Retrying handles these transient network startup delays gracefully.

### manager-service/cmd/api/main.go
- **Change**: Integrated `ensureMinIOBuckets` call on startup.
- **Rationale**: Automates the creation of the 5 required MinIO buckets (`mapreduce-inputs`, etc.). This eliminates the "opaque" S3 errors that previously required manual `mc mb` commands to fix.

### Log Level Normalization
- **Change**: Updated `loggerWriter` in API, Manager, and Worker to use `slog.LevelError` for `log.Fatalf` bridges.
- **Rationale**: Previously, critical crashes logged via the standard `log` package were being swallowed as `INFO` logs in the structured output, making them invisible to log aggregators and debugging tools.
