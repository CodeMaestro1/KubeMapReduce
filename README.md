# KubeMapReduce

KubeMapReduce is a distributed MapReduce platform for Kubernetes. It provides a CLI for users, a Manager for scheduling and fault tolerance, worker pods for execution, Keycloak-based authentication, PostgreSQL as DDS state, and MinIO for object storage.

## Architecture at a glance

- **CLI service**: user/admin commands (`login`, `jobs submit`, `admin create-user`, etc.)
- **Manager API**: REST endpoints for job and admin operations
- **Manager gRPC server**: worker registration, heartbeats, completion/failure reporting
- **Workers**: dynamic Kubernetes Jobs spawned per task attempt
- **Auth**: Keycloak + JWT validation
- **Storage**: PostgreSQL (state) + MinIO (input/shuffle/output objects)

## Repository layout

```text
cli-service/         CLI entrypoint and command handlers
manager-service/     API, scheduler/orchestrator, gRPC worker server
worker-service/      Worker runtime/execution pipeline
auth-service/        Keycloak bootstrap and auth helpers
proto/               gRPC/protobuf contract (mapreduce.proto)
k8s/                 Kubernetes manifests
migrations/          Numbered PostgreSQL schema migrations
docs/                Architecture, deployment, and operations guides
infra/               Dockerfiles and local infra assets
```

## Prerequisites

- Go 1.26+
- Docker + Docker Compose (local infra)
- `kubectl` (Kubernetes deployment / Minikube flow)
- A Kubernetes cluster (required for real worker execution)

## Local development (infra smoke path)

This path is useful for API/auth/storage integration checks. Real MapReduce execution still requires Kubernetes worker Jobs.

1. Copy environment template:

```bash
cp infra/docker/.env.example infra/docker/.env
```

2. Start infra stack:

```bash
cd infra/docker
docker compose up -d
```

3. Apply migrations:

```powershell
Get-Content migrations\0001_initial_schema.sql | docker exec -i mapreduce-postgres psql -U mapreduce -d mapreduce
```

4. Create initial admin user:

```bash
go run ./auth-service/cmd/setup --admin-password admin --username platform-admin --email platform-admin@example.com --prompt-password --role ADMIN
```

5. Verify CLI/API path:

```bash
go run ./cli-service/cmd/cli login --username platform-admin
go run ./cli-service/cmd/cli health
go run ./cli-service/cmd/cli jobs list
```

For full local Kubernetes execution, use **[docs/MINIKUBE_LOCAL_DEV.md](docs/MINIKUBE_LOCAL_DEV.md)**.

## Kubernetes deployment

For full deployment details and production-oriented setup:

- **[docs/CLUSTER_DEPLOYMENT.md](docs/CLUSTER_DEPLOYMENT.md)**
- **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)**
- **[k8s/README.md](k8s/README.md)**

Core behavior:

- Manager runs as a StatefulSet
- Workers are created dynamically as `batch/v1` Jobs
- API/UI are stateless services
- Gateway API handles external routing (`api`, `storage`, `auth`)

## Build and quality commands

```bash
go build -o bin/cli ./cli-service/cmd/cli
go build -o bin/manager ./manager-service/cmd/manager
go build -o bin/api ./manager-service/cmd/api
go build -o bin/worker ./worker-service/cmd/worker
go build -o bin/auth-setup ./auth-service/cmd/setup
```

This enables:
- **Automatic mTLS** between all pods (24h certificate rotation)
- **Per-RPC method timeouts** (Heartbeat 2s, Register 5s, TaskComplete/TaskFailed 10s)
- **Retry policies** with exponential backoff (configurable per RPC)
- **Circuit breaking** for external dependencies (PostgreSQL, MinIO, MinIO clients)
- **Prometheus metrics** and Linkerd dashboard for traffic visualization

See [Linkerd Setup Guide](docs/LINKERD_SETUP.md) for detailed instructions and [Timeout Configuration](docs/TIMEOUT_CONFIGURATION.md) for tuning guidelines.

## Repository Layout

This repository now follows a polyrepo-ready microservice layout:

```text
cli-service/      # UI service (CLI entrypoint + CLI logic)
manager-service/  # manager API, scheduler, task/state orchestration
auth-service/     # authentication/bootstrap logic and auth libraries
k8s/              # Kubernetes manifests
infra/            # infra/deploy assets (docker-compose, env files, etc.)
```

Service code is isolated under the service folders so each service can be extracted into its own repository later.

## Authentication and Roles

The CLI authenticates with Keycloak using **username/password** (Resource Owner
Password Credentials grant) and stores a dual-token pair (short-lived access
token + longer-lived refresh token) in a local credentials file. Tokens are
refreshed automatically when they expire.

- API role rules:
  - `/jobs`: `USER` or `ADMIN`
  - `/admin/*`: `ADMIN` only

### Architecture

All admin user management flows through the API server:

```
Admin -> CLI -> API Server (POST /api/v1/admin/users, DELETE /api/v1/admin/users/{username}) -> Keycloak
```

The API server holds the Keycloak admin credentials and proxies create/delete
requests. The CLI never talks directly to Keycloak for user management — it
only needs a valid JWT with the `ADMIN` role.

## CLI Usage

```
kubemapreduce <command> [flags]

Commands:
  login                         Authenticate with Keycloak and store tokens
  logout                        Clear stored authentication tokens
  whoami                        Show the currently logged-in user
  health                        Check API server health
  jobs submit                   Upload code/input files and submit a MapReduce job
  jobs list                     List all submitted jobs
  jobs status --id <id>         Show the status of a specific job
  jobs download --id <id>       Download completed job results
  jobs cancel --id <id>         Cancel a running or submitted job
  admin create-user             Create a user in Keycloak (ADMIN)
  admin delete-user             Delete a user from Keycloak (ADMIN)
  admin worker-config           Update worker configuration (ADMIN)
  admin configure-nodes         Set per-node resource limits (ADMIN)
  token inspect                 Show raw JWT claims for the stored access token
  help                          Show this help message
```

### Login

```bash
go run ./cli-service/cmd/cli login
# Username: platform-admin
# Password: ********
# Login successful!
# Credentials saved to ~/.config/kubemapreduce/credentials.json
```

You can also pass `--username platform-admin` to skip the username prompt.

### Submit a Job

The CLI uploads mapper, reducer, and input files to MinIO via pre-signed PUT URLs, then submits the job to the API. A Kubernetes cluster is required for the workers to actually execute.

```bash
go run ./cli-service/cmd/cli jobs submit \
  --mapper  mapper.py \
  --reducer reducer.py \
  --input   input.jsonl \
  --reducers 1

# Check status
go run ./cli-service/cmd/cli jobs list
go run ./cli-service/cmd/cli jobs status --id <job-id>

# Download results when completed
go run ./cli-service/cmd/cli jobs download --id <job-id> --output ./results/
```

Supported code languages: `.py` (Python), `.java`/`.jar` (Java), `.c` (C), `.cpp`/`.cc`/`.cxx` (C++).

### Admin Commands

Admin commands require the `ADMIN` role. User management routes through the
API server, which proxies to Keycloak:

```bash
# Create a user (prompts for the new user's password)
go run ./cli-service/cmd/cli admin create-user --username bob --email bob@example.com --prompt-password --role USER

# Delete a user
go run ./cli-service/cmd/cli admin delete-user --username bob

# Update worker configuration
go run ./cli-service/cmd/cli admin worker-config --replicas 4 --max-jobs 8
```

All three commands check the local token for the `ADMIN` role before making
the request. The API server re-validates the JWT and enforces the role
server-side as well.

### Who Am I

```bash
go run ./cli-service/cmd/cli whoami
# Username: platform-admin
# Email:    platform-admin@example.com
# Subject:  a1b2c3d4-...
# Roles:    ADMIN, default-roles-mapreduce
```

### Token Inspect

Dump the full raw JWT claims (useful for debugging):

```bash
go run ./cli-service/cmd/cli token inspect
```

## Documentation

Comprehensive guides for deploying, configuring, and troubleshooting KubeMapReduce:

### Core Documentation

- **[Cluster Deployment Checklist](docs/CLUSTER_DEPLOYMENT.md)** — ⚡ **START HERE** — 10-step production cluster deployment checklist with copy-paste commands
- **[Deployment Guide](docs/DEPLOYMENT.md)** — Complete step-by-step deployment guide for both local Docker Compose and production GKE clusters
- **[Benchmarking Report](docs/PRESENTATION_TESTS.md)** — Summary of performance metrics, scalability analysis, and fault-tolerance verification
- **[Extended Benchmarks](docs/EXTENDED_BENCHMARKS.md)** — In-depth architectural proofs, concurrency stress tests, and system recovery benchmarks
- **[External Access Guide](docs/EXTERNAL_ACCESS.md)** — How to expose the API to external clients (minikube Ingress, production Gateway API, NodePort, port forwarding)
- **[Linkerd Setup Guide](docs/LINKERD_SETUP.md)** — Service mesh installation and configuration for automatic mTLS and per-RPC timeouts (production-recommended)
- **[Timeout Configuration Guide](docs/TIMEOUT_CONFIGURATION.md)** — Detailed tuning guidelines for timeout values, retry strategies, and circuit breaker settings
- **[Architecture Documentation](docs/ARCHITECTURE.md)** — System design, component interactions, and fault tolerance patterns

### Quick References

- **API Reference** — REST and gRPC API specifications (see `proto/mapreduce.proto` for RPC definitions)
- **Troubleshooting** — Common issues and solutions in [Deployment Guide](docs/DEPLOYMENT.md#troubleshooting)
- **Security** — Authentication, authorization, and TLS configuration in relevant guides

### Development

- **Build all services:** `make` or `make build`
- **Build specific services:** `make cli`, `make manager`, `make api`, `make worker`, `make auth-setup`
- **Run tests:** `make test`
- **Format code:** `make fmt`
- **Vet code:** `make vet`
- **Lint code:** `make lint` (requires golangci-lint)
- **Clean build artifacts:** `make clean`

### CI/PR Quality Gate

Before opening or updating a PR, run the same checks used by CI:

```bash
go fmt ./...
go vet ./...
go mod tidy
go test -v -race -coverprofile=coverage.out ./...
govulncheck ./...
```

Equivalent Make targets:

```bash
make build
make fmt
make vet
make test
make lint
```

## CLI Commands Reference

```text
kubemapreduce login
kubemapreduce logout
kubemapreduce health
kubemapreduce jobs submit|list|status|download|cancel
kubemapreduce whoami
kubemapreduce admin create-user|delete-user|worker-config|configure-nodes
kubemapreduce token inspect
```

Environment defaults used by the CLI:

- `API_URL` (default `http://localhost:8081`)
- `KEYCLOAK_BASE_URL` (default `http://localhost:8080`)
- `KEYCLOAK_REALM` (default `mapreduce`)
- `KEYCLOAK_AUDIENCE` (default `mapreduce-api`)

## Token Storage

After login, credentials are stored in a platform-appropriate config directory:

- **Windows**: `%APPDATA%\kubemapreduce\credentials.json`
- **Linux/macOS**: `$XDG_CONFIG_HOME/kubemapreduce/credentials.json` (defaults to `~/.config`)

The file is created with `0600` permissions. The stored structure:

```json
{
  "access_token": "eyJ...",
  "refresh_token": "eyJ...",
  "expires_at": 1682349600,
  "server_url": "http://localhost:8081"
}
```

When the access token expires, the CLI silently refreshes it using the refresh
token. If the refresh token itself has expired, the user is prompted to log in
again.

For request base URL resolution, the CLI uses deterministic precedence:

1. `server_url` from stored credentials, when present.
2. `API_URL` environment variable (or `http://localhost:8081` if unset).

Legacy credentials that do not contain `server_url` are supported. The CLI
falls back to `API_URL` and best-effort persists the resolved value back to
the credentials file for future runs.

## Public REST API (manager-service API)

All protected endpoints require a `Authorization: Bearer <token>` header.

| Method   | Path                                  | Auth              | Description                            |
| -------- | ------------------------------------- | ----------------- | -------------------------------------- |
| `GET`    | `/healthz`                            | None              | Liveness check                         |
| `GET`    | `/readyz`                             | None              | Readiness check                        |
| `POST`   | `/api/v1/jobs`                        | `USER` or `ADMIN` | Submit a MapReduce job                 |
| `GET`    | `/api/v1/jobs`                        | `USER` or `ADMIN` | List all jobs                          |
| `GET`    | `/api/v1/jobs/{job_id}`               | `USER` or `ADMIN` | Get job status                         |
| `DELETE` | `/api/v1/jobs/{job_id}`               | `USER` or `ADMIN` | Cancel a job (204 No Content)          |
| `POST`   | `/api/v1/uploads/presigned`           | `USER` or `ADMIN` | Get pre-signed PUT URL for file upload |
| `POST`   | `/api/v1/downloads/presigned`         | `USER` or `ADMIN` | Get pre-signed GET URLs for results    |
| `POST`   | `/api/v1/admin/users`                 | `ADMIN`           | Create a user in Keycloak              |
| `DELETE` | `/api/v1/admin/users/{username}`      | `ADMIN`           | Delete a user from Keycloak            |
| `POST`   | `/api/v1/admin/config/workers`        | `ADMIN`           | Update worker configuration            |
| `GET`    | `/api/v1/admin/config/workers`        | `ADMIN`           | Get current worker configuration       |

`DELETE /api/v1/admin/users/{username}` returns **204 No Content** with no body. The username is in the path because some HTTP clients and proxies do not support bodies on `DELETE` requests.

## gRPC contract (manager <-> worker)

Defined in `proto/mapreduce.proto`:

- `Register`
- `Heartbeat`
- `TaskComplete`
- `TaskFailed`

Regenerate stubs after editing proto:

```bash
protoc --go_out=. --go-grpc_out=. proto/mapreduce.proto
```

## Documentation

- **Architecture**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Data locality architecture**: [docs/ARCHITECTURE_DATA_LOCALITY.md](docs/ARCHITECTURE_DATA_LOCALITY.md)
- **External access architecture**: [docs/ARCHITECTURE_EXTERNAL_ACCESS.md](docs/ARCHITECTURE_EXTERNAL_ACCESS.md)
- **Cluster deployment checklist**: [docs/CLUSTER_DEPLOYMENT.md](docs/CLUSTER_DEPLOYMENT.md)
- **Minikube local flow**: [docs/MINIKUBE_LOCAL_DEV.md](docs/MINIKUBE_LOCAL_DEV.md)
- **Linkerd setup**: [docs/LINKERD_SETUP.md](docs/LINKERD_SETUP.md)
- **Timeout tuning**: [docs/TIMEOUT_CONFIGURATION.md](docs/TIMEOUT_CONFIGURATION.md)
- **Monitoring/results**: [docs/MONITORING_AND_RESULTS.md](docs/MONITORING_AND_RESULTS.md)
- **Migrations guide**: [migrations/README.md](migrations/README.md)

## Benchmarking & Verification

KubeMapReduce includes a comprehensive benchmarking suite and failure-injection tests to validate the system's scalability and resilience.

### Performance Benchmarks
To run the automated performance benchmarks on a live cluster:
1. Ensure the CLI is configured and logged in.
2. Run the benchmark script:
   ```bash
   python benchmarks/distributed_benchmark.py
   ```
For detailed dataset preparation instructions and result analysis, see [benchmarks/README.md](benchmarks/README.md).

### E2E Failure Validation (INF-419)

KubeMapReduce includes a dedicated end-to-end failure-injection suite to validate the system's resilience against infrastructure failures, as required for the INF-419 Principles of Distributed Systems deliverable.

### Prerequisites

- A running Kubernetes cluster (Kind, Minikube, or EKS/GKE).
- `kubectl` configured to point to the cluster.
- KubeMapReduce deployed to the cluster with the following resources:
  - Manager: a StatefulSet named `manager`
  - Worker Jobs: labeled with `app=kubemapreduce-worker`
- Active CLI authentication (`kubemapreduce login`).

### Running the Validation Suite

The suite automates three high-impact failure scenarios:
1. **Worker Pod Kill**: Deletes a worker pod mid-execution to verify task reassignment.
2. **Manager Restart**: Restarts the manager StatefulSet to verify recovery of active attempts from the DDS.
3. **Zombie Fencing**: Pauses a worker (SIGSTOP) until its lease expires, then resumes it (SIGCONT) to verify that its subsequent commit attempt is rejected by the manager.

Execute the suite from the repository root:

```bash
./scripts/e2e_failure_injection.sh
```

### Reports and Artifacts

Each run generates a timestamped report directory in `./reports/`. This directory contains:
- `suite.log`: Detailed execution trace with timestamps.
- `zombie_worker.log`: Captured logs from the fenced worker showing rejection errors (e.g., `PermissionDenied`).

These artifacts provide the necessary evidence for the final validation report and presentation.

## Documentation

- **Architecture**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Data locality architecture**: [docs/ARCHITECTURE_DATA_LOCALITY.md](docs/ARCHITECTURE_DATA_LOCALITY.md)
- **External access architecture**: [docs/ARCHITECTURE_EXTERNAL_ACCESS.md](docs/ARCHITECTURE_EXTERNAL_ACCESS.md)
- **Cluster deployment checklist**: [docs/CLUSTER_DEPLOYMENT.md](docs/CLUSTER_DEPLOYMENT.md)
- **Minikube local flow**: [docs/MINIKUBE_LOCAL_DEV.md](docs/MINIKUBE_LOCAL_DEV.md)
- **Linkerd setup**: [docs/LINKERD_SETUP.md](docs/LINKERD_SETUP.md)
- **Timeout tuning**: [docs/TIMEOUT_CONFIGURATION.md](docs/TIMEOUT_CONFIGURATION.md)
- **Monitoring/results**: [docs/MONITORING_AND_RESULTS.md](docs/MONITORING_AND_RESULTS.md)
- **Migrations guide**: [migrations/README.md](migrations/README.md)
