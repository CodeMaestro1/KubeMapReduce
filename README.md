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

## CLI commands

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

## Public REST API (manager-service API)

All protected routes require `Authorization: Bearer <token>`.

| Method | Endpoint | Auth |
|---|---|---|
| `GET` | `/healthz` | None |
| `GET` | `/readyz` | None |
| `POST` | `/api/v1/jobs` | `USER` or `ADMIN` |
| `GET` | `/api/v1/jobs` | `USER` or `ADMIN` |
| `GET` | `/api/v1/jobs/{job_id}` | `USER` or `ADMIN` |
| `DELETE` | `/api/v1/jobs/{job_id}` | `USER` or `ADMIN` |
| `POST` | `/api/v1/uploads/presigned` | `USER` or `ADMIN` |
| `POST` | `/api/v1/downloads/presigned` | `USER` or `ADMIN` |
| `POST` | `/api/v1/admin/users` | `ADMIN` |
| `DELETE` | `/api/v1/admin/users/{username}` | `ADMIN` |
| `POST` | `/api/v1/admin/config/workers` | `ADMIN` |
| `GET`  | `/api/v1/admin/config/workers` | `ADMIN` |

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

## Benchmarking & Verification

KubeMapReduce includes a comprehensive benchmarking suite and failure-injection tests to validate the system's scalability and resilience.

### Performance Benchmarks

Apache Bench results and distributed scalability data are available in `benchmarks/results/`:

| Metric | Value |
|--------|-------|
| Throughput (Apache Bench, C=20, n=500) | 20.43 req/s |
| Combiner improvement (no combiner vs with combiner) | 69s → 47s (33% reduction) |
| Grep scalability (1→8 reducers) | 20s → 44s (sub-linear due to shuffle overhead) |
| Worker kill recovery | 20s total (15s detection + 5s reschedule) |
| Manager restart recovery | 22s total (5s pod term + 15s startup + 2s state recovery) |
| Zombie fencing (lease-based) | 30s lease expiry, data integrity preserved |

**Run distributed benchmarks on a live cluster:**

```bash
# Prerequisites: logged in via CLI, API_URL set
export API_URL="http://<YOUR_API_IP>"
python benchmarks/distributed_benchmark.py
```

**Run local validation (WordCount with combiner):**

```bash
python benchmarks/wordcount/validate.py
```

**Run local validation (Grep):**

```bash
python benchmarks/grep/validate.py --pattern "love"
```

### Cluster Deployment (GKE)

For a complete production-grade GKE deployment with managed PostgreSQL, MinIO, and Keycloak:

- **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** — Step-by-step GKE deployment guide
- **[docs/CLUSTER_DEPLOYMENT.md](docs/CLUSTER_DEPLOYMENT.md)** — 10-step production checklist
- **[docs/PRESENTATION_TESTS.md](docs/PRESENTATION_TESTS.md)** — E2E failure injection results
- **[docs/EXTENDED_BENCHMARKS.md](docs/EXTENDED_BENCHMARKS.md)** — Scalability and concurrency proofs
- **[docs/MONITORING_AND_RESULTS.md](docs/MONITORING_AND_RESULTS.md)** — Prometheus/Grafana dashboards

### Docs Quick Reference

- **Architecture**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Data locality architecture**: [docs/ARCHITECTURE_DATA_LOCALITY.md](docs/ARCHITECTURE_DATA_LOCALITY.md)
- **External access architecture**: [docs/ARCHITECTURE_EXTERNAL_ACCESS.md](docs/ARCHITECTURE_EXTERNAL_ACCESS.md)
- **Minikube local flow**: [docs/MINIKUBE_LOCAL_DEV.md](docs/MINIKUBE_LOCAL_DEV.md)
- **Linkerd setup**: [docs/LINKERD_SETUP.md](docs/LINKERD_SETUP.md)
- **Timeout tuning**: [docs/TIMEOUT_CONFIGURATION.md](docs/TIMEOUT_CONFIGURATION.md)
- **Migrations guide**: [migrations/README.md](migrations/README.md)
