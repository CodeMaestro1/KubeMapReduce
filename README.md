# KubeMapReduce

A distributed MapReduce platform built on Kubernetes. Workers run as K8s Jobs spawned dynamically by the Manager. Includes Keycloak authentication, role-based authorization, MinIO object storage, and a CLI for job submission and user management.

## Prerequisites

- Go 1.26+
- Docker + Docker Compose
- `kubectl` (for Kubernetes deployment)
- **Kubernetes 1.21+** (for Linkerd service mesh)
- **Linkerd 2.15+** (optional, recommended for production mTLS and timeout management)
  - Install with: `curl -fsL https://linkerd.io/install-edge | sh`
  - See [Linkerd Setup Guide](docs/LINKERD_SETUP.md) for deployment instructions

## Local Development (infrastructure only)

The `docker-compose.yml` starts Keycloak, Postgres, MinIO, the API server, and the Manager. It does **not** run real worker jobs — the Manager spawns workers as Kubernetes Jobs and requires an in-cluster environment for actual job execution. Use this setup to develop and test the API, auth, and job submission flows.

Copy the example env file (`.env` is gitignored — each developer maintains their own copy):

```bash
cp infra/docker/.env.example infra/docker/.env
```

The example contains working local-dev defaults. **For production**, replace all placeholder secrets and set `ALLOW_INSECURE_WORKER_RPC=false` with a real TLS cert pair.

Then start the stack:

```bash
cd infra/docker
docker compose up -d
```

The Keycloak realm, client, and roles are bootstrapped automatically by the `auth-setup` container on first start. Keycloak data is persisted in a named volume — realm config and users survive restarts. To wipe all state: `docker compose down -v`.

Run DB migrations after the first start:

```bash
docker exec -i mapreduce-postgres psql -U mapreduce -d mapreduce < migrations/0001_initial_schema.sql
```

On Windows (PowerShell), use `Get-Content` to pipe the file:

```powershell
Get-Content migrations\0001_initial_schema.sql | docker exec -i mapreduce-postgres psql -U mapreduce -d mapreduce
```

| Service    | URL / Port                          |
|------------|-------------------------------------|
| Keycloak   | http://localhost:8080               |
| API        | http://localhost:8081               |
| PostgreSQL | localhost:5432 (user/db: mapreduce) |
| MinIO S3   | localhost:9000                      |
| MinIO UI   | http://localhost:9001               |

## Quick Start (local smoke test)

1. Start all services:

   ```bash
   cd infra/docker
   docker compose up -d
   ```

   The `auth-setup` container bootstraps the Keycloak realm automatically. Wait for all containers to be healthy before proceeding.

2. Run DB migrations (first start only):

   ```bash
   # Linux/macOS
   docker exec -i mapreduce-postgres psql -U mapreduce -d mapreduce < migrations/0001_initial_schema.sql

   # Windows (PowerShell)
   Get-Content migrations\0001_initial_schema.sql | docker exec -i mapreduce-postgres psql -U mapreduce -d mapreduce
   ```

3. Create the first admin user:

   ```bash
   go run ./auth-service/cmd/setup \
     --admin-password admin \
     --username platform-admin \
     --email platform-admin@example.com \
     --prompt-password \
     --role ADMIN
   ```

   - `--admin-password` matches `KEYCLOAK_ADMIN_PASSWORD` in `.env`.
   - Use `--prompt-password` so the new user's password is not visible in shell history.
   - Use `--role USER` to create a normal user instead of an admin.
   - After the first user exists, use `kubemapreduce admin create-user` for additional users.

4. Verify the stack is healthy and submit a job:

   ```bash
   go run ./cli-service/cmd/cli login --username platform-admin
   go run ./cli-service/cmd/cli health
   go run ./cli-service/cmd/cli jobs list
   ```

   A response of `[]` from `jobs list` confirms auth, API, database, and MinIO are all wired up correctly.

## Kubernetes Deployment (full job execution)

Workers run as `batch/v1` Jobs spawned by the Manager at runtime. A real Kubernetes cluster is required to run actual MapReduce jobs.

### Cluster options

- **Lightweight VM + k3s** — a 4 vCPU / 8 GB VM is sufficient. k3s includes containerd, CoreDNS, and a local-path provisioner.
  ```bash
  curl -sfL https://get.k3s.io | sh -
  ```
- **kind or minikube** — for local machine testing (no cloud required).
- **Managed K8s** (GKE / EKS / AKS) — zero ops, higher cost.

### Build and push images

The Manager looks for the worker image as `kubemapreduce-worker:latest` (hardcoded in `manager-service/cmd/manager/main.go`). Tag your builds accordingly.

```bash
docker build -f infra/docker/Dockerfile.manager -t kubemapreduce/manager:latest .
docker build -f infra/docker/Dockerfile.worker  -t kubemapreduce-worker:latest  .
docker push kubemapreduce/manager:latest
docker push kubemapreduce-worker:latest
```

For k3s without a registry, import images directly:

```bash
docker save kubemapreduce/manager:latest | sudo k3s ctr images import -
docker save kubemapreduce-worker:latest  | sudo k3s ctr images import -
```

### Create secrets

```bash
kubectl apply -f k8s/00-namespace.yaml

kubectl -n mapreduce create secret generic postgres-creds \
  --from-literal=POSTGRES_USER=mapreduce \
  --from-literal=POSTGRES_PASSWORD="$(openssl rand -hex 16)" \
  --from-literal=POSTGRES_DB=mapreduce

kubectl -n mapreduce create secret generic minio-creds \
  --from-literal=MINIO_ROOT_USER=mapreduce \
  --from-literal=MINIO_ROOT_PASSWORD="$(openssl rand -hex 32)" \
  --from-literal=S3_ACCESS_KEY=mapreduce \
  --from-literal=S3_SECRET_KEY="$(openssl rand -hex 32)" \
  --from-literal=S3_ENDPOINT=minio.mapreduce.svc.cluster.local:9000

kubectl -n mapreduce create secret generic manager-secrets \
  --from-literal=MANAGER_INTERNAL_API_KEY="$(openssl rand -hex 16)" \
  --from-literal=MANAGER_WORKER_RPC_TOKEN="$(openssl rand -hex 16)"

# TLS cert for gRPC (self-signed for testing)
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout tls.key -out tls.crt \
  -subj "/CN=manager" \
  -addext "subjectAltName=DNS:*.manager-headless.mapreduce.svc.cluster.local"
kubectl -n mapreduce create secret tls grpc-tls --cert=tls.crt --key=tls.key
```

### Deploy

```bash
# Bake migrations into a ConfigMap
kubectl -n mapreduce create configmap postgres-init \
  --from-file=migrations/ --dry-run=client -o yaml | kubectl apply -f -

# Apply all manifests
kubectl apply -k k8s/

# Wait for rollouts
kubectl -n mapreduce rollout status statefulset/postgres
kubectl -n mapreduce rollout status statefulset/minio
kubectl -n mapreduce rollout status deployment/keycloak
kubectl -n mapreduce rollout status statefulset/manager
```

### Submit a job

```bash
# Point the CLI at the cluster API
export API_URL=http://<cluster-ip-or-loadbalancer>:8081

go run ./cli-service/cmd/cli login --username platform-admin
go run ./cli-service/cmd/cli jobs submit \
  --mapper  mapper.py \
  --reducer reducer.py \
  --input   input.jsonl \
  --reducers 1

go run ./cli-service/cmd/cli jobs list
go run ./cli-service/cmd/cli jobs status   --id <job-id>
go run ./cli-service/cmd/cli jobs download --id <job-id>
```

### Linkerd Service Mesh (Optional)

For production deployments with automatic mTLS, per-RPC timeouts, and advanced traffic policies, deploy Linkerd 2.15+:

```bash
# 1. Install Linkerd control plane
linkerd install | kubectl apply -f -

# 2. Wait for control plane to be ready
linkerd check

# 3. Install traffic policies into mapreduce namespace
kubectl apply -f k8s/01-linkerd-namespace.yaml
kubectl apply -f k8s/02-linkerd-crds.yaml
kubectl apply -f k8s/04-manager-linkerd-policy.yaml
kubectl apply -f k8s/05-linkerd-storage-policies.yaml

# 4. Re-roll pods to inject Linkerd sidecar proxies
kubectl -n mapreduce rollout restart statefulset/manager
kubectl -n mapreduce rollout restart deployment/ui
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
Admin -> CLI -> API Server (POST /admin/users, DELETE /admin/users/{username}) -> Keycloak
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
# Username: alice
# Password: ********
# Login successful!
# Credentials saved to C:\Users\alice\AppData\Roaming\kubemapreduce\credentials.json
```

You can also pass `--username alice` to skip the username prompt.

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

- **[Deployment Guide](docs/DEPLOYMENT.md)** — Complete step-by-step deployment guide for both local Docker Compose and production Kubernetes clusters
- **[External Access Guide](docs/EXTERNAL_ACCESS.md)** — How to expose the API to external clients (minikube Ingress, production Gateway API, NodePort, port forwarding)
- **[Linkerd Setup Guide](docs/LINKERD_SETUP.md)** — Service mesh installation and configuration for automatic mTLS and per-RPC timeouts (production-recommended)
- **[Timeout Configuration Guide](docs/TIMEOUT_CONFIGURATION.md)** — Detailed tuning guidelines for timeout values, retry strategies, and circuit breaker settings
- **[Architecture Documentation](docs/ARCHITECTURE.md)** — System design, component interactions, and fault tolerance patterns

### Quick References

- **API Reference** — REST and gRPC API specifications (see `proto/mapreduce.proto` for RPC definitions)
- **Troubleshooting** — Common issues and solutions in [Deployment Guide](docs/DEPLOYMENT.md#troubleshooting)
- **Security** — Authentication, authorization, and TLS configuration in relevant guides

### Development

- **Build all services:** `go build ./...`
- **Run tests:** `go test ./...`
- **Format code:** `go fmt ./...`
- **Lint code:** `go vet ./...`

## Configuration

**Manager / API server** (defaults shown):

| Variable | Default | Notes |
|---|---|---|
| `KEYCLOAK_BASE_URL` | `http://localhost:8080` | |
| `KEYCLOAK_REALM` | `mapreduce` | |
| `KEYCLOAK_AUDIENCE` | `mapreduce-api` | |
| `KEYCLOAK_ADMIN_USERNAME` | — | **Required** — used for proxied user management |
| `KEYCLOAK_ADMIN_PASSWORD` | — | **Required** |
| `SERVER_ADDR` | `:8081` | |
| `GRPC_ADDR` | `:50051` | |
| `DATABASE_DSN` | — | Postgres connection string |
| `MINIO_ENDPOINT` | — | e.g. `minio:9000` |
| `MINIO_ACCESS_KEY` | — | |
| `MINIO_SECRET_KEY` | — | |
| `MANAGER_INTERNAL_API_KEY` | — | Internal API auth token |
| `MANAGER_WORKER_RPC_TOKEN` | — | Shared secret for worker→manager gRPC |
| `GRPC_TLS_CERT_FILE` | — | Path to TLS cert; if set, `GRPC_TLS_KEY_FILE` also required |
| `GRPC_TLS_KEY_FILE` | — | Path to TLS key |
| `ALLOW_INSECURE_WORKER_RPC` | `false` | Set `true` for local dev without TLS or a token |

The manager refuses to start without either a `MANAGER_WORKER_RPC_TOKEN`, a TLS cert pair, or `ALLOW_INSECURE_WORKER_RPC=true`.

**CLI**:

| Variable | Default |
|---|---|
| `API_URL` | `http://localhost:8081` |
| `KEYCLOAK_BASE_URL` | `http://localhost:8080` |
| `KEYCLOAK_REALM` | `mapreduce` |
| `KEYCLOAK_AUDIENCE` | `mapreduce-api` |

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

## API Endpoints

All protected endpoints require a `Authorization: Bearer <token>` header.

| Method   | Path                           | Auth              | Description                            |
| -------- | ------------------------------ | ----------------- | -------------------------------------- |
| `GET`    | `/healthz`                     | None              | Liveness check                         |
| `GET`    | `/readyz`                      | None              | Readiness check                        |
| `POST`   | `/api/v1/jobs`                 | `USER` or `ADMIN` | Submit a MapReduce job                 |
| `GET`    | `/api/v1/jobs`                 | `USER` or `ADMIN` | List all jobs                          |
| `GET`    | `/api/v1/jobs/{id}`            | `USER` or `ADMIN` | Get job status                         |
| `DELETE` | `/api/v1/jobs/{id}`            | `USER` or `ADMIN` | Cancel a job (204 No Content)          |
| `POST`   | `/api/v1/uploads/presigned`    | `USER` or `ADMIN` | Get pre-signed PUT URL for file upload |
| `POST`   | `/api/v1/downloads/presigned`  | `USER` or `ADMIN` | Get pre-signed GET URLs for results    |
| `POST`   | `/admin/users`                 | `ADMIN`           | Create a user in Keycloak              |
| `DELETE` | `/admin/users/{username}`      | `ADMIN`           | Delete a user from Keycloak            |
| `PUT`    | `/admin/workers/config`        | `ADMIN`           | Update worker configuration            |
| `PUT`    | `/admin/nodes/config`          | `ADMIN`           | Set per-node resource limits           |

`DELETE /admin/users/{username}` returns **204 No Content** with no body. The username is in the path because some HTTP clients and proxies do not support bodies on `DELETE` requests.

## E2E Failure Validation (INF-419)

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
