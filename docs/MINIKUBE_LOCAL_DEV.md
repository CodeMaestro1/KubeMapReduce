# KubeMapReduce - Minikube Local Development Guide

This guide documents how to deploy and test the full KubeMapReduce distributed system
on a local Minikube cluster. Unlike the Docker Compose setup (which only tests auth/API
flows), this deployment runs real worker pods as Kubernetes Jobs - enabling fault
tolerance testing, network partition simulation, zombie fencing, and all the INF-419
failure injection scenarios.

## Prerequisites

- Linux (any distribution - Ubuntu, Fedora, Debian, Arch, etc.)
- Minikube v1.38+
- kubectl v1.31+
- Docker (with docker-compose plugin)
- Go 1.26+
- jq (for JSON parsing in scripts)
- openssl (for generating secrets)

## Step 1: Start Minikube

```bash
minikube start --driver=docker --cpus=4 --memory=8192 --disk-size=20g
```

Expected output confirms:
- `kubectl is now configured to use "minikube" cluster`

Verify:

```bash
kubectl cluster-info
kubectl get nodes
# Should show 1 node with status Ready
```

---

## Step 2: Create Namespace and Secrets

```bash
# Create the mapreduce namespace
kubectl apply -f k8s/00-namespace.yaml

# Generate all required secrets (postgres, minio, keycloak, manager TLS)
NODE_IP="$(minikube ip)" bash scripts/create-secrets.sh

# Generate postgres-tls (required for SSL mode in this branch)
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout /tmp/postgres.key -out /tmp/postgres.crt \
  -subj "/CN=postgres" \
  -addext "subjectAltName=DNS:postgres.mapreduce.svc.cluster.local"
# Idempotent create/apply so reruns do not fail
kubectl -n mapreduce create secret tls postgres-tls \
  --cert=/tmp/postgres.crt --key=/tmp/postgres.key \
  --dry-run=client -o yaml | kubectl apply -f -
rm /tmp/postgres.key /tmp/postgres.crt
```

This creates the following secrets in the `mapreduce` namespace:
- `postgres-creds` - PostgreSQL user/password/database
- `minio-creds` - MinIO root credentials + S3 access keys
- `manager-secrets` - Internal API key + worker RPC token
- `kubemapreduce-secrets` - Mounted into every worker pod
- `keycloak-creds` - Keycloak admin credentials
- `grpc-tls` - Self-signed TLS cert for gRPC between workers and manager
- `postgres-tls` - Self-signed TLS cert for PostgreSQL (required for SSL mode)

---
## Step 3: Build Custom Images

The project has 4 custom Docker images that need to be built inside Minikube's
Docker daemon so the cluster can pull them directly.

```bash
# Point your Docker CLI to Minikube's Docker daemon
eval $(minikube docker-env)

# Build all 4 images (from repository root)
docker build -f infra/docker/Dockerfile.manager    -t kubemapreduce/manager:latest    .
docker build -f infra/docker/Dockerfile.api         -t kubemapreduce/api:latest        .
docker build -f infra/docker/Dockerfile.worker      -t kubemapreduce/worker:latest     .
docker build -f infra/docker/Dockerfile.auth-setup  -t kubemapreduce/auth-setup:latest .
```

> **Note:** `eval $(minikube docker-env)` only affects the current shell session.
> Re-run it in every new terminal before building or pushing images.

The public images (`postgres:15`, `keycloak:23.0.6`, `minio:RELEASE.2024-...`) are
pulled automatically from Docker Hub and Quay.io by the cluster.

Verify images are available:

```bash
eval $(minikube docker-env)
docker images | grep kubemapreduce
```

---

## Step 4: Apply Kubernetes Manifests

Apply core manifests in order. Skip `55-tls.yaml` and `60-gateway.yaml` -
they require cert-manager and Gateway API CRDs not available on bare Minikube.
Use port-forwarding for local access instead.

```bash
kubectl apply -f k8s/10-postgres.yaml
kubectl apply -f k8s/20-minio.yaml
kubectl apply -f k8s/25-keycloak.yaml
kubectl apply -f k8s/30-manager.yaml
kubectl apply -f k8s/35-api.yaml
kubectl apply -f k8s/50-worker-rbac.yaml
```

If you prefer direct host access via Minikube IP + NodePorts (instead of
`kubectl port-forward`), run the helper script:

```bash
bash scripts/fix-cluster.sh
```

This patches Keycloak and API services to NodePort (`30080`, `30081`), keeps
MinIO NodePorts (`30900`, `30901`), and applies service-selector fixes needed
for some local kustomize flows. For full host/DNS setup, see
`docs/MINIKUBE_EXTERNAL_ACCESS.md`.

## Step 5: Run Database Migrations

The ConfigMap placeholder in `10-postgres.yaml` only runs `SELECT 1`. The real
schema should be applied using the migration script:

```bash
# Apply all migrations in order (idempotent for reruns)
bash scripts/run-migrations.sh
```

Verify:

```bash
kubectl -n mapreduce exec postgres-0 -- psql -U mapreduce -d mapreduce -c \
  "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename IN ('jobs','job_configs','tasks','system_config');"
```

Wait for all pods to be READY (1/1):

```bash
kubectl -n mapreduce get pods
# Expected: postgres-0, minio-0, keycloak-XXX, manager-0, api-XXX all 1/1 Running
```

---

## Step 6: Bootstrap Keycloak Realm

Create the `mapreduce` realm, OIDC client, roles (`ADMIN`/`USER`), and an
initial admin user:

```bash
# Port-forward Keycloak
kubectl -n mapreduce port-forward svc/keycloak 8080:8080 &

# Get the generated Keycloak admin password
KC_ADMIN_PW=$(kubectl -n mapreduce get secret keycloak-creds -o jsonpath='{.data.KEYCLOAK_ADMIN_PASSWORD}' | base64 -d)

# Choose password for the initial platform-admin user (used later for CLI login)
PLATFORM_ADMIN_PASSWORD=admin

# Bootstrap the realm (idempotent - safe to run multiple times)
go run ./auth-service/cmd/setup \
  --admin-password "$KC_ADMIN_PW" \
  --username platform-admin \
  --email platform-admin@example.com \
  --password "$PLATFORM_ADMIN_PASSWORD" \
  --role ADMIN

# Set manager-config issuer to the issuer advertised by Keycloak
# (may be https://auth.mapreduce.local even when accessed through localhost port-forward).
KEYCLOAK_ISSUER="$(curl -fsS http://localhost:8080/realms/mapreduce/.well-known/openid-configuration | jq -r '.issuer')"
kubectl -n mapreduce patch configmap manager-config -p "{\"data\":{\"KEYCLOAK_ISSUER\":\"$KEYCLOAK_ISSUER\"}}"
kubectl -n mapreduce rollout restart deploy/api
```

> `KC_ADMIN_PW` is the Keycloak **master admin** password, not the `platform-admin`
> user password. The CLI login password is whatever you set in
> `PLATFORM_ADMIN_PASSWORD` (or what you typed with `--prompt-password`).

## Step 7: Access Services & Login

Use `kubectl port-forward` to expose services locally.
If you are using NodePort mode (`scripts/fix-cluster.sh`), skip this section and use
`http://$(minikube ip):30080` (Keycloak) and `http://$(minikube ip):30081` (API) instead.

```bash
# Terminal 1: Keycloak (already forwarded if you kept it)
kubectl -n mapreduce port-forward svc/keycloak 8080:8080 &

# Terminal 2: API
kubectl -n mapreduce port-forward svc/api 8081:8081 &

# Terminal 3: MinIO console
kubectl -n mapreduce port-forward svc/minio 9001:9001 &
```

Login and verify:

```bash
# Login (Interactive - will prompt for password)
go run ./cli-service/cmd/cli login --username platform-admin

# OR Automated Login (Non-TTY)
go run ./cli-service/cmd/cli login --username platform-admin --password "$PLATFORM_ADMIN_PASSWORD"

# Verify identity and health
go run ./cli-service/cmd/cli whoami
# Expected: platform-admin with ADMIN role

go run ./cli-service/cmd/cli health
# Expected: {"status":"ok"}

go run ./cli-service/cmd/cli jobs list
# Expected: No jobs found (empty cluster)
```

## Step 8: Submit a Test Job

KubeMapReduce provides bundled test data for verification. The CLI simplifies job submission by automatically uploading local files to MinIO:

```bash
# Submit Word Count
go run ./cli-service/cmd/cli jobs submit \
  --mapper testdata/job1-wordcount/mapper.py \
  --reducer testdata/job1-wordcount/reducer.py \
  --input testdata/job1-wordcount/input.jsonl

# Monitor status
go run ./cli-service/cmd/cli jobs status --id <JOB_ID>
```

When the status reaches `Completed`, you can download the results:

```bash
go run ./cli-service/cmd/cli jobs download --id <JOB_ID> --output ./results/
```

---

## Step 9: Failure Injection Testing (E2E)

The E2E test suite simulates real-world infrastructure failures to verify the system's fault tolerance and consistency.

### Configure Environment

```bash
# Enable live cluster testing
export E2E_LIVE_CLUSTER=true
# Set API URL (if port-forwarding to 8081)
export API_URL=http://localhost:8081
```

### Run Tests

The tests are located in the `e2e/` directory. They require `kubectl` to be configured for Minikube.

```bash
# Run all failure injection tests
go test -v ./e2e/... -run TestE2E
```

This will execute:
1. **Worker Kill Scenario**: Deletes a worker pod mid-job to verify task re-assignment.
2. **Manager Restart Scenario**: Restarts the manager statefulset to verify state recovery from DDS.
3. **Zombie Fencing Scenario**: Simulates a network partition (SIGSTOP) and verifies the zombie worker is rejected after its lease expires.

---

## Step 10: Troubleshooting & Common Issues

### Locality Scheduling (Node Permissions)
The Manager requires permission to list cluster nodes for locality-aware scheduling. If you see warnings in manager logs about `cannot list nodes`, apply the cluster RBAC:

```bash
kubectl apply -f k8s/51-manager-cluster-rbac.yaml
```

### Image Pull Failures
If pods are stuck in `ErrImagePull`, ensure you ran `eval $(minikube docker-env)` in the same terminal used for `docker build`.

### Resource Constraints
If workers are stuck in `Pending`, check if the node has enough CPU/Memory. Minikube started with `--cpus=4 --memory=8192` is recommended.

### Storage Persistence
MinIO and PostgreSQL data are persisted in Kubernetes `PersistentVolumes`. If you need a clean slate, delete the PVCs:

```bash
kubectl -n mapreduce delete pvc --all
```
