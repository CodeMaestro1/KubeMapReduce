# KubeMapReduce Deployment Guide

Complete guide for deploying KubeMapReduce to Kubernetes with optional Linkerd service mesh for production-grade security and observability.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development](#local-development-docker-compose)
3. [Kubernetes Deployment](#kubernetes-deployment)
4. [Service Mesh Setup](#service-mesh-setup-linkerd-optional)
5. [Verification](#verification)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Tools

- **Go 1.26+** — for building services
- **Docker + Docker Compose** — for local development
- **kubectl 1.21+** — for Kubernetes management
- **Helm 3+** (optional) — for package management
- **Git** — for source control

### Kubernetes Cluster

Choose one:

- **k3s on Ubuntu/Debian** — lightweight, minimal ops
  ```bash
  curl -sfL https://get.k3s.io | sh -
  ```
- **kind** — local testing on any OS
  ```bash
  go install sigs.k8s.io/kind@latest
  kind create cluster --name kubemapreduce
  ```
- **minikube** — VirtualBox + Kubernetes
  ```bash
  minikube start --cpus=4 --memory=8192
  ```
- **Managed K8s** — GKE, EKS, AKS (recommended for production)

### Optional: Linkerd Service Mesh

For automatic mTLS, per-RPC timeouts, and traffic observability (recommended for production):

```bash
curl -fsL https://linkerd.io/install-edge | sh
linkerd version
```

Verify Kubernetes API supports CRDs (required for traffic policies):

```bash
kubectl api-resources | grep -i gateway
# Should show: httproutes, backendpolicies, etc.
```

---

## Local Development (Docker Compose)

### Setup

1. **Clone repository and copy env file:**

   ```bash
   cd infra/docker
   cp .env.example .env
   ```

   The `.env` contains local dev defaults:
   - Keycloak ADMIN_PASSWORD: `admin`
   - PostgreSQL credentials
   - MinIO S3 credentials
   - Manager internal tokens

2. **Start services:**

   ```bash
   docker compose up -d
   ```

   Wait for all containers to be healthy:

   ```bash
   docker compose ps
   docker logs mapreduce-postgres  # Check for migrations
   ```

3. **Initialize database (first start only):**

   Linux/macOS:
   ```bash
   docker exec -i mapreduce-postgres psql -U mapreduce -d mapreduce < ../../migrations/0001_initial_schema.sql
   ```

   Windows PowerShell:
   ```powershell
   Get-Content ../../migrations/0001_initial_schema.sql | docker exec -i mapreduce-postgres psql -U mapreduce -d mapreduce
   ```

4. **Create admin user:**

   ```bash
   go run ../../auth-service/cmd/setup \
     --admin-password admin \
     --username platform-admin \
     --email platform-admin@example.com \
     --prompt-password \
     --role ADMIN
   ```

5. **Verify stack:**

   ```bash
   go run ../../cli-service/cmd/cli login --username platform-admin
   go run ../../cli-service/cmd/cli health
   go run ../../cli-service/cmd/cli jobs list
   ```

   Expected output: `[]` (empty job list confirms all services wired correctly)

### Shutting Down

```bash
# Stop without removing volumes (data persists)
docker compose stop

# Stop and remove everything (clean slate)
docker compose down -v
```

---

## Kubernetes Deployment

### 1. Prepare Kubernetes Cluster

```bash
# Verify cluster is running
kubectl cluster-info
kubectl get nodes

# Verify DNS works
kubectl run -it --rm busybox --image=busybox --restart=Never -- wget -O- https://kubernetes.default.svc.cluster.local
```

### 2. Build and Push Images

Tag images consistently with your registry:

```bash
# Replace 'myregistry' with your Docker Hub username or private registry
REGISTRY=myregistry

docker build -f infra/docker/Dockerfile.manager -t $REGISTRY/manager:latest .
docker build -f infra/docker/Dockerfile.worker  -t $REGISTRY/worker:latest  .
docker build -f infra/docker/Dockerfile.ui      -t $REGISTRY/ui:latest      .

docker push $REGISTRY/manager:latest
docker push $REGISTRY/worker:latest
docker push $REGISTRY/ui:latest
```

For local k3s (no registry needed):

```bash
docker save myregistry/manager:latest | sudo k3s ctr images import -
docker save myregistry/worker:latest  | sudo k3s ctr images import -
docker save myregistry/ui:latest      | sudo k3s ctr images import -
```

### 3. Create Namespace and Secrets

```bash
# Create namespace
kubectl apply -f k8s/00-namespace.yaml

# Create PostgreSQL credentials
kubectl -n mapreduce create secret generic postgres-creds \
  --from-literal=POSTGRES_USER=mapreduce \
  --from-literal=POSTGRES_PASSWORD="$(openssl rand -hex 16)" \
  --from-literal=POSTGRES_DB=mapreduce

# Create MinIO credentials
kubectl -n mapreduce create secret generic minio-creds \
  --from-literal=MINIO_ROOT_USER=mapreduce \
  --from-literal=MINIO_ROOT_PASSWORD="$(openssl rand -hex 32)" \
  --from-literal=S3_ACCESS_KEY=mapreduce \
  --from-literal=S3_SECRET_KEY="$(openssl rand -hex 32)" \
  --from-literal=S3_ENDPOINT=minio.mapreduce.svc.cluster.local:9000 \
  --from-literal=MINIO_BUCKET=mapreduce

# Create Manager internal API token
kubectl -n mapreduce create secret generic manager-secrets \
  --from-literal=MANAGER_INTERNAL_API_KEY="$(openssl rand -hex 16)" \
  --from-literal=MANAGER_WORKER_RPC_TOKEN="$(openssl rand -hex 16)"

# Create gRPC TLS certificate (self-signed for non-production; use proper CA in production)
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout tls.key -out tls.crt \
  -subj "/CN=manager" \
  -addext "subjectAltName=DNS:*.manager-headless.mapreduce.svc.cluster.local,DNS:*.manager.mapreduce.svc.cluster.local"

kubectl -n mapreduce create secret tls grpc-tls --cert=tls.crt --key=tls.key
```

### 4. Load Database Migrations

```bash
kubectl -n mapreduce create configmap postgres-init \
  --from-file=migrations/ \
  --dry-run=client -o yaml | kubectl apply -f -
```

### 5. Deploy Services

Update image names in `k8s/30-manager.yaml`, `k8s/40-ui.yaml` if using a registry:

```bash
sed -i 's/kubemapreduce\/manager:latest/myregistry\/manager:latest/g' k8s/30-manager.yaml
sed -i 's/kubemapreduce-worker:latest/myregistry\/worker:latest/g' k8s/*.yaml
```

Deploy all manifests:

```bash
kubectl apply -k k8s/
```

### 6. Wait for Rollout

```bash
# Wait for each component to be ready
kubectl -n mapreduce rollout status statefulset/postgres --timeout=5m
kubectl -n mapreduce rollout status statefulset/minio --timeout=5m
kubectl -n mapreduce rollout status deployment/keycloak --timeout=5m
kubectl -n mapreduce rollout status statefulset/manager --timeout=5m
kubectl -n mapreduce rollout status deployment/ui --timeout=5m
```

### 7. Expose API Endpoint

Get the external IP/hostname of the API service:

```bash
# If using LoadBalancer
kubectl -n mapreduce get svc api -o wide
API_URL=$(kubectl -n mapreduce get svc api -o jsonpath='{.status.loadBalancer.ingress[0].ip}'):8081

# If using port-forward (local testing)
kubectl -n mapreduce port-forward svc/api 8081:8081 &
API_URL=http://localhost:8081
```

### 8. Submit a Job

```bash
export API_URL=http://<cluster-api-endpoint>:8081

go run ./cli-service/cmd/cli login --username platform-admin
go run ./cli-service/cmd/cli jobs list

# Submit a MapReduce job
go run ./cli-service/cmd/cli jobs submit \
  --mapper   path/to/mapper.py \
  --reducer  path/to/reducer.py \
  --input    path/to/input.jsonl \
  --reducers 2
```

---

## Service Mesh Setup (Linkerd, Optional)

For production deployments, deploy Linkerd 2.15+ for automatic mTLS encryption, per-RPC method timeouts, and observability.

### 1. Install Linkerd Control Plane

```bash
# Install Linkerd control plane (3 replicas)
linkerd install | kubectl apply -f -

# Wait for control plane to be ready
linkerd check

# Verify data plane is injecting sidecars
kubectl -n linkerd get deployment
```

### 2. Enable Automatic Sidecar Injection

Annotate the mapreduce namespace for automatic injection:

```bash
kubectl annotate namespace mapreduce linkerd.io/inject=enabled --overwrite

# Re-roll pods to inject sidecar proxies
kubectl -n mapreduce rollout restart statefulset/postgres
kubectl -n mapreduce rollout restart statefulset/minio
kubectl -n mapreduce rollout restart deployment/keycloak
kubectl -n mapreduce rollout restart statefulset/manager
kubectl -n mapreduce rollout restart deployment/ui
```

### 3. Deploy Traffic Policies

Apply per-method timeout and retry policies:

```bash
kubectl apply -f k8s/01-linkerd-namespace.yaml
kubectl apply -f k8s/02-linkerd-crds.yaml          # PolicyAPI CRDs (if not already installed)
kubectl apply -f k8s/04-manager-linkerd-policy.yaml
kubectl apply -f k8s/05-linkerd-storage-policies.yaml
```

### 4. Verify Service Mesh is Active

```bash
# Check pod sidecar injection
kubectl -n mapreduce get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[*].name}{"\n"}{end}'

# Should show: <pod-name>    <container1> <container2> ... linkerd-proxy

# Check traffic policy status
kubectl get policies -n mapreduce
kubectl describe policy manager-worker-policy -n mapreduce

# View Linkerd dashboard
linkerd dashboard
```

---

## Verification

### Basic Health Checks

```bash
# Check all services are running
kubectl -n mapreduce get pods

# Check logs for errors
kubectl -n mapreduce logs -l app=manager --tail=50
kubectl -n mapreduce logs -l app=ui --tail=50

# Check services are discoverable
kubectl -n mapreduce get svc
```

### gRPC Timeout Enforcement

If Linkerd is deployed, verify per-RPC timeouts are enforced:

```bash
# Check Manager receives timeout policies
kubectl get httproute -n mapreduce

# Check policy statistics
linkerd routes -n mapreduce pod/manager-0

# Monitor timeout events
kubectl -n mapreduce logs -f -l app=manager | grep -i timeout
```

### Presigned URL Operations

Test file upload/download with presigned URLs:

```bash
go run ./cli-service/cmd/cli uploads presigned-url

# Should return:
# {
#   "upload_url": "https://minio.../...",
#   "url_expiry": "2026-05-05T12:00:00Z"
# }
```

### Job Execution

Submit and monitor a test job:

```bash
go run ./cli-service/cmd/cli jobs submit \
  --mapper   mapper.py \
  --reducer  reducer.py \
  --input    input.jsonl \
  --reducers 2

# Monitor job
JOB_ID=<returned-job-id>
go run ./cli-service/cmd/cli jobs status --id $JOB_ID

# Download results when complete
go run ./cli-service/cmd/cli jobs download --id $JOB_ID --output results.jsonl
```

---

## Troubleshooting

### Pod Stuck in CrashLoopBackOff

```bash
# Check pod logs
kubectl -n mapreduce logs <pod-name>

# Check events
kubectl -n mapreduce describe pod <pod-name>

# Check resource limits
kubectl -n mapreduce top pods
```

### Service Discovery Issues

```bash
# From within a pod, test DNS resolution
kubectl -n mapreduce exec -it <pod-name> -- nslookup manager.mapreduce.svc.cluster.local

# Check CoreDNS logs
kubectl -n kube-system logs -l k8s-app=kube-dns
```

### MinIO Connection Errors

```bash
# Verify MinIO is running
kubectl -n mapreduce port-forward svc/minio 9000:9000 &
curl http://localhost:9000/minio/health/live

# Check credentials in secret
kubectl -n mapreduce get secret minio-creds -o yaml
```

### Linkerd Dashboard Not Accessible

```bash
# Start port-forward
linkerd dashboard

# Or manually:
kubectl -n linkerd port-forward svc/web 8084:8084 &
# Then open http://localhost:8084
```

### gRPC Timeout Issues

If workers are timing out during Heartbeat:

1. Check network latency to Manager:
   ```bash
   kubectl -n mapreduce exec -it <worker-pod> -- ping manager-0.manager-headless
   ```

2. Increase Heartbeat timeout in `pkg/grpc/timeout_interceptor.go` (default: 2s)

3. Check Linkerd traffic policies are applied:
   ```bash
   kubectl get policies -n mapreduce -o yaml | grep Heartbeat
   ```

### Certificate Rotation Issues (Linkerd)

If pods fail after certificate rotation:

```bash
# Linkerd handles this automatically (24h default TTL)
# But if manual rotation needed:

# Re-issue cert
linkerd sp validate

# Force pod restart to pick up new cert
kubectl -n mapreduce rollout restart statefulset/manager
```

---

## Production Checklist

- [ ] Use managed Kubernetes (GKE/EKS/AKS) or production-grade k3s cluster
- [ ] Deploy Linkerd for automatic mTLS (24h cert rotation)
- [ ] Use proper CA-signed TLS certificates (not self-signed)
- [ ] Enable RBAC on Kubernetes cluster
- [ ] Set resource limits/requests on all pods
- [ ] Configure HPA (Horizontal Pod Autoscaler) for Manager (3 replicas minimum)
- [ ] Set up persistent storage (PVC) for PostgreSQL and MinIO
- [ ] Configure backup strategy for PostgreSQL
- [ ] Monitor logs with ELK/Datadog/CloudWatch
- [ ] Monitor metrics with Prometheus + Grafana (Linkerd provides built-in dashboards)
- [ ] Set up alerting for pod failures, resource exhaustion, timeout events
- [ ] Test disaster recovery (pod failures, node failures, AZ failures)
- [ ] Run load tests to verify timeout values are appropriate
- [ ] Document runbooks for common failures
- [ ] Implement budget-aware workload prioritization (if needed)

---

## References

- [Linkerd Setup Guide](./LINKERD_SETUP.md) — Step-by-step Linkerd deployment
- [Timeout Configuration Guide](./TIMEOUT_CONFIGURATION.md) — Tuning timeouts for workloads
- [Architecture Documentation](./ARCHITECTURE.md) — System design and component interactions
- [API Reference](./API.md) — REST and gRPC API specifications
