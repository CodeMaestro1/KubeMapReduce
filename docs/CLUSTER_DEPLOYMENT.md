# Production Cluster Deployment Checklist

**Quick answer:** Follow this 10-step checklist to run KubeMapReduce in a production cluster.

---

## 📋 **Pre-Deployment Checklist**

### ✅ Step 1: Choose Your Cluster

| Option | Best For | Cost | Setup Time |
|--------|----------|------|-----------|
| **GKE (Google Cloud)** | ⭐⭐⭐ Production, managed | Medium | 10 min |
| **EKS (AWS)** | ⭐⭐⭐ Production, AWS ecosystem | Medium | 15 min |
| **AKS (Azure)** | ⭐⭐⭐ Production, Azure ecosystem | Medium | 15 min |
| **k3s** | ⭐⭐⭐ On-premise, lightweight | Low | 5 min |
| **kind** | Testing/staging, local | Free | 5 min |
| **minikube** | Development, local | Free | 5 min |

**Recommended:** GKE, EKS, or AKS for production (managed, auto-scaling, monitoring).

---

## 🚀 **10-Step Production Deployment**

### **Step 1: Verify Prerequisites**

```bash
# Check cluster access
kubectl cluster-info
kubectl get nodes

# Verify resources (need at least 4 nodes with 4 CPU, 8GB each)
kubectl top nodes

# Install required tools
docker --version      # Docker 20.10+
go version            # Go 1.26+
kubectl version       # kubectl 1.21+
```

### **Step 2: Create Namespace**

```bash
# Create isolated namespace for the project
kubectl create namespace mapreduce

# Set as default for convenience
kubectl config set-context --current --namespace=mapreduce

# Verify
kubectl get namespace mapreduce
```

### **Step 3: Build & Push Container Images**

```bash
# Set your container registry
export REGISTRY=gcr.io/my-project  # GCP
# OR
export REGISTRY=my-account.dkr.ecr.us-east-1.amazonaws.com  # AWS
# OR
export REGISTRY=myregistry.azurecr.io  # Azure

# Build all images
docker build -t $REGISTRY/kubemapreduce/ui:latest ./cli-service
docker build -t $REGISTRY/kubemapreduce/api:latest ./manager-service/cmd/api
docker build -t $REGISTRY/kubemapreduce/setup:latest ./auth-service/cmd/setup
docker build -t $REGISTRY/kubemapreduce/worker:latest ./worker-service

# Verify images
docker images | grep kubemapreduce

# Push to registry
docker push $REGISTRY/kubemapreduce/ui:latest
docker push $REGISTRY/kubemapreduce/api:latest
docker push $REGISTRY/kubemapreduce/setup:latest
docker push $REGISTRY/kubemapreduce/worker:latest
```

### **Step 4: Create Secrets & ConfigMaps**

```bash
# PostgreSQL credentials
kubectl create secret generic postgres-creds \
  --from-literal=POSTGRES_USER=mapreduce \
  --from-literal=POSTGRES_PASSWORD=<strong-password> \
  --from-literal=POSTGRES_DB=mapreduce \
  -n mapreduce

# MinIO credentials
kubectl create secret generic minio-creds \
  --from-literal=S3_ENDPOINT=minio.mapreduce.svc.cluster.local:9000 \
  --from-literal=S3_ACCESS_KEY=minioadmin \
  --from-literal=S3_SECRET_KEY=<strong-password> \
  -n mapreduce

# Manager internal API key
kubectl create secret generic manager-secrets \
  --from-literal=MANAGER_INTERNAL_API_KEY=<uuid> \
  --from-literal=MANAGER_WORKER_RPC_TOKEN=<uuid> \
  -n mapreduce

# Keycloak admin password
kubectl create secret generic keycloak-creds \
  --from-literal=KEYCLOAK_ADMIN=admin \
  --from-literal=KEYCLOAK_ADMIN_PASSWORD=<strong-password> \
  -n mapreduce

# TLS certificates (for HTTPS, optional but recommended)
kubectl create secret tls mapreduce-tls \
  --cert=path/to/cert.crt \
  --key=path/to/key.key \
  -n mapreduce
```

### **Step 5: Deploy Infrastructure Services**

```bash
# Deploy PostgreSQL (use managed option for production)
kubectl apply -f k8s/10-postgres.yaml

# Wait for PostgreSQL to be ready
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgres -n mapreduce --timeout=300s

# Apply database migrations
kubectl exec -it postgres-0 -n mapreduce -- psql -U mapreduce -d mapreduce < migrations/0001_initial_schema.sql

# Deploy MinIO
kubectl apply -f k8s/20-minio.yaml

# Wait for MinIO
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=minio -n mapreduce --timeout=300s

# Deploy Keycloak
kubectl apply -f k8s/25-keycloak.yaml

# Wait for Keycloak
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=keycloak -n mapreduce --timeout=300s

# Initialize Keycloak (create users and realm)
kubectl exec -it deployment/keycloak -n mapreduce -- bash
# Inside pod:
./keycloak.sh start-dev &
sleep 10
# ... or use kcadm.sh commands via setup container
```

### **Step 6: Deploy Manager & API Services**

```bash
# Deploy Manager
kubectl apply -f k8s/30-manager.yaml

# Deploy UI/API
kubectl apply -f k8s/40-ui.yaml

# Wait for readiness
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=api -n mapreduce --timeout=300s

# Verify health
kubectl exec -it deployment/ui -n mapreduce -- wget -O- http://localhost:8080/healthz
```

### **Step 7: Configure External Access**

**Option A: Production Gateway API (Recommended)**

```bash
# Install Gateway API CRDs
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.0.0/standard-install.yaml

# Install Gateway Controller (e.g., Envoy Gateway)
helm repo add envoy-gateway https://envoyproxy.io/charts
helm install envoy-gateway envoy-gateway/envoy-gateway \
  --namespace envoy-gateway-system --create-namespace

# Apply Gateway + HTTPRoutes
kubectl apply -f k8s/60-gateway.yaml

# Get external IP (cloud provider will assign)
kubectl get gateway mapreduce -n mapreduce -w
# EXTERNAL-IP will appear in 2-5 minutes
```

**Option B: Simple LoadBalancer (if Gateway API not available)**

```bash
# Create LoadBalancer service
kubectl expose service ui --type=LoadBalancer --port=80 --target-port=8080 -n mapreduce

# Get external IP
kubectl get svc ui -n mapreduce -w
```

### **Step 8: Configure DNS**

```bash
# Get external IP from gateway/LoadBalancer
EXTERNAL_IP=$(kubectl get svc -n envoy-gateway-system -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}')

# Point DNS records to this IP (in your DNS provider):
# api.mapreduce.yourdomain.com        A  $EXTERNAL_IP
# storage.mapreduce.yourdomain.com    A  $EXTERNAL_IP
# auth.mapreduce.yourdomain.com       A  $EXTERNAL_IP

# Or for internal-only, add to /etc/hosts on client machines
echo "$EXTERNAL_IP api.mapreduce.local" >> /etc/hosts
```

### **Step 9: (Optional) Deploy Linkerd Service Mesh**

```bash
# For production security, deploy Linkerd
kubectl apply -f k8s/01-linkerd-namespace.yaml
kubectl apply -f k8s/02-linkerd-crds.yaml
kubectl apply -f k8s/03-linkerd-control-plane.yaml

# Wait for control plane
kubectl wait --for=condition=ready pod -l app.kubernetes.io/part-of=linkerd -n linkerd --timeout=300s

# Apply policies
kubectl apply -f k8s/04-manager-linkerd-policy.yaml
kubectl apply -f k8s/05-linkerd-storage-policies.yaml

# Enable automatic injection on namespace
kubectl annotate namespace mapreduce linkerd.io/inject=enabled --overwrite
```

### **Step 10: Verify Deployment**

```bash
# Check all pods are running
kubectl get pods -n mapreduce
# Expected: ui, api, postgres, minio, keycloak all RUNNING

# Check services are ready
kubectl get svc -n mapreduce
# Expected: All services have CLUSTER-IP (and EXTERNAL-IP if LoadBalancer)

# Test API health
kubectl exec -it deployment/ui -n mapreduce -- curl -v http://localhost:8080/healthz
# Expected: 200 OK

# Test Keycloak is accessible
kubectl port-forward svc/keycloak 8080:8080 -n mapreduce &
curl http://localhost:8080/admin
# Expected: Keycloak login page

# Create admin user
kubectl exec -it deployment/keycloak -n mapreduce -- \
  /opt/keycloak/bin/kcadm.sh create users \
    -r mapreduce \
    -s username=admin \
    -s email=admin@example.com \
    -s enabled=true \
    --password admin123

# Test job submission
TOKEN=$(curl -s -X POST http://auth.mapreduce.local:8080/realms/mapreduce/protocol/openid-connect/token \
  -d "client_id=mapreduce-api&grant_type=password&username=admin&password=admin123" \
  | jq -r '.access_token')

curl -X POST http://api.mapreduce.local/api/v1/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{...job spec...}'
```

---

## 📊 **Resource Requirements**

### Minimum (Development/Testing)
- **Nodes:** 2 nodes
- **CPU per node:** 2 cores
- **Memory per node:** 4 GB
- **Storage:** 20 GB (PostgreSQL + MinIO)

### Recommended (Production)
- **Nodes:** 3+ nodes
- **CPU per node:** 4 cores (8+ recommended)
- **Memory per node:** 8 GB (16+ recommended)
- **Storage:** 100+ GB (persistent volumes for PostgreSQL + MinIO)
- **Network:** 100 Mbps+ bandwidth between nodes

### Production Best Practices
- ✅ Use managed storage (GCP Persistent Disks, AWS EBS, Azure Managed Disks)
- ✅ Enable auto-scaling (cluster + pod autoscaling)
- ✅ Configure Pod Disruption Budgets (PDB)
- ✅ Use node anti-affinity for Manager replicas
- ✅ Enable pod monitoring (Prometheus + Grafana or managed equivalent)
- ✅ Use managed PostgreSQL (Cloud SQL, RDS, Azure Database)
- ✅ Use managed object storage (S3, GCS, Azure Blob) instead of in-cluster MinIO

---

## 🔐 **Security Checklist**

- [ ] All secrets stored in Kubernetes Secrets (not ConfigMaps)
- [ ] TLS certificates installed (HTTPS enabled)
- [ ] Network policies configured (if using Calico/Cilium)
- [ ] RBAC roles defined for service accounts
- [ ] Pod security policies enforced (or Pod Security Standards)
- [ ] Image scanning enabled in registry
- [ ] Container security context configured (non-root, read-only filesystem)
- [ ] Linkerd mTLS enabled for inter-pod communication
- [ ] Backup strategy configured (PostgreSQL + MinIO snapshots)

---

## 📈 **Monitoring & Observability**

```bash
# Deploy Prometheus + Grafana (if using managed K8s without built-in monitoring)
kubectl apply -f https://github.com/prometheus-operator/kube-prometheus/releases/download/v0.12.0/kube-prometheus-complete.yaml

# Or use cloud-native monitoring:
# - GCP: Cloud Monitoring (pre-installed)
# - AWS: CloudWatch + X-Ray
# - Azure: Azure Monitor

# If using Linkerd, access built-in dashboard:
linkerd viz dashboard
```

---

## 🚨 **Troubleshooting**

**Pods not starting?**
```bash
kubectl describe pod <pod-name> -n mapreduce
kubectl logs <pod-name> -n mapreduce
```

**Database connection issues?**
```bash
kubectl exec -it postgres-0 -n mapreduce -- psql -U mapreduce -d mapreduce -c "SELECT 1"
```

**External access not working?**
```bash
kubectl get gateway mapreduce -n mapreduce
kubectl get svc -n envoy-gateway-system
nslookup api.mapreduce.yourdomain.com
```

**Performance issues?**
```bash
kubectl top nodes
kubectl top pods -n mapreduce
kubectl describe nodes
```

---

## 📚 **Referenced Documentation**

- **[docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md)** — Complete Kubernetes setup guide
- **[docs/EXTERNAL_ACCESS.md](../docs/EXTERNAL_ACCESS.md)** — External routing options
- **[docs/LINKERD_SETUP.md](../docs/LINKERD_SETUP.md)** — Service mesh installation
- **[docs/TIMEOUT_CONFIGURATION.md](../docs/TIMEOUT_CONFIGURATION.md)** — Performance tuning

---

## 🎯 **Common Issues & Solutions**

| Issue | Cause | Solution |
|-------|-------|----------|
| Pods CrashLoopBackOff | Missing secrets | Run Step 4 again |
| Database connection refused | PostgreSQL not ready | `kubectl logs postgres-0` |
| 502 Bad Gateway | API pods down | `kubectl get pods`, check logs |
| High latency | Network policies too restrictive | Check network policies, disable if testing |
| Storage full | MinIO or PostgreSQL running out of space | Increase PVC size, configure cleanup policies |

---

## ✅ **Success Criteria**

You've successfully deployed when:

- ✅ All pods are RUNNING and READY
- ✅ `kubectl get svc` shows all services with valid IPs
- ✅ Health check passes: `curl http://api.mapreduce.local/healthz` → 200 OK
- ✅ Can create admin user via Keycloak setup
- ✅ Can submit job: `kubemapreduce jobs submit --mapper ... --reducer ...`
- ✅ Job appears in `kubemapreduce jobs list`
- ✅ Worker pod spawns automatically when job starts
- ✅ Job completes successfully
- ✅ Results downloadable via `kubemapreduce jobs download --id <job-id>`

---

## 🚀 **Next Steps After Deployment**

1. **Configure backups** — Schedule PostgreSQL and MinIO snapshots
2. **Set up monitoring** — Prometheus + Grafana dashboards
3. **Enable autoscaling** — HPA for UI/API, cluster autoscaler
4. **Performance tune** — Adjust timeout values, worker resource limits
5. **Load test** — Submit many jobs simultaneously to verify stability
6. **Document runbooks** — Create team-specific deployment procedures

That's it! Your cluster is now ready to run KubeMapReduce. 🎉
