# Linkerd Service Mesh Setup Guide for KubeMapReduce

This guide walks through deploying Linkerd 2.15+ with automatic mTLS and per-RPC method timeouts on KubeMapReduce.

## Overview

**Linkerd** is a lightweight service mesh that adds:
- **Automatic mTLS**: End-to-end encryption between services without code changes
- **Traffic Policy**: Per-method timeouts, retries, circuit breakers, and traffic splitting
- **Observability**: Live traffic metrics and service topology visualization
- **Fault Tolerance**: Automatic retries with backoff, timeouts, and circuit breakers

KubeMapReduce uses Linkerd for:
1. **gRPC security**: Manager ↔ Worker communication encrypted with auto-rotated certs (24h TTL)
2. **Per-method timeouts**: Heartbeat (2s), Register (5s), TaskComplete/TaskFailed (10s)
3. **Fault-tolerant retries**: Exponential backoff for transient failures (except Heartbeat, which must fail-fast)
4. **Circuit breaking**: Automatic graceful degradation when services are unhealthy

---

## Prerequisites

- Kubernetes 1.24+ cluster with `kubectl` configured
- 2 CPUs and 256MB RAM available for Linkerd control plane
- Installed: `linkerd` CLI tool (see [Linkerd installation](https://linkerd.io/2.15/getting-started/))

```bash
# Install Linkerd CLI (macOS/Linux)
curl --proto '=https' --tlsv1.2 -sSfL https://run.linkerd.io/install | sh
export PATH=$PATH:$HOME/.linkerd2/bin

# Verify CLI
linkerd version
# Output: Client version: stable-2.15.x
```

---

## Step 1: Deploy Linkerd Control Plane

The control plane provides certificate issuance, traffic policy enforcement, and the proxy injection webhook.

```bash
# 1a. Create Linkerd namespace with RBAC
kubectl apply -f k8s/01-linkerd-namespace.yaml

# 1b. Install PolicyAPI CRDs (required for traffic policies)
kubectl apply -f k8s/02-linkerd-crds.yaml

# 1c. Deploy Linkerd control plane (Destination, Identity, ProxyInjector)
kubectl apply -f k8s/03-linkerd-control-plane.yaml
```

**Verify control plane is running:**

```bash
# Check that all Linkerd pods are ready
kubectl get pods -n linkerd
# Expected output:
# NAME                            READY   STATUS    RESTARTS   AGE
# destination-7d4c5d5b9f-xxx      1/1     Running   0          30s
# identity-5f8f4c9b8-yyy          1/1     Running   0          30s
# proxy-injector-8d5c6b4f9-zzz    1/1     Running   0          30s

# Run full health check
linkerd check
# If successful, output ends with: "√ All checks passed!"
```

---

## Step 2: Deploy KubeMapReduce Services

Deploy PostgreSQL, MinIO, Keycloak, and Manager services. The Kubernetes manifests already include Linkerd injection annotations.

```bash
# Deploy infrastructure services
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/10-postgres.yaml
kubectl apply -f k8s/20-minio.yaml
kubectl apply -f k8s/25-keycloak.yaml

# Deploy Manager (annotated with linkerd.io/inject: enabled)
kubectl apply -f k8s/30-manager.yaml
kubectl apply -f k8s/50-worker-rbac.yaml
```

**Verify services are injected:**

```bash
# Check that pods have Linkerd sidecars
kubectl get pods -n mapreduce --show-labels | grep injected

# For each Manager pod, you should see:
# manager-0   2/2   Running   ...   linkerd.io/inject=enabled
# manager-1   2/2   Running   ...   linkerd.io/inject=enabled
# manager-2   2/2   Running   ...   linkerd.io/inject=enabled
```

---

## Step 3: Deploy Traffic Policies

Traffic policies define per-method timeouts, retries, and circuit breaker behavior.

```bash
# Deploy gRPC traffic policies (Manager ↔ Worker)
kubectl apply -f k8s/04-manager-linkerd-policy.yaml

# Deploy storage traffic policies (Manager ↔ PostgreSQL, Manager ↔ MinIO, Worker ↔ MinIO)
kubectl apply -f k8s/05-linkerd-storage-policies.yaml
```

**Verify policies are loaded:**

```bash
# Check policies
kubectl get policies -n mapreduce
# Expected output:
# NAME                      AGE
# manager-grpc-inbound      5s
# worker-to-manager         5s
# manager-to-postgres       5s
# manager-to-minio          5s
# worker-to-minio           5s

# Check HTTPRoutes
kubectl get httproutes -n mapreduce
# Expected output:
# NAME                      PARENTS   HOSTNAMES                                    AGE
# manager-grpc-inbound      1         manager-headless.mapreduce.svc.cluster.local 5s
# worker-to-manager         1         manager-headless.mapreduce.svc.cluster.local 5s
# manager-to-postgres       1         postgres.mapreduce.svc.cluster.local         5s
# manager-to-minio          1         minio.mapreduce.svc.cluster.local            5s
# worker-to-minio           1         minio.mapreduce.svc.cluster.local            5s
```

---

## Step 4: Deploy KubeMapReduce UI

```bash
kubectl apply -f k8s/40-ui.yaml
```

The UI service is also annotated with `linkerd.io/inject: enabled`, so it will automatically get a sidecar proxy.

---

## Step 5: Verify End-to-End Security

### 5a. Check certificate rotation

```bash
# Get the identity-issuer secret (contains the CA cert used to sign workload certs)
kubectl get secret -n linkerd identity-issuer -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -text -noout | grep -A2 "Validity"

# Output should show:
# Validity
#     Not Before: <date>
#     Not After:  <date>
```

### 5b. Check mTLS traffic

Linkerd automatically encrypts all traffic between injected pods. You can verify this with:

```bash
# Port-forward to Linkerd Prometheus for metrics
kubectl port-forward -n linkerd deployment/prometheus 9090:9090 &

# Open browser to http://localhost:9090
# Query: sum(rate(linkerd_io_http_requests_total{tls="true"}[1m])) by (dst_deployment)
# This shows traffic encrypted by Linkerd
```

### 5c. Test timeout enforcement

When a Worker pod registers with the Manager:

```bash
# Get worker logs
kubectl logs -n mapreduce <worker-pod-name> -c linkerd-proxy | grep -i timeout

# Should show: timeout enforcement active
```

---

## Step 6: Monitor with Linkerd Dashboard

```bash
# Start Linkerd dashboard (opens http://localhost:50750)
linkerd viz dashboard &
```

In the dashboard, you can view:
- **Live traffic**: Real-time request flow between services
- **Traffic metrics**: Request rate, latency, success rate per method
- **Service topology**: Visual graph of inter-service communication
- **Pod metrics**: CPU, memory usage for each injected pod

---

## Timeout Configuration Details

### gRPC Per-Method Timeouts

| RPC Method | Timeout | Rationale |
|-----------|---------|-----------|
| `Heartbeat` | 2s | Frequent, must fail-fast to trigger Active Reaper quickly |
| `Register` | 5s | Task startup, allows connection pool overhead |
| `TaskComplete` | 10s | Critical state transition, serialized atomically |
| `TaskFailed` | 10s | Critical state transition, serialized atomically |
| (default) | 10s | Safe fallback for unspecified methods |

### Retry Strategy

| RPC Method | Max Retries | Backoff | Notes |
|-----------|-------------|---------|-------|
| `Heartbeat` | 0 | N/A | No retries; missing heartbeat is the signal to reaper |
| `Register` | 3 | Exponential | Allows temporary transient failures during startup |
| `TaskComplete` | 2 | Exponential | Critical, but don't retry forever |
| `TaskFailed` | 2 | Exponential | Same as TaskComplete |
| (default) | 1 | Exponential | Safe default for other methods |

### Circuit Breaker Configuration

| Service | Max Requests | Max Pending | Error Threshold | Min Requests |
|---------|--------------|-------------|-----------------|--------------|
| PostgreSQL | 100 | 50 | 50% | 10 |
| MinIO (Manager) | 50 | 25 | 50% | 5 |
| MinIO (Worker) | 20 | 10 | 50% | 3 |

Circuit breaker opens when error rate exceeds threshold after minimum request volume.

---

## Application-Level Timeout Integration

The KubeMapReduce application also implements timeout interceptors at the gRPC level:

- **Server-side**: `manager-service/cmd/manager/main.go` chains timeout interceptor with auth interceptor
- **Client-side**: `worker-service/cmd/worker/main.go` uses client timeout interceptor for Manager calls

These work in concert with Linkerd policies: application-level timeouts fire first, Linkerd policies provide a backup defense layer.

---

## Troubleshooting

### Issue: Pods not getting injected

**Symptom**: Pod shows `1/1` containers instead of `2/2` (missing linkerd-proxy sidecar)

**Solution**:
```bash
# Check MutatingWebhookConfiguration is working
kubectl get mutatingwebhookconfigurations | grep linkerd

# Verify webhook is callable
kubectl get pods -n linkerd proxy-injector
# Should show `Running` status

# Re-apply the pod or StatefulSet to trigger injection
kubectl delete pod <pod-name> -n mapreduce
# Pod will restart with sidecar
```

### Issue: Traffic policy not enforcing timeouts

**Symptom**: Requests exceed timeout without error

**Solution**:
```bash
# Check policies are installed
kubectl get policies -n mapreduce
kubectl get httproutes -n mapreduce

# Check policy targeting is correct
kubectl describe policy manager-grpc-inbound -n mapreduce
# Verify targetRef matches service name and namespace

# Check Linkerd logs for policy errors
kubectl logs -n linkerd -l app=policy-webhook
```

### Issue: Certificate rotation errors

**Symptom**: TLS handshake failures after some time

**Solution**:
```bash
# Check identity-issuer certificate (should be valid for 1 year)
kubectl get secret -n linkerd identity-issuer -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -text -noout | grep "Not After"

# Workload certs (24h TTL) are auto-rotated; if errors continue:
kubectl logs -n linkerd -l app=identity --tail=50
```

### Issue: High latency after Linkerd deployment

**Symptom**: Request latency increased 10-50ms

**Cause**: Sidecar proxy adds network hop and context switch overhead

**Solution**:
- This is normal for service mesh; tune circuit breaker thresholds if needed
- Monitor actual user impact (end-to-end latency) not individual RPC latency
- Consider reducing MaxHeaderBytes or ReadHeaderTimeout if acceptable

---

## Rollback (Remove Linkerd)

If Linkerd needs to be removed:

```bash
# 1. Remove policies
kubectl delete policies --all -n mapreduce
kubectl delete httproutes --all -n mapreduce

# 2. Remove injection annotation from pods
kubectl edit statefulset manager -n mapreduce
# Remove: linkerd.io/inject: enabled

# 3. Delete and recreate pods (to remove sidecars)
kubectl delete pod -l app.kubernetes.io/name=manager -n mapreduce

# 4. Uninstall Linkerd control plane
kubectl delete ns linkerd
kubectl delete crd policies.policy.linkerd.io httproutes.gateway.networking.k8s.io

# 5. Remove MutatingWebhookConfiguration
kubectl delete mutatingwebhookconfigurations -l app.kubernetes.io/part-of=linkerd
```

---

## References

- [Linkerd 2.15 Documentation](https://linkerd.io/2.15/overview/)
- [Linkerd Policy API](https://linkerd.io/2.15/tasks/using-policy/)
- [mTLS in Linkerd](https://linkerd.io/2.15/features/automatic-mtls/)
- [KubeMapReduce Architecture](../README.md)
