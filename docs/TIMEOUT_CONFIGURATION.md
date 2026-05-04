# Timeout Configuration and Tuning Guide for KubeMapReduce

This guide explains how timeouts work in KubeMapReduce and how to tune them for different operational requirements.

## Architecture

Timeouts are enforced at **three layers**:

1. **Application Level (Go code)**: gRPC interceptors in `pkg/grpc/timeout_interceptor.go`
2. **Service Mesh Level (Linkerd)**: Traffic policies in `k8s/04-*.yaml` and `k8s/05-*.yaml`
3. **Transport Level (HTTP/gRPC)**: Server-side HTTP timeouts in `cmd/manager/main.go` and `cmd/api/main.go`

This layered approach provides defense-in-depth: if one layer fails, others still protect against hangs.

---

## gRPC Per-Method Timeouts

### Current Configuration

Source: `pkg/grpc/timeout_interceptor.go` → `NewDefaultTimeoutConfig()`

```go
Heartbeat:     2s  // High-frequency, fail-fast path
Register:      5s  // Task startup (less frequent)
TaskComplete: 10s  // Critical state transition
TaskFailed:   10s  // Critical state transition
(default):    10s  // Fallback for unspecified methods
```

### When Timeouts Fire

The **Manager server** applies timeouts via `UnaryInterceptor()` when Workers call:

```
┌─────────────────────────────────────────────────────┐
│ Worker calls Manager gRPC RPC                       │
│ (e.g., Heartbeat("manager-0:50051", ...))          │
└─────────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│ Linkerd proxy at Worker pod intercepts call         │
│ (applies Linkerd policy retries/circuit-break)      │
└─────────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│ Network: Worker → Manager (encrypted by mTLS)       │
│ (takes ~1-10ms in-cluster, more over internet)      │
└─────────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│ Linkerd proxy at Manager pod intercepts request     │
│ (may apply inbound policy)                          │
└─────────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│ Manager handler processes RPC                       │
│ (timeout fires here if total time > 2s/5s/10s)     │
│ → context.DeadlineExceeded if timeout              │
└─────────────────────────────────────────────────────┘
```

### Tuning Guidelines

#### Heartbeat (2s)

**Purpose**: Frequent signal from Worker to Manager to prove liveness

**Current value**: 2s

**Considerations**:
- Heartbeat happens every 10s (see `HEARTBEAT_INTERVAL_SEC` in `k8s/30-manager.yaml`)
- If Manager is unavailable, Worker should detect within 2 heartbeats (~20s total)
- Missing 3 consecutive heartbeats (>30s) triggers Active Reaper (K8s Job deletion)

**When to increase**:
- If network latency regularly exceeds 1.5s (unlikely in-cluster)
- Example: Multi-region deployment with 500ms+ latency → increase to 3-5s

**When to decrease**:
- If you want faster failure detection (e.g., <15s total)
- Example: Decrease to 1s for aggressive fail-fast

**Formula**:
```
Heartbeat timeout = min(network_latency_95th_percentile * 2, heartbeat_interval / 2)
```

#### Register (5s)

**Purpose**: Worker registers at startup, requests task assignment

**Current value**: 5s

**Considerations**:
- Infrequent operation (once per Worker Pod startup)
- Manager may need to perform DB lookups, network calls to MinIO
- Network overhead + Manager processing time is typically 100-500ms

**When to increase**:
- If Manager is under heavy load (100+ concurrent Workers registering)
- If database latency is high (PostgreSQL connection pool saturation)
- Example: Increase to 10s for large cluster with slow DB

**When to decrease**:
- If you want faster failure feedback during startup
- Example: Decrease to 3s if network is very fast (<50ms latency)

**Formula**:
```
Register timeout = db_query_time_95th + network_latency_95th + 2s buffer
```

#### TaskComplete / TaskFailed (10s)

**Purpose**: Worker reports task outcome to Manager

**Current value**: 10s

**Considerations**:
- Critical state transition; Manager persists to PostgreSQL atomically
- Worker may retry on timeout, causing duplicate output processing
- Manager implements idempotent re-execution via `attempt_id` fencing

**When to increase**:
- If PostgreSQL transaction time is high (complex shuffle merge, large task output)
- If network is congested (batching multiple Worker updates)
- Example: Increase to 15-30s if average task runs 5-10 minutes

**When to decrease**:
- If you want to fail fast on Manager unresponsiveness
- Example: Decrease to 5s if critical for SLA compliance

**Formula**:
```
TaskComplete timeout = max(db_transaction_time_95th, network_latency_95th) + 3s buffer
```

---

## HTTP Server Timeouts

### Current Configuration

Source: `manager-service/cmd/api/main.go` and `manager-service/cmd/manager/main.go`

```go
ReadHeaderTimeout: 10s  // Time to read request headers (Slowloris defense)
ReadTimeout:       30s  // Time to read full request body
WriteTimeout:      45s  // Time to write response
IdleTimeout:       60s  // Keep-alive connection duration
MaxHeaderBytes:    16KB // Prevent header-based attacks
```

### Timeline of HTTP Request Processing

```
T0: Client opens connection
     ↓
T0 + ReadHeaderTimeout (10s): Deadline to finish reading headers
     ↓
T0 + ReadTimeout (30s): Deadline to finish reading entire request body
     ↓ [Handler processes request]
T0 + WriteTimeout (45s): Deadline to write response headers and body
     ↓
T0 + IdleTimeout (60s): Deadline for keep-alive (if connection not closed)
```

### Use Cases

#### Presigned URL Upload (Manager → MinIO)

- Client uploads large file via presigned URL
- Network latency: ~10ms
- File upload time (10MB @ 1Mbps): ~80s

**Timeout impact**:
- ReadTimeout (30s) is too short for large files!
- But presigned URLs bypass Manager; Worker downloads directly from MinIO
- Manager only orchestrates the presigned URL generation (< 1s)

#### Presigned URL Download (Worker → MinIO via presigned URL)

- Worker downloads input split via pre-signed URL
- Network latency: ~10ms
- File size: up to 100MB
- Expected time: 10-120s

**Linkerd policy handles this**: Worker → MinIO policy has 120s timeout

#### Admin API (UI → Manager)

- User queries job status, views logs
- Network latency: 10-100ms
- DB query time: 50-500ms

**Timeout sufficient**: 30s read is more than enough

---

## Linkerd Traffic Policy Timeouts

### Current Configuration

Source: `k8s/04-manager-linkerd-policy.yaml`, `k8s/05-linkerd-storage-policies.yaml`

These policies **mirror** the gRPC interceptor timeouts and add retry/circuit-break behavior.

#### Manager ↔ Worker (gRPC)

```yaml
Heartbeat:     2s, 0 retries   # No retries; fail-fast
Register:      5s, 3 retries   # Exponential backoff
TaskComplete: 10s, 2 retries   # Two retries before giving up
TaskFailed:   10s, 2 retries   # Same as TaskComplete
```

**Why different retry counts**:
- Heartbeat: Missing one heartbeat is the signal to reaper; retrying defeats the purpose
- Register: Transient network hiccup during startup? Retry with exponential backoff
- TaskComplete/TaskFailed: Critical, but don't retry forever (max 2 to avoid storms)

#### Manager ↔ PostgreSQL (TCP connection)

```yaml
30s timeout, 0 retries
```

**Why no retries**:
- Database transactions are not idempotent
- Retrying may cause duplicate writes or deadlocks
- Application should handle retry logic (pessimistic locking, transactions)

**Why 30s**:
- Typical query: 10-100ms
- Lock contention: up to 1s
- Network latency: 1-10ms
- Buffer for slow/complex queries: 10-30s

#### Manager ↔ MinIO (S3 presigned URLs)

```yaml
60s timeout, 3 retries with exponential backoff
Circuit breaker: 50 max requests, 25 pending, 50% error threshold
```

**Why 60s**:
- Small uploads/downloads: 100-500ms
- Large file transfers: 10-60s
- Allows transient slowness without failing

**Why 3 retries**:
- Transient network errors (common in cloud)
- MinIO can temporarily throttle requests
- Backoff prevents thundering herd

#### Worker ↔ MinIO (presigned URL downloads)

```yaml
120s timeout, 2 retries with exponential backoff
Circuit breaker: 20 max requests, 10 pending, 50% error threshold
```

**Why 120s**:
- Workers download input splits (up to 100MB)
- Network latency: 10-1000ms (depending on cloud region)
- Ensures large downloads don't timeout mid-transfer

**Why 2 retries** (vs 3 for Manager):
- Worker retries are more expensive (duplicate compute)
- Rather fail early and let Manager handle rescheduling

---

## Troubleshooting Timeout Issues

### Symptom 1: Frequent "Heartbeat timeout" errors

**Logs**:
```
WARN gRPC method timeout method=/proto.WorkerService/Heartbeat timeout=2s component=grpc
```

**Diagnosis**:
1. Check Manager CPU/memory (may be overloaded)
   ```bash
   kubectl top pod manager-0 -n mapreduce
   ```
2. Check network latency between Worker and Manager
   ```bash
   kubectl exec -it worker-pod -n mapreduce -- ping manager-0.manager-headless
   ```
3. Check Linkerd proxy logs
   ```bash
   kubectl logs worker-pod -c linkerd-proxy -n mapreduce | tail -20
   ```

**Solutions**:
- Increase Heartbeat timeout to 3-5s (in `NewDefaultTimeoutConfig()`)
- Scale up Manager replicas to reduce load per replica
- Check for network congestion (use `linkerd viz dashboard`)

### Symptom 2: Register RPC failing after 5s

**Logs**:
```
ERROR failed to register with manager error=context deadline exceeded
```

**Diagnosis**:
1. Check Manager registration handler performance
   ```bash
   kubectl logs manager-0 -n mapreduce --tail=100 | grep -i register
   ```
2. Check database connection pool
   ```bash
   kubectl exec -it postgres-0 -n mapreduce -- psql -U mapreduce -d mapreduce -c "SELECT count(*) FROM pg_stat_activity;"
   ```

**Solutions**:
- Increase Register timeout to 10s (in `NewDefaultTimeoutConfig()`)
- Increase PostgreSQL `max_connections` (in `k8s/10-postgres.yaml`)
- Scale Workers' startup stagger to avoid registration thundering herd

### Symptom 3: TaskComplete failing, tasks stuck in "In-Progress"

**Logs**:
```
ERROR failed to report task completion error=context deadline exceeded
```

**Diagnosis**:
1. Check if Manager is experiencing database lock contention
   ```bash
   kubectl logs manager-0 -n mapreduce | grep -i "lock"
   ```
2. Check Linkerd circuit breaker status
   ```bash
   kubectl describe policy manager-grpc-inbound -n mapreduce
   ```

**Solutions**:
- Increase TaskComplete timeout to 15-20s if database is slow
- Reduce the number of concurrent Workers (reduces lock contention)
- Tune circuit breaker thresholds in Linkerd policies

### Symptom 4: Slow presigned URL operations (MinIO timeout)

**Logs**:
```
WARN MinIO operation timeout error=context deadline exceeded
```

**Diagnosis**:
1. Test MinIO connectivity directly
   ```bash
   kubectl run -it minio-test --image=minio/mc -- sh
   mc alias set minio http://minio:9000 <access-key> <secret-key>
   mc ls minio/mapreduce/
   ```
2. Check MinIO pod performance
   ```bash
   kubectl logs minio-0 -n mapreduce --tail=50
   ```

**Solutions**:
- Increase Worker → MinIO policy timeout to 180s for very large files
- Scale MinIO to multiple pods/disks
- Use network performance tuning (`tc` qdisc, MTU optimization)

---

## Configuration Checklist

When tuning timeouts, verify:

- [ ] **gRPC interceptor timeouts** (`pkg/grpc/timeout_interceptor.go`)
  - Heartbeat: 2s ✓
  - Register: 5s ✓
  - TaskComplete: 10s ✓
  - TaskFailed: 10s ✓

- [ ] **HTTP server timeouts** (`cmd/api/main.go`, `cmd/manager/main.go`)
  - ReadHeaderTimeout: 10s ✓
  - ReadTimeout: 30s ✓
  - WriteTimeout: 45s ✓
  - IdleTimeout: 60s ✓

- [ ] **Linkerd policies** (`k8s/04-*.yaml`, `k8s/05-*.yaml`)
  - Heartbeat: 2s, 0 retries ✓
  - Register: 5s, 3 retries ✓
  - TaskComplete: 10s, 2 retries ✓
  - TaskFailed: 10s, 2 retries ✓
  - PostgreSQL: 30s, 0 retries ✓
  - MinIO: 60s, 3 retries ✓
  - Worker MinIO: 120s, 2 retries ✓

- [ ] **Deployment environment**
  - [ ] Network latency known (ping test)
  - [ ] Database performance baselined (slow query log)
  - [ ] MinIO throughput benchmarked (mc benchmark)
  - [ ] Worker startup load understood (max concurrent Pods)

---

## Performance Tuning Formula

For custom timeout values, use this formula:

```
timeout = base_operation_time_95th_percentile + 2 × network_latency_95th + 2s buffer

Example for new operation:
- Base operation time (no network): 100ms
- Network latency (in-cluster): 5ms
- Timeout = 100ms + 2×5ms + 2s = 2.11s ≈ 3s (round up)
```

---

## References

- [gRPC timeouts best practices](https://grpc.io/blog/deadlines/)
- [Linkerd traffic policy](https://linkerd.io/2.15/tasks/using-policy/)
- [TCP/IP timeout handling](https://linux-kernel-labs.github.io/master/labs/networking_architecture/)
- KubeMapReduce source:
  - `pkg/grpc/timeout_interceptor.go` (gRPC interceptors)
  - `manager-service/cmd/manager/main.go` (HTTP server config)
  - `k8s/04-manager-linkerd-policy.yaml` (Linkerd policies)
