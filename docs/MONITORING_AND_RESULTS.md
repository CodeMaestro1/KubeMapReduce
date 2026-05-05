# Monitoring, Results Collection & Effectiveness Evaluation

**Question:** "How will I check my application for effectiveness? How are the results collected?"

**Short answer:** You'll use three approaches: (1) Prometheus metrics for real-time performance, (2) Log analysis for debugging, (3) Direct result download and validation.

---

## 📊 **1. Application Effectiveness Metrics**

### **What KubeMapReduce Tracks (Built-in)**

Your system already has **Prometheus metrics** built into the manager service:

| Metric | Type | Purpose |
|--------|------|---------|
| `kubemapreduce_tasks_scheduled_total` | Counter | Total tasks created (split by Map/Reduce) |
| `kubemapreduce_tasks_completed_total` | Counter | Successfully completed tasks |
| `kubemapreduce_tasks_failed_total` | Counter | Failed task attempts (worker or reaper) |
| `kubemapreduce_heartbeats_total` | Counter | Worker heartbeats (split by action: CONTINUE/TERMINATE) |
| `kubemapreduce_reaper_recovered_total` | Counter | Stale tasks reclaimed by fault recovery |
| `kubemapreduce_reaper_cycle_seconds` | Histogram | Duration of each scheduler reaper cycle |
| `kubemapreduce_http_request_duration_seconds` | Histogram | API request latency (split by method, status class) |

### **Example: How to Calculate Effectiveness**

```bash
# Get metrics from Prometheus endpoint
curl http://api.mapreduce.local:8080/metrics

# Example output:
kubemapreduce_tasks_scheduled_total{task_type="Map"} 100
kubemapreduce_tasks_scheduled_total{task_type="Reduce"} 50
kubemapreduce_tasks_completed_total 145
kubemapreduce_tasks_failed_total 5
kubemapreduce_heartbeats_total{action="CONTINUE"} 2847
kubemapreduce_heartbeats_total{action="TERMINATE"} 12

# Calculate success rate:
# (145 / (145 + 5)) * 100 = 96.7% success rate
```

### **Key Effectiveness Indicators**

```
Success Rate = completed_tasks / (completed_tasks + failed_tasks)
Failure Recovery = reaper_recovered / failed_tasks  (how many were automatically recovered)
Average Task Duration = reaper_cycle_seconds / heartbeats_total
Worker Availability = heartbeats_total / (heartbeats_total + terminations)
```

---

## 📥 **2. How Results Are Collected**

### **Result Storage Architecture**

```
┌─────────────┐
│   Worker    │
│ (Running    │
│  Task)      │
└──────┬──────┘
       │
       │ Writes output to MinIO
       │ s3://mapreduce-outputs/<job_id>/partition-N.jsonl
       │
       ▼
┌──────────────────────────────────────┐
│         MinIO Storage (S3)           │
│                                      │
│  outputs/<job_id>/                   │
│  ├─ partition-0.jsonl               │
│  ├─ partition-1.jsonl               │
│  └─ partition-N.jsonl               │
└──────────────────────────────────────┘
       │
       │ Manager reads output URIs from DB
       │
       ▼
┌──────────────────────────────────────┐
│     PostgreSQL (TASK_OUTPUTS table)  │
│                                      │
│  task_id | partition | output_uri   │
│  task-1  | 0        | s3://...      │
│  task-2  | 1        | s3://...      │
└──────────────────────────────────────┘
       │
       │ User calls download endpoint
       │
       ▼
┌──────────────────────────────────────┐
│    API Returns Pre-signed URLs       │
│    (Temporary, limited access)       │
└──────────────────────────────────────┘
       │
       │ Client downloads directly from MinIO
       │
       ▼
┌──────────────────────────────────────┐
│    Local Machine (User's Device)     │
│  results/job-123-part-0.json         │
│  results/job-123-part-1.json         │
└──────────────────────────────────────┘
```

### **Result Download Flow (Step-by-Step)**

**Step 1: Job completes**
```bash
# Worker reports successful task completion via gRPC
# Including output locations in MinIO:
# - s3://mapreduce-outputs/job-123/partition-0.jsonl
# - s3://mapreduce-outputs/job-123/partition-1.jsonl
```

**Step 2: Check job status**
```bash
kubemapreduce jobs status --id job-123
# Returns:
# {
#   "jobId": "job-123",
#   "status": "Completed",
#   "tasksCompleted": 47,
#   "tasksFailed": 0
# }
```

**Step 3: Request download**
```bash
# API endpoint: POST /api/v1/downloads/presigned
# This returns pre-signed URLs for all output files:
curl -X POST http://api.mapreduce.local/api/v1/downloads/presigned \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"job_id": "job-123"}' | jq

# Response:
{
  "jobId": "job-123",
  "urls": [
    "https://minio.mapreduce.local:9000/mapreduce-outputs/job-123/partition-0.jsonl?X-Amz-Signature=...",
    "https://minio.mapreduce.local:9000/mapreduce-outputs/job-123/partition-1.jsonl?X-Amz-Signature=..."
  ]
}
```

**Step 4: Download locally**
```bash
# CLI automatically downloads all shards
kubemapreduce jobs download --id job-123 --output ./results/

# Output files:
# results/job-123-part-0.json
# results/job-123-part-1.json
# results/job-123-part-N.json

# Combined size: hundreds MB to GB depending on data
```

**Step 5: Validate results**
```bash
# Check file sizes
ls -lh results/

# Verify JSON format
head -5 results/job-123-part-0.json | jq

# Count records
jq -s 'length' results/job-123-part-0.json

# Aggregate results
jq -s '.' results/job-123-part-*.json | jq 'group_by(.key)'
```

---

## 🔍 **3. Monitoring Setup**

### **Option A: Prometheus + Grafana (Recommended)**

**1. Deploy Prometheus**
```bash
# Create ConfigMap with scrape config
kubectl create configmap prometheus-config \
  --from-literal=prometheus.yml='
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "mapreduce-api"
    static_configs:
      - targets: ["api.mapreduce.svc.cluster.local:8080"]
    metrics_path: "/metrics"
  - job_name: "mapreduce-manager"
    static_configs:
      - targets: ["manager.mapreduce.svc.cluster.local:8081"]
    metrics_path: "/metrics"
' -n mapreduce

# Deploy Prometheus
kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: prometheus
  namespace: mapreduce
spec:
  selector:
    app: prometheus
  ports:
    - port: 9090
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: mapreduce
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
        - name: prometheus
          image: prom/prometheus:latest
          ports:
            - containerPort: 9090
          volumeMounts:
            - name: config
              mountPath: /etc/prometheus
      volumes:
        - name: config
          configMap:
            name: prometheus-config
EOF
```

**2. Deploy Grafana**
```bash
# Deploy Grafana
kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: grafana
  namespace: mapreduce
spec:
  selector:
    app: grafana
  type: LoadBalancer
  ports:
    - port: 3000
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana
  namespace: mapreduce
spec:
  replicas: 1
  selector:
    matchLabels:
      app: grafana
  template:
    metadata:
      labels:
        app: grafana
    spec:
      containers:
        - name: grafana
          image: grafana/grafana:latest
          ports:
            - containerPort: 3000
          env:
            - name: GF_SECURITY_ADMIN_PASSWORD
              value: admin
EOF
```

**3. Add Prometheus Data Source in Grafana**
- Visit: http://grafana.mapreduce.local:3000
- Login: admin / admin
- Data Sources → Add Prometheus
- URL: http://prometheus.mapreduce.svc.cluster.local:9090
- Save & Test

**4. Create Sample Dashboards**
```
Panel 1: Task Success Rate
Query: (kubemapreduce_tasks_completed_total / (kubemapreduce_tasks_completed_total + kubemapreduce_tasks_failed_total)) * 100

Panel 2: Tasks Scheduled vs Completed
Query: kubemapreduce_tasks_scheduled_total, kubemapreduce_tasks_completed_total

Panel 3: Worker Heartbeat Rate
Query: rate(kubemapreduce_heartbeats_total[5m])

Panel 4: API Request Latency
Query: histogram_quantile(0.95, rate(kubemapreduce_http_request_duration_seconds_bucket[5m]))

Panel 5: Reaper Recovery Rate
Query: rate(kubemapreduce_reaper_recovered_total[5m])
```

### **Option B: Linkerd Dashboard (Built-in, if using Linkerd)**

```bash
# If you deployed Linkerd service mesh:
linkerd viz dashboard

# Automatically shows:
# - Live traffic rates
# - Latency percentiles (p50, p99, p99.9)
# - Success rates
# - TCP connection status
# - Pod-to-pod communication patterns
```

### **Option C: Cloud-Native Monitoring**

| Cloud Provider | Solution | Command |
|---|---|---|
| **GCP** | Cloud Monitoring (built-in) | `gcloud monitoring dashboards create --config-from-file=...` |
| **AWS** | CloudWatch + X-Ray | `aws cloudwatch put-metric-data ...` |
| **Azure** | Azure Monitor | `az monitor metrics create ...` |

---

## 📝 **4. Log Analysis for Debugging**

### **View Logs in Real-Time**

```bash
# Follow API logs
kubectl logs -f deployment/ui -n mapreduce

# Follow Manager logs
kubectl logs -f deployment/api -n mapreduce

# Follow Worker logs (as they spawn)
kubectl logs -f <worker-pod-name> -n mapreduce
```

### **Log Format**

```json
{
  "time": "2026-05-05T16:00:00Z",
  "level": "INFO",
  "message": "http_request",
  "service": "api",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "method": "POST",
  "path": "/api/v1/jobs",
  "status": 202,
  "duration": "1.234s",
  "bytes": 512
}
```

### **Example: Find Errors**

```bash
# View error logs for a specific job
kubectl logs -l app=api -n mapreduce | jq 'select(.level == "ERROR")'

# Find slow API calls
kubectl logs -l app=api -n mapreduce | jq 'select(.duration | tonumber > 5)'

# Track a specific request (use request_id)
kubectl logs -l app=api -n mapreduce | jq 'select(.request_id == "550e8400-e29b-41d4-a716-446655440000")'
```

---

## 📈 **5. Performance Evaluation Checklist**

### **During Job Execution**

- [ ] **Success Rate** — Tasks completed / (completed + failed) → Target: > 99%
- [ ] **Worker Availability** — Heartbeats / expected → Target: 100%
- [ ] **API Latency** — p95 request time → Target: < 500ms
- [ ] **Task Duration** — Time from assignment to completion → Target: varies by workload
- [ ] **Recovery Rate** — Reaper-recovered tasks / total failures → Target: > 95% (stale tasks recovered automatically)

### **Example Job Analysis**

```bash
# 1. Submit job
JOB_ID=$(kubemapreduce jobs submit --mapper mapper.py --reducer reducer.py --input input.jsonl | jq -r '.jobId')

# 2. Monitor in real-time
kubectl top pods -n mapreduce  # CPU/memory per pod
prometheus> rate(kubemapreduce_tasks_completed_total[1m])  # completion rate

# 3. Wait for completion
kubemapreduce jobs status --id $JOB_ID  # poll until Completed

# 4. Download results
kubemapreduce jobs download --id $JOB_ID --output ./results/

# 5. Validate
# Check file sizes
ls -lh results/
# Count output records
jq -s 'length' results/job-123-part-*.json

# 6. Calculate metrics
# - Total time: start to completion
# - Output records: sum of all parts
# - Throughput: input_records / total_time
# - Efficiency: output_size / input_size
```

---

## 🎯 **What to Monitor**

### **Real-Time Dashboards (For Operators)**

```
┌─────────────────────────────────────┐
│  KubeMapReduce Operations Dashboard │
├─────────────────────────────────────┤
│ Active Jobs: 5                      │ ← Running jobs
│ Tasks Scheduled: 2,341              │ ← Total across all jobs
│ Tasks Completed: 2,298 (98%)        │ ← Success rate
│ Failed Tasks: 43                    │ ← Failures (reaper recovered 40)
│                                     │
│ API Response Time (p95): 234ms      │ ← Latency
│ Worker Heartbeat Rate: 847/min      │ ← Health check
│ Reaper Cycles: 2,431 (avg 2.3s)    │ ← Fault recovery
│                                     │
│ Active Pods:                        │
│   - API: 2/2 READY                  │ ← Deployment status
│   - Manager: 3/3 READY              │
│   - Workers: 12 running, 0 pending  │
│   - PostgreSQL: 1/1 READY           │
│   - MinIO: 1/1 READY                │
└─────────────────────────────────────┘
```

### **Business Metrics (For Decision Makers)**

```
┌─────────────────────────────────────┐
│  KubeMapReduce Business Metrics     │
├─────────────────────────────────────┤
│ Jobs Completed (Today): 156         │ ← Throughput
│ Total Data Processed: 2.4 TB        │ ← Volume
│ Average Job Duration: 12m 34s       │ ← Performance
│ Success Rate: 98.2%                 │ ← Reliability
│ System Uptime: 99.97%               │ ← Availability
│                                     │
│ Cost per Job: $1.23                 │ ← Economics
│ Data per Dollar: 15.7 MB            │
└─────────────────────────────────────┘
```

---

## 🚀 **Quick Start: Enable Monitoring**

```bash
# 1. Ensure /metrics endpoint is running
curl http://api.mapreduce.local:8080/metrics | head -20

# 2. Deploy Prometheus (see section 3A above)

# 3. Deploy Grafana (see section 3A above)

# 4. Create first dashboard with these queries:
# - Success rate
# - Task throughput
# - API latency
# - Worker availability

# 5. Set up alerts (optional)
# - Alert if success_rate < 95%
# - Alert if api_latency > 1000ms
# - Alert if any pod is down > 5min
```

---

## 📊 **Result Collection Examples**

### **Example 1: Word Count Job**

```bash
# Submit job
kubemapreduce jobs submit \
  --mapper word_count_mapper.py \
  --reducer word_count_reducer.py \
  --input documents.jsonl \
  --reducers 4

# Download results
kubemapreduce jobs download --id job-123 --output ./results/

# Results format (JSONL per partition):
cat results/job-123-part-0.json
# {"word": "the", "count": 45234}
# {"word": "and", "count": 32145}
# {"word": "a", "count": 28901}

# Aggregate
jq -s 'sort_by(.count) | reverse | .[0:10]' results/job-123-part-*.json
# [{"word": "the", "count": 45234}, ...]  ← Top 10 words
```

### **Example 2: Data Transformation Job**

```bash
# Results stored in multiple partitions for scalability
ls -lh results/job-456-part-*.json
# results/job-456-part-0.json  2.3G
# results/job-456-part-1.json  2.1G
# results/job-456-part-2.json  1.9G

# Total: 6.3GB across 3 partitions
# Each partition processed independently by a reducer

# Merge all results
cat results/job-456-part-*.json > final_results.jsonl

# Validate
wc -l final_results.jsonl  # 123,456,789 lines
```

---

## ✅ **Success Criteria for Effectiveness**

Your application is effective when:

- ✅ **Success Rate > 99%** — Jobs complete without errors
- ✅ **Recovery Rate > 95%** — Stale/failed tasks are recovered automatically
- ✅ **API Latency p95 < 500ms** — Users get quick feedback
- ✅ **Worker Heartbeat Rate > 99%** — Machines are responsive
- ✅ **Results Consistent** — Same input always produces same output
- ✅ **Scalability Linear** — Double workers → ~2x faster
- ✅ **Uptime > 99.9%** — 3 nines of availability (8.6 hours/year down)

---

## 📚 **Further Resources**

- [Prometheus docs](https://prometheus.io/docs/) — Time series metrics
- [Grafana docs](https://grafana.com/docs/) — Dashboards and alerts
- [Linkerd observability](https://linkerd.io/latest/features/dashboards/) — Service mesh metrics
- Your logs: `kubectl logs -f <pod-name> -n mapreduce`

That's everything you need to monitor effectiveness and collect results! 🎉
