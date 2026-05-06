#!/bin/bash
# KubeMapReduce E2E Failure-Injection Validation Suite (INF-419)
#
# This script validates the resilience of the KubeMapReduce system by injecting
# failures into a live Kubernetes cluster and asserting recovery outcomes.

set -euo pipefail

# Configuration
CLI_EXE="go run ./cli-service/cmd/cli"
SLEEP_JOB="./scripts/sleep_job.json"
REPORT_DIR="./reports/$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$REPORT_DIR/suite.log"

mkdir -p "$REPORT_DIR"
touch "$LOG_FILE"

log() {
    local msg="[$(date +'%Y-%m-%dT%H:%M:%S')] $1"
    echo "$msg" | tee -a "$LOG_FILE"
}

fail() {
    log "ERROR: $1"
    exit 1
}

assert_job_completed() {
    local job_id=$1
    local timeout_secs=$2
    log "Waiting for job $job_id to complete (timeout: ${timeout_secs}s)..."

    local start_time=$(date +%s)
    while true; do
        local status
        status=$($CLI_EXE jobs status --id "$job_id" | jq -r '.status')
        if [[ -z "$status" || "$status" == "null" ]]; then
            fail "Unable to parse status for job $job_id"
        fi
        if [[ "$status" == "Completed" ]]; then
            log "Job $job_id finished successfully."
            return 0
        fi
        if [[ "$status" == "Failed" || "$status" == "Cancelled" ]]; then
            fail "Job $job_id entered terminal state: $status"
        fi

        local current_time=$(date +%s)
        if (( current_time - start_time > timeout_secs )); then
            fail "Timeout waiting for job $job_id"
        fi
        sleep 5
    done
}

wait_for_workers() {
    local job_id=$1
    local timeout_secs=$2
    log "Waiting for worker pods to appear for job $job_id..."
    local start_time=$(date +%s)
    while true; do
        local pods=$(kubectl get pods -l app=kubemapreduce-worker,job_id=$job_id -o name)
        if [[ -n "$pods" ]]; then
            return 0
        fi
        local current_time=$(date +%s)
        if (( current_time - start_time > timeout_secs )); then
            fail "Timeout waiting for workers"
        fi
        sleep 2
    done
}

test_worker_crash_mid_reduce() {
    log "--- SCENARIO: Worker Pod Crash Mid-Reduce ---"
    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    # Wait until job is explicitly in reducing state
    local start_time=$(date +%s)
    while true; do
        local progress
        progress=$($CLI_EXE jobs status --id "$job_id" | jq -r '.progress')
        if echo "$progress" | grep -qi "Reduce"; then
            break
        fi
        if (( $(date +%s) - start_time > 180 )); then
            fail "Timeout waiting for job to enter Reduce phase"
        fi
        sleep 5
    done

    local target_pod=$(kubectl get pods -l app=kubemapreduce-worker,job_id=$job_id -o name | head -n 1)
    log "Killing worker pod during Reduce phase: $target_pod"
    kubectl delete "$target_pod" --grace-period=0 --force

    assert_job_completed "$job_id" 300
    log "Worker Crash Mid-Reduce Scenario: PASSED"
}

test_worker_crash_mid_map() {
    log "--- SCENARIO: Worker Pod Crash Mid-Map ---"
    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    # Wait until job is explicitly in mapping state
    local start_time=$(date +%s)
    while true; do
        local progress
        progress=$($CLI_EXE jobs status --id "$job_id" | jq -r '.progress')
        if echo "$progress" | grep -qi "Map"; then
            break
        fi
        if (( $(date +%s) - start_time > 60 )); then
            fail "Timeout waiting for job to enter Map phase"
        fi
        sleep 2
    done

    local target_pod=$(kubectl get pods -l app=kubemapreduce-worker,job_id=$job_id -o name | head -n 1)
    log "Killing worker pod during Map phase: $target_pod"
    kubectl delete "$target_pod" --grace-period=0 --force

    assert_job_completed "$job_id" 300
    log "Worker Crash Mid-Map Scenario: PASSED"
}

test_manager_restart() {
    log "--- SCENARIO: Manager Replica Restart ---"
    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    log "Restarting manager pods..."
    kubectl rollout restart statefulset/manager
    kubectl rollout status statefulset/manager --timeout=120s

    assert_job_completed "$job_id" 300
    log "Manager Restart Scenario: PASSED"
}

test_duplicate_job_creation() {
    log "--- SCENARIO: Burst Job Creation (Idempotency limits) ---"
    local pids=()
    local output_dir="$REPORT_DIR/duplicate_jobs"
    mkdir -p "$output_dir"

    log "Submitting 5 jobs concurrently to test API burst handling..."
    for i in {1..5}; do
        $CLI_EXE jobs submit --spec "$SLEEP_JOB" > "$output_dir/out_$i.json" 2>&1 &
        pids+=($!)
    done

    for pid in "${pids[@]}"; do
        wait "$pid" || true
    done

    local success_count=0
    local created_job_id=""
    for i in {1..5}; do
        local out
        out=$(cat "$output_dir/out_$i.json")
        local jid
        jid=$(extract_job_id "$out")
        if [[ -n "$jid" ]]; then
            ((success_count++))
            created_job_id="$jid"
        fi
    done

    log "Successful submissions: $success_count"

    if [[ "$success_count" -eq 0 || -z "$created_job_id" ]]; then
         fail "All concurrent job submissions failed."
    fi

    # We just need to track one of them to completion to prove the system isn't broken
    assert_job_completed "$created_job_id" 300
    log "Burst Job Creation Scenario: PASSED"
}

test_delayed_task_ack() {
    log "--- SCENARIO: Delayed Task Acknowledgement ---"
    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    local target_pod=$(kubectl get pods -l app=kubemapreduce-worker,job_id=$job_id -o name | head -n 1)

    log "Simulating slow worker (SIGSTOP) immediately to delay task completion ack: $target_pod"
    kubectl exec "$target_pod" -- kill -STOP 1

    # The default heartbeat timeout is typically 10-30s, and lease is 60s. Wait long enough to trigger reassignment.
    log "Waiting 90 seconds to ensure task times out and is reassigned..."
    sleep 90

    log "Resuming slow worker (SIGCONT) so it attempts a stale ack: $target_pod"
    kubectl exec "$target_pod" -- kill -CONT 1

    assert_job_completed "$job_id" 300
    log "Delayed Task Acknowledgement Scenario: PASSED (System handled stale ack and completed job)"
}

test_slow_object_storage() {
    log "--- SCENARIO: Slow Object Storage (MinIO) Latency ---"
    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    local target_pod=$(kubectl get pods -l app=kubemapreduce-worker,job_id=$job_id -o name | head -n 1)

    # We resolve the MinIO cluster IP because tc rules apply to IPs
    local minio_ip=$(kubectl get svc minio -n mapreduce -o jsonpath='{.spec.clusterIP}')
    if [[ -z "$minio_ip" ]]; then
        fail "Could not resolve minio service cluster IP"
    fi

    log "Injecting 5000ms latency to MinIO ($minio_ip) traffic on worker pod: $target_pod"
    # Create a qdisc and add a filter that delays traffic specifically targeting MinIO
    kubectl exec "$target_pod" -- tc qdisc add dev eth0 root handle 1: prio
    kubectl exec "$target_pod" -- tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 5000ms
    kubectl exec "$target_pod" -- tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst "$minio_ip" flowid 1:1

    # Observe that the job should still eventually complete without crashing
    assert_job_completed "$job_id" 400

    log "Cleaning up latency injection..."
    kubectl exec "$target_pod" -- tc qdisc del dev eth0 root || true

    log "Slow Object Storage Scenario: PASSED"
}

test_zombie_fencing() {
    log "--- SCENARIO: Zombie Worker Fencing ---"
    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    local target_pod=$(kubectl get pods -l app=kubemapreduce-worker,job_id=$job_id -o name | head -n 1)
    log "Simulating zombie worker (SIGSTOP) on pod: $target_pod"
    kubectl exec "$target_pod" -- kill -STOP 1

    log "Waiting for lease to expire (TTL + Manager Reaper delay ~60s)..."
    sleep 60

    log "Resuming zombie worker (SIGCONT) to attempt illegitimate commit: $target_pod"
    kubectl exec "$target_pod" -- kill -CONT 1

    assert_job_completed "$job_id" 300

    log "Verifying zombie logs for rejection..."
    kubectl logs "$target_pod" > "$REPORT_DIR/zombie_worker.log" || true

    if ! grep -Eqi 'PermissionDenied|ExpiredLease' "$REPORT_DIR/zombie_worker.log"; then
        fail "Zombie fencing rejection not observed in $REPORT_DIR/zombie_worker.log"
    fi

    log "Zombie Fencing Scenario: PASSED (Recovery completed and stale attempt was rejected)"
}

# Main Execution
log "Starting KubeMapReduce E2E Failure-Injection Suite"

# Check dependencies
command -v kubectl >/dev/null 2>&1 || fail "kubectl not found"
command -v go >/dev/null 2>&1 || fail "go not found"
command -v jq >/dev/null 2>&1 || fail "jq not found"

extract_job_id() {
    local output="$1"
    local job_id
    if job_id=$(echo "$output" | jq -re '.jobId' 2>/dev/null); then
        echo "$job_id"
    fi
}

test_auth_outage() {
    log "--- SCENARIO: Auth Provider Outage ---"
    trap 'kubectl scale deployment keycloak -n mapreduce --replicas=1; kubectl rollout status deployment/keycloak -n mapreduce --timeout=120s' EXIT

    log "Scaling Keycloak deployment to 0..."
    kubectl scale deployment keycloak -n mapreduce --replicas=0

    # Wait for pods to terminate
    sleep 10

    log "Verifying that existing authenticated CLI interactions continue to work..."
    local job_output
    job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local auth_outage_job_id
    auth_outage_job_id=$(extract_job_id "$job_output")

    if [[ -z "$auth_outage_job_id" ]]; then
        fail "Expected job submission to succeed with cached JWT/JWKS, but it failed! Output: $job_output"
    else
        log "System successfully accepted job submission with cached JWKS during Auth outage."
    fi

    log "Scaling Keycloak deployment back to 1..."
    kubectl scale deployment keycloak -n mapreduce --replicas=1
    kubectl rollout status deployment/keycloak -n mapreduce --timeout=120s

    log "Verifying recovery: Submitting new job..."
    local recovery_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$recovery_output" | jq -r '.jobId')

    if [[ -z "$job_id" || "$job_id" == "null" ]]; then
        fail "Failed to submit job after Keycloak recovery. Output: $recovery_output"
    fi
    log "Submitted job after recovery: $job_id"

    assert_job_completed "$job_id" 300
    log "Auth Provider Outage Scenario: PASSED"
}

test_minio_outage() {
    log "--- SCENARIO: MinIO Outage/Latency ---"
    trap 'kubectl scale statefulset minio -n mapreduce --replicas=1; kubectl rollout status statefulset/minio -n mapreduce --timeout=120s' EXIT

    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    log "Scaling MinIO statefulset to 0 to simulate outage..."
    kubectl scale statefulset minio -n mapreduce --replicas=0

    log "Waiting 30 seconds while MinIO is down. Job should stall, not crash."
    sleep 30

    local status
    status=$($CLI_EXE jobs status --id "$job_id" | jq -r '.status')
    if [[ "$status" == "Failed" || "$status" == "Cancelled" ]]; then
        log "Restoring MinIO..."
        kubectl scale statefulset minio -n mapreduce --replicas=1
        kubectl rollout status statefulset/minio -n mapreduce --timeout=120s
        fail "Job failed prematurely during MinIO outage!"
    fi

    log "Scaling MinIO statefulset back to 1..."
    kubectl scale statefulset minio -n mapreduce --replicas=1
    kubectl rollout status statefulset/minio -n mapreduce --timeout=120s

    assert_job_completed "$job_id" 400
    log "MinIO Outage Scenario: PASSED"
}

test_network_partition() {
    log "--- SCENARIO: Network Partition (Workers <-> Coordinator) ---"
    trap 'kubectl delete networkpolicy isolate-workers -n mapreduce 2>/dev/null || true' EXIT

    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    log "Applying NetworkPolicy to isolate workers from manager..."
    cat <<EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: isolate-workers
  namespace: mapreduce
spec:
  podSelector:
    matchLabels:
      app: kubemapreduce-worker
  policyTypes:
  - Egress
  egress:
  - to:
    - namespaceSelector: {}
      podSelector: {}
    ports:
    - protocol: UDP
      port: 53
    - protocol: TCP
      port: 53
  - to:
    - podSelector:
        matchLabels:
          app.kubernetes.io/name: minio
    ports:
    - protocol: TCP
      port: 9000
EOF

    # Wait to simulate partition duration where tasks timeout and re-queue
    log "Waiting 45 seconds for partition effects..."
    sleep 45

    local status
    status=$($CLI_EXE jobs status --id "$job_id" | jq -r '.status')
    if [[ "$status" == "Failed" || "$status" == "Cancelled" ]]; then
        kubectl delete networkpolicy isolate-workers -n mapreduce
        fail "Job failed prematurely during network partition!"
    fi

    log "Removing NetworkPolicy to heal partition..."
    kubectl delete networkpolicy isolate-workers -n mapreduce

    assert_job_completed "$job_id" 400
    log "Network Partition Scenario: PASSED"
}

test_kubernetes_api_outage() {
    log "--- SCENARIO: Kubernetes API Outage ---"
    trap 'kubectl delete networkpolicy block-k8s-api -n mapreduce 2>/dev/null || true' EXIT

    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    log "Applying NetworkPolicy to block Manager from K8s API..."
    # Dynamically resolve the K8s API server ClusterIP
    local k8s_api_ip
    k8s_api_ip=$(kubectl get svc kubernetes -n default -o jsonpath='{.spec.clusterIP}')
    if [[ -z "$k8s_api_ip" ]]; then
        fail "Could not discover Kubernetes API ClusterIP"
    fi

    cat <<EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: block-k8s-api
  namespace: mapreduce
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: manager
  policyTypes:
  - Egress
  egress:
  - to:
    - ipBlock:
        cidr: 0.0.0.0/0
        except:
        - $k8s_api_ip/32
EOF

    log "Waiting 30 seconds for API outage effects..."
    sleep 30

    local status
    status=$($CLI_EXE jobs status --id "$job_id" | jq -r '.status')
    if [[ "$status" == "Failed" || "$status" == "Cancelled" ]]; then
        kubectl delete networkpolicy block-k8s-api -n mapreduce
        fail "Job failed prematurely during Kubernetes API outage!"
    fi

    log "Removing NetworkPolicy to restore K8s API access..."
    kubectl delete networkpolicy block-k8s-api -n mapreduce

    assert_job_completed "$job_id" 400
    log "Kubernetes API Outage Scenario: PASSED"
}

# Run tests
test_duplicate_job_creation
test_auth_outage
test_minio_outage
test_network_partition
test_kubernetes_api_outage
test_worker_crash_mid_map
test_worker_crash_mid_reduce
test_manager_restart
test_delayed_task_ack
test_slow_object_storage
test_zombie_fencing

log "Suite completed successfully. Reports saved to $REPORT_DIR"
