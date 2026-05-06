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

test_worker_kill() {
    log "--- SCENARIO: Worker Pod Kill ---"
    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"
    
    wait_for_workers "$job_id" 60
    
    local target_pod=$(kubectl get pods -l app=kubemapreduce-worker,job_id=$job_id -o name | head -n 1)
    log "Killing worker pod: $target_pod"
    kubectl delete "$target_pod" --grace-period=0 --force
    
    assert_job_completed "$job_id" 300
    log "Worker Kill Scenario: PASSED"
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
    log "--- SCENARIO: Duplicate Job Creation ---"
    local pids=()
    local output_dir="$REPORT_DIR/duplicate_jobs"
    mkdir -p "$output_dir"

    log "Submitting 5 identical jobs concurrently..."
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
        if grep -q "jobId" "$output_dir/out_$i.json"; then
            ((success_count++))
            created_job_id=$(jq -r '.jobId' "$output_dir/out_$i.json")
        fi
    done

    log "Successful submissions: $success_count"
    # Depending on how idempotency is implemented, either 1 job is created and others fail,
    # or all return the same job ID.
    if [[ "$success_count" -gt 1 ]]; then
        # Check if they all have the same jobId
        local unique_jobs=$(cat "$output_dir"/out_*.json | jq -r '.jobId' | grep -v null | sort -u | wc -l)
        if [[ "$unique_jobs" -gt 1 ]]; then
            fail "Duplicate jobs were scheduled! Expected 1, got $unique_jobs"
        else
            log "Idempotency worked: Multiple requests returned the same Job ID."
        fi
    elif [[ "$success_count" -eq 1 ]]; then
        log "Idempotency worked: 1 request succeeded, others failed or were rejected."
    else
        fail "All job submissions failed."
    fi

    assert_job_completed "$created_job_id" 300
    log "Duplicate Job Creation Scenario: PASSED"
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

test_auth_outage() {
    log "--- SCENARIO: Auth Provider Outage ---"

    log "Scaling Keycloak deployment to 0..."
    kubectl scale deployment keycloak -n mapreduce --replicas=0

    # Wait for pods to terminate
    sleep 10

    log "Verifying that new job submission fails gracefully..."
    local job_output
    job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB" 2>&1 || true)

    if echo "$job_output" | grep -Eqi "unauthorized|503|connection refused|failed"; then
        log "System rejected submission gracefully during Auth outage."
    else
        log "Restoring Keycloak..."
        kubectl scale deployment keycloak -n mapreduce --replicas=1
        kubectl rollout status deployment/keycloak -n mapreduce --timeout=120s
        fail "Expected submission to fail gracefully due to auth outage, but it did not. Output: $job_output"
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
  - Ingress
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

    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    log "Applying NetworkPolicy to block Manager from K8s API..."
    # The default Kubernetes API is typically at 10.96.0.1 (kubernetes.default.svc)
    # We will block all egress to the default namespace API server IP.
    cat <<EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: block-k8s-api
  namespace: mapreduce
spec:
  podSelector:
    matchLabels:
      app: manager
  policyTypes:
  - Egress
  egress:
  - to:
    - ipBlock:
        cidr: 0.0.0.0/0
        except:
        - 10.96.0.1/32
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
test_worker_kill
test_manager_restart
test_zombie_fencing

log "Suite completed successfully. Reports saved to $REPORT_DIR"
