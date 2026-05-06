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

# extract_job_id parses a .jobId UUID from a JSON string.
# Uses jq -re so that null/missing values cause a non-zero exit, which is
# caught by the caller before proceeding with the ID.
extract_job_id() {
    local json="$1"
    local id
    if ! id=$(echo "$json" | jq -re '.jobId' 2>/dev/null) || [[ -z "$id" ]]; then
        return 1
    fi
    printf '%s' "$id"
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
    log "--- SCENARIO: Burst Concurrent Job Submission ---"
    local pids=()
    local output_dir="$REPORT_DIR/duplicate_jobs"
    mkdir -p "$output_dir"

    # The API always generates a fresh UUID per submission so concurrent identical
    # submits legitimately create multiple jobs (each with a unique UUID). This
    # scenario validates that the system handles burst concurrent submits without
    # crashing or corrupting state, and that accepted jobs complete successfully.
    log "Submitting 5 jobs concurrently..."
    for i in {1..5}; do
        $CLI_EXE jobs submit --spec "$SLEEP_JOB" > "$output_dir/out_$i.json" 2>&1 &
        pids+=($!)
    done

    for pid in "${pids[@]}"; do
        wait "$pid" || true
    done

    local success_count=0
    local job_ids=()
    for i in {1..5}; do
        local file="$output_dir/out_$i.json"
        local id
        # Only treat the file as JSON if jq succeeds; otherwise skip as a failed submission.
        # jq -re exits non-zero for null/missing fields, so no extra null check is needed.
        if id=$(jq -re '.jobId' "$file" 2>/dev/null) && [[ -n "$id" ]]; then
            ((success_count++))
            job_ids+=("$id")
        fi
    done

    log "Successful submissions: $success_count / 5"
    if [[ "$success_count" -eq 0 ]]; then
        fail "All job submissions failed."
    fi

    # Validate the first successfully parsed job ID before polling.
    local created_job_id="${job_ids[0]}"
    if [[ -z "$created_job_id" || "$created_job_id" == "null" ]]; then
        fail "Could not obtain a valid job ID from any successful submission."
    fi

    log "Verifying that at least one submitted job completes successfully (job: $created_job_id)..."
    assert_job_completed "$created_job_id" 300
    log "Burst Concurrent Job Submission Scenario: PASSED ($success_count job(s) accepted)"
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

test_auth_outage() {
    log "--- SCENARIO: Auth Provider Outage ---"

    # Ensure Keycloak is restored on any exit path.
    _cleanup_auth() {
        log "Cleanup: Restoring Keycloak to 1 replica..."
        kubectl scale deployment keycloak -n mapreduce --replicas=1 2>/dev/null || true
        kubectl rollout status deployment/keycloak -n mapreduce --timeout=120s 2>/dev/null || true
        trap - EXIT
    }
    trap _cleanup_auth EXIT

    log "Scaling Keycloak deployment to 0..."
    kubectl scale deployment keycloak -n mapreduce --replicas=0

    # Wait for pods to terminate
    sleep 10

    # The API validates JWTs using a cached JWKS key set, so a still-valid access
    # token will continue to work while Keycloak is down. The correct resilience
    # assertion is that existing authenticated operations continue uninterrupted.
    log "Verifying that existing authenticated operations continue with a cached token..."
    local job_output
    job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB" 2>&1 || true)

    local job_id
    # jq -re exits non-zero for null/missing .jobId, so a separate null guard is redundant.
    if ! job_id=$(extract_job_id "$job_output") || [[ -z "$job_id" ]]; then
        fail "Existing job submission failed during Keycloak outage (cached token should suffice). Output: $job_output"
    fi
    log "Authenticated submission succeeded during Keycloak outage (job: $job_id)."

    log "Scaling Keycloak deployment back to 1..."
    kubectl scale deployment keycloak -n mapreduce --replicas=1
    kubectl rollout status deployment/keycloak -n mapreduce --timeout=120s

    trap - EXIT

    assert_job_completed "$job_id" 300
    log "Auth Provider Outage Scenario: PASSED"
}

test_minio_outage() {
    log "--- SCENARIO: MinIO Outage/Latency ---"

    # Ensure MinIO is restored on any exit path.
    _cleanup_minio() {
        log "Cleanup: Restoring MinIO to 1 replica..."
        kubectl scale statefulset minio -n mapreduce --replicas=1 2>/dev/null || true
        kubectl rollout status statefulset/minio -n mapreduce --timeout=120s 2>/dev/null || true
        trap - EXIT
    }
    trap _cleanup_minio EXIT

    local job_output
    job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id
    # jq -re exits non-zero for null/missing .jobId, so a separate null guard is redundant.
    if ! job_id=$(extract_job_id "$job_output") || [[ -z "$job_id" ]]; then
        fail "Failed to submit job before MinIO outage. Output: $job_output"
    fi
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    log "Scaling MinIO statefulset to 0 to simulate outage..."
    kubectl scale statefulset minio -n mapreduce --replicas=0

    log "Waiting 30 seconds while MinIO is down. Job should stall, not crash."
    sleep 30

    local status
    status=$($CLI_EXE jobs status --id "$job_id" | jq -r '.status')
    if [[ "$status" == "Failed" || "$status" == "Cancelled" ]]; then
        fail "Job failed prematurely during MinIO outage!"
    fi

    log "Scaling MinIO statefulset back to 1..."
    kubectl scale statefulset minio -n mapreduce --replicas=1
    kubectl rollout status statefulset/minio -n mapreduce --timeout=120s

    trap - EXIT

    assert_job_completed "$job_id" 400
    log "MinIO Outage Scenario: PASSED"
}

test_network_partition() {
    log "--- SCENARIO: Network Partition (Workers <-> Coordinator) ---"

    # Ensure the NetworkPolicy is removed on any exit path.
    _cleanup_network_partition() {
        log "Cleanup: Removing isolate-workers NetworkPolicy..."
        kubectl delete networkpolicy isolate-workers -n mapreduce 2>/dev/null || true
        trap - EXIT
    }
    trap _cleanup_network_partition EXIT

    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    # Block only worker <-> manager traffic while still allowing DNS and MinIO
    # egress so that workers can continue storage I/O and DNS resolution.
    # The missing heartbeats to the manager will trigger lease expiry and rescheduling.
    log "Applying NetworkPolicy to partition workers from manager (DNS + MinIO still allowed)..."
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
  # Allow DNS so workers can resolve service names
  - ports:
    - port: 53
      protocol: UDP
    - port: 53
      protocol: TCP
  # Allow MinIO so workers can continue storage operations
  - to:
    - podSelector:
        matchLabels:
          app: minio
    ports:
    - port: 9000
      protocol: TCP
EOF

    # Wait to simulate partition duration where tasks timeout and re-queue
    log "Waiting 45 seconds for partition effects..."
    sleep 45

    local status
    status=$($CLI_EXE jobs status --id "$job_id" | jq -r '.status')
    if [[ "$status" == "Failed" || "$status" == "Cancelled" ]]; then
        fail "Job failed prematurely during network partition!"
    fi

    log "Removing NetworkPolicy to heal partition..."
    kubectl delete networkpolicy isolate-workers -n mapreduce

    trap - EXIT

    assert_job_completed "$job_id" 400
    log "Network Partition Scenario: PASSED"
}

test_kubernetes_api_outage() {
    log "--- SCENARIO: Kubernetes API Outage ---"

    # Ensure the NetworkPolicy is removed on any exit path.
    _cleanup_k8s_api() {
        log "Cleanup: Removing block-k8s-api NetworkPolicy..."
        kubectl delete networkpolicy block-k8s-api -n mapreduce 2>/dev/null || true
        trap - EXIT
    }
    trap _cleanup_k8s_api EXIT

    local job_output=$($CLI_EXE jobs submit --spec "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | jq -r '.jobId')
    log "Submitted job: $job_id"

    wait_for_workers "$job_id" 60

    # Discover the Kubernetes API service ClusterIP dynamically to avoid
    # hardcoding a cluster-dependent address. kubectl's own error output goes
    # directly to stderr so the caller can diagnose RBAC or connectivity issues.
    local k8s_api_ip
    if ! k8s_api_ip=$(kubectl get svc kubernetes -n default -o jsonpath='{.spec.clusterIP}'); then
        fail "Could not query the 'kubernetes' service in the 'default' namespace (see kubectl error above)."
    fi
    if [[ -z "$k8s_api_ip" ]]; then
        fail "Kubernetes API service ClusterIP is empty."
    fi
    log "Kubernetes API ClusterIP: $k8s_api_ip"

    # Manager pods are labelled app.kubernetes.io/name=manager in the manifests.
    log "Applying NetworkPolicy to block Manager from K8s API (${k8s_api_ip}/32)..."
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
        - ${k8s_api_ip}/32
EOF

    log "Waiting 30 seconds for API outage effects..."
    sleep 30

    local status
    status=$($CLI_EXE jobs status --id "$job_id" | jq -r '.status')
    if [[ "$status" == "Failed" || "$status" == "Cancelled" ]]; then
        fail "Job failed prematurely during Kubernetes API outage!"
    fi

    log "Removing NetworkPolicy to restore K8s API access..."
    kubectl delete networkpolicy block-k8s-api -n mapreduce

    trap - EXIT

    assert_job_completed "$job_id" 400
    log "Kubernetes API Outage Scenario: PASSED"
}

# Main Execution
log "Starting KubeMapReduce E2E Failure-Injection Suite"

# Check dependencies
command -v kubectl >/dev/null 2>&1 || fail "kubectl not found"
command -v go >/dev/null 2>&1 || fail "go not found"
command -v jq >/dev/null 2>&1 || fail "jq not found"

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
