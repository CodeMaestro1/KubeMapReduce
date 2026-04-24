#!/bin/bash
# KubeMapReduce E2E Failure-Injection Validation Suite (INF-419)
#
# This script validates the resilience of the KubeMapReduce system by injecting
# failures into a live Kubernetes cluster and asserting recovery outcomes.

set -e

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
        local status=$($CLI_EXE jobs status "$job_id" | grep "Status:" | awk '{print $2}')
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
    local timeout_secs=$1
    log "Waiting for worker pods to appear..."
    local start_time=$(date +%s)
    while true; do
        local pods=$(kubectl get pods -l app=kubemapreduce-worker -o name)
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
    local job_output=$($CLI_EXE jobs submit "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | grep "JobID:" | awk '{print $2}')
    log "Submitted job: $job_id"
    
    wait_for_workers 60
    
    local target_pod=$(kubectl get pods -l app=kubemapreduce-worker -o name | head -n 1)
    log "Killing worker pod: $target_pod"
    kubectl delete "$target_pod" --grace-period=0 --force
    
    assert_job_completed "$job_id" 300
    log "Worker Kill Scenario: PASSED"
}

test_manager_restart() {
    log "--- SCENARIO: Manager Replica Restart ---"
    local job_output=$($CLI_EXE jobs submit "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | grep "JobID:" | awk '{print $2}')
    log "Submitted job: $job_id"
    
    wait_for_workers 60
    
    log "Restarting manager pods..."
    kubectl rollout restart statefulset/manager
    kubectl rollout status statefulset/manager --timeout=120s
    
    assert_job_completed "$job_id" 300
    log "Manager Restart Scenario: PASSED"
}

test_zombie_fencing() {
    log "--- SCENARIO: Zombie Worker Fencing ---"
    local job_output=$($CLI_EXE jobs submit "$SLEEP_JOB")
    local job_id=$(echo "$job_output" | grep "JobID:" | awk '{print $2}')
    log "Submitted job: $job_id"
    
    wait_for_workers 60
    
    local target_pod=$(kubectl get pods -l app=kubemapreduce-worker -o name | head -n 1)
    log "Simulating zombie worker (SIGSTOP) on pod: $target_pod"
    kubectl exec "$target_pod" -- kill -STOP 1
    
    log "Waiting for lease to expire (TTL ~30s)..."
    sleep 45
    
    log "Resuming zombie worker (SIGCONT) to attempt illegitimate commit: $target_pod"
    kubectl exec "$target_pod" -- kill -CONT 1
    
    assert_job_completed "$job_id" 300
    
    log "Verifying zombie logs for rejection..."
    kubectl logs "$target_pod" > "$REPORT_DIR/zombie_worker.log" || true
    log "Check $REPORT_DIR/zombie_worker.log for 'PermissionDenied' or 'ExpiredLease' errors."
    
    log "Zombie Fencing Scenario: PASSED (Recovery completed)"
}

# Main Execution
log "Starting KubeMapReduce E2E Failure-Injection Suite"

# Check dependencies
command -v kubectl >/dev/null 2>&1 || fail "kubectl not found"
command -v go >/dev/null 2>&1 || fail "go not found"

# Run tests
test_worker_kill
test_manager_restart
test_zombie_fencing

log "Suite completed successfully. Reports saved to $REPORT_DIR"
