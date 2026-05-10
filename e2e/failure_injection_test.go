package e2e

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// NOTE: These tests require a live Kubernetes cluster configured in ~/.kube/config
// and the kubemapreduce API running at the URL defined by API_URL.

func apiURL() string {
	url := os.Getenv("API_URL")
	if url == "" {
		return "http://localhost:8081"
	}
	return url
}

func submitSleepJob(t *testing.T) string {
	cmd := exec.Command("../bin/cli", "jobs", "submit",
		"--mapper", "../testdata/job3-sleep/mapper.py",
		"--reducer", "../testdata/job3-sleep/reducer.py",
		"--input", "../testdata/job3-sleep/input.jsonl")
	out, err := cmd.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			t.Fatalf("failed to submit job via cli: %v\nStderr: %s", err, string(exitError.Stderr))
		}
		t.Fatalf("failed to execute cli: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("failed to decode cli output: %v\nOutput: %s", err, string(out))
	}

	jobID, ok := result["jobId"].(string)
	if !ok {
		t.Fatalf("jobId not found in cli output: %v", string(out))
	}
	return jobID
}

func waitForJobCompletion(t *testing.T, jobID string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		cmd := exec.Command("../bin/cli", "jobs", "status", "--id", jobID)
		out, err := cmd.Output()
		if err == nil {
			var result map[string]interface{}
			if err := json.Unmarshal(out, &result); err == nil {
				state, _ := result["status"].(string)
				if state == "Completed" {
					return
				} else if state == "Failed" {
					t.Fatalf("job %s failed", jobID)
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("timeout waiting for job %s to complete", jobID)
}

func getWorkerPodForJob(t *testing.T, jobID string) string {
	for i := 0; i < 30; i++ {
		cmd := exec.Command("kubectl", "-n", "mapreduce", "get", "pods", "-l", "app=kubemapreduce-worker,job_id="+jobID, "-o", "jsonpath={.items[0].metadata.name}")
		out, err := cmd.Output()
		if err == nil {
			pod := strings.TrimSpace(string(out))
			if pod != "" {
				return pod
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("no worker pods found for job %s after 30s", jobID)
	return ""
}

func TestE2E_WorkerKillScenario(t *testing.T) {
	if os.Getenv("E2E_LIVE_CLUSTER") != "true" {
		t.Skip("Skipping E2E test in unit-test environment without a live cluster")
	}

	jobID := submitSleepJob(t)
	t.Logf("Submitted job: %s", jobID)

	// Wait for workers to spawn and be ready
	waitForWorkerPodsReady(t, jobID)

	podName := getWorkerPodForJob(t, jobID)
	t.Logf("Killing worker pod: %s", podName)

	cmd := exec.Command("kubectl", "-n", "mapreduce", "delete", "pod", podName, "--grace-period=0", "--force")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to delete pod: %v", err)
	}

	waitForJobCompletion(t, jobID, 10*time.Minute)
}

func TestE2E_ManagerRestartScenario(t *testing.T) {
	if os.Getenv("E2E_LIVE_CLUSTER") != "true" {
		t.Skip("Skipping E2E test in unit-test environment without a live cluster")
	}

	jobID := submitSleepJob(t)
	t.Logf("Submitted job: %s", jobID)

	time.Sleep(5 * time.Second)

	t.Logf("Restarting manager statefulset...")
	cmd := exec.Command("kubectl", "-n", "mapreduce", "rollout", "restart", "statefulset/manager")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to restart manager: %v", err)
	}

	cmd = exec.Command("kubectl", "-n", "mapreduce", "rollout", "status", "statefulset/manager", "--timeout=300s")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to wait for manager rollout: %v", err)
	}

	waitForJobCompletion(t, jobID, 10*time.Minute)
}

func TestE2E_ZombieFencingScenario(t *testing.T) {
	if os.Getenv("E2E_LIVE_CLUSTER") != "true" {
		t.Skip("Skipping E2E test in unit-test environment without a live cluster")
	}

	jobID := submitSleepJob(t)
	t.Logf("Submitted job: %s", jobID)

	// Wait for pods to be Ready before injecting failure
	waitForWorkerPodsReady(t, jobID)

	podName := getWorkerPodForJob(t, jobID)
	t.Logf("Simulating zombie worker (SIGSTOP) on pod: %s", podName)

	cmd := exec.Command("kubectl", "-n", "mapreduce", "exec", podName, "--", "sh", "-c", "kill -STOP 1")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to SIGSTOP worker: %v", err)
	}

	t.Log("Waiting for lease to expire (~20s)...")
	time.Sleep(20 * time.Second)

	t.Logf("Resuming zombie worker (SIGCONT) on pod: %s", podName)
	cmd = exec.Command("kubectl", "-n", "mapreduce", "exec", podName, "--", "sh", "-c", "kill -CONT 1")
	if err := cmd.Run(); err != nil {
		t.Logf("Warning: failed to SIGCONT worker (it might have been deleted by the orchestrator): %v", err)
	}

	waitForJobCompletion(t, jobID, 10*time.Minute)

	t.Log("Verifying zombie status/logs for fencing...")
	cmd = exec.Command("kubectl", "-n", "mapreduce", "get", "pod", podName)
	if err := cmd.Run(); err != nil {
		t.Logf("Zombie pod %s is gone. Checking if job completed successfully...", podName)
		cmd = exec.Command("../bin/cli", "jobs", "status", "--id", jobID)
		out, err := cmd.Output()
		if err == nil {
			var result map[string]interface{}
			if err := json.Unmarshal(out, &result); err == nil {
				state, _ := result["status"].(string)
				if state == "Completed" {
					t.Log("Job completed successfully and zombie was cleaned up. Fencing successful.")
					return
				}
			}
		}
		t.Fatalf("Zombie pod is gone but job is not completed: %s", string(out))
	}

	t.Log("Verifying zombie logs for rejection...")
	cmd = exec.Command("kubectl", "-n", "mapreduce", "logs", podName)
	out, _ := cmd.Output()
	logs := string(out)

	if !strings.Contains(strings.ToLower(logs), "permissiondenied") && !strings.Contains(strings.ToLower(logs), "expiredlease") && !strings.Contains(strings.ToLower(logs), "staleattempt") {
		t.Fatalf("Zombie fencing rejection not observed in logs:\n%s", logs)
	}
}

func waitForWorkerPodsReady(t *testing.T, jobID string) {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command("kubectl", "-n", "mapreduce", "get", "pods", "-l", "app=kubemapreduce-worker,job_id="+jobID, "-o", "jsonpath={.items[*].status.containerStatuses[*].ready}")
		out, err := cmd.Output()
		if err == nil {
			statuses := strings.Split(strings.TrimSpace(string(out)), " ")
			allReady := true
			for _, s := range statuses {
				if s != "true" {
					allReady = false
					break
				}
			}
			if allReady && len(statuses) > 0 {
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("worker pods for job %s not ready after 60s", jobID)
}
