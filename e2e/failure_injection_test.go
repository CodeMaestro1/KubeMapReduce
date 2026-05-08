package e2e

import (
	"encoding/json"
	"net/http"
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
	payload := `{"spec": "sleep_job", "reducers": 1}`
	req, err := http.NewRequest("POST", apiURL()+"/api/v1/jobs", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	// In a real environment we would need a valid token. Mocking for now since E2E assumes auth.
	req.Header.Set("Authorization", "Bearer mock-e2e-token")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code submitting job: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	jobID, ok := result["jobId"].(string)
	if !ok {
		t.Fatalf("jobId not found in response: %v", result)
	}
	return jobID
}

func waitForJobCompletion(t *testing.T, jobID string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}

	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", apiURL()+"/api/v1/jobs/"+jobID, nil)
		req.Header.Set("Authorization", "Bearer mock-e2e-token")

		resp, err := client.Do(req)
		if err == nil {
			var status map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&status)
			resp.Body.Close()

			state, _ := status["status"].(string)
			if state == "Completed" {
				return
			} else if state == "Failed" {
				t.Fatalf("job %s failed", jobID)
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timeout waiting for job %s to complete", jobID)
}

func getWorkerPodForJob(t *testing.T, jobID string) string {
	cmd := exec.Command("kubectl", "get", "pods", "-l", "app=kubemapreduce-worker,job_id="+jobID, "-o", "jsonpath={.items[0].metadata.name}")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("failed to get worker pod: %v", err)
	}
	pod := strings.TrimSpace(string(out))
	if pod == "" {
		t.Fatalf("no worker pods found for job %s", jobID)
	}
	return pod
}

func TestE2E_WorkerKillScenario(t *testing.T) {
	if os.Getenv("E2E_LIVE_CLUSTER") != "true" {
		t.Skip("Skipping E2E test in unit-test environment without a live cluster")
	}

	jobID := submitSleepJob(t)
	t.Logf("Submitted job: %s", jobID)

	// Wait for workers to spawn
	time.Sleep(10 * time.Second)

	podName := getWorkerPodForJob(t, jobID)
	t.Logf("Killing worker pod: %s", podName)

	cmd := exec.Command("kubectl", "delete", "pod", podName, "--grace-period=0", "--force")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to delete pod: %v", err)
	}

	waitForJobCompletion(t, jobID, 3*time.Minute)
}

func TestE2E_ManagerRestartScenario(t *testing.T) {
	if os.Getenv("E2E_LIVE_CLUSTER") != "true" {
		t.Skip("Skipping E2E test in unit-test environment without a live cluster")
	}

	jobID := submitSleepJob(t)
	t.Logf("Submitted job: %s", jobID)

	time.Sleep(5 * time.Second)

	t.Logf("Restarting manager statefulset...")
	cmd := exec.Command("kubectl", "rollout", "restart", "statefulset/manager")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to restart manager: %v", err)
	}

	cmd = exec.Command("kubectl", "rollout", "status", "statefulset/manager", "--timeout=120s")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to wait for manager rollout: %v", err)
	}

	waitForJobCompletion(t, jobID, 3*time.Minute)
}

func TestE2E_ZombieFencingScenario(t *testing.T) {
	if os.Getenv("E2E_LIVE_CLUSTER") != "true" {
		t.Skip("Skipping E2E test in unit-test environment without a live cluster")
	}

	jobID := submitSleepJob(t)
	t.Logf("Submitted job: %s", jobID)

	time.Sleep(10 * time.Second)

	podName := getWorkerPodForJob(t, jobID)
	t.Logf("Simulating zombie worker (SIGSTOP) on pod: %s", podName)

	cmd := exec.Command("kubectl", "exec", podName, "--", "kill", "-STOP", "1")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to SIGSTOP worker: %v", err)
	}

	t.Log("Waiting for lease to expire (~60s)...")
	time.Sleep(60 * time.Second)

	t.Logf("Resuming zombie worker (SIGCONT) on pod: %s", podName)
	cmd = exec.Command("kubectl", "exec", podName, "--", "kill", "-CONT", "1")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to SIGCONT worker: %v", err)
	}

	waitForJobCompletion(t, jobID, 3*time.Minute)

	t.Log("Verifying zombie logs for rejection...")
	cmd = exec.Command("kubectl", "logs", podName)
	out, _ := cmd.Output()
	logs := string(out)

	if !strings.Contains(strings.ToLower(logs), "permissiondenied") && !strings.Contains(strings.ToLower(logs), "expiredlease") {
		t.Fatalf("Zombie fencing rejection not observed in logs:\n%s", logs)
	}
}
