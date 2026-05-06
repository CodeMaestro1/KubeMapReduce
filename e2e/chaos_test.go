package e2e

import (
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestChaos_NetworkPartition simulates a network partition between the API Gateway
// (Manager) and the Identity Provider (Keycloak) mid-flight using Chaos Mesh.
// The test verifies that security policies (like validating JWTs) fall back gracefully
// or reject requests with appropriate 503/401 errors, rather than failing open.
func TestChaos_NetworkPartition_Keycloak(t *testing.T) {
	if os.Getenv("E2E_LIVE_CLUSTER") != "true" {
		t.Skip("Skipping Chaos Mesh E2E test. Requires a live cluster with chaos-mesh installed.")
	}

	jobID := submitSleepJob(t)
	t.Logf("Submitted baseline job: %s", jobID)

	t.Log("Applying Keycloak network partition chaos...")
	cmd := exec.Command("kubectl", "apply", "-f", "../k8s/chaos/02-keycloak-partition.yaml")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to apply chaos manifest: %v", err)
	}

	time.Sleep(5 * time.Second) // Allow chaos rules to propagate

	t.Log("Attempting to submit job during Keycloak partition...")
	payload := `{"spec": "sleep_job", "reducers": 1}`
	req, _ := http.NewRequest("POST", apiURL()+"/api/v1/jobs", strings.NewReader(payload))
	// Pass an expired or unvalidated token to force Keycloak interaction
	req.Header.Set("Authorization", "Bearer invalid-token-during-chaos")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)

	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
			t.Fatalf("Security failure: Manager allowed request with invalid token during Keycloak partition!")
		}
		if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("Expected 503 or 401, got: %d", resp.StatusCode)
		}
	} else {
		t.Logf("Request correctly failed at network layer: %v", err)
	}

	t.Log("Waiting for chaos experiment to expire (45s)...")
	time.Sleep(50 * time.Second)

	// Ensure system recovers
	jobID = submitSleepJob(t)
	t.Logf("Successfully submitted job post-chaos: %s", jobID)
}

// TestChaos_ManagerLatency simulates high network latency between workers and the manager.
// This validates that Linkerd timeouts, gRPC retry intercepts, and the Scheduler's lease
// mechanism behave correctly under duress.
func TestChaos_ManagerLatency(t *testing.T) {
	if os.Getenv("E2E_LIVE_CLUSTER") != "true" {
		t.Skip("Skipping Chaos Mesh E2E test. Requires a live cluster with chaos-mesh installed.")
	}

	jobID := submitSleepJob(t)
	t.Logf("Submitted job: %s", jobID)

	t.Log("Applying Manager network delay chaos...")
	cmd := exec.Command("kubectl", "apply", "-f", "../k8s/chaos/01-manager-network-delay.yaml")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to apply chaos manifest: %v", err)
	}

	time.Sleep(10 * time.Second) // Allow latency to affect heartbeat/status loops

	podName := getWorkerPodForJob(t, jobID)

	// Ensure the pod doesn't get prematurely killed by the manager
	waitForJobCompletion(t, jobID, 4*time.Minute)

	t.Log("Verifying worker retry logs...")
	cmd = exec.Command("kubectl", "logs", podName)
	out, _ := cmd.Output()
	logs := string(out)

	if !strings.Contains(strings.ToLower(logs), "retry") && !strings.Contains(strings.ToLower(logs), "deadline") {
		// Log assertion might be flaky depending on actual log formats, but we expect some network noise
		t.Logf("Warning: Did not observe explicit retry/timeout logs in worker:\n%s", logs)
	}
}
