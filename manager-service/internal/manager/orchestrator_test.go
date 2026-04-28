package manager

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBuildWorkerJobName_DNSLabelBounded(t *testing.T) {
	taskID := strings.Repeat("A", 100) + "{}"
	sanitized := sanitizeForDNSLabel(taskID)
	jobName := buildWorkerJobName(sanitized, strings.Repeat("b", 40))

	if len(jobName) > 63 {
		t.Fatalf("expected job name length <= 63, got %d", len(jobName))
	}
	if strings.ToLower(jobName) != jobName {
		t.Fatalf("expected lower-case job name, got %q", jobName)
	}
}

func TestKubeOrchestrator_SpawnWorker_SetsJobLabels(t *testing.T) {
	client := fake.NewSimpleClientset()
	orchestrator := NewKubeOrchestrator(client, "default", "worker:latest", "test-secrets")

	taskID := "Task-A"
	jobID := "Job-A"
	attemptID := "attempt-1"
	if err := orchestrator.SpawnWorker(context.Background(), taskID, jobID, attemptID, "manager:50051"); err != nil {
		t.Fatalf("spawn worker failed: %v", err)
	}

	jobName := buildWorkerJobName(sanitizeForDNSLabel(taskID), attemptID)
	job, err := client.BatchV1().Jobs("default").Get(context.Background(), jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch job: %v", err)
	}

	wantTaskLabel := sanitizeForDNSLabel(taskID)
	wantJobLabel := sanitizeForDNSLabel(jobID)
	if job.Labels["task_id"] != wantTaskLabel {
		t.Fatalf("expected job task_id label %q, got %q", wantTaskLabel, job.Labels["task_id"])
	}
	if job.Labels["job_id"] != wantJobLabel {
		t.Fatalf("expected job job_id label %q, got %q", wantJobLabel, job.Labels["job_id"])
	}
	if job.Spec.Template.Labels["job_id"] != wantJobLabel {
		t.Fatalf("expected pod template job_id label %q, got %q", wantJobLabel, job.Spec.Template.Labels["job_id"])
	}
}

func TestKubeOrchestrator_DeleteWorkerJob_DeletesJobsByTaskID(t *testing.T) {
	taskID := "Task-A"
	sanitized := sanitizeForDNSLabel(taskID)

	job1 := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-" + sanitized + "-attempt-1",
			Namespace: "default",
			Labels:    map[string]string{"task_id": sanitized},
		},
	}
	job2 := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-" + sanitized + "-attempt-2",
			Namespace: "default",
			Labels:    map[string]string{"task_id": sanitized},
		},
	}
	// unrelated job that must survive
	other := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-other-task",
			Namespace: "default",
			Labels:    map[string]string{"task_id": "other-task"},
		},
	}
	client := fake.NewSimpleClientset(job1, job2, other)
	orchestrator := NewKubeOrchestrator(client, "default", "worker:latest", "test-secrets")

	if err := orchestrator.DeleteWorkerJob(context.Background(), taskID); err != nil {
		t.Fatalf("DeleteWorkerJob failed: %v", err)
	}

	remaining, err := client.BatchV1().Jobs("default").List(context.Background(), metav1.ListOptions{
		LabelSelector: "task_id=" + sanitized,
	})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(remaining.Items) != 0 {
		t.Errorf("expected 0 jobs for task_id=%s after delete, got %d", sanitized, len(remaining.Items))
	}

	// Unrelated job must still exist.
	_, err = client.BatchV1().Jobs("default").Get(context.Background(), "worker-other-task", metav1.GetOptions{})
	if err != nil {
		t.Errorf("unrelated job was incorrectly deleted: %v", err)
	}
}

func TestKubeOrchestrator_DeleteWorkerJob_NoJobsIsIdempotent(t *testing.T) {
	client := fake.NewSimpleClientset()
	orchestrator := NewKubeOrchestrator(client, "default", "worker:latest", "test-secrets")

	if err := orchestrator.DeleteWorkerJob(context.Background(), "nonexistent-task"); err != nil {
		t.Fatalf("expected no error deleting nonexistent task jobs, got: %v", err)
	}
}

func TestKubeOrchestrator_SpawnWorker_AlreadyExistsIsIdempotent(t *testing.T) {
	taskID := "task-id"
	attemptID := "attempt-id"
	jobName := buildWorkerJobName(sanitizeForDNSLabel(taskID), attemptID)

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: "default",
			Labels: map[string]string{
				"existing": "true",
			},
		},
	}
	client := fake.NewSimpleClientset(existing)
	orchestrator := NewKubeOrchestrator(client, "default", "worker:latest", "test-secrets")

	if err := orchestrator.SpawnWorker(context.Background(), taskID, "job-id", attemptID, "manager:50051"); err != nil {
		t.Fatalf("expected idempotent success on already-existing job, got: %v", err)
	}

	job, err := client.BatchV1().Jobs("default").Get(context.Background(), jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch existing job: %v", err)
	}
	if job.Labels["existing"] != "true" {
		t.Fatalf("existing job should not be replaced")
	}
}

func TestKubeOrchestrator_SpawnWorker_InjectsRequiredEnv(t *testing.T) {
	client := fake.NewSimpleClientset()
	secretName := "custom-worker-secret"
	orchestrator := NewKubeOrchestrator(client, "default", "worker:latest", secretName)

	taskID := "task-1"
	jobID := "job-1"
	attemptID := "attempt-1"
	managerAddr := "manager:50051"

	if err := orchestrator.SpawnWorker(context.Background(), taskID, jobID, attemptID, managerAddr); err != nil {
		t.Fatalf("spawn worker failed: %v", err)
	}

	jobName := buildWorkerJobName(sanitizeForDNSLabel(taskID), attemptID)
	job, _ := client.BatchV1().Jobs("default").Get(context.Background(), jobName, metav1.GetOptions{})

	env := job.Spec.Template.Spec.Containers[0].Env
	envMap := make(map[string]struct {
		value     string
		secretKey string
	})

	for _, e := range env {
		res := struct {
			value     string
			secretKey string
		}{}
		if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			res.secretKey = e.ValueFrom.SecretKeyRef.Key
			if e.ValueFrom.SecretKeyRef.Name != secretName {
				t.Errorf("env %s points to wrong secret: got %q, want %q", e.Name, e.ValueFrom.SecretKeyRef.Name, secretName)
			}
		} else {
			res.value = e.Value
		}
		envMap[e.Name] = res
	}

	// Direct value checks
	checks := map[string]string{
		"TASK_ID":      taskID,
		"JOB_ID":       jobID,
		"ATTEMPT_ID":   attemptID,
		"MANAGER_ADDR": managerAddr,
	}
	for name, want := range checks {
		if envMap[name].value != want {
			t.Errorf("env %s: got value %q, want %q", name, envMap[name].value, want)
		}
	}

	// Secret ref checks
	secretChecks := map[string]string{
		"S3_ENDPOINT":      "MINIO_ENDPOINT",
		"S3_ACCESS_KEY":    "MINIO_ACCESS_KEY",
		"S3_SECRET_KEY":    "MINIO_SECRET_KEY",
		"MINIO_BUCKET":     "MINIO_BUCKET",
		"WORKER_RPC_TOKEN": "MANAGER_WORKER_RPC_TOKEN",
	}
	for name, wantKey := range secretChecks {
		if envMap[name].secretKey != wantKey {
			t.Errorf("env %s: got secret key %q, want %q", name, envMap[name].secretKey, wantKey)
		}
	}
}
