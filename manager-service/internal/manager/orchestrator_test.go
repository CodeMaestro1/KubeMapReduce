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
	orchestrator := NewKubeOrchestrator(client, "default", "worker:latest")

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
	orchestrator := NewKubeOrchestrator(client, "default", "worker:latest")

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
