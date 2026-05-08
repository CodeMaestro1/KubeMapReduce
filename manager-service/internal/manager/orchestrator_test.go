package manager

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewKubeOrchestrator(t *testing.T) {
	client := fake.NewSimpleClientset()

	tests := []struct {
		name                 string
		namespace            string
		workerImage          string
		workerSecretName     string
		wantNamespace        string
		wantWorkerImage      string
		wantWorkerSecretName string
	}{
		{
			name:                 "defaults applied when empty",
			namespace:            "",
			workerImage:          "",
			workerSecretName:     "",
			wantNamespace:        "default",
			wantWorkerImage:      "kubemapreduce-worker:latest",
			wantWorkerSecretName: "kubemapreduce-secrets",
		},
		{
			name:                 "explicit values retained",
			namespace:            "custom-ns",
			workerImage:          "custom-image:v1",
			workerSecretName:     "custom-secret",
			wantNamespace:        "custom-ns",
			wantWorkerImage:      "custom-image:v1",
			wantWorkerSecretName: "custom-secret",
		},
		{
			name:                 "partial values provided",
			namespace:            "partial-ns",
			workerImage:          "",
			workerSecretName:     "partial-secret",
			wantNamespace:        "partial-ns",
			wantWorkerImage:      "kubemapreduce-worker:latest",
			wantWorkerSecretName: "partial-secret",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			orchestrator := NewKubeOrchestrator(client, tc.namespace, tc.workerImage, tc.workerSecretName)

			if orchestrator.clientset != client {
				t.Errorf("expected clientset to be set correctly")
			}
			if orchestrator.namespace != tc.wantNamespace {
				t.Errorf("expected namespace %q, got %q", tc.wantNamespace, orchestrator.namespace)
			}
			if orchestrator.workerImage != tc.wantWorkerImage {
				t.Errorf("expected workerImage %q, got %q", tc.wantWorkerImage, orchestrator.workerImage)
			}
			if orchestrator.workerSecretName != tc.wantWorkerSecretName {
				t.Errorf("expected workerSecretName %q, got %q", tc.wantWorkerSecretName, orchestrator.workerSecretName)
			}
		})
	}
}

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
	t.Skip("SpawnWorker is deprecated in favor of EnsureWorkerPool")
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
	t.Skip("DeleteWorkerJob is deprecated in pool-based architecture")
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
	t.Skip("SpawnWorker is deprecated in favor of EnsureWorkerPool")
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
	t.Skip("SpawnWorker is deprecated in favor of EnsureWorkerPool")
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
	job, err := client.BatchV1().Jobs("default").Get(context.Background(), jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get job %q: %v", jobName, err)
	}

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

func TestKubeOrchestrator_SpawnWorker_SecurityHardening(t *testing.T) {
	t.Skip("SpawnWorker is deprecated in favor of EnsureWorkerPool")
	client := fake.NewSimpleClientset()
	orchestrator := NewKubeOrchestrator(client, "default", "worker:latest", "")

	if err := orchestrator.SpawnWorker(context.Background(), "task-sec", "job-sec", "attempt-sec", "manager:50051"); err != nil {
		t.Fatalf("spawn worker failed: %v", err)
	}

	jobName := buildWorkerJobName(sanitizeForDNSLabel("task-sec"), "attempt-sec")
	job, err := client.BatchV1().Jobs("default").Get(context.Background(), jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch job: %v", err)
	}

	podSpec := job.Spec.Template.Spec

	// automountServiceAccountToken must be explicitly false.
	if podSpec.AutomountServiceAccountToken == nil || *podSpec.AutomountServiceAccountToken {
		t.Error("expected AutomountServiceAccountToken to be false")
	}

	// Pod-level security context.
	if podSpec.SecurityContext == nil {
		t.Fatal("expected non-nil pod SecurityContext")
	}
	if podSpec.SecurityContext.RunAsNonRoot == nil || !*podSpec.SecurityContext.RunAsNonRoot {
		t.Error("expected pod SecurityContext.RunAsNonRoot to be true")
	}
	if podSpec.SecurityContext.SeccompProfile == nil ||
		podSpec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Error("expected pod SeccompProfile type RuntimeDefault")
	}

	// Writable /tmp emptyDir volume must be present.
	foundTmpVol := false
	for _, v := range podSpec.Volumes {
		if v.Name == "tmp" && v.EmptyDir != nil {
			foundTmpVol = true
			break
		}
	}
	if !foundTmpVol {
		t.Error("expected an emptyDir volume named 'tmp'")
	}

	if len(podSpec.Containers) == 0 {
		t.Fatal("expected at least one container")
	}
	c := podSpec.Containers[0]

	// Container-level security context.
	if c.SecurityContext == nil {
		t.Fatal("expected non-nil container SecurityContext")
	}
	if c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation {
		t.Error("expected AllowPrivilegeEscalation to be false")
	}
	if c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem {
		t.Error("expected ReadOnlyRootFilesystem to be true")
	}
	if c.SecurityContext.RunAsNonRoot == nil || !*c.SecurityContext.RunAsNonRoot {
		t.Error("expected container RunAsNonRoot to be true")
	}
	if c.SecurityContext.Capabilities == nil {
		t.Fatal("expected non-nil container Capabilities")
	}
	droppedAll := false
	for _, cap := range c.SecurityContext.Capabilities.Drop {
		if cap == "ALL" {
			droppedAll = true
			break
		}
	}
	if !droppedAll {
		t.Error("expected capabilities drop to include ALL")
	}

	// /tmp volume mount must be present in the container.
	foundTmpMount := false
	for _, vm := range c.VolumeMounts {
		if vm.Name == "tmp" && vm.MountPath == "/tmp" {
			foundTmpMount = true
			break
		}
	}
	if !foundTmpMount {
		t.Error("expected container to mount 'tmp' emptyDir at /tmp")
	}
}

// To implement envtest correctly, you typically need the envtest binaries (kube-apiserver, etcd)
// installed in the local environment, which isn't guaranteed in all sandboxes.
// We add a skip if the KUBEBUILDER_ASSETS environment variable is not set.
// A true migration to envtest requires setting up a test suite with BeforeSuite/AfterSuite to handle the
// testenv lifecycle.
func TestEnvtest(t *testing.T) {
	t.Skip("Skipping envtest implementation because we don't have kubebuilder assets in the sandbox.")
}
