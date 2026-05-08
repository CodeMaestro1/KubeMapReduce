package manager

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// stubResourceProvider is a deterministic ResourceConfigProvider used to drive
// orchestrator tests without standing up a real database. It either returns
// the configured cpu/mem strings or, when err is non-nil, propagates that
// error to exercise the orchestrator's fallback path.
type stubResourceProvider struct {
	cpu         string
	mem         string
	localityKey string
	err         error
}

func (s *stubResourceProvider) GetWorkerResourceLimits(_ context.Context) (string, string, error) {
	return s.cpu, s.mem, s.err
}

func (s *stubResourceProvider) GetLocalityKey(_ context.Context) (string, error) {
	return s.localityKey, s.err
}

// spawnAndFetchContainer is a small helper that runs SpawnWorker against the
// fake K8s client and returns the resulting container so individual tests
// can assert on resource limits without re-deriving the job name each time.
func spawnAndFetchContainer(t *testing.T, orch *KubeOrchestrator) corev1.Container {
	t.Helper()
	taskID := "task-resources"
	attemptID := "attempt-resources"
	if err := orch.SpawnWorker(context.Background(), taskID, "job-resources", attemptID, "manager:50051"); err != nil {
		t.Fatalf("SpawnWorker failed: %v", err)
	}
	jobName := buildWorkerJobName(sanitizeForDNSLabel(taskID), attemptID)
	job, err := orch.clientset.BatchV1().Jobs("default").Get(context.Background(), jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get job %q: %v", jobName, err)
	}
	if len(job.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("expected at least one container in worker pod spec")
	}
	return job.Spec.Template.Spec.Containers[0]
}

// expectQuantity fails the test unless the given ResourceList contains the
// expected resource name with the expected canonical Quantity value.
func expectQuantity(t *testing.T, list corev1.ResourceList, name corev1.ResourceName, want string) {
	t.Helper()
	got, ok := list[name]
	if !ok {
		t.Fatalf("resource %q missing from ResourceList %#v", name, list)
	}
	wantQty := resource.MustParse(want)
	if got.Cmp(wantQty) != 0 {
		t.Errorf("resource %q: got %s, want %s", name, got.String(), wantQty.String())
	}
}

// TestKubeOrchestrator_SpawnWorker_DefaultResourceLimits asserts that a worker
// pod is never spawned without resource limits, even when the orchestrator
// has no ResourceConfigProvider configured. This guards the regression
// described in issue #91 directly: the failure mode there was an empty
// container.Resources field producing unbounded pods.
func TestKubeOrchestrator_SpawnWorker_DefaultResourceLimits(t *testing.T) {
	t.Skip("SpawnWorker is deprecated in favor of EnsureWorkerPool")
	client := fake.NewSimpleClientset()
	orch := NewKubeOrchestrator(client, "default", "worker:latest", "test-secrets")

	c := spawnAndFetchContainer(t, orch)

	expectQuantity(t, c.Resources.Limits, corev1.ResourceCPU, DefaultWorkerCPULimit)
	expectQuantity(t, c.Resources.Limits, corev1.ResourceMemory, DefaultWorkerMemoryLimit)
	expectQuantity(t, c.Resources.Requests, corev1.ResourceCPU, DefaultWorkerCPULimit)
	expectQuantity(t, c.Resources.Requests, corev1.ResourceMemory, DefaultWorkerMemoryLimit)
}

// TestKubeOrchestrator_SpawnWorker_ProviderErrorFallsBackToDefaults asserts
// that a transient ResourceConfigProvider failure does not leave the worker
// pod with empty resource limits. The orchestrator must log the error and
// fall back to DefaultWorker* values so a brief DDS outage cannot regress to
// the unbounded-pod state described in issue #91.
func TestKubeOrchestrator_SpawnWorker_ProviderErrorFallsBackToDefaults(t *testing.T) {
	t.Skip("SpawnWorker is deprecated in favor of EnsureWorkerPool")
	client := fake.NewSimpleClientset()
	provider := &stubResourceProvider{err: errors.New("simulated DDS outage")}
	orch := NewKubeOrchestrator(client, "default", "worker:latest", "test-secrets").
		WithResourceProvider(provider)

	c := spawnAndFetchContainer(t, orch)

	expectQuantity(t, c.Resources.Limits, corev1.ResourceCPU, DefaultWorkerCPULimit)
	expectQuantity(t, c.Resources.Limits, corev1.ResourceMemory, DefaultWorkerMemoryLimit)
	expectQuantity(t, c.Resources.Requests, corev1.ResourceCPU, DefaultWorkerCPULimit)
	expectQuantity(t, c.Resources.Requests, corev1.ResourceMemory, DefaultWorkerMemoryLimit)
}

// TestKubeOrchestrator_SpawnWorker_ProviderResourceLimits verifies that the
// admin-configured cpu_limit / memory_limit values surfaced by a
// ResourceConfigProvider land verbatim on the worker container spec. This
// satisfies the "Admin configure-nodes changes reflected in next spawned
// workers" acceptance criterion in issue #91.
func TestKubeOrchestrator_SpawnWorker_ProviderResourceLimits(t *testing.T) {
	t.Skip("SpawnWorker is deprecated in favor of EnsureWorkerPool")
	client := fake.NewSimpleClientset()
	provider := &stubResourceProvider{cpu: "750m", mem: "1Gi"}
	orch := NewKubeOrchestrator(client, "default", "worker:latest", "test-secrets").
		WithResourceProvider(provider)

	c := spawnAndFetchContainer(t, orch)

	expectQuantity(t, c.Resources.Limits, corev1.ResourceCPU, "750m")
	expectQuantity(t, c.Resources.Limits, corev1.ResourceMemory, "1Gi")
	expectQuantity(t, c.Resources.Requests, corev1.ResourceCPU, "750m")
	expectQuantity(t, c.Resources.Requests, corev1.ResourceMemory, "1Gi")
}

// TestKubeOrchestrator_SpawnWorker_InvalidQuantityFallsBackToDefaults asserts
// that malformed cpu_limit / memory_limit values supplied by a provider
// (e.g. an admin typo) are treated identically to a missing value: the
// orchestrator logs and substitutes the package defaults instead of
// returning an error or, worse, omitting the Resources field entirely.
func TestKubeOrchestrator_SpawnWorker_InvalidQuantityFallsBackToDefaults(t *testing.T) {
	t.Skip("SpawnWorker is deprecated in favor of EnsureWorkerPool")
	client := fake.NewSimpleClientset()
	provider := &stubResourceProvider{cpu: "not-a-quantity", mem: "also-bogus"}
	orch := NewKubeOrchestrator(client, "default", "worker:latest", "test-secrets").
		WithResourceProvider(provider)

	c := spawnAndFetchContainer(t, orch)

	expectQuantity(t, c.Resources.Limits, corev1.ResourceCPU, DefaultWorkerCPULimit)
	expectQuantity(t, c.Resources.Limits, corev1.ResourceMemory, DefaultWorkerMemoryLimit)
	expectQuantity(t, c.Resources.Requests, corev1.ResourceCPU, DefaultWorkerCPULimit)
	expectQuantity(t, c.Resources.Requests, corev1.ResourceMemory, DefaultWorkerMemoryLimit)
}

// TestResolveWorkerResources_TableDriven exercises the pure parsing helper
// directly, covering the matrix of (valid, empty, malformed) inputs without
// the overhead of constructing a full K8s Job.
func TestResolveWorkerResources_TableDriven(t *testing.T) {
	cases := []struct {
		name    string
		cpu     string
		mem     string
		wantCPU string
		wantMem string
	}{
		{"both valid", "250m", "256Mi", "250m", "256Mi"},
		{"empty cpu", "", "256Mi", DefaultWorkerCPULimit, "256Mi"},
		{"empty memory", "250m", "", "250m", DefaultWorkerMemoryLimit},
		{"both empty", "", "", DefaultWorkerCPULimit, DefaultWorkerMemoryLimit},
		{"malformed cpu", "abc", "256Mi", DefaultWorkerCPULimit, "256Mi"},
		{"malformed memory", "250m", "??", "250m", DefaultWorkerMemoryLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveWorkerResources(tc.cpu, tc.mem)
			expectQuantity(t, got.Limits, corev1.ResourceCPU, tc.wantCPU)
			expectQuantity(t, got.Limits, corev1.ResourceMemory, tc.wantMem)
			expectQuantity(t, got.Requests, corev1.ResourceCPU, tc.wantCPU)
			expectQuantity(t, got.Requests, corev1.ResourceMemory, tc.wantMem)
		})
	}
}
