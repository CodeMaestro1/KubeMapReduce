package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// WorkerOrchestrator defines the interface for managing the lifecycle of physical worker processes.
//
// This abstraction allows the Manager to remain agnostic of the underlying execution platform
// (e.g., Kubernetes, local Docker, or bare processes), facilitating easier testing and
// future-proofing against platform migrations.
type WorkerOrchestrator interface {
	// SpawnWorker initiates a new worker process for a specific task attempt.
	//
	// Callers must provide a unique attemptID which acts as the fencing token in the gRPC layer.
	// The orchestrator is responsible for ensuring environment variables are correctly set so
	// the worker can dial back to the managerAddr and identify itself.
	SpawnWorker(ctx context.Context, taskID string, jobID string, attemptID string, managerAddr string) error

	// CancelJob terminates all active worker processes associated with a specific job.
	//
	// This is used during job cancellation or failure to immediately reclaim cluster resources.
	// Implementation should be idempotent and handle cases where some workers might already be dead.
	CancelJob(ctx context.Context, jobID string) error

	// DeleteWorkerJob removes the K8s Job (and its pod) for a specific task, identified by
	// the task_id label. Called before SpawnWorker on retry paths so the old pod is
	// evicted before the new attempt starts, preventing two pods from running concurrently.
	// Implementation must be idempotent: no error when no matching job exists.
	DeleteWorkerJob(ctx context.Context, taskID string) error
}

// KubeOrchestrator implements WorkerOrchestrator using Kubernetes Jobs.
//
// Each worker is wrapped in a K8s Job with a restartPolicy of Never. This ensures that
// task retries are controlled exclusively by the Manager's scheduler logic rather than
// the K8s kubelet, preventing "zombie" retries from interfering with new attempts.
type KubeOrchestrator struct {
	clientset        kubernetes.Interface
	namespace        string
	workerImage      string
	workerSecretName string
}

// NewKubeOrchestrator creates a new Kubernetes-backed orchestrator.
//
// It defaults to the "default" namespace and "kubemapreduce-worker:latest" image if not
// specified. If workerSecretName is empty it defaults to "kubemapreduce-secrets"; that
// Kubernetes Secret must contain the MinIO and RPC credentials that are injected into
// every worker pod via SecretKeyRef so that plaintext credentials never appear in the
// Job manifest.
func NewKubeOrchestrator(clientset kubernetes.Interface, namespace, workerImage, workerSecretName string) *KubeOrchestrator {
	if namespace == "" {
		namespace = "default"
	}
	if workerImage == "" {
		workerImage = "kubemapreduce-worker:latest"
	}
	if workerSecretName == "" {
		workerSecretName = "kubemapreduce-secrets"
	}
	return &KubeOrchestrator{
		clientset:        clientset,
		namespace:        namespace,
		workerImage:      workerImage,
		workerSecretName: workerSecretName,
	}
}

// SpawnWorker creates a K8s Job for a task attempt.
//
// It uses a deterministic naming scheme (worker-[taskID]-[hash]) to prevent duplicate jobs
// for the same attempt. The backoffLimit is set to 0 because the Manager handles retries
// at the application level to maintain strict state consistency in the DDS.
func (k *KubeOrchestrator) SpawnWorker(ctx context.Context, taskID string, jobID string, attemptID string, managerAddr string) error {
	sanitizedTaskID := sanitizeForDNSLabel(taskID)
	sanitizedJobID := sanitizeForDNSLabel(jobID)
	jobName := buildWorkerJobName(sanitizedTaskID, attemptID)
	backoffLimit := int32(0)
	labels := map[string]string{
		"app":     "kubemapreduce-worker",
		"task_id": sanitizedTaskID,
		"job_id":  sanitizedJobID,
	}

	falseVal := false
	trueVal := true

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: k.namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: &falseVal,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &trueVal,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "tmp",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "worker",
							Image: k.workerImage,
							Env: []corev1.EnvVar{
								{Name: "TASK_ID", Value: taskID},
								{Name: "JOB_ID", Value: jobID},
								{Name: "MANAGER_ADDR", Value: managerAddr},
								{Name: "ATTEMPT_ID", Value: attemptID},
								secretEnvVar("S3_ENDPOINT", k.workerSecretName, "MINIO_ENDPOINT"),
								secretEnvVar("S3_ACCESS_KEY", k.workerSecretName, "MINIO_ACCESS_KEY"),
								secretEnvVar("S3_SECRET_KEY", k.workerSecretName, "MINIO_SECRET_KEY"),
								secretEnvVar("MINIO_BUCKET", k.workerSecretName, "MINIO_BUCKET"),
								secretEnvVar("WORKER_RPC_TOKEN", k.workerSecretName, "MANAGER_WORKER_RPC_TOKEN"),
							},
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: &falseVal,
								ReadOnlyRootFilesystem:   &trueVal,
								RunAsNonRoot:             &trueVal,
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "tmp",
									MountPath: "/tmp",
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := k.clientset.BatchV1().Jobs(k.namespace).Create(ctx, job, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// CancelJob deletes all K8s Jobs tagged with the job_id label.
//
// PropagationPolicy is set to Background to allow the API call to return quickly while
// K8s cleans up the underlying pods asynchronously.
func (k *KubeOrchestrator) CancelJob(ctx context.Context, jobID string) error {
	sanitizedJobID := sanitizeForDNSLabel(jobID)
	policy := metav1.DeletePropagationBackground
	return k.clientset.BatchV1().Jobs(k.namespace).DeleteCollection(ctx,
		metav1.DeleteOptions{PropagationPolicy: &policy},
		metav1.ListOptions{LabelSelector: fmt.Sprintf("job_id=%s", sanitizedJobID)},
	)
}

// DeleteWorkerJob removes all K8s Jobs tagged with task_id=<taskID>, freeing the
// pod slot before a new attempt is spawned. It lists matching jobs and deletes
// each individually (rather than DeleteCollection) so unrelated jobs are never
// affected and per-job errors can be reported precisely.
// PropagationPolicy Background lets pods be reaped asynchronously.
// NotFound on any individual job is silently ignored (idempotent).
func (k *KubeOrchestrator) DeleteWorkerJob(ctx context.Context, taskID string) error {
	sanitizedTaskID := sanitizeForDNSLabel(taskID)
	jobs, err := k.clientset.BatchV1().Jobs(k.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("task_id=%s", sanitizedTaskID),
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("list jobs for task %s: %w", taskID, err)
	}
	policy := metav1.DeletePropagationBackground
	for i := range jobs.Items {
		delErr := k.clientset.BatchV1().Jobs(k.namespace).Delete(ctx, jobs.Items[i].Name,
			metav1.DeleteOptions{PropagationPolicy: &policy})
		if delErr != nil && !apierrors.IsNotFound(delErr) {
			return fmt.Errorf("delete job %s: %w", jobs.Items[i].Name, delErr)
		}
	}
	return nil
}

// MockOrchestrator is a no-op implementation for unit testing.
type MockOrchestrator struct{}

func (m *MockOrchestrator) SpawnWorker(ctx context.Context, taskID string, jobID string, attemptID string, managerAddr string) error {
	return nil
}

func (m *MockOrchestrator) CancelJob(ctx context.Context, jobID string) error {
	return nil
}

func (m *MockOrchestrator) DeleteWorkerJob(ctx context.Context, taskID string) error {
	return nil
}

// secretEnvVar constructs a corev1.EnvVar whose value is sourced from a Kubernetes
// Secret. Using this helper for all secret-backed variables keeps the secret name
// wired consistently and reduces the risk of copy-paste mistakes.
func secretEnvVar(envName, secretName, secretKey string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: envName,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  secretKey,
			},
		},
	}
}

func sanitizeForDNSLabel(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "task"
	}

	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}

	sanitized := strings.Trim(b.String(), "-")
	if sanitized == "" {
		return "task"
	}
	return sanitized
}

func buildWorkerJobName(sanitizedTaskID string, attemptID string) string {
	const (
		prefix       = "worker-"
		maxDNSLength = 63
	)
	attemptPart := sanitizeForDNSLabel(attemptID)
	nameBase := sanitizedTaskID + "-" + attemptPart

	if len(prefix)+len(nameBase) <= maxDNSLength {
		return prefix + nameBase
	}

	sum := sha256.Sum256([]byte(nameBase))
	hash := hex.EncodeToString(sum[:])[:12]
	maxTaskPart := maxDNSLength - len(prefix) - len("-") - len(hash)
	if maxTaskPart < 1 {
		maxTaskPart = 1
	}
	if maxTaskPart > len(nameBase) {
		maxTaskPart = len(nameBase)
	}
	return fmt.Sprintf("%s%s-%s", prefix, nameBase[:maxTaskPart], hash)
}
