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

type WorkerOrchestrator interface {
	SpawnWorker(ctx context.Context, taskID string, managerAddr string) error
}

type KubeOrchestrator struct {
	clientset   kubernetes.Interface
	namespace   string
	workerImage string
}

func NewKubeOrchestrator(clientset kubernetes.Interface, namespace, workerImage string) *KubeOrchestrator {
	if namespace == "" {
		namespace = "default"
	}
	if workerImage == "" {
		workerImage = "kubemapreduce-worker:latest"
	}
	return &KubeOrchestrator{
		clientset:   clientset,
		namespace:   namespace,
		workerImage: workerImage,
	}
}

func (k *KubeOrchestrator) SpawnWorker(ctx context.Context, taskID string, managerAddr string) error {
	sanitizedTaskID := sanitizeForDNSLabel(taskID)
	jobName := buildWorkerJobName(sanitizedTaskID)
	backoffLimit := int32(0)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: k.namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":     "kubemapreduce-worker",
						"task_id": sanitizedTaskID,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "worker",
							Image: k.workerImage,
							Env: []corev1.EnvVar{
								{
									Name:  "TASK_ID",
									Value: taskID,
								},
								{
									Name:  "MANAGER_ADDR",
									Value: managerAddr,
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

type MockOrchestrator struct{}

func (m *MockOrchestrator) SpawnWorker(ctx context.Context, taskID string, managerAddr string) error {
	return nil
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

func buildWorkerJobName(sanitizedTaskID string) string {
	const (
		prefix       = "worker-"
		maxDNSLength = 63
	)

	if len(prefix)+len(sanitizedTaskID) <= maxDNSLength {
		return prefix + sanitizedTaskID
	}

	sum := sha256.Sum256([]byte(sanitizedTaskID))
	hash := hex.EncodeToString(sum[:])[:12]
	maxTaskPart := maxDNSLength - len(prefix) - len("-") - len(hash)
	if maxTaskPart < 1 {
		maxTaskPart = 1
	}
	if maxTaskPart > len(sanitizedTaskID) {
		maxTaskPart = len(sanitizedTaskID)
	}
	return fmt.Sprintf("%s%s-%s", prefix, sanitizedTaskID[:maxTaskPart], hash)
}
