package manager

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
	jobName := fmt.Sprintf("worker-%s", taskID)
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
						"task_id": taskID,
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
	return err
}

type MockOrchestrator struct{}

func (m *MockOrchestrator) SpawnWorker(ctx context.Context, taskID string, managerAddr string) error {
	return nil
}
