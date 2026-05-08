package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// WorkerOrchestrator defines the interface for managing the lifecycle of physical worker processes.
//
// This abstraction allows the Manager to remain agnostic of the underlying execution platform
// (e.g., Kubernetes, local Docker, or bare processes), facilitating easier testing and
// future-proofing against platform migrations.
type WorkerOrchestrator interface {
	// EnsureWorkerPool ensures that a pool of workers is running for a specific job.
	// It uses a K8s Deployment or similar to maintain the desired number of replicas.
	EnsureWorkerPool(ctx context.Context, jobID string, numWorkers int, managerAddr string) error

	// CancelJob terminates all active worker processes associated with a specific job.
	//
	// This is used during job cancellation or failure to immediately reclaim cluster resources.
	// Implementation should be idempotent and handle cases where some workers might already be dead.
	CancelJob(ctx context.Context, jobID string) error
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
	resourceProvider ResourceConfigProvider
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

// WithResourceProvider attaches a ResourceConfigProvider that SpawnWorker
// consults to set CPU and memory limits on the worker container. Passing nil
// disables provider-driven limits and reverts to the package defaults
// (DefaultWorkerCPULimit / DefaultWorkerMemoryLimit). The setter returns the
// receiver so callers can chain it onto NewKubeOrchestrator at construction
// time without splitting the wiring across multiple statements.
func (k *KubeOrchestrator) WithResourceProvider(p ResourceConfigProvider) *KubeOrchestrator {
	k.resourceProvider = p
	return k
}

// resolveContainerResources returns the corev1.ResourceRequirements applied to
// every spawned worker container.
//
// When no ResourceConfigProvider is configured the package defaults are used,
// guaranteeing that issue #91 (unbounded worker pods) cannot regress even if
// production wiring forgets to call WithResourceProvider. Provider errors are
// logged and the defaults are substituted so the spawn path never fails just
// because the DDS is briefly unreachable.
func (k *KubeOrchestrator) resolveContainerResources(ctx context.Context) corev1.ResourceRequirements {
	cpuLimit := DefaultWorkerCPULimit
	memLimit := DefaultWorkerMemoryLimit
	if k.resourceProvider != nil {
		cpu, mem, err := k.resourceProvider.GetWorkerResourceLimits(ctx)
		if err != nil {
			slog.WarnContext(ctx, "failed to read worker resource limits, using defaults",
				slog.String("component", "orchestrator"),
				slog.Any("err", err),
			)
		} else {
			cpuLimit = cpu
			memLimit = mem
		}
	}
	return resolveWorkerResources(cpuLimit, memLimit)
}

// resolveLocalityKey returns the Kubernetes topology key used for pod affinity.
func (k *KubeOrchestrator) resolveLocalityKey(ctx context.Context) string {
	if k.resourceProvider != nil {
		key, err := k.resourceProvider.GetLocalityKey(ctx)
		if err != nil {
			slog.WarnContext(ctx, "failed to read locality key, disabling locality",
				slog.String("component", "orchestrator"),
				slog.Any("err", err),
			)
			return ""
		}
		return key
	}
	return ""
}

// SpawnWorker creates a K8s Job for a task attempt.
//
// It uses a deterministic naming scheme (worker-[taskID]-[hash]) to prevent duplicate jobs
// for the same attempt. The backoffLimit is set to 0 because the Manager handles retries
// at the application level to maintain strict state consistency in the DDS.
//
// The worker container's Resources field is populated from
// SYSTEM_CONFIG.cpu_limit / memory_limit via the configured
// ResourceConfigProvider (see WithResourceProvider). When no provider is
// configured, or the provider returns an error, or the configured values
// fail resource.ParseQuantity, the orchestrator falls back to
// DefaultWorkerCPULimit / DefaultWorkerMemoryLimit so worker pods are never
// scheduled with unbounded resource usage. This closes the regression
// described in issue #91.
// EnsureWorkerPool ensures that a pool of workers is running for a specific job.
// It uses a K8s Deployment to maintain the desired number of replicas.
func (k *KubeOrchestrator) EnsureWorkerPool(ctx context.Context, jobID string, numWorkers int, managerAddr string) error {
	sanitizedJobID := sanitizeForDNSLabel(jobID)
	deploymentName := fmt.Sprintf("worker-pool-%s", sanitizedJobID)
	replicas := int32(numWorkers)
	labels := map[string]string{
		"app":    "kubemapreduce-worker",
		"job_id": sanitizedJobID,
	}

	falseVal := false
	trueVal := true

	resources := k.resolveContainerResources(ctx)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: k.namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"linkerd.io/inject": "enabled",
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyAlways,
					ServiceAccountName:           "worker",
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
								EmptyDir: &corev1.EmptyDirVolumeSource{
									SizeLimit: func() *resource.Quantity {
										q := resource.MustParse(DefaultWorkerEphemeralStorageLimit)
										return &q
									}(),
								},
							},
						},
						{
							Name: "grpc-tls",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: "grpc-tls",
								},
							},
						},
						{
							Name: "worker-secrets",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: k.workerSecretName,
									Items: []corev1.KeyToPath{
										{Key: "MINIO_ENDPOINT", Path: "endpoint"},
										{Key: "MINIO_ACCESS_KEY", Path: "access-key"},
										{Key: "MINIO_SECRET_KEY", Path: "secret-key"},
									},
									DefaultMode: func() *int32 { mode := int32(0400); return &mode }(),
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "worker",
							Image: k.workerImage,
							Env: []corev1.EnvVar{
								{Name: "JOB_ID", Value: jobID},
								{Name: "MANAGER_ADDR", Value: managerAddr},
								{Name: "GRPC_TLS_CERT_FILE", Value: "/tls/tls.crt"},
								secretEnvVar("S3_ENDPOINT", k.workerSecretName, "MINIO_ENDPOINT"),
								secretEnvVar("S3_ACCESS_KEY", k.workerSecretName, "MINIO_ACCESS_KEY"),
								secretEnvVar("S3_SECRET_KEY", k.workerSecretName, "MINIO_SECRET_KEY"),
								secretEnvVar("MINIO_BUCKET", k.workerSecretName, "MINIO_BUCKET"),
								secretEnvVar("WORKER_RPC_TOKEN", k.workerSecretName, "MANAGER_WORKER_RPC_TOKEN"),
							},
							Resources: resources,
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
								{
									Name:      "grpc-tls",
									MountPath: "/tls",
									ReadOnly:  true,
								},
								{
									Name:      "worker-secrets",
									MountPath: "/etc/worker-secrets",
									ReadOnly:  true,
								},
							},
						},
					},
				},
			},
		},
	}

	localityKey := k.resolveLocalityKey(ctx)
	if localityKey != "" {
		deployment.Spec.Template.Spec.Affinity = &corev1.Affinity{
			PodAffinity: &corev1.PodAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
					{
						Weight: 100,
						PodAffinityTerm: corev1.PodAffinityTerm{
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"app.kubernetes.io/name": "minio",
								},
							},
							TopologyKey: localityKey,
						},
					},
				},
			},
		}
	}

	_, err := k.clientset.AppsV1().Deployments(k.namespace).Create(ctx, deployment, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := k.clientset.AppsV1().Deployments(k.namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		existing.Spec = deployment.Spec
		_, err = k.clientset.AppsV1().Deployments(k.namespace).Update(ctx, existing, metav1.UpdateOptions{})
		return err
	}
	return err
}

// CancelJob deletes the worker pool Deployment for a job.
func (k *KubeOrchestrator) CancelJob(ctx context.Context, jobID string) error {
	sanitizedJobID := sanitizeForDNSLabel(jobID)
	deploymentName := fmt.Sprintf("worker-pool-%s", sanitizedJobID)
	policy := metav1.DeletePropagationBackground
	err := k.clientset.AppsV1().Deployments(k.namespace).Delete(ctx, deploymentName, metav1.DeleteOptions{
		PropagationPolicy: &policy,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (k *KubeOrchestrator) SpawnWorker(ctx context.Context, taskID string, jobID string, attemptID string, managerAddr string) error {
	return nil // Deprecated: use EnsureWorkerPool
}

func (k *KubeOrchestrator) DeleteWorkerJob(ctx context.Context, taskID string) error {
	return nil // Deprecated: pool-based workers are managed via Deployment replicas
}

// MockOrchestrator is a no-op implementation for unit testing.
type MockOrchestrator struct{}

func (m *MockOrchestrator) EnsureWorkerPool(ctx context.Context, jobID string, numWorkers int, managerAddr string) error {
	return nil
}

func (m *MockOrchestrator) CancelJob(ctx context.Context, jobID string) error {
	return nil
}

func (m *MockOrchestrator) SpawnWorker(ctx context.Context, taskID string, jobID string, attemptID string, managerAddr string) error {
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
