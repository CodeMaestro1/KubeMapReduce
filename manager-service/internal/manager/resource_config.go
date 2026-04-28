package manager

import (
	"context"
	"log"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// DefaultWorkerCPULimit is the fallback CPU limit applied to worker pods when
// the SYSTEM_CONFIG.cpu_limit value is missing, unparseable, or otherwise
// unavailable. The value matches the schema default in
// migrations/0001_initial_schema.sql so a fresh database and the in-process
// fallback agree on the same conservative ceiling.
const DefaultWorkerCPULimit = "500m"

// DefaultWorkerMemoryLimit is the fallback memory limit applied to worker pods
// under the same conditions as DefaultWorkerCPULimit. It matches the schema
// default in migrations/0001_initial_schema.sql.
const DefaultWorkerMemoryLimit = "512Mi"

// ResourceConfigProvider supplies per-worker resource limits to the orchestrator
// at pod-spawn time.
//
// Implementations are expected to be cheap and side-effect free. The orchestrator
// calls GetWorkerResourceLimits once per SpawnWorker invocation so the latest
// admin-configured values from SYSTEM_CONFIG are honoured without restarting
// the Manager. Returning an error is non-fatal: the orchestrator will fall back
// to DefaultWorkerCPULimit / DefaultWorkerMemoryLimit so worker pods always
// boot with bounded resource usage.
type ResourceConfigProvider interface {
	// GetWorkerResourceLimits returns the CPU and memory limit strings (in
	// Kubernetes resource.Quantity syntax, e.g. "500m", "512Mi") to apply to
	// freshly spawned worker pods.
	GetWorkerResourceLimits(ctx context.Context) (cpuLimit, memoryLimit string, err error)
}

// resolveWorkerResources parses raw CPU and memory limit strings into a
// corev1.ResourceRequirements value suitable for direct assignment onto a
// container spec.
//
// The function is intentionally tolerant: if either string is empty or fails
// resource.ParseQuantity, the corresponding default is substituted and a
// warning is logged. This guarantees that worker pods are never left with
// unbounded resource usage just because an operator configured a malformed
// value in SYSTEM_CONFIG. Both Limits and Requests are populated with the
// same quantity so the K8s scheduler can place the pod predictably while the
// kernel still enforces the ceiling.
func resolveWorkerResources(cpuLimit, memoryLimit string) corev1.ResourceRequirements {
	cpuQty, ok := parseQuantityOrDefault("cpu", cpuLimit, DefaultWorkerCPULimit)
	if !ok {
		cpuQty, _ = parseQuantityOrDefault("cpu", DefaultWorkerCPULimit, DefaultWorkerCPULimit)
	}
	memQty, ok := parseQuantityOrDefault("memory", memoryLimit, DefaultWorkerMemoryLimit)
	if !ok {
		memQty, _ = parseQuantityOrDefault("memory", DefaultWorkerMemoryLimit, DefaultWorkerMemoryLimit)
	}
	return corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    cpuQty,
			corev1.ResourceMemory: memQty,
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    cpuQty,
			corev1.ResourceMemory: memQty,
		},
	}
}

// parseQuantityOrDefault parses raw using resource.ParseQuantity. On any
// failure (empty string or invalid syntax) it logs a warning and returns the
// parsed default instead. The boolean second return is true iff the originally
// requested raw value was used; callers can use it for telemetry but the
// returned Quantity is always usable.
func parseQuantityOrDefault(kind, raw, fallback string) (resource.Quantity, bool) {
	if raw != "" {
		if q, err := resource.ParseQuantity(raw); err == nil {
			return q, true
		} else {
			log.Printf("orchestrator: invalid %s limit %q from SYSTEM_CONFIG, using default %q: %v", kind, raw, fallback, err)
		}
	}
	q, err := resource.ParseQuantity(fallback)
	if err != nil {
		// fallback is a compile-time constant; this branch should be
		// unreachable but we surface it loudly if a future edit breaks it.
		log.Printf("orchestrator: BUG: default %s limit %q is not a valid Quantity: %v", kind, fallback, err)
	}
	return q, false
}
