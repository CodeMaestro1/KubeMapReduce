package manager

import "context"

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
