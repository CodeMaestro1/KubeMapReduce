package manager

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
