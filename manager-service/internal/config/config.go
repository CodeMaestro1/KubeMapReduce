package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all environment-sourced configuration for the Manager Service.
//
// This central struct ensures that all external dependencies (Keycloak, Postgres,
// MinIO) and internal tunables (Lease TTL, Heartbeat intervals) are validated
// and typed before the service starts.
type Config struct {
	// Keycloak configuration for user authentication and admin operations.
	KeycloakBaseURL string
	Realm           string
	JWKSURL         string
	Issuer          string
	Audience        string
	AdminUsername   string
	AdminPassword   string

	// Server addresses for REST and gRPC interfaces.
	ServerAddr string
	GRPCAddr   string

	// Persistence layer connection string.
	DatabaseDSN       string
	DBMaxOpenConns    int
	DBMaxIdleConns    int
	DBConnMaxLifetime time.Duration

	// Distributed scheduling parameters.
	// TotalReplicas matches the K8s StatefulSet replica count for hashing logic.
	TotalReplicas int
	// HeartbeatInterval defines how often workers must check in.
	HeartbeatInterval int
	// MaxMissedHeartbeats is the tolerance before a worker is marked as failed.
	MaxMissedHeartbeats int
	// LeaseTTL is calculated as HeartbeatInterval * MaxMissedHeartbeats.
	LeaseTTL int
	// LeaseClockSkewSeconds is the tolerance (in seconds) added to the lease
	// TTL check to account for clock skew between the Manager and worker nodes.
	// Configurable via LEASE_CLOCK_SKEW_SECONDS; defaults to 5.
	LeaseClockSkewSeconds int

	// Object storage configuration for input and intermediate data.
	MinioEndpoint string
	// MinioAccessKey and MinioSecretKey are expected to be injected via environment
	// variables (typically from Kubernetes Secrets).
	MinioAccessKey string
	MinioSecretKey string
	MinioUseSSL    bool

	// Security tokens for internal and worker communication.
	InternalAPIKey                  string
	AllowInsecureInternalCancelAuth bool
	WorkerRPCToken                  string

	// gRPC security and reflection settings.
	GRPCTLSCertFile string
	GRPCTLSKeyFile  string
	// EnableGRPCReflection enables service reflection on the gRPC server.
	// SECURITY: Reflection exposes service definitions and is disabled by default.
	// Only enable in development environments with explicit opt-in via ENABLE_GRPC_REFLECTION=true.
	// Additionally requires DEBUG_MODE=true to prevent accidental production exposure.
	EnableGRPCReflection   bool
	AllowInsecureWorkerRPC bool

	// Manager internal endpoint
	ManagerAddr string

	// ManifestThresholdBytes is the maximum serialized TaskAssignment size (in
	// bytes) before the Manager falls back to manifest mode and uploads the
	// data_locations list to MinIO. Configurable via MANAGER_MANIFEST_THRESHOLD_BYTES.
	ManifestThresholdBytes int

	// WorkerSecretName is the name of the Kubernetes Secret to be used for
	// injecting sensitive credentials into worker pods.
	WorkerSecretName string

	// Feature flags for event-driven coordinator decoupling (Phase 0+)
	// EnableOutboxRelay enables the transactional outbox pattern and event publishing.
	// When disabled, the system behaves exactly as before (no behavior change).
	EnableOutboxRelay bool
	// OutboxRelayInterval is how often the relay service polls for undelivered events.
	OutboxRelayInterval time.Duration
	// OutboxMaxRetries is the maximum number of delivery attempts before moving to DLQ.
	OutboxMaxRetries int
	// OutboxBatchSize bounds how many outbox rows the relay claims per cycle.
	OutboxBatchSize int
	// OutboxQueueDepthInterval is how often the queue-depth gauge is refreshed.
	OutboxQueueDepthInterval time.Duration
	// HeartbeatEventSampleN emits the tasks.heartbeat.received event only on
	// every N-th heartbeat to control outbox volume. 0 or 1 emits every heartbeat.
	HeartbeatEventSampleN int
	// NATSRequireTLS, when true, refuses to start with a plain nats:// URL.
	NATSRequireTLS bool

	// NATS configuration for event publishing
	NATSURL       string
	NATSCredsFile string
}

// DefaultManifestThresholdBytes is the default size threshold (2 MiB) at which
// oversized TaskAssignment payloads are written as manifests in MinIO.
const DefaultManifestThresholdBytes = 2 * 1024 * 1024

// Load populates the [Config] struct from environment variables.
//
// It applies sensible defaults for local development but expects specific
// overrides in production (e.g. via Helm chart env sections). Returns an error
// if critical numeric values are malformed.
func Load() (*Config, error) {
	keycloakBaseURL := getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080")
	realm := getEnv("KEYCLOAK_REALM", "mapreduce")
	audience := getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api")
	adminUsername := strings.TrimSpace(os.Getenv("KEYCLOAK_ADMIN_USERNAME"))
	adminPassword := strings.TrimSpace(os.Getenv("KEYCLOAK_ADMIN_PASSWORD"))

	totalReplicas, err := getEnvInt("STATEFULSET_REPLICAS", 1)
	if err != nil {
		return nil, err
	}

	hbInterval, err := getEnvInt("HEARTBEAT_INTERVAL_SEC", 10)
	if err != nil {
		return nil, err
	}

	maxMissed, err := getEnvInt("MAX_MISSED_HEARTBEATS", 3)
	if err != nil {
		return nil, err
	}

	manifestThreshold, err := getEnvInt("MANAGER_MANIFEST_THRESHOLD_BYTES", DefaultManifestThresholdBytes)
	if err != nil {
		return nil, err
	}
	if manifestThreshold <= 0 {
		return nil, fmt.Errorf("MANAGER_MANIFEST_THRESHOLD_BYTES must be positive, got %d", manifestThreshold)
	}

	outboxRelayIntervalSec, err := getEnvInt("OUTBOX_RELAY_INTERVAL_SEC", 5)
	if err != nil {
		return nil, err
	}
	outboxMaxRetries, err := getEnvInt("OUTBOX_MAX_RETRIES", 3)
	if err != nil {
		return nil, err
	}
	outboxBatchSize, err := getEnvInt("OUTBOX_BATCH_SIZE", 100)
	if err != nil {
		return nil, err
	}
	outboxDepthIntervalSec, err := getEnvInt("OUTBOX_QUEUE_DEPTH_INTERVAL_SEC", 15)
	if err != nil {
		return nil, err
	}
	dbMaxOpenConns, err := getEnvInt("DB_MAX_OPEN_CONNS", 25)
	if err != nil {
		return nil, err
	}
	dbMaxIdleConns, err := getEnvInt("DB_MAX_IDLE_CONNS", 10)
	if err != nil {
		return nil, err
	}
	dbConnMaxLifetimeSec, err := getEnvInt("DB_CONN_MAX_LIFETIME_SEC", 300)
	if err != nil {
		return nil, err
	}
	heartbeatEventSampleN, err := getEnvInt("HEARTBEAT_EVENT_SAMPLE_N", 1)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		KeycloakBaseURL:                 keycloakBaseURL,
		Realm:                           realm,
		JWKSURL:                         getEnv("KEYCLOAK_JWKS_URL", keycloakBaseURL+"/realms/"+realm+"/protocol/openid-connect/certs"),
		Issuer:                          getEnv("KEYCLOAK_ISSUER", keycloakBaseURL+"/realms/"+realm),
		Audience:                        audience,
		ServerAddr:                      getEnv("SERVER_ADDR", ":8081"),
		AdminUsername:                   adminUsername,
		AdminPassword:                   adminPassword,
		DatabaseDSN:                     getEnv("DATABASE_DSN", "postgres://user:pass@localhost:5432/mapreduce?sslmode=disable"),
		DBMaxOpenConns:                  dbMaxOpenConns,
		DBMaxIdleConns:                  dbMaxIdleConns,
		DBConnMaxLifetime:               time.Duration(dbConnMaxLifetimeSec) * time.Second,
		GRPCAddr:                        getEnv("GRPC_ADDR", getEnv("GRPC_PORT", ":50051")),
		TotalReplicas:                   totalReplicas,
		HeartbeatInterval:               hbInterval,
		MaxMissedHeartbeats:             maxMissed,
		MinioEndpoint:                   getEnv("MINIO_ENDPOINT", ""),
		MinioAccessKey:                  getEnv("MINIO_ACCESS_KEY", ""),
		MinioSecretKey:                  getEnv("MINIO_SECRET_KEY", ""),
		MinioUseSSL:                     getEnvBool("MINIO_USE_SSL", false),
		InternalAPIKey:                  getEnv("MANAGER_INTERNAL_API_KEY", ""),
		AllowInsecureInternalCancelAuth: getEnvBool("ALLOW_INSECURE_INTERNAL_CANCEL_AUTH", false),
		WorkerRPCToken:                  getEnv("MANAGER_WORKER_RPC_TOKEN", ""),
		GRPCTLSCertFile:                 getEnv("GRPC_TLS_CERT_FILE", ""),
		GRPCTLSKeyFile:                  getEnv("GRPC_TLS_KEY_FILE", ""),
		EnableGRPCReflection:            getEnvBool("ENABLE_GRPC_REFLECTION", false),
		AllowInsecureWorkerRPC:          getEnvBool("ALLOW_INSECURE_WORKER_RPC", false),
		ManagerAddr:                     getEnv("MANAGER_ADDR", "manager-0.manager-hs.default.svc.cluster.local:8081"),
		ManifestThresholdBytes:          manifestThreshold,
		WorkerSecretName:                getEnv("WORKER_SECRET_NAME", "kubemapreduce-secrets"),
		EnableOutboxRelay:               getEnvBool("ENABLE_OUTBOX_RELAY", false),
		OutboxRelayInterval:             time.Duration(outboxRelayIntervalSec) * time.Second,
		OutboxMaxRetries:                outboxMaxRetries,
		OutboxBatchSize:                 outboxBatchSize,
		OutboxQueueDepthInterval:        time.Duration(outboxDepthIntervalSec) * time.Second,
		HeartbeatEventSampleN:           heartbeatEventSampleN,
		NATSRequireTLS:                  getEnvBool("NATS_REQUIRE_TLS", false),
		NATSURL:                         getEnv("NATS_URL", ""),
		NATSCredsFile:                   getEnv("NATS_CREDS_FILE", ""),
	}
	cfg.LeaseTTL = cfg.HeartbeatInterval * cfg.MaxMissedHeartbeats
	clockSkew, err := getEnvInt("LEASE_CLOCK_SKEW_SECONDS", 5)
	if err != nil {
		return nil, err
	}
	cfg.LeaseClockSkewSeconds = clockSkew
	return cfg, nil
}

func getEnvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	v, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return v
}

func getEnvInt(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	if v, err := strconv.Atoi(value); err == nil {
		return v, nil
	}
	return 0, fmt.Errorf("invalid integer for %s: %q", key, value)
}

func getEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
