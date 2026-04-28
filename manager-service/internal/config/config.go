package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
	DatabaseDSN string

	// Distributed scheduling parameters.
	// TotalReplicas matches the K8s StatefulSet replica count for hashing logic.
	TotalReplicas int
	// HeartbeatInterval defines how often workers must check in.
	HeartbeatInterval int
	// MaxMissedHeartbeats is the tolerance before a worker is marked as failed.
	MaxMissedHeartbeats int
	// LeaseTTL is calculated as HeartbeatInterval * MaxMissedHeartbeats.
	LeaseTTL int

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
	GRPCTLSCertFile        string
	GRPCTLSKeyFile         string
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
	}
	cfg.LeaseTTL = cfg.HeartbeatInterval * cfg.MaxMissedHeartbeats
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
