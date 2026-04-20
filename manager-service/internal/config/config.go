package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KeycloakBaseURL     string
	Realm               string
	JWKSURL             string
	Issuer              string
	Audience            string
	ServerAddr          string
	AdminUsername       string
	AdminPassword       string
	DatabaseDSN         string
	GRPCAddr            string
	TotalReplicas       int
	HeartbeatInterval   int
	MaxMissedHeartbeats int
	LeaseTTL            int
	MinioEndpoint       string
	// MinioAccessKey and MinioSecretKey are expected to be injected via environment
	// variables (typically from Kubernetes Secrets) when manifest fallback is enabled.
	MinioAccessKey           string
	MinioSecretKey           string
	MinioUseSSL              bool
	InternalAPIKey           string
	WorkerRPCToken           string
	GRPCTLSCertFile          string
	GRPCTLSKeyFile           string
	EnableGRPCReflection     bool
	AllowInsecureWorkerRPC   bool
	AllowInsecureInternalAPI bool
}

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

	cfg := &Config{
		KeycloakBaseURL:          keycloakBaseURL,
		Realm:                    realm,
		JWKSURL:                  getEnv("KEYCLOAK_JWKS_URL", keycloakBaseURL+"/realms/"+realm+"/protocol/openid-connect/certs"),
		Issuer:                   getEnv("KEYCLOAK_ISSUER", keycloakBaseURL+"/realms/"+realm),
		Audience:                 audience,
		ServerAddr:               getEnv("SERVER_ADDR", ":8081"),
		AdminUsername:            adminUsername,
		AdminPassword:            adminPassword,
		DatabaseDSN:              getEnv("DATABASE_DSN", "postgres://user:pass@localhost:5432/mapreduce?sslmode=disable"),
		GRPCAddr:                 getEnv("GRPC_ADDR", getEnv("GRPC_PORT", ":50051")),
		TotalReplicas:            totalReplicas,
		HeartbeatInterval:        hbInterval,
		MaxMissedHeartbeats:      maxMissed,
		MinioEndpoint:            getEnv("MINIO_ENDPOINT", ""),
		MinioAccessKey:           getEnv("MINIO_ACCESS_KEY", ""),
		MinioSecretKey:           getEnv("MINIO_SECRET_KEY", ""),
		MinioUseSSL:              getEnvBool("MINIO_USE_SSL", false),
		InternalAPIKey:           getEnv("MANAGER_INTERNAL_API_KEY", ""),
		WorkerRPCToken:           getEnv("MANAGER_WORKER_RPC_TOKEN", ""),
		GRPCTLSCertFile:          getEnv("GRPC_TLS_CERT_FILE", ""),
		GRPCTLSKeyFile:           getEnv("GRPC_TLS_KEY_FILE", ""),
		EnableGRPCReflection:     getEnvBool("ENABLE_GRPC_REFLECTION", false),
		AllowInsecureWorkerRPC:   getEnvBool("ALLOW_INSECURE_WORKER_RPC", false),
		AllowInsecureInternalAPI: getEnvBool("ALLOW_INSECURE_INTERNAL_API", false),
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
