package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KeycloakBaseURL string
	Realm           string
	JWKSURL         string
	Issuer          string
	Audience        string
	ServerAddr      string
	AdminUsername   string
	AdminPassword   string
	DatabaseDSN     string
	GRPCAddr        string
	TotalReplicas   int
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

	return &Config{
		KeycloakBaseURL: keycloakBaseURL,
		Realm:           realm,
		JWKSURL:         getEnv("KEYCLOAK_JWKS_URL", keycloakBaseURL+"/realms/"+realm+"/protocol/openid-connect/certs"),
		Issuer:          getEnv("KEYCLOAK_ISSUER", keycloakBaseURL+"/realms/"+realm),
		Audience:        audience,
		ServerAddr:      getEnv("SERVER_ADDR", ":8081"),
		AdminUsername:   adminUsername,
		AdminPassword:   adminPassword,
		DatabaseDSN:     getEnv("DATABASE_DSN", "postgres://user:pass@localhost:5432/mapreduce?sslmode=disable"),
		GRPCAddr:        getEnv("GRPC_ADDR", getEnv("GRPC_PORT", ":50051")),
		TotalReplicas:   totalReplicas,
	}, nil
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
