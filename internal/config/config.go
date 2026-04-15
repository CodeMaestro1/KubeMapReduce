package config

import (
	"os"
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
}

func Load() (*Config, error) {
	keycloakBaseURL := getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080")
	realm := getEnv("KEYCLOAK_REALM", "mapreduce")
	audience := getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api")
	adminUsername := strings.TrimSpace(os.Getenv("KEYCLOAK_ADMIN_USERNAME"))
	adminPassword := strings.TrimSpace(os.Getenv("KEYCLOAK_ADMIN_PASSWORD"))

	return &Config{
		KeycloakBaseURL: keycloakBaseURL,
		Realm:           realm,
		JWKSURL:         getEnv("KEYCLOAK_JWKS_URL", keycloakBaseURL+"/realms/"+realm+"/protocol/openid-connect/certs"),
		Issuer:          getEnv("KEYCLOAK_ISSUER", keycloakBaseURL+"/realms/"+realm),
		Audience:        audience,
		ServerAddr:      getEnv("SERVER_ADDR", ":8081"),
		AdminUsername:   adminUsername,
		AdminPassword:   adminPassword,
	}, nil
}

func getEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
