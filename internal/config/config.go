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
	AdminUsername   string
	AdminPassword   string
	ServerAddr      string
}

func Load() *Config {
	keycloakBaseURL := getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080")
	realm := getEnv("KEYCLOAK_REALM", "mapreduce")
	audience := getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api")

	return &Config{
		KeycloakBaseURL: keycloakBaseURL,
		Realm:           realm,
		JWKSURL:         getEnv("KEYCLOAK_JWKS_URL", keycloakBaseURL+"/realms/"+realm+"/protocol/openid-connect/certs"),
		Issuer:          getEnv("KEYCLOAK_ISSUER", keycloakBaseURL+"/realms/"+realm),
		Audience:        audience,
		AdminUsername:   os.Getenv("KEYCLOAK_ADMIN_USERNAME"),
		AdminPassword:   os.Getenv("KEYCLOAK_ADMIN_PASSWORD"),
		ServerAddr:      getEnv("SERVER_ADDR", ":8081"),
	}
}

func getEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
