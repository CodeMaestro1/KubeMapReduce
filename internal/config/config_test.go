package config

import (
	"os"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear relevant env vars to test defaults.
	envVars := []string{
		"KEYCLOAK_BASE_URL", "KEYCLOAK_REALM", "KEYCLOAK_AUDIENCE",
		"KEYCLOAK_JWKS_URL", "KEYCLOAK_ISSUER", "SERVER_ADDR",
	}
	for _, key := range envVars {
		t.Setenv(key, "")
	}

	cfg := Load()

	if cfg.KeycloakBaseURL != "http://localhost:8080" {
		t.Errorf("KeycloakBaseURL = %q, want %q", cfg.KeycloakBaseURL, "http://localhost:8080")
	}
	if cfg.Realm != "mapreduce" {
		t.Errorf("Realm = %q, want %q", cfg.Realm, "mapreduce")
	}
	if cfg.Audience != "mapreduce-api" {
		t.Errorf("Audience = %q, want %q", cfg.Audience, "mapreduce-api")
	}
	if cfg.ServerAddr != ":8081" {
		t.Errorf("ServerAddr = %q, want %q", cfg.ServerAddr, ":8081")
	}

	expectedJWKS := "http://localhost:8080/realms/mapreduce/protocol/openid-connect/certs"
	if cfg.JWKSURL != expectedJWKS {
		t.Errorf("JWKSURL = %q, want %q", cfg.JWKSURL, expectedJWKS)
	}

	expectedIssuer := "http://localhost:8080/realms/mapreduce"
	if cfg.Issuer != expectedIssuer {
		t.Errorf("Issuer = %q, want %q", cfg.Issuer, expectedIssuer)
	}
}

func TestLoad_CustomEnvVars(t *testing.T) {
	t.Setenv("KEYCLOAK_BASE_URL", "http://kc:9090")
	t.Setenv("KEYCLOAK_REALM", "custom-realm")
	t.Setenv("KEYCLOAK_AUDIENCE", "custom-api")
	t.Setenv("KEYCLOAK_JWKS_URL", "http://kc:9090/jwks")
	t.Setenv("KEYCLOAK_ISSUER", "http://kc:9090/issuer")
	t.Setenv("SERVER_ADDR", ":9999")

	cfg := Load()

	if cfg.KeycloakBaseURL != "http://kc:9090" {
		t.Errorf("KeycloakBaseURL = %q, want %q", cfg.KeycloakBaseURL, "http://kc:9090")
	}
	if cfg.Realm != "custom-realm" {
		t.Errorf("Realm = %q, want %q", cfg.Realm, "custom-realm")
	}
	if cfg.Audience != "custom-api" {
		t.Errorf("Audience = %q, want %q", cfg.Audience, "custom-api")
	}
	if cfg.JWKSURL != "http://kc:9090/jwks" {
		t.Errorf("JWKSURL = %q, want %q", cfg.JWKSURL, "http://kc:9090/jwks")
	}
	if cfg.Issuer != "http://kc:9090/issuer" {
		t.Errorf("Issuer = %q, want %q", cfg.Issuer, "http://kc:9090/issuer")
	}
	if cfg.ServerAddr != ":9999" {
		t.Errorf("ServerAddr = %q, want %q", cfg.ServerAddr, ":9999")
	}
}

func TestLoad_WhitespaceEnvTreatedAsEmpty(t *testing.T) {
	t.Setenv("KEYCLOAK_BASE_URL", "   ")
	t.Setenv("KEYCLOAK_REALM", "  ")

	cfg := Load()

	// Whitespace-only values should fall back to defaults.
	if cfg.KeycloakBaseURL != "http://localhost:8080" {
		t.Errorf("KeycloakBaseURL = %q, want default", cfg.KeycloakBaseURL)
	}
	if cfg.Realm != "mapreduce" {
		t.Errorf("Realm = %q, want default", cfg.Realm)
	}
}

func TestLoad_JWKSAndIssuerDeriveFromBaseAndRealm(t *testing.T) {
	t.Setenv("KEYCLOAK_BASE_URL", "http://myhost:1234")
	t.Setenv("KEYCLOAK_REALM", "myrealm")
	// Don't set JWKS_URL or ISSUER — they should be derived.
	os.Unsetenv("KEYCLOAK_JWKS_URL")
	os.Unsetenv("KEYCLOAK_ISSUER")

	cfg := Load()

	expectedJWKS := "http://myhost:1234/realms/myrealm/protocol/openid-connect/certs"
	if cfg.JWKSURL != expectedJWKS {
		t.Errorf("JWKSURL = %q, want %q", cfg.JWKSURL, expectedJWKS)
	}

	expectedIssuer := "http://myhost:1234/realms/myrealm"
	if cfg.Issuer != expectedIssuer {
		t.Errorf("Issuer = %q, want %q", cfg.Issuer, expectedIssuer)
	}
}
