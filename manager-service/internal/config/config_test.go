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
		"KEYCLOAK_ADMIN_USERNAME", "KEYCLOAK_ADMIN_PASSWORD",
		"GRPC_ADDR", "GRPC_PORT",
	}
	for _, key := range envVars {
		t.Setenv(key, "")
	}

	t.Setenv("KEYCLOAK_ADMIN_USERNAME", "admin")
	t.Setenv("KEYCLOAK_ADMIN_PASSWORD", "admin")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

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
	if cfg.GRPCAddr != ":50051" {
		t.Errorf("GRPCAddr = %q, want %q", cfg.GRPCAddr, ":50051")
	}
	if cfg.AdminUsername != "admin" {
		t.Errorf("AdminUsername = %q, want %q", cfg.AdminUsername, "admin")
	}
	if cfg.AdminPassword != "admin" {
		t.Errorf("AdminPassword = %q, want %q", cfg.AdminPassword, "admin")
	}
	if cfg.MinioEndpoint != "" || cfg.MinioAccessKey != "" || cfg.MinioSecretKey != "" {
		t.Errorf("expected MinIO defaults to be disabled/empty, got endpoint=%q access=%q secret=%q", cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey)
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
	t.Setenv("GRPC_ADDR", "0.0.0.0:50052")
	t.Setenv("KEYCLOAK_ADMIN_USERNAME", "u")
	t.Setenv("KEYCLOAK_ADMIN_PASSWORD", "p")
	t.Setenv("MINIO_ENDPOINT", "minio.default.svc.cluster.local:9000")
	t.Setenv("MINIO_ACCESS_KEY", "access")
	t.Setenv("MINIO_SECRET_KEY", "secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

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
	if cfg.GRPCAddr != "0.0.0.0:50052" {
		t.Errorf("GRPCAddr = %q, want %q", cfg.GRPCAddr, "0.0.0.0:50052")
	}
	if cfg.AdminUsername != "u" {
		t.Errorf("AdminUsername = %q, want %q", cfg.AdminUsername, "u")
	}
	if cfg.AdminPassword != "p" {
		t.Errorf("AdminPassword = %q, want %q", cfg.AdminPassword, "p")
	}
	if cfg.MinioEndpoint != "minio.default.svc.cluster.local:9000" {
		t.Errorf("MinioEndpoint = %q, want %q", cfg.MinioEndpoint, "minio.default.svc.cluster.local:9000")
	}
	if cfg.MinioAccessKey != "access" {
		t.Errorf("MinioAccessKey = %q, want %q", cfg.MinioAccessKey, "access")
	}
	if cfg.MinioSecretKey != "secret" {
		t.Errorf("MinioSecretKey = %q, want %q", cfg.MinioSecretKey, "secret")
	}
}

func TestLoad_WhitespaceEnvTreatedAsEmpty(t *testing.T) {
	t.Setenv("KEYCLOAK_BASE_URL", "   ")
	t.Setenv("KEYCLOAK_REALM", "  ")

	t.Setenv("KEYCLOAK_ADMIN_USERNAME", "admin")
	t.Setenv("KEYCLOAK_ADMIN_PASSWORD", "admin")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

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

	t.Setenv("KEYCLOAK_ADMIN_USERNAME", "admin")
	t.Setenv("KEYCLOAK_ADMIN_PASSWORD", "admin")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	expectedJWKS := "http://myhost:1234/realms/myrealm/protocol/openid-connect/certs"
	if cfg.JWKSURL != expectedJWKS {
		t.Errorf("JWKSURL = %q, want %q", cfg.JWKSURL, expectedJWKS)
	}

	expectedIssuer := "http://myhost:1234/realms/myrealm"
	if cfg.Issuer != expectedIssuer {
		t.Errorf("Issuer = %q, want %q", cfg.Issuer, expectedIssuer)
	}
}

func TestLoad_MissingAdminUsernameFails(t *testing.T) {
	t.Setenv("KEYCLOAK_ADMIN_USERNAME", "")
	t.Setenv("KEYCLOAK_ADMIN_PASSWORD", "secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.AdminUsername != "" {
		t.Fatalf("expected empty AdminUsername when KEYCLOAK_ADMIN_USERNAME is missing, got %q", cfg.AdminUsername)
	}
	if cfg.AdminPassword != "secret" {
		t.Fatalf("expected AdminPassword to remain set, got %q", cfg.AdminPassword)
	}
}

func TestLoad_MissingAdminPasswordFails(t *testing.T) {
	t.Setenv("KEYCLOAK_ADMIN_USERNAME", "admin")
	t.Setenv("KEYCLOAK_ADMIN_PASSWORD", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.AdminUsername != "admin" {
		t.Fatalf("expected AdminUsername to remain set, got %q", cfg.AdminUsername)
	}
	if cfg.AdminPassword != "" {
		t.Fatalf("expected empty AdminPassword when KEYCLOAK_ADMIN_PASSWORD is missing, got %q", cfg.AdminPassword)
	}
}

func TestLoad_MissingBothAdminCredentialsDoesNotFail(t *testing.T) {
	t.Setenv("KEYCLOAK_ADMIN_USERNAME", "")
	t.Setenv("KEYCLOAK_ADMIN_PASSWORD", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.AdminUsername != "" || cfg.AdminPassword != "" {
		t.Fatalf("expected empty admin credentials, got username=%q password=%q", cfg.AdminUsername, cfg.AdminPassword)
	}
}

func TestLoad_InvalidStatefulSetReplicasReturnsError(t *testing.T) {
	t.Setenv("STATEFULSET_REPLICAS", "not-a-number")

	_, err := Load()
	if err == nil {
		t.Fatal("expected Load to fail for non-numeric STATEFULSET_REPLICAS")
	}
}

func TestLoad_ParsesStatefulSetReplicas(t *testing.T) {
	t.Setenv("STATEFULSET_REPLICAS", "3")
	t.Setenv("KEYCLOAK_ADMIN_USERNAME", "admin")
	t.Setenv("KEYCLOAK_ADMIN_PASSWORD", "admin")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected Load to succeed, got %v", err)
	}
	if cfg.TotalReplicas != 3 {
		t.Fatalf("expected TotalReplicas=3, got %d", cfg.TotalReplicas)
	}
}
