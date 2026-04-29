package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"kubemapreduce/auth-service/pkg/auth"
)

func TestRunLogin_Success(t *testing.T) {
	// Mock Keycloak server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/realms/mapreduce/protocol/openid-connect/token" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("unexpected method: %s", r.Method)
		}

		resp := auth.OAuthTokenResponse{
			AccessToken:  "mock-access-token",
			RefreshToken: "mock-refresh-token",
			ExpiresIn:    3600,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	// Mock environment
	t.Setenv("KEYCLOAK_BASE_URL", ts.URL)
	t.Setenv("KEYCLOAK_REALM", "mapreduce")
	t.Setenv("KEYCLOAK_AUDIENCE", "mapreduce-api")
	t.Setenv("API_URL", "http://localhost:8081")

	// Mock home directory for token storage
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)

	// Mock password input
	oldReadPasswordFn := readPasswordFn
	defer func() { readPasswordFn = oldReadPasswordFn }()
	readPasswordFn = func(fd int) ([]byte, error) {
		return []byte("test-password"), nil
	}

	err := runLogin([]string{"--username", "testuser"})
	if err != nil {
		t.Fatalf("runLogin failed: %v", err)
	}

	// Verify tokens were saved
	tokens, err := auth.LoadTokens()
	if err != nil {
		t.Fatalf("failed to load tokens: %v", err)
	}
	if tokens.AccessToken != "mock-access-token" {
		t.Errorf("wrong access token: %s", tokens.AccessToken)
	}
}

func TestRunLogin_Failure(t *testing.T) {
	// Mock Keycloak server with error
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "invalid credentials")
	}))
	defer ts.Close()

	t.Setenv("KEYCLOAK_BASE_URL", ts.URL)
	t.Setenv("HOME", t.TempDir())

	oldReadPasswordFn := readPasswordFn
	defer func() { readPasswordFn = oldReadPasswordFn }()
	readPasswordFn = func(fd int) ([]byte, error) {
		return []byte("wrong-password"), nil
	}

	err := runLogin([]string{"--username", "testuser"})
	if err == nil {
		t.Fatal("expected runLogin to fail")
	}
}

func TestRunLogout(t *testing.T) {
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)

	// Pre-create a token file
	configDir := filepath.Join(tmpHome, ".config", "kubemapreduce")
	os.MkdirAll(configDir, 0755)
	os.WriteFile(filepath.Join(configDir, "credentials.json"), []byte("{}"), 0644)

	err := runLogout()
	if err != nil {
		t.Fatalf("runLogout failed: %v", err)
	}

	// Verify token file is gone
	if _, err := os.Stat(filepath.Join(configDir, "credentials.json")); !os.IsNotExist(err) {
		t.Errorf("token file should have been deleted")
	}
}

func TestRunHealth_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		fmt.Fprint(w, "OK")
	}))
	defer ts.Close()

	t.Setenv("API_URL", ts.URL)

	err := runHealth()
	if err != nil {
		t.Fatalf("runHealth failed: %v", err)
	}
}

func TestRunHealth_Failure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Server Error")
	}))
	defer ts.Close()

	t.Setenv("API_URL", ts.URL)

	err := runHealth()
	if err == nil {
		t.Fatal("expected runHealth to fail")
	}
}
