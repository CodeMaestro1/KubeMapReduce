package auth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCreateUserSuccess(t *testing.T) {
	calledToken := false
	calledCreateUser := false
	calledSetPassword := false
	calledFetchRole := false
	calledAssignRole := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/realms/master/protocol/openid-connect/token":
			calledToken = true
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			if r.Form.Get("client_id") != "admin-cli" {
				t.Fatalf("expected client_id admin-cli, got %q", r.Form.Get("client_id"))
			}
			if r.Form.Get("username") != "admin" || r.Form.Get("password") != "admin" {
				t.Fatalf("unexpected admin credentials in token request")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"access_token":"token-1"}`))

		case r.Method == http.MethodPost && r.URL.Path == "/admin/realms/mapreduce/users":
			calledCreateUser = true
			if r.Header.Get("Authorization") != "Bearer token-1" {
				t.Fatalf("expected bearer token for user create, got %q", r.Header.Get("Authorization"))
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read create user body: %v", err)
			}
			var payload keycloakUser
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("decode create user payload: %v", err)
			}
			if payload.Username != "alice" || payload.Email != "alice@example.com" || !payload.Enabled {
				t.Fatalf("unexpected create user payload: %+v", payload)
			}
			w.Header().Set("Location", "/admin/realms/mapreduce/users/user-123")
			w.WriteHeader(http.StatusCreated)

		case r.Method == http.MethodPut && r.URL.Path == "/admin/realms/mapreduce/users/user-123/reset-password":
			calledSetPassword = true
			if r.Header.Get("Authorization") != "Bearer token-1" {
				t.Fatalf("expected bearer token for set password, got %q", r.Header.Get("Authorization"))
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read password body: %v", err)
			}
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("decode password payload: %v", err)
			}
			if payload["type"] != "password" || payload["value"] != "secret" || payload["temporary"] != false {
				t.Fatalf("unexpected password payload: %+v", payload)
			}
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/admin/realms/mapreduce/roles/"):
			calledFetchRole = true
			if !strings.Contains(r.RequestURI, "/admin/realms/mapreduce/roles/ADMIN") {
				t.Fatalf("expected normalized ADMIN role request URI, got %q", r.RequestURI)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"role-id","name":"ADMIN"}`))

		case r.Method == http.MethodPost && r.URL.Path == "/admin/realms/mapreduce/users/user-123/role-mappings/realm":
			calledAssignRole = true
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read role mapping body: %v", err)
			}
			var payload []roleMapping
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("decode role mapping payload: %v", err)
			}
			if len(payload) != 1 || payload[0].ID != "role-id" || payload[0].Name != "ADMIN" {
				t.Fatalf("unexpected role mapping payload: %+v", payload)
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewKeycloakAdminClient(server.URL, "mapreduce", "admin", "admin")
	client.httpClient = server.Client()

	err := client.CreateUser(context.Background(), CreateUserRequest{
		Username: "alice",
		Email:    "alice@example.com",
		Password: "secret",
		Role:     " admin ",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if !calledToken || !calledCreateUser || !calledSetPassword || !calledFetchRole || !calledAssignRole {
		t.Fatalf("expected all create user flow calls to run (token=%t create=%t password=%t fetchRole=%t assignRole=%t)",
			calledToken, calledCreateUser, calledSetPassword, calledFetchRole, calledAssignRole)
	}
}

func TestAssignRealmRoleNormalizesAndEscapesRoleLookupPath(t *testing.T) {
	var requestedRolePath string
	var requestedRoleRequestURI string
	var requestedRoleRawPath string
	var assignPayload []roleMapping

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/admin/realms/mapreduce/roles/"):
			requestedRolePath = r.URL.Path
			requestedRoleRequestURI = r.RequestURI
			requestedRoleRawPath = r.URL.RawPath
			if r.Header.Get("Authorization") != "Bearer token-1" {
				t.Fatalf("expected authorization header, got %q", r.Header.Get("Authorization"))
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"role-id","name":"ADMIN/SUPER"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/admin/realms/mapreduce/users/user-1/role-mappings/realm":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if err := json.Unmarshal(body, &assignPayload); err != nil {
				t.Fatalf("decode assign payload: %v", err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewKeycloakAdminClient(server.URL, "mapreduce", "admin", "admin")
	client.httpClient = server.Client()

	err := client.assignRealmRole(context.Background(), "token-1", "user-1", "  admin/super  ")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if requestedRolePath != "/admin/realms/mapreduce/roles/ADMIN/SUPER" {
		t.Fatalf("expected decoded URL path, got %q", requestedRolePath)
	}
	if !strings.Contains(requestedRoleRequestURI, "/admin/realms/mapreduce/roles/ADMIN%2FSUPER") {
		t.Fatalf("expected escaped role lookup request URI, got %q (rawPath=%q)", requestedRoleRequestURI, requestedRoleRawPath)
	}

	if len(assignPayload) != 1 {
		t.Fatalf("expected one role mapping, got %d", len(assignPayload))
	}
	if assignPayload[0].ID != "role-id" || assignPayload[0].Name != "ADMIN/SUPER" {
		t.Fatalf("unexpected role payload: %+v", assignPayload[0])
	}
}

func TestAssignRealmRoleRejectsEmptyRoleNameAfterTrim(t *testing.T) {
	client := NewKeycloakAdminClient("http://localhost:8080", "mapreduce", "admin", "admin")

	err := client.assignRealmRole(context.Background(), "token-1", "user-1", "   ")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "role name is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewKeycloakAdminClientSetsHTTPTimeout(t *testing.T) {
	client := NewKeycloakAdminClient("http://localhost:8080", "mapreduce", "admin", "admin")

	if client.httpClient == nil {
		t.Fatal("expected http client to be configured")
	}
	if client.httpClient.Timeout != 10*time.Second {
		t.Fatalf("expected timeout 10s, got %v", client.httpClient.Timeout)
	}
}

func TestGetAdminAccessToken_UsesCacheWhenTokenStillValid(t *testing.T) {
	var tokenCalls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/realms/master/protocol/openid-connect/token" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		atomic.AddInt32(&tokenCalls, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"access_token":"token-1","expires_in":3600}`))
	}))
	defer server.Close()

	client := NewKeycloakAdminClient(server.URL, "mapreduce", "admin", "admin")
	client.httpClient = server.Client()

	first, err := client.getAdminAccessToken(context.Background())
	if err != nil {
		t.Fatalf("first token call failed: %v", err)
	}
	second, err := client.getAdminAccessToken(context.Background())
	if err != nil {
		t.Fatalf("second token call failed: %v", err)
	}

	if first != "token-1" || second != "token-1" {
		t.Fatalf("unexpected token values: first=%q second=%q", first, second)
	}

	if got := atomic.LoadInt32(&tokenCalls); got != 1 {
		t.Fatalf("expected exactly one token endpoint call, got %d", got)
	}
}

func TestGetAdminAccessToken_RefreshesExpiredCachedToken(t *testing.T) {
	var tokenCalls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/realms/master/protocol/openid-connect/token" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}

		call := atomic.AddInt32(&tokenCalls, 1)
		w.WriteHeader(http.StatusOK)
		if call == 1 {
			_, _ = w.Write([]byte(`{"access_token":"token-1","expires_in":3600}`))
			return
		}
		_, _ = w.Write([]byte(`{"access_token":"token-2","expires_in":3600}`))
	}))
	defer server.Close()

	client := NewKeycloakAdminClient(server.URL, "mapreduce", "admin", "admin")
	client.httpClient = server.Client()

	first, err := client.getAdminAccessToken(context.Background())
	if err != nil {
		t.Fatalf("first token call failed: %v", err)
	}

	client.tokenMu.Lock()
	client.cachedAdminTokenTill = time.Now().Add(5 * time.Second)
	client.tokenMu.Unlock()

	second, err := client.getAdminAccessToken(context.Background())
	if err != nil {
		t.Fatalf("second token call failed: %v", err)
	}

	if first != "token-1" {
		t.Fatalf("expected first token token-1, got %q", first)
	}
	if second != "token-2" {
		t.Fatalf("expected refreshed token token-2, got %q", second)
	}

	if got := atomic.LoadInt32(&tokenCalls); got != 2 {
		t.Fatalf("expected two token endpoint calls after forced expiry, got %d", got)
	}
}

func TestGetAdminAccessToken_RejectsNegativeExpiresIn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/realms/master/protocol/openid-connect/token" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"access_token":"token-1","expires_in":-1}`))
	}))
	defer server.Close()

	client := NewKeycloakAdminClient(server.URL, "mapreduce", "admin", "admin")
	client.httpClient = server.Client()

	_, err := client.getAdminAccessToken(context.Background())
	if err == nil {
		t.Fatal("expected error for negative expires_in, got nil")
	}
	if !strings.Contains(err.Error(), "invalid token expires_in value") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetAdminAccessToken_ClampsLongExpiresIn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/realms/master/protocol/openid-connect/token" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"access_token":"token-1","expires_in":999999}`))
	}))
	defer server.Close()

	client := NewKeycloakAdminClient(server.URL, "mapreduce", "admin", "admin")
	client.httpClient = server.Client()

	_, err := client.getAdminAccessToken(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	client.tokenMu.Lock()
	remaining := time.Until(client.cachedAdminTokenTill)
	client.tokenMu.Unlock()

	if remaining > maxAdminTokenTTL+time.Second {
		t.Fatalf("expected ttl to be clamped to <= %v, got %v", maxAdminTokenTTL, remaining)
	}
}
