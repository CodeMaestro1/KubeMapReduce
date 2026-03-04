package auth

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAssignRealmRoleNormalizesAndEscapesRoleLookupPath(t *testing.T) {
	var requestedRolePath string
	var assignPayload []roleMapping

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/admin/realms/mapreduce/roles/"):
			requestedRolePath = r.URL.Path
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

	err := client.assignRealmRole("token-1", "user-1", "  admin/super  ")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if requestedRolePath != "/admin/realms/mapreduce/roles/ADMIN%2FSUPER" {
		t.Fatalf("expected escaped role lookup path, got %q", requestedRolePath)
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

	err := client.assignRealmRole("token-1", "user-1", "   ")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "role name is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}
