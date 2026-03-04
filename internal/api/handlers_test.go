package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kubemapreduce/pkg/auth"
)

type fakeAdminClient struct {
	createdRequests []auth.CreateUserRequest
	deletedUsers    []string
}

func (f *fakeAdminClient) CreateUser(req auth.CreateUserRequest) error {
	f.createdRequests = append(f.createdRequests, req)
	return nil
}

func (f *fakeAdminClient) DeleteUserByUsername(username string) error {
	f.deletedUsers = append(f.deletedUsers, username)
	return nil
}

func TestHandleCreateUser_NormalizesRoleForAdminClientAndResponse(t *testing.T) {
	fakeClient := &fakeAdminClient{}
	h := NewHandlers(fakeClient, "", UIConfig{})

	body := `{"username":"alice","email":"alice@example.com","password":"secret","role":"admin"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleCreateUser(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}

	if len(fakeClient.createdRequests) != 1 {
		t.Fatalf("expected 1 admin create call, got %d", len(fakeClient.createdRequests))
	}

	if got := fakeClient.createdRequests[0].Role; got != "ADMIN" {
		t.Fatalf("expected normalized role ADMIN forwarded to admin client, got %q", got)
	}

	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response json: %v", err)
	}

	if got := payload["role"]; got != "ADMIN" {
		t.Fatalf("expected normalized role ADMIN in response, got %q", got)
	}
}

func TestParseUsernameFromDeletePath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    string
		wantErr bool
	}{
		{name: "simple username", path: "/admin/users/alice", want: "alice"},
		{name: "url-encoded username", path: "/admin/users/alice%40example.com", want: "alice@example.com"},
		{name: "missing username", path: "/admin/users/", wantErr: true},
		{name: "extra segments rejected", path: "/admin/users/alice/extra", wantErr: true},
		{name: "invalid encoding rejected", path: "/admin/users/alice%2", wantErr: true},
		{name: "decoded slash rejected", path: "/admin/users/alice%2Fextra", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseUsernameFromDeletePath(tc.path)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got none and username %q", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tc.want {
				t.Fatalf("expected username %q, got %q", tc.want, got)
			}
		})
	}
}

func TestHandleDeleteUser_RejectsExtraSegments(t *testing.T) {
	fakeClient := &fakeAdminClient{}
	h := NewHandlers(fakeClient, "", UIConfig{})

	req := httptest.NewRequest(http.MethodDelete, "/admin/users/alice/extra", nil)
	rec := httptest.NewRecorder()

	h.HandleDeleteUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	if len(fakeClient.deletedUsers) != 0 {
		t.Fatalf("expected no delete call on bad path, got %d", len(fakeClient.deletedUsers))
	}
}
