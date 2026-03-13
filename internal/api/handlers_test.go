package api

import (
	"kubemapreduce/pkg/auth"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestHandlers() *Handlers {
	return NewHandlers(nil)
}

func TestHandleHealth(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	h.HandleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("expected body to contain status ok, got %q", rec.Body.String())
	}
}

func TestHandleHealth_RejectsNonGet(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rec := httptest.NewRecorder()

	h.HandleHealth(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHandleJobsSubmit_RejectsInvalidPayload(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsSubmit_RejectsEmptyFilename(t *testing.T) {
	h := newTestHandlers()

	body := `{"filename":"","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsSubmit_AcceptsValidJob(t *testing.T) {
	h := newTestHandlers()

	body := `{"filename":"data.csv","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	if !strings.Contains(rec.Body.String(), `"status":"accepted"`) {
		t.Fatalf("expected accepted status in body, got %q", rec.Body.String())
	}
}

func TestHandleWorkerConfig_RejectsInvalidValues(t *testing.T) {
	h := newTestHandlers()

	body := `{"workerReplicas":0,"maxJobsPerNode":5}`
	req := httptest.NewRequest(http.MethodPut, "/admin/workers/config", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleWorkerConfig(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleWorkerConfig_AcceptsValidConfig(t *testing.T) {
	h := newTestHandlers()

	body := `{"workerReplicas":4,"maxJobsPerNode":8}`
	req := httptest.NewRequest(http.MethodPut, "/admin/workers/config", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleWorkerConfig(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("expected status %d, got %d", http.StatusNotImplemented, rec.Code)
	}
}

// ── Mock Keycloak helper ────────────────────────────────────

// fakeKeycloak returns an httptest.Server that satisfies the Keycloak admin
// API endpoints used by CreateUser and DeleteUserByUsername.
func fakeKeycloak(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		// Token endpoint
		case r.Method == http.MethodPost && r.URL.Path == "/realms/master/protocol/openid-connect/token":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"access_token":"fake-token"}`))

		// Create user
		case r.Method == http.MethodPost && r.URL.Path == "/admin/realms/test/users":
			w.Header().Set("Location", "/admin/realms/test/users/uid-1")
			w.WriteHeader(http.StatusCreated)

		// Set password
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/reset-password"):
			w.WriteHeader(http.StatusNoContent)

		// Fetch role
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/roles/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"role-1","name":"USER"}`))

		// Assign role
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/role-mappings/realm"):
			w.WriteHeader(http.StatusNoContent)

		// Find user by username (for delete)
		case r.Method == http.MethodGet && r.URL.Path == "/admin/realms/test/users" && r.URL.Query().Get("username") != "":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"uid-1","username":"` + r.URL.Query().Get("username") + `"}]`))

		// Delete user
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/admin/realms/test/users/"):
			w.WriteHeader(http.StatusNoContent)

		default:
			t.Logf("unhandled request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func newTestHandlersWithKeycloak(t *testing.T) (*Handlers, *httptest.Server) {
	t.Helper()
	kc := fakeKeycloak(t)
	adminClient := auth.NewKeycloakAdminClient(kc.URL, "test", "admin", "admin")
	return NewHandlers(adminClient), kc
}

// ── Admin Create User tests ─────────────────────────────────

func TestHandleAdminCreateUser_RejectsNonPost(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	req := httptest.NewRequest(http.MethodGet, "/admin/users", nil)
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHandleAdminCreateUser_RejectsInvalidJSON(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	req := httptest.NewRequest(http.MethodPost, "/admin/users", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleAdminCreateUser_RejectsMissingFields(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	body := `{"username":"alice","password":"","role":"USER"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleAdminCreateUser_RejectsInvalidRole(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	body := `{"username":"alice","password":"secret","role":"SUPERUSER"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "role must be ADMIN or USER") {
		t.Fatalf("expected role error message, got %q", rec.Body.String())
	}
}

func TestHandleAdminCreateUser_Success(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	body := `{"username":"alice","email":"alice@example.com","password":"secret","role":"USER"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"created"`) {
		t.Fatalf("expected created status in body, got %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"username":"alice"`) {
		t.Fatalf("expected username in body, got %q", rec.Body.String())
	}
}

// ── Admin Delete User tests ─────────────────────────────────

func TestHandleAdminDeleteUser_RejectsNonDelete(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	req := httptest.NewRequest(http.MethodGet, "/admin/users", nil)
	rec := httptest.NewRecorder()
	h.HandleAdminDeleteUser(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHandleAdminDeleteUser_RejectsInvalidJSON(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	req := httptest.NewRequest(http.MethodDelete, "/admin/users", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()
	h.HandleAdminDeleteUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleAdminDeleteUser_RejectsEmptyUsername(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	body := `{"username":""}`
	req := httptest.NewRequest(http.MethodDelete, "/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminDeleteUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleAdminDeleteUser_Success(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	body := `{"username":"alice"}`
	req := httptest.NewRequest(http.MethodDelete, "/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminDeleteUser(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"deleted"`) {
		t.Fatalf("expected deleted status in body, got %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"username":"alice"`) {
		t.Fatalf("expected username in body, got %q", rec.Body.String())
	}
}

// ── Admin handler with nil client (service unavailable) ─────

func TestHandleAdminCreateUser_NilClientPanics(t *testing.T) {
	h := newTestHandlers() // nil adminClient

	body := `{"username":"alice","password":"secret","role":"USER"}`
	req := httptest.NewRequest(http.MethodPost, "/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic with nil adminClient, but none occurred")
		}
	}()
	h.HandleAdminCreateUser(rec, req)
}
