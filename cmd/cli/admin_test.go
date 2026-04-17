package main

import (
	"kubemapreduce/internal/api"
	"kubemapreduce/pkg/auth"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConfigureNodesStatusError_AcceptedIsNil(t *testing.T) {
	if err := configureNodesStatusError(http.StatusAccepted, `{"status":"accepted"}`); err != nil {
		t.Fatalf("expected nil error for accepted response, got %v", err)
	}
}

func TestConfigureNodesStatusError_NotImplementedIncludesHint(t *testing.T) {
	err := configureNodesStatusError(http.StatusNotImplemented, `{"message":"node configuration backend integration is not implemented yet"}`)
	if err == nil {
		t.Fatal("expected non-nil error for HTTP 501")
	}
	msg := err.Error()
	if !strings.Contains(msg, "HTTP 501") {
		t.Fatalf("expected HTTP 501 in error, got %q", msg)
	}
	if !strings.Contains(msg, "not implemented") {
		t.Fatalf("expected not implemented text in error, got %q", msg)
	}
	if !strings.Contains(msg, "pending backend implementation") {
		t.Fatalf("expected roadmap hint in error, got %q", msg)
	}
}

func TestConfigureNodesStatusError_UnexpectedStatusIncludesCodeAndBody(t *testing.T) {
	err := configureNodesStatusError(http.StatusBadRequest, `invalid payload`)
	if err == nil {
		t.Fatal("expected non-nil error for unexpected status")
	}
	msg := err.Error()
	if !strings.Contains(msg, "HTTP 400") {
		t.Fatalf("expected HTTP status in error, got %q", msg)
	}
	if !strings.Contains(msg, "invalid payload") {
		t.Fatalf("expected response body in error, got %q", msg)
	}
}

// TestCLIAdminRoutes_MatchAPIRoutes is a contract test that verifies the HTTP
// paths hardcoded in the CLI admin commands are actually registered in the API
// router. If either side drifts, this test fails with a 404.
func TestCLIAdminRoutes_MatchAPIRoutes(t *testing.T) {
	mux := http.NewServeMux()
	h := api.NewHandlers(nil)
	v := new(auth.JWTValidator)
	api.RegisterRoutes(mux, h, v)

	// Paths below must match the URLs built in admin.go command functions.
	cliAdminPaths := []struct {
		method string
		path   string
		cmd    string
	}{
		{http.MethodPut, "/admin/workers/config", "admin worker-config"},
		{http.MethodPut, "/admin/nodes/config", "admin configure-nodes"},
		{http.MethodPost, "/admin/users", "admin create-user"},
		{http.MethodDelete, "/admin/users/testuser", "admin delete-user"},
	}

	for _, tc := range cliAdminPaths {
		t.Run(tc.cmd, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code == http.StatusNotFound {
				t.Fatalf("route drift: CLI %q targets %s %s but API returns 404",
					tc.cmd, tc.method, tc.path)
			}
		})
	}
}
