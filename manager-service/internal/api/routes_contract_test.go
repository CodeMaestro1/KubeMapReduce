package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kubemapreduce/auth-service/pkg/auth"
)

// Keep API admin routes aligned with CLI expectations to prevent integration drift.
func TestCLIAdminRoutes_MatchAPIRoutes(t *testing.T) {
	mux := http.NewServeMux()
	store := NewMemoryJobStore(24*time.Hour, 10000, nil)
	h := NewHandlers(nil, store, nil, "", "")
	v := new(auth.JWTValidator)
	RegisterRoutes(mux, h, v)

	cliAdminPaths := []struct {
		method string
		path   string
		cmd    string
	}{
		{http.MethodPost, "/api/v1/admin/config/workers", "admin configure-nodes / worker-config"},
		{http.MethodPost, "/api/v1/admin/users", "admin create-user"},
		{http.MethodDelete, "/api/v1/admin/users/testuser", "admin delete-user"},
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
