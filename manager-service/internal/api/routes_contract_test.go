package api

import (
	"encoding/json"
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

// TestDeleteJobContract_Returns204 verifies that DELETE /api/v1/jobs/{job_id}
// returns 204 No Content with an empty body per the API specification (Table 10.1).
func TestDeleteJobContract_Returns204(t *testing.T) {
	// Fake manager that accepts the cancellation proxy request.
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mgr.Close()

	store := NewMemoryJobStore(24*time.Hour, 10000, nil)
	h := NewHandlers(nil, store, nil, mgr.Listener.Addr().String(), "")

	// Submit a job directly so a valid job_id is available.
	submitBody := `{"filename":"f.json","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	submitReq := newAuthedRequest(http.MethodPost, "/api/v1/jobs", submitBody, testSubject)
	submitRec := httptest.NewRecorder()
	h.HandleJobsSubmit(submitRec, submitReq)
	if submitRec.Code != http.StatusAccepted {
		t.Fatalf("setup: job submit returned %d: %s", submitRec.Code, submitRec.Body.String())
	}

	var resp struct {
		JobID string `json:"jobId"`
	}
	if err := json.Unmarshal(submitRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode submit response: %v", err)
	}

	delReq := newAuthedRequest(http.MethodDelete, "/api/v1/jobs/"+resp.JobID, "", testSubject)
	delReq.SetPathValue("job_id", resp.JobID)
	delRec := httptest.NewRecorder()
	h.HandleJobsDelete(delRec, delReq)

	if delRec.Code != http.StatusNoContent {
		t.Fatalf("contract violation: DELETE /api/v1/jobs/{job_id} must return 204, got %d: %s",
			delRec.Code, delRec.Body.String())
	}
	if delRec.Body.Len() != 0 {
		t.Fatalf("contract violation: DELETE /api/v1/jobs/{job_id} must return empty body, got %q",
			delRec.Body.String())
	}
}
