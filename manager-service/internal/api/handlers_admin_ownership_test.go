package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"kubemapreduce/auth-service/pkg/auth"

	"github.com/golang-jwt/jwt/v5"
)

// adminClaims constructs request claims that include the ADMIN realm role,
// matching the shape produced by the Keycloak token issuer.
func adminClaims(sub string) jwt.MapClaims {
	return jwt.MapClaims{
		"sub": sub,
		"realm_access": map[string]interface{}{
			"roles": []interface{}{"USER", "ADMIN"},
		},
	}
}

func userClaims(sub string) jwt.MapClaims {
	return jwt.MapClaims{
		"sub": sub,
		"realm_access": map[string]interface{}{
			"roles": []interface{}{"USER"},
		},
	}
}

func newReqWithClaims(method, target string, claims jwt.MapClaims) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	return req.WithContext(auth.ContextWithClaims(req.Context(), claims))
}

// seedJob inserts a JobRecord directly into the in-memory store, bypassing the
// HTTP layer so each test can control ownership precisely.
func seedJob(t *testing.T, h *Handlers, owner, jobID string, created time.Time) {
	t.Helper()
	rec := JobRecord{
		JobID:     jobID,
		UserID:    owner,
		Status:    "Pending",
		Filename:  "in.txt",
		Reducers:  1,
		CreatedAt: created,
	}
	if err := h.store.CreateJob(context.Background(), rec); err != nil {
		t.Fatalf("seed CreateJob failed: %v", err)
	}
}

func TestHandleJobsList_NonAdminSeesOnlyOwnJobs(t *testing.T) {
	h := newTestHandlers()
	owner := "11111111-1111-1111-1111-111111111111"
	other := "22222222-2222-2222-2222-222222222222"
	seedJob(t, h, owner, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", time.Now().UTC())
	seedJob(t, h, other, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", time.Now().UTC())

	req := newReqWithClaims(http.MethodGet, "/api/v1/jobs", userClaims(owner))
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 job for non-admin owner, got %d (%s)", len(got), rec.Body.String())
	}
	if id, _ := got[0]["jobId"].(string); !strings.EqualFold(id, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa") {
		t.Fatalf("unexpected jobId in non-admin list: %v", got[0])
	}
}

func TestHandleJobsList_AdminSeesAllJobs(t *testing.T) {
	h := newTestHandlers()
	owner := "11111111-1111-1111-1111-111111111111"
	other := "22222222-2222-2222-2222-222222222222"
	seedJob(t, h, owner, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", time.Now().UTC())
	seedJob(t, h, other, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", time.Now().UTC())

	admin := "33333333-3333-3333-3333-333333333333"
	req := newReqWithClaims(http.MethodGet, "/api/v1/jobs", adminClaims(admin))
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("admin should see all jobs, got %d", len(got))
	}
}

func TestHandleJobsGet_NonAdmin_ReturnsNotFoundForOtherUserJob(t *testing.T) {
	h := newTestHandlers()
	owner := "11111111-1111-1111-1111-111111111111"
	jobID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	seedJob(t, h, owner, jobID, time.Now().UTC())

	stranger := "22222222-2222-2222-2222-222222222222"
	req := newReqWithClaims(http.MethodGet, "/api/v1/jobs/"+jobID, userClaims(stranger))
	req.SetPathValue("job_id", jobID)
	rec := httptest.NewRecorder()
	h.HandleJobsGet(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 to avoid leaking other user's job, got %d", rec.Code)
	}
}

func TestHandleJobsGet_Admin_CanFetchAnyJob(t *testing.T) {
	h := newTestHandlers()
	owner := "11111111-1111-1111-1111-111111111111"
	jobID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	seedJob(t, h, owner, jobID, time.Now().UTC())

	admin := "33333333-3333-3333-3333-333333333333"
	req := newReqWithClaims(http.MethodGet, "/api/v1/jobs/"+jobID, adminClaims(admin))
	req.SetPathValue("job_id", jobID)
	rec := httptest.NewRecorder()
	h.HandleJobsGet(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("admin should be able to fetch any job, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJobsDelete_NonAdmin_ReturnsNotFoundForOtherUserJob(t *testing.T) {
	h := newTestHandlers()
	owner := "11111111-1111-1111-1111-111111111111"
	jobID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	seedJob(t, h, owner, jobID, time.Now().UTC())

	stranger := "22222222-2222-2222-2222-222222222222"
	req := newReqWithClaims(http.MethodDelete, "/api/v1/jobs/"+jobID, userClaims(stranger))
	req.SetPathValue("job_id", jobID)
	rec := httptest.NewRecorder()
	h.HandleJobsDelete(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("non-admin should not be able to cancel another user's job; got %d", rec.Code)
	}
}
