package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleHealth(t *testing.T) {
	h := NewHandlers()

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
	h := NewHandlers()

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rec := httptest.NewRecorder()

	h.HandleHealth(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHandleJobsSubmit_RejectsInvalidPayload(t *testing.T) {
	h := NewHandlers()

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsSubmit_RejectsEmptyFilename(t *testing.T) {
	h := NewHandlers()

	body := `{"filename":"","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsSubmit_AcceptsValidJob(t *testing.T) {
	h := NewHandlers()

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
	h := NewHandlers()

	body := `{"workerReplicas":0,"maxJobsPerNode":5}`
	req := httptest.NewRequest(http.MethodPut, "/admin/workers/config", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleWorkerConfig(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleWorkerConfig_AcceptsValidConfig(t *testing.T) {
	h := NewHandlers()

	body := `{"workerReplicas":4,"maxJobsPerNode":8}`
	req := httptest.NewRequest(http.MethodPut, "/admin/workers/config", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleWorkerConfig(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}
}

func TestHandleWorkerConfig_ReturnsNotImplemented(t *testing.T) {
	fakeClient := &fakeAdminClient{}
	h := NewHandlers(fakeClient, "", UIConfig{})

	body := `{"workerReplicas":3,"maxJobsPerNode":10}`
	req := httptest.NewRequest(http.MethodPut, "/admin/workers/config", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleWorkerConfig(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("expected status %d, got %d", http.StatusNotImplemented, rec.Code)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response json: %v", err)
	}

	if got := payload["status"]; got != "not_implemented" {
		t.Fatalf("expected status not_implemented, got %v", got)
	}
}
