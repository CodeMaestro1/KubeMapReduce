package httputil

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type failingWriteResponseWriter struct {
	header http.Header
	status int
}

func (w *failingWriteResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *failingWriteResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

func (w *failingWriteResponseWriter) Write(p []byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestWriteJSON_Success(t *testing.T) {
	rec := httptest.NewRecorder()

	err := WriteJSON(rec, http.StatusAccepted, map[string]string{"status": "ok"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}

	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected content type application/json, got %q", got)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `"status":"ok"`) {
		t.Fatalf("expected json response body to contain status field, got %q", body)
	}
}

func TestWriteJSON_MarshalFailureReturns500(t *testing.T) {
	rec := httptest.NewRecorder()

	err := WriteJSON(rec, http.StatusOK, map[string]interface{}{"bad": make(chan int)})
	if err == nil {
		t.Fatal("expected marshal error, got nil")
	}

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, rec.Code)
	}

	if !strings.Contains(rec.Body.String(), "failed to encode response") {
		t.Fatalf("expected internal error message, got %q", rec.Body.String())
	}
}

func TestWriteJSON_WriteFailureReturnsError(t *testing.T) {
	rw := &failingWriteResponseWriter{}

	err := WriteJSON(rw, http.StatusOK, map[string]string{"status": "ok"})
	if err == nil {
		t.Fatal("expected write error, got nil")
	}

	if rw.status != http.StatusOK {
		t.Fatalf("expected status %d before write failure, got %d", http.StatusOK, rw.status)
	}
}

func TestWriteError_WritesStatusAndMessage(t *testing.T) {
	rec := httptest.NewRecorder()

	WriteError(rec, http.StatusBadGateway, "upstream auth failed")

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected status %d, got %d", http.StatusBadGateway, rec.Code)
	}

	if !strings.Contains(rec.Body.String(), "upstream auth failed") {
		t.Fatalf("expected response body to contain message, got %q", rec.Body.String())
	}
}

// ── DecodeJSONBody tests ────────────────────────────────────

func TestDecodeJSONBody_Success(t *testing.T) {
	body := `{"name":"test"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()

	var dst struct{ Name string }
	err := DecodeJSONBody(rec, req, &dst, 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if dst.Name != "test" {
		t.Fatalf("expected Name=test, got %q", dst.Name)
	}
}

func TestDecodeJSONBody_RejectsOversizedBody(t *testing.T) {
	// Create a body larger than the limit.
	payload := `{"name":"` + strings.Repeat("x", 128) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	rec := httptest.NewRecorder()

	var dst struct{ Name string }
	err := DecodeJSONBody(rec, req, &dst, 16) // 16 byte limit
	if err == nil {
		t.Fatal("expected error for oversized body, got nil")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status %d, got %d", http.StatusRequestEntityTooLarge, rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "request body too large") {
		t.Fatalf("expected body-too-large message, got %q", rec.Body.String())
	}
}

func TestDecodeJSONBody_RejectsMalformedJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()

	var dst struct{ Name string }
	err := DecodeJSONBody(rec, req, &dst, 0)
	if err == nil {
		t.Fatal("expected error for bad JSON, got nil")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "invalid request payload") {
		t.Fatalf("expected invalid payload message, got %q", rec.Body.String())
	}
}

func TestDecodeJSONBody_UsesDefaultLimitWhenZero(t *testing.T) {
	// A body well under 1 MB should succeed with limit=0 (default).
	body := `{"ok":true}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()

	var dst struct{ Ok bool }
	if err := DecodeJSONBody(rec, req, &dst, 0); err != nil {
		t.Fatalf("expected no error with default limit, got %v", err)
	}
	if !dst.Ok {
		t.Fatal("expected Ok=true")
	}
}
