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
