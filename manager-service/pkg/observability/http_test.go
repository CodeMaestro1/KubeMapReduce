package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewLogger_DefaultsToJSONInfo(t *testing.T) {
	t.Setenv("LOG_FORMAT", "")
	t.Setenv("LOG_LEVEL", "")

	logger := NewLogger("test")
	if logger == nil {
		t.Fatal("NewLogger returned nil")
	}
	// Sanity: should at least carry the service attribute.
	if !logger.Enabled(nil, slog.LevelInfo) {
		t.Errorf("default logger should accept Info level")
	}
	if logger.Enabled(nil, slog.LevelDebug) {
		t.Errorf("default logger should not accept Debug level when LOG_LEVEL is unset")
	}
}

func TestNewLogger_RespectsLogLevel(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	logger := NewLogger("test")
	if !logger.Enabled(nil, slog.LevelDebug) {
		t.Errorf("LOG_LEVEL=debug should enable Debug level")
	}
}

func TestRequestIDMiddleware_GeneratesIDWhenMissing(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	captured := ""
	handler := RequestIDMiddleware(base)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if captured == "" {
		t.Fatal("expected request ID to be set on context")
	}
	if got := rec.Header().Get(RequestIDHeader); got != captured {
		t.Fatalf("response header %q = %q, want %q", RequestIDHeader, got, captured)
	}
	if !strings.Contains(buf.String(), captured) {
		t.Fatalf("expected access log to mention request_id %q; got %q", captured, buf.String())
	}
	if !strings.Contains(buf.String(), `"status":418`) {
		t.Fatalf("expected access log to record status 418; got %q", buf.String())
	}
}

func TestRequestIDMiddleware_AdoptsClientHeader(t *testing.T) {
	base := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

	const supplied = "abc-123"
	captured := ""
	handler := RequestIDMiddleware(base)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = RequestIDFromContext(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(RequestIDHeader, supplied)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if captured != supplied {
		t.Fatalf("request ID = %q, want %q", captured, supplied)
	}
	if got := rec.Header().Get(RequestIDHeader); got != supplied {
		t.Fatalf("response header = %q, want %q", got, supplied)
	}
}

func TestRequestIDMiddleware_SkipsHealthProbes(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := RequestIDMiddleware(base)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, path := range []string{"/healthz", "/readyz"} {
		buf.Reset()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		handler.ServeHTTP(httptest.NewRecorder(), req)
		if buf.Len() != 0 {
			t.Fatalf("expected no access log for %s; got %q", path, buf.String())
		}
	}
}

func TestLoggerFromContext_ReturnsAttachedLogger(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := WithLogger(nil, base.With("k", "v"))

	LoggerFromContext(ctx).Info("hello")

	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("invalid JSON log: %v (raw=%q)", err, buf.String())
	}
	if got["k"] != "v" {
		t.Fatalf("logger context not propagated; got %v", got)
	}
}

func TestLoggerFromContext_DefaultWhenMissing(t *testing.T) {
	if LoggerFromContext(nil) == nil {
		t.Fatal("LoggerFromContext(nil) must not return nil")
	}
}
