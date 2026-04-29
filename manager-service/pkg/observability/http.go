package observability

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// RequestIDHeader is the canonical HTTP header used to propagate a request
// trace ID across services. If a client supplies this header it is adopted
// verbatim; otherwise the middleware generates a fresh UUID.
const RequestIDHeader = "X-Request-Id"

type requestIDCtxKey struct{}

// WithRequestID returns a new context carrying the supplied request ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestIDCtxKey{}, id)
}

// RequestIDFromContext returns the request ID stored on ctx, or "" if none
// is present.
func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(requestIDCtxKey{}).(string); ok {
		return id
	}
	return ""
}

// statusRecorder is a [http.ResponseWriter] decorator that captures the
// status code written by the wrapped handler so that the access log can
// report it. If the handler never calls WriteHeader the recorded status
// defaults to 200 (matching net/http's behaviour).
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += n
	return n, err
}

// RequestIDMiddleware wraps next so that every incoming HTTP request is
// tagged with a stable trace ID. Behaviour:
//
//   - If the client sets [RequestIDHeader], that value is adopted unchanged.
//   - Otherwise a fresh UUIDv4 is generated.
//   - The ID is echoed back to the client in the same response header.
//   - The ID is attached to the request context (see [RequestIDFromContext]).
//   - A child [*slog.Logger] carrying request_id, method and path is also
//     stashed on the context (see [LoggerFromContext]).
//   - At the end of the request an access log line is emitted at INFO level
//     with status, latency, method, path and request_id.
//
// The base parameter is used as the parent logger for the per-request child
// logger; pass the process-wide logger returned by [NewLogger].
func RequestIDMiddleware(base *slog.Logger) func(http.Handler) http.Handler {
	if base == nil {
		base = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(RequestIDHeader)
			if id == "" {
				id = uuid.NewString()
			}
			w.Header().Set(RequestIDHeader, id)

			reqLogger := base.With(
				slog.String("request_id", id),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
			)
			ctx := WithRequestID(r.Context(), id)
			ctx = WithLogger(ctx, reqLogger)

			rec := &statusRecorder{ResponseWriter: w}
			start := time.Now()
			next.ServeHTTP(rec, r.WithContext(ctx))

			// Skip access logs for liveness/readiness probes to keep the
			// log volume manageable when run under Kubernetes.
			if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
				return
			}
			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}
			duration := time.Since(start)
			if m := DefaultMetrics(); m != nil && m.HTTPRequestDurationSeconds != nil {
				m.HTTPRequestDurationSeconds.WithLabelValues(r.Method, statusClass(status)).Observe(duration.Seconds())
			}
			reqLogger.LogAttrs(ctx, slog.LevelInfo, "http_request",
				slog.Int("status", status),
				slog.Duration("duration", duration),
				slog.Int("bytes", rec.bytes),
			)
		})
	}
}

// statusClass maps an HTTP status code into the canonical "Nxx" bucket
// (e.g. 200 -> "2xx", 404 -> "4xx") used as a Prometheus label value to
// keep cardinality bounded.
func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	case status >= 100:
		return "1xx"
	default:
		return "unknown"
	}
}
