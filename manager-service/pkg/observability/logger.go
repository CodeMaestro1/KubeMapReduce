// Package observability provides shared cross-cutting helpers for structured
// logging, Prometheus metrics, and HTTP request tracing across the
// KubeMapReduce services.
//
// The package is deliberately minimal:
//
//   - [NewLogger] returns a process-wide [*slog.Logger] configured for either
//     human-readable text (local development) or JSON (production / containers).
//   - [WithRequestID] / [RequestIDFromContext] propagate a per-request UUID
//     through context for log correlation.
//   - [RequestIDMiddleware] is an [net/http] middleware that generates (or
//     adopts) a request ID and emits an access log line at the end of each
//     request.
//
// All exported identifiers are safe for concurrent use.
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// LogFormat selects the slog handler implementation.
type LogFormat string

const (
	// LogFormatJSON emits one JSON object per log record. Recommended in
	// production and inside containers where logs are scraped by a collector.
	LogFormatJSON LogFormat = "json"
	// LogFormatText emits human-readable key=value pairs. Recommended for
	// local development.
	LogFormatText LogFormat = "text"
)

// NewLogger constructs a [*slog.Logger] configured from the LOG_LEVEL and
// LOG_FORMAT environment variables.
//
// LOG_LEVEL accepts "debug", "info", "warn", "error" (case-insensitive) and
// defaults to "info". LOG_FORMAT accepts "json" or "text" and defaults to
// "json".
//
// The returned logger always carries a "service" attribute with the supplied
// name so that records from different services can be distinguished in a
// shared log stream.
func NewLogger(service string) *slog.Logger {
	level := parseLevel(os.Getenv("LOG_LEVEL"))
	format := parseFormat(os.Getenv("LOG_FORMAT"))

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	switch format {
	case LogFormatText:
		handler = slog.NewTextHandler(os.Stderr, opts)
	default:
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}

	return slog.New(handler).With(slog.String("service", service))
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func parseFormat(raw string) LogFormat {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "text":
		return LogFormatText
	default:
		return LogFormatJSON
	}
}

// LoggerFromContext returns the [*slog.Logger] previously stored on ctx by
// [WithLogger], or [slog.Default] if none is present. It never returns nil.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return slog.Default()
	}
	if l, ok := ctx.Value(loggerCtxKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

// WithLogger returns a new context carrying logger. Subsequent calls to
// [LoggerFromContext] on the returned context (or any derived context) will
// return logger.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, loggerCtxKey{}, logger)
}

type loggerCtxKey struct{}
