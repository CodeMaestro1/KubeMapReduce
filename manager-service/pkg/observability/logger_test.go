package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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

func TestNewLogger_AllLogLevels(t *testing.T) {
	tests := []struct {
		envLevel string
		enabled  slog.Level
		disabled slog.Level
	}{
		{"debug", slog.LevelDebug, slog.Level(-8)},
		{"warn", slog.LevelWarn, slog.LevelInfo},
		{"warning", slog.LevelWarn, slog.LevelInfo},
		{"error", slog.LevelError, slog.LevelWarn},
		{"unknown", slog.LevelInfo, slog.LevelDebug},
		{"  INFO  ", slog.LevelInfo, slog.LevelDebug},
	}

	for _, tt := range tests {
		t.Run(tt.envLevel, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", tt.envLevel)
			logger := NewLogger("test")
			if !logger.Enabled(nil, tt.enabled) {
				t.Errorf("expected level %v to be enabled for LOG_LEVEL=%q", tt.enabled, tt.envLevel)
			}
			if logger.Enabled(nil, tt.disabled) {
				t.Errorf("expected level %v to be disabled for LOG_LEVEL=%q", tt.disabled, tt.envLevel)
			}
		})
	}
}

func TestNewLogger_RespectsLogFormatText(t *testing.T) {
	t.Setenv("LOG_FORMAT", "text")
	logger := NewLogger("test")
	if logger == nil {
		t.Fatal("NewLogger returned nil")
	}

	// text format isn't directly observable easily from the returned logger without capturing stderr,
	// but calling it verifies the code path doesn't panic.
	if !logger.Enabled(nil, slog.LevelInfo) {
		t.Errorf("text logger should accept Info level")
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

func TestLoggerFromContext_InvalidValueType(t *testing.T) {
	ctx := context.WithValue(context.Background(), loggerCtxKey{}, "not-a-logger")
	if LoggerFromContext(ctx) == nil {
		t.Fatal("LoggerFromContext with invalid type must not return nil")
	}
}

func TestWithLogger_NilContext(t *testing.T) {
	logger := slog.Default()
	ctx := WithLogger(nil, logger)
	if ctx == nil {
		t.Fatal("WithLogger(nil) returned nil context")
	}
	if LoggerFromContext(ctx) != logger {
		t.Fatal("WithLogger(nil) did not attach logger correctly")
	}
}
