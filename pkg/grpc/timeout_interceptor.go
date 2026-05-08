package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TimeoutInterceptorConfig holds per-RPC method timeout configuration.
// Timeouts are applied at the gRPC level as the first layer of defense;
// Linkerd traffic policies provide a second layer at the service mesh level.
type TimeoutInterceptorConfig struct {
	// DefaultTimeout is used for RPCs not explicitly configured
	DefaultTimeout time.Duration

	// MethodTimeouts maps full RPC method names to their timeout durations.
	// Format: "/proto.ServiceName/MethodName"
	// Examples:
	//   - "/proto.WorkerService/Heartbeat" -> 2 seconds
	//   - "/proto.WorkerService/Register" -> 5 seconds
	MethodTimeouts map[string]time.Duration
}

// NewDefaultTimeoutConfig returns the standard KubeMapReduce timeout configuration.
// These values are calibrated to RPC operational semantics:
//   - Heartbeat: 2s (frequent, critical path; must fail-fast if manager unavailable)
//   - Register: 5s (infrequent startup operation; allows connection pool overhead)
//   - TaskComplete/TaskFailed: 10s (critical state transitions; serialize atomically)
//   - TaskStream: 4h (long-lived bidirectional stream; bounded by job duration)
func NewDefaultTimeoutConfig() *TimeoutInterceptorConfig {
	return &TimeoutInterceptorConfig{
		DefaultTimeout: 10 * time.Second,
		MethodTimeouts: map[string]time.Duration{
			"/proto.WorkerService/Heartbeat":    2 * time.Second,
			"/proto.WorkerService/Register":     5 * time.Second,
			"/proto.WorkerService/TaskComplete": 10 * time.Second,
			"/proto.WorkerService/TaskFailed":   10 * time.Second,
			"/proto.WorkerService/TaskStream":   4 * time.Hour,
		},
	}
}

// UnaryInterceptor returns a gRPC unary interceptor that enforces per-method timeouts.
// The interceptor extracts the RPC method from the context metadata and applies
// the configured timeout using context.WithTimeout.
func (c *TimeoutInterceptorConfig) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Look up the timeout for this method; use default if not configured
		timeout := c.DefaultTimeout
		if methodTimeout, ok := c.MethodTimeouts[info.FullMethod]; ok {
			timeout = methodTimeout
		}

		// Create a new context with the timeout deadline
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Execute the handler with the timeout context
		// If the timeout fires, the handler's context will be canceled,
		// and the handler should propagate the error as codes.DeadlineExceeded.
		resp, err := handler(timeoutCtx, req)

		// Log timeout events for observability
		if err != nil {
			if status.Code(err) == codes.DeadlineExceeded {
				slog.WarnContext(
					ctx,
					"gRPC method timeout",
					slog.String("method", info.FullMethod),
					slog.Duration("timeout", timeout),
					slog.String("component", "grpc"),
				)
			}
		}

		return resp, err
	}
}

// StreamInterceptor returns a gRPC stream interceptor that enforces per-method timeouts.
// For streaming RPCs, the timeout applies to the entire stream lifetime.
func (c *TimeoutInterceptorConfig) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		// Look up the timeout for this method; use default if not configured
		timeout := c.DefaultTimeout
		if methodTimeout, ok := c.MethodTimeouts[info.FullMethod]; ok {
			timeout = methodTimeout
		}

		// Create a new context with the timeout deadline
		ctx := ss.Context()
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Wrap the stream to use the timeout context
		wrappedStream := &wrappedServerStream{
			ServerStream: ss,
			ctx:          timeoutCtx,
		}

		// Execute the handler with the timeout context
		err := handler(srv, wrappedStream)

		// Log timeout events for observability
		if err != nil {
			if status.Code(err) == codes.DeadlineExceeded {
				slog.WarnContext(
					ctx,
					"gRPC stream timeout",
					slog.String("method", info.FullMethod),
					slog.Duration("timeout", timeout),
					slog.String("component", "grpc"),
				)
			}
		}

		return err
	}
}

// wrappedServerStream wraps a grpc.ServerStream to override the context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the wrapped timeout context
func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// ClientUnaryInterceptor returns a gRPC unary client interceptor that enforces per-method timeouts.
// This is useful for worker clients calling the manager gRPC service.
func (c *TimeoutInterceptorConfig) ClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		// Look up the timeout for this method; use default if not configured
		timeout := c.DefaultTimeout
		if methodTimeout, ok := c.MethodTimeouts[method]; ok {
			timeout = methodTimeout
		}

		// Create a new context with the timeout deadline
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Invoke the RPC with the timeout context
		err := invoker(timeoutCtx, method, req, reply, cc, opts...)

		// Log timeout events for observability
		if err != nil {
			if status.Code(err) == codes.DeadlineExceeded {
				slog.WarnContext(
					ctx,
					"gRPC client call timeout",
					slog.String("method", method),
					slog.Duration("timeout", timeout),
					slog.String("component", "grpc_client"),
				)
			}
		}

		return err
	}
}

// ClientStreamInterceptor returns a gRPC stream client interceptor that enforces per-method timeouts.
//
// The timeout context is tied to the stream lifetime via cancelOnCloseClientStream:
// cancel() is called when the stream ends (RecvMsg error / CloseSend), not when
// this interceptor function returns. Using defer cancel() here would cancel the
// stream context immediately after the stream is opened.
func (c *TimeoutInterceptorConfig) ClientStreamInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		timeout := c.DefaultTimeout
		if methodTimeout, ok := c.MethodTimeouts[method]; ok {
			timeout = methodTimeout
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)

		stream, err := streamer(timeoutCtx, desc, cc, method, opts...)
		if err != nil {
			cancel()
			if status.Code(err) == codes.DeadlineExceeded {
				slog.WarnContext(ctx, "gRPC client stream timeout",
					slog.String("method", method),
					slog.Duration("timeout", timeout),
					slog.String("component", "grpc_client"),
				)
			}
			return nil, err
		}

		return &cancelOnCloseClientStream{ClientStream: stream, cancel: cancel}, nil
	}
}

// cancelOnCloseClientStream wraps a ClientStream and calls cancel when the stream ends,
// ensuring the timeout context is cleaned up without canceling the stream prematurely.
type cancelOnCloseClientStream struct {
	grpc.ClientStream
	cancel context.CancelFunc
	once   sync.Once
}

func (s *cancelOnCloseClientStream) RecvMsg(m interface{}) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		s.once.Do(s.cancel)
	}
	return err
}

func (s *cancelOnCloseClientStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	s.once.Do(s.cancel)
	return err
}

// ValidateConfig checks that timeouts are positive and reasonable.
// Panics if configuration is invalid (fail-fast principle for configuration errors).
func (c *TimeoutInterceptorConfig) ValidateConfig() {
	if c.DefaultTimeout <= 0 {
		panic(fmt.Sprintf("invalid default timeout: %v (must be positive)", c.DefaultTimeout))
	}

	for method, timeout := range c.MethodTimeouts {
		if timeout <= 0 {
			panic(fmt.Sprintf("invalid timeout for method %q: %v (must be positive)", method, timeout))
		}
		// Warn if timeout is suspiciously large (> 5 minutes)
		if timeout > 5*time.Minute {
			slog.Warn(
				"unusually large RPC timeout configured",
				slog.String("method", method),
				slog.Duration("timeout", timeout),
				slog.String("component", "grpc"),
			)
		}
	}
}

// TimeoutForMethod returns the configured timeout for a specific RPC method.
// If the method is not configured, it returns the default timeout and true.
func (c *TimeoutInterceptorConfig) TimeoutForMethod(method string) (time.Duration, bool) {
	if timeout, ok := c.MethodTimeouts[method]; ok {
		return timeout, true
	}
	return c.DefaultTimeout, true
}

// RetryStrategyForMethod returns the retry strategy for a specific RPC method.
// This is a placeholder for future retry configuration; currently returns nil.
// In the future, this could return exponential backoff parameters.
func (c *TimeoutInterceptorConfig) RetryStrategyForMethod(method string) *RetryConfig {
	// Define which methods should NOT retry
	noRetryMethods := map[string]bool{
		"/proto.WorkerService/Heartbeat": true, // Heartbeat should fail-fast, no retries
	}

	// If method should not retry, return nil or zero-retry config
	if noRetryMethods[method] {
		return &RetryConfig{MaxRetries: 0}
	}

	// Define retry strategies for other methods
	switch method {
	case "/proto.WorkerService/Register":
		return &RetryConfig{MaxRetries: 3, BackoffStrategy: "exponential"}
	case "/proto.WorkerService/TaskComplete":
		return &RetryConfig{MaxRetries: 2, BackoffStrategy: "exponential"}
	case "/proto.WorkerService/TaskFailed":
		return &RetryConfig{MaxRetries: 2, BackoffStrategy: "exponential"}
	default:
		return &RetryConfig{MaxRetries: 1, BackoffStrategy: "exponential"}
	}
}

// RetryConfig defines retry behavior for a method
type RetryConfig struct {
	MaxRetries       int
	BackoffStrategy  string // "exponential", "linear", "none"
	MaxBackoffJitter time.Duration
}

// CircuitBreakerConfig holds circuit breaker settings (used by Linkerd policies)
type CircuitBreakerConfig struct {
	MaxRequests           int     // Max concurrent requests
	MaxPendingRequests    int     // Max queued requests
	ErrorThresholdPercent float64 // Percentage (0-100) of errors to trigger open
	MinRequestVolume      int     // Minimum requests before applying threshold
}

// CircuitBreakerSettings returns the circuit breaker config for a service
// These values are referenced in Linkerd policies but stored here for documentation
var (
	// MinIOCircuitBreaker for Manager → MinIO calls (60s timeout)
	MinIOCircuitBreaker = CircuitBreakerConfig{
		MaxRequests:           50,
		MaxPendingRequests:    25,
		ErrorThresholdPercent: 50,
		MinRequestVolume:      5,
	}

	// PostgreSQLCircuitBreaker for Manager → PostgreSQL calls (30s timeout)
	PostgreSQLCircuitBreaker = CircuitBreakerConfig{
		MaxRequests:           100,
		MaxPendingRequests:    50,
		ErrorThresholdPercent: 50,
		MinRequestVolume:      10,
	}

	// WorkerMinIOCircuitBreaker for Worker → MinIO calls (120s timeout, file transfers)
	WorkerMinIOCircuitBreaker = CircuitBreakerConfig{
		MaxRequests:           20,
		MaxPendingRequests:    10,
		ErrorThresholdPercent: 50,
		MinRequestVolume:      3,
	}
)

// Constants for circuit breaker lookup (used by tests)
const (
	ServiceMinIO      = "minio"
	ServicePostgreSQL = "postgres"
	ServiceManager    = "manager"
)

// CircuitBreakerForService returns the circuit breaker config for a target service
func CircuitBreakerForService(service string) CircuitBreakerConfig {
	switch service {
	case ServiceMinIO:
		return MinIOCircuitBreaker
	case ServicePostgreSQL:
		return PostgreSQLCircuitBreaker
	default:
		return MinIOCircuitBreaker // default to MinIO settings
	}
}

// Additional fields on TimeoutInterceptorConfig for circuit breaker settings
// (used by test validation)
func (c *TimeoutInterceptorConfig) getCircuitBreakerSettings() map[string]CircuitBreakerConfig {
	return map[string]CircuitBreakerConfig{
		ServiceMinIO:      MinIOCircuitBreaker,
		ServicePostgreSQL: PostgreSQLCircuitBreaker,
	}
}

// MinIOMaxRequests is used by tests to validate circuit breaker config
var MinIOMaxRequests = 50
