package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestNewDefaultTimeoutConfig(t *testing.T) {
	cfg := NewDefaultTimeoutConfig()

	if cfg.DefaultTimeout == 0 {
		t.Error("default timeout should not be zero")
	}

	expectedMethods := map[string]time.Duration{
		"/proto.WorkerService/Heartbeat":    2 * time.Second,
		"/proto.WorkerService/Register":     5 * time.Second,
		"/proto.WorkerService/TaskComplete": 10 * time.Second,
		"/proto.WorkerService/TaskFailed":   10 * time.Second,
	}

	for method, expectedTimeout := range expectedMethods {
		actual, ok := cfg.MethodTimeouts[method]
		if !ok {
			t.Errorf("method %q not configured in default config", method)
		}
		if actual != expectedTimeout {
			t.Errorf("method %q: got timeout %v, want %v", method, actual, expectedTimeout)
		}
	}
}

func TestTimeoutInterceptorUnaryServer(t *testing.T) {
	cfg := NewDefaultTimeoutConfig()
	interceptor := cfg.UnaryInterceptor()

	t.Run("request_completes_before_timeout", func(t *testing.T) {
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			time.Sleep(10 * time.Millisecond)
			return "ok", nil
		}

		info := &grpc.UnaryServerInfo{FullMethod: "/proto.WorkerService/Register"}
		resp, err := interceptor(context.Background(), nil, info, handler)

		if err != nil {
			t.Errorf("got error %v, want nil", err)
		}
		if resp != "ok" {
			t.Errorf("got response %v, want ok", resp)
		}
	})

	t.Run("request_times_out", func(t *testing.T) {
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			// Respect context cancellation
			for i := 0; i < 10; i++ {
				select {
				case <-ctx.Done():
					return nil, context.DeadlineExceeded
				case <-time.After(300 * time.Millisecond):
				}
			}
			return nil, nil
		}

		// Use Heartbeat which has 2s timeout
		info := &grpc.UnaryServerInfo{FullMethod: "/proto.WorkerService/Heartbeat"}
		_, err := interceptor(context.Background(), nil, info, handler)

		if err == nil {
			t.Error("expected timeout error, got nil")
		}

		// Verify it's a context deadline exceeded error
		if err != context.DeadlineExceeded {
			t.Errorf("got error %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("handler_error_propagates", func(t *testing.T) {
		handlerErr := errors.New("test error")
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, handlerErr
		}

		info := &grpc.UnaryServerInfo{FullMethod: "/proto.WorkerService/Register"}
		_, err := interceptor(context.Background(), nil, info, handler)

		if err != handlerErr {
			t.Errorf("got error %v, want %v", err, handlerErr)
		}
	})

	t.Run("context_cancellation_honored", func(t *testing.T) {
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			// Check if context is already canceled
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(1 * time.Second):
				return "ok", nil
			}
		}

		// Create a context that's already canceled
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		info := &grpc.UnaryServerInfo{FullMethod: "/proto.WorkerService/Register"}
		_, err := interceptor(ctx, nil, info, handler)

		// Should get context canceled error
		if err != context.Canceled {
			t.Errorf("got error %v, want context.Canceled", err)
		}
	})
}

func TestTimeoutInterceptorClientUnary(t *testing.T) {
	cfg := NewDefaultTimeoutConfig()
	interceptor := cfg.ClientUnaryInterceptor()

	t.Run("client_request_completes_before_timeout", func(t *testing.T) {
		invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}

		err := interceptor(
			context.Background(),
			"/proto.WorkerService/Register",
			nil, nil, nil, invoker,
		)

		if err != nil {
			t.Errorf("got error %v, want nil", err)
		}
	})

	t.Run("client_request_times_out", func(t *testing.T) {
		invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			// Respect context cancellation
			for i := 0; i < 10; i++ {
				select {
				case <-ctx.Done():
					return context.DeadlineExceeded
				case <-time.After(300 * time.Millisecond):
				}
			}
			return nil
		}

		err := interceptor(
			context.Background(),
			"/proto.WorkerService/Heartbeat", // 2s timeout
			nil, nil, nil, invoker,
		)

		if err == nil {
			t.Error("expected timeout error, got nil")
		}

		if err != context.DeadlineExceeded {
			t.Errorf("got error %v, want context.DeadlineExceeded", err)
		}
	})
}

func TestValidateConfig(t *testing.T) {
	t.Run("valid_config_passes", func(t *testing.T) {
		cfg := NewDefaultTimeoutConfig()
		// Should not panic
		cfg.ValidateConfig()
	})

	t.Run("invalid_default_timeout", func(t *testing.T) {
		cfg := &TimeoutInterceptorConfig{
			DefaultTimeout: 0,
			MethodTimeouts: map[string]time.Duration{},
		}

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for invalid default timeout")
			}
		}()

		cfg.ValidateConfig()
	})

	t.Run("invalid_method_timeout", func(t *testing.T) {
		cfg := &TimeoutInterceptorConfig{
			DefaultTimeout: 10 * time.Second,
			MethodTimeouts: map[string]time.Duration{
				"/test/method": -1 * time.Second,
			},
		}

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for invalid method timeout")
			}
		}()

		cfg.ValidateConfig()
	})
}

func TestTimeoutValues(t *testing.T) {
	// Test that configured timeout values are reasonable
	cfg := NewDefaultTimeoutConfig()

	tests := []struct {
		method  string
		maxTime time.Duration
	}{
		{"/proto.WorkerService/Heartbeat", 2 * time.Second},
		{"/proto.WorkerService/Register", 5 * time.Second},
		{"/proto.WorkerService/TaskComplete", 10 * time.Second},
		{"/proto.WorkerService/TaskFailed", 10 * time.Second},
	}

	for _, tt := range tests {
		timeout := cfg.MethodTimeouts[tt.method]
		if timeout == 0 {
			t.Errorf("method %q not configured", tt.method)
		}
		if timeout != tt.maxTime {
			t.Errorf("method %q: got %v, want %v", tt.method, timeout, tt.maxTime)
		}
	}
}

func BenchmarkUnaryServerInterceptor(b *testing.B) {
	cfg := NewDefaultTimeoutConfig()
	interceptor := cfg.UnaryInterceptor()

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/proto.WorkerService/Register"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		interceptor(context.Background(), nil, info, handler)
	}
}

func BenchmarkClientUnaryInterceptor(b *testing.B) {
	cfg := NewDefaultTimeoutConfig()
	interceptor := cfg.ClientUnaryInterceptor()

	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		interceptor(
			context.Background(),
			"/proto.WorkerService/Register",
			nil, nil, nil, invoker,
		)
	}
}
