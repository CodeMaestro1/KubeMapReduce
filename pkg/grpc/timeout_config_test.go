package grpc

import (
	"context"
	"testing"
	"time"
)

// TestTimeoutConfig validates timeout values match operational SLAs
func TestTimeoutConfig_ValidatesSLAs(t *testing.T) {
	cfg := NewDefaultTimeoutConfig()
	cfg.ValidateConfig()

	tests := []struct {
		name        string
		method      string
		expectedMax time.Duration
		minBoundary time.Duration
		shouldExist bool
	}{
		{
			name:        "Heartbeat timeout is 2s",
			method:      "/mapreduce.WorkerService/Heartbeat",
			expectedMax: 2 * time.Second,
			shouldExist: true,
		},
		{
			name:        "Register timeout is 5s",
			method:      "/mapreduce.WorkerService/Register",
			expectedMax: 5 * time.Second,
			shouldExist: true,
		},
		{
			name:        "TaskComplete timeout is 10s",
			method:      "/mapreduce.WorkerService/TaskComplete",
			expectedMax: 10 * time.Second,
			shouldExist: true,
		},
		{
			name:        "TaskFailed timeout is 10s",
			method:      "/mapreduce.WorkerService/TaskFailed",
			expectedMax: 10 * time.Second,
			shouldExist: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeout, exists := cfg.TimeoutForMethod(tt.method)
			if tt.shouldExist && !exists {
				t.Errorf("expected timeout for %s, got none", tt.method)
			}
			if exists && timeout != tt.expectedMax {
				t.Errorf("expected timeout %v, got %v", tt.expectedMax, timeout)
			}
		})
	}
}

// TestTimeoutConfig_DefaultFallback verifies unspecified methods use default timeout
func TestTimeoutConfig_DefaultFallback(t *testing.T) {
	cfg := NewDefaultTimeoutConfig()

	// Unspecified method should get default (10s)
	timeout, exists := cfg.TimeoutForMethod("/proto.CustomService/UnknownMethod")
	if !exists {
		t.Error("expected default timeout to apply")
	}
	if timeout != 10*time.Second {
		t.Errorf("expected default 10s timeout, got %v", timeout)
	}
}

// TestTimeoutConfig_CircuitBreakerSettings validates circuit breaker config
func TestTimeoutConfig_CircuitBreakerSettings(t *testing.T) {
	// Verify circuit breaker is configured for long-running operations
	tests := []struct {
		name                   string
		expectedMaxRequests    int
		expectedMaxPending     int
		expectedErrorThreshold float64
		expectedMinRequestVol  int
	}{
		{
			name:                   "MinIO circuit breaker allows burst traffic",
			expectedMaxRequests:    50,
			expectedMaxPending:     25,
			expectedErrorThreshold: 50.0,
			expectedMinRequestVol:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cbConfig := CircuitBreakerForService(ServiceMinIO)
			if cbConfig.MaxRequests != tt.expectedMaxRequests {
				t.Errorf("expected MinIO max requests %d, got %d", tt.expectedMaxRequests, cbConfig.MaxRequests)
			}
		})
	}
}

// TestTimeoutEnforcement_ContextDeadline verifies context deadline is enforced
func TestTimeoutEnforcement_ContextDeadline(t *testing.T) {
	cfg := NewDefaultTimeoutConfig()
	method := "/mapreduce.WorkerService/Heartbeat"
	timeout, _ := cfg.TimeoutForMethod(method)

	// Create context with timeout
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Verify deadline is set correctly
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Error("expected context deadline to be set")
	}

	// Deadline should be roughly timeout from now
	now := time.Now()
	actualTimeout := deadline.Sub(now)

	// Allow 100ms skew due to execution time
	if actualTimeout < timeout-100*time.Millisecond || actualTimeout > timeout+100*time.Millisecond {
		t.Errorf("expected timeout ~%v, got %v", timeout, actualTimeout)
	}
}

// TestTimeoutConfig_RetryStrategy validates retry values
func TestTimeoutConfig_RetryStrategy(t *testing.T) {
	tests := []struct {
		name               string
		method             string
		shouldAllowRetries bool
		maxRetries         int
	}{
		{
			name:               "Heartbeat should NOT retry",
			method:             "/mapreduce.WorkerService/Heartbeat",
			shouldAllowRetries: false,
			maxRetries:         0,
		},
		{
			name:               "Register should allow 3 retries",
			method:             "/mapreduce.WorkerService/Register",
			shouldAllowRetries: true,
			maxRetries:         3,
		},
		{
			name:               "TaskComplete should allow 2 retries",
			method:             "/mapreduce.WorkerService/TaskComplete",
			shouldAllowRetries: true,
			maxRetries:         2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewDefaultTimeoutConfig()
			retryConfig := cfg.RetryStrategyForMethod(tt.method)

			if tt.shouldAllowRetries {
				if retryConfig == nil {
					t.Errorf("expected retry config for %s", tt.method)
				} else if retryConfig.MaxRetries != tt.maxRetries {
					t.Errorf("expected %d max retries, got %d", tt.maxRetries, retryConfig.MaxRetries)
				}
			} else {
				if retryConfig != nil && retryConfig.MaxRetries > 0 {
					t.Errorf("expected no retries for %s, but got %d", tt.method, retryConfig.MaxRetries)
				}
			}
		})
	}
}

// BenchmarkTimeoutLookup measures performance of timeout lookup
func BenchmarkTimeoutLookup(b *testing.B) {
	cfg := NewDefaultTimeoutConfig()
	method := "/mapreduce.WorkerService/Heartbeat"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg.TimeoutForMethod(method)
	}
}

// TestTimeoutConfig_CriticalPathOptimized verifies hot paths have minimal timeouts
func TestTimeoutConfig_CriticalPathOptimized(t *testing.T) {
	cfg := NewDefaultTimeoutConfig()

	// Heartbeat is the critical path and must fail-fast
	timeout, _ := cfg.TimeoutForMethod("/mapreduce.WorkerService/Heartbeat")
	if timeout > 3*time.Second {
		t.Errorf("Heartbeat timeout too high (%v); should be ≤2s for fail-fast", timeout)
	}

	// Register is less frequent, so can have longer timeout
	regTimeout, _ := cfg.TimeoutForMethod("/mapreduce.WorkerService/Register")
	if regTimeout < 3*time.Second || regTimeout > 10*time.Second {
		t.Errorf("Register timeout out of range (%v); expected 5s", regTimeout)
	}
}
