package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"kubemapreduce/auth-service/pkg/auth"
)

func TestTokenBucketLimiter_Allow(t *testing.T) {
	tests := []struct {
		name              string
		requestsPerSecond float64
		requests          int
		wantAllowed       int
	}{
		{
			name:              "single request per second",
			requestsPerSecond: 1,
			requests:          3,
			wantAllowed:       1, // First request succeeds, rest fail
		},
		{
			name:              "ten requests per second",
			requestsPerSecond: 10,
			requests:          15,
			wantAllowed:       10, // First 10 succeed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := NewTokenBucketLimiter(tt.requestsPerSecond)

			allowed := 0
			for i := 0; i < tt.requests; i++ {
				if limiter.Allow() {
					allowed++
				}
			}

			if allowed != tt.wantAllowed {
				t.Errorf("Allow() got %d requests allowed, want %d", allowed, tt.wantAllowed)
			}
		})
	}
}

func TestTokenBucketLimiter_RefillsOverTime(t *testing.T) {
	limiter := NewTokenBucketLimiter(1) // 1 request per second

	// First request should succeed
	if !limiter.Allow() {
		t.Fatal("first request should be allowed")
	}

	// Second request should fail (no tokens left)
	if limiter.Allow() {
		t.Fatal("second request should be denied (no tokens)")
	}

	// Wait 1.1 seconds for token to refill
	time.Sleep(1100 * time.Millisecond)

	// Now another request should succeed
	if !limiter.Allow() {
		t.Fatal("third request after refill should be allowed")
	}
}

func TestPerUserRateLimitMiddleware_AllowsWithoutClaims(t *testing.T) {
	middleware := PerUserRateLimitMiddleware(1)

	called := false
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(nextHandler)

	// Request without JWT claims should be allowed
	req := httptest.NewRequest("GET", "/api/v1/jobs", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if !called {
		t.Fatal("next handler should be called when no claims present")
	}
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestPerUserRateLimitMiddleware_EnforcesSeparateLimitsPerUser(t *testing.T) {
	middleware := PerUserRateLimitMiddleware(1) // 1 request per second per user

	callCount := 0
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(nextHandler)

	// Create two requests for different users
	userAClaims := jwt.MapClaims{
		"sub": "user-a",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	userBClaims := jwt.MapClaims{
		"sub": "user-b",
		"exp": time.Now().Add(time.Hour).Unix(),
	}

	// First request from user-a should succeed
	req1 := httptest.NewRequest("GET", "/api/v1/jobs", nil)
	ctx1 := auth.ContextWithClaims(req1.Context(), userAClaims)
	req1 = req1.WithContext(ctx1)
	w1 := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Errorf("user-a first request: expected status 200, got %d", w1.Code)
	}

	// Second request from user-a should fail (rate limit)
	req2 := httptest.NewRequest("GET", "/api/v1/jobs", nil)
	ctx2 := auth.ContextWithClaims(req2.Context(), userAClaims)
	req2 = req2.WithContext(ctx2)
	w2 := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w2, req2)

	if w2.Code != http.StatusTooManyRequests {
		t.Errorf("user-a second request: expected status 429, got %d", w2.Code)
	}

	// First request from user-b should succeed (separate rate limit)
	req3 := httptest.NewRequest("GET", "/api/v1/jobs", nil)
	ctx3 := auth.ContextWithClaims(req3.Context(), userBClaims)
	req3 = req3.WithContext(ctx3)
	w3 := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w3, req3)

	if w3.Code != http.StatusOK {
		t.Errorf("user-b first request: expected status 200, got %d", w3.Code)
	}

	if callCount != 2 {
		t.Errorf("expected 2 successful calls, got %d", callCount)
	}
}

func TestPerUserRateLimitMiddleware_ReturnsRetryAfterHeader(t *testing.T) {
	middleware := PerUserRateLimitMiddleware(1)

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(nextHandler)

	claims := jwt.MapClaims{
		"sub": "test-user",
		"exp": time.Now().Add(time.Hour).Unix(),
	}

	// Make two requests to exceed limit
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/api/v1/jobs", nil)
		ctx := auth.ContextWithClaims(req.Context(), claims)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()
		wrappedHandler.ServeHTTP(w, req)

		if i == 1 { // Second request should be rate limited
			if w.Code != http.StatusTooManyRequests {
				t.Errorf("expected status 429, got %d", w.Code)
			}
			if w.Header().Get("Retry-After") != "1" {
				t.Errorf("expected Retry-After header '1', got %q", w.Header().Get("Retry-After"))
			}
		}
	}
}

func TestPerUserRateLimitMiddleware_HandlesInvalidSubjectGracefully(t *testing.T) {
	middleware := PerUserRateLimitMiddleware(1)

	called := false
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(nextHandler)

	// Claims without 'sub' field
	invalidClaims := jwt.MapClaims{
		"exp": time.Now().Add(time.Hour).Unix(),
	}

	req := httptest.NewRequest("GET", "/api/v1/jobs", nil)
	ctx := auth.ContextWithClaims(req.Context(), invalidClaims)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if !called {
		t.Fatal("next handler should be called when subject extraction fails")
	}
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}
