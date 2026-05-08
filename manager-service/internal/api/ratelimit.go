package api

import (
	"net/http"
	"sync"
	"time"

	"kubemapreduce/auth-service/pkg/auth"
	"kubemapreduce/manager-service/pkg/httputil"
)

// TokenBucketLimiter implements a thread-safe token bucket rate limiter.
// It allows at most `capacity` requests per `window` time period.
type TokenBucketLimiter struct {
	mu         sync.Mutex
	tokens     float64 // Current number of tokens
	capacity   float64 // Maximum number of tokens
	refillRate float64 // Tokens added per second
	lastRefill time.Time
}

// NewTokenBucketLimiter creates a rate limiter that allows `requestsPerSecond` requests per second.
// For example, NewTokenBucketLimiter(10) allows 10 requests per second.
func NewTokenBucketLimiter(requestsPerSecond float64) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		tokens:     requestsPerSecond,
		capacity:   requestsPerSecond,
		refillRate: requestsPerSecond,
		lastRefill: time.Now(),
	}
}

// Allow checks if a request can proceed. If allowed, removes a token and returns true.
// If the token bucket is empty, returns false (rate limit exceeded).
func (l *TokenBucketLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	l.lastRefill = now

	// Refill tokens: add (refillRate * elapsed) tokens
	l.tokens += l.refillRate * elapsed
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}

	if l.tokens >= 1.0 {
		l.tokens -= 1.0
		return true
	}
	return false
}

// PerUserRateLimitMiddleware enforces per-user rate limits using the user ID from JWT claims.
// Each user gets their own token bucket with `requestsPerSecond` capacity.
// Requests without valid JWT claims bypass the limit.
func PerUserRateLimitMiddleware(requestsPerSecond float64) func(http.Handler) http.Handler {
	limiters := make(map[string]*TokenBucketLimiter)
	var mu sync.RWMutex

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract user ID from JWT claims using auth package utility
			userID, err := auth.GetSubject(r)
			if err != nil {
				// No valid claims found; allow request (unauthenticated paths)
				next.ServeHTTP(w, r)
				return
			}

			// Get or create per-user limiter
			mu.RLock()
			limiter, exists := limiters[userID]
			mu.RUnlock()

			if !exists {
				limiter = NewTokenBucketLimiter(requestsPerSecond)
				mu.Lock()
				limiters[userID] = limiter
				mu.Unlock()
			}

			if !limiter.Allow() {
				w.Header().Set("Retry-After", "1")
				httputil.WriteErrorJSON(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
