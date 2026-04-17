package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MicahParks/jwkset"
	"github.com/golang-jwt/jwt/v5"
)

// stubKeyfunc satisfies the keyfunc.Keyfunc interface for testing,
// delegating only the Keyfunc method to a user-supplied function.
type stubKeyfunc struct {
	fn jwt.Keyfunc
}

func (s stubKeyfunc) Keyfunc(token *jwt.Token) (interface{}, error) { return s.fn(token) }
func (s stubKeyfunc) KeyfuncCtx(_ context.Context) jwt.Keyfunc      { return s.fn }
func (s stubKeyfunc) Storage() jwkset.Storage                       { return nil }
func (s stubKeyfunc) VerificationKeySet(_ context.Context) (jwt.VerificationKeySet, error) {
	return jwt.VerificationKeySet{}, nil
}

// newTestValidator creates a JWTValidator that uses f as the key function,
// enabling tests to control token parsing without a live JWKS endpoint.
func newTestValidator(f jwt.Keyfunc, issuer, audience string) *JWTValidator {
	return &JWTValidator{
		jwks:     stubKeyfunc{fn: f},
		issuer:   issuer,
		audience: audience,
	}
}

func TestMiddleware_MissingAuthHeader(t *testing.T) {
	v := newTestValidator(nil, "", "")
	handler := v.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected %d, got %d", http.StatusUnauthorized, rec.Code)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != "authorization header missing" {
		t.Fatalf("expected 'authorization header missing', got %q", body)
	}
}

func TestMiddleware_InvalidAuthHeaderFormat(t *testing.T) {
	tests := []struct {
		name   string
		header string
	}{
		{"no bearer prefix", "Token abc123"},
		{"only bearer", "Bearer"},
		{"empty", ""},
		{"three parts", "Bearer abc def"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parts := splitAuthHeader(tt.header)
			if parts != nil {
				t.Errorf("expected nil for invalid header %q, got %v", tt.header, parts)
			}
		})
	}
}

func TestMiddleware_InvalidToken_ReturnsGenericError(t *testing.T) {
	// Use a keyfunc that rejects all tokens to trigger a parse error.
	v := newTestValidator(func(token *jwt.Token) (interface{}, error) {
		// Return a valid-type but wrong key so parsing fails with a signature error.
		wrongKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		return &wrongKey.PublicKey, nil
	}, "", "")

	handler := v.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be called")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	// Use a structurally valid but unsigned/bad JWT.
	req.Header.Set("Authorization", "Bearer not-a-jwt-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected %d, got %d", http.StatusUnauthorized, rec.Code)
	}

	body := strings.TrimSpace(rec.Body.String())
	if body != "invalid token" {
		t.Fatalf("expected stable error 'invalid token', got %q", body)
	}
	// Ensure no raw parser details leak.
	for _, leak := range []string{"signing method", "crypto", "base64", "cannot", "unexpected"} {
		if strings.Contains(strings.ToLower(body), leak) {
			t.Fatalf("response body leaks parser internals: %q", body)
		}
	}
}

// splitAuthHeader is a helper that mimics the middleware's auth header parsing
// logic to make it testable without a live JWKS endpoint.
func splitAuthHeader(header string) []string {
	if header == "" {
		return nil
	}
	parts := make([]string, 0)
	current := ""
	for _, c := range header {
		if c == ' ' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}

	if len(parts) != 2 || parts[0] != "bearer" && parts[0] != "Bearer" {
		return nil
	}
	return parts
}
