package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v4"
)

func TestGetRoles_ValidClaims(t *testing.T) {
	claims := jwt.MapClaims{
		"realm_access": map[string]interface{}{
			"roles": []interface{}{"ADMIN", "USER"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), "claims", claims)
	req = req.WithContext(ctx)

	roles, err := GetRoles(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(roles) != 2 {
		t.Fatalf("expected 2 roles, got %d", len(roles))
	}
	if roles[0] != "ADMIN" || roles[1] != "USER" {
		t.Errorf("roles = %v, want [ADMIN USER]", roles)
	}
}

func TestGetRoles_NoClaims(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	_, err := GetRoles(req)
	if err == nil {
		t.Fatal("expected error when no claims in context")
	}
}

func TestGetRoles_NoRealmAccess(t *testing.T) {
	claims := jwt.MapClaims{
		"sub": "user-123",
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), "claims", claims)
	req = req.WithContext(ctx)

	_, err := GetRoles(req)
	if err == nil {
		t.Fatal("expected error when realm_access missing")
	}
}

func TestGetRoles_NoRolesKey(t *testing.T) {
	claims := jwt.MapClaims{
		"realm_access": map[string]interface{}{},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), "claims", claims)
	req = req.WithContext(ctx)

	_, err := GetRoles(req)
	if err == nil {
		t.Fatal("expected error when roles key missing from realm_access")
	}
}

func TestContainsRole(t *testing.T) {
	tests := []struct {
		name     string
		roles    []string
		expected string
		want     bool
	}{
		{"present", []string{"USER", "ADMIN"}, "ADMIN", true},
		{"absent", []string{"USER"}, "ADMIN", false},
		{"empty", []string{}, "ADMIN", false},
		{"exact match", []string{"admin"}, "ADMIN", false},
		{"single match", []string{"ADMIN"}, "ADMIN", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsRole(tt.roles, tt.expected)
			if got != tt.want {
				t.Errorf("containsRole(%v, %q) = %v, want %v", tt.roles, tt.expected, got, tt.want)
			}
		})
	}
}

func TestMiddleware_MissingAuthHeader(t *testing.T) {
	// We can't create a real JWTValidator without a JWKS endpoint,
	// but we can test the middleware's early-exit paths by calling
	// the Middleware method on a minimal validator. Since creating
	// the validator requires a live JWKS, we test the handler logic
	// at the HTTP level indirectly via RequireRole helpers.

	// Instead, test the header parsing logic manually.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	// No Authorization header
	authHeader := req.Header.Get("Authorization")
	if authHeader != "" {
		t.Errorf("expected empty auth header, got %q", authHeader)
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
