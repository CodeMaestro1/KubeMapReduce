package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func TestGetRoles_ValidClaims(t *testing.T) {
	claims := jwt.MapClaims{
		"realm_access": map[string]interface{}{
			"roles": []interface{}{"ADMIN", "USER"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), claimsKey, claims)
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
	ctx := context.WithValue(req.Context(), claimsKey, claims)
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
	ctx := context.WithValue(req.Context(), claimsKey, claims)
	req = req.WithContext(ctx)

	_, err := GetRoles(req)
	if err == nil {
		t.Fatal("expected error when roles key missing from realm_access")
	}
}

func TestGetRoles_MalformedRoleEntry(t *testing.T) {
	claims := jwt.MapClaims{
		"realm_access": map[string]interface{}{
			"roles": []interface{}{"ADMIN", 123},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), claimsKey, claims)
	req = req.WithContext(ctx)

	_, err := GetRoles(req)
	if err == nil {
		t.Fatal("expected error when role entry is not a string")
	}
	if !errors.Is(err, ErrMalformedRoles) {
		t.Fatalf("expected ErrMalformedRoles, got %v", err)
	}
}

func TestRequireRoles_MalformedRoleEntryReturnsServiceUnavailable(t *testing.T) {
	claims := jwt.MapClaims{
		"realm_access": map[string]interface{}{
			"roles": []interface{}{"ADMIN", 123},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), claimsKey, claims)
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})

	requireRoles(rr, req, []string{"ADMIN"}, false, next)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, rr.Code)
	}
	if nextCalled {
		t.Fatal("expected next handler not to be called on malformed roles")
	}
}

func TestRequireRoles_MissingRequiredRoleReturnsForbidden(t *testing.T) {
	claims := jwt.MapClaims{
		"realm_access": map[string]interface{}{
			"roles": []interface{}{"USER"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), claimsKey, claims)
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	requireRoles(rr, req, []string{"ADMIN"}, false, next)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, rr.Code)
	}
}

func TestRequireRoles_NoClaims_ReturnsGenericForbidden(t *testing.T) {
	// Request without any claims in context triggers the error path.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be called")
	})

	requireRoles(rr, req, []string{"USER"}, false, next)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, rr.Code)
	}
	body := strings.TrimSpace(rr.Body.String())
	if body != "forbidden: insufficient permissions" {
		t.Fatalf("expected stable error message, got %q", body)
	}
	// Ensure no internal details leak.
	for _, leak := range []string{"no claims", "no realm_access", "no roles", "context"} {
		if strings.Contains(strings.ToLower(body), leak) {
			t.Fatalf("response body leaks internals: %q", body)
		}
	}
}

func TestRequireRoles_NoRealmAccess_ReturnsGenericForbidden(t *testing.T) {
	claims := jwt.MapClaims{"sub": "user-123"}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), claimsKey, claims)
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be called")
	})

	requireRoles(rr, req, []string{"USER"}, false, next)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, rr.Code)
	}
	body := strings.TrimSpace(rr.Body.String())
	if body != "forbidden: insufficient permissions" {
		t.Fatalf("expected stable error message, got %q", body)
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
