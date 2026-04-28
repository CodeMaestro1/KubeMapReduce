package auth

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/golang-jwt/jwt/v5"
)

// ErrMalformedRoles indicates that the 'realm_access.roles' claim is present
// but does not match the expected structure (a list of strings).
//
// This is an internal configuration or schema issue with the identity provider.
var ErrMalformedRoles = errors.New("malformed realm_access.roles")

// GetRoles extracts the realm roles from the JWT claims stored in the request
// context.
//
// It expects the claims to be populated by the [JWTValidator.Middleware].
// If no claims are found, it returns an error. The roles are expected to be
// in the 'realm_access.roles' path, which is standard for Keycloak.
func GetRoles(r *http.Request) ([]string, error) {
	claims, ok := r.Context().Value(claimsKey).(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("no claims in context")
	}

	realmAccess, ok := claims["realm_access"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("no realm_access")
	}

	rawRoles, ok := realmAccess["roles"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("no roles")
	}

	roles := make([]string, len(rawRoles))
	for i, role := range rawRoles {
		roleStr, ok := role.(string)
		if !ok {
			return nil, fmt.Errorf("%w: role[%d] is %T", ErrMalformedRoles, i, role)
		}
		roles[i] = roleStr
	}

	return roles, nil
}

// RequireRole returns a middleware handler that verifies the user has the
// specified role.
//
// It implicitly includes [JWTValidator.Middleware] to ensure the token is
// validated before checking roles. If the role is missing, it returns
// 403 Forbidden.
func RequireRole(role string, validator *JWTValidator, next http.Handler) http.Handler {
	return validator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireRoles(w, r, []string{role}, false, next)
	}))
}

// RequireAnyRole returns a middleware handler that verifies the user has at
// least one of the specified roles.
//
// Like [RequireRole], it wraps the validator's middleware. This is useful for
// endpoints that can be accessed by multiple tiers of users (e.g. USER or ADMIN).
func RequireAnyRole(requiredRoles []string, validator *JWTValidator, next http.Handler) http.Handler {
	return validator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireRoles(w, r, requiredRoles, true, next)
	}))
}

// requireRoles is the core logic for role-based access control.
// It checks the current request's roles against a required set.
func requireRoles(w http.ResponseWriter, r *http.Request, requiredRoles []string, anyMatch bool, next http.Handler) {
	roles, err := GetRoles(r)
	if err != nil {
		if errors.Is(err, ErrMalformedRoles) {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		slog.Warn("role extraction failed", "error", err)
		http.Error(w, "forbidden: insufficient permissions", http.StatusForbidden)
		return
	}

	hasRole := !anyMatch
	for _, requiredRole := range requiredRoles {
		if anyMatch {
			if containsRole(roles, requiredRole) {
				hasRole = true
				break
			}
			continue
		}

		if !containsRole(roles, requiredRole) {
			hasRole = false
			break
		}
	}

	if !hasRole {
		http.Error(w, "forbidden: required role missing", http.StatusForbidden)
		return
	}

	next.ServeHTTP(w, r)
}

// containsRole checks if a slice of roles contains a specific role name.
func containsRole(roles []string, expected string) bool {
	for _, role := range roles {
		if role == expected {
			return true
		}
	}

	return false
}

// HasRole reports whether the request's JWT claims include the specified
// realm role.
//
// It returns false (without error) when claims are missing or malformed so
// that callers can use it as a simple predicate for optional privilege
// elevation (for example, granting admin users access to other users' data
// without short-circuiting normal user-scoped authorization).
func HasRole(r *http.Request, role string) bool {
	roles, err := GetRoles(r)
	if err != nil {
		return false
	}
	return containsRole(roles, role)
}

// IsAdmin reports whether the request's JWT claims include the ADMIN realm
// role. It is a convenience wrapper around [HasRole] used by API handlers
// that need to widen access scope for administrators.
func IsAdmin(r *http.Request) bool {
	return HasRole(r, "ADMIN")
}
