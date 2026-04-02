package auth

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/golang-jwt/jwt/v5"
)

var ErrMalformedRoles = errors.New("malformed realm_access.roles")

// GetRoles extracts the realm roles from the JWT claims stored in the request
// context by the JWTValidator middleware.
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

// RequireRole returns a middleware handler that verifies the user has the specified role.
// It wraps the provided next handler with JWT validation automatically.
func RequireRole(role string, validator *JWTValidator, next http.Handler) http.Handler {
	return validator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireRoles(w, r, []string{role}, false, next)
	}))
}

// RequireAnyRole returns a middleware handler that verifies the user has at least one
// of the specified roles. It wraps the provided next handler with JWT validation.
func RequireAnyRole(requiredRoles []string, validator *JWTValidator, next http.Handler) http.Handler {
	return validator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireRoles(w, r, requiredRoles, true, next)
	}))
}

func requireRoles(w http.ResponseWriter, r *http.Request, requiredRoles []string, anyMatch bool, next http.Handler) {
	roles, err := GetRoles(r)
	if err != nil {
		if errors.Is(err, ErrMalformedRoles) {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "forbidden: "+err.Error(), http.StatusForbidden)
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

func containsRole(roles []string, expected string) bool {
	for _, role := range roles {
		if role == expected {
			return true
		}
	}

	return false
}
