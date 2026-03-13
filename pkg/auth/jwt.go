package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc"
	"github.com/golang-jwt/jwt/v4"
)

type JWTValidator struct {
	jwks     *keyfunc.JWKS
	issuer   string
	audience string
}

func NewJWTValidator(jwksURL string, issuer string, audience string) (*JWTValidator, error) {
	options := keyfunc.Options{
		Ctx: context.Background(),
	}

	jwks, err := keyfunc.Get(jwksURL, options)
	if err != nil {
		return nil, err
	}

	return &JWTValidator{
		jwks:     jwks,
		issuer:   issuer,
		audience: audience,
	}, nil
}

func (v *JWTValidator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "authorization header missing", http.StatusUnauthorized)
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			http.Error(w, "invalid authorization header format", http.StatusUnauthorized)
			return
		}

		tokenString := parts[1]

		token, err := jwt.Parse(tokenString, v.jwks.Keyfunc)
		if err != nil {
			http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
			return
		}

		if !token.Valid {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			http.Error(w, "invalid token claims", http.StatusUnauthorized)
			return
		}

		// Verify Issuer
		if !claims.VerifyIssuer(v.issuer, true) {
			http.Error(w, "invalid issuer", http.StatusUnauthorized)
			return
		}

		// Verify Audience
		if !claims.VerifyAudience(v.audience, true) {
			http.Error(w, "invalid audience", http.StatusUnauthorized)
			return
		}

		// Set claims in context so downstream handlers can extract them (e.g. via GetRoles)
		ctx := context.WithValue(r.Context(), "claims", claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRoles extracts the roles from the JWT claims in the request context.
func GetRoles(r *http.Request) ([]string, error) {
	claims, ok := r.Context().Value("claims").(jwt.MapClaims)
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
		roles[i] = role.(string)
	}

	return roles, nil
}

// RequireRole returns a middleware handler that verifies the user has the specified role.
// It wraps the provided next handler with JWT validation automatically.
func RequireRole(role string, validator *JWTValidator, next http.Handler) http.Handler {
	return validator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		roles, err := GetRoles(r)
		if err != nil {
			http.Error(w, "forbidden: "+err.Error(), http.StatusForbidden)
			return
		}

		hasRole := containsRole(roles, role)

		if !hasRole {
			http.Error(w, "forbidden: required role missing", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	}))
}

// RequireAnyRole returns a middleware handler that verifies the user has at least one
// of the specified roles. It wraps the provided next handler with JWT validation.
func RequireAnyRole(requiredRoles []string, validator *JWTValidator, next http.Handler) http.Handler {
	return validator.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		roles, err := GetRoles(r)
		if err != nil {
			http.Error(w, "forbidden: "+err.Error(), http.StatusForbidden)
			return
		}

		hasRole := false
		for _, requiredRole := range requiredRoles {
			if containsRole(roles, requiredRole) {
				hasRole = true
				break
			}
		}

		if !hasRole {
			http.Error(w, "forbidden: required role missing", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	}))
}

func containsRole(roles []string, expected string) bool {
	for _, role := range roles {
		if role == expected {
			return true
		}
	}

	return false
}
