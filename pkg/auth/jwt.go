package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc"
	"github.com/golang-jwt/jwt/v4"
)

// contextKey is an unexported type used for context keys in this package,
// preventing collisions with keys defined in other packages.
type contextKey string

const claimsKey contextKey = "claims"

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
		ctx := context.WithValue(r.Context(), claimsKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
