package auth

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// contextKey is an unexported type used for context keys in this package,
// preventing collisions with keys defined in other packages.
type contextKey string

// claimsKey is the key used to store [jwt.MapClaims] in the request context.
// Exported via helper functions rather than directly to maintain encapsulation.
const claimsKey contextKey = "claims"

// JWTValidator manages the validation of OpenID Connect (OIDC) Bearer tokens.
//
// It caches the public keys (JWKS) from the identity provider to perform
// local, cryptographic signature verification without per-request network calls.
type JWTValidator struct {
	jwks     keyfunc.Keyfunc
	issuer   string
	audience string
}

// NewJWTValidator initializes a [JWTValidator] by fetching the JWKS from the
// specified URL.
//
// This is normally called once during service bootstrap. If the JWKS cannot
// be reached, it returns an error, preventing the service from starting in an
// insecure or non-functional state.
func NewJWTValidator(jwksURL string, issuer string, audience string) (*JWTValidator, error) {
	jwks, err := keyfunc.NewDefaultCtx(context.Background(), []string{jwksURL})
	if err != nil {
		return nil, err
	}

	return &JWTValidator{
		jwks:     jwks,
		issuer:   issuer,
		audience: audience,
	}, nil
}

// Middleware returns an [http.Handler] that validates the Authorization header.
//
// It expects a "Bearer <token>" format. If valid, it extracts the claims and
// injects them into the request context using [claimsKey]. If invalid or missing,
// it terminates the request with 401 Unauthorized.
//
// This middleware ensures that downstream handlers can assume a valid,
// authenticated identity is present in the context.
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

		token, err := jwt.ParseWithClaims(
			tokenString,
			jwt.MapClaims{},
			v.jwks.Keyfunc,
			jwt.WithIssuer(v.issuer),
			jwt.WithAudience(v.audience),
			jwt.WithValidMethods([]string{"RS256"}),
			jwt.WithLeeway(0), // Removed leeway to prevent slightly expired tokens from being accepted
		)
		if err != nil {
			slog.Warn("JWT validation failed", "error", err)
			http.Error(w, "invalid token", http.StatusUnauthorized)
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

		// Set claims in context so downstream handlers can extract them (e.g. via GetRoles)
		ctx := context.WithValue(r.Context(), claimsKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
