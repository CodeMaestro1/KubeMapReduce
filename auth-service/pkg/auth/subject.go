package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// ContextWithClaims attaches validated JWT claims to a context for downstream use.
func ContextWithClaims(ctx context.Context, claims jwt.MapClaims) context.Context {
	return context.WithValue(ctx, claimsKey, claims)
}

// GetSubject extracts the JWT subject claim from the request context.
func GetSubject(r *http.Request) (string, error) {
	claims, ok := r.Context().Value(claimsKey).(jwt.MapClaims)
	if !ok {
		return "", fmt.Errorf("no claims in context")
	}

	sub, ok := claims["sub"].(string)
	if !ok {
		return "", fmt.Errorf("no subject claim")
	}

	sub = strings.TrimSpace(sub)
	if sub == "" {
		return "", fmt.Errorf("no subject claim")
	}

	return sub, nil
}
