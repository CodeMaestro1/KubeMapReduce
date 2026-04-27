package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"kubemapreduce/auth-service/pkg/auth"
)

// ── whoami ─────────────────────────────────────────────────

// cmdWhoAmI displays information about the currently authenticated user.
//
// It extracts and prints the username, email, subject (UUID), and realm roles
// from the locally stored JWT access token. This allows users to verify their
// current identity and permissions without making a network request to the
// auth server.
func cmdWhoAmI() {
	tokens, claims := loadTokensAndClaims("not logged in — run 'kubemapreduce login' first", "failed to read token")

	username, _ := claims["preferred_username"].(string)
	email, _ := claims["email"].(string)
	sub, _ := claims["sub"].(string)

	roles := extractRealmRoles(claims)

	fmt.Printf("Username: %s\n", username)
	if email != "" {
		fmt.Printf("Email:    %s\n", email)
	}
	fmt.Printf("Subject:  %s\n", sub)
	if len(roles) > 0 {
		fmt.Printf("Roles:    %s\n", strings.Join(roles, ", "))
	}

	if tokens.IsAccessExpired() {
		fmt.Println("\nAccess token is expired — it will be refreshed on the next API call.")
	}
}

// ── token inspect ──────────────────────────────────────────

// cmdTokenInspect displays the raw, decoded JWT claims for the current access token.
//
// This is a diagnostic command used to inspect the full set of claims provided
// by Keycloak. It is particularly useful for debugging role assignments,
// audience mismatches, or token expiration issues.
func cmdTokenInspect() {
	tokens, claims := loadTokensAndClaims("not logged in — run 'kubemapreduce login' first", "failed to decode token")

	formatted, _ := json.MarshalIndent(claims, "", "  ")
	fmt.Println(string(formatted))

	// Print a quick summary of key fields.
	fmt.Println("\n--- Summary ---")
	fmt.Printf("  iss: %v\n", claims["iss"])
	fmt.Printf("  aud: %v\n", claims["aud"])
	fmt.Printf("  sub: %v\n", claims["sub"])
	fmt.Printf("  preferred_username: %v\n", claims["preferred_username"])
	if ra, ok := claims["realm_access"].(map[string]any); ok {
		fmt.Printf("  realm_access.roles: %v\n", ra["roles"])
	}

	if tokens.IsAccessExpired() {
		fmt.Println("  ⚠ access token is EXPIRED")
	} else {
		fmt.Println("  ✓ access token is valid")
	}
}

// ── helpers ────────────────────────────────────────────────

// loadTokensAndClaims loads stored tokens and decodes their claims.
// If loading or decoding fails, it logs a fatal error with one of the provided messages.
func loadTokensAndClaims(notLoggedInMsg, decodeFailMsg string) (*auth.StoredTokens, map[string]any) {
	tokens, err := loadStoredTokens()
	if err != nil {
		log.Fatal(notLoggedInMsg)
	}

	claims, err := decodeTokenClaims(tokens.AccessToken)
	if err != nil {
		log.Fatalf("%s: %v", decodeFailMsg, err)
	}

	return tokens, claims
}

// decodeTokenClaims decodes the JWT payload without verification.
//
// This function performs a manual base64 decoding of the JWT's second part.
// It is intended for informational display purposes in the CLI; actual security
// verification of the token is performed by the API server and the auth-service
// using public keys from Keycloak.
func decodeTokenClaims(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("not a valid JWT")
	}

	payload := parts[1]
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil, err
	}

	var claims map[string]any
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil, err
	}
	return claims, nil
}

func extractRealmRoles(claims map[string]any) []string {
	var roles []string
	ra, ok := claims["realm_access"].(map[string]any)
	if !ok {
		return roles
	}

	r, ok := ra["roles"].([]any)
	if !ok {
		return roles
	}

	for _, v := range r {
		if s, ok := v.(string); ok {
			roles = append(roles, s)
		}
	}

	return roles
}

func hasRealmRole(claims map[string]any, role string) bool {
	targetRole := strings.TrimSpace(role)
	if targetRole == "" {
		return false
	}

	for _, r := range extractRealmRoles(claims) {
		if strings.EqualFold(strings.TrimSpace(r), targetRole) {
			return true
		}
	}
	return false
}

func hasAdminRole(claims map[string]any) bool {
	return hasRealmRole(claims, "ADMIN")
}
