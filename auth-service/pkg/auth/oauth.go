package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultOAuthHTTPTimeout is the default timeout for OAuth network requests.
const DefaultOAuthHTTPTimeout = 30 * time.Second

var defaultOAuthHTTPClient = &http.Client{Timeout: DefaultOAuthHTTPTimeout}

// OAuthTokenResponse is the standard JSON structure returned by the
// OpenID Connect (OIDC) token endpoint.
type OAuthTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

// RequestTokens performs an interactive login via the 'password' grant.
//
// This is used exclusively by the CLI for initial authentication. It handles
// full request-response lifecycle with a default timeout.
func RequestTokens(keycloakBaseURL, realm, clientID, username, password string) (*OAuthTokenResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOAuthHTTPTimeout)
	defer cancel()

	return RequestTokensWithContext(ctx, defaultOAuthHTTPClient, keycloakBaseURL, realm, clientID, username, password)
}

// RequestTokensWithContext performs an interactive login via the 'password' grant.
//
// Like [RequestTokens], but allows for custom contexts (for cancellation)
// and [http.Client] overrides.
func RequestTokensWithContext(ctx context.Context, client *http.Client, keycloakBaseURL, realm, clientID, username, password string) (*OAuthTokenResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		client = defaultOAuthHTTPClient
	}

	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", keycloakBaseURL, realm)

	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", clientID)
	form.Set("username", username)
	form.Set("password", password)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create authentication request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to contact authentication server: %w", err)
	}
	defer resp.Body.Close()

	body, err := readBoundedResponseBody(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("authentication failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var tokenResp OAuthTokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("failed to parse token response: %w", err)
	}

	return &tokenResp, nil
}

// RefreshTokens uses a refresh token to obtain a fresh access token.
//
// This is used by the CLI to maintain a session without re-prompting the user.
func RefreshTokens(keycloakBaseURL, realm, clientID, refreshToken string) (*OAuthTokenResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultOAuthHTTPTimeout)
	defer cancel()

	return RefreshTokensWithContext(ctx, defaultOAuthHTTPClient, keycloakBaseURL, realm, clientID, refreshToken)
}

// RefreshTokensWithContext uses a refresh token to obtain a fresh access token.
//
// Like [RefreshTokens], but allows for custom contexts (for cancellation)
// and [http.Client] overrides.
func RefreshTokensWithContext(ctx context.Context, client *http.Client, keycloakBaseURL, realm, clientID, refreshToken string) (*OAuthTokenResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		client = defaultOAuthHTTPClient
	}

	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", keycloakBaseURL, realm)

	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("client_id", clientID)
	form.Set("refresh_token", refreshToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create token refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to contact authentication server: %w", err)
	}
	defer resp.Body.Close()

	body, err := readBoundedResponseBody(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token refresh failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var tokenResp OAuthTokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("failed to parse token response: %w", err)
	}

	return &tokenResp, nil
}
