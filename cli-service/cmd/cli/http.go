package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"kubemapreduce/auth-service/pkg/auth"
)

// ── environment helpers ────────────────────────────────────

func getEnv(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func apiURL() string           { return getEnv("API_URL", "http://localhost:8081") }
func keycloakBaseURL() string  { return getEnv("KEYCLOAK_BASE_URL", "http://localhost:8080") }
func keycloakRealm() string    { return getEnv("KEYCLOAK_REALM", "mapreduce") }
func keycloakClientID() string { return getEnv("KEYCLOAK_AUDIENCE", "mapreduce-api") }

const cliRequestTimeout = 30 * time.Second
const maxCLIResponseBodyBytes int64 = 4 << 20

var cliHTTPClient = &http.Client{Timeout: cliRequestTimeout}
var loadStoredTokens = auth.LoadTokens
var saveStoredTokens = auth.SaveTokens
var refreshStoredTokens = auth.RefreshTokensWithContext

func cliRequestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), cliRequestTimeout)
}

// ── token management ───────────────────────────────────────

// resolveServerURL returns a deterministic API base URL.
// Precedence: stored credentials server_url, then API_URL env/default.
func resolveServerURL(tokens *auth.StoredTokens) (string, bool) {
	if tokens == nil {
		return apiURL(), false
	}

	storedServerURL := strings.TrimSpace(tokens.ServerURL)
	if storedServerURL != "" {
		return storedServerURL, false
	}

	tokens.ServerURL = apiURL()
	return tokens.ServerURL, true
}

// getValidToken retrieves a valid access token, refreshing it if necessary.
//
// It encapsulates the token lifecycle management for the CLI. If the stored access
// token is expired, it automatically attempts a refresh using the refresh token
// against Keycloak. This ensures that the user remains authenticated during
// long-running operations or multiple sequential commands without manual re-login.
// It also returns the resolved server URL associated with the session.
func getValidToken() (token string, serverURL string) {
	tokens, err := loadStoredTokens()
	if err != nil {
		log.Fatalf("%v\nRun 'kubemapreduce login' first.", err)
	}

	resolvedServerURL, migratedServerURL := resolveServerURL(tokens)
	if migratedServerURL {
		if err := saveStoredTokens(tokens); err != nil {
			log.Printf("warning: failed to persist resolved server_url; continuing with fallback API URL: %v", err)
		}
	}

	if !tokens.IsAccessExpired() {
		return tokens.AccessToken, resolvedServerURL
	}

	// Access token expired — try refreshing with the refresh token.
	ctx, cancel := cliRequestContext()
	defer cancel()

	// Use stored Keycloak URL if available (saved during login), falling back to env var.
	kcBaseURL := tokens.KeycloakBaseURL
	if kcBaseURL == "" {
		kcBaseURL = keycloakBaseURL()
	}
	tokenResp, err := refreshStoredTokens(
		ctx,
		cliHTTPClient,
		kcBaseURL,
		keycloakRealm(),
		keycloakClientID(),
		tokens.RefreshToken,
	)
	if err != nil {
		log.Fatalf("session expired, please login again: %v", err)
	}

	tokens.AccessToken = tokenResp.AccessToken
	tokens.RefreshToken = tokenResp.RefreshToken
	tokens.ExpiresAt = time.Now().Unix() + int64(tokenResp.ExpiresIn)
	tokens.ServerURL = resolvedServerURL
	tokens.KeycloakBaseURL = kcBaseURL

	if err := saveStoredTokens(tokens); err != nil {
		log.Fatalf("failed to update credentials: %v", err)
	}

	return tokens.AccessToken, resolvedServerURL
}

// ── HTTP helpers ───────────────────────────────────────────

// doAuthRequest executes an authenticated HTTP request using the provided token.
//
// It provides a high-level wrapper around [doAuthRequestWithContext], using
// context.Background so the caller can read resp.Body without the context
// being canceled early. Request-level timeout is enforced by cliHTTPClient.Timeout.
func doAuthRequest(method, reqURL, token string, body []byte) (*http.Response, error) {
	return doAuthRequestWithContext(context.Background(), method, reqURL, token, body)
}

// doAuthRequestWithContext executes an authenticated HTTP request with a specific context.
//
// This function enforces security invariants: it ensures the token is non-empty,
// restricts requests to HTTP/HTTPS schemes to prevent credential leakage to
// local files or other protocols, and sets the Bearer Authorization header.
func doAuthRequestWithContext(ctx context.Context, method, reqURL, token string, body []byte) (*http.Response, error) {
	trimmedToken := strings.TrimSpace(token)
	if trimmedToken == "" {
		// Guard against leaking malformed Authorization headers when credentials are missing/corrupt.
		return nil, fmt.Errorf("missing access token")
	}

	parsedURL, err := url.Parse(reqURL)
	if err != nil {
		return nil, fmt.Errorf("invalid request URL: %w", err)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		// Restrict outbound authenticated requests to HTTP(S) only to avoid credential misuse.
		return nil, fmt.Errorf("unsupported URL scheme %q", parsedURL.Scheme)
	}
	if strings.TrimSpace(parsedURL.Host) == "" {
		return nil, fmt.Errorf("invalid request URL: missing host")
	}

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, reader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+trimmedToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return cliHTTPClient.Do(req)
}

// doAuthRequestExpect executes an authenticated request and fails if the status code is unexpected.
//
// This is a "fail-fast" helper used by CLI commands that expect a specific
// success code (e.g., 201 Created for user creation). It ensures that the CLI
// provides immediate and clear feedback to the user when an operation fails
// at the API level.
func doAuthRequestExpect(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
	resp, err := doAuthRequest(method, reqURL, token, body)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}

	if resp.StatusCode != expectedStatus {
		defer resp.Body.Close()
		respBody, readErr := readResponseBody(resp.Body)
		if readErr != nil {
			log.Fatalf("%s (HTTP %d): failed to read response body: %v", failPrefix, resp.StatusCode, readErr)
		}
		log.Fatalf("%s (HTTP %d): %s", failPrefix, resp.StatusCode, string(respBody))
	}

	return resp
}

// printResponse reads and prints the response body, formatting it as JSON if possible.
func printResponse(resp *http.Response) {
	body, err := readResponseBody(resp.Body)
	if err != nil {
		log.Fatalf("failed to read response body: %v", err)
	}

	var buf bytes.Buffer
	if json.Indent(&buf, body, "", "  ") == nil {
		fmt.Println(buf.String())
	} else {
		fmt.Print(string(body))
	}
}

// readResponseBody reads the response body up to a predefined limit.
//
// This limit prevents the CLI from being overwhelmed by excessively large
// response bodies (e.g., a multi-gigabyte log file returned by mistake),
// protecting the system's memory integrity.
func readResponseBody(body io.Reader) ([]byte, error) {
	// Cap body reads so large/malicious responses cannot exhaust CLI memory.
	payload, err := io.ReadAll(io.LimitReader(body, maxCLIResponseBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > maxCLIResponseBodyBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxCLIResponseBodyBytes)
	}
	return payload, nil
}
