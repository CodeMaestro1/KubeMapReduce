package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"kubemapreduce/pkg/auth"
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

	tokenResp, err := refreshStoredTokens(
		ctx,
		cliHTTPClient,
		keycloakBaseURL(),
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

	if err := saveStoredTokens(tokens); err != nil {
		log.Fatalf("failed to update credentials: %v", err)
	}

	return tokens.AccessToken, resolvedServerURL
}

// ── HTTP helpers ───────────────────────────────────────────

func doAuthRequest(method, reqURL, token string, body []byte) (*http.Response, error) {
	ctx, cancel := cliRequestContext()
	defer cancel()

	return doAuthRequestWithContext(ctx, method, reqURL, token, body)
}

func doAuthRequestWithContext(ctx context.Context, method, reqURL, token string, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, reader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return cliHTTPClient.Do(req)
}

func doAuthRequestExpect(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
	resp, err := doAuthRequest(method, reqURL, token, body)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}

	if resp.StatusCode != expectedStatus {
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		log.Fatalf("%s (HTTP %d): %s", failPrefix, resp.StatusCode, string(respBody))
	}

	return resp
}

func printResponse(resp *http.Response) {
	body, _ := io.ReadAll(resp.Body)

	var buf bytes.Buffer
	if json.Indent(&buf, body, "", "  ") == nil {
		fmt.Println(buf.String())
	} else {
		fmt.Print(string(body))
	}
}
