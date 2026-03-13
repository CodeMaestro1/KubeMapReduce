package main

import (
	"bytes"
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

// ── token management ───────────────────────────────────────

func getValidToken() (token string, serverURL string) {
	tokens, err := auth.LoadTokens()
	if err != nil {
		log.Fatalf("%v\nRun 'kubemapreduce login' first.", err)
	}

	if !tokens.IsAccessExpired() {
		return tokens.AccessToken, tokens.ServerURL
	}

	// Access token expired — try refreshing with the refresh token.
	tokenResp, err := auth.RefreshTokens(
		keycloakBaseURL(), keycloakRealm(), keycloakClientID(), tokens.RefreshToken,
	)
	if err != nil {
		log.Fatalf("session expired, please login again: %v", err)
	}

	tokens.AccessToken = tokenResp.AccessToken
	tokens.RefreshToken = tokenResp.RefreshToken
	tokens.ExpiresAt = time.Now().Unix() + int64(tokenResp.ExpiresIn)

	if err := auth.SaveTokens(tokens); err != nil {
		log.Fatalf("failed to update credentials: %v", err)
	}

	return tokens.AccessToken, tokens.ServerURL
}

// ── HTTP helpers ───────────────────────────────────────────

func doAuthRequest(method, reqURL, token string, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, reqURL, reader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return http.DefaultClient.Do(req)
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
