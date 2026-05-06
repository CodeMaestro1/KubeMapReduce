package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"kubemapreduce/auth-service/pkg/auth"
)

func TestResolveServerURL_StoredValueWins(t *testing.T) {
	tokens := &auth.StoredTokens{ServerURL: "http://stored.example:8081"}
	resolved, migrated := resolveServerURL(tokens)

	if resolved != "http://stored.example:8081" {
		t.Fatalf("expected stored server URL to be used, got %q", resolved)
	}
	if migrated {
		t.Fatal("expected no migration when stored server_url exists")
	}
}

func TestResolveServerURL_FallsBackToAPIURLForLegacyTokens(t *testing.T) {
	t.Setenv("API_URL", "http://env.example:9999")
	tokens := &auth.StoredTokens{}
	resolved, migrated := resolveServerURL(tokens)

	if resolved != "http://env.example:9999" {
		t.Fatalf("expected API_URL fallback, got %q", resolved)
	}
	if !migrated {
		t.Fatal("expected migration=true when server_url missing")
	}
	if tokens.ServerURL != "http://env.example:9999" {
		t.Fatalf("expected server_url to be backfilled in tokens, got %q", tokens.ServerURL)
	}
}

func TestGetValidToken_LegacyCredentialsFallbackAndPersist(t *testing.T) {
	originalLoad := loadStoredTokens
	originalSave := saveStoredTokens
	originalRefresh := refreshStoredTokens
	defer func() {
		loadStoredTokens = originalLoad
		saveStoredTokens = originalSave
		refreshStoredTokens = originalRefresh
	}()

	t.Setenv("API_URL", "http://fallback.example:8081")

	loadStoredTokens = func() (*auth.StoredTokens, error) {
		return &auth.StoredTokens{
			AccessToken:  "legacy-access",
			RefreshToken: "legacy-refresh",
			ExpiresAt:    time.Now().Unix() + 600,
			ServerURL:    "",
		}, nil
	}

	refreshCalled := false
	refreshStoredTokens = func(ctx context.Context, client *http.Client, keycloakBaseURL, realm, clientID, refreshToken string) (*auth.OAuthTokenResponse, error) {
		refreshCalled = true
		return &auth.OAuthTokenResponse{}, nil
	}

	saveCalled := false
	var saved *auth.StoredTokens
	saveStoredTokens = func(tokens *auth.StoredTokens) error {
		saveCalled = true
		snapshot := *tokens
		saved = &snapshot
		return nil
	}

	token, serverURL := getValidToken()
	if token != "legacy-access" {
		t.Fatalf("expected existing access token, got %q", token)
	}
	if serverURL != "http://fallback.example:8081" {
		t.Fatalf("expected fallback server URL, got %q", serverURL)
	}
	if !saveCalled {
		t.Fatal("expected migrated server_url to be persisted")
	}
	if saved == nil || saved.ServerURL != "http://fallback.example:8081" {
		t.Fatalf("expected persisted server_url fallback, got %+v", saved)
	}
	if refreshCalled {
		t.Fatal("did not expect refresh for non-expired token")
	}
}

func TestGetValidToken_NewCredentialsUseStoredServerURL(t *testing.T) {
	originalLoad := loadStoredTokens
	originalSave := saveStoredTokens
	originalRefresh := refreshStoredTokens
	defer func() {
		loadStoredTokens = originalLoad
		saveStoredTokens = originalSave
		refreshStoredTokens = originalRefresh
	}()

	loadStoredTokens = func() (*auth.StoredTokens, error) {
		return &auth.StoredTokens{
			AccessToken:  "new-access",
			RefreshToken: "new-refresh",
			ExpiresAt:    time.Now().Unix() + 600,
			ServerURL:    "http://stored.example:8081",
		}, nil
	}

	saveCalled := false
	saveStoredTokens = func(tokens *auth.StoredTokens) error {
		saveCalled = true
		return nil
	}

	token, serverURL := getValidToken()
	if token != "new-access" {
		t.Fatalf("expected existing access token, got %q", token)
	}
	if serverURL != "http://stored.example:8081" {
		t.Fatalf("expected stored server URL, got %q", serverURL)
	}
	if saveCalled {
		t.Fatal("did not expect save when server_url already present and token not expired")
	}
}

func TestDoAuthRequestWithContext_CanceledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := doAuthRequestWithContext(ctx, http.MethodGet, server.URL, "token", nil)
	if err == nil {
		if resp != nil {
			resp.Body.Close()
		}
		t.Fatal("expected error for canceled request context")
	}
}

func TestDoAuthRequestWithContext_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block longer than the context deadline.
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	resp, err := doAuthRequestWithContext(ctx, http.MethodGet, server.URL, "token", nil)
	if err == nil {
		if resp != nil {
			resp.Body.Close()
		}
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("expected context deadline exceeded error, got: %v", err)
	}
}

func TestDoAuthRequestWithContext_SetsHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("expected Authorization header 'Bearer test-token', got %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := doAuthRequestWithContext(ctx, http.MethodPost, server.URL, "test-token", []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()
}

func TestDoAuthRequestWithContext_RejectsMissingToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := doAuthRequestWithContext(ctx, http.MethodGet, "http://example.test/jobs", "   ", nil)
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if !strings.Contains(err.Error(), "missing access token") {
		t.Fatalf("expected missing token error, got: %v", err)
	}
}

func TestDoAuthRequestWithContext_RejectsNonHTTPSchemes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := doAuthRequestWithContext(ctx, http.MethodGet, "ftp://example.test/jobs", "token", nil)
	if err == nil {
		t.Fatal("expected error for unsupported URL scheme")
	}
	if !strings.Contains(err.Error(), "unsupported URL scheme") {
		t.Fatalf("expected unsupported scheme error, got: %v", err)
	}
}

func TestCliRequestContext_HasBoundedDeadline(t *testing.T) {
	ctx, cancel := cliRequestContext()
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected cliRequestContext to set a deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > cliRequestTimeout+time.Second {
		t.Fatalf("expected deadline within %v, got %v remaining", cliRequestTimeout, remaining)
	}
}

func TestCliHTTPClient_HasTimeout(t *testing.T) {
	if cliHTTPClient.Timeout != cliRequestTimeout {
		t.Fatalf("expected cliHTTPClient.Timeout=%v, got %v", cliRequestTimeout, cliHTTPClient.Timeout)
	}
}

func TestGetValidToken_RefreshLogic(t *testing.T) {
	// Setup overrides for test isolation
	originalLoad := loadStoredTokens
	originalSave := saveStoredTokens
	originalRefresh := refreshStoredTokens
	defer func() {
		loadStoredTokens = originalLoad
		saveStoredTokens = originalSave
		refreshStoredTokens = originalRefresh
	}()

	t.Run("ValidTokenReturnsImmediately", func(t *testing.T) {
		loadStoredTokens = func() (*auth.StoredTokens, error) {
			return &auth.StoredTokens{
				AccessToken: "valid-token",
				ExpiresAt:   time.Now().Add(10 * time.Minute).Unix(),
				ServerURL:   "http://test.example",
			}, nil
		}

		refreshCalled := false
		refreshStoredTokens = func(ctx context.Context, client *http.Client, baseURL, realm, clientID, refreshToken string) (*auth.OAuthTokenResponse, error) {
			refreshCalled = true
			return nil, nil
		}

		token, url := getValidToken()
		if token != "valid-token" {
			t.Errorf("expected valid-token, got %q", token)
		}
		if url != "http://test.example" {
			t.Errorf("expected http://test.example, got %q", url)
		}
		if refreshCalled {
			t.Errorf("expected refresh not to be called for valid token")
		}
	})
}

func TestDoAuthRequestWithContext_ValidatesTokenAndURL(t *testing.T) {
	ctx := context.Background()

	_, err := doAuthRequestWithContext(ctx, "GET", "http://example.com", "", nil)
	if err == nil || !strings.Contains(err.Error(), "missing access token") {
		t.Errorf("expected missing token error, got %v", err)
	}

	_, err = doAuthRequestWithContext(ctx, "GET", "file:///etc/passwd", "token", nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported URL scheme") {
		t.Errorf("expected unsupported URL scheme error, got %v", err)
	}

	_, err = doAuthRequestWithContext(ctx, "GET", "http://", "token", nil)
	if err == nil || !strings.Contains(err.Error(), "missing host") {
		t.Errorf("expected missing host error, got %v", err)
	}
}

func TestDoAuthRequestWithContext_SendsBearerToken(t *testing.T) {
	var capturedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx := context.Background()
	_, err := doAuthRequestWithContext(ctx, "GET", server.URL, "my-test-token", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedAuth != "Bearer my-test-token" {
		t.Errorf("expected Bearer my-test-token, got %q", capturedAuth)
	}
}
func TestGetValidToken_RefreshLogicExpired(t *testing.T) {
	// Setup overrides for test isolation
	originalLoad := loadStoredTokens
	originalSave := saveStoredTokens
	originalRefresh := refreshStoredTokens
	defer func() {
		loadStoredTokens = originalLoad
		saveStoredTokens = originalSave
		refreshStoredTokens = originalRefresh
	}()

	loadStoredTokens = func() (*auth.StoredTokens, error) {
		return &auth.StoredTokens{
			AccessToken:  "expired-token",
			RefreshToken: "valid-refresh-token",
			ExpiresAt:    time.Now().Add(-10 * time.Minute).Unix(),
			ServerURL:    "http://test.example",
		}, nil
	}

	saveCalled := false
	saveStoredTokens = func(tokens *auth.StoredTokens) error {
		saveCalled = true
		return nil
	}

	refreshCalled := false
	refreshStoredTokens = func(ctx context.Context, client *http.Client, baseURL, realm, clientID, refreshToken string) (*auth.OAuthTokenResponse, error) {
		refreshCalled = true
		return &auth.OAuthTokenResponse{
			AccessToken:  "new-valid-token",
			RefreshToken: "new-refresh-token",
			ExpiresIn:    300,
		}, nil
	}

	token, url := getValidToken()
	if token != "new-valid-token" {
		t.Errorf("expected new-valid-token, got %q", token)
	}
	if url != "http://test.example" {
		t.Errorf("expected http://test.example, got %q", url)
	}
	if !refreshCalled {
		t.Errorf("expected refresh to be called for expired token")
	}
	if !saveCalled {
		t.Errorf("expected save to be called after refresh")
	}
}
