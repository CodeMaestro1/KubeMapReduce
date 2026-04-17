package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequestTokens_Success(t *testing.T) {
	expected := OAuthTokenResponse{
		AccessToken:  "access-tok",
		RefreshToken: "refresh-tok",
		ExpiresIn:    300,
		TokenType:    "Bearer",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("failed to parse form: %v", err)
		}
		if r.FormValue("grant_type") != "password" {
			t.Errorf("expected grant_type=password, got %s", r.FormValue("grant_type"))
		}
		if r.FormValue("client_id") != "my-client" {
			t.Errorf("expected client_id=my-client, got %s", r.FormValue("client_id"))
		}
		if r.FormValue("username") != "alice" {
			t.Errorf("expected username=alice, got %s", r.FormValue("username"))
		}
		if r.FormValue("password") != "secret" {
			t.Errorf("expected password=secret, got %s", r.FormValue("password"))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expected)
	}))
	defer server.Close()

	resp, err := RequestTokens(server.URL, "testrealm", "my-client", "alice", "secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.AccessToken != expected.AccessToken {
		t.Errorf("access token = %q, want %q", resp.AccessToken, expected.AccessToken)
	}
	if resp.RefreshToken != expected.RefreshToken {
		t.Errorf("refresh token = %q, want %q", resp.RefreshToken, expected.RefreshToken)
	}
	if resp.ExpiresIn != expected.ExpiresIn {
		t.Errorf("expires_in = %d, want %d", resp.ExpiresIn, expected.ExpiresIn)
	}
}

func TestRequestTokens_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("invalid credentials"))
	}))
	defer server.Close()

	_, err := RequestTokens(server.URL, "testrealm", "my-client", "alice", "wrong")
	if err == nil {
		t.Fatal("expected error for 401 response, got nil")
	}
}

func TestRequestTokens_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not-json"))
	}))
	defer server.Close()

	_, err := RequestTokens(server.URL, "testrealm", "my-client", "alice", "pw")
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestRequestTokens_ConnectionError(t *testing.T) {
	_, err := RequestTokens("http://127.0.0.1:1", "testrealm", "my-client", "alice", "pw")
	if err == nil {
		t.Fatal("expected error for unreachable server, got nil")
	}
}

func TestRequestTokensWithContext_CanceledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"access_token":"tok"}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := RequestTokensWithContext(ctx, nil, server.URL, "testrealm", "my-client", "alice", "pw")
	if err == nil {
		t.Fatal("expected cancellation error, got nil")
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected context cancellation error, got %v", err)
	}
}

func TestRefreshTokens_Success(t *testing.T) {
	expected := OAuthTokenResponse{
		AccessToken:  "new-access",
		RefreshToken: "new-refresh",
		ExpiresIn:    300,
		TokenType:    "Bearer",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("failed to parse form: %v", err)
		}
		if r.FormValue("grant_type") != "refresh_token" {
			t.Errorf("expected grant_type=refresh_token, got %s", r.FormValue("grant_type"))
		}
		if r.FormValue("refresh_token") != "old-refresh" {
			t.Errorf("expected refresh_token=old-refresh, got %s", r.FormValue("refresh_token"))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expected)
	}))
	defer server.Close()

	resp, err := RefreshTokens(server.URL, "testrealm", "my-client", "old-refresh")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.AccessToken != expected.AccessToken {
		t.Errorf("access token = %q, want %q", resp.AccessToken, expected.AccessToken)
	}
	if resp.RefreshToken != expected.RefreshToken {
		t.Errorf("refresh token = %q, want %q", resp.RefreshToken, expected.RefreshToken)
	}
}

func TestRefreshTokens_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("token expired"))
	}))
	defer server.Close()

	_, err := RefreshTokens(server.URL, "testrealm", "my-client", "expired-token")
	if err == nil {
		t.Fatal("expected error for 400 response, got nil")
	}
}

func TestRefreshTokens_ConnectionError(t *testing.T) {
	_, err := RefreshTokens("http://127.0.0.1:1", "testrealm", "my-client", "tok")
	if err == nil {
		t.Fatal("expected error for unreachable server, got nil")
	}
}

func TestRefreshTokensWithContext_DeadlineExceeded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"access_token":"tok"}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	_, err := RefreshTokensWithContext(ctx, nil, server.URL, "testrealm", "my-client", "tok")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("expected deadline exceeded error, got %v", err)
	}
}

func TestRequestTokens_URLConstruction(t *testing.T) {
	var receivedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(OAuthTokenResponse{AccessToken: "t"})
	}))
	defer server.Close()

	_, _ = RequestTokens(server.URL, "my-realm", "cid", "u", "p")

	expected := "/realms/my-realm/protocol/openid-connect/token"
	if receivedPath != expected {
		t.Errorf("URL path = %q, want %q", receivedPath, expected)
	}
}
