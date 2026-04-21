package auth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewKeycloakBootstrapperRequiresCoreFields(t *testing.T) {
	tests := []struct {
		name string
		cfg  BootstrapConfig
	}{
		{
			name: "missing base url",
			cfg: BootstrapConfig{
				Realm:         "mapreduce",
				ClientID:      "mapreduce-api",
				UIOrigin:      "http://localhost:8081",
				AdminUsername: "admin",
				AdminPassword: "admin",
			},
		},
		{
			name: "missing realm",
			cfg: BootstrapConfig{
				BaseURL:       "http://localhost:8080",
				ClientID:      "mapreduce-api",
				UIOrigin:      "http://localhost:8081",
				AdminUsername: "admin",
				AdminPassword: "admin",
			},
		},
		{
			name: "missing client id",
			cfg: BootstrapConfig{
				BaseURL:       "http://localhost:8080",
				Realm:         "mapreduce",
				UIOrigin:      "http://localhost:8081",
				AdminUsername: "admin",
				AdminPassword: "admin",
			},
		},
		{
			name: "missing ui origin",
			cfg: BootstrapConfig{
				BaseURL:       "http://localhost:8080",
				Realm:         "mapreduce",
				ClientID:      "mapreduce-api",
				AdminUsername: "admin",
				AdminPassword: "admin",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newKeycloakBootstrapper(tc.cfg, nil)
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if err.Error() != "keycloak-base-url, realm, client-id, and ui-origin are required" {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewKeycloakBootstrapperRequiresAdminCredentials(t *testing.T) {
	_, err := newKeycloakBootstrapper(BootstrapConfig{
		BaseURL:  "http://localhost:8080",
		Realm:    "mapreduce",
		ClientID: "mapreduce-api",
		UIOrigin: "http://localhost:8081",
	}, nil)

	if err == nil {
		t.Fatal("expected admin credential validation error, got nil")
	}
	if err.Error() != "admin credentials are required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewKeycloakBootstrapperNormalizesTrimmedValues(t *testing.T) {
	b, err := newKeycloakBootstrapper(BootstrapConfig{
		BaseURL:       "  http://localhost:8080/  ",
		Realm:         "  mapreduce  ",
		ClientID:      "  mapreduce-api  ",
		UIOrigin:      "  http://localhost:8081/  ",
		AdminUsername: "  admin  ",
		AdminPassword: "  admin  ",
	}, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if b.baseURL != "http://localhost:8080" {
		t.Fatalf("expected normalized baseURL, got %q", b.baseURL)
	}
	if b.realm != "mapreduce" {
		t.Fatalf("expected normalized realm, got %q", b.realm)
	}
	if b.clientID != "mapreduce-api" {
		t.Fatalf("expected normalized clientID, got %q", b.clientID)
	}
	if b.uiOrigin != "http://localhost:8081" {
		t.Fatalf("expected normalized uiOrigin, got %q", b.uiOrigin)
	}
	if b.adminUsername != "admin" {
		t.Fatalf("expected normalized adminUsername, got %q", b.adminUsername)
	}
	if b.adminPassword != "admin" {
		t.Fatalf("expected normalized adminPassword, got %q", b.adminPassword)
	}
}

func TestNewKeycloakBootstrapperUsesDefaultHTTPClientTimeouts(t *testing.T) {
	b, err := newKeycloakBootstrapper(validBootstrapConfig(), nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if b.httpClient == nil {
		t.Fatal("expected http client to be set")
	}
	if b.httpClient.Timeout != defaultHTTPTimeout {
		t.Fatalf("expected timeout %v, got %v", defaultHTTPTimeout, b.httpClient.Timeout)
	}

	transport, ok := b.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", b.httpClient.Transport)
	}

	if transport.TLSHandshakeTimeout != defaultTLSHandshakeTimeout {
		t.Fatalf("expected TLS timeout %v, got %v", defaultTLSHandshakeTimeout, transport.TLSHandshakeTimeout)
	}
	if transport.ResponseHeaderTimeout != defaultResponseHeaderTimeout {
		t.Fatalf("expected response header timeout %v, got %v", defaultResponseHeaderTimeout, transport.ResponseHeaderTimeout)
	}
	if transport.ExpectContinueTimeout != defaultExpectContinueTimeout {
		t.Fatalf("expected expect-continue timeout %v, got %v", defaultExpectContinueTimeout, transport.ExpectContinueTimeout)
	}
	if transport.IdleConnTimeout != defaultIdleConnTimeout {
		t.Fatalf("expected idle conn timeout %v, got %v", defaultIdleConnTimeout, transport.IdleConnTimeout)
	}
}

func TestNewKeycloakBootstrapperAugmentsCustomClientWhenUnset(t *testing.T) {
	provided := &http.Client{}

	b, err := newKeycloakBootstrapper(BootstrapConfig{
		BaseURL:       "http://localhost:8080",
		Realm:         "mapreduce",
		ClientID:      "mapreduce-api",
		UIOrigin:      "http://localhost:8081",
		AdminUsername: "admin",
		AdminPassword: "admin",
		HTTPClient:    provided,
	}, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if b.httpClient == provided {
		t.Fatal("expected bootstrapper to copy provided client")
	}
	if provided.Timeout != 0 {
		t.Fatalf("expected original client timeout unchanged, got %v", provided.Timeout)
	}
	if b.httpClient.Timeout != defaultHTTPTimeout {
		t.Fatalf("expected timeout %v, got %v", defaultHTTPTimeout, b.httpClient.Timeout)
	}
	if _, ok := b.httpClient.Transport.(*http.Transport); !ok {
		t.Fatalf("expected default *http.Transport, got %T", b.httpClient.Transport)
	}
}

func TestNewKeycloakBootstrapperPreservesCustomClientSettings(t *testing.T) {
	customTransport := &http.Transport{ResponseHeaderTimeout: 3 * time.Second}
	provided := &http.Client{
		Timeout:   5 * time.Second,
		Transport: customTransport,
	}

	b, err := newKeycloakBootstrapper(BootstrapConfig{
		BaseURL:       "http://localhost:8080",
		Realm:         "mapreduce",
		ClientID:      "mapreduce-api",
		UIOrigin:      "http://localhost:8081",
		AdminUsername: "admin",
		AdminPassword: "admin",
		HTTPClient:    provided,
	}, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if b.httpClient.Timeout != 5*time.Second {
		t.Fatalf("expected timeout 5s, got %v", b.httpClient.Timeout)
	}
	if b.httpClient.Transport != customTransport {
		t.Fatal("expected custom transport to be preserved")
	}
}

func TestGetAdminTokenSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected %s, got %s", http.MethodPost, r.Method)
		}
		if r.URL.Path != "/realms/master/protocol/openid-connect/token" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if r.Form.Get("grant_type") != "password" {
			t.Fatalf("expected grant_type=password, got %q", r.Form.Get("grant_type"))
		}
		if r.Form.Get("client_id") != "admin-cli" {
			t.Fatalf("expected client_id=admin-cli, got %q", r.Form.Get("client_id"))
		}
		if r.Form.Get("username") != "admin" {
			t.Fatalf("expected username=admin, got %q", r.Form.Get("username"))
		}
		if r.Form.Get("password") != "secret" {
			t.Fatalf("expected password=secret, got %q", r.Form.Get("password"))
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"access_token":"token-123"}`))
	}))
	defer server.Close()

	b := &keycloakBootstrapper{
		baseURL:       server.URL,
		adminUsername: "admin",
		adminPassword: "secret",
		httpClient:    server.Client(),
	}

	token, err := b.getAdminToken(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if token != "token-123" {
		t.Fatalf("expected token token-123, got %q", token)
	}
}

func TestGetAdminTokenReturnsErrorForNonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("invalid credentials"))
	}))
	defer server.Close()

	b := &keycloakBootstrapper{
		baseURL:       server.URL,
		adminUsername: "admin",
		adminPassword: "wrong",
		httpClient:    server.Client(),
	}

	_, err := b.getAdminToken(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to get admin token: status 401: invalid credentials") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetAdminTokenReturnsErrorForOversizedErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(strings.Repeat("x", maxAuthResponseBytes+1)))
	}))
	defer server.Close()

	b := &keycloakBootstrapper{
		baseURL:       server.URL,
		adminUsername: "admin",
		adminPassword: "wrong",
		httpClient:    server.Client(),
	}

	_, err := b.getAdminToken(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "response body exceeds limit") {
		t.Fatalf("expected bounded read error, got %v", err)
	}
}

func TestGetAdminTokenReturnsErrorWhenAccessTokenMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"token_type":"bearer"}`))
	}))
	defer server.Close()

	b := &keycloakBootstrapper{
		baseURL:       server.URL,
		adminUsername: "admin",
		adminPassword: "secret",
		httpClient:    server.Client(),
	}

	_, err := b.getAdminToken(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "admin token missing in response" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCallJSONSendsAuthAndJSONPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("expected %s, got %s", http.MethodPut, r.Method)
		}
		if r.URL.Path != "/admin/check" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("expected bearer auth header, got %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("expected JSON content type, got %q", r.Header.Get("Content-Type"))
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}

		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if payload["name"] != "worker-1" {
			t.Fatalf("expected payload name=worker-1, got %v", payload["name"])
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	b := &keycloakBootstrapper{
		baseURL:    server.URL,
		token:      "test-token",
		httpClient: server.Client(),
	}

	status, body, err := b.callJSON(context.Background(), http.MethodPut, "/admin/check", map[string]any{"name": "worker-1"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if status != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d", status)
	}
	if len(body) != 0 {
		t.Fatalf("expected empty body, got %q", string(body))
	}
}

func TestCallJSONWithoutPayloadOmitsContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected %s, got %s", http.MethodGet, r.Method)
		}
		if r.Header.Get("Content-Type") != "" {
			t.Fatalf("expected empty content type for nil payload, got %q", r.Header.Get("Content-Type"))
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	b := &keycloakBootstrapper{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	status, body, err := b.callJSON(context.Background(), http.MethodGet, "/ping", nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("expected status 200, got %d", status)
	}
	if string(body) != "ok" {
		t.Fatalf("expected body ok, got %q", string(body))
	}
}

func TestCallJSONRetriesAndReturnsServiceUnavailable(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("temporarily unavailable"))
	}))
	defer server.Close()

	b := &keycloakBootstrapper{
		baseURL:        server.URL,
		httpClient:     server.Client(),
		retryAttempts:  3,
		retryBaseDelay: time.Millisecond,
	}

	_, _, err := b.callJSON(context.Background(), http.MethodGet, "/ping", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !IsServiceUnavailable(err) {
		t.Fatalf("expected service unavailable error, got %v", err)
	}
	if got := atomic.LoadInt32(&requestCount); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

func TestCallJSONReturnsErrorForOversizedBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(strings.Repeat("y", maxAuthResponseBytes+1)))
	}))
	defer server.Close()

	b := &keycloakBootstrapper{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	_, _, err := b.callJSON(context.Background(), http.MethodGet, "/too-large", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "response body exceeds limit") {
		t.Fatalf("expected bounded read error, got %v", err)
	}
}

func TestCallJSONHonorsCanceledContext(t *testing.T) {
	b := &keycloakBootstrapper{
		baseURL:        "http://example.invalid",
		httpClient:     newBootstrapHTTPClient(),
		retryAttempts:  3,
		retryBaseDelay: time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := b.callJSON(ctx, http.MethodGet, "/ping", nil)
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestBootstrapKeycloakWithContextCanceled(t *testing.T) {
	b, err := newKeycloakBootstrapper(validBootstrapConfig(), nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.bootstrap(ctx)
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestCallJSONRetriesNetworkErrorsAndReturnsServiceUnavailable(t *testing.T) {
	var attempts int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&attempts, 1)
		return nil, &url.Error{Op: req.Method, URL: req.URL.String(), Err: errors.New("connection reset")}
	})

	b := &keycloakBootstrapper{
		baseURL:        "http://example.invalid",
		httpClient:     &http.Client{Transport: transport},
		retryAttempts:  3,
		retryBaseDelay: time.Millisecond,
	}

	_, _, err := b.callJSON(context.Background(), http.MethodGet, "/ping", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !IsServiceUnavailable(err) {
		t.Fatalf("expected service unavailable error, got %v", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

func validBootstrapConfig() BootstrapConfig {
	return BootstrapConfig{
		BaseURL:       "http://localhost:8080",
		Realm:         "mapreduce",
		ClientID:      "mapreduce-api",
		UIOrigin:      "http://localhost:8081",
		AdminUsername: "admin",
		AdminPassword: "admin",
	}
}
