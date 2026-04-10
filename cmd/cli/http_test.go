package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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
