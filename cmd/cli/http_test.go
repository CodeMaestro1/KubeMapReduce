package main

import (
	"context"
	"net/http"
	"net/http/httptest"
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
