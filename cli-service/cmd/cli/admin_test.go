package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestRunAdminConfigureNodes_SendsPutToCorrectEndpoint(t *testing.T) {
	origGetToken := adminGetValidToken
	origRequireAdmin := adminRequireAdminRole
	defer func() {
		adminGetValidToken = origGetToken
		adminRequireAdminRole = origRequireAdmin
	}()
	adminGetValidToken = func() (string, string) { return "test-token", "http://example.test" }
	adminRequireAdminRole = func(_ string) {}

	var capturedMethod, capturedURL string
	stub := func(method, url, _ string, body []byte) (*http.Response, error) {
		capturedMethod = method
		capturedURL = url
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			t.Fatalf("body is not valid JSON: %v", err)
		}
		if _, ok := m["maxConcurrentPods"]; !ok {
			t.Fatalf("expected maxConcurrentPods in payload, got %v", m)
		}
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Body:       io.NopCloser(strings.NewReader(`{"status":"accepted"}`)),
		}, nil
	}

	if err := runAdminConfigureNodes([]string{"--max-pods", "4"}, stub); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedMethod != http.MethodPost {
		t.Errorf("expected POST, got %s", capturedMethod)
	}
	if capturedURL != "http://example.test/api/v1/admin/config/workers" {
		t.Errorf("expected /api/v1/admin/config/workers, got %s", capturedURL)
	}
}

func TestRunAdminConfigureNodes_MissingMaxPodsReturnsError(t *testing.T) {
	origGetToken := adminGetValidToken
	origRequireAdmin := adminRequireAdminRole
	defer func() {
		adminGetValidToken = origGetToken
		adminRequireAdminRole = origRequireAdmin
	}()
	adminGetValidToken = func() (string, string) { return "test-token", "http://example.test" }
	adminRequireAdminRole = func(_ string) {}

	neverCalled := func(_, _, _ string, _ []byte) (*http.Response, error) {
		t.Fatal("doAuthReq must not be called when --max-pods is missing or zero")
		return nil, nil
	}

	err := runAdminConfigureNodes([]string{"--max-pods", "0"}, neverCalled)
	if err == nil {
		t.Fatal("expected error for missing --max-pods, got nil")
	}
}

func TestConfigureNodesStatusError_AcceptedIsNil(t *testing.T) {
	if err := configureNodesStatusError(http.StatusAccepted, `{"status":"accepted"}`); err != nil {
		t.Fatalf("expected nil error for accepted response, got %v", err)
	}
}

func TestConfigureNodesStatusError_NotImplementedIncludesHint(t *testing.T) {
	err := configureNodesStatusError(http.StatusNotImplemented, `{"message":"node configuration backend integration is not implemented yet"}`)
	if err == nil {
		t.Fatal("expected non-nil error for HTTP 501")
	}
	msg := err.Error()
	if !strings.Contains(msg, "HTTP 501") {
		t.Fatalf("expected HTTP 501 in error, got %q", msg)
	}
	if !strings.Contains(msg, "not implemented") {
		t.Fatalf("expected not implemented text in error, got %q", msg)
	}
	if !strings.Contains(msg, "pending backend implementation") {
		t.Fatalf("expected roadmap hint in error, got %q", msg)
	}
}

func TestConfigureNodesStatusError_UnexpectedStatusIncludesCodeAndBody(t *testing.T) {
	err := configureNodesStatusError(http.StatusBadRequest, `invalid payload`)
	if err == nil {
		t.Fatal("expected non-nil error for unexpected status")
	}
	msg := err.Error()
	if !strings.Contains(msg, "HTTP 400") {
		t.Fatalf("expected HTTP status in error, got %q", msg)
	}
	if !strings.Contains(msg, "invalid payload") {
		t.Fatalf("expected response body in error, got %q", msg)
	}
}
