package main

import (
	"net/http"
	"strings"
	"testing"
)

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
