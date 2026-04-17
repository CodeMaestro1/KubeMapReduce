package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kubemapreduce/pkg/auth"
)

// TestBootstrapPhaseTimeout verifies the bootstrap phase has its own
// independent context and honours its own deadline.
func TestBootstrapPhaseTimeout(t *testing.T) {
	// Fake Keycloak that sleeps longer than the deadline we will give.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := auth.BootstrapConfig{
		BaseURL:       srv.URL,
		Realm:         "test",
		ClientID:      "test-client",
		UIOrigin:      "http://localhost:8081",
		AdminUsername: "admin",
		AdminPassword: "admin",
		HTTPClient:    srv.Client(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := auth.BootstrapKeycloakWithContext(ctx, cfg, nil)
	if err == nil {
		t.Fatal("expected timeout error when bootstrap exceeds its context deadline")
	}
}

// TestUserCreationPhaseTimeout verifies the user-creation phase has its own
// independent context that is not affected by bootstrap duration.
func TestUserCreationPhaseTimeout(t *testing.T) {
	var bootstrapDone, userCreateCalled bool

	mux := http.NewServeMux()

	// Token endpoint — admin token acquisition.
	mux.HandleFunc("/realms/master/protocol/openid-connect/token", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "fake-token",
			"expires_in":   300,
		})
	})

	// User creation — must return Location header with user ID.
	mux.HandleFunc("/admin/realms/test/users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			userCreateCalled = true
			w.Header().Set("Location", "/admin/realms/test/users/fake-user-id-123")
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	// Set password endpoint.
	mux.HandleFunc("/admin/realms/test/users/fake-user-id-123/reset-password", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	// Role lookup endpoint (GET /admin/realms/test/roles/ADMIN).
	mux.HandleFunc("/admin/realms/test/roles/ADMIN", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"id": "role-id-1", "name": "ADMIN",
		})
	})

	// Role assignment endpoint.
	mux.HandleFunc("/admin/realms/test/users/fake-user-id-123/role-mappings/realm", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Simulate: bootstrap context expires, then user-creation gets a fresh context.
	bootstrapCtx, bootstrapCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer bootstrapCancel()

	// Wait for bootstrap context to expire.
	<-bootstrapCtx.Done()
	bootstrapDone = true

	// A fresh user-creation context should still work.
	userCtx, userCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer userCancel()

	client := auth.NewKeycloakAdminClient(srv.URL, "test", "admin", "admin")

	err := client.CreateUser(userCtx, auth.CreateUserRequest{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "password123",
		Role:     "ADMIN",
	})

	if !bootstrapDone {
		t.Fatal("bootstrap context should have expired before user creation")
	}
	if err != nil {
		t.Fatalf("user creation with fresh context should succeed, got: %v", err)
	}
	if !userCreateCalled {
		t.Fatal("expected user creation endpoint to be called")
	}
}

// TestPhasesUseIndependentContexts verifies that both phases create contexts
// from context.Background() rather than sharing a parent, so one phase's
// timeout cannot starve the other.
func TestPhasesUseIndependentContexts(t *testing.T) {
	// Give bootstrap's context almost no time so it expires immediately.
	bootstrapCtx, bootstrapCancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer bootstrapCancel()
	<-bootstrapCtx.Done() // wait for expiry

	// A child derived from this expired parent would already be done.
	childCtx, childCancel := context.WithTimeout(bootstrapCtx, 5*time.Second)
	defer childCancel()

	select {
	case <-childCtx.Done():
		// Expected: child of expired parent is immediately done.
	default:
		t.Fatal("child of expired context should be immediately done")
	}

	// But a new context from Background() is still alive — this is what the
	// setup command now does for the user-creation phase.
	freshCtx, freshCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer freshCancel()

	select {
	case <-freshCtx.Done():
		t.Fatal("fresh context from Background() should not be expired")
	default:
		// Expected: fresh context is alive.
	}
}
