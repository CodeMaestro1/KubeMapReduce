package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"kubemapreduce/auth-service/pkg/auth"
	"kubemapreduce/manager-service/internal/api"
	"kubemapreduce/manager-service/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	validator, err := auth.NewJWTValidator(cfg.JWKSURL, cfg.Issuer, cfg.Audience)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			log.Fatalf("failed to initialize JWT validator (jwks=%s, issuer=%s, audience=%s): Keycloak returned 404. Ensure realm %q and client %q exist, or override KEYCLOAK_REALM/KEYCLOAK_JWKS_URL/KEYCLOAK_ISSUER. Original error: %v",
				cfg.JWKSURL, cfg.Issuer, cfg.Audience, cfg.Realm, cfg.Audience, err)
		}
		log.Fatalf("failed to initialize JWT validator (jwks=%s, issuer=%s, audience=%s): %v",
			cfg.JWKSURL, cfg.Issuer, cfg.Audience, err)
	}

	var adminClient *auth.KeycloakAdminClient
	if cfg.AdminUsername != "" && cfg.AdminPassword != "" {
		adminClient = auth.NewKeycloakAdminClient(
			cfg.KeycloakBaseURL,
			cfg.Realm,
			cfg.AdminUsername,
			cfg.AdminPassword,
		)
	} else {
		log.Printf("admin credentials not configured; /admin/* endpoints will return %d", http.StatusServiceUnavailable)
	}

	db, err := sql.Open("postgres", cfg.DatabaseDSN)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	store := api.NewPostgresJobStore(db)
	handlers := api.NewHandlers(adminClient, store)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handlers, validator)

	srv := &http.Server{
		Addr:              cfg.ServerAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("API running on %s", cfg.ServerAddr)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-sigCtx.Done():
		log.Printf("shutdown signal received, draining in-flight requests")
	case err := <-errCh:
		log.Fatalf("server failed: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
		if closeErr := srv.Close(); closeErr != nil {
			log.Printf("force close failed: %v", closeErr)
		}
	}

	log.Println("API server stopped")
}
