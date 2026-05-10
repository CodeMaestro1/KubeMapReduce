package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
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
	"kubemapreduce/manager-service/pkg/observability"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// main bootstraps the UI Service API server.
//
// The bootstrapping sequence follows these steps:
//  1. Load configuration from environment variables via [config.Load].
//  2. Initialize the JWT Validator using Keycloak's JWKS endpoint to secure all routes.
//  3. (Optional) Initialize the Keycloak Admin Client if credentials are provided, enabling user management.
//  4. Connect to the PostgreSQL database for job metadata storage.
//  5. Initialize the Job Store and HTTP handlers.
//  6. Register routes and start the HTTP server with production-grade timeouts.
//  7. Listen for termination signals (SIGINT, SIGTERM) to initiate a graceful shutdown,
//     allowing in-flight requests 15 seconds to complete before forcing exit.
func main() {
	// Bootstrap structured logging before anything else so all subsequent
	// records (including legacy log.Printf calls bridged via slog.NewLogLogger)
	// follow the same JSON schema and carry the service attribute.
	logger := observability.NewLogger("api")
	slog.SetDefault(logger)
	log.SetFlags(0)
	log.SetOutput(loggerWriter{logger: logger})

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

	db.SetMaxOpenConns(cfg.DBMaxOpenConns)
	db.SetMaxIdleConns(cfg.DBMaxIdleConns)
	db.SetConnMaxLifetime(cfg.DBConnMaxLifetime)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	var minioClient *minio.Client
	if cfg.MinioEndpoint != "" && cfg.MinioAccessKey != "" && cfg.MinioSecretKey != "" {
		log.Printf("MinIO: endpoint=%s access_key=%s use_ssl=%v", cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioUseSSL)
		minioClient, err = minio.New(cfg.MinioEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
			Secure: cfg.MinioUseSSL,
		})
		if err != nil {
			log.Fatalf("failed to initialize minio client: %v", err)
		}
		bucketCtx, bucketCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if ensureErr := ensureMinIOBuckets(bucketCtx, minioClient); ensureErr != nil {
			bucketCancel()
			log.Fatalf("failed to ensure MinIO buckets: %v", ensureErr)
		}
		bucketCancel()
	}

	store := api.NewPostgresJobStore(db, cfg.TotalReplicas)
	handlers := api.NewHandlers(adminClient, store, minioClient, cfg.ManagerAddr, cfg.InternalAPIKey)

	// Register Prometheus collectors and mount the /metrics endpoint
	// before user-facing routes so scrapes never block on auth middleware.
	observability.SetDefaultMetrics(observability.NewMetrics())
	mux := http.NewServeMux()
	mux.Handle("/metrics", observability.MetricsHandler())
	api.RegisterRoutes(mux, handlers, validator)

	srv := &http.Server{
		Addr:              cfg.ServerAddr,
		Handler:           observability.RequestIDMiddleware(logger)(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      45 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 * 1024, // 16 KB
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

// requiredMinIOBuckets lists all buckets that must exist for the platform to
// operate. The slice is shared between ensureMinIOBuckets callers in both the
// API and manager binaries.
var requiredMinIOBuckets = []string{
	"mapreduce-inputs",
	"mapreduce-outputs",
	"mapreduce-shuffle",
	"mapreduce-manifests",
	"mapreduce-staging",
}

// ensureMinIOBuckets validates MinIO connectivity and creates any of the five
// required buckets that do not yet exist. The first BucketExists call acts as
// a startup ping — if MinIO is unreachable an error is returned immediately.
func ensureMinIOBuckets(ctx context.Context, mc *minio.Client) error {
	for _, bucket := range requiredMinIOBuckets {
		exists, err := mc.BucketExists(ctx, bucket)
		if err != nil {
			return fmt.Errorf("MinIO health check failed (bucket=%s): %w", bucket, err)
		}
		if !exists {
			if makeErr := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); makeErr != nil {
				return fmt.Errorf("create bucket %s: %w", bucket, makeErr)
			}
			slog.Info("created MinIO bucket", "bucket", bucket)
		}
	}
	return nil
}

// loggerWriter is an [io.Writer] that forwards every line written by the
// stdlib [log] package through the supplied [*slog.Logger] at ERROR level,
// stripping the trailing newline. This bridge ensures that legacy log.Printf
// callsites still emit structured JSON without requiring a sweeping rewrite.
type loggerWriter struct {
	logger *slog.Logger
}

func (w loggerWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\r\n")
	if msg == "" {
		return len(p), nil
	}
	w.logger.Error(msg, "source", "log.bridge")
	return len(p), nil
}
