package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpccredentials "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"kubemapreduce/manager-service/internal/config"
	mgrpc "kubemapreduce/manager-service/internal/grpc"
	"kubemapreduce/manager-service/internal/manager"
	"kubemapreduce/manager-service/pkg/observability"
	timeoutgrpc "kubemapreduce/pkg/grpc"
	pb "kubemapreduce/proto"
)

// main bootstraps the MapReduce Manager Service.
//
// The bootstrapping sequence follows these steps:
//  1. Load configuration and connect to the PostgreSQL database (DDS).
//  2. Resolve the replica index from the hostname (StatefulSet ordinal) for task partitioning.
//  3. Initialize the Kubernetes Orchestrator (or a mock if running locally).
//  4. Initialize the Scheduler and recover any interrupted tasks from the database.
//  5. Start background maintenance loops: Cleanup Reconciler and the Active Reaper (stale task cleanup).
//  6. Start the HTTP server for health/readiness probes and internal job cancellation.
//  7. Start the gRPC server for Worker communication (Register, Heartbeat, etc.) with optional TLS and token auth.
//  8. Handle graceful shutdown by stopping background loops and draining gRPC/HTTP connections.
func main() {
	// Bootstrap structured logging before anything else so all subsequent
	// records (including legacy log.Printf calls bridged via the loggerWriter
	// adapter below) follow the same JSON schema and carry the service
	// attribute.
	logger := observability.NewLogger("manager")
	slog.SetDefault(logger)
	log.SetFlags(0)
	log.SetOutput(loggerWriter{logger: logger})

	// Register Prometheus collectors so internal packages can record
	// observations through observability.DefaultMetrics().
	observability.SetDefaultMetrics(observability.NewMetrics())

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	// 1. Connect to Database
	db, err := sql.Open("postgres", cfg.DatabaseDSN)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	// For StatefulSet replica routing
	hostname, _ := os.Hostname()
	replicaIndex := resolveReplicaIndex(hostname)
	namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
	if namespace == "" {
		namespace = "default"
	}
	headlessService := strings.TrimSpace(os.Getenv("MANAGER_HEADLESS_SERVICE"))
	if headlessService == "" {
		headlessService = "manager-headless"
	}

	// 2. Initialize Kubernetes Orchestrator
	k8sConfig, err := rest.InClusterConfig()
	var orchestrator manager.WorkerOrchestrator
	if err != nil {
		log.Printf("failed to get in-cluster k8s config (running locally?): %v. Using mock orchestrator.", err)
		orchestrator = &manager.MockOrchestrator{}
	} else {
		clientset, err := kubernetes.NewForConfig(k8sConfig)
		if err != nil {
			log.Fatalf("failed to create k8s clientset: %v", err)
		}
		orchestrator = manager.NewKubeOrchestrator(clientset, namespace, "kubemapreduce-worker:latest", cfg.WorkerSecretName).
			WithResourceProvider(manager.NewDBResourceConfigProvider(db))
	}

	// 3. Initialize Scheduler
	_, port, err := net.SplitHostPort(cfg.GRPCAddr)
	if err != nil {
		port = "50051"
	}
	managerAddr := resolveManagerAddr(hostname, headlessService, namespace, port)
	var stagingCleaner manager.StagingCleaner
	if cfg.MinioEndpoint != "" && cfg.MinioAccessKey != "" && cfg.MinioSecretKey != "" {
		mc, mcErr := minio.New(cfg.MinioEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
			Secure: cfg.MinioUseSSL,
		})
		if mcErr != nil {
			log.Printf("staging cleaner: failed to create minio client: %v", mcErr)
		} else {
			stagingCleaner = manager.NewMinioStagingCleaner(mc)
		}
	}

	scheduler, err := manager.NewScheduler(db, replicaIndex, cfg.TotalReplicas, orchestrator, managerAddr, cfg.LeaseTTL, stagingCleaner)
	if err != nil {
		log.Fatalf("failed to create scheduler: %v", err)
	}

	if err := scheduler.Recover(context.Background()); err != nil {
		log.Fatalf("failed to recover scheduler tasks: %v", err)
	}

	// 3. Start background loops
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler.StartCleanupReconciler(ctx, 15*time.Second)
	startReaper(ctx, scheduler, defaultReaperInterval(cfg.LeaseTTL))

	// 4. Start HTTP Server for Health & Readiness
	mux := setupInternalMux(scheduler, db, cfg)

	httpSrv := &http.Server{
		Addr:              cfg.ServerAddr,
		Handler:           observability.RequestIDMiddleware(logger)(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      45 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 * 1024, // 16 KB
	}
	go func() {
		log.Printf("HTTP health server running on %s", cfg.ServerAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	// 5. Start gRPC Server
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("failed to listen on gRPC address %s: %v", cfg.GRPCAddr, err)
	}

	var minioClient *minio.Client
	if cfg.MinioEndpoint != "" && cfg.MinioAccessKey != "" && cfg.MinioSecretKey != "" {
		minioClient, err = minio.New(cfg.MinioEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
			Secure: cfg.MinioUseSSL,
		})
		if err != nil {
			log.Printf("failed to initialize minio client: %v", err)
		}
	} else if cfg.MinioEndpoint != "" {
		log.Printf("minio endpoint configured without credentials; manifest fallback disabled")
	}

	// Initialize timeout configuration for per-RPC method timeouts
	timeoutCfg := timeoutgrpc.NewDefaultTimeoutConfig()
	timeoutCfg.ValidateConfig()

	grpcOpts := []grpc.ServerOption{
		// Chain auth and timeout interceptors
		grpc.ChainUnaryInterceptor(
			workerAuthUnaryInterceptor(cfg.WorkerRPCToken),
			timeoutCfg.UnaryInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			timeoutCfg.StreamInterceptor(),
		),
		grpc.MaxRecvMsgSize(4 << 20),
		grpc.MaxSendMsgSize(16 << 20),
	}
	if err := validateWorkerRPCSecurityConfig(cfg); err != nil {
		log.Fatalf("insecure worker RPC configuration: %v", err)
	}
	emitWorkerRPCSecurityWarnings(cfg)

	useTLS := strings.TrimSpace(cfg.GRPCTLSCertFile) != "" || strings.TrimSpace(cfg.GRPCTLSKeyFile) != ""
	if useTLS {
		if strings.TrimSpace(cfg.GRPCTLSCertFile) == "" || strings.TrimSpace(cfg.GRPCTLSKeyFile) == "" {
			log.Fatalf("both GRPC_TLS_CERT_FILE and GRPC_TLS_KEY_FILE must be set to enable gRPC TLS")
		}
		grpcCreds, err := grpccredentials.NewServerTLSFromFile(cfg.GRPCTLSCertFile, cfg.GRPCTLSKeyFile)
		if err != nil {
			log.Fatalf("failed to load gRPC TLS credentials: %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(grpcCreds))
	}

	grpcServer := grpc.NewServer(grpcOpts...)
	workerServer := mgrpc.NewWorkerServer(scheduler, minioClient, cfg.ManifestThresholdBytes)
	pb.RegisterWorkerServiceServer(grpcServer, workerServer)

	// gRPC Reflection is disabled by default for security.
	// Reflection exposes service definitions and allows clients to discover available RPC methods.
	// Only enable in development environments with explicit opt-in via:
	//   1. ENABLE_GRPC_REFLECTION=true environment variable
	//   2. DEBUG_MODE=true environment variable (additional guard for production safety)
	// In production, keep reflection disabled to reduce attack surface.
	if cfg.EnableGRPCReflection {
		if os.Getenv("DEBUG_MODE") != "true" {
			slog.Warn(
				"gRPC reflection is requested but DEBUG_MODE=true is not set; reflection disabled for security",
				slog.String("component", "grpc"),
				slog.String("recommendation", "set DEBUG_MODE=true only in development environments"),
			)
		} else {
			slog.Warn(
				"gRPC reflection is enabled; service definitions are exposed",
				slog.String("component", "grpc"),
				slog.String("security_note", "only enable in development environments"),
			)
			reflection.Register(grpcServer)
		}
	}

	go func() {
		log.Printf("gRPC server running on %s", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// 6. Graceful Shutdown
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-sigCtx.Done()
	log.Println("shutdown signal received, initiating graceful shutdown")

	cancel() // Stop reaper
	grpcServer.GracefulStop()

	httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpCancel()
	httpSrv.Shutdown(httpCtx)

	log.Println("manager service stopped")
}

func resolveReplicaIndex(hostname string) int {
	ordinal := strings.TrimSpace(os.Getenv("STATEFULSET_ORDINAL"))
	if ordinal != "" {
		if idx, err := strconv.Atoi(ordinal); err == nil && idx >= 0 {
			return idx
		}
		log.Printf("[WARN] invalid STATEFULSET_ORDINAL=%q, falling back to hostname parsing", ordinal)
	}
	return parseReplicaIndexFromHostname(hostname)
}

func parseReplicaIndexFromHostname(hostname string) int {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return 0
	}
	lastDash := strings.LastIndex(hostname, "-")
	if lastDash == -1 {
		return 0
	}
	idx, err := strconv.Atoi(hostname[lastDash+1:])
	if err != nil || idx < 0 {
		return 0
	}
	return idx
}

func resolveManagerAddr(hostname, headlessService, namespace, port string) string {
	if explicit := strings.TrimSpace(os.Getenv("MANAGER_ADDR")); explicit != "" {
		return explicit
	}
	podName := strings.TrimSpace(hostname)
	// Pod hostnames in StatefulSets are expected to be short names (e.g. manager-0).
	// If the value is empty or already FQDN-like, fall back to a stable default.
	if podName == "" || strings.Contains(podName, ".") {
		podName = "manager-0"
	}
	return net.JoinHostPort(podName+"."+headlessService+"."+namespace+".svc.cluster.local", port)
}

func isAuthorizedInternalRequest(r *http.Request, expectedToken string, allowInsecureLoopback bool) bool {
	expectedToken = strings.TrimSpace(expectedToken)
	if expectedToken != "" {
		return r.Header.Get("X-Internal-Token") == expectedToken
	}
	return allowInsecureLoopback && isLoopbackRemoteAddr(r.RemoteAddr)
}

func isLoopbackRemoteAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err != nil {
		host = strings.TrimSpace(remoteAddr)
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func workerAuthUnaryInterceptor(expectedToken string) grpc.UnaryServerInterceptor {
	expectedToken = strings.TrimSpace(expectedToken)
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if expectedToken != "" && !isAuthorizedWorkerRPC(ctx, expectedToken) {
			return nil, status.Error(codes.Unauthenticated, "missing or invalid worker rpc token")
		}
		return handler(ctx, req)
	}
}

func isAuthorizedWorkerRPC(ctx context.Context, expectedToken string) bool {
	expectedToken = strings.TrimSpace(expectedToken)
	if expectedToken == "" {
		return true
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get("x-worker-token")
	if len(values) == 0 {
		return false
	}
	return values[0] == expectedToken
}

func validateWorkerRPCSecurityConfig(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	hasWorkerToken := strings.TrimSpace(cfg.WorkerRPCToken) != ""
	hasCert := strings.TrimSpace(cfg.GRPCTLSCertFile) != ""
	hasKey := strings.TrimSpace(cfg.GRPCTLSKeyFile) != ""
	useTLS := hasCert || hasKey

	if !hasWorkerToken && !useTLS && !cfg.AllowInsecureWorkerRPC {
		return errors.New("set MANAGER_WORKER_RPC_TOKEN and/or GRPC_TLS_CERT_FILE+GRPC_TLS_KEY_FILE (or explicitly set ALLOW_INSECURE_WORKER_RPC=true for local development only)")
	}
	if useTLS && (!hasCert || !hasKey) {
		return errors.New("both GRPC_TLS_CERT_FILE and GRPC_TLS_KEY_FILE must be set to enable gRPC TLS")
	}
	return nil
}

func emitWorkerRPCSecurityWarnings(cfg *config.Config) {
	hasWorkerToken := strings.TrimSpace(cfg.WorkerRPCToken) != ""
	useTLS := strings.TrimSpace(cfg.GRPCTLSCertFile) != "" || strings.TrimSpace(cfg.GRPCTLSKeyFile) != ""
	if !hasWorkerToken && useTLS {
		log.Printf("[WARN] worker RPC token is not configured; relying on transport-level TLS controls only")
	}
	if hasWorkerToken && !useTLS {
		log.Printf("[WARN] gRPC TLS is not configured; worker token is enforced over plaintext transport")
	}
	if cfg.AllowInsecureWorkerRPC && !hasWorkerToken && !useTLS {
		log.Printf("[WARN] ALLOW_INSECURE_WORKER_RPC=true: worker RPC is running without token auth and without TLS")
	}
}

// ReaperScheduler defines the scheduler method used by the active reaper loop.
type ReaperScheduler interface {
	FailStaleTasks(ctx context.Context) (int, error)
}

// JobScheduler defines the interface for scheduler operations used by HTTP handlers.
type JobScheduler interface {
	CancelJob(ctx context.Context, jobID string) error
	ScheduleJob(ctx context.Context, req manager.ScheduleJobRequest) error
	UpsertSystemConfig(ctx context.Context, update manager.SystemConfigUpdate) error
}

// Pingable abstracts the database ping method.
type Pingable interface {
	PingContext(ctx context.Context) error
}

func defaultReaperInterval(leaseTTLSeconds int) time.Duration {
	if leaseTTLSeconds <= 0 {
		return 15 * time.Second
	}
	interval := (time.Duration(leaseTTLSeconds) * time.Second) / 2
	if interval < time.Second {
		return time.Second
	}
	return interval
}

func startReaper(ctx context.Context, scheduler ReaperScheduler, interval time.Duration) {
	if scheduler == nil {
		return
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reaperCtx, reaperCancel := context.WithTimeout(ctx, 30*time.Second)
				cycleStart := time.Now()
				recovered, err := scheduler.FailStaleTasks(reaperCtx)
				reaperCancel()
				if m := observability.DefaultMetrics(); m != nil {
					m.SchedulerCycleSeconds.Observe(time.Since(cycleStart).Seconds())
					if recovered > 0 {
						m.ReaperRecovered.Add(float64(recovered))
					}
				}
				if err != nil {
					slog.Error("reaper cycle failed", slog.Any("err", err))
				} else if recovered > 0 {
					slog.Info("reaper recovered stale tasks", slog.Int("recovered", recovered))
				}
			}
		}
	}()
}

func setupInternalMux(scheduler JobScheduler, db Pingable, cfg *config.Config) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", observability.MetricsHandler())
	mux.HandleFunc("DELETE /internal/jobs/{job_id}", func(w http.ResponseWriter, r *http.Request) {
		if !isAuthorizedInternalRequest(r, cfg.InternalAPIKey, cfg.AllowInsecureInternalCancelAuth) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		jobID := r.PathValue("job_id")
		if jobID == "" {
			http.Error(w, "missing job_id", http.StatusBadRequest)
			return
		}
		cancelCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := scheduler.CancelJob(cancelCtx, jobID); err != nil {
			log.Printf("failed to cancel job %s: %v", jobID, err)
			http.Error(w, "failed to cancel job", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("POST /internal/schedule", func(w http.ResponseWriter, r *http.Request) {
		if !isAuthorizedInternalRequest(r, cfg.InternalAPIKey, cfg.AllowInsecureInternalCancelAuth) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req manager.ScheduleJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid payload", http.StatusBadRequest)
			return
		}
		schedCtx, schedCancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer schedCancel()
		if err := scheduler.ScheduleJob(schedCtx, req); err != nil {
			log.Printf("failed to schedule job %s: %v", req.JobID, err)
			http.Error(w, "failed to schedule job", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("PUT /internal/config", func(w http.ResponseWriter, r *http.Request) {
		if !isAuthorizedInternalRequest(r, cfg.InternalAPIKey, cfg.AllowInsecureInternalCancelAuth) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var update manager.SystemConfigUpdate
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			http.Error(w, "invalid payload", http.StatusBadRequest)
			return
		}
		if err := scheduler.UpsertSystemConfig(r.Context(), update); err != nil {
			log.Printf("failed to update system config: %v", err)
			http.Error(w, "failed to update config", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	readinessHandler := func(w http.ResponseWriter, r *http.Request) {
		if db != nil {
			pingCtx, pingCancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer pingCancel()
			if err := db.PingContext(pingCtx); err != nil {
				http.Error(w, "Database not ready", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
	mux.HandleFunc("/readyz", readinessHandler)
	return mux
}

// loggerWriter is an [io.Writer] that forwards every line written by the
// stdlib [log] package through the supplied [*slog.Logger] at INFO level,
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
	w.logger.Info(msg)
	return len(p), nil
}
