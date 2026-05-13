package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpccredentials "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"kubemapreduce/manager-service/internal/config"
	mgrpc "kubemapreduce/manager-service/internal/grpc"
	"kubemapreduce/manager-service/internal/manager"
	"kubemapreduce/manager-service/internal/relay"
	"kubemapreduce/manager-service/internal/store"
	"kubemapreduce/manager-service/pkg/observability"
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

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	db.SetMaxOpenConns(cfg.DBMaxOpenConns)
	db.SetMaxIdleConns(cfg.DBMaxIdleConns)
	db.SetConnMaxLifetime(cfg.DBConnMaxLifetime)

	// For StatefulSet replica routing
	hostname, _ := os.Hostname()
	totalReplicas := cfg.TotalReplicas
	replicaIndex := resolveReplicaIndex(hostname, totalReplicas)
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
		statefulSetName := strings.TrimSpace(os.Getenv("MANAGER_STATEFULSET_NAME"))
		if statefulSetName == "" {
			statefulSetName = "manager"
		}
		if discoveredReplicas, discoverErr := discoverStatefulSetReplicas(context.Background(), clientset, namespace, statefulSetName); discoverErr != nil {
			log.Printf("[WARN] failed to discover StatefulSet replicas for %s/%s: %v; using configured STATEFULSET_REPLICAS=%d", namespace, statefulSetName, discoverErr, totalReplicas)
		} else {
			totalReplicas = discoveredReplicas
			replicaIndex = resolveReplicaIndex(hostname, totalReplicas)
		}
		orchestrator = manager.NewKubeOrchestrator(clientset, namespace, "kubemapreduce/worker:latest", cfg.WorkerSecretName).
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
			log.Fatalf("staging cleaner: failed to create minio client: %v", mcErr)
		}
		bucketCtx, bucketCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if ensureErr := ensureMinIOBuckets(bucketCtx, mc); ensureErr != nil {
			bucketCancel()
			log.Fatalf("failed to ensure MinIO buckets: %v", ensureErr)
		}
		bucketCancel()
		stagingCleaner = manager.NewMinioStagingCleaner(mc)
	}

	// 3. Initialize servers
	workerServer := mgrpc.NewWorkerServer(nil, nil, cfg.ManifestThresholdBytes)
	shuffleServer := mgrpc.NewShuffleServer()

	scheduler, err := manager.NewScheduler(db, replicaIndex, totalReplicas, orchestrator, workerServer, managerAddr, cfg.LeaseTTL, stagingCleaner)
	if err != nil {
		log.Fatalf("failed to create scheduler: %v", err)
	}
	workerServer.SetScheduler(scheduler)
	scheduler.SetLeaseClockSkewSeconds(cfg.LeaseClockSkewSeconds)

	// Background context for reaper, reconciler, and outbox relay.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Tracked across the shutdown path so we can drain the relay and close
	// the broker connection cleanly on SIGTERM.
	var (
		relaySvc       *relay.RelayService
		brokerToClose  relay.BrokerPublisher
		outboxForPurge *store.OutboxStore
		outboxWg       sync.WaitGroup
	)

	if cfg.EnableOutboxRelay {
		outboxStore := store.NewOutboxStore(db)
		// Install the SQL backoff helper used by RecordDeliveryFailures. Errors
		// are non-fatal — the relay will fall back to the function being absent
		// only if the migration has not been applied yet, which would surface
		// as failures on the first claim and is loud enough on its own.
		if err := outboxStore.EnsureBackoffFunction(ctx); err != nil {
			slog.Warn("failed to ensure outbox_backoff helper", "err", err)
		}
		emitter := manager.NewLiveEventEmitter(outboxStore)
		scheduler.SetEventEmitter(emitter)
		scheduler.SetHeartbeatEventSampleN(cfg.HeartbeatEventSampleN)

		// Choose publisher: NATS if configured, otherwise NoopPublisher.
		// Log the resolved emitter mode so operators can verify configuration.
		var publisher relay.BrokerPublisher = &relay.NoopPublisher{}
		emitterMode := "noop"
		if cfg.NATSURL != "" {
			if cfg.NATSRequireTLS && !strings.HasPrefix(cfg.NATSURL, "tls://") {
				log.Fatalf("NATS_REQUIRE_TLS=true but NATS_URL does not use tls:// scheme: %s", cfg.NATSURL)
			}
			natsPublisher, err := relay.NewNATSPublisher(cfg.NATSURL, cfg.NATSCredsFile)
			if err != nil {
				log.Fatalf("failed to create NATS publisher: %v", err)
			}
			publisher = natsPublisher
			emitterMode = "nats"
			slog.Info("NATS publisher configured", "url", cfg.NATSURL, "tls_required", cfg.NATSRequireTLS)
		} else {
			slog.Info("NATS not configured, using NoopPublisher (events logged to outbox only)")
		}

		metricsHook := &relay.MetricsHook{
			ObservePublishLatency: func(eventType string, seconds float64) {
				if m := observability.DefaultMetrics(); m != nil {
					m.EventPublishLatencySeconds.WithLabelValues(eventType).Observe(seconds)
				}
			},
			IncRetry: func(eventType string) {
				if m := observability.DefaultMetrics(); m != nil {
					m.EventRetries.WithLabelValues(eventType).Inc()
				}
			},
			IncDeadLetter: func(eventType string) {
				if m := observability.DefaultMetrics(); m != nil {
					m.EventDeadLettered.WithLabelValues(eventType).Inc()
				}
			},
			IncPublishedTotal: func(eventType string, success bool) {
				// Successful publishes are accounted for via OutboxQueueDepth
				// drainage and EventPublishLatencySeconds; failures roll up
				// through IncRetry/IncDeadLetter.
				_ = eventType
				_ = success
			},
		}

		relaySvc = relay.NewRelayService(outboxStore, publisher, relay.RelayConfig{
			Enabled:    cfg.EnableOutboxRelay,
			MaxRetries: cfg.OutboxMaxRetries,
			Interval:   cfg.OutboxRelayInterval,
			BatchSize:  cfg.OutboxBatchSize,
			Metrics:    metricsHook,
		})
		relaySvc.Start(ctx)
		brokerToClose = publisher
		outboxForPurge = outboxStore

		// Periodic queue-depth refresh so kubemapreduce_outbox_queue_depth is
		// usable for alerting independently of relay activity.
		outboxWg.Add(1)
		go func() {
			defer outboxWg.Done()
			runOutboxQueueDepthLoop(ctx, outboxStore, cfg.OutboxQueueDepthInterval)
		}()

		// Periodic purge of delivered rows so the outbox does not grow
		// unbounded under healthy operation.
		outboxWg.Add(1)
		go func() {
			defer outboxWg.Done()
			runOutboxPurgeLoop(ctx, outboxStore)
		}()

		slog.Info("outbox relay enabled",
			"emitter_mode", emitterMode,
			"batch_size", cfg.OutboxBatchSize,
			"max_retries", cfg.OutboxMaxRetries,
			"interval", cfg.OutboxRelayInterval,
			"heartbeat_sample_n", cfg.HeartbeatEventSampleN)
	}

	if err := scheduler.Recover(context.Background()); err != nil {
		log.Fatalf("failed to recover scheduler tasks: %v", err)
	}

	// 3. Start background loops
	scheduler.StartCleanupReconciler(ctx, 15*time.Second)
	startReaper(ctx, scheduler, defaultReaperInterval(cfg.LeaseTTL))

	// 4. Start HTTP Server for Health & Readiness
	mux := setupInternalMux(scheduler, db, cfg)

	httpSrv := &http.Server{
		Addr:              cfg.ServerAddr,
		Handler:           observability.RequestIDMiddleware(logger)(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
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

	if cfg.MinioEndpoint != "" && cfg.MinioAccessKey != "" && cfg.MinioSecretKey != "" {
		mc, mcErr := minio.New(cfg.MinioEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
			Secure: cfg.MinioUseSSL,
		})
		if mcErr != nil {
			log.Printf("failed to initialize minio client: %v", mcErr)
		} else {
			workerServer.SetMinioClient(mc)
		}
	}

	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			workerAuthUnaryInterceptor(cfg.WorkerRPCToken),
		),
		grpc.ChainStreamInterceptor(
			workerAuthStreamInterceptor(cfg.WorkerRPCToken),
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
	pb.RegisterWorkerServiceServer(grpcServer, workerServer)
	pb.RegisterShuffleServiceServer(grpcServer, shuffleServer)
	if cfg.EnableGRPCReflection {
		debugMode := strings.EqualFold(strings.TrimSpace(os.Getenv("DEBUG_MODE")), "true")
		if !debugMode {
			slog.Warn("gRPC reflection requested but DEBUG_MODE is not set to true; skipping")
		} else if !useTLS {
			slog.Warn("gRPC reflection requested without gRPC TLS; skipping")
		} else {
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

	cancel()        // Stop reaper, queue-depth poll, purge loop
	outboxWg.Wait() // Drain outbox goroutines before closing connections
	if relaySvc != nil {
		relaySvc.Stop()
	}
	grpcServer.GracefulStop()

	httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpCancel()
	httpSrv.Shutdown(httpCtx)

	if brokerToClose != nil {
		if err := brokerToClose.Close(); err != nil {
			slog.Warn("broker publisher close failed", "err", err)
		}
	}
	_ = outboxForPurge // referenced by runOutboxPurgeLoop goroutine

	log.Println("manager service stopped")
}

func resolveReplicaIndex(hostname string, totalReplicas int) int {
	ordinal := strings.TrimSpace(os.Getenv("STATEFULSET_ORDINAL"))
	if ordinal != "" {
		if idx, err := strconv.Atoi(ordinal); err == nil && idx >= 0 {
			if totalReplicas > 0 && idx >= totalReplicas {
				log.Printf("[WARN] STATEFULSET_ORDINAL=%d is out of range for total replicas=%d, falling back to hostname parsing", idx, totalReplicas)
				return parseReplicaIndexFromHostname(hostname, totalReplicas)
			}
			return idx
		}
		log.Printf("[WARN] invalid STATEFULSET_ORDINAL=%q, falling back to hostname parsing", ordinal)
	}
	return parseReplicaIndexFromHostname(hostname, totalReplicas)
}

func parseReplicaIndexFromHostname(hostname string, totalReplicas int) int {
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
	if totalReplicas > 0 && idx >= totalReplicas {
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
		provided := r.Header.Get("X-Internal-Token")
		return subtle.ConstantTimeCompare([]byte(provided), []byte(expectedToken)) == 1
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
	return subtle.ConstantTimeCompare([]byte(values[0]), []byte(expectedToken)) == 1
}

func workerAuthStreamInterceptor(expectedToken string) grpc.StreamServerInterceptor {
	expectedToken = strings.TrimSpace(expectedToken)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if expectedToken != "" && !isAuthorizedWorkerRPC(ss.Context(), expectedToken) {
			return status.Error(codes.Unauthenticated, "missing or invalid worker rpc token")
		}
		return handler(srv, ss)
	}
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
	// GetSystemConfig reads the current cluster-wide configuration from the DDS.
	// It is used by the GET /internal/config endpoint so the API service can
	// surface worker configuration to admin callers.
	GetSystemConfig(ctx context.Context) (manager.SystemConfigUpdate, error)
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

func discoverStatefulSetReplicas(ctx context.Context, clientset kubernetes.Interface, namespace string, name string) (int, error) {
	sts, err := clientset.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas <= 0 {
		return 0, fmt.Errorf("statefulset %s/%s has invalid replica count", namespace, name)
	}
	return int(*sts.Spec.Replicas), nil
}

// runOutboxQueueDepthLoop periodically samples the undelivered-outbox count
// and exposes it on the OutboxQueueDepth gauge so alerting works even when
// the relay loop is healthy and draining slower than producers.
func runOutboxQueueDepthLoop(ctx context.Context, store *store.OutboxStore, interval time.Duration) {
	if store == nil {
		return
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			undelivered, _, _, err := store.GetOutboxStats(ctx)
			if err != nil {
				slog.Warn("failed to sample outbox queue depth", "err", err)
				continue
			}
			if m := observability.DefaultMetrics(); m != nil {
				m.OutboxQueueDepth.Set(float64(undelivered))
			}
		}
	}
}

// runOutboxPurgeLoop deletes successfully-delivered outbox rows older than a
// retention window so the table does not grow unbounded under healthy
// operation. Runs hourly and retains the last 24 hours of delivered rows.
func runOutboxPurgeLoop(ctx context.Context, s *store.OutboxStore) {
	if s == nil {
		return
	}
	const interval = time.Hour
	const retention = 24 * time.Hour
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-retention)
			n, err := s.PurgeDeliveredOlderThan(ctx, cutoff)
			if err != nil {
				slog.Warn("outbox purge failed", "err", err)
				continue
			}
			if n > 0 {
				slog.Info("outbox purge", "rows_deleted", n, "older_than", cutoff)
			}
		}
	}
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
	mux.HandleFunc("GET /internal/config", func(w http.ResponseWriter, r *http.Request) {
		if !isAuthorizedInternalRequest(r, cfg.InternalAPIKey, cfg.AllowInsecureInternalCancelAuth) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		cfgData, err := scheduler.GetSystemConfig(r.Context())
		if err != nil {
			log.Printf("failed to read system config: %v", err)
			http.Error(w, "failed to read config", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(cfgData); err != nil {
			log.Printf("failed to encode system config: %v", err)
		}
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

// requiredMinIOBuckets lists all buckets that must exist for the platform to
// operate. Creating them on startup avoids opaque "bucket does not exist"
// errors from workers and managers during normal operation.
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
// After all buckets are confirmed, it applies the 24-hour expiry lifecycle
// policy on the temp/ prefix of mapreduce-inputs (see ensureTempLifecyclePolicy).
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
	return ensureTempLifecyclePolicy(ctx, mc)
}

// ensureTempLifecyclePolicy configures a MinIO ILM rule that automatically
// expires every object whose key starts with "temp/" after 1 day.
//
// This is the server-side implementation of the Orphaned Upload Mitigation
// requirement: when the CLI uploads a file via a presigned PUT but the job
// submission never completes (network drop, Ctrl-C, crash), the object
// stays in temp/<userID>/<filename> indefinitely.  Setting this rule
// ensures MinIO garbage-collects those orphans within 24 hours without
// any application-side cleanup.
//
// Calling SetBucketLifecycle is idempotent — it replaces the existing
// configuration atomically, so re-running the service after a restart is safe.
func ensureTempLifecyclePolicy(ctx context.Context, mc *minio.Client) error {
	const inputsBucket = "mapreduce-inputs"
	cfg := lifecycle.NewConfiguration()
	cfg.Rules = []lifecycle.Rule{
		{
			ID:     "expire-temp",
			Status: "Enabled",
			RuleFilter: lifecycle.Filter{
				Prefix: "temp/",
			},
			Expiration: lifecycle.Expiration{
				Days: 1,
			},
		},
	}
	if err := mc.SetBucketLifecycle(ctx, inputsBucket, cfg); err != nil {
		return fmt.Errorf("set lifecycle policy on %s: %w", inputsBucket, err)
	}
	slog.Info("applied 24h expiry lifecycle policy", "bucket", inputsBucket, "prefix", "temp/")
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
