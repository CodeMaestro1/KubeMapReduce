package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
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
		orchestrator = manager.NewKubeOrchestrator(clientset, namespace, "kubemapreduce-worker:latest")
	}

	// 3. Initialize Scheduler
	_, port, err := net.SplitHostPort(cfg.GRPCAddr)
	if err != nil {
		port = "50051"
	}
	managerAddr := resolveManagerAddr(hostname, headlessService, namespace, port)
	scheduler, err := manager.NewScheduler(db, replicaIndex, cfg.TotalReplicas, orchestrator, managerAddr, cfg.LeaseTTL)
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

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reaperCtx, reaperCancel := context.WithTimeout(ctx, 30*time.Second)
				recovered, err := scheduler.FailStaleTasks(reaperCtx)
				reaperCancel()
				if err != nil {
					log.Printf("reaper error: %v", err)
				} else if recovered > 0 {
					log.Printf("reaper recovered %d stale tasks", recovered)
				}
			}
		}
	}()

	// 4. Start HTTP Server for Health & Readiness
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /internal/jobs/{job_id}", func(w http.ResponseWriter, r *http.Request) {
		if !isAuthorizedInternalCancel(r, cfg.InternalAPIKey, cfg.AllowInsecureInternalCancelAuth) {
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
		if !isAuthorizedInternalCancel(r, cfg.InternalAPIKey, cfg.AllowInsecureInternalCancelAuth) {
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
		if !isAuthorizedInternalCancel(r, cfg.InternalAPIKey, cfg.AllowInsecureInternalCancelAuth) {
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
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(); err != nil {
			http.Error(w, "Database not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	httpSrv := &http.Server{
		Addr:              cfg.ServerAddr,
		Handler:           mux,
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

	grpcOpts := []grpc.ServerOption{
		grpc.UnaryInterceptor(workerAuthUnaryInterceptor(cfg.WorkerRPCToken)),
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
	workerServer := mgrpc.NewWorkerServer(scheduler, minioClient)
	pb.RegisterWorkerServiceServer(grpcServer, workerServer)
	if cfg.EnableGRPCReflection {
		reflection.Register(grpcServer)
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

func isAuthorizedInternalCancel(r *http.Request, expectedToken string, allowInsecureLoopback bool) bool {
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
