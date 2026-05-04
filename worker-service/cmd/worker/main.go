package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"
	grpccreds "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"kubemapreduce/manager-service/pkg/observability"
	timeoutgrpc "kubemapreduce/pkg/grpc"
	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/config"
	"kubemapreduce/worker-service/internal/worker"
)

func main() {
	logger := observability.NewLogger("worker")
	slog.SetDefault(logger)
	log.SetFlags(0)
	log.SetOutput(loggerWriter{logger: logger})

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Initialize timeout configuration for per-RPC method timeouts
	timeoutCfg := timeoutgrpc.NewDefaultTimeoutConfig()
	timeoutCfg.ValidateConfig()

	dialOpts := []grpc.DialOption{
		transportOption(cfg),
		grpc.WithChainUnaryInterceptor(timeoutCfg.ClientUnaryInterceptor()),
		grpc.WithChainStreamInterceptor(timeoutCfg.ClientStreamInterceptor()),
	}
	if cfg.WorkerRPCToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(rpcToken{cfg.WorkerRPCToken}))
	}

	conn, err := grpc.NewClient(cfg.ManagerAddr, dialOpts...)
	if err != nil {
		log.Fatalf("dial %s: %v", cfg.ManagerAddr, err)
	}
	defer conn.Close()

	minioClient, err := buildMinioClient(cfg)
	if err != nil {
		log.Fatalf("minio: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w := worker.New(cfg, pb.NewWorkerServiceClient(conn), minioClient)
	if err := w.Run(ctx); err != nil {
		log.Printf("worker exited with error: %v", err)
		os.Exit(1)
	}
}

func transportOption(cfg *config.Config) grpc.DialOption {
	if cfg.GRPCTLSCertFile != "" {
		creds, err := grpccreds.NewClientTLSFromFile(cfg.GRPCTLSCertFile, "")
		if err != nil {
			log.Fatalf("TLS cert %s: %v", cfg.GRPCTLSCertFile, err)
		}
		return grpc.WithTransportCredentials(creds)
	}
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// rpcToken attaches a static bearer token to every outgoing gRPC call.
type rpcToken struct{ token string }

func (r rpcToken) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"x-worker-token": r.token}, nil
}

func (r rpcToken) RequireTransportSecurity() bool { return false }

// loggerWriter bridges legacy log.Print* calls to the structured slog
// pipeline so every line emitted by the standard logger is routed through
// the same JSON sink as the rest of the worker process.
type loggerWriter struct {
	logger *slog.Logger
}

func (w loggerWriter) Write(p []byte) (int, error) {
	msg := string(p)
	// strip a trailing newline so JSON records read cleanly
	if n := len(msg); n > 0 && msg[n-1] == '\n' {
		msg = msg[:n-1]
	}
	w.logger.Info(msg)
	return len(p), nil
}

func buildMinioClient(cfg *config.Config) (*minio.Client, error) {
	if cfg.MinioEndpoint == "" {
		return nil, nil
	}

	// The minio-go client will use the context passed to its methods for timeout control.
	// Callers must pass a context with timeout when using GetObject/PutObject.
	return minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  miniocreds.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioUseSSL,
	})
}
