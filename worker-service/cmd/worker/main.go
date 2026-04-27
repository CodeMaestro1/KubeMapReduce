package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"
	grpccreds "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/config"
	"kubemapreduce/worker-service/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	dialOpts := []grpc.DialOption{transportOption(cfg)}
	if cfg.WorkerRPCToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(rpcToken{cfg.WorkerRPCToken}))
	}

	conn, err := grpc.NewClient(cfg.ManagerAddr, dialOpts...)
	if err != nil {
		log.Fatalf("dial %s: %v", cfg.ManagerAddr, err)
	}
	defer conn.Close()

	var minioClient *minio.Client
	if cfg.MinioEndpoint != "" {
		minioClient, err = minio.New(cfg.MinioEndpoint, &minio.Options{
			Creds:  miniocreds.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
			Secure: cfg.MinioUseSSL,
		})
		if err != nil {
			log.Fatalf("minio: %v", err)
		}
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
