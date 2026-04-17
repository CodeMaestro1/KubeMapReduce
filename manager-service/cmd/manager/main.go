package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"kubemapreduce/manager-service/internal/config"
	mgrpc "kubemapreduce/manager-service/internal/grpc"
	"kubemapreduce/manager-service/internal/manager"
	pb "kubemapreduce/proto"
)

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
	replicaIndex := 0
	hostname, _ := os.Hostname()
	// Usually StatefulSet pods are named like manager-0, manager-1
	if len(hostname) > 0 {
		lastChar := hostname[len(hostname)-1:]
		if idx, err := strconv.Atoi(lastChar); err == nil {
			replicaIndex = idx
		}
	}

	// 2. Initialize Scheduler
	scheduler, err := manager.NewScheduler(db, replicaIndex)
	if err != nil {
		log.Fatalf("failed to create scheduler: %v", err)
	}

	// 3. Start Reaper loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				recovered, err := scheduler.FailStaleTasks()
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
		Addr:    cfg.ServerAddr,
		Handler: mux,
	}
	go func() {
		log.Printf("HTTP health server running on %s", cfg.ServerAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	// 5. Start gRPC Server
	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to listen on gRPC port %s: %v", cfg.GRPCPort, err)
	}

	grpcServer := grpc.NewServer()
	workerServer := mgrpc.NewWorkerServer(scheduler)
	pb.RegisterWorkerServiceServer(grpcServer, workerServer)
	reflection.Register(grpcServer)

	go func() {
		log.Printf("gRPC server running on %s", cfg.GRPCPort)
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
