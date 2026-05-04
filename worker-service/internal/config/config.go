package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config holds all environment-sourced configuration for the Worker.
// TASK_ID, ATTEMPT_ID, and MANAGER_ADDR are injected by KubeOrchestrator.
//
// MinIO credentials can be provided via either:
// 1. Environment variables (legacy): S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY or MINIO_* variants
// 2. Files (recommended for Kubernetes): /etc/worker-secrets/{endpoint,access-key,secret-key}
//
// The file-based approach is preferred in production to prevent credential exposure.
type Config struct {
	TaskID    string
	AttemptID string

	// ManagerAddr is the gRPC address of the Manager service.
	ManagerAddr string

	// WorkerRPCToken is sent as "x-worker-token" gRPC metadata if non-empty.
	WorkerRPCToken  string
	GRPCTLSCertFile string

	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	MinioUseSSL    bool

	// HeartbeatIntervalSec controls how often the worker sends Heartbeat RPCs.
	HeartbeatIntervalSec int

	// MapSortSpillThresholdMB is the in-memory record budget before sort spills to disk.
	MapSortSpillThresholdMB int

	// ShuffleBatchSize is the maximum number of concurrent open streams per merge pass.
	// Maps to SHUFFLE_BATCH_SIZE env var. Defaults to 500.
	ShuffleBatchSize int

	// ShuffleMaxRecordBytes is the maximum allowed size for a single JSONL line during merge.
	// Maps to SHUFFLE_MAX_RECORD_BYTES env var. Defaults to 1 MiB.
	ShuffleMaxRecordBytes int

	TempDir string
}

const (
	secretMountPath = "/etc/worker-secrets"
	endpointFile    = secretMountPath + "/endpoint"
	accessKeyFile   = secretMountPath + "/access-key"
	secretKeyFile   = secretMountPath + "/secret-key"
)

// readSecretFile attempts to read a secret from a file.
// Returns empty string if the file doesn't exist (graceful fallback to env vars).
func readSecretFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil // File doesn't exist; allow fallback to env vars
		}
		return "", fmt.Errorf("failed to read secret file %s: %w", path, err)
	}
	return strings.TrimSpace(string(data)), nil
}

func Load() (*Config, error) {
	taskID := strings.TrimSpace(os.Getenv("TASK_ID"))
	if taskID == "" {
		return nil, fmt.Errorf("TASK_ID is required")
	}
	attemptID := strings.TrimSpace(os.Getenv("ATTEMPT_ID"))
	if attemptID == "" {
		return nil, fmt.Errorf("ATTEMPT_ID is required")
	}
	managerAddr := strings.TrimSpace(os.Getenv("MANAGER_ADDR"))
	if managerAddr == "" {
		return nil, fmt.Errorf("MANAGER_ADDR is required")
	}

	hb, err := getEnvInt("HEARTBEAT_INTERVAL_SEC", 10)
	if err != nil {
		return nil, err
	}
	spill, err := getEnvInt("MAP_SORT_SPILL_THRESHOLD_MB", 256)
	if err != nil {
		return nil, err
	}
	shuffleBatch, err := getEnvInt("SHUFFLE_BATCH_SIZE", 500)
	if err != nil {
		return nil, err
	}
	shuffleMaxRecord, err := getEnvInt("SHUFFLE_MAX_RECORD_BYTES", 1*1024*1024)
	if err != nil {
		return nil, err
	}

	tempDir := strings.TrimSpace(os.Getenv("WORKER_TEMP_DIR"))
	if tempDir == "" {
		tempDir = os.TempDir()
	} else {
		if strings.Contains(tempDir, "..") {
			return nil, fmt.Errorf("WORKER_TEMP_DIR contains directory traversal characters")
		}
		tempDir = filepath.Clean(tempDir)
		if !filepath.IsAbs(tempDir) {
			return nil, fmt.Errorf("WORKER_TEMP_DIR must be an absolute path")
		}
	}

	// Load MinIO credentials: prefer file-based secrets, fall back to env vars
	minioEndpoint, err := readSecretFile(endpointFile)
	if err != nil {
		return nil, err
	}
	if minioEndpoint == "" {
		minioEndpoint = firstNonEmptyEnv("S3_ENDPOINT", "MINIO_ENDPOINT")
	}

	minioAccessKey, err := readSecretFile(accessKeyFile)
	if err != nil {
		return nil, err
	}
	if minioAccessKey == "" {
		minioAccessKey = firstNonEmptyEnv("S3_ACCESS_KEY", "MINIO_ACCESS_KEY")
	}

	minioSecretKey, err := readSecretFile(secretKeyFile)
	if err != nil {
		return nil, err
	}
	if minioSecretKey == "" {
		minioSecretKey = firstNonEmptyEnv("S3_SECRET_KEY", "MINIO_SECRET_KEY")
	}

	return &Config{
		TaskID:                  taskID,
		AttemptID:               attemptID,
		ManagerAddr:             managerAddr,
		WorkerRPCToken:          strings.TrimSpace(os.Getenv("WORKER_RPC_TOKEN")),
		GRPCTLSCertFile:         strings.TrimSpace(os.Getenv("GRPC_TLS_CERT_FILE")),
		MinioEndpoint:           minioEndpoint,
		MinioAccessKey:          minioAccessKey,
		MinioSecretKey:          minioSecretKey,
		MinioUseSSL:             getEnvBool("MINIO_USE_SSL", false),
		HeartbeatIntervalSec:    hb,
		MapSortSpillThresholdMB: spill,
		ShuffleBatchSize:        shuffleBatch,
		ShuffleMaxRecordBytes:   shuffleMaxRecord,
		TempDir:                 tempDir,
	}, nil
}

func getEnvBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func getEnvInt(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid integer for %s: %q", key, v)
	}
	return i, nil
}
