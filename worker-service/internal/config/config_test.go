package config

import (
	"os"
	"testing"
)

func TestLoad_MissingRequired(t *testing.T) {
	os.Unsetenv("TASK_ID")
	os.Unsetenv("ATTEMPT_ID")
	os.Unsetenv("MANAGER_ADDR")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing TASK_ID")
	}
}

func TestLoad_MissingAttemptID(t *testing.T) {
	t.Setenv("TASK_ID", "task-1")
	os.Unsetenv("ATTEMPT_ID")
	os.Unsetenv("MANAGER_ADDR")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing ATTEMPT_ID")
	}
}

func TestLoad_MissingManagerAddr(t *testing.T) {
	t.Setenv("TASK_ID", "task-1")
	t.Setenv("ATTEMPT_ID", "attempt-1")
	os.Unsetenv("MANAGER_ADDR")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing MANAGER_ADDR")
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("TASK_ID", "task-abc")
	t.Setenv("ATTEMPT_ID", "attempt-xyz")
	t.Setenv("MANAGER_ADDR", "localhost:50051")
	os.Unsetenv("HEARTBEAT_INTERVAL_SEC")
	os.Unsetenv("MAP_SORT_SPILL_THRESHOLD_MB")
	os.Unsetenv("SHUFFLE_BATCH_SIZE")
	os.Unsetenv("SHUFFLE_MAX_RECORD_BYTES")
	os.Unsetenv("WORKER_RPC_TOKEN")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.TaskID != "task-abc" {
		t.Errorf("TaskID: got %q", cfg.TaskID)
	}
	if cfg.AttemptID != "attempt-xyz" {
		t.Errorf("AttemptID: got %q", cfg.AttemptID)
	}
	if cfg.HeartbeatIntervalSec != 10 {
		t.Errorf("HeartbeatIntervalSec: got %d, want 10", cfg.HeartbeatIntervalSec)
	}
	if cfg.MapSortSpillThresholdMB != 256 {
		t.Errorf("MapSortSpillThresholdMB: got %d, want 256", cfg.MapSortSpillThresholdMB)
	}
	if cfg.ShuffleBatchSize != 500 {
		t.Errorf("ShuffleBatchSize: got %d, want 500", cfg.ShuffleBatchSize)
	}
	if cfg.ShuffleMaxRecordBytes != 1*1024*1024 {
		t.Errorf("ShuffleMaxRecordBytes: got %d, want %d", cfg.ShuffleMaxRecordBytes, 1*1024*1024)
	}
}

func TestLoad_InvalidInt(t *testing.T) {
	t.Setenv("TASK_ID", "t")
	t.Setenv("ATTEMPT_ID", "a")
	t.Setenv("MANAGER_ADDR", "h:50051")
	t.Setenv("HEARTBEAT_INTERVAL_SEC", "notanint")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for invalid HEARTBEAT_INTERVAL_SEC")
	}
}

func TestLoad_AllFields(t *testing.T) {
	t.Setenv("TASK_ID", "t1")
	t.Setenv("ATTEMPT_ID", "a1")
	t.Setenv("MANAGER_ADDR", "manager:50051")
	t.Setenv("WORKER_RPC_TOKEN", "secret")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "ak")
	t.Setenv("MINIO_SECRET_KEY", "sk")
	t.Setenv("MINIO_USE_SSL", "true")
	t.Setenv("HEARTBEAT_INTERVAL_SEC", "5")
	t.Setenv("MAP_SORT_SPILL_THRESHOLD_MB", "128")
	t.Setenv("SHUFFLE_BATCH_SIZE", "200")
	t.Setenv("SHUFFLE_MAX_RECORD_BYTES", "524288")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.WorkerRPCToken != "secret" {
		t.Errorf("WorkerRPCToken: got %q", cfg.WorkerRPCToken)
	}
	if !cfg.MinioUseSSL {
		t.Error("MinioUseSSL should be true")
	}
	if cfg.HeartbeatIntervalSec != 5 {
		t.Errorf("HeartbeatIntervalSec: got %d", cfg.HeartbeatIntervalSec)
	}
	if cfg.MapSortSpillThresholdMB != 128 {
		t.Errorf("MapSortSpillThresholdMB: got %d", cfg.MapSortSpillThresholdMB)
	}
	if cfg.ShuffleBatchSize != 200 {
		t.Errorf("ShuffleBatchSize: got %d, want 200", cfg.ShuffleBatchSize)
	}
	if cfg.ShuffleMaxRecordBytes != 524288 {
		t.Errorf("ShuffleMaxRecordBytes: got %d, want 524288", cfg.ShuffleMaxRecordBytes)
	}
}
