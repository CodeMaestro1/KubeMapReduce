package manager

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewDBResourceConfigProvider(t *testing.T) {
	t.Run("nil database returns nil provider", func(t *testing.T) {
		provider := NewDBResourceConfigProvider(nil)
		if provider != nil {
			t.Errorf("expected nil provider for nil db, got %v", provider)
		}
	})

	t.Run("valid database returns configured provider", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create sqlmock: %v", err)
		}
		defer db.Close()

		provider := NewDBResourceConfigProvider(db)
		if provider == nil {
			t.Fatal("expected non-nil provider for valid db, got nil")
		}
		if provider.db != db {
			t.Errorf("expected provider db to be set to the provided db instance")
		}
	})
}

func TestDBResourceConfigProvider_GetWorkerResourceLimits(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	provider := NewDBResourceConfigProvider(db)

	mock.ExpectQuery("SELECT cpu_limit, memory_limit FROM SYSTEM_CONFIG WHERE config_id = 1").
		WillReturnRows(sqlmock.NewRows([]string{"cpu_limit", "memory_limit"}).AddRow("1", "512Mi"))

	cpu, mem, err := provider.GetWorkerResourceLimits(context.Background())

	if err != nil || cpu != "1" || mem != "512Mi" {
		t.Fatalf("expected 1, 512Mi, no error; got %s, %s, %v", cpu, mem, err)
	}
}
