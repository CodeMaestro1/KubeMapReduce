package manager

import (
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
