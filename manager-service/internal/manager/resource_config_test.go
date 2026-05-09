package manager

import (
	"context"
	"database/sql"
	"regexp"
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

func TestDBResourceConfigProvider_GetLocalityKey(t *testing.T) {
	t.Run("returns empty string on nil provider", func(t *testing.T) {
		var p *DBResourceConfigProvider
		key, err := p.GetLocalityKey(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key != "" {
			t.Errorf("expected empty string, got %q", key)
		}
	})

	t.Run("returns empty string on no config found", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta(QueryGetLocalityKey)).WillReturnError(sql.ErrNoRows)

		provider := NewDBResourceConfigProvider(db)
		key, err := provider.GetLocalityKey(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key != "" {
			t.Errorf("expected empty string, got %q", key)
		}
	})

	t.Run("returns configured locality key", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta(QueryGetLocalityKey)).
			WillReturnRows(sqlmock.NewRows([]string{"locality_key"}).AddRow("topology.kubernetes.io/zone"))

		provider := NewDBResourceConfigProvider(db)
		key, err := provider.GetLocalityKey(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key != "topology.kubernetes.io/zone" {
			t.Errorf("expected zone key, got %q", key)
		}
	})
}

func TestDBResourceConfigProvider_GetLocalityLabelSelector(t *testing.T) {
	t.Run("returns empty string on nil provider", func(t *testing.T) {
		var p *DBResourceConfigProvider
		selector, err := p.GetLocalityLabelSelector(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if selector != "" {
			t.Errorf("expected empty string, got %q", selector)
		}
	})

	t.Run("returns empty string on no config found", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta(QueryGetLocalityLabelSelector)).WillReturnError(sql.ErrNoRows)

		provider := NewDBResourceConfigProvider(db)
		selector, err := provider.GetLocalityLabelSelector(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if selector != "" {
			t.Errorf("expected empty string, got %q", selector)
		}
	})

	t.Run("returns configured locality label selector", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta(QueryGetLocalityLabelSelector)).
			WillReturnRows(sqlmock.NewRows([]string{"locality_label_selector"}).AddRow("app.kubernetes.io/name=minio"))

		provider := NewDBResourceConfigProvider(db)
		selector, err := provider.GetLocalityLabelSelector(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if selector != "app.kubernetes.io/name=minio" {
			t.Errorf("expected label selector, got %q", selector)
		}
	})

	t.Run("returns default selector when empty string configured", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta(QueryGetLocalityLabelSelector)).
			WillReturnRows(sqlmock.NewRows([]string{"locality_label_selector"}).AddRow(""))

		provider := NewDBResourceConfigProvider(db)
		selector, err := provider.GetLocalityLabelSelector(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if selector != "app.kubernetes.io/name=minio" {
			t.Errorf("expected default label selector, got %q", selector)
		}
	})

	t.Run("parses complex label selector", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta(QueryGetLocalityLabelSelector)).
			WillReturnRows(sqlmock.NewRows([]string{"locality_label_selector"}).AddRow("app.kubernetes.io/name=minio,app.kubernetes.io/instance=test"))

		provider := NewDBResourceConfigProvider(db)
		selector, err := provider.GetLocalityLabelSelector(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if selector != "app.kubernetes.io/name=minio,app.kubernetes.io/instance=test" {
			t.Errorf("expected label selector, got %q", selector)
		}
	})
}
