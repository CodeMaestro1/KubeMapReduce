package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIsAccessExpired_NotExpired(t *testing.T) {
	tokens := StoredTokens{ExpiresAt: time.Now().Unix() + 600}
	if tokens.IsAccessExpired() {
		t.Error("expected token to be valid, got expired")
	}
}

func TestIsAccessExpired_Expired(t *testing.T) {
	tokens := StoredTokens{ExpiresAt: time.Now().Unix() - 60}
	if !tokens.IsAccessExpired() {
		t.Error("expected token to be expired, got valid")
	}
}

func TestIsAccessExpired_WithinGracePeriod(t *testing.T) {
	// Token expires in 20 seconds — within the 30-second grace window.
	tokens := StoredTokens{ExpiresAt: time.Now().Unix() + 20}
	if !tokens.IsAccessExpired() {
		t.Error("expected token within 30s grace period to be treated as expired")
	}
}

func TestTokenStorePath_ReturnsNonEmpty(t *testing.T) {
	path, err := TokenStorePath()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path == "" {
		t.Fatal("expected non-empty path")
	}
	if filepath.Base(path) != "credentials.json" {
		t.Errorf("expected filename credentials.json, got %s", filepath.Base(path))
	}
}

func TestSaveAndLoadTokens(t *testing.T) {
	// Use a temp directory to avoid touching real credentials.
	tmpDir := t.TempDir()
	origAppData := os.Getenv("APPDATA")
	origXDG := os.Getenv("XDG_CONFIG_HOME")
	t.Setenv("APPDATA", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)
	defer func() {
		os.Setenv("APPDATA", origAppData)
		os.Setenv("XDG_CONFIG_HOME", origXDG)
	}()

	original := &StoredTokens{
		AccessToken:  "acc-tok",
		RefreshToken: "ref-tok",
		ExpiresAt:    time.Now().Unix() + 300,
		ServerURL:    "http://localhost:8081",
	}

	if err := SaveTokens(original); err != nil {
		t.Fatalf("SaveTokens failed: %v", err)
	}

	loaded, err := LoadTokens()
	if err != nil {
		t.Fatalf("LoadTokens failed: %v", err)
	}

	if loaded.AccessToken != original.AccessToken {
		t.Errorf("AccessToken = %q, want %q", loaded.AccessToken, original.AccessToken)
	}
	if loaded.RefreshToken != original.RefreshToken {
		t.Errorf("RefreshToken = %q, want %q", loaded.RefreshToken, original.RefreshToken)
	}
	if loaded.ExpiresAt != original.ExpiresAt {
		t.Errorf("ExpiresAt = %d, want %d", loaded.ExpiresAt, original.ExpiresAt)
	}
	if loaded.ServerURL != original.ServerURL {
		t.Errorf("ServerURL = %q, want %q", loaded.ServerURL, original.ServerURL)
	}
}

func TestClearTokens(t *testing.T) {
	tmpDir := t.TempDir()
	origAppData := os.Getenv("APPDATA")
	origXDG := os.Getenv("XDG_CONFIG_HOME")
	t.Setenv("APPDATA", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)
	defer func() {
		os.Setenv("APPDATA", origAppData)
		os.Setenv("XDG_CONFIG_HOME", origXDG)
	}()

	// Save, then clear.
	if err := SaveTokens(&StoredTokens{AccessToken: "x"}); err != nil {
		t.Fatalf("SaveTokens failed: %v", err)
	}
	if err := ClearTokens(); err != nil {
		t.Fatalf("ClearTokens failed: %v", err)
	}

	// LoadTokens should now fail.
	_, err := LoadTokens()
	if err == nil {
		t.Fatal("expected error after clearing tokens, got nil")
	}
}

func TestClearTokens_NoFileNoop(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("APPDATA", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	// Clearing when no file exists should not error.
	if err := ClearTokens(); err != nil {
		t.Fatalf("ClearTokens on nonexistent file should not error: %v", err)
	}
}

func TestLoadTokens_NotAuthenticated(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("APPDATA", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	_, err := LoadTokens()
	if err == nil {
		t.Fatal("expected error when no credentials file exists")
	}
}

func TestSaveTokens_OverwritesExisting(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("APPDATA", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	first := &StoredTokens{AccessToken: "first"}
	second := &StoredTokens{AccessToken: "second"}

	if err := SaveTokens(first); err != nil {
		t.Fatalf("first SaveTokens failed: %v", err)
	}
	if err := SaveTokens(second); err != nil {
		t.Fatalf("second SaveTokens failed: %v", err)
	}

	loaded, err := LoadTokens()
	if err != nil {
		t.Fatalf("LoadTokens failed: %v", err)
	}
	if loaded.AccessToken != "second" {
		t.Errorf("expected overwritten token 'second', got %q", loaded.AccessToken)
	}
}

func TestLoadTokens_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("APPDATA", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	path, _ := TokenStorePath()
	os.MkdirAll(filepath.Dir(path), 0700)
	os.WriteFile(path, []byte("not-json"), 0600)

	_, err := LoadTokens()
	if err == nil {
		t.Fatal("expected error for invalid JSON credentials file")
	}
}

func TestStoredTokens_JSONRoundTrip(t *testing.T) {
	original := StoredTokens{
		AccessToken:  "at",
		RefreshToken: "rt",
		ExpiresAt:    1234567890,
		ServerURL:    "http://example.com",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded StoredTokens
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded != original {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, original)
	}
}
