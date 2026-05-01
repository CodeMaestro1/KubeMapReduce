package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kubemapreduce/worker-service/internal/config"
)

// ── rpcToken ─────────────────────────────────────────────────────────────────

func TestRPCToken_GetRequestMetadata(t *testing.T) {
	tok := rpcToken{token: "secret-token"}
	meta, err := tok.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := meta["x-worker-token"]; got != "secret-token" {
		t.Errorf("x-worker-token: got %q, want %q", got, "secret-token")
	}
}

func TestRPCToken_RequireTransportSecurity(t *testing.T) {
	tok := rpcToken{}
	if !tok.RequireTransportSecurity() {
		t.Error("expected true")
	}
}

// ── loggerWriter ──────────────────────────────────────────────────────────────

func TestLoggerWriter_Write_StripsNewline(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := loggerWriter{logger: logger}

	n, err := w.Write([]byte("hello\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 6 {
		t.Errorf("Write: returned n=%d, want 6", n)
	}
	out := buf.String()
	if !bytes.Contains([]byte(out), []byte("hello")) {
		t.Errorf("expected log to contain 'hello', got: %s", out)
	}
	// The raw '\n' must NOT appear in the logged message value
	// (TextHandler adds its own newline at the end of each record)
	// We check that "hello\n" as a literal msg value is absent.
	if bytes.Contains([]byte(out), []byte("hello\\n")) {
		t.Errorf("newline should be stripped before logging, got: %s", out)
	}
}

func TestLoggerWriter_Write_NoNewline(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := loggerWriter{logger: logger}

	_, err := w.Write([]byte("no-newline"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("no-newline")) {
		t.Errorf("expected log to contain 'no-newline', got: %s", buf.String())
	}
}

// ── transportOption ───────────────────────────────────────────────────────────

func TestTransportOption_Insecure(t *testing.T) {
	cfg := &config.Config{} // GRPCTLSCertFile is empty
	opt := transportOption(cfg)
	if opt == nil {
		t.Error("expected non-nil DialOption")
	}
}

func TestTransportOption_TLS(t *testing.T) {
	certFile := selfSignedCertFile(t)
	cfg := &config.Config{GRPCTLSCertFile: certFile}
	opt := transportOption(cfg)
	if opt == nil {
		t.Error("expected non-nil DialOption")
	}
}

// selfSignedCertFile generates a minimal self-signed TLS certificate,
// writes it to a temp file, and returns the path.
func selfSignedCertFile(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "cert.pem")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatalf("encode pem: %v", err)
	}
	return path
}

// ── buildMinioClient ──────────────────────────────────────────────────────────

func TestBuildMinioClient_EmptyEndpoint(t *testing.T) {
	cfg := &config.Config{MinioEndpoint: ""}
	client, err := buildMinioClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != nil {
		t.Error("expected nil client when endpoint is empty")
	}
}

func TestBuildMinioClient_WithEndpoint(t *testing.T) {
	cfg := &config.Config{
		MinioEndpoint:  "localhost:9000",
		MinioAccessKey: "access",
		MinioSecretKey: "secret",
		MinioUseSSL:    false,
	}
	client, err := buildMinioClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Error("expected non-nil client")
	}
}
