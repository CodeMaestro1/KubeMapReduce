package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

// staticStorage is a minimal objectStorage stub that serves a fixed payload for
// a single (bucket, key) pair.
type staticStorage struct {
	bucket  string
	key     string
	payload []byte
}

func (s *staticStorage) GetObject(_ context.Context, bucket, key string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	if bucket != s.bucket || key != s.key {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}
	return io.NopCloser(strings.NewReader(string(s.payload))), nil
}

func (s *staticStorage) PutObject(_ context.Context, _, _ string, _ io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, fmt.Errorf("not implemented")
}

// manifestPayload builds a valid manifest JSON payload for the given URIs.
func manifestPayload(locs []string) []byte {
	b, _ := json.Marshal(map[string][]string{"data_locations": locs})
	return b
}

// digestOf returns the lowercase hex SHA-256 of b.
func digestOf(b []byte) string {
	d := sha256.Sum256(b)
	return hex.EncodeToString(d[:])
}

func TestParseManifestURIFragment(t *testing.T) {
	cases := []struct {
		rawURI     string
		wantURI    string
		wantDigest string
	}{
		{
			rawURI:     "s3://bucket/key.json",
			wantURI:    "s3://bucket/key.json",
			wantDigest: "",
		},
		{
			rawURI:     "s3://bucket/key.json#sha256=abc123",
			wantURI:    "s3://bucket/key.json",
			wantDigest: "abc123",
		},
		{
			rawURI:     "s3://bucket/key.json#other=value",
			wantURI:    "s3://bucket/key.json",
			wantDigest: "",
		},
		{
			rawURI:     "s3://bucket/key.json#",
			wantURI:    "s3://bucket/key.json",
			wantDigest: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.rawURI, func(t *testing.T) {
			gotURI, gotDigest := parseManifestURIFragment(tc.rawURI)
			if gotURI != tc.wantURI {
				t.Errorf("URI: got %q want %q", gotURI, tc.wantURI)
			}
			if gotDigest != tc.wantDigest {
				t.Errorf("digest: got %q want %q", gotDigest, tc.wantDigest)
			}
		})
	}
}

func TestFetchManifest_ValidDigest(t *testing.T) {
	locs := []string{"s3://inputs/a.jsonl", "s3://inputs/b.jsonl"}
	payload := manifestPayload(locs)
	digest := digestOf(payload)

	store := &staticStorage{bucket: "manifests", key: "job1/manifest.json", payload: payload}
	uri := fmt.Sprintf("s3://manifests/job1/manifest.json#sha256=%s", digest)

	got, err := fetchManifest(context.Background(), store, uri)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != len(locs) {
		t.Fatalf("got %d locations, want %d", len(got), len(locs))
	}
	for i := range locs {
		if got[i] != locs[i] {
			t.Errorf("location[%d]: got %q want %q", i, got[i], locs[i])
		}
	}
}

func TestFetchManifest_CorruptedManifest(t *testing.T) {
	locs := []string{"s3://inputs/a.jsonl"}
	original := manifestPayload(locs)
	digest := digestOf(original)

	// Serve a tampered payload but embed the original digest.
	tampered := append([]byte(nil), original...)
	tampered[0] = 'X'

	store := &staticStorage{bucket: "manifests", key: "job1/manifest.json", payload: tampered}
	uri := fmt.Sprintf("s3://manifests/job1/manifest.json#sha256=%s", digest)

	_, err := fetchManifest(context.Background(), store, uri)
	if err == nil {
		t.Fatal("expected digest mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "digest mismatch") {
		t.Errorf("error should mention digest mismatch, got: %v", err)
	}
}

func TestFetchManifest_NoDigestFragment(t *testing.T) {
	locs := []string{"s3://inputs/c.jsonl"}
	payload := manifestPayload(locs)

	store := &staticStorage{bucket: "manifests", key: "job1/manifest.json", payload: payload}
	uri := "s3://manifests/job1/manifest.json" // no #sha256= fragment

	got, err := fetchManifest(context.Background(), store, uri)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != locs[0] {
		t.Errorf("got %v, want %v", got, locs)
	}
}

func TestFetchManifest_StorageError(t *testing.T) {
	store := &staticStorage{bucket: "other", key: "other.json", payload: nil}
	uri := "s3://manifests/job1/missing.json"

	_, err := fetchManifest(context.Background(), store, uri)
	if err == nil {
		t.Fatal("expected error for missing object, got nil")
	}
}

// TestDownloadCode_ChmodC verifies that downloadCode applies os.Chmod(0755)
// to the compiled binary for a .c source file.
func TestDownloadCode_ChmodC(t *testing.T) {
	if _, err := exec.LookPath("gcc"); err != nil {
		t.Skip("gcc not available; skipping C compilation test")
	}
	src := `#include <stdio.h>
int main(){return 0;}
`
	tempDir := t.TempDir()
	store := &staticStorage{
		bucket:  "mapreduce-inputs",
		key:     "mapper.c",
		payload: []byte(src),
	}

	outPath, cleanup, err := downloadCode(context.Background(), store, "s3://mapreduce-inputs/mapper.c", tempDir)
	if err != nil {
		t.Fatalf("downloadCode() error = %v", err)
	}
	defer cleanup()

	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("Stat(%s) error = %v", outPath, err)
	}
	if info.Mode()&0o111 == 0 {
		t.Errorf("compiled binary %s is not executable (mode=%o)", outPath, info.Mode())
	}
}

// TestDownloadCode_ChmodCpp verifies that downloadCode applies os.Chmod(0755)
// to the compiled binary for a .cpp source file.
func TestDownloadCode_ChmodCpp(t *testing.T) {
	if _, err := exec.LookPath("g++"); err != nil {
		t.Skip("g++ not available; skipping C++ compilation test")
	}
	src := `int main(){return 0;}
`
	tempDir := t.TempDir()
	store := &staticStorage{
		bucket:  "mapreduce-inputs",
		key:     "mapper.cpp",
		payload: []byte(src),
	}

	outPath, cleanup, err := downloadCode(context.Background(), store, "s3://mapreduce-inputs/mapper.cpp", tempDir)
	if err != nil {
		t.Fatalf("downloadCode() error = %v", err)
	}
	defer cleanup()

	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("Stat(%s) error = %v", outPath, err)
	}
	if info.Mode()&0o111 == 0 {
		t.Errorf("compiled binary %s is not executable (mode=%o)", outPath, info.Mode())
	}
}
