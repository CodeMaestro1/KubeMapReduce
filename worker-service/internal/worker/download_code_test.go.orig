package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

// erroringStorage returns a fixed error from every GetObject call.
type erroringStorage struct{ err error }

func (s *erroringStorage) GetObject(_ context.Context, _, _ string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	return nil, s.err
}

func (s *erroringStorage) PutObject(_ context.Context, _, _ string, _ io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, s.err
}

// ── downloadCode ──────────────────────────────────────────────────────────────

func TestDownloadCode_PythonReturnsPathAndCleanup(t *testing.T) {
	payload := []byte("#!/usr/bin/env python3\nprint('hi')\n")
	store := &staticStorage{bucket: "mapreduce-inputs", key: "mapper.py", payload: payload}
	tempDir := t.TempDir()

	execPath, cleanup, err := downloadCode(context.Background(), store, "s3://mapreduce-inputs/mapper.py", tempDir)
	if err != nil {
		t.Fatalf("downloadCode: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup func is nil")
	}
	defer cleanup()

	if execPath != filepath.Join(tempDir, "usercode.py") {
		t.Errorf("execPath = %q, want %q", execPath, filepath.Join(tempDir, "usercode.py"))
	}

	got, err := os.ReadFile(execPath)
	if err != nil {
		t.Fatalf("read written code: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("written file content mismatch")
	}

	// File must be removable by the cleanup func.
	cleanup()
	if _, err := os.Stat(execPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("cleanup did not remove %s: stat err=%v", execPath, err)
	}
}

func TestDownloadCode_GetObjectError(t *testing.T) {
	store := &erroringStorage{err: fmt.Errorf("simulated minio failure")}
	_, _, err := downloadCode(context.Background(), store, "s3://mapreduce-inputs/missing.py", t.TempDir())
	if err == nil {
		t.Fatal("expected error from GetObject")
	}
	if !strings.Contains(err.Error(), "GetObject") {
		t.Errorf("error should wrap GetObject, got: %v", err)
	}
}

func TestDownloadCode_InvalidS3URI(t *testing.T) {
	store := &staticStorage{}
	_, _, err := downloadCode(context.Background(), store, "http://not-s3/file.py", t.TempDir())
	if err == nil {
		t.Fatal("expected parse error for non-s3 URI")
	}
}

func TestDownloadCode_CompilesCSource(t *testing.T) {
	if _, err := exec.LookPath("gcc"); err != nil {
		t.Skip("gcc not available on this host")
	}

	src := []byte(`#include <stdio.h>
int main(void) { printf("hello\n"); return 0; }
`)
	store := &staticStorage{bucket: "mapreduce-inputs", key: "mapper.c", payload: src}
	tempDir := t.TempDir()

	execPath, cleanup, err := downloadCode(context.Background(), store, "s3://mapreduce-inputs/mapper.c", tempDir)
	if err != nil {
		t.Fatalf("downloadCode: %v", err)
	}
	defer cleanup()

	// Compiled binary must exist and the .c source must have been written.
	if _, statErr := os.Stat(execPath); statErr != nil {
		t.Errorf("compiled binary missing at %s: %v", execPath, statErr)
	}
	if filepath.Ext(execPath) == ".c" {
		t.Errorf("execPath should have stripped .c extension, got %q", execPath)
	}

	cleanup()
	if _, statErr := os.Stat(execPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("cleanup did not remove compiled binary: %v", statErr)
	}
}

func TestDownloadCode_CompilesCppSource(t *testing.T) {
	if _, err := exec.LookPath("g++"); err != nil {
		t.Skip("g++ not available on this host")
	}

	src := []byte(`#include <iostream>
int main() { std::cout << "hi" << std::endl; return 0; }
`)
	store := &staticStorage{bucket: "mapreduce-inputs", key: "mapper.cpp", payload: src}
	tempDir := t.TempDir()

	execPath, cleanup, err := downloadCode(context.Background(), store, "s3://mapreduce-inputs/mapper.cpp", tempDir)
	if err != nil {
		t.Fatalf("downloadCode: %v", err)
	}
	defer cleanup()

	if _, statErr := os.Stat(execPath); statErr != nil {
		t.Errorf("compiled C++ binary missing at %s: %v", execPath, statErr)
	}
	if filepath.Ext(execPath) == ".cpp" {
		t.Errorf("execPath should have stripped .cpp extension, got %q", execPath)
	}
}

func TestDownloadCode_CompilesCcSource(t *testing.T) {
	if _, err := exec.LookPath("g++"); err != nil {
		t.Skip("g++ not available on this host")
	}

	src := []byte(`#include <iostream>
int main() { std::cout << "hi" << std::endl; return 0; }
`)
	store := &staticStorage{bucket: "mapreduce-inputs", key: "mapper.cc", payload: src}
	tempDir := t.TempDir()

	execPath, cleanup, err := downloadCode(context.Background(), store, "s3://mapreduce-inputs/mapper.cc", tempDir)
	if err != nil {
		t.Fatalf("downloadCode: %v", err)
	}
	defer cleanup()

	if _, statErr := os.Stat(execPath); statErr != nil {
		t.Errorf("compiled binary missing at %s: %v", execPath, statErr)
	}
	if filepath.Ext(execPath) == ".cc" {
		t.Errorf("execPath should have stripped .cc extension, got %q", execPath)
	}
}

func TestDownloadCode_CompilesCxxSource(t *testing.T) {
	if _, err := exec.LookPath("g++"); err != nil {
		t.Skip("g++ not available on this host")
	}

	src := []byte(`#include <iostream>
int main() { std::cout << "hi" << std::endl; return 0; }
`)
	store := &staticStorage{bucket: "mapreduce-inputs", key: "mapper.cxx", payload: src}
	tempDir := t.TempDir()

	execPath, cleanup, err := downloadCode(context.Background(), store, "s3://mapreduce-inputs/mapper.cxx", tempDir)
	if err != nil {
		t.Fatalf("downloadCode: %v", err)
	}
	defer cleanup()

	if _, statErr := os.Stat(execPath); statErr != nil {
		t.Errorf("compiled binary missing at %s: %v", execPath, statErr)
	}
	if filepath.Ext(execPath) == ".cxx" {
		t.Errorf("execPath should have stripped .cxx extension, got %q", execPath)
	}
}

func TestDownloadCode_CompileFailureReturnsError(t *testing.T) {
	if _, err := exec.LookPath("gcc"); err != nil {
		t.Skip("gcc not available on this host")
	}

	// Source that will not compile.
	src := []byte("this is not C\n")
	store := &staticStorage{bucket: "mapreduce-inputs", key: "broken.c", payload: src}

	_, _, err := downloadCode(context.Background(), store, "s3://mapreduce-inputs/broken.c", t.TempDir())
	if err == nil {
		t.Fatal("expected compile failure for invalid C source")
	}
}

// ── fetchManifest: malformed JSON ─────────────────────────────────────────────

func TestFetchManifest_InvalidJSON(t *testing.T) {
	// Serve content that is not valid JSON. No checksum fragment, so the
	// JSON decoder is the first thing to reject the payload.
	store := &staticStorage{
		bucket:  "manifests",
		key:     "job1/manifest.json",
		payload: []byte("{not valid json"),
	}

	_, err := fetchManifest(context.Background(), store, "s3://manifests/job1/manifest.json")
	if err == nil {
		t.Fatal("expected JSON decode error")
	}
	if !strings.Contains(err.Error(), "decode manifest") {
		t.Errorf("error should mention decode manifest, got: %v", err)
	}
}
