package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// compile-time interface compliance checks
var _ objectStorage = (*minioStorage)(nil)
var _ objectStorage = (*unavailableStorage)(nil)

func TestUnavailableStorage_GetObject(t *testing.T) {
	sentinel := errors.New("minio unavailable")
	s := newUnavailableStorage(sentinel)

	rc, err := s.GetObject(context.Background(), "bucket", "key", minio.GetObjectOptions{})
	if rc != nil {
		t.Errorf("GetObject: expected nil reader, got %v", rc)
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("GetObject: expected sentinel error, got %v", err)
	}
}

func TestUnavailableStorage_PutObject(t *testing.T) {
	sentinel := errors.New("minio unavailable")
	s := newUnavailableStorage(sentinel)

	info, err := s.PutObject(context.Background(), "bucket", "key", strings.NewReader("data"), 4, minio.PutObjectOptions{})
	if info != (minio.UploadInfo{}) {
		t.Errorf("PutObject: expected empty UploadInfo, got %+v", info)
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("PutObject: expected sentinel error, got %v", err)
	}
}

func TestMinioStorage_GetObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bucket/key" {
			w.Header().Set("Last-Modified", "Mon, 2 Jan 2006 15:04:05 GMT")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("mock data"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	endpoint := strings.TrimPrefix(server.URL, "http://")
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("user", "pass", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("failed to create minio client: %v", err)
	}

	storage := newMinioStorage(client)

	obj, err := storage.GetObject(context.Background(), "bucket", "key", minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("GetObject error: %v", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if string(data) != "mock data" {
		t.Errorf("expected mock data, got %q", string(data))
	}
}

func TestMinioStorage_PutObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bucket/key" && r.Method == http.MethodPut {
			w.Header().Set("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\"")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	endpoint := strings.TrimPrefix(server.URL, "http://")
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("user", "pass", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("failed to create minio client: %v", err)
	}

	storage := newMinioStorage(client)

	data := []byte("")
	info, err := storage.PutObject(context.Background(), "bucket", "key", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject error: %v", err)
	}

	if info.Bucket != "bucket" || info.Key != "key" {
		t.Errorf("PutObject unexpected info: %+v", info)
	}
}
