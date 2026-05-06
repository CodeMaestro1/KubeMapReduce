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

// faultInjectingRoundTripper is a custom transport that can simulate
// network faults like connection resets halfway through a request.
type faultInjectingRoundTripper struct {
	// Proxies to a real http.DefaultTransport
	Transport http.RoundTripper

	// Number of requests to fail before succeeding
	FailCount int
	failed    int
}

func (f *faultInjectingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if f.failed < f.FailCount {
		f.failed++
		// Simulate a TCP connection reset or generic network error
		return nil, errors.New("read: connection reset by peer")
	}
	if f.Transport == nil {
		f.Transport = http.DefaultTransport
	}
	return f.Transport.RoundTrip(req)
}

func TestMinioStorage_PutObject_RetryOnConnectionReset(t *testing.T) {
	// The Minio client has built-in retry logic for certain network errors.
	// We want to ensure that our storage wrapper inherits or utilizes this correctly
	// when we hit a simulated connection reset mid-upload.

	var successfulPuts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bucket/retry-key" && r.Method == http.MethodPut {
			successfulPuts++
			w.Header().Set("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\"")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	endpoint := strings.TrimPrefix(server.URL, "http://")

	// Create a client with our fault-injecting transport
	transport := &faultInjectingRoundTripper{FailCount: 2} // Fail twice, succeed on 3rd try

	opts := &minio.Options{
		Creds:     credentials.NewStaticV4("user", "pass", ""),
		Secure:    false,
		Region:    "us-east-1",
		Transport: transport,
	}

	client, err := minio.New(endpoint, opts)
	if err != nil {
		t.Fatalf("failed to create minio client: %v", err)
	}

	storage := newMinioStorage(client)

	data := []byte("some resilient data")
	// PutObject should abstractly handle the retries underneath if MaxRetries allows it
	// By default, minio-go retries on connection resets up to MaxRetries (default is usually 4 for PutObject internal retryer)
	info, err := storage.PutObject(context.Background(), "bucket", "retry-key", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})

	if err != nil {
		t.Fatalf("PutObject failed despite retries: %v", err)
	}

	if info.Bucket != "bucket" || info.Key != "retry-key" {
		t.Errorf("PutObject unexpected info: %+v", info)
	}

	if successfulPuts != 1 {
		t.Errorf("Expected exactly 1 successful Put request at the server, got %d", successfulPuts)
	}
	if transport.failed != 2 {
		t.Errorf("Expected exactly 2 simulated failures, got %d", transport.failed)
	}
}
