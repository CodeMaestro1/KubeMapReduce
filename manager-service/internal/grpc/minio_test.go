package grpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestMinioManifestUploader_UploadManifest_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// MinIO sometimes issues OPTIONS or other requests
		// By answering 200 to all with right headers it might work, or it needs a fake MinIO
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusOK)
		// For PutObject response it expects an empty body
		w.Write([]byte(``))
	}))
	defer server.Close()

	endpoint := strings.TrimPrefix(server.URL, "http://")
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("mock-access", "mock-secret", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("Failed to create minio client: %v", err)
	}

	uploader := &minioManifestUploader{client: client}
	url, err := uploader.UploadManifest(context.Background(), "my-bucket", "my-object.json", []byte(`{"hello": "world"}`))
	if err != nil {
		t.Fatalf("UploadManifest failed: %v", err)
	}
	expected := "s3://my-bucket/my-object.json"
	if url != expected {
		t.Fatalf("Expected %s, got %s", expected, url)
	}
}

func TestMinioManifestUploader_UploadManifest_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	endpoint := strings.TrimPrefix(server.URL, "http://")
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("mock-access", "mock-secret", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("Failed to create minio client: %v", err)
	}

	uploader := &minioManifestUploader{client: client}
	_, err = uploader.UploadManifest(context.Background(), "my-bucket", "my-object.json", []byte(`{"hello": "world"}`))
	if err == nil {
		t.Fatalf("Expected UploadManifest to fail, but it succeeded")
	}
	if !strings.Contains(err.Error(), "failed to upload manifest") {
		t.Fatalf("Expected error message to contain 'failed to upload manifest', got: %v", err)
	}
}
