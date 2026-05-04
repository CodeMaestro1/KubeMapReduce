package manager

import (
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestNewMinioStagingCleaner(t *testing.T) {
	client, err := minio.New("localhost:9000", &minio.Options{})
	if err != nil {
		t.Fatalf("failed to create dummy minio client: %v", err)
	}

	cleaner := NewMinioStagingCleaner(client)

	msc, ok := cleaner.(*minioStagingCleaner)
	if !ok {
		t.Fatalf("expected NewMinioStagingCleaner to return *minioStagingCleaner, got %T", cleaner)
	}

	if msc.client != client {
		t.Errorf("expected client to be %p, got %p", client, msc.client)
	}

	if msc.bucket != stagingBucketName {
		t.Errorf("expected bucket to be %q, got %q", stagingBucketName, msc.bucket)
	}
}
