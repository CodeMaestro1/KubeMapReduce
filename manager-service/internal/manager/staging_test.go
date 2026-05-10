package manager

import (
	"context"
	"errors"
	"strings"
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

	if msc.store == nil {
		t.Fatal("expected staging object store to be configured")
	}
}

type fakeStagingObjectStore struct {
	listObjectsFn   func(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	removeObjectsFn func(ctx context.Context, bucketName string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError
}

func (f *fakeStagingObjectStore) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	return f.listObjectsFn(ctx, bucketName, opts)
}

func (f *fakeStagingObjectStore) RemoveObjects(ctx context.Context, bucketName string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
	return f.removeObjectsFn(ctx, bucketName, objectsCh, opts)
}

func TestDeleteStagingObjects_PropagatesListError(t *testing.T) {
	listErr := errors.New("list failed")
	fakeStore := &fakeStagingObjectStore{
		listObjectsFn: func(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
			ch := make(chan minio.ObjectInfo, 1)
			ch <- minio.ObjectInfo{Err: listErr}
			close(ch)
			return ch
		},
		removeObjectsFn: func(ctx context.Context, bucketName string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
			for range objectsCh {
			}
			ch := make(chan minio.RemoveObjectError)
			close(ch)
			return ch
		},
	}

	cleaner := newMinioStagingCleanerWithStore(fakeStore, stagingBucketName)
	err := cleaner.DeleteStagingObjects(context.Background(), "job-1")
	if err == nil {
		t.Fatal("expected list error, got nil")
	}
	if !strings.Contains(err.Error(), "list staging objects") {
		t.Fatalf("expected list error context, got: %v", err)
	}
	if !strings.Contains(err.Error(), listErr.Error()) {
		t.Fatalf("expected wrapped list error, got: %v", err)
	}
}

func TestDeleteStagingObjects_Success(t *testing.T) {
	fakeStore := &fakeStagingObjectStore{
		listObjectsFn: func(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
			ch := make(chan minio.ObjectInfo, 2)
			ch <- minio.ObjectInfo{Key: "job-1/a"}
			ch <- minio.ObjectInfo{Key: "job-1/b"}
			close(ch)
			return ch
		},
		removeObjectsFn: func(ctx context.Context, bucketName string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
			for range objectsCh {
			}
			ch := make(chan minio.RemoveObjectError)
			close(ch)
			return ch
		},
	}

	cleaner := newMinioStagingCleanerWithStore(fakeStore, stagingBucketName)
	if err := cleaner.DeleteStagingObjects(context.Background(), "job-1"); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}
