package manager

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
)

const stagingBucketName = "mapreduce-staging"

// StagingCleaner deletes intermediate MapReduce objects written by map workers.
type StagingCleaner interface {
	DeleteStagingObjects(ctx context.Context, jobID string) error
}

// minioStagingCleaner implements StagingCleaner using the MinIO client.
type minioStagingCleaner struct {
	store  stagingObjectStore
	client *minio.Client
	bucket string
}

type stagingObjectStore interface {
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	RemoveObjects(ctx context.Context, bucketName string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError
}

// NewMinioStagingCleaner returns a StagingCleaner backed by the given MinIO client.
func NewMinioStagingCleaner(client *minio.Client) StagingCleaner {
	return &minioStagingCleaner{store: client, client: client, bucket: stagingBucketName}
}

func newMinioStagingCleanerWithStore(store stagingObjectStore, bucket string) *minioStagingCleaner {
	if bucket == "" {
		bucket = stagingBucketName
	}
	return &minioStagingCleaner{store: store, bucket: bucket}
}

// DeleteStagingObjects removes all objects under <jobID>/ using the MinIO bulk-delete API.
// The first removal error is returned; partial deletes are safe because the reconciler retries.
func (m *minioStagingCleaner) DeleteStagingObjects(ctx context.Context, jobID string) error {
	prefix := jobID + "/"
	if m.store == nil {
		return fmt.Errorf("staging object store is not configured")
	}

	objectsCh := make(chan minio.ObjectInfo)
	listErrCh := make(chan error, 1)
	go func() {
		defer close(objectsCh)
		for obj := range m.store.ListObjects(ctx, m.bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			if obj.Err != nil {
				listErrCh <- obj.Err
				return
			}
			select {
			case objectsCh <- obj:
			case <-ctx.Done():
				return
			}
		}
	}()

	for removeErr := range m.store.RemoveObjects(ctx, m.bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if removeErr.Err != nil {
			return fmt.Errorf("remove staging object %s: %w", removeErr.ObjectName, removeErr.Err)
		}
	}

	select {
	case err := <-listErrCh:
		return fmt.Errorf("list staging objects for prefix %s: %w", prefix, err)
	default:
	}
	return nil
}
