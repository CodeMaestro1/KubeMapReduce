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
	client *minio.Client
	bucket string
}

// NewMinioStagingCleaner returns a StagingCleaner backed by the given MinIO client.
func NewMinioStagingCleaner(client *minio.Client) StagingCleaner {
	return &minioStagingCleaner{client: client, bucket: stagingBucketName}
}

// DeleteStagingObjects removes all objects under <jobID>/ using the MinIO bulk-delete API.
// The first removal error is returned; partial deletes are safe because the reconciler retries.
func (m *minioStagingCleaner) DeleteStagingObjects(ctx context.Context, jobID string) error {
	prefix := jobID + "/"

	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		for obj := range m.client.ListObjects(ctx, m.bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			if obj.Err != nil {
				return
			}
			objectsCh <- obj
		}
	}()

	for removeErr := range m.client.RemoveObjects(ctx, m.bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if removeErr.Err != nil {
			return fmt.Errorf("remove staging object %s: %w", removeErr.ObjectName, removeErr.Err)
		}
	}
	return nil
}
