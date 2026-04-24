package main

import (
	"context"
	"fmt"
	"log"

	"github.com/minio/minio-go/v7"
)

// minioStagingCleaner bulk-deletes all objects under staging/<jobID>/ in MinIO.
// It implements manager.StagingCleaner and is used by the Scheduler to remove
// shuffle data when a job enters the Cleaning phase before its terminal state.
type minioStagingCleaner struct {
	client *minio.Client
	bucket string
}

func newMinioStagingCleaner(client *minio.Client, bucket string) *minioStagingCleaner {
	return &minioStagingCleaner{client: client, bucket: bucket}
}

// DeleteStagingPrefix removes all objects whose key starts with
// "staging/<jobID>/". It is idempotent: if the prefix is already absent the
// function returns nil.
func (m *minioStagingCleaner) DeleteStagingPrefix(ctx context.Context, jobID string) error {
	prefix := fmt.Sprintf("staging/%s/", jobID)
	objectsCh := m.client.ListObjects(ctx, m.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	errorsCh := m.client.RemoveObjects(ctx, m.bucket, objectsCh, minio.RemoveObjectsOptions{})

	var firstErr error
	for e := range errorsCh {
		if firstErr == nil {
			firstErr = fmt.Errorf("removing object %s: %v", e.ObjectName, e.Err)
		}
		log.Printf("failed to remove staging object %s for job %s: %v", e.ObjectName, jobID, e.Err)
	}
	return firstErr
}
