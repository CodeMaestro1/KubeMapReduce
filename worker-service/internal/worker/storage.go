package worker

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"
)

// objectStorage abstracts MinIO operations so the worker is testable without a live server.
type objectStorage interface {
	GetObject(ctx context.Context, bucket, key string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
}

type minioStorage struct{ client *minio.Client }

type unavailableStorage struct{ err error }

func newMinioStorage(c *minio.Client) objectStorage { return &minioStorage{client: c} }

func newUnavailableStorage(err error) objectStorage { return &unavailableStorage{err: err} }

func (m *minioStorage) GetObject(ctx context.Context, bucket, key string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	return m.client.GetObject(ctx, bucket, key, opts)
}

func (m *minioStorage) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return m.client.PutObject(ctx, bucket, key, reader, size, opts)
}

func (u *unavailableStorage) GetObject(context.Context, string, string, minio.GetObjectOptions) (io.ReadCloser, error) {
	return nil, u.err
}

func (u *unavailableStorage) PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, u.err
}
