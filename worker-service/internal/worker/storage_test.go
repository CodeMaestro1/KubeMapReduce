package worker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
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
