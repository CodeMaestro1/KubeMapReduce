package manager

import (
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net/http"
	"net/http/httptest"
	"testing"
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

func TestMinioStagingCleaner_DeleteStagingObjects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
			<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			  <Name>mapreduce-staging</Name>
			  <Prefix>job-1/</Prefix>
			  <IsTruncated>false</IsTruncated>
			  <Contents>
				<Key>job-1/obj1</Key>
				<Size>1</Size>
			  </Contents>
			</ListBucketResult>`))
		} else if r.Method == "POST" {
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
			<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			  <Deleted>
				<Key>job-1/obj1</Key>
			  </Deleted>
			</DeleteResult>`))
		}
	}))
	defer server.Close()

	client, err := minio.New(server.Listener.Addr().String(), &minio.Options{
		Creds:  credentials.NewStaticV4("access", "secret", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("failed to create minio client: %v", err)
	}
	cleaner := NewMinioStagingCleaner(client)
	if err := cleaner.DeleteStagingObjects(context.Background(), "job-1"); err != nil {
		t.Fatalf("DeleteStagingObjects failed: %v", err)
	}
}
