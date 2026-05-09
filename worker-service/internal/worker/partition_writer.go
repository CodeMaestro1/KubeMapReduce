package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"

	"github.com/minio/minio-go/v7"

	pb "kubemapreduce/proto"
)

// partitionWriter streams a single mapper partition's records to its
// destination — either the ShuffleService gRPC stream (production) or object
// storage (legacy/test fallback) — without buffering the partition's full
// payload in memory. SHA-256 is computed incrementally as bytes pass through.
type partitionWriter interface {
	Write(p []byte) (int, error)
	Close() (uri, checksum string, err error)
	Abort()
}

// openPartitionWriter selects the production or fallback writer for a single
// reducer partition, opening the underlying stream lazily only when there is
// at least one record to send.
func (w *Worker) openPartitionWriter(ctx context.Context, jobID, taskID string, partIdx int) (partitionWriter, error) {
	if w.shuffleClient != nil {
		stream, err := w.shuffleClient.PushShuffleData(ctx)
		if err != nil {
			return nil, fmt.Errorf("open shuffle stream: %w", err)
		}
		return &shufflePartitionWriter{
			stream:      stream,
			jobID:       jobID,
			partitionID: int32(partIdx),
			hasher:      sha256.New(),
			buf:         make([]byte, 0, shuffleChunkSizeBytes),
		}, nil
	}
	objKey := fmt.Sprintf("%s/%s/partition-%d.jsonl", jobID, taskID, partIdx)
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		_, err := w.storage.PutObject(ctx, stagingBucket, objKey, pr, -1,
			minio.PutObjectOptions{ContentType: "application/jsonl"})
		// Ensure the writer side never blocks if PutObject returned early.
		_ = pr.CloseWithError(err)
		errCh <- err
	}()
	return &storagePartitionWriter{
		bucket: stagingBucket,
		key:    objKey,
		pw:     pw,
		hasher: sha256.New(),
		errCh:  errCh,
	}, nil
}

// shufflePartitionWriter chunks records into ~shuffleChunkSizeBytes-sized
// gRPC frames and forwards them on the ShuffleService PushShuffleData stream.
type shufflePartitionWriter struct {
	stream      pb.ShuffleService_PushShuffleDataClient
	jobID       string
	partitionID int32
	hasher      hash.Hash
	buf         []byte
	closed      bool
}

func (s *shufflePartitionWriter) Write(p []byte) (int, error) {
	s.hasher.Write(p)
	s.buf = append(s.buf, p...)
	for len(s.buf) >= shuffleChunkSizeBytes {
		chunk := s.buf[:shuffleChunkSizeBytes]
		if err := s.stream.Send(&pb.ShuffleDataChunk{
			JobId:       s.jobID,
			PartitionId: s.partitionID,
			Data:        chunk,
		}); err != nil {
			return 0, fmt.Errorf("send shuffle chunk: %w", err)
		}
		s.buf = s.buf[shuffleChunkSizeBytes:]
	}
	return len(p), nil
}

func (s *shufflePartitionWriter) Close() (string, string, error) {
	if s.closed {
		return "", "", fmt.Errorf("partition writer already closed")
	}
	s.closed = true
	if len(s.buf) > 0 {
		if err := s.stream.Send(&pb.ShuffleDataChunk{
			JobId:       s.jobID,
			PartitionId: s.partitionID,
			Data:        s.buf,
		}); err != nil {
			return "", "", fmt.Errorf("send final shuffle chunk: %w", err)
		}
		s.buf = nil
	}
	if _, err := s.stream.CloseAndRecv(); err != nil {
		return "", "", fmt.Errorf("close shuffle stream: %w", err)
	}
	return fmt.Sprintf("shuffle://%s/%d", s.jobID, s.partitionID),
		hex.EncodeToString(s.hasher.Sum(nil)), nil
}

func (s *shufflePartitionWriter) Abort() {
	if s.closed {
		return
	}
	s.closed = true
	_, _ = s.stream.CloseAndRecv()
}

// storagePartitionWriter feeds bytes into a goroutine that drives MinIO
// PutObject with size=-1, so partition data is uploaded as a multipart
// stream rather than being buffered to a single bytes.Buffer.
type storagePartitionWriter struct {
	bucket, key string
	pw          *io.PipeWriter
	hasher      hash.Hash
	errCh       chan error
	closed      bool
}

func (s *storagePartitionWriter) Write(p []byte) (int, error) {
	s.hasher.Write(p)
	return s.pw.Write(p)
}

func (s *storagePartitionWriter) Close() (string, string, error) {
	if s.closed {
		return "", "", fmt.Errorf("partition writer already closed")
	}
	s.closed = true
	if err := s.pw.Close(); err != nil {
		<-s.errCh
		return "", "", fmt.Errorf("close partition pipe: %w", err)
	}
	if err := <-s.errCh; err != nil {
		return "", "", fmt.Errorf("upload partition: %w", err)
	}
	return fmt.Sprintf("s3://%s/%s", s.bucket, s.key),
		hex.EncodeToString(s.hasher.Sum(nil)), nil
}

func (s *storagePartitionWriter) Abort() {
	if s.closed {
		return
	}
	s.closed = true
	_ = s.pw.CloseWithError(fmt.Errorf("partition aborted"))
	<-s.errCh
}
