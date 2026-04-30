package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

// rangeAwareStorage is an objectStorage stub that honors the HTTP Range header
// set by minio.GetObjectOptions.SetRange, slicing its in-memory payload to
// exactly the requested byte range. It is used by the getRawRange tests below
// to simulate the chunked byte-range download path that the real worker
// performs against MinIO.
type rangeAwareStorage struct {
	bucket  string
	key     string
	payload []byte

	// getErr, when non-nil, is returned on every GetObject call. Used to
	// exercise the storage-error branch of getRawRange.
	getErr error

	// calls records the (start, end) of every range request, in order, so
	// tests can assert that the function read the expected number of chunks.
	calls []rangeCall
}

type rangeCall struct {
	start int64
	end   int64
}

func (s *rangeAwareStorage) GetObject(_ context.Context, bucket, key string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	if bucket != s.bucket || key != s.key {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}

	rangeHeader := opts.Header().Get("Range")
	start, end, err := parseRangeHeader(rangeHeader, int64(len(s.payload)))
	if err != nil {
		return nil, err
	}
	s.calls = append(s.calls, rangeCall{start: start, end: end})

	if start >= int64(len(s.payload)) {
		return io.NopCloser(strings.NewReader("")), nil
	}
	if end >= int64(len(s.payload)) {
		end = int64(len(s.payload)) - 1
	}
	return io.NopCloser(bytes.NewReader(s.payload[start : end+1])), nil
}

func (s *rangeAwareStorage) PutObject(_ context.Context, _, _ string, _ io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, fmt.Errorf("not implemented")
}

// parseRangeHeader extracts (start, end) from a "bytes=start-end" Range header.
func parseRangeHeader(header string, payloadLen int64) (int64, int64, error) {
	if header == "" {
		return 0, payloadLen - 1, nil
	}
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, fmt.Errorf("unsupported Range: %q", header)
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("malformed Range: %q", header)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("range start: %w", err)
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("range end: %w", err)
	}
	return start, end, nil
}

// ── getRawRange ───────────────────────────────────────────────────────────────

func TestGetRawRange_NewlineInFirstChunk(t *testing.T) {
	payload := []byte("line-0\nline-1\nline-2\n")
	store := &rangeAwareStorage{bucket: "b", key: "k", payload: payload}

	got, err := getRawRange(context.Background(), store, "b", "k", 0, int64(len("line-0")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Contains(got, []byte("\n")) {
		t.Fatalf("expected newline in raw output, got %q", got)
	}
	if len(store.calls) != 1 {
		t.Errorf("expected 1 GetObject call, got %d", len(store.calls))
	}
	if store.calls[0].start != 0 {
		t.Errorf("first call start: got %d, want 0", store.calls[0].start)
	}
}

func TestGetRawRange_ExtendsAcrossMultipleChunks(t *testing.T) {
	// Build a payload that is larger than splitReadChunkSize (1 MiB) and that
	// places its first newline only after the chunk boundary, forcing
	// getRawRange to issue at least two range requests.
	chunkSize := int(splitReadChunkSize)
	prefix := bytes.Repeat([]byte("x"), chunkSize+128)
	payload := append(prefix, []byte("\nrest\n")...)

	store := &rangeAwareStorage{bucket: "b", key: "k", payload: payload}

	got, err := getRawRange(context.Background(), store, "b", "k", 0, int64(chunkSize/2))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Contains(got, []byte("\n")) {
		t.Fatal("expected at least one newline in the merged output")
	}
	if len(store.calls) < 2 {
		t.Fatalf("expected at least 2 range requests, got %d", len(store.calls))
	}
	// Sequential range requests must be contiguous and non-overlapping.
	if store.calls[1].start != store.calls[0].start+int64(chunkSize) {
		t.Errorf("second request start: got %d, want %d",
			store.calls[1].start, store.calls[0].start+int64(chunkSize))
	}
	// Sanity: returned bytes should be a prefix of the full payload starting at
	// the requested byte_start (here 0).
	if !bytes.HasPrefix(payload, got) {
		t.Errorf("returned bytes are not a prefix of the source payload")
	}
}

func TestGetRawRange_EOFBeforeNewline(t *testing.T) {
	// Payload is shorter than splitReadChunkSize and contains no newline at all,
	// so the very first chunk hits EOF without satisfying the boundary rule.
	payload := []byte("no-newline-here-and-no-eof-marker")
	store := &rangeAwareStorage{bucket: "b", key: "k", payload: payload}

	_, err := getRawRange(context.Background(), store, "b", "k", 0, int64(len(payload)-1))
	if err == nil {
		t.Fatal("expected error when payload ends before any newline")
	}
	if !strings.Contains(err.Error(), "EOF before newline") {
		t.Errorf("error should mention EOF before newline, got: %v", err)
	}
}

func TestGetRawRange_InvalidRangeRejected(t *testing.T) {
	store := &rangeAwareStorage{bucket: "b", key: "k", payload: []byte("anything\n")}
	_, err := getRawRange(context.Background(), store, "b", "k", 10, 5)
	if err == nil {
		t.Fatal("expected error for end < start")
	}
	if !strings.Contains(err.Error(), "invalid range") {
		t.Errorf("error should mention invalid range, got: %v", err)
	}
}

func TestGetRawRange_StorageError(t *testing.T) {
	store := &rangeAwareStorage{
		bucket:  "b",
		key:     "k",
		payload: []byte("data\n"),
		getErr:  fmt.Errorf("simulated storage failure"),
	}
	_, err := getRawRange(context.Background(), store, "b", "k", 0, 3)
	if err == nil {
		t.Fatal("expected error when storage fails")
	}
	if !strings.Contains(err.Error(), "GetObject range") {
		t.Errorf("error should wrap GetObject failure, got: %v", err)
	}
}
