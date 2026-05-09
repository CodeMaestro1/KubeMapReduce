package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
)

// ── extractSplitLines ─────────────────────────────────────────────────────────

func TestExtractSplitLines_FullFile(t *testing.T) {
	raw := []byte("line1\nline2\nline3\n")
	lines, err := extractSplitLines(raw, 0, int64(len(raw)-1), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"line1", "line2", "line3"}
	assertLines(t, lines, want)
}

func TestExtractSplitLines_SkipsPartialFirstLine(t *testing.T) {
	// byteStart=6 means we start mid-way through a file at offset 6.
	// raw starts at that offset: first content is the rest of a partial line.
	raw := []byte("partial\nline2\nline3\n")
	// byteStart=3 (non-zero), atLineBoundary=false triggers the skip-first-line logic.
	lines, err := extractSplitLines(raw, 3, int64(3+len(raw)-1), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// "partial" should be skipped; line2 and line3 included.
	want := []string{"line2", "line3"}
	assertLines(t, lines, want)
}

func TestExtractSplitLines_FinishesLineAfterByteEnd(t *testing.T) {
	raw := []byte("aaaa\nbbbb\ncccc\n")
	// byteEnd = 9 is in the middle of "bbbb\n" (bytes 5-9).
	// We expect both "aaaa" and "bbbb" (bbbb finishes the crossed line).
	lines, err := extractSplitLines(raw, 0, 9, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"aaaa", "bbbb"}
	assertLines(t, lines, want)
}

func TestExtractSplitLines_ExactBoundary(t *testing.T) {
	raw := []byte("aaaa\nbbbb\n")
	// byteEnd = 4 is the '\n' after "aaaa". Only "aaaa" is in range.
	lines, err := extractSplitLines(raw, 0, 4, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"aaaa"}
	assertLines(t, lines, want)
}

func TestExtractSplitLines_SingleRecord(t *testing.T) {
	raw := []byte(`{"key":"k","value":"v"}` + "\n")
	lines, err := extractSplitLines(raw, 0, int64(len(raw)-1), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d", len(lines))
	}
	if string(lines[0]) != `{"key":"k","value":"v"}` {
		t.Errorf("unexpected line: %s", lines[0])
	}
}

func TestExtractSplitLines_EmptyInput(t *testing.T) {
	lines, err := extractSplitLines([]byte{}, 0, 0, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("want 0 lines, got %d", len(lines))
	}
}

func TestExtractSplitLines_CRLFNewlines(t *testing.T) {
	raw := []byte("line1\r\nline2\r\nline3\r\n")
	lines, err := extractSplitLines(raw, 0, int64(len(raw)-1), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"line1", "line2", "line3"}
	assertLines(t, lines, want)
}

func TestExtractSplitLines_SkipLeavesNoRecords(t *testing.T) {
	// byteStart > 0, atLineBoundary=false and only one line in raw: after skipping it, nothing remains.
	raw := []byte("onlyone\n")
	lines, err := extractSplitLines(raw, 1, int64(len(raw)-1), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lines) != 0 {
		t.Fatalf("want 0 lines after skip, got %d: %q", len(lines), lines)
	}
}

func TestExtractSplitLines_AtLineBoundaryDoesNotSkip(t *testing.T) {
	// byteStart > 0 but atLineBoundary=true means the byte before byteStart
	// was '\n', so the first record in raw is a complete record and must not be skipped.
	raw := []byte("complete\nline2\n")
	// Simulate byteStart=6 sitting right after a '\n' in the original file.
	lines, err := extractSplitLines(raw, 6, int64(6+len(raw)-1), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"complete", "line2"}
	assertLines(t, lines, want)
}

func TestReadSplitRecords_ExtendsPastChunkUntilNewline(t *testing.T) {
	store := newChunkingStorage(int(splitReadChunkSize))
	largeValue := strings.Repeat("x", int(splitReadChunkSize)+128)
	record := fmt.Sprintf(`{"key":"k","value":"%s"}`+"\n", largeValue)
	store.put("mapreduce-inputs", "big.jsonl", []byte(record))

	lines, err := readSplitRecords(context.Background(), store, "s3://mapreduce-inputs/big.jsonl", 0, splitReadChunkSize/2, "")
	if err != nil {
		t.Fatalf("readSplitRecords: %v", err)
	}
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d", len(lines))
	}
	want := strings.TrimSuffix(record, "\n")
	if string(lines[0]) != want {
		t.Fatalf("truncated line: got %d bytes, want %d", len(lines[0]), len(want))
	}
}

// ── validateChecksum ──────────────────────────────────────────────────────────

func TestValidateChecksum_Match(t *testing.T) {
	data := []byte("hello world")
	sum := sha256.Sum256(data)
	hex := hex.EncodeToString(sum[:])
	if err := validateChecksum(data, hex); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateChecksum_Mismatch(t *testing.T) {
	if err := validateChecksum([]byte("hello"), "deadbeef"); err == nil {
		t.Fatal("expected error for checksum mismatch")
	}
}

func TestValidateChecksum_EmptyExpected(t *testing.T) {
	// Empty expected hex means "skip validation".
	if err := validateChecksum([]byte("anything"), ""); err != nil {
		t.Fatalf("unexpected error for empty checksum: %v", err)
	}
}

// ── parseS3URI ──────────────────────────────────────────────────────────���─────

func TestParseS3URI(t *testing.T) {
	cases := []struct {
		uri        string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{"s3://my-bucket/path/to/file.jsonl", "my-bucket", "path/to/file.jsonl", false},
		{"s3://bucket-only", "bucket-only", "", false},
		{"http://not-s3/key", "", "", true},
		{"", "", "", true},
	}
	for _, tc := range cases {
		b, k, err := parseS3URI(tc.uri)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseS3URI(%q): want error", tc.uri)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseS3URI(%q): unexpected error: %v", tc.uri, err)
			continue
		}
		if b != tc.wantBucket || k != tc.wantKey {
			t.Errorf("parseS3URI(%q): got (%q, %q), want (%q, %q)", tc.uri, b, k, tc.wantBucket, tc.wantKey)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func assertLines(t *testing.T, got [][]byte, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("line count: got %d, want %d\ngot:  %q\nwant: %q", len(got), len(want), got, want)
	}
	for i, w := range want {
		if string(got[i]) != w {
			t.Errorf("line[%d]: got %q, want %q", i, got[i], w)
		}
	}
}

type chunkingStorage struct {
	objects   map[string][]byte
	chunkSize int
	cursors   map[string]int
}

func newChunkingStorage(chunkSize int) *chunkingStorage {
	return &chunkingStorage{
		objects:   make(map[string][]byte),
		chunkSize: chunkSize,
		cursors:   make(map[string]int),
	}
}

func (s *chunkingStorage) put(bucket, key string, data []byte) {
	s.objects[bucket+"/"+key] = data
}

func (s *chunkingStorage) GetObject(_ context.Context, bucket, key string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	objKey := bucket + "/" + key
	data, ok := s.objects[objKey]
	if !ok {
		return nil, fmt.Errorf("object not found: %s", objKey)
	}
	pos := s.cursors[objKey]
	if pos >= len(data) {
		return io.NopCloser(strings.NewReader("")), nil
	}
	next := pos + s.chunkSize
	if next > len(data) {
		next = len(data)
	}
	s.cursors[objKey] = next
	return io.NopCloser(bytes.NewReader(data[pos:next])), nil
}

func (s *chunkingStorage) PutObject(_ context.Context, bucket, key string, reader io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	s.put(bucket, key, data)
	return minio.UploadInfo{Bucket: bucket, Key: key}, nil
}
