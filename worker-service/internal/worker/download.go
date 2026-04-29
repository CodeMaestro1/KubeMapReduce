package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
)

const splitReadChunkSize int64 = 1 << 20

// parseS3URI splits "s3://bucket/key/path" into (bucket, key).
func parseS3URI(uri string) (bucket, key string, err error) {
	if !strings.HasPrefix(uri, "s3://") {
		return "", "", fmt.Errorf("not an s3 URI: %q", uri)
	}
	rest := strings.TrimPrefix(uri, "s3://")
	idx := strings.IndexByte(rest, '/')
	if idx == -1 {
		return rest, "", nil
	}
	return rest[:idx], rest[idx+1:], nil
}

// fetchManifest downloads a data_locations manifest from MinIO.
// The manifest JSON has the shape {"data_locations": ["s3://...", ...]}.
func fetchManifest(ctx context.Context, storage objectStorage, manifestURI string) ([]string, error) {
	bucket, key, err := parseS3URI(manifestURI)
	if err != nil {
		return nil, err
	}
	rc, err := storage.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("GetObject manifest %s: %w", manifestURI, err)
	}
	defer rc.Close()

	var m struct {
		DataLocations []string `json:"data_locations"`
	}
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return m.DataLocations, nil
}

// downloadCode fetches user code from MinIO and prepares it for execution.
// For C/C++ it compiles the source. Returns the path to run and a cleanup func.
func downloadCode(ctx context.Context, storage objectStorage, codeURI, tempDir string) (execPath string, cleanup func(), err error) {
	bucket, key, parseErr := parseS3URI(codeURI)
	if parseErr != nil {
		return "", func() {}, parseErr
	}

	rc, err := storage.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return "", func() {}, fmt.Errorf("GetObject code %s: %w", codeURI, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return "", func() {}, fmt.Errorf("read code: %w", err)
	}

	baseName := filepath.Base(key)
	codePath := filepath.Join(tempDir, baseName)
	if err := os.WriteFile(codePath, data, 0o755); err != nil {
		return "", func() {}, fmt.Errorf("write code: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(key))
	switch ext {
	case ".c":
		outPath := strings.TrimSuffix(codePath, ext)
		if compileErr := compileC(codePath, outPath); compileErr != nil {
			os.Remove(codePath)
			return "", func() {}, compileErr
		}
		return outPath, func() { os.Remove(codePath); os.Remove(outPath) }, nil
	case ".cpp":
		outPath := strings.TrimSuffix(codePath, ext)
		if compileErr := compileCpp(codePath, outPath); compileErr != nil {
			os.Remove(codePath)
			return "", func() {}, compileErr
		}
		return outPath, func() { os.Remove(codePath); os.Remove(outPath) }, nil
	default:
		return codePath, func() { os.Remove(codePath) }, nil
	}
}

// validateChecksum returns an error if the SHA-256 of data does not match expectedHex.
// A blank expectedHex is treated as "no validation required".
func validateChecksum(data []byte, expectedHex string) error {
	if expectedHex == "" {
		return nil
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	if got != expectedHex {
		return fmt.Errorf("SHA-256 mismatch: got %s, want %s", got, expectedHex)
	}
	return nil
}

// splitChecksumURI splits a URI of the form "s3://bucket/key#sha256=<hex>"
// into the base URI and the hex checksum. If no fragment is present the
// checksum is returned as an empty string.
func splitChecksumURI(raw string) (uri, checksum string) {
	idx := strings.LastIndexByte(raw, '#')
	if idx < 0 {
		return raw, ""
	}
	fragment := raw[idx+1:]
	if after, ok := strings.CutPrefix(fragment, "sha256="); ok {
		return raw[:idx], after
	}
	return raw, ""
}

// getRawRange downloads bytes starting at byteStart and keeps extending the
// read in fixed chunks until it reaches the newline that terminates the record
// crossing byteEnd.
func getRawRange(ctx context.Context, storage objectStorage, bucket, key string, start, end int64) ([]byte, error) {
	if end < start {
		return nil, fmt.Errorf("invalid range [%d, %d]", start, end)
	}

	var raw bytes.Buffer
	nextStart := start
	for {
		nextEnd := nextStart + splitReadChunkSize - 1
		opts := minio.GetObjectOptions{}
		if err := opts.SetRange(nextStart, nextEnd); err != nil {
			return nil, fmt.Errorf("SetRange [%d, %d]: %w", nextStart, nextEnd, err)
		}

		rc, err := storage.GetObject(ctx, bucket, key, opts)
		if err != nil {
			return nil, fmt.Errorf("GetObject range %s/%s [%d,%d]: %w", bucket, key, nextStart, nextEnd, err)
		}

		chunk, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read range %s/%s [%d,%d]: %w", bucket, key, nextStart, nextEnd, readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close range %s/%s [%d,%d]: %w", bucket, key, nextStart, nextEnd, closeErr)
		}

		if len(chunk) == 0 {
			return raw.Bytes(), nil
		}

		raw.Write(chunk)
		if bytes.IndexByte(chunk, '\n') >= 0 {
			return raw.Bytes(), nil
		}

		if int64(len(chunk)) < splitReadChunkSize {
			return nil, fmt.Errorf("split %s/%s reached EOF before newline after byte_end %d", bucket, key, end)
		}

		nextStart += int64(len(chunk))
	}
}
