package worker

import (
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

// getRawRange downloads bytes [start, end+margin] from MinIO.
// The extra margin ensures the last JSONL record is always complete.
func getRawRange(ctx context.Context, storage objectStorage, bucket, key string, start, end int64) ([]byte, error) {
	const margin = 1 * 1024 * 1024 // 1 MiB: enough for any single JSONL record
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(start, end+margin); err != nil {
		return nil, fmt.Errorf("SetRange [%d, %d]: %w", start, end+margin, err)
	}
	rc, err := storage.GetObject(ctx, bucket, key, opts)
	if err != nil {
		return nil, fmt.Errorf("GetObject range %s/%s [%d,%d]: %w", bucket, key, start, end, err)
	}
	defer rc.Close()
	return io.ReadAll(rc)
}
