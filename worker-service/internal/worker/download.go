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

// parseManifestURIFragment splits a manifest URI of the form
// "s3://bucket/key#sha256=<hex>" into the bare URI and the expected
// SHA-256 hex digest.  If no fragment is present, expectedDigest is "".
// The function only recognises the "sha256=" prefix; any other fragment
// is silently ignored (expectedDigest == "").
func parseManifestURIFragment(rawURI string) (uri, expectedDigest string) {
	uri, fragment, hasFragment := strings.Cut(rawURI, "#")
	if !hasFragment {
		return rawURI, ""
	}
	if strings.HasPrefix(fragment, "sha256=") {
		expectedDigest = strings.TrimPrefix(fragment, "sha256=")
	}
	return uri, expectedDigest
}

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
//
// manifestURI may include a "#sha256=<hex>" fragment.  When the fragment is
// present the downloaded bytes are verified against the embedded digest before
// the JSON is decoded.  If the digest does not match, an error is returned and
// the manifest is not used.  URIs without the fragment are accepted as-is for
// backwards compatibility.
func fetchManifest(ctx context.Context, storage objectStorage, manifestURI string) ([]string, error) {
	uri, expectedDigest := parseManifestURIFragment(manifestURI)

	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return nil, err
	}
	rc, err := storage.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("GetObject manifest %s: %w", uri, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", uri, err)
	}

	if expectedDigest != "" {
		actual := sha256.Sum256(data)
		actualHex := hex.EncodeToString(actual[:])
		if actualHex != expectedDigest {
			return nil, fmt.Errorf("manifest digest mismatch for %s: expected sha256=%s got sha256=%s",
				uri, expectedDigest, actualHex)
		}
	}

	var m struct {
		DataLocations []string `json:"data_locations"`
	}
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
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
		if chmodErr := os.Chmod(outPath, 0o755); chmodErr != nil {
			os.Remove(codePath)
			os.Remove(outPath)
			return "", func() {}, fmt.Errorf("chmod compiled binary: %w", chmodErr)
		}
		return outPath, func() { os.Remove(codePath); os.Remove(outPath) }, nil
	case ".cpp", ".cc", ".cxx":
		outPath := strings.TrimSuffix(codePath, ext)
		if compileErr := compileCpp(codePath, outPath); compileErr != nil {
			os.Remove(codePath)
			return "", func() {}, compileErr
		}
		if chmodErr := os.Chmod(outPath, 0o755); chmodErr != nil {
			os.Remove(codePath)
			os.Remove(outPath)
			return "", func() {}, fmt.Errorf("chmod compiled binary: %w", chmodErr)
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
