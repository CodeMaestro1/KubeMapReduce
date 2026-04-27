// Package manifest contains shared helpers for the oversized-assignment
// manifest fallback path. The Manager writes a manifest object to MinIO when a
// TaskAssignment exceeds the configured size threshold and replaces the
// data_locations field with a single URI of the form
//
//	<storage-uri>#sha256=<hex>
//
// Worker code calls Parse to extract the storage URI and checksum from the
// fragment, then FetchAndValidate to download the manifest body, verify its
// SHA-256 digest, and decode the embedded data_locations list before continuing
// the normal shuffle-merge flow.
package manifest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// fragmentPrefix is the URI fragment prefix used by the Manager to attach the
// hex-encoded SHA-256 checksum of an uploaded manifest payload.
const fragmentPrefix = "#sha256="

// Errors returned by Parse and FetchAndValidate. They are exported so worker
// code can branch on the failure mode (e.g. surface "corrupt manifest" vs.
// "malformed assignment" to the Manager via TaskFailed).
var (
	// ErrMissingChecksum indicates the manifest URI did not carry a sha256
	// fragment. The assignment is considered malformed and must be rejected.
	ErrMissingChecksum = errors.New("manifest: URI is missing #sha256= fragment")

	// ErrInvalidChecksum indicates the sha256 fragment was not valid hex of the
	// expected length.
	ErrInvalidChecksum = errors.New("manifest: sha256 fragment is not valid hex")

	// ErrChecksumMismatch indicates the fetched manifest body did not match the
	// checksum advertised in the URI fragment.
	ErrChecksumMismatch = errors.New("manifest: fetched payload checksum does not match URI fragment")

	// ErrMalformedManifest indicates the manifest body could not be decoded as a
	// valid manifest envelope (missing data_locations, invalid JSON, etc.).
	ErrMalformedManifest = errors.New("manifest: payload is malformed")
)

// Reference is a parsed manifest URI: the bare storage URI plus the expected
// SHA-256 digest of the object body.
type Reference struct {
	URI      string
	Checksum []byte
}

// envelope is the JSON shape produced by manager-service/internal/grpc when
// uploading a manifest object.
type envelope struct {
	DataLocations []string `json:"data_locations"`
}

// Parse splits a manifest URI of the form "<uri>#sha256=<hex>" into its
// components. It returns ErrMissingChecksum if the fragment is absent and
// ErrInvalidChecksum if the fragment is not 64 hex characters.
func Parse(rawURI string) (Reference, error) {
	idx := strings.Index(rawURI, fragmentPrefix)
	if idx < 0 {
		return Reference{}, ErrMissingChecksum
	}
	uri := rawURI[:idx]
	hexDigest := rawURI[idx+len(fragmentPrefix):]
	if len(hexDigest) != sha256.Size*2 {
		return Reference{}, fmt.Errorf("%w: expected %d hex chars, got %d", ErrInvalidChecksum, sha256.Size*2, len(hexDigest))
	}
	checksum, err := hex.DecodeString(hexDigest)
	if err != nil {
		return Reference{}, fmt.Errorf("%w: %v", ErrInvalidChecksum, err)
	}
	if uri == "" {
		return Reference{}, fmt.Errorf("%w: storage URI is empty", ErrMissingChecksum)
	}
	return Reference{URI: uri, Checksum: checksum}, nil
}

// Fetcher resolves a manifest storage URI and returns a reader for the object
// body. Workers typically wrap a MinIO client or an HTTP GET against a
// pre-signed URL. The returned ReadCloser is closed by FetchAndValidate.
type Fetcher interface {
	Fetch(ctx context.Context, uri string) (io.ReadCloser, error)
}

// FetcherFunc adapts a plain function to the Fetcher interface.
type FetcherFunc func(ctx context.Context, uri string) (io.ReadCloser, error)

// Fetch satisfies the Fetcher interface.
func (f FetcherFunc) Fetch(ctx context.Context, uri string) (io.ReadCloser, error) {
	return f(ctx, uri)
}

// FetchAndValidate parses rawURI, downloads the manifest body via fetcher,
// verifies the SHA-256 digest, and returns the embedded data_locations list.
// It enforces the same envelope schema produced by the Manager.
func FetchAndValidate(ctx context.Context, fetcher Fetcher, rawURI string) ([]string, error) {
	if fetcher == nil {
		return nil, errors.New("manifest: fetcher is required")
	}
	ref, err := Parse(rawURI)
	if err != nil {
		return nil, err
	}

	body, err := fetcher.Fetch(ctx, ref.URI)
	if err != nil {
		return nil, fmt.Errorf("manifest: fetch %q: %w", ref.URI, err)
	}
	defer body.Close()

	payload, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("manifest: read body: %w", err)
	}

	got := sha256.Sum256(payload)
	if !bytesEqualConstantTime(got[:], ref.Checksum) {
		return nil, ErrChecksumMismatch
	}

	var env envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedManifest, err)
	}
	if len(env.DataLocations) == 0 {
		return nil, fmt.Errorf("%w: data_locations is empty", ErrMalformedManifest)
	}
	for i, loc := range env.DataLocations {
		if strings.TrimSpace(loc) == "" {
			return nil, fmt.Errorf("%w: data_locations[%d] is blank", ErrMalformedManifest, i)
		}
	}
	return env.DataLocations, nil
}

// bytesEqualConstantTime is a small helper that avoids importing crypto/subtle
// just to compare two short digests.
func bytesEqualConstantTime(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
