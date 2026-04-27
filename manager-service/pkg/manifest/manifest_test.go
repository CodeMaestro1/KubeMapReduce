package manifest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func makeManifestURI(uri string, payload []byte) string {
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("%s#sha256=%s", uri, hex.EncodeToString(digest[:]))
}

func staticFetcher(payload []byte, err error) Fetcher {
	return FetcherFunc(func(_ context.Context, _ string) (io.ReadCloser, error) {
		if err != nil {
			return nil, err
		}
		return io.NopCloser(strings.NewReader(string(payload))), nil
	})
}

func TestParse_Success(t *testing.T) {
	payload := []byte(`{"data_locations":["s3://bucket/a","s3://bucket/b"]}`)
	uri := "s3://mapreduce-manifests/job/attempt-manifest.json"
	ref, err := Parse(makeManifestURI(uri, payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ref.URI != uri {
		t.Fatalf("expected URI %q, got %q", uri, ref.URI)
	}
	want := sha256.Sum256(payload)
	if hex.EncodeToString(ref.Checksum) != hex.EncodeToString(want[:]) {
		t.Fatalf("checksum mismatch")
	}
}

func TestParse_MissingFragmentReturnsError(t *testing.T) {
	_, err := Parse("s3://bucket/no-fragment")
	if !errors.Is(err, ErrMissingChecksum) {
		t.Fatalf("expected ErrMissingChecksum, got %v", err)
	}
}

func TestParse_InvalidChecksumLengthReturnsError(t *testing.T) {
	_, err := Parse("s3://bucket/x#sha256=deadbeef")
	if !errors.Is(err, ErrInvalidChecksum) {
		t.Fatalf("expected ErrInvalidChecksum, got %v", err)
	}
}

func TestParse_NonHexChecksumReturnsError(t *testing.T) {
	bad := "zz" + strings.Repeat("0", 62)
	_, err := Parse("s3://bucket/x#sha256=" + bad)
	if !errors.Is(err, ErrInvalidChecksum) {
		t.Fatalf("expected ErrInvalidChecksum, got %v", err)
	}
}

func TestFetchAndValidate_Success(t *testing.T) {
	payload := []byte(`{"data_locations":["s3://bucket/a","s3://bucket/b"]}`)
	uri := makeManifestURI("s3://mapreduce-manifests/test.json", payload)

	got, err := FetchAndValidate(context.Background(), staticFetcher(payload, nil), uri)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0] != "s3://bucket/a" || got[1] != "s3://bucket/b" {
		t.Fatalf("unexpected data_locations: %v", got)
	}
}

func TestFetchAndValidate_ChecksumMismatchRejectsTamperedPayload(t *testing.T) {
	original := []byte(`{"data_locations":["s3://bucket/a"]}`)
	tampered := []byte(`{"data_locations":["s3://attacker/evil"]}`)
	// URI advertises the checksum of the original payload, but the fetcher
	// returns a tampered body. Validation must reject the swap.
	uri := makeManifestURI("s3://mapreduce-manifests/test.json", original)

	_, err := FetchAndValidate(context.Background(), staticFetcher(tampered, nil), uri)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestFetchAndValidate_MalformedJSONReturnsError(t *testing.T) {
	payload := []byte(`{not-json`)
	uri := makeManifestURI("s3://mapreduce-manifests/test.json", payload)

	_, err := FetchAndValidate(context.Background(), staticFetcher(payload, nil), uri)
	if !errors.Is(err, ErrMalformedManifest) {
		t.Fatalf("expected ErrMalformedManifest, got %v", err)
	}
}

func TestFetchAndValidate_EmptyDataLocationsReturnsError(t *testing.T) {
	payload := []byte(`{"data_locations":[]}`)
	uri := makeManifestURI("s3://mapreduce-manifests/test.json", payload)

	_, err := FetchAndValidate(context.Background(), staticFetcher(payload, nil), uri)
	if !errors.Is(err, ErrMalformedManifest) {
		t.Fatalf("expected ErrMalformedManifest, got %v", err)
	}
}

func TestFetchAndValidate_BlankEntryReturnsError(t *testing.T) {
	payload := []byte(`{"data_locations":["s3://ok","   "]}`)
	uri := makeManifestURI("s3://mapreduce-manifests/test.json", payload)

	_, err := FetchAndValidate(context.Background(), staticFetcher(payload, nil), uri)
	if !errors.Is(err, ErrMalformedManifest) {
		t.Fatalf("expected ErrMalformedManifest, got %v", err)
	}
}

func TestFetchAndValidate_FetcherErrorPropagates(t *testing.T) {
	uri := makeManifestURI("s3://mapreduce-manifests/test.json", []byte(`{"data_locations":["x"]}`))
	boom := errors.New("network down")

	_, err := FetchAndValidate(context.Background(), staticFetcher(nil, boom), uri)
	if err == nil || !strings.Contains(err.Error(), "network down") {
		t.Fatalf("expected fetcher error to propagate, got %v", err)
	}
}

func TestFetchAndValidate_NilFetcherReturnsError(t *testing.T) {
	_, err := FetchAndValidate(context.Background(), nil, "s3://x#sha256="+strings.Repeat("0", 64))
	if err == nil {
		t.Fatalf("expected error for nil fetcher")
	}
}
