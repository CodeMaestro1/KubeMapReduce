package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type uploadCapture struct {
	Method string
	Path   string
	Body   string
}

type submitScenarioResult struct {
	Payload      cliJobPayload
	PresignCalls []map[string]string
	UploadBodies map[string]string
}

func writeTempJobFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func runJobsSubmitUploadScenario(t *testing.T, withCombiner bool) {
	t.Helper()

	tmpDir := t.TempDir()
	mapperPath := writeTempJobFile(t, tmpDir, "mapper.py", "print('mapper')\n")
	reducerPath := writeTempJobFile(t, tmpDir, "reducer.py", "print('reducer')\n")
	inputPath := writeTempJobFile(t, tmpDir, "input.jsonl", "{\"key\":\"value\"}\n")
	combinerPath := ""
	if withCombiner {
		combinerPath = writeTempJobFile(t, tmpDir, "combiner.py", "print('combiner')\n")
	}

	originalGetValidToken := jobsSubmitGetValidToken
	originalDoAuthRequest := jobsSubmitDoAuthRequest
	originalDoAuthRequestExpect := jobsSubmitDoAuthRequestExpect
	originalHTTPClient := jobsSubmitHTTPClient
	defer func() {
		jobsSubmitGetValidToken = originalGetValidToken
		jobsSubmitDoAuthRequest = originalDoAuthRequest
		jobsSubmitDoAuthRequestExpect = originalDoAuthRequestExpect
		jobsSubmitHTTPClient = originalHTTPClient
	}()

	uploadBodies := map[string]string{}
	presignCalls := make([]map[string]string, 0, 4)
	uploadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upload body: %v", err)
		}
		uploadBodies[r.URL.Path] = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer uploadServer.Close()

	jobsSubmitGetValidToken = func() (string, string) {
		return "test-token", "http://api.test"
	}
	jobsSubmitHTTPClient = uploadServer.Client()
	jobsSubmitDoAuthRequest = func(method, reqURL, token string, body []byte) (*http.Response, error) {
		if method != http.MethodPost {
			t.Fatalf("presign method = %s, want POST", method)
		}
		if reqURL != "http://api.test/api/v1/uploads/presigned" {
			t.Fatalf("presign URL = %s", reqURL)
		}
		if token != "test-token" {
			t.Fatalf("presign token = %s", token)
		}
		var payload map[string]string
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("decode presign body: %v", err)
		}
		presignCalls = append(presignCalls, payload)
		uploadURL := uploadServer.URL + "/upload/" + url.PathEscape(payload["bucket"]) + "/" + url.PathEscape(payload["key"])
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(fmt.Sprintf(`{"url":%q}`, uploadURL))),
		}, nil
	}

	var payload cliJobPayload
	jobsSubmitDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		if method != http.MethodPost {
			t.Fatalf("submit method = %s, want POST", method)
		}
		if reqURL != "http://api.test/api/v1/jobs" {
			t.Fatalf("submit URL = %s", reqURL)
		}
		if token != "test-token" {
			t.Fatalf("submit token = %s", token)
		}
		if expectedStatus != http.StatusAccepted {
			t.Fatalf("expected status = %d", expectedStatus)
		}
		if failPrefix != "job submission failed" {
			t.Fatalf("unexpected fail prefix: %s", failPrefix)
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("decode submit payload: %v", err)
		}
		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(strings.NewReader(`{"jobId":"job-123"}`))}
	}

	args := []string{"--mapper", mapperPath, "--reducer", reducerPath, "--input", inputPath, "--reducers", "2"}
	if withCombiner {
		args = append(args, "--combiner", combinerPath)
	}
	cmdJobsSubmit(args)

	if len(presignCalls) != 3 && !withCombiner {
		t.Fatalf("expected 3 presign calls without combiner, got %d", len(presignCalls))
	}
	if len(presignCalls) != 4 && withCombiner {
		t.Fatalf("expected 4 presign calls with combiner, got %d", len(presignCalls))
	}

	if payload.InputChecksum == "" {
		t.Fatal("expected input checksum to be propagated")
	}
	wantInputChecksum := fmt.Sprintf("%x", sha256.Sum256([]byte("{\"key\":\"value\"}\n")))
	if payload.InputChecksum != wantInputChecksum {
		t.Fatalf("expected input checksum %q, got %q", wantInputChecksum, payload.InputChecksum)
	}
	wantInputURI := fmt.Sprintf("s3://%s/%s", presignCalls[0]["bucket"], presignCalls[0]["key"])
	wantMapperURI := fmt.Sprintf("s3://%s/%s", presignCalls[1]["bucket"], presignCalls[1]["key"])
	wantReducerURI := fmt.Sprintf("s3://%s/%s", presignCalls[2]["bucket"], presignCalls[2]["key"])
	if payload.Filename != wantInputURI {
		t.Fatalf("expected input filename %q, got %q", wantInputURI, payload.Filename)
	}
	if payload.Mapper.Artifact != wantMapperURI {
		t.Fatalf("expected mapper artifact %q, got %q", wantMapperURI, payload.Mapper.Artifact)
	}
	if payload.Reducer.Artifact != wantReducerURI {
		t.Fatalf("expected reducer artifact %q, got %q", wantReducerURI, payload.Reducer.Artifact)
	}
	if withCombiner {
		if payload.Combiner == nil {
			t.Fatal("expected combiner payload to be present")
		}
		wantCombinerURI := fmt.Sprintf("s3://%s/%s", presignCalls[3]["bucket"], presignCalls[3]["key"])
		if payload.Combiner.Artifact != wantCombinerURI {
			t.Fatalf("expected combiner artifact %q, got %q", wantCombinerURI, payload.Combiner.Artifact)
		}
	} else if payload.Combiner != nil {
		t.Fatalf("expected no combiner payload, got %+v", payload.Combiner)
	}

	if len(uploadBodies) != len(presignCalls) {
		t.Fatalf("expected %d uploaded objects, got %d", len(presignCalls), len(uploadBodies))
	}

	for _, call := range presignCalls {
		path := "/upload/" + url.PathEscape(call["bucket"]) + "/" + call["key"]
		body, ok := uploadBodies[path]
		if !ok {
			t.Fatalf("missing uploaded body for %s", path)
		}
		if body == "" {
			t.Fatalf("empty body uploaded for %s", path)
		}
	}
}

func TestPresignAndUploadFile_PresignFailure(t *testing.T) {
	originalDoAuthRequest := jobsSubmitDoAuthRequest
	originalHTTPClient := jobsSubmitHTTPClient
	defer func() {
		jobsSubmitDoAuthRequest = originalDoAuthRequest
		jobsSubmitHTTPClient = originalHTTPClient
	}()

	jobsSubmitDoAuthRequest = func(method, reqURL, token string, body []byte) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(strings.NewReader("boom"))}, nil
	}
	jobsSubmitHTTPClient = http.DefaultClient

	tmpDir := t.TempDir()
	path := writeTempJobFile(t, tmpDir, "input.jsonl", "hello\n")

	_, err := presignAndUploadFile(t.Context(), "token", "http://api.test", "inputs", "key", path)
	if err == nil || !strings.Contains(err.Error(), "presign upload request failed") {
		t.Fatalf("expected presign error, got %v", err)
	}
}

func TestPresignAndUploadFile_UploadFailure(t *testing.T) {
	originalDoAuthRequest := jobsSubmitDoAuthRequest
	originalHTTPClient := jobsSubmitHTTPClient
	defer func() {
		jobsSubmitDoAuthRequest = originalDoAuthRequest
		jobsSubmitHTTPClient = originalHTTPClient
	}()

	uploadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("denied"))
	}))
	defer uploadServer.Close()

	jobsSubmitDoAuthRequest = func(method, reqURL, token string, body []byte) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(fmt.Sprintf(`{"url":%q}`, uploadServer.URL+"/upload"))),
		}, nil
	}
	jobsSubmitHTTPClient = uploadServer.Client()

	tmpDir := t.TempDir()
	path := writeTempJobFile(t, tmpDir, "input.jsonl", "hello\n")

	_, err := presignAndUploadFile(t.Context(), "token", "http://api.test", "inputs", "key", path)
	if err == nil || !strings.Contains(err.Error(), "upload failed") {
		t.Fatalf("expected upload error, got %v", err)
	}
}

func TestCmdJobsSubmit_UploadsArtifactsWithCombiner(t *testing.T) {
	runJobsSubmitUploadScenario(t, true)
}
