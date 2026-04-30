package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

type testExit struct {
	code int
}

func (e testExit) Error() string {
	return "exit"
}

func TestValidateReducersCount(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		if err := validateReducersCount(1); err != nil {
			t.Fatalf("expected nil error for valid reducers count, got %v", err)
		}
	})

	t.Run("zero", func(t *testing.T) {
		err := validateReducersCount(0)
		if err == nil {
			t.Fatal("expected error for zero reducers")
		}
		if !strings.Contains(err.Error(), "--reducers must be > 0") {
			t.Fatalf("expected actionable reducers message, got %q", err.Error())
		}
	})

	t.Run("negative", func(t *testing.T) {
		err := validateReducersCount(-2)
		if err == nil {
			t.Fatal("expected error for negative reducers")
		}
		if !strings.Contains(err.Error(), "--reducers must be > 0") {
			t.Fatalf("expected actionable reducers message, got %q", err.Error())
		}
	})
}

func TestCmdJobsSubmit_InvalidReducersDoesNotAttemptNetwork(t *testing.T) {
	originalGetValidToken := jobsSubmitGetValidToken
	originalDoAuthRequestExpect := jobsSubmitDoAuthRequestExpect
	originalExit := jobsSubmitExit
	originalUpload := jobsSubmitUploadFile
	defer func() {
		jobsSubmitGetValidToken = originalGetValidToken
		jobsSubmitDoAuthRequestExpect = originalDoAuthRequestExpect
		jobsSubmitExit = originalExit
		jobsSubmitUploadFile = originalUpload
	}()

	getTokenCalled := false
	doRequestCalled := false
	uploadCalled := false

	jobsSubmitGetValidToken = func() (string, string) {
		getTokenCalled = true
		return "", ""
	}
	jobsSubmitDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		doRequestCalled = true
		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(bytes.NewBufferString(`{"ok":true}`))}
	}
	jobsSubmitUploadFile = func(token, serverURL, bucket, key, localPath string) (string, string, error) {
		uploadCalled = true
		return "", "", nil
	}
	jobsSubmitExit = func(code int) {
		panic(testExit{code: code})
	}

	originalStderr := os.Stderr
	readStderr, writeStderr, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create stderr pipe: %v", err)
	}
	os.Stderr = writeStderr
	defer func() {
		os.Stderr = originalStderr
		_ = writeStderr.Close()
		_ = readStderr.Close()
	}()

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected command to exit for invalid reducers")
			}
			exitErr, ok := r.(testExit)
			if !ok {
				t.Fatalf("expected testExit panic, got %T", r)
			}
			if exitErr.code != 1 {
				t.Fatalf("expected exit code 1, got %d", exitErr.code)
			}
		}()
		cmdJobsSubmit([]string{"--mapper", "mapper.py", "--reducer", "reducer.py", "--input", "input.jsonl", "--reducers", "0"})
	}()

	_ = writeStderr.Close()
	stderrBytes, readErr := io.ReadAll(readStderr)
	if readErr != nil {
		t.Fatalf("failed to read stderr: %v", readErr)
	}
	stderrText := string(stderrBytes)
	if !strings.Contains(stderrText, "--reducers must be > 0") {
		t.Fatalf("expected actionable reducers error in stderr, got %q", stderrText)
	}

	if getTokenCalled {
		t.Fatal("expected auth/token retrieval not to be called for invalid reducers")
	}
	if doRequestCalled {
		t.Fatal("expected HTTP request not to be attempted for invalid reducers")
	}
	if uploadCalled {
		t.Fatal("expected upload not to be called for invalid reducers")
	}
}

func TestCmdJobsSubmit_ValidReducersCallsSubmit(t *testing.T) {
	originalGetValidToken := jobsSubmitGetValidToken
	originalDoAuthRequestExpect := jobsSubmitDoAuthRequestExpect
	originalUpload := jobsSubmitUploadFile
	defer func() {
		jobsSubmitGetValidToken = originalGetValidToken
		jobsSubmitDoAuthRequestExpect = originalDoAuthRequestExpect
		jobsSubmitUploadFile = originalUpload
	}()

	getTokenCalled := false
	doRequestCalled := false
	uploadCount := 0

	jobsSubmitGetValidToken = func() (string, string) {
		getTokenCalled = true
		return "test-token", "http://example.test"
	}

	jobsSubmitUploadFile = func(token, serverURL, bucket, key, localPath string) (string, string, error) {
		uploadCount++
		uri := fmt.Sprintf("s3://%s/%s", bucket, key)
		return uri, "deadbeef", nil
	}

	jobsSubmitDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		doRequestCalled = true
		if method != http.MethodPost {
			t.Fatalf("expected POST, got %s", method)
		}
		if reqURL != "http://example.test/api/v1/jobs" {
			t.Fatalf("unexpected URL: %s", reqURL)
		}
		if token != "test-token" {
			t.Fatalf("unexpected token: %s", token)
		}
		if expectedStatus != http.StatusAccepted {
			t.Fatalf("unexpected expected status: %d", expectedStatus)
		}
		if !strings.Contains(string(body), `"reducers":2`) {
			t.Fatalf("expected reducers field in payload, got %s", string(body))
		}
		if !strings.Contains(string(body), `"inputChecksum"`) {
			t.Fatalf("expected inputChecksum field in payload, got %s", string(body))
		}

		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(bytes.NewBufferString(`{"jobId":"abc"}`))}
	}

	cmdJobsSubmit([]string{"--mapper", "mapper.py", "--reducer", "reducer.py", "--input", "input.jsonl", "--reducers", "2"})

	if !getTokenCalled {
		t.Fatal("expected auth/token retrieval for valid reducers")
	}
	if !doRequestCalled {
		t.Fatal("expected HTTP request for valid reducers")
	}
	if uploadCount != 3 {
		t.Fatalf("expected 3 uploads (mapper, reducer, input), got %d", uploadCount)
	}
}

func TestCmdJobsSubmit_UploadFlow(t *testing.T) {
	originalGetValidToken := jobsSubmitGetValidToken
	originalDoAuthRequestExpect := jobsSubmitDoAuthRequestExpect
	originalUpload := jobsSubmitUploadFile
	defer func() {
		jobsSubmitGetValidToken = originalGetValidToken
		jobsSubmitDoAuthRequestExpect = originalDoAuthRequestExpect
		jobsSubmitUploadFile = originalUpload
	}()

	type uploadCall struct {
		bucket    string
		key       string
		localPath string
	}
	var uploads []uploadCall

	jobsSubmitGetValidToken = func() (string, string) {
		// Return a minimal JWT-shaped token with sub claim.
		// header.payload.sig — payload is base64url({"sub":"user-42"})
		return "x.eyJzdWIiOiJ1c2VyLTQyIn0.x", "http://api.test"
	}

	jobsSubmitUploadFile = func(token, serverURL, bucket, key, localPath string) (string, string, error) {
		uploads = append(uploads, uploadCall{bucket, key, localPath})
		return fmt.Sprintf("s3://%s/%s", bucket, key), "aabbcc", nil
	}

	jobsSubmitDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		var payload cliJobPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}

		// Mapper artifact must be a MinIO URI, not a bare basename.
		if !strings.HasPrefix(payload.Mapper.Artifact, "s3://") {
			t.Errorf("mapper artifact not a MinIO URI: %q", payload.Mapper.Artifact)
		}
		if !strings.HasPrefix(payload.Reducer.Artifact, "s3://") {
			t.Errorf("reducer artifact not a MinIO URI: %q", payload.Reducer.Artifact)
		}
		// Input filename is still the basename (server constructs full URI).
		if strings.HasPrefix(payload.Filename, "s3://") {
			t.Errorf("filename should be basename, got: %q", payload.Filename)
		}
		if payload.InputChecksum == "" {
			t.Error("inputChecksum must be set in payload")
		}
		// Mapper artifact must embed user ID from token sub claim.
		if !strings.Contains(payload.Mapper.Artifact, "user-42") {
			t.Errorf("mapper artifact should contain user ID, got: %q", payload.Mapper.Artifact)
		}

		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(bytes.NewBufferString(`{"jobId":"j1"}`))}
	}

	cmdJobsSubmit([]string{"--mapper", "mapper.py", "--reducer", "reducer.py", "--input", "input.jsonl", "--reducers", "1"})

	if len(uploads) != 3 {
		t.Fatalf("expected 3 uploads, got %d: %v", len(uploads), uploads)
	}

	// mapper upload uses code bucket with temp/<userID>/ prefix
	if !strings.Contains(uploads[0].key, "user-42") {
		t.Errorf("mapper key should contain user ID, got %q", uploads[0].key)
	}
	// input upload key is just the basename
	if uploads[2].key != "input.jsonl" {
		t.Errorf("input key should be basename, got %q", uploads[2].key)
	}
}

func TestCmdJobsSubmit_CombinerUploaded(t *testing.T) {
	originalGetValidToken := jobsSubmitGetValidToken
	originalDoAuthRequestExpect := jobsSubmitDoAuthRequestExpect
	originalUpload := jobsSubmitUploadFile
	defer func() {
		jobsSubmitGetValidToken = originalGetValidToken
		jobsSubmitDoAuthRequestExpect = originalDoAuthRequestExpect
		jobsSubmitUploadFile = originalUpload
	}()

	uploadCount := 0
	jobsSubmitGetValidToken = func() (string, string) { return "tok", "http://srv.test" }
	jobsSubmitUploadFile = func(token, serverURL, bucket, key, localPath string) (string, string, error) {
		uploadCount++
		return fmt.Sprintf("s3://%s/%s", bucket, key), "sum", nil
	}
	jobsSubmitDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		var payload cliJobPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if payload.Combiner == nil {
			t.Error("combiner must be set in payload")
		} else if !strings.HasPrefix(payload.Combiner.Artifact, "s3://") {
			t.Errorf("combiner artifact not a MinIO URI: %q", payload.Combiner.Artifact)
		}
		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(bytes.NewBufferString(`{"jobId":"j2"}`))}
	}

	cmdJobsSubmit([]string{
		"--mapper", "mapper.py",
		"--reducer", "reducer.py",
		"--combiner", "combiner.py",
		"--input", "input.jsonl",
	})

	if uploadCount != 4 {
		t.Fatalf("expected 4 uploads (mapper, reducer, combiner, input), got %d", uploadCount)
	}
}

func TestUploadFileToStorage_PresignAndPUT(t *testing.T) {
	// Create a temp file to upload.
	content := []byte("hello mapreduce")
	tmpFile, err := os.CreateTemp(t.TempDir(), "upload-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	putReceived := false
	var putBody []byte

	// Test HTTP server: handles presign POST and PUT upload.
	// doAuthRequest is a real function that passes through to cliHTTPClient,
	// so we point both calls at the same test server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/files/presign-upload":
			var req map[string]string
			json.NewDecoder(r.Body).Decode(&req)
			if req["bucket"] != "test-bucket" || req["key"] != "test-key" {
				http.Error(w, "bad presign request", http.StatusBadRequest)
				return
			}
			putURL := fmt.Sprintf("http://%s/minio/put", r.Host)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"url": putURL})

		case r.Method == http.MethodPut && r.URL.Path == "/minio/put":
			putReceived = true
			putBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)

		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer srv.Close()

	// Replace cliHTTPClient so both presign and PUT go to the test server.
	origClient := cliHTTPClient
	cliHTTPClient = srv.Client()
	defer func() { cliHTTPClient = origClient }()

	minioURI, checksum, err := uploadFileToStorage("fake-token", srv.URL, "test-bucket", "test-key", tmpFile.Name())
	if err != nil {
		t.Fatalf("uploadFileToStorage: %v", err)
	}

	if minioURI != "s3://test-bucket/test-key" {
		t.Errorf("minioURI = %q, want %q", minioURI, "s3://test-bucket/test-key")
	}
	if checksum == "" {
		t.Error("checksum must not be empty")
	}
	if !putReceived {
		t.Error("PUT request not received by storage server")
	}
	if !bytes.Equal(putBody, content) {
		t.Errorf("PUT body = %q, want %q", putBody, content)
	}
}

func TestCmdJobsCancel_NoJobIDFlagExits(t *testing.T) {
	originalExit := jobsCancelExit
	originalGetValidToken := jobsCancelGetValidToken
	defer func() {
		jobsCancelExit = originalExit
		jobsCancelGetValidToken = originalGetValidToken
	}()

	exitCode := 0
	jobsCancelExit = func(code int) {
		exitCode = code
		panic(testExit{code: code})
	}

	err := catchPanic(func() {
		cmdJobsCancel([]string{})
	})

	if err == nil {
		t.Fatal("expected panic from os.Exit")
	}
	if exitCode != 1 {
		t.Errorf("exit code = %d, want 1", exitCode)
	}
}

func TestCmdJobsCancel_DeletesJobAndDisplaysConfirmation(t *testing.T) {
	originalGetValidToken := jobsCancelGetValidToken
	originalDoAuthRequest := jobsCancelDoAuthRequest
	defer func() {
		jobsCancelGetValidToken = originalGetValidToken
		jobsCancelDoAuthRequest = originalDoAuthRequest
	}()

	getTokenCalled := false
	deleteRequestReceived := false
	requestURL := ""

	jobsCancelGetValidToken = func() (string, string) {
		getTokenCalled = true
		return "test-token", "http://api.test"
	}

	jobsCancelDoAuthRequest = func(method, url, token string, body []byte) (*http.Response, error) {
		requestURL = url
		deleteRequestReceived = method == http.MethodDelete
		// Return mock 204 response
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(bytes.NewBufferString("")),
		}, nil
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdJobsCancel([]string{"--id", "job-123"})

	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)

	if !getTokenCalled {
		t.Error("jobsCancelGetValidToken was not called")
	}
	if !deleteRequestReceived {
		t.Error("DELETE request was not sent")
	}
	if !strings.Contains(requestURL, "job-123") {
		t.Errorf("request URL = %q, expected to contain 'job-123'", requestURL)
	}
	if !strings.Contains(string(output), "cancelled") {
		t.Errorf("output = %q, expected to contain 'cancelled'", string(output))
	}
}

func TestCmdJobsCancel_AcceptsHTTP200(t *testing.T) {
	originalGetValidToken := jobsCancelGetValidToken
	originalDoAuthRequest := jobsCancelDoAuthRequest
	defer func() {
		jobsCancelGetValidToken = originalGetValidToken
		jobsCancelDoAuthRequest = originalDoAuthRequest
	}()

	jobsCancelGetValidToken = func() (string, string) {
		return "test-token", "http://api.test"
	}

	jobsCancelDoAuthRequest = func(method, url, token string, body []byte) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString("")),
		}, nil
	}

	cmdJobsCancel([]string{"--id", "job-123"})
}

func TestIsJobsCancelSuccessStatus(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   bool
	}{
		{name: "no content", status: http.StatusNoContent, want: true},
		{name: "ok from proxy", status: http.StatusOK, want: true},
		{name: "bad request", status: http.StatusBadRequest, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isJobsCancelSuccessStatus(tt.status); got != tt.want {
				t.Fatalf("isJobsCancelSuccessStatus(%d) = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}

func catchPanic(f func()) (panicVal interface{}) {
	defer func() {
		panicVal = recover()
	}()
	f()
	return nil
}

func TestUserIDFromToken(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  string
	}{
		{
			name: "valid JWT with sub",
			// header.base64url({"sub":"abc-123"}).sig
			token: "x.eyJzdWIiOiJhYmMtMTIzIn0.x",
			want:  "abc-123",
		},
		{
			name:  "malformed token",
			token: "notajwt",
			want:  "",
		},
		{
			name:  "empty token",
			token: "",
			want:  "",
		},
		{
			name:  "missing sub claim",
			token: "x.eyJmb28iOiJiYXIifQ.x",
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := userIDFromToken(tt.token)
			if got != tt.want {
				t.Errorf("userIDFromToken(%q) = %q, want %q", tt.token, got, tt.want)
			}
		})
	}
}

func TestJobRequestPath_EscapesJobID(t *testing.T) {
	got := jobRequestPath("../jobs/abc 123", "/results")
	want := "/api/v1/jobs/..%2Fjobs%2Fabc%20123/results"
	if got != want {
		t.Fatalf("jobRequestPath() = %q, want %q", got, want)
	}
}

func TestSafeJobResultFilename_SanitizesTraversal(t *testing.T) {
	got := safeJobResultFilename("../windows\\system32:evil")
	want := "windows_system32_evil.json"
	if got != want {
		t.Fatalf("safeJobResultFilename() = %q, want %q", got, want)
	}
}

func TestSafeJobResultFilename_DefaultsOnEmptyInput(t *testing.T) {
	got := safeJobResultFilename("   ")
	want := "job.json"
	if got != want {
		t.Fatalf("safeJobResultFilename() = %q, want %q", got, want)
	}
}
