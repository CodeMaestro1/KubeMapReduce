package main

import (
	"bytes"
	"io"
	"net/http"
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
	defer func() {
		jobsSubmitGetValidToken = originalGetValidToken
		jobsSubmitDoAuthRequestExpect = originalDoAuthRequestExpect
		jobsSubmitExit = originalExit
	}()

	getTokenCalled := false
	doRequestCalled := false

	jobsSubmitGetValidToken = func() (string, string) {
		getTokenCalled = true
		return "", ""
	}
	jobsSubmitDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		doRequestCalled = true
		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(bytes.NewBufferString(`{"ok":true}`))}
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
}

func TestCmdJobsSubmit_ValidReducersCallsSubmit(t *testing.T) {
	originalGetValidToken := jobsSubmitGetValidToken
	originalDoAuthRequestExpect := jobsSubmitDoAuthRequestExpect
	defer func() {
		jobsSubmitGetValidToken = originalGetValidToken
		jobsSubmitDoAuthRequestExpect = originalDoAuthRequestExpect
	}()

	getTokenCalled := false
	doRequestCalled := false

	jobsSubmitGetValidToken = func() (string, string) {
		getTokenCalled = true
		return "test-token", "http://example.test"
	}

	jobsSubmitDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		doRequestCalled = true
		if method != http.MethodPost {
			t.Fatalf("expected POST, got %s", method)
		}
		if reqURL != "http://example.test/jobs" {
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

		return &http.Response{StatusCode: http.StatusAccepted, Body: io.NopCloser(bytes.NewBufferString(`{"jobId":"abc"}`))}
	}

	cmdJobsSubmit([]string{"--mapper", "mapper.py", "--reducer", "reducer.py", "--input", "input.jsonl", "--reducers", "2"})

	if !getTokenCalled {
		t.Fatal("expected auth/token retrieval for valid reducers")
	}
	if !doRequestCalled {
		t.Fatal("expected HTTP request for valid reducers")
	}
}

func TestJobRequestPath_EscapesJobID(t *testing.T) {
	got := jobRequestPath("../jobs/abc 123", "/results")
	want := "/jobs/..%2Fjobs%2Fabc%20123/results"
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

// ── cmdJobsList tests ──────────────────────────────────────

// saveAndRestoreJobsList saves the current jobsList function vars and restores
// them when the returned function is called.
func saveAndRestoreJobsList(t *testing.T) {
	t.Helper()
	origGetToken := jobsListGetValidToken
	origDoReq := jobsListDoAuthRequestExpect
	origExit := jobsListExit
	t.Cleanup(func() {
		jobsListGetValidToken = origGetToken
		jobsListDoAuthRequestExpect = origDoReq
		jobsListExit = origExit
	})
}

func TestCmdJobsList_UnexpectedSchema_ExitsNonZero(t *testing.T) {
	saveAndRestoreJobsList(t)

	jobsListGetValidToken = func() (string, string) {
		return "tok", "http://test"
	}
	jobsListDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		// Return a valid HTTP 200 but with a non-array JSON body.
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`{"unexpected":"object"}`)),
		}
	}
	jobsListExit = func(code int) {
		panic(testExit{code: code})
	}

	// Capture stderr for diagnostic message.
	origStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w
	defer func() { os.Stderr = origStderr }()

	func() {
		defer func() {
			rec := recover()
			if rec == nil {
				t.Fatal("expected non-zero exit on unexpected schema")
			}
			exitErr, ok := rec.(testExit)
			if !ok {
				t.Fatalf("expected testExit panic, got %T: %v", rec, rec)
			}
			if exitErr.code != 1 {
				t.Fatalf("expected exit code 1, got %d", exitErr.code)
			}
		}()
		cmdJobsList()
	}()

	_ = w.Close()
	stderrBytes, _ := io.ReadAll(r)
	stderr := string(stderrBytes)
	if !strings.Contains(stderr, "unexpected response schema") {
		t.Fatalf("expected diagnostic message in stderr, got %q", stderr)
	}
	if !strings.Contains(stderr, "raw response") {
		t.Fatalf("expected raw response in stderr, got %q", stderr)
	}
}

func TestCmdJobsList_InvalidJSON_ExitsNonZero(t *testing.T) {
	saveAndRestoreJobsList(t)

	jobsListGetValidToken = func() (string, string) {
		return "tok", "http://test"
	}
	jobsListDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`not json at all`)),
		}
	}
	jobsListExit = func(code int) {
		panic(testExit{code: code})
	}

	func() {
		defer func() {
			rec := recover()
			if rec == nil {
				t.Fatal("expected non-zero exit on invalid JSON")
			}
			exitErr, ok := rec.(testExit)
			if !ok {
				t.Fatalf("expected testExit panic, got %T: %v", rec, rec)
			}
			if exitErr.code != 1 {
				t.Fatalf("expected exit code 1, got %d", exitErr.code)
			}
		}()
		cmdJobsList()
	}()
}

func TestCmdJobsList_ValidResponse_ExitsZero(t *testing.T) {
	saveAndRestoreJobsList(t)

	jobsListGetValidToken = func() (string, string) {
		return "tok", "http://test"
	}
	jobsListDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`[{"jobId":"j1","status":"Running","filename":"data.jsonl","createdAt":"2026-04-17T10:00:00Z"}]`)),
		}
	}

	exitCalled := false
	jobsListExit = func(code int) {
		exitCalled = true
	}

	cmdJobsList()

	if exitCalled {
		t.Fatal("expected no exit call for valid response")
	}
}

func TestCmdJobsList_EmptyArray_PrintsNoJobs(t *testing.T) {
	saveAndRestoreJobsList(t)

	jobsListGetValidToken = func() (string, string) {
		return "tok", "http://test"
	}
	jobsListDoAuthRequestExpect = func(method, reqURL, token string, body []byte, expectedStatus int, failPrefix string) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`[]`)),
		}
	}

	exitCalled := false
	jobsListExit = func(code int) {
		exitCalled = true
	}

	cmdJobsList()

	if exitCalled {
		t.Fatal("expected no exit call for empty jobs array")
	}
}
