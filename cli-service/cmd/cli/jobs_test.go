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
	runJobsSubmitUploadScenario(t, false)
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
