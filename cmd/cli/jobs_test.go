package main

import "testing"

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
