package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseReplicaIndexFromHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		want     int
	}{
		{name: "single digit ordinal", hostname: "manager-2", want: 2},
		{name: "multi digit ordinal", hostname: "manager-10", want: 10},
		{name: "missing suffix", hostname: "manager", want: 0},
		{name: "invalid suffix", hostname: "manager-a", want: 0},
		{name: "empty hostname", hostname: "", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseReplicaIndexFromHostname(tc.hostname); got != tc.want {
				t.Fatalf("parseReplicaIndexFromHostname(%q) = %d, want %d", tc.hostname, got, tc.want)
			}
		})
	}
}

func TestResolveReplicaIndexUsesStatefulsetOrdinalWhenValid(t *testing.T) {
	t.Setenv("STATEFULSET_ORDINAL", "7")
	if got := resolveReplicaIndex("manager-2"); got != 7 {
		t.Fatalf("resolveReplicaIndex should use STATEFULSET_ORDINAL, got %d", got)
	}
}

func TestResolveReplicaIndexFallsBackToHostname(t *testing.T) {
	t.Setenv("STATEFULSET_ORDINAL", "invalid")
	if got := resolveReplicaIndex("manager-12"); got != 12 {
		t.Fatalf("resolveReplicaIndex should fall back to hostname, got %d", got)
	}
}

func TestIsAuthorizedInternalCancel_WithToken(t *testing.T) {
	req := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	req.Header.Set("X-Internal-Token", "secret")
	req.RemoteAddr = "10.10.10.10:4567"
	if !isAuthorizedInternalCancel(req, "secret") {
		t.Fatalf("expected request with matching token to be authorized")
	}
}

func TestIsAuthorizedInternalCancel_WithoutTokenRequiresLoopback(t *testing.T) {
	localReq := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	localReq.RemoteAddr = "127.0.0.1:4000"
	if !isAuthorizedInternalCancel(localReq, "") {
		t.Fatalf("expected loopback request to be authorized when token is unset")
	}

	remoteReq := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	remoteReq.RemoteAddr = "10.0.0.1:4000"
	if isAuthorizedInternalCancel(remoteReq, "") {
		t.Fatalf("expected non-loopback request to be denied when token is unset")
	}
}
