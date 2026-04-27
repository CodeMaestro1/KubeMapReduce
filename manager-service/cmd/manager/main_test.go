package main

import (
	"context"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/metadata"

	"kubemapreduce/manager-service/internal/config"
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
	if !isAuthorizedInternalCancel(req, "secret", false) {
		t.Fatalf("expected request with matching token to be authorized")
	}
}

func TestIsAuthorizedInternalCancel_WithoutTokenDeniedByDefault(t *testing.T) {
	localReq := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	localReq.RemoteAddr = "127.0.0.1:4000"
	if isAuthorizedInternalCancel(localReq, "", false) {
		t.Fatalf("expected loopback request to be denied when token is unset and insecure fallback is disabled")
	}
}

func TestIsAuthorizedInternalCancel_WithoutTokenAllowsLoopbackWhenExplicitlyEnabled(t *testing.T) {
	localReq := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	localReq.RemoteAddr = "127.0.0.1:4000"
	if !isAuthorizedInternalCancel(localReq, "", true) {
		t.Fatalf("expected loopback request to be authorized when insecure fallback is explicitly enabled")
	}

	remoteReq := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	remoteReq.RemoteAddr = "10.0.0.1:4000"
	if isAuthorizedInternalCancel(remoteReq, "", true) {
		t.Fatalf("expected non-loopback request to be denied even when insecure fallback is enabled")
	}
}

func TestIsAuthorizedWorkerRPC_WithExpectedToken(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-worker-token", "abc123"))
	if !isAuthorizedWorkerRPC(ctx, "abc123") {
		t.Fatalf("expected worker rpc token match to authorize")
	}
}

func TestIsAuthorizedWorkerRPC_MissingOrWrongToken(t *testing.T) {
	noTokenCtx := context.Background()
	if isAuthorizedWorkerRPC(noTokenCtx, "abc123") {
		t.Fatalf("expected missing worker rpc token to be rejected")
	}

	wrongTokenCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-worker-token", "wrong"))
	if isAuthorizedWorkerRPC(wrongTokenCtx, "abc123") {
		t.Fatalf("expected wrong worker rpc token to be rejected")
	}
}

func TestValidateWorkerRPCSecurityConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr bool
	}{
		{
			name: "token only is allowed",
			cfg: &config.Config{
				WorkerRPCToken: "token",
			},
			wantErr: false,
		},
		{
			name: "tls only is allowed when cert and key set",
			cfg: &config.Config{
				GRPCTLSCertFile: "tls.crt",
				GRPCTLSKeyFile:  "tls.key",
			},
			wantErr: false,
		},
		{
			name:    "insecure mode requires explicit opt in",
			cfg:     &config.Config{},
			wantErr: true,
		},
		{
			name: "insecure mode explicit opt in passes",
			cfg: &config.Config{
				AllowInsecureWorkerRPC: true,
			},
			wantErr: false,
		},
		{
			name: "partial tls config fails",
			cfg: &config.Config{
				GRPCTLSCertFile: "tls.crt",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWorkerRPCSecurityConfig(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateWorkerRPCSecurityConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
