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

	ipv6Req := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	ipv6Req.RemoteAddr = "[::1]:4000"
	if !isAuthorizedInternalCancel(ipv6Req, "") {
		t.Fatalf("expected IPv6 loopback request to be authorized when token is unset")
	}
}

func TestIsAuthorizedInternalCancel_WrongToken(t *testing.T) {
	req := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	req.Header.Set("X-Internal-Token", "wrong")
	req.RemoteAddr = "127.0.0.1:4000"
	if isAuthorizedInternalCancel(req, "secret") {
		t.Fatalf("expected request with wrong token to be denied even if on loopback")
	}
}

func TestIsAuthorizedInternalCancel_EmptyTokenHeader(t *testing.T) {
	req := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	req.Header.Set("X-Internal-Token", "")
	req.RemoteAddr = "127.0.0.1:4000"
	if isAuthorizedInternalCancel(req, "secret") {
		t.Fatalf("expected request with empty token to be denied when token is configured")
	}
}

func TestIsAuthorizedInternalCancel_TokenTakesPrecedence(t *testing.T) {
	req := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	req.Header.Set("X-Internal-Token", "secret")
	req.RemoteAddr = "10.0.0.1:4000"
	if !isAuthorizedInternalCancel(req, "secret") {
		t.Fatalf("expected valid token to authorize from non-loopback addr")
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

func TestValidateInternalCancelSecurityConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr bool
	}{
		{
			name: "token set is allowed",
			cfg: &config.Config{
				InternalAPIKey: "secret",
			},
			wantErr: false,
		},
		{
			name: "token set and insecure opt-in is allowed",
			cfg: &config.Config{
				InternalAPIKey:           "secret",
				AllowInsecureInternalAPI: true,
			},
			wantErr: false,
		},
		{
			name: "no token and insecure opt-in is allowed",
			cfg: &config.Config{
				AllowInsecureInternalAPI: true,
			},
			wantErr: false,
		},
		{
			name:    "no token and no opt-in is rejected",
			cfg:     &config.Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInternalCancelSecurityConfig(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateInternalCancelSecurityConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
