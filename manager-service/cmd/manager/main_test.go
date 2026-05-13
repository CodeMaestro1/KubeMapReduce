package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"kubemapreduce/manager-service/internal/config"
	"kubemapreduce/manager-service/internal/manager"
)

func TestParseReplicaIndexFromHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		total    int
		want     int
	}{
		{name: "single digit ordinal", hostname: "manager-2", total: 3, want: 2},
		{name: "multi digit ordinal", hostname: "manager-10", total: 11, want: 10},
		{name: "out of range ordinal", hostname: "manager-10", total: 3, want: 0},
		{name: "missing suffix", hostname: "manager", total: 3, want: 0},
		{name: "invalid suffix", hostname: "manager-a", total: 3, want: 0},
		{name: "empty hostname", hostname: "", total: 3, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseReplicaIndexFromHostname(tc.hostname, tc.total); got != tc.want {
				t.Fatalf("parseReplicaIndexFromHostname(%q) = %d, want %d", tc.hostname, got, tc.want)
			}
		})
	}
}

func TestResolveReplicaIndexUsesStatefulsetOrdinalWhenValid(t *testing.T) {
	t.Setenv("STATEFULSET_ORDINAL", "7")
	if got := resolveReplicaIndex("manager-2", 8); got != 7 {
		t.Fatalf("resolveReplicaIndex should use STATEFULSET_ORDINAL, got %d", got)
	}
}

func TestResolveReplicaIndexFallsBackToHostname(t *testing.T) {
	t.Setenv("STATEFULSET_ORDINAL", "invalid")
	if got := resolveReplicaIndex("manager-12", 13); got != 12 {
		t.Fatalf("resolveReplicaIndex should fall back to hostname, got %d", got)
	}
}

func TestResolveReplicaIndexFallsBackWhenOrdinalOutOfRange(t *testing.T) {
	t.Setenv("STATEFULSET_ORDINAL", "12")
	if got := resolveReplicaIndex("manager-1", 3); got != 1 {
		t.Fatalf("resolveReplicaIndex should fall back to hostname for out-of-range ordinal, got %d", got)
	}
}

func TestDiscoverStatefulSetReplicas(t *testing.T) {
	replicas := int32(5)
	client := fake.NewSimpleClientset(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "manager", Namespace: "mapreduce"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	})

	got, err := discoverStatefulSetReplicas(context.Background(), client, "mapreduce", "manager")
	if err != nil {
		t.Fatalf("discoverStatefulSetReplicas() error = %v", err)
	}
	if got != 5 {
		t.Fatalf("discoverStatefulSetReplicas() = %d, want 5", got)
	}
}

type fakeReaperScheduler struct {
	mu    sync.Mutex
	calls int
	hits  chan struct{}
}

func (f *fakeReaperScheduler) FailStaleTasks(context.Context) (int, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	select {
	case f.hits <- struct{}{}:
	default:
	}
	return 0, nil
}

func (f *fakeReaperScheduler) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestDefaultReaperInterval_UsesHalfLeaseTTL(t *testing.T) {
	if got := defaultReaperInterval(30); got != 15*time.Second {
		t.Fatalf("defaultReaperInterval(30) = %s, want %s", got, 15*time.Second)
	}
}

func TestStartReaper_InvokesFailStaleTasksPeriodically(t *testing.T) {
	fake := &fakeReaperScheduler{hits: make(chan struct{}, 8)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startReaper(ctx, fake, 15*time.Millisecond)

	deadline := time.After(400 * time.Millisecond)
	seen := 0
	for seen < 2 {
		select {
		case <-fake.hits:
			seen++
		case <-deadline:
			t.Fatalf("expected at least 2 reaper invocations, got %d", fake.callCount())
		}
	}
}

func TestIsAuthorizedInternalRequest_WithToken(t *testing.T) {
	req := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	req.Header.Set("X-Internal-Token", "secret")
	req.RemoteAddr = "10.10.10.10:4567"
	if !isAuthorizedInternalRequest(req, "secret", false) {
		t.Fatalf("expected request with matching token to be authorized")
	}
}

func TestIsAuthorizedInternalRequest_WithoutTokenDeniedByDefault(t *testing.T) {
	localReq := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	localReq.RemoteAddr = "127.0.0.1:4000"
	if isAuthorizedInternalRequest(localReq, "", false) {
		t.Fatalf("expected loopback request to be denied when token is unset and insecure fallback is disabled")
	}
}

func TestIsAuthorizedInternalRequest_WithoutTokenAllowsLoopbackWhenExplicitlyEnabled(t *testing.T) {
	localReq := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	localReq.RemoteAddr = "127.0.0.1:4000"
	if !isAuthorizedInternalRequest(localReq, "", true) {
		t.Fatalf("expected loopback request to be authorized when insecure fallback is explicitly enabled")
	}

	remoteReq := httptest.NewRequest("DELETE", "/internal/jobs/job-1", nil)
	remoteReq.RemoteAddr = "10.0.0.1:4000"
	if isAuthorizedInternalRequest(remoteReq, "", true) {
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

type mockJobScheduler struct {
	scheduleFunc func(ctx context.Context, req manager.ScheduleJobRequest) error
}

func (m *mockJobScheduler) CancelJob(ctx context.Context, jobID string) error {
	return nil
}

func (m *mockJobScheduler) ScheduleJob(ctx context.Context, req manager.ScheduleJobRequest) error {
	if m.scheduleFunc != nil {
		return m.scheduleFunc(ctx, req)
	}
	return nil
}

func (m *mockJobScheduler) UpsertSystemConfig(ctx context.Context, update manager.SystemConfigUpdate) error {
	return nil
}

func (m *mockJobScheduler) GetSystemConfig(ctx context.Context) (manager.SystemConfigUpdate, error) {
	return manager.SystemConfigUpdate{
		MaxConcurrentPods: 10,
		CPULimit:          "500m",
		MemoryLimit:       "1Gi",
		WorkerReplicas:    3,
		MaxJobsPerNode:    5,
	}, nil
}

type mockPingable struct {
	pingFunc func(ctx context.Context) error
}

func (m *mockPingable) PingContext(ctx context.Context) error {
	if m.pingFunc != nil {
		return m.pingFunc(ctx)
	}
	return nil
}

func TestSetupInternalMux_Readyz(t *testing.T) {
	cfg := &config.Config{}
	sched := &mockJobScheduler{}

	t.Run("db ping success", func(t *testing.T) {
		db := &mockPingable{pingFunc: func(ctx context.Context) error { return nil }}
		mux := setupInternalMux(sched, db, cfg)
		req := httptest.NewRequest("GET", "/readyz", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected 200 OK, got %d", rec.Code)
		}
	})

	t.Run("db ping failure", func(t *testing.T) {
		db := &mockPingable{pingFunc: func(ctx context.Context) error { return errors.New("db down") }}
		mux := setupInternalMux(sched, db, cfg)
		req := httptest.NewRequest("GET", "/readyz", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("expected 503 Service Unavailable, got %d", rec.Code)
		}
	})

	t.Run("healthz liveness", func(t *testing.T) {
		mux := setupInternalMux(sched, nil, cfg)
		req := httptest.NewRequest("GET", "/healthz", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("/healthz: expected 200 OK, got %d", rec.Code)
		}
	})
}

func TestSetupInternalMux_Schedule(t *testing.T) {
	cfg := &config.Config{
		InternalAPIKey: "secret-token",
	}

	t.Run("unauthorized", func(t *testing.T) {
		sched := &mockJobScheduler{}
		mux := setupInternalMux(sched, nil, cfg)
		req := httptest.NewRequest("POST", "/internal/schedule", bytes.NewReader([]byte("{}")))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized, got %d", rec.Code)
		}
	})

	t.Run("invalid payload", func(t *testing.T) {
		sched := &mockJobScheduler{}
		mux := setupInternalMux(sched, nil, cfg)
		req := httptest.NewRequest("POST", "/internal/schedule", bytes.NewReader([]byte("{invalid-json}")))
		req.Header.Set("X-Internal-Token", "secret-token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request, got %d", rec.Code)
		}
	})

	t.Run("scheduler failure", func(t *testing.T) {
		sched := &mockJobScheduler{
			scheduleFunc: func(ctx context.Context, req manager.ScheduleJobRequest) error {
				return errors.New("scheduler failed")
			},
		}
		mux := setupInternalMux(sched, nil, cfg)
		payload, err := json.Marshal(manager.ScheduleJobRequest{JobID: "00000000-0000-0000-0000-000000000001"})
		if err != nil {
			t.Fatalf("failed to marshal request payload: %v", err)
		}
		req := httptest.NewRequest("POST", "/internal/schedule", bytes.NewReader(payload))
		req.Header.Set("X-Internal-Token", "secret-token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 Internal Server Error, got %d", rec.Code)
		}
	})

	t.Run("success", func(t *testing.T) {
		const testJobID = "00000000-0000-0000-0000-000000000002"
		called := false
		sched := &mockJobScheduler{
			scheduleFunc: func(ctx context.Context, req manager.ScheduleJobRequest) error {
				called = true
				if req.JobID != testJobID {
					return errors.New("wrong job id")
				}
				return nil
			},
		}
		mux := setupInternalMux(sched, nil, cfg)
		payload, err := json.Marshal(manager.ScheduleJobRequest{JobID: testJobID})
		if err != nil {
			t.Fatalf("failed to marshal request payload: %v", err)
		}
		req := httptest.NewRequest("POST", "/internal/schedule", bytes.NewReader(payload))
		req.Header.Set("X-Internal-Token", "secret-token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Errorf("expected 202 Accepted, got %d", rec.Code)
		}
		if !called {
			t.Errorf("expected scheduleFunc to be called")
		}
	})
}
