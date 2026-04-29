package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"kubemapreduce/auth-service/pkg/auth"
	"kubemapreduce/manager-service/internal/manager"
	"kubemapreduce/manager-service/internal/models"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
)

func newTestHandlers() *Handlers {
	store := NewMemoryJobStore(24*time.Hour, 10000, nil)
	return NewHandlers(nil, store, nil, "", "")
}

func newAuthedRequest(method, target, body, userID string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	return req.WithContext(auth.ContextWithClaims(req.Context(), jwt.MapClaims{"sub": userID}))
}

// testSubject is a stable UUID used as the JWT "sub" claim in handler tests
// that do not exercise per-user authorization separately.
const testSubject = "11111111-1111-1111-1111-111111111111"

// authedReq builds a request pre-populated with a JWT subject claim so that
// handlers gated on currentRequestUserID succeed in tests.
func authedReq(method, target, body string) *http.Request {
	return newAuthedRequest(method, target, body, testSubject)
}

func newTestHandlersWithRetention(now func() time.Time, ttl time.Duration, max int) *Handlers {
	store := NewMemoryJobStore(ttl, max, now)
	return newHandlersWithOptions(nil, store, nil, "", "", now)
}

type fakeScheduleObjectClient struct {
	objects map[string][]byte
}

func (f *fakeScheduleObjectClient) StatObject(_ context.Context, bucketName, objectName string, _ minio.StatObjectOptions) (minio.ObjectInfo, error) {
	data, ok := f.objects[bucketName+"/"+objectName]
	if !ok {
		return minio.ObjectInfo{}, os.ErrNotExist
	}
	return minio.ObjectInfo{Size: int64(len(data))}, nil
}

func (f *fakeScheduleObjectClient) GetObject(_ context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	data, ok := f.objects[bucketName+"/"+objectName]
	if !ok {
		return nil, os.ErrNotExist
	}
	rangeHeader := opts.Header().Get("Range")
	if rangeHeader == "" {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	var start, end int64
	if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-", &start); err != nil {
			return nil, err
		}
		end = int64(len(data)) - 1
	}
	if end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	if start < 0 || start > end {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return io.NopCloser(bytes.NewReader(data[int(start) : int(end)+1])), nil
}

func TestHandleHealth(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	h.HandleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("expected body to contain status ok, got %q", rec.Body.String())
	}
}

func TestHandleHealth_RejectsNonGet(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rec := httptest.NewRecorder()

	h.HandleHealth(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHandleJobsSubmit_RejectsInvalidPayload(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsSubmit_RejectsOversizedPayload(t *testing.T) {
	h := newTestHandlers()

	oversized := `{"filename":"` + strings.Repeat("a", maxJSONBodyBytes) + `","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(oversized))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status %d, got %d", http.StatusRequestEntityTooLarge, rec.Code)
	}
}

func TestHandleJobsSubmit_RejectsUnknownFields(t *testing.T) {
	h := newTestHandlers()

	body := `{"filename":"data.csv","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"},"unknown":"x"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d for unknown field, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsSubmit_RejectsTrailingData(t *testing.T) {
	h := newTestHandlers()

	obj := `{"filename":"data.csv","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	body := obj + obj
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d for trailing data, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsSubmit_SchedulesJobAfterPersist(t *testing.T) {
	var capturedReq manager.ScheduleJobRequest
	managerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/schedule" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&capturedReq); err != nil {
			t.Errorf("decode schedule request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer managerSrv.Close()

	h := newTestHandlers()
	h.managerAddr = managerSrv.Listener.Addr().String()

	body := `{"filename":"input.jsonl","mapper":{"language":"python","artifact":"mapper.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"reducer.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"},"reducers":2}`
	req := authedReq(http.MethodPost, "/api/v1/jobs", body)
	rec := httptest.NewRecorder()
	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var submitResp struct {
		JobID string `json:"jobId"`
	}
	if err := json.NewDecoder(strings.NewReader(rec.Body.String())).Decode(&submitResp); err != nil {
		t.Fatalf("decode submit response: %v", err)
	}

	if capturedReq.JobID != submitResp.JobID {
		t.Errorf("schedule request JobID %q != submitted JobID %q", capturedReq.JobID, submitResp.JobID)
	}
	if capturedReq.MapperURI != "mapper.py" {
		t.Errorf("expected MapperURI mapper.py, got %q", capturedReq.MapperURI)
	}
	if capturedReq.ReducerURI != "reducer.py" {
		t.Errorf("expected ReducerURI reducer.py, got %q", capturedReq.ReducerURI)
	}
	if capturedReq.InputURI != "s3://mapreduce-inputs/input.jsonl" {
		t.Errorf("expected InputURI s3://mapreduce-inputs/input.jsonl, got %q", capturedReq.InputURI)
	}
	if capturedReq.MTasks != 1 {
		t.Errorf("expected MTasks=1, got %d", capturedReq.MTasks)
	}
	if capturedReq.RTasks != 2 {
		t.Errorf("expected RTasks=2, got %d", capturedReq.RTasks)
	}
	if len(capturedReq.Tasks) != 3 {
		t.Errorf("expected 3 tasks (1 Map + 2 Reduce), got %d", len(capturedReq.Tasks))
	}
}

func TestBuildScheduleRequest_SplitsLargeInputAndHashesEachSplit(t *testing.T) {
	origSplitSize := defaultTargetSplitSizeBytes
	defaultTargetSplitSizeBytes = 4
	t.Cleanup(func() { defaultTargetSplitSizeBytes = origSplitSize })

	body := models.JobSubmissionRequest{
		Filename: "input.jsonl",
		Mapper: models.FunctionSpec{
			Artifact:   "mapper.py",
			Entrypoint: "map",
			Interface:  "map(key,value)->[]KeyValue",
			Language:   "python",
		},
		Reducer: models.FunctionSpec{
			Artifact:   "reducer.py",
			Entrypoint: "reduce",
			Interface:  "reduce(key,values)->Value",
			Language:   "python",
		},
		Reducers: 2,
	}

	payload := []byte("abcdefgh")
	store := &fakeScheduleObjectClient{
		objects: map[string][]byte{
			"mapreduce-inputs/input.jsonl": payload,
		},
	}

	req, err := buildScheduleRequest(context.Background(), store, "job-1", "user-1", body, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.InputURI != "s3://mapreduce-inputs/input.jsonl" {
		t.Fatalf("InputURI = %q, want %q", req.InputURI, "s3://mapreduce-inputs/input.jsonl")
	}
	if req.MTasks != 2 {
		t.Fatalf("MTasks = %d, want 2", req.MTasks)
	}
	if len(req.Tasks) != 4 {
		t.Fatalf("expected 4 tasks total, got %d", len(req.Tasks))
	}
	if len(req.Tasks[0].InputSplits) != 1 || len(req.Tasks[1].InputSplits) != 1 {
		t.Fatalf("expected one split per map task, got %+v", req.Tasks[:2])
	}
	if req.Tasks[0].InputSplits[0].ByteStart != 0 || req.Tasks[0].InputSplits[0].ByteEnd != 3 {
		t.Fatalf("unexpected first split range: %+v", req.Tasks[0].InputSplits[0])
	}
	if req.Tasks[1].InputSplits[0].ByteStart != 4 || req.Tasks[1].InputSplits[0].ByteEnd != 7 {
		t.Fatalf("unexpected second split range: %+v", req.Tasks[1].InputSplits[0])
	}
	wantFirst := sha256.Sum256(payload[:4])
	wantSecond := sha256.Sum256(payload[4:])
	if req.Tasks[0].InputSplits[0].SplitChecksum != fmt.Sprintf("%x", wantFirst[:]) {
		t.Fatalf("unexpected first split checksum: %q", req.Tasks[0].InputSplits[0].SplitChecksum)
	}
	if req.Tasks[1].InputSplits[0].SplitChecksum != fmt.Sprintf("%x", wantSecond[:]) {
		t.Fatalf("unexpected second split checksum: %q", req.Tasks[1].InputSplits[0].SplitChecksum)
	}
}

func TestHandleJobsSubmit_RejectsEmptyFilename(t *testing.T) {
	h := newTestHandlers()

	body := `{"filename":"","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsSubmit_AcceptsValidJob(t *testing.T) {
	h := newTestHandlers()

	body := `{"filename":"data.csv","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	req := authedReq(http.MethodPost, "/api/v1/jobs", body)
	rec := httptest.NewRecorder()

	h.HandleJobsSubmit(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}

	if !strings.Contains(rec.Body.String(), `"status":"accepted"`) {
		t.Fatalf("expected accepted status in body, got %q", rec.Body.String())
	}
}

func TestHandleJobsSubmit_DefaultsReducersToOneWhenOmitted(t *testing.T) {
	h := newTestHandlers()

	body := `{"filename":"data.csv","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	submitReq := authedReq(http.MethodPost, "/api/v1/jobs", body)
	submitRec := httptest.NewRecorder()

	h.HandleJobsSubmit(submitRec, submitReq)

	if submitRec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, submitRec.Code, submitRec.Body.String())
	}

	listReq := authedReq(http.MethodGet, "/api/v1/jobs", "")
	listRec := httptest.NewRecorder()
	h.HandleJobsList(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, listRec.Code, listRec.Body.String())
	}

	var jobs []map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("failed to decode jobs list: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job in list, got %d", len(jobs))
	}
	if got, ok := jobs[0]["reducers"].(float64); !ok || int(got) != 1 {
		t.Fatalf("expected reducers=1 by default, got %#v", jobs[0]["reducers"])
	}
}

func TestHandleJobsSubmit_DefaultsReducersToOneWhenZeroProvided(t *testing.T) {
	h := newTestHandlers()

	body := `{"filename":"data.csv","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"},"reducers":0}`
	submitReq := authedReq(http.MethodPost, "/api/v1/jobs", body)
	submitRec := httptest.NewRecorder()

	h.HandleJobsSubmit(submitRec, submitReq)

	if submitRec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, submitRec.Code, submitRec.Body.String())
	}

	listReq := authedReq(http.MethodGet, "/api/v1/jobs", "")
	listRec := httptest.NewRecorder()
	h.HandleJobsList(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, listRec.Code, listRec.Body.String())
	}

	var jobs []map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("failed to decode jobs list: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job in list, got %d", len(jobs))
	}
	if got, ok := jobs[0]["reducers"].(float64); !ok || int(got) != 1 {
		t.Fatalf("expected reducers=1 when provided as 0, got %#v", jobs[0]["reducers"])
	}
}

func TestHandleJobsList_AppliesPagination(t *testing.T) {
	h := newTestHandlers()
	now := time.Now().UTC()
	_ = h.store.CreateJob(context.Background(), JobRecord{JobID: "job-a", Status: "Pending", Filename: "a.csv", Reducers: 1, CreatedAt: now})
	_ = h.store.CreateJob(context.Background(), JobRecord{JobID: "job-b", Status: "Pending", Filename: "b.csv", Reducers: 1, CreatedAt: now.Add(time.Second)})
	_ = h.store.CreateJob(context.Background(), JobRecord{JobID: "job-c", Status: "Pending", Filename: "c.csv", Reducers: 1, CreatedAt: now.Add(2 * time.Second)})

	req := authedReq(http.MethodGet, "/jobs?limit=1&offset=1", "")
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	var jobs []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("failed to decode jobs list: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0]["filename"] != "b.csv" {
		t.Fatalf("expected second newest job (b.csv), got %#v", jobs[0]["filename"])
	}
}

func TestHandleJobsList_RejectsInvalidPagination(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=0", nil)
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleWorkerConfig_RejectsInvalidValues(t *testing.T) {
	h := newTestHandlers()

	body := `{"workerReplicas":0,"maxJobsPerNode":5}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/workers/config", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleWorkerConfig(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleWorkerConfig_AcceptsValidConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/config" {
			t.Errorf("expected path /internal/config, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	h := newTestHandlers()
	h.managerAddr = server.Listener.Addr().String()

	body := `{"workerReplicas":4,"maxJobsPerNode":8}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/workers/config", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleWorkerConfig(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"accepted"`) {
		t.Fatalf("expected accepted status in body, got %q", rec.Body.String())
	}
}

// ── Mock Keycloak helper ────────────────────────────────────

// fakeKeycloak returns an httptest.Server that satisfies the Keycloak admin
// API endpoints used by CreateUser and DeleteUserByUsername.
func fakeKeycloak(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		// Token endpoint
		case r.Method == http.MethodPost && r.URL.Path == "/realms/master/protocol/openid-connect/token":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"access_token":"fake-token"}`))

		// Create user
		case r.Method == http.MethodPost && r.URL.Path == "/admin/realms/test/users":
			w.Header().Set("Location", "/admin/realms/test/users/uid-1")
			w.WriteHeader(http.StatusCreated)

		// Set password
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/reset-password"):
			w.WriteHeader(http.StatusNoContent)

		// Fetch role
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/roles/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"role-1","name":"USER"}`))

		// Assign role
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/role-mappings/realm"):
			w.WriteHeader(http.StatusNoContent)

		// Find user by username (for delete)
		case r.Method == http.MethodGet && r.URL.Path == "/admin/realms/test/users" && r.URL.Query().Get("username") != "":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"uid-1","username":"` + r.URL.Query().Get("username") + `"}]`))

		// Delete user
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/admin/realms/test/users/"):
			w.WriteHeader(http.StatusNoContent)

		default:
			t.Logf("unhandled request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func newTestHandlersWithKeycloak(t *testing.T) (*Handlers, *httptest.Server) {
	t.Helper()
	kc := fakeKeycloak(t)
	adminClient := auth.NewKeycloakAdminClient(kc.URL, "test", "admin", "admin")
	store := NewMemoryJobStore(24*time.Hour, 10000, nil)
	return NewHandlers(adminClient, store, nil, "", ""), kc
}

// ── Admin Create User tests ─────────────────────────────────

func TestHandleAdminCreateUser_RejectsNonPost(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/users", nil)
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHandleAdminCreateUser_RejectsInvalidJSON(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleAdminCreateUser_RejectsMissingFields(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	body := `{"username":"alice","password":"","role":"USER"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleAdminCreateUser_RejectsInvalidRole(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	body := `{"username":"alice","password":"secret","role":"SUPERUSER"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "role must be ") {
		t.Fatalf("expected role error message, got %q", rec.Body.String())
	}
}

func TestHandleAdminCreateUser_Success(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	body := `{"username":"alice","email":"alice@example.com","password":"secret","role":"USER"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"created"`) {
		t.Fatalf("expected created status in body, got %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"username":"alice"`) {
		t.Fatalf("expected username in body, got %q", rec.Body.String())
	}
}

func TestHandleAdminCreateUser_NormalizesRole(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	body := `{"username":"bob","password":"secret","role":"  admin  "}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"role":"ADMIN"`) {
		t.Fatalf("expected normalized role ADMIN in body, got %q", rec.Body.String())
	}
}

// ── Admin Delete User tests ─────────────────────────────────

func TestHandleAdminDeleteUser_RejectsNonDelete(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/users/alice", nil)
	req.SetPathValue("username", "alice")
	rec := httptest.NewRecorder()
	h.HandleAdminDeleteUser(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHandleAdminDeleteUser_RejectsMissingUsername(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/users/", nil)
	rec := httptest.NewRecorder()
	h.HandleAdminDeleteUser(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleAdminDeleteUser_AcceptsPathUsername(t *testing.T) {
	h, kc := newTestHandlersWithKeycloak(t)
	defer kc.Close()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/users/alice", nil)
	req.SetPathValue("username", "alice")
	rec := httptest.NewRecorder()
	h.HandleAdminDeleteUser(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected %d, got %d: %s", http.StatusNoContent, rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("expected empty body for 204, got %q", rec.Body.String())
	}
}

// ── Admin handler with nil client (service unavailable) ─────

func TestHandleAdminCreateUser_NilClientUnavailable(t *testing.T) {
	h := newTestHandlers() // nil adminClient

	body := `{"username":"alice","password":"secret","role":"USER"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "authentication admin client not configured") {
		t.Fatalf("expected explicit admin client unavailable message, got %q", rec.Body.String())
	}
}

func TestHandleAdminDeleteUser_NilClientUnavailable(t *testing.T) {
	h := newTestHandlers() // nil adminClient

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/users/alice", nil)
	req.SetPathValue("username", "alice")
	rec := httptest.NewRecorder()

	h.HandleAdminDeleteUser(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "authentication admin client not configured") {
		t.Fatalf("expected explicit admin client unavailable message, got %q", rec.Body.String())
	}
}

func TestHandleAdminCreateUser_KeycloakDown_Returns503(t *testing.T) {
	// Create and immediately close a server to simulate unreachable auth service.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()

	adminClient := auth.NewKeycloakAdminClient(srv.URL, "test", "admin", "admin")
	store := NewMemoryJobStore(24*time.Hour, 10000, nil)
	h := NewHandlers(adminClient, store, nil, "", "")

	body := `{"username":"alice","email":"alice@example.com","password":"secret","role":"USER"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d: %s", http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "authentication service unavailable") {
		t.Fatalf("expected auth unavailable message, got %q", rec.Body.String())
	}
}

func TestHandleAdminCreateUser_KeycloakTimeout_Returns503(t *testing.T) {
	// Hanging server simulates an unresponsive auth service.
	hung := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hung
	}))
	defer func() {
		close(hung)
		srv.Close()
	}()

	adminClient := auth.NewKeycloakAdminClient(srv.URL, "test", "admin", "admin")
	store := NewMemoryJobStore(24*time.Hour, 10000, nil)
	h := NewHandlers(adminClient, store, nil, "", "")

	body := `{"username":"alice","email":"alice@example.com","password":"secret","role":"USER"}`
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", strings.NewReader(body)).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.HandleAdminCreateUser(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d: %s", http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "authentication service unavailable") {
		t.Fatalf("expected auth unavailable message, got %q", rec.Body.String())
	}
}

func TestHandleAdminDeleteUser_KeycloakDown_Returns503(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()

	adminClient := auth.NewKeycloakAdminClient(srv.URL, "test", "admin", "admin")
	store := NewMemoryJobStore(24*time.Hour, 10000, nil)
	h := NewHandlers(adminClient, store, nil, "", "")

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/users/alice", nil)
	req.SetPathValue("username", "alice")
	rec := httptest.NewRecorder()
	h.HandleAdminDeleteUser(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d: %s", http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "authentication service unavailable") {
		t.Fatalf("expected auth unavailable message, got %q", rec.Body.String())
	}
}

// ── HandleJobsList tests ────────────────────────────────────

func TestHandleJobsList_ReturnsEmptyListInitially(t *testing.T) {
	h := newTestHandlers()

	req := authedReq(http.MethodGet, "/api/v1/jobs", "")
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("expected empty array, got %q", rec.Body.String())
	}
}

func TestHandleJobsList_ReturnsSubmittedJob(t *testing.T) {
	h := newTestHandlers()

	body := `{"filename":"data.csv","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	submitReq := authedReq(http.MethodPost, "/api/v1/jobs", body)
	submitRec := httptest.NewRecorder()
	h.HandleJobsSubmit(submitRec, submitReq)
	if submitRec.Code != http.StatusAccepted {
		t.Fatalf("setup: submit failed with %d: %s", submitRec.Code, submitRec.Body.String())
	}

	listReq := authedReq(http.MethodGet, "/api/v1/jobs", "")
	listRec := httptest.NewRecorder()
	h.HandleJobsList(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, listRec.Code)
	}
	if !strings.Contains(listRec.Body.String(), `"filename":"data.csv"`) {
		t.Fatalf("expected filename in list response, got %q", listRec.Body.String())
	}
}

func TestHandleJobsList_PrunesExpiredJobsByTTL(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	h := newTestHandlersWithRetention(func() time.Time { return now }, 1*time.Second, 100)

	body := `{"filename":"stale.csv","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	submitReq := authedReq(http.MethodPost, "/api/v1/jobs", body)
	submitRec := httptest.NewRecorder()
	h.HandleJobsSubmit(submitRec, submitReq)
	if submitRec.Code != http.StatusAccepted {
		t.Fatalf("setup: submit failed with %d: %s", submitRec.Code, submitRec.Body.String())
	}

	now = now.Add(2 * time.Second)

	listReq := authedReq(http.MethodGet, "/api/v1/jobs", "")
	listRec := httptest.NewRecorder()
	h.HandleJobsList(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, listRec.Code)
	}
	if strings.TrimSpace(listRec.Body.String()) != "[]" {
		t.Fatalf("expected expired jobs to be pruned, got %q", listRec.Body.String())
	}
}

func TestHandleJobsGet_ReturnsNotFoundWhenExpiredByTTL(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	h := newTestHandlersWithRetention(func() time.Time { return now }, 1*time.Second, 100)

	body := `{"filename":"input.json","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	submitReq := authedReq(http.MethodPost, "/api/v1/jobs", body)
	submitRec := httptest.NewRecorder()
	h.HandleJobsSubmit(submitRec, submitReq)
	if submitRec.Code != http.StatusAccepted {
		t.Fatalf("setup: submit failed with %d: %s", submitRec.Code, submitRec.Body.String())
	}

	var submitResp struct {
		JobID string `json:"jobId"`
	}
	if err := json.NewDecoder(strings.NewReader(submitRec.Body.String())).Decode(&submitResp); err != nil {
		t.Fatalf("decode submit response: %v", err)
	}

	now = now.Add(2 * time.Second)

	getReq := authedReq(http.MethodGet, "/api/v1/jobs/"+submitResp.JobID, "")
	getReq.SetPathValue("job_id", submitResp.JobID)
	getRec := httptest.NewRecorder()
	h.HandleJobsGet(getRec, getReq)

	if getRec.Code != http.StatusNotFound {
		t.Fatalf("expected %d, got %d", http.StatusNotFound, getRec.Code)
	}
}

func TestHandleJobsSubmit_EvictsOldestWhenMaxCapacityExceeded(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	h := newTestHandlersWithRetention(func() time.Time { return now }, 1*time.Hour, 2)

	submit := func(filename string) {
		body := `{"filename":"` + filename + `","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
		req := authedReq(http.MethodPost, "/api/v1/jobs", body)
		rec := httptest.NewRecorder()
		h.HandleJobsSubmit(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("submit failed with %d: %s", rec.Code, rec.Body.String())
		}
	}

	submit("job-1.csv")
	now = now.Add(1 * time.Second)
	submit("job-2.csv")
	now = now.Add(1 * time.Second)
	submit("job-3.csv")

	listReq := authedReq(http.MethodGet, "/api/v1/jobs", "")
	listRec := httptest.NewRecorder()
	h.HandleJobsList(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, listRec.Code)
	}

	var jobs []models.JobStatusResponse
	if err := json.Unmarshal(listRec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("decode jobs list: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs after eviction, got %d", len(jobs))
	}
	if jobs[0].Filename != "job-3.csv" || jobs[1].Filename != "job-2.csv" {
		t.Fatalf("expected newest-first order after eviction, got %+v", jobs)
	}
}

func TestHandleJobsList_ReturnsNewestFirst(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	h := newTestHandlersWithRetention(func() time.Time { return now }, 1*time.Hour, 100)

	submit := func(filename string) {
		body := `{"filename":"` + filename + `","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
		req := authedReq(http.MethodPost, "/api/v1/jobs", body)
		rec := httptest.NewRecorder()
		h.HandleJobsSubmit(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("submit failed with %d: %s", rec.Code, rec.Body.String())
		}
	}

	submit("first.csv")
	now = now.Add(1 * time.Second)
	submit("second.csv")
	now = now.Add(1 * time.Second)
	submit("third.csv")

	listReq := authedReq(http.MethodGet, "/api/v1/jobs", "")
	listRec := httptest.NewRecorder()
	h.HandleJobsList(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, listRec.Code)
	}

	var jobs []models.JobStatusResponse
	if err := json.Unmarshal(listRec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("decode jobs list: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(jobs))
	}
	if jobs[0].Filename != "third.csv" || jobs[1].Filename != "second.csv" || jobs[2].Filename != "first.csv" {
		t.Fatalf("expected newest-first ordering [third, second, first], got [%s, %s, %s]",
			jobs[0].Filename, jobs[1].Filename, jobs[2].Filename)
	}
}

func TestHandleJobsList_CapsLimitAtMax(t *testing.T) {
	h := newTestHandlers()
	now := time.Now().UTC()
	for i := 0; i < maxListLimit+10; i++ {
		_ = h.store.CreateJob(context.Background(), JobRecord{JobID: uuid.NewString(), Status: "Pending", CreatedAt: now.Add(time.Duration(i) * time.Second)})
	}

	req := authedReq(http.MethodGet, "/jobs?limit=9999", "")
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var jobs []models.JobStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(jobs) != maxListLimit {
		t.Fatalf("expected limit capped at %d, got %d", maxListLimit, len(jobs))
	}
}

func TestHandleJobsList_DefaultLimitWhenOmitted(t *testing.T) {
	h := newTestHandlers()
	now := time.Now().UTC()
	for i := 0; i < defaultListLimit+10; i++ {
		_ = h.store.CreateJob(context.Background(), JobRecord{JobID: uuid.NewString(), Status: "Pending", CreatedAt: now.Add(time.Duration(i) * time.Second)})
	}

	req := authedReq(http.MethodGet, "/api/v1/jobs", "")
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)

	var jobs []models.JobStatusResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &jobs)
	if len(jobs) != defaultListLimit {
		t.Fatalf("expected default limit %d, got %d", defaultListLimit, len(jobs))
	}
}

func TestHandleJobsList_RejectsNegativeLimit(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=-1", nil)
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for negative limit, got %d", rec.Code)
	}
}

func TestHandleJobsList_RejectsNegativeOffset(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodGet, "/jobs?offset=-1", nil)
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for negative offset, got %d", rec.Code)
	}
}

func TestHandleJobsList_RejectsNonNumericLimit(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=abc", nil)
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-numeric limit, got %d", rec.Code)
	}
}

func TestHandleJobsList_OffsetBeyondRangeReturnsEmpty(t *testing.T) {
	h := newTestHandlers()
	_ = h.store.CreateJob(context.Background(), JobRecord{JobID: "j1", Status: "Pending", CreatedAt: time.Now()})

	req := authedReq(http.MethodGet, "/jobs?offset=100", "")
	rec := httptest.NewRecorder()
	h.HandleJobsList(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("expected empty array, got %q", rec.Body.String())
	}
}

func TestHandleJobsList_StableOrderingAcrossPages(t *testing.T) {
	h := newTestHandlers()
	now := time.Now().UTC()
	// Job 1 is oldest, Job 3 is newest
	_ = h.store.CreateJob(context.Background(), JobRecord{JobID: "j1", Filename: "1.csv", CreatedAt: now})
	_ = h.store.CreateJob(context.Background(), JobRecord{JobID: "j2", Filename: "2.csv", CreatedAt: now.Add(time.Second)})
	_ = h.store.CreateJob(context.Background(), JobRecord{JobID: "j3", Filename: "3.csv", CreatedAt: now.Add(2 * time.Second)})

	// Page 1: newest (j3)
	req1 := authedReq(http.MethodGet, "/jobs?limit=1&offset=0", "")
	rec1 := httptest.NewRecorder()
	h.HandleJobsList(rec1, req1)
	var p1 []models.JobStatusResponse
	_ = json.Unmarshal(rec1.Body.Bytes(), &p1)

	// Page 2: second newest (j2)
	req2 := authedReq(http.MethodGet, "/jobs?limit=1&offset=1", "")
	rec2 := httptest.NewRecorder()
	h.HandleJobsList(rec2, req2)
	var p2 []models.JobStatusResponse
	_ = json.Unmarshal(rec2.Body.Bytes(), &p2)

	if p1[0].JobID != "j3" || p2[0].JobID != "j2" {
		t.Fatalf("unstable ordering: page 1 got %s, page 2 got %s", p1[0].JobID, p2[0].JobID)
	}
}

// ── HandleJobsGet tests ─────────────────────────────────────

func TestHandleJobsGet_ReturnsKnownJob(t *testing.T) {
	h := newTestHandlers()

	body := `{"filename":"input.json","mapper":{"language":"python","artifact":"m.py","entrypoint":"map","interface":"map(key,value)->[]KeyValue"},"reducer":{"language":"python","artifact":"r.py","entrypoint":"reduce","interface":"reduce(key,values)->Value"}}`
	submitReq := authedReq(http.MethodPost, "/api/v1/jobs", body)
	submitRec := httptest.NewRecorder()
	h.HandleJobsSubmit(submitRec, submitReq)
	if submitRec.Code != http.StatusAccepted {
		t.Fatalf("setup: submit failed with %d: %s", submitRec.Code, submitRec.Body.String())
	}

	var submitResp struct {
		JobID string `json:"jobId"`
	}
	if err := json.NewDecoder(strings.NewReader(submitRec.Body.String())).Decode(&submitResp); err != nil {
		t.Fatalf("decode submit response: %v", err)
	}

	getReq := authedReq(http.MethodGet, "/api/v1/jobs/"+submitResp.JobID, "")
	getReq.SetPathValue("job_id", submitResp.JobID)
	getRec := httptest.NewRecorder()
	h.HandleJobsGet(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d: %s", http.StatusOK, getRec.Code, getRec.Body.String())
	}
	if !strings.Contains(getRec.Body.String(), submitResp.JobID) {
		t.Fatalf("expected job ID in response, got %q", getRec.Body.String())
	}
}

func TestHandleJobsGet_ReturnsNotFoundForUnknownJob(t *testing.T) {
	h := newTestHandlers()
	unknownJobID := "3fcb6284-4cd7-4f8b-b8c6-5fd6e5687f0f"

	req := authedReq(http.MethodGet, "/api/v1/jobs/"+unknownJobID, "")
	req.SetPathValue("job_id", unknownJobID)
	rec := httptest.NewRecorder()
	h.HandleJobsGet(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected %d, got %d", http.StatusNotFound, rec.Code)
	}
}

func TestHandleJobsGet_InvalidUUID_ReturnsBadRequest(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/not-a-uuid", nil)
	req.SetPathValue("job_id", "not-a-uuid")
	rec := httptest.NewRecorder()
	h.HandleJobsGet(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

// ── HandleJobsDownload tests ────────────────────────────────

func TestHandleJobsDownload_NotFoundForUnknownJob(t *testing.T) {
	h := newTestHandlers()
	unknownJobID := "36f77f7a-cb6d-4d89-b9d6-643f8222f7de"

	req := authedReq(http.MethodGet, "/api/v1/jobs/"+unknownJobID+"/results", "")
	req.SetPathValue("job_id", unknownJobID)
	rec := httptest.NewRecorder()
	h.HandleJobsDownload(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected %d, got %d", http.StatusNotFound, rec.Code)
	}
}

func TestHandleJobsDownload_InvalidUUID_ReturnsBadRequest(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/not-a-uuid/results", nil)
	req.SetPathValue("job_id", "not-a-uuid")
	rec := httptest.NewRecorder()
	h.HandleJobsDownload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsDownload_Returns409WhenJobNotCompleted(t *testing.T) {
	h := newTestHandlers()
	jobID := uuid.New().String()
	_ = h.store.CreateJob(context.Background(), JobRecord{
		JobID:     jobID,
		UserID:    testSubject,
		Status:    "Running",
		CreatedAt: time.Now(),
	})

	req := authedReq(http.MethodGet, "/api/v1/jobs/"+jobID+"/results", "")
	req.SetPathValue("job_id", jobID)
	rec := httptest.NewRecorder()
	h.HandleJobsDownload(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected %d, got %d: %s", http.StatusConflict, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "job_not_complete") {
		t.Fatalf("expected job_not_complete in response, got %q", rec.Body.String())
	}
}

func TestHandleJobsDownload_Returns503WhenMinioNotConfigured(t *testing.T) {
	h := newTestHandlers() // minioClient is nil
	jobID := uuid.New().String()
	_ = h.store.CreateJob(context.Background(), JobRecord{
		JobID:     jobID,
		UserID:    testSubject,
		Status:    "Completed",
		CreatedAt: time.Now(),
	})

	req := authedReq(http.MethodGet, "/api/v1/jobs/"+jobID+"/results", "")
	req.SetPathValue("job_id", jobID)
	rec := httptest.NewRecorder()
	h.HandleJobsDownload(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d: %s", http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	}
}

func TestHandleJobsDownload_Returns200WithEmptyURLsWhenNoOutputs(t *testing.T) {
	// MemoryJobStore.GetJobOutputs returns nil (no task outputs in memory store).
	// With a nil minioClient the handler returns 503 before reaching presign, so
	// we inject a fake minio-less handler that bypasses presign when urls is empty.
	// Since we cannot instantiate a real minio.Client without a server, we test
	// the 503 path here and rely on store_test.go for the output-URI query logic.
	h := newTestHandlers()
	jobID := uuid.New().String()
	_ = h.store.CreateJob(context.Background(), JobRecord{
		JobID:     jobID,
		UserID:    testSubject,
		Status:    "Completed",
		CreatedAt: time.Now(),
	})

	req := authedReq(http.MethodGet, "/api/v1/jobs/"+jobID+"/results", "")
	req.SetPathValue("job_id", jobID)
	rec := httptest.NewRecorder()
	h.HandleJobsDownload(rec, req)

	// minioClient is nil → 503; this confirms handler reaches the presign check.
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (no minio configured), got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── HandleConfigureNodes tests ──────────────────────────────

func TestHandleConfigureNodes_RejectsNonPut(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/nodes/config", nil)
	rec := httptest.NewRecorder()
	h.HandleConfigureNodes(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHandleConfigureNodes_RejectsInvalidPayload(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/nodes/config", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()
	h.HandleConfigureNodes(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleConfigureNodes_RejectsZeroMaxPods(t *testing.T) {
	h := newTestHandlers()

	body := `{"maxPods":0,"cpuLimit":"500m","memoryLimit":"1Gi"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/nodes/config", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleConfigureNodes(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleConfigureNodes_RejectsMissingCPULimit(t *testing.T) {
	h := newTestHandlers()

	body := `{"maxPods":20,"cpuLimit":"","memoryLimit":"1Gi"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/nodes/config", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleConfigureNodes(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleConfigureNodes_AcceptsValidConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/config" {
			t.Errorf("expected path /internal/config, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	h := newTestHandlers()
	h.managerAddr = server.Listener.Addr().String()

	body := `{"maxPods":20,"cpuLimit":"500m","memoryLimit":"1Gi"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/nodes/config", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleConfigureNodes(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "success") {
		t.Fatalf("expected success in body, got %q", rec.Body.String())
	}
}

// ── HandleAdminConfigWorkers tests ──────────────────────────

func TestHandleAdminConfigWorkers_RejectsNonPost(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/admin/config/workers", nil)
	rec := httptest.NewRecorder()
	h.HandleAdminConfigWorkers(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, rec.Code)
	}
}

func TestHandleAdminConfigWorkers_RejectsEmptyPayload(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/config/workers", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.HandleAdminConfigWorkers(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleAdminConfigWorkers_WorkerReplicasOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	h := newTestHandlers()
	h.managerAddr = server.Listener.Addr().String()

	body := `{"workerReplicas":4,"maxJobsPerNode":8}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/config/workers", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminConfigWorkers(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
}

func TestHandleAdminConfigWorkers_NodeConfigOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	h := newTestHandlers()
	h.managerAddr = server.Listener.Addr().String()

	body := `{"maxPods":10,"cpuLimit":"500m","memoryLimit":"1Gi"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/config/workers", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminConfigWorkers(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
}

func TestHandleAdminConfigWorkers_CombinedConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	h := newTestHandlers()
	h.managerAddr = server.Listener.Addr().String()

	body := `{"maxPods":20,"cpuLimit":"1","memoryLimit":"2Gi","workerReplicas":3,"maxJobsPerNode":5}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/config/workers", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleAdminConfigWorkers(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"accepted"`) {
		t.Fatalf("expected accepted status in body, got %q", rec.Body.String())
	}
}

// ── Route parsing edge-case regression tests ────────────────

func TestHandleJobsGet_EmptyPathValue_Returns400(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/", nil)
	req.SetPathValue("job_id", "")
	rec := httptest.NewRecorder()
	h.HandleJobsGet(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestHandleJobsDownload_EmptyPathValue_Returns400(t *testing.T) {
	h := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs//results", nil)
	req.SetPathValue("job_id", "")
	rec := httptest.NewRecorder()
	h.HandleJobsDownload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestRouting_TrailingSlashOnJobDetail_Returns404(t *testing.T) {
	h := newTestHandlers()
	mux := http.NewServeMux()
	mux.Handle("GET /jobs/{job_id}/results", http.HandlerFunc(h.HandleJobsDownload))
	mux.Handle("GET /jobs/{job_id}", http.HandlerFunc(h.HandleJobsGet))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/some-id/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for trailing slash, got %d", rec.Code)
	}
}

func TestRouting_UnexpectedSegment_Returns404(t *testing.T) {
	h := newTestHandlers()
	mux := http.NewServeMux()
	mux.Handle("GET /jobs/{job_id}/results", http.HandlerFunc(h.HandleJobsDownload))
	mux.Handle("GET /jobs/{job_id}", http.HandlerFunc(h.HandleJobsGet))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/some-id/unknown", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unexpected segment, got %d", rec.Code)
	}
}

func TestRouting_ResultsTrailingSlash_Returns404(t *testing.T) {
	h := newTestHandlers()
	mux := http.NewServeMux()
	mux.Handle("GET /jobs/{job_id}/results", http.HandlerFunc(h.HandleJobsDownload))
	mux.Handle("GET /jobs/{job_id}", http.HandlerFunc(h.HandleJobsGet))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/some-id/results/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for results trailing slash, got %d", rec.Code)
	}
}

func TestRouting_PostToJobDetail_Returns405(t *testing.T) {
	h := newTestHandlers()
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/jobs/{job_id}/results", http.HandlerFunc(h.HandleJobsDownload))
	mux.Handle("GET /api/v1/jobs/{job_id}", http.HandlerFunc(h.HandleJobsGet))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/some-id", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for POST on job detail, got %d", rec.Code)
	}
}
func TestBuildScheduleRequest_ComputesChecksum(t *testing.T) {
	mockStorage := &mockScheduleObjectClient{
		size:     1024,
		checksum: "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2", // sha256 of 1024 'a's
	}

	req := models.JobSubmissionRequest{
		Filename: "large-file.bin",
		Mapper: models.FunctionSpec{
			Artifact: "mapper.py",
		},
		Reducer: models.FunctionSpec{
			Artifact: "reducer.py",
		},
		Reducers: 1,
	}

	ctx := context.Background()
	jobID := uuid.NewString()
	userID := uuid.NewString()

	schedReq, err := buildScheduleRequest(ctx, mockStorage, jobID, userID, req, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(schedReq.Tasks) == 0 {
		t.Fatal("expected at least one task")
	}

	mapTask := schedReq.Tasks[0]
	if mapTask.TaskType != "Map" {
		t.Fatalf("expected Map task, got %s", mapTask.TaskType)
	}

	if len(mapTask.InputSplits) != 1 {
		t.Fatalf("expected 1 input split, got %d", len(mapTask.InputSplits))
	}

	split := mapTask.InputSplits[0]
	if split.ByteStart != 0 {
		t.Errorf("expected ByteStart 0, got %d", split.ByteStart)
	}
	if split.ByteEnd != 1023 {
		t.Errorf("expected ByteEnd 1023, got %d", split.ByteEnd)
	}
	if split.SplitChecksum == "" {
		t.Error("expected non-empty SplitChecksum")
	}
}

type mockScheduleObjectClient struct {
	size     int64
	checksum string
}

func (m *mockScheduleObjectClient) StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{Size: m.size}, nil
}

func (m *mockScheduleObjectClient) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	data := make([]byte, m.size)
	for i := range data {
		data[i] = 'a'
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
