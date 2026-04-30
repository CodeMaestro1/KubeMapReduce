package api

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// ── PostgresJobStore persistence tests ──────────────────────

func TestPostgresJobStore_CreateJob_PersistsToDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)
	jobID := uuid.New().String()
	ownerUUID := uuid.New()
	now := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO JOBS").
		WithArgs(sqlmock.AnyArg(), ownerUUID, now, now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO JOB_CONFIGS").
		WithArgs(sqlmock.AnyArg(), "data.csv", "", "", "", 0, 4, "").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	rec := JobRecord{
		JobID:     jobID,
		UserID:    ownerUUID.String(),
		Status:    "Pending",
		Filename:  "data.csv",
		Reducers:  4,
		CreatedAt: now,
	}
	if err := store.CreateJob(context.Background(), rec); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresJobStore_GetJob_ReturnsPersistedJob(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)
	jobUUID := uuid.New()
	ownerUUID := uuid.New()
	created := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)

	rows := sqlmock.NewRows([]string{"job_id", "status", "input_uri", "r_tasks", "created_at"}).
		AddRow(jobUUID, "Running", "input.csv", 2, created)
	mock.ExpectQuery("SELECT j.job_id").
		WithArgs(jobUUID, ownerUUID).
		WillReturnRows(rows)

	rec, err := store.GetJob(context.Background(), ownerUUID.String(), jobUUID.String())
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if rec == nil {
		t.Fatal("expected job record, got nil")
	}
	if rec.JobID != jobUUID.String() {
		t.Fatalf("expected jobID %s, got %s", jobUUID.String(), rec.JobID)
	}
	if rec.Status != "Running" {
		t.Fatalf("expected status Running, got %s", rec.Status)
	}
	if rec.Filename != "input.csv" {
		t.Fatalf("expected filename input.csv, got %s", rec.Filename)
	}
	if rec.Reducers != 2 {
		t.Fatalf("expected 2 reducers, got %d", rec.Reducers)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresJobStore_GetJob_ReturnsNilForMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)
	jobUUID := uuid.New()
	ownerUUID := uuid.New()

	rows := sqlmock.NewRows([]string{"job_id", "status", "input_uri", "r_tasks", "created_at"})
	mock.ExpectQuery("SELECT j.job_id").
		WithArgs(jobUUID, ownerUUID).
		WillReturnRows(rows)

	rec, err := store.GetJob(context.Background(), ownerUUID.String(), jobUUID.String())
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil for missing job, got %+v", rec)
	}
}

func TestPostgresJobStore_GetJob_ReturnsErrorForInvalidUUID(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)

	rec, err := store.GetJob(context.Background(), uuid.New().String(), "not-a-uuid")
	if !errors.Is(err, ErrInvalidJobID) {
		t.Fatalf("expected ErrInvalidJobID, got %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil for invalid UUID, got %+v", rec)
	}
}

func TestPostgresJobStore_ListJobs_ReturnsAllJobs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)
	ownerUUID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()
	t1 := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 4, 18, 13, 0, 0, 0, time.UTC)

	rows := sqlmock.NewRows([]string{"job_id", "status", "input_uri", "r_tasks", "created_at"}).
		AddRow(id2, "Running", "b.csv", 2, t2).
		AddRow(id1, "Pending", "a.csv", 1, t1)
	mock.ExpectQuery("SELECT j.job_id").WithArgs(ownerUUID, 100, 0).WillReturnRows(rows)

	jobs, err := store.ListJobs(context.Background(), ownerUUID.String(), 100, 0)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	if jobs[0].Filename != "b.csv" || jobs[1].Filename != "a.csv" {
		t.Fatalf("expected [b.csv, a.csv], got [%s, %s]", jobs[0].Filename, jobs[1].Filename)
	}
}

func TestPostgresJobStore_ListJobs_ReturnsEmptySlice(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)
	ownerUUID := uuid.New()
	rows := sqlmock.NewRows([]string{"job_id", "status", "input_uri", "r_tasks", "created_at"})
	mock.ExpectQuery("SELECT j.job_id").WithArgs(ownerUUID, 100, 0).WillReturnRows(rows)

	jobs, err := store.ListJobs(context.Background(), ownerUUID.String(), 100, 0)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected nil or empty, got %d jobs", len(jobs))
	}
}

// ── Replica-safety: same store returns consistent state ─────

func TestPostgresJobStore_CreateThenGet_Consistent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)
	jobUUID := uuid.New()
	ownerUUID := uuid.New()
	now := time.Date(2026, 4, 18, 14, 0, 0, 0, time.UTC)

	// Create
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO JOBS").
		WithArgs(sqlmock.AnyArg(), ownerUUID, now, now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO JOB_CONFIGS").
		WithArgs(sqlmock.AnyArg(), "input.jsonl", "", "", "", 0, 3, "").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	rec := JobRecord{
		JobID:     jobUUID.String(),
		UserID:    ownerUUID.String(),
		Status:    "Pending",
		Filename:  "input.jsonl",
		Reducers:  3,
		CreatedAt: now,
	}
	if err := store.CreateJob(context.Background(), rec); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// Get (simulates another replica reading from same DB)
	rows := sqlmock.NewRows([]string{"job_id", "status", "input_uri", "r_tasks", "created_at"}).
		AddRow(jobUUID, "Pending", "input.jsonl", 3, now)
	mock.ExpectQuery("SELECT j.job_id").
		WithArgs(jobUUID, ownerUUID).
		WillReturnRows(rows)

	got, err := store.GetJob(context.Background(), ownerUUID.String(), jobUUID.String())
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected job, got nil")
	}
	if got.Status != "Pending" {
		t.Fatalf("expected Pending, got %s", got.Status)
	}
	if got.Filename != "input.jsonl" {
		t.Fatalf("expected input.jsonl, got %s", got.Filename)
	}
	if got.Reducers != 3 {
		t.Fatalf("expected 3 reducers, got %d", got.Reducers)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// ── GetJobOutputs tests ─────────────────────────────────────

func TestPostgresJobStore_GetJobOutputs_ReturnsReduceURIs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)
	jobID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobOutputURIs)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"output_uri"}).
			AddRow("s3://results/output-0.jsonl").
			AddRow("s3://results/output-1.jsonl"))

	uris, err := store.GetJobOutputs(context.Background(), jobID.String())
	if err != nil {
		t.Fatalf("GetJobOutputs failed: %v", err)
	}
	if len(uris) != 2 {
		t.Fatalf("expected 2 URIs, got %d", len(uris))
	}
	if uris[0] != "s3://results/output-0.jsonl" || uris[1] != "s3://results/output-1.jsonl" {
		t.Fatalf("unexpected URIs: %v", uris)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresJobStore_GetJobOutputs_EmptyForNoOutputs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)
	jobID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta(QueryGetJobOutputURIs)).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"output_uri"}))

	uris, err := store.GetJobOutputs(context.Background(), jobID.String())
	if err != nil {
		t.Fatalf("GetJobOutputs failed: %v", err)
	}
	if len(uris) != 0 {
		t.Fatalf("expected empty, got %v", uris)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresJobStore_GetJobOutputs_InvalidUUIDReturnsError(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	store := NewPostgresJobStore(db)
	_, err = store.GetJobOutputs(context.Background(), "not-a-uuid")
	if !errors.Is(err, ErrInvalidJobID) {
		t.Fatalf("expected ErrInvalidJobID, got %v", err)
	}
}

// ── MemoryJobStore tests (sanity) ───────────────────────────

func TestMemoryJobStore_SurvivesRoundTrip(t *testing.T) {
	store := NewMemoryJobStore(time.Hour, 100, nil)

	rec := JobRecord{
		JobID:     "test-id",
		Status:    "Pending",
		Filename:  "data.csv",
		Reducers:  2,
		CreatedAt: time.Now().UTC(),
	}

	if err := store.CreateJob(context.Background(), rec); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	got, err := store.GetJob(context.Background(), "user-1", "test-id")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got == nil || got.Filename != "data.csv" {
		t.Fatalf("expected round-tripped record, got %+v", got)
	}
}

func TestMemoryJobStore_ListReturnsNewestFirst(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := NewMemoryJobStore(time.Hour, 100, func() time.Time { return now })

	store.CreateJob(context.Background(), JobRecord{JobID: "a", Filename: "a.csv", CreatedAt: now})
	now = now.Add(time.Second)
	store.CreateJob(context.Background(), JobRecord{JobID: "b", Filename: "b.csv", CreatedAt: now})

	list, _ := store.ListJobs(context.Background(), "user-1", 100, 0)
	if len(list) != 2 || list[0].Filename != "b.csv" {
		t.Fatalf("expected newest first, got %+v", list)
	}
}
