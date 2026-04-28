package api

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// JobRecord holds the persisted state for a single job, mapping to the JOBS
// and JOB_CONFIGS DDS tables.
type JobRecord struct {
	JobID       string
	UserID      string
	Status      string
	Filename    string
	Reducers    int
	CreatedAt   time.Time
	MapperURI   string
	ReducerURI  string
	CombinerURI string
	MTasks      int
}

// ErrInvalidJobID is returned when a provided job ID is not a valid UUID.
var ErrInvalidJobID = errors.New("invalid job id")

// ErrInvalidUserID is returned when a provided user ID is not a valid UUID.
var ErrInvalidUserID = errors.New("invalid user id")

// JobStore abstracts persistent job storage for the API handlers.
//
// This interface allows the API layer to remain agnostic of the underlying
// storage technology. In production, a [PostgresJobStore] is used for durability;
// for unit tests, a [MemoryJobStore] provides fast, isolated execution.
type JobStore interface {
	// CreateJob persists a new job record.
	CreateJob(ctx context.Context, rec JobRecord) error
	// GetJob retrieves a job by its unique identifier for a specific user.
	GetJob(ctx context.Context, userID, jobID string) (*JobRecord, error)
	// ListJobs returns a slice of jobs for a specific user with pagination support.
	ListJobs(ctx context.Context, userID string, limit, offset int) ([]JobRecord, error)
	// ListAllJobs returns a slice of jobs across every user with pagination
	// support. It is intended for ADMIN-scoped requests only; route-level
	// authorization (e.g. auth.RequireRole / auth.HasRole) MUST gate the
	// caller before invoking this method.
	ListAllJobs(ctx context.Context, limit, offset int) ([]JobRecord, error)
	// GetJobOutputs returns ordered output URIs for completed reduce tasks of a job.
	GetJobOutputs(ctx context.Context, jobID string) ([]string, error)
}

// ── PostgreSQL implementation ───────────────────────────────

const (
	queryInsertAPIJob = `
		INSERT INTO JOBS (job_id, user_id, status, created_at, updated_at)
		VALUES ($1, $2, 'Pending', $3, $4)`

	queryInsertAPIJobConfig = `
		INSERT INTO JOB_CONFIGS (job_id, input_uri, mapper_uri, reducer_uri, combiner_uri, m_tasks, r_tasks, input_checksum)
		VALUES ($1, $2, $3, $4, $5, $6, $7, '')`

	queryListAPIJobs = `
		SELECT j.job_id, j.status, COALESCE(jc.input_uri, ''), COALESCE(jc.r_tasks, 0), j.created_at
		FROM JOBS j
		LEFT JOIN JOB_CONFIGS jc ON j.job_id = jc.job_id
		WHERE j.user_id = $1
		ORDER BY j.created_at DESC
		LIMIT $2 OFFSET $3`

	queryListAPIJobsAll = `
		SELECT j.job_id, j.status, COALESCE(jc.input_uri, ''), COALESCE(jc.r_tasks, 0), j.created_at
		FROM JOBS j
		LEFT JOIN JOB_CONFIGS jc ON j.job_id = jc.job_id
		ORDER BY j.created_at DESC
		LIMIT $1 OFFSET $2`

	queryGetAPIJob = `
		SELECT j.job_id, j.status, COALESCE(jc.input_uri, ''), COALESCE(jc.r_tasks, 0), j.created_at
		FROM JOBS j
		LEFT JOIN JOB_CONFIGS jc ON j.job_id = jc.job_id
		WHERE j.job_id = $1 AND j.user_id = $2`

	// QueryGetJobOutputURIs fetches ordered output URIs for completed reduce tasks of a job.
	QueryGetJobOutputURIs = `
		SELECT o.output_uri
		FROM TASK_OUTPUTS o
		JOIN TASKS t ON o.task_id = t.task_id
		WHERE t.job_id = $1
		  AND t.task_type = 'Reduce'
		  AND t.status = 'Completed'
		ORDER BY o.partition_index NULLS LAST, o.output_id`
)

func parseUserUUID(userID string) (uuid.UUID, error) {
	if userID == "" {
		return uuid.Nil, ErrInvalidUserID
	}
	userUUID, err := uuid.Parse(userID)
	if err != nil {
		return uuid.Nil, ErrInvalidUserID
	}
	return userUUID, nil
}

// PostgresJobStore implements JobStore backed by DDS/PostgreSQL tables.
//
// It is the primary production implementation, ensuring that all API instances
// share a consistent view of job statuses.
type PostgresJobStore struct {
	db *sql.DB
}

// NewPostgresJobStore returns a PostgreSQL-backed JobStore.
func NewPostgresJobStore(db *sql.DB) *PostgresJobStore {
	return &PostgresJobStore{db: db}
}

// CreateJob inserts a new job and its configuration into the DDS within a
// single transaction, ensuring atomicity across the JOBS and JOB_CONFIGS tables.
//
// This atomicity is critical: it prevents a "partial" job where the metadata
// exists but the configuration required for scheduling is missing.
func (s *PostgresJobStore) CreateJob(ctx context.Context, rec JobRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	jobUUID, err := uuid.Parse(rec.JobID)
	if err != nil {
		return err
	}

	now := rec.CreatedAt
	userUUID, err := parseUserUUID(rec.UserID)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, queryInsertAPIJob, jobUUID, userUUID, now, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, queryInsertAPIJobConfig, jobUUID, rec.Filename, rec.MapperURI, rec.ReducerURI, rec.CombinerURI, rec.MTasks, rec.Reducers); err != nil {
		return err
	}

	return tx.Commit()
}

// GetJob retrieves a single job by ID. Returns nil, nil when not found.
func (s *PostgresJobStore) GetJob(ctx context.Context, userID, jobID string) (*JobRecord, error) {
	userUUID, err := parseUserUUID(userID)
	if err != nil {
		return nil, err
	}
	jobUUID, err := uuid.Parse(jobID)
	if err != nil {
		return nil, ErrInvalidJobID
	}

	var rec JobRecord
	var dbID uuid.UUID
	err = s.db.QueryRowContext(ctx, queryGetAPIJob, jobUUID, userUUID).Scan(
		&dbID, &rec.Status, &rec.Filename, &rec.Reducers, &rec.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec.JobID = dbID.String()
	return &rec, nil
}

// ListJobs returns all jobs ordered by creation time descending.
func (s *PostgresJobStore) ListJobs(ctx context.Context, userID string, limit, offset int) ([]JobRecord, error) {
	userUUID, err := parseUserUUID(userID)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, queryListAPIJobs, userUUID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []JobRecord
	for rows.Next() {
		var rec JobRecord
		var dbID uuid.UUID
		if err := rows.Scan(&dbID, &rec.Status, &rec.Filename, &rec.Reducers, &rec.CreatedAt); err != nil {
			return nil, err
		}
		rec.JobID = dbID.String()
		jobs = append(jobs, rec)
	}
	return jobs, rows.Err()
}

// ListAllJobs returns every job ordered by creation time descending,
// regardless of ownership. It is intended for ADMIN callers only; the route
// layer is responsible for enforcing the role check before invocation.
func (s *PostgresJobStore) ListAllJobs(ctx context.Context, limit, offset int) ([]JobRecord, error) {
	rows, err := s.db.QueryContext(ctx, queryListAPIJobsAll, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []JobRecord
	for rows.Next() {
		var rec JobRecord
		var dbID uuid.UUID
		if err := rows.Scan(&dbID, &rec.Status, &rec.Filename, &rec.Reducers, &rec.CreatedAt); err != nil {
			return nil, err
		}
		rec.JobID = dbID.String()
		jobs = append(jobs, rec)
	}
	return jobs, rows.Err()
}

// GetJobOutputs returns ordered output URIs for completed reduce tasks of a job.
func (s *PostgresJobStore) GetJobOutputs(ctx context.Context, jobID string) ([]string, error) {
	jobUUID, err := uuid.Parse(jobID)
	if err != nil {
		return nil, ErrInvalidJobID
	}
	rows, err := s.db.QueryContext(ctx, QueryGetJobOutputURIs, jobUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var uris []string
	for rows.Next() {
		var uri string
		if err := rows.Scan(&uri); err != nil {
			return nil, err
		}
		uris = append(uris, uri)
	}
	return uris, rows.Err()
}

// ── In-memory implementation (tests) ────────────────────────

// MemoryJobStore implements JobStore with in-memory storage.
//
// It is used for unit tests to provide fast execution and isolation between
// test cases. It simulates TTL and capacity limits to match historical
// behavior of older non-persistent versions of the API.
type MemoryJobStore struct {
	mu            sync.Mutex
	jobs          map[string]JobRecord
	jobStatusTTL  time.Duration
	maxStoredJobs int
	now           func() time.Time
}

// NewMemoryJobStore creates an in-memory store with configurable TTL and
// maximum capacity, suitable for handler unit tests.
func NewMemoryJobStore(ttl time.Duration, maxJobs int, now func() time.Time) *MemoryJobStore {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if maxJobs <= 0 {
		maxJobs = 10000
	}
	if now == nil {
		now = time.Now
	}
	return &MemoryJobStore{
		jobs:          make(map[string]JobRecord),
		jobStatusTTL:  ttl,
		maxStoredJobs: maxJobs,
		now:           now,
	}
}

// CreateJob stores a job record in memory, performing TTL cleanup and
// capacity eviction as needed.
func (m *MemoryJobStore) CreateJob(_ context.Context, rec JobRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanup()
	m.jobs[rec.JobID] = rec
	m.evict()
	return nil
}

// GetJob retrieves a job by ID; returns nil, nil when not found or expired.
func (m *MemoryJobStore) GetJob(_ context.Context, userID, jobID string) (*JobRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanup()
	if userID == "" {
		return nil, ErrInvalidUserID
	}
	rec, ok := m.jobs[jobID]
	if !ok {
		return nil, nil
	}
	if rec.UserID != "" && rec.UserID != userID {
		return nil, nil
	}
	return &rec, nil
}

// ListJobs returns all non-expired jobs ordered by creation time descending.
func (m *MemoryJobStore) ListJobs(_ context.Context, userID string, limit, offset int) ([]JobRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanup()
	if userID == "" {
		return nil, ErrInvalidUserID
	}

	var list []JobRecord
	for _, rec := range m.jobs {
		if rec.UserID != "" && rec.UserID != userID {
			continue
		}
		list = append(list, rec)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].CreatedAt.After(list[j].CreatedAt)
	})
	if offset >= len(list) {
		return []JobRecord{}, nil
	}
	end := offset + limit
	if end > len(list) {
		end = len(list)
	}
	return list[offset:end], nil
}

// ListAllJobs returns every non-expired job ordered by creation time
// descending, regardless of ownership. It mirrors
// [PostgresJobStore.ListAllJobs] for unit-test environments.
func (m *MemoryJobStore) ListAllJobs(_ context.Context, limit, offset int) ([]JobRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanup()

	list := make([]JobRecord, 0, len(m.jobs))
	for _, rec := range m.jobs {
		list = append(list, rec)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].CreatedAt.After(list[j].CreatedAt)
	})
	if offset >= len(list) {
		return []JobRecord{}, nil
	}
	end := offset + limit
	if end > len(list) {
		end = len(list)
	}
	return list[offset:end], nil
}

// GetJobOutputs returns nil — MemoryJobStore has no task output data.
func (m *MemoryJobStore) GetJobOutputs(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (m *MemoryJobStore) cleanup() {
	now := m.now().UTC()
	for id, rec := range m.jobs {
		if now.Sub(rec.CreatedAt) > m.jobStatusTTL {
			delete(m.jobs, id)
		}
	}
}

func (m *MemoryJobStore) evict() {
	if m.maxStoredJobs <= 0 || len(m.jobs) <= m.maxStoredJobs {
		return
	}

	type entry struct {
		id      string
		created time.Time
	}
	entries := make([]entry, 0, len(m.jobs))
	for id, rec := range m.jobs {
		entries = append(entries, entry{id, rec.CreatedAt})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].created.Before(entries[j].created)
	})
	for _, e := range entries[:len(entries)-m.maxStoredJobs] {
		delete(m.jobs, e.id)
	}
}
