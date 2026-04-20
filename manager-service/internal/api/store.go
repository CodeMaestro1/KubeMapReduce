package api

import (
	"context"
	"database/sql"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// JobRecord holds the persisted state for a single job, mapping to the JOBS
// and JOB_CONFIGS DDS tables.
type JobRecord struct {
	JobID     string
	Status    string
	Filename  string
	Reducers  int
	CreatedAt time.Time
}

// JobStore abstracts persistent job storage for the API handlers.
// Production uses PostgresJobStore; tests use MemoryJobStore.
type JobStore interface {
	CreateJob(ctx context.Context, rec JobRecord) error
	GetJob(ctx context.Context, jobID string) (*JobRecord, error)
	ListJobs(ctx context.Context, limit, offset int) ([]JobRecord, error)
}

// ── PostgreSQL implementation ───────────────────────────────

const (
	queryInsertAPIJob = `
		INSERT INTO JOBS (job_id, user_id, status, created_at, updated_at)
		VALUES ($1, $2, 'Pending', $3, $4)`

	queryInsertAPIJobConfig = `
		INSERT INTO JOB_CONFIGS (job_id, input_uri, mapper_uri, reducer_uri, combiner_uri, m_tasks, r_tasks, input_checksum)
		VALUES ($1, $2, '', '', '', 0, $3, '')`

	queryListAPIJobs = `
		SELECT j.job_id, j.status, COALESCE(jc.input_uri, ''), COALESCE(jc.r_tasks, 0), j.created_at
		FROM JOBS j
		LEFT JOIN JOB_CONFIGS jc ON j.job_id = jc.job_id
		ORDER BY j.created_at DESC
		LIMIT $1 OFFSET $2`

	queryGetAPIJob = `
		SELECT j.job_id, j.status, COALESCE(jc.input_uri, ''), COALESCE(jc.r_tasks, 0), j.created_at
		FROM JOBS j
		LEFT JOIN JOB_CONFIGS jc ON j.job_id = jc.job_id
		WHERE j.job_id = $1`
)

// PostgresJobStore implements JobStore backed by DDS/PostgreSQL tables.
type PostgresJobStore struct {
	db *sql.DB
}

// NewPostgresJobStore returns a PostgreSQL-backed JobStore.
func NewPostgresJobStore(db *sql.DB) *PostgresJobStore {
	return &PostgresJobStore{db: db}
}

// CreateJob inserts a new job and its configuration into the DDS within a
// single transaction, ensuring atomicity across the JOBS and JOB_CONFIGS tables.
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
	if _, err := tx.ExecContext(ctx, queryInsertAPIJob, jobUUID, uuid.Nil, now, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, queryInsertAPIJobConfig, jobUUID, rec.Filename, rec.Reducers); err != nil {
		return err
	}

	return tx.Commit()
}

// GetJob retrieves a single job by ID. Returns nil, nil when not found.
func (s *PostgresJobStore) GetJob(ctx context.Context, jobID string) (*JobRecord, error) {
	jobUUID, err := uuid.Parse(jobID)
	if err != nil {
		return nil, nil // invalid UUID → not found
	}

	var rec JobRecord
	var dbID uuid.UUID
	err = s.db.QueryRowContext(ctx, queryGetAPIJob, jobUUID).Scan(
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
func (s *PostgresJobStore) ListJobs(ctx context.Context, limit, offset int) ([]JobRecord, error) {
	limit, offset = clampPagination(limit, offset)
	rows, err := s.db.QueryContext(ctx, queryListAPIJobs, limit, offset)
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

// ── In-memory implementation (tests) ────────────────────────

// MemoryJobStore implements JobStore with in-memory storage.
// Used for unit tests to preserve backward-compatible TTL and
// capacity-eviction behaviour.
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
func (m *MemoryJobStore) GetJob(_ context.Context, jobID string) (*JobRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanup()
	rec, ok := m.jobs[jobID]
	if !ok {
		return nil, nil
	}
	return &rec, nil
}

// ListJobs returns all non-expired jobs ordered by creation time descending.
func (m *MemoryJobStore) ListJobs(_ context.Context, limit, offset int) ([]JobRecord, error) {
	limit, offset = clampPagination(limit, offset)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanup()

	var list []JobRecord
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

// clampPagination ensures limit and offset are within valid operational bounds.
func clampPagination(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
