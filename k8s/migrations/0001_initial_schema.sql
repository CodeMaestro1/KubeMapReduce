-- migrations/0001_initial_schema.sql
--
-- Initial DDS schema for the KubeMapReduce platform.
-- Target engine: PostgreSQL 15+.
--
-- This migration is idempotent on a fresh database. Status enum values are
-- enforced with CHECK constraints (rather than native ENUM types) so that
-- adding a new state in a later migration does not require a global ALTER
-- TYPE … RENAME / DROP cycle.

BEGIN;

-- ---------------------------------------------------------------------------
-- Extensions
-- ---------------------------------------------------------------------------

-- gen_random_uuid() lives in pgcrypto on PostgreSQL 15+. Enabled defensively
-- so application code can rely on it for default UUID generation.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- JOBS
-- ---------------------------------------------------------------------------
-- Root entity for each MapReduce submission. The status enum is enforced via
-- CHECK constraint and matches models.JobStatus.
CREATE TABLE IF NOT EXISTS JOBS (
    job_id     UUID        PRIMARY KEY,
    user_id    UUID        NOT NULL,
    status     VARCHAR(16) NOT NULL
        CHECK (status IN ('Pending', 'Running', 'Completed',
                          'Cancelled', 'Failed', 'Cleaning')),
    replica_index INTEGER   NOT NULL CHECK (replica_index >= 0) DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- JOB_CONFIGS
-- ---------------------------------------------------------------------------
-- Immutable, user-supplied configuration. Kept separate from JOBS so the hot
-- status table is not bloated by long URI strings.
CREATE TABLE IF NOT EXISTS JOB_CONFIGS (
    job_id         UUID PRIMARY KEY
        REFERENCES JOBS(job_id) ON DELETE CASCADE,
    input_uri      TEXT NOT NULL,
    mapper_uri     TEXT NOT NULL,
    reducer_uri    TEXT NOT NULL,
    combiner_uri   TEXT NOT NULL DEFAULT '',
    m_tasks        INTEGER NOT NULL CHECK (m_tasks  > 0),
    r_tasks        INTEGER NOT NULL CHECK (r_tasks  > 0),
    input_checksum TEXT NOT NULL DEFAULT ''
);

-- ---------------------------------------------------------------------------
-- TASKS
-- ---------------------------------------------------------------------------
-- Logical Map/Reduce work units. The current_attempt_id column is the
-- denormalized fence pointer used by Manager commit-time validation
-- (Section 5.1, Attempt-Based Commit Protocol). The FK to TASK_ATTEMPTS is
-- added later in this migration, after TASK_ATTEMPTS exists, to break the
-- circular dependency.
CREATE TABLE IF NOT EXISTS TASKS (
    task_id            UUID        PRIMARY KEY,
    job_id             UUID        NOT NULL
        REFERENCES JOBS(job_id) ON DELETE CASCADE,
    task_type          VARCHAR(8)  NOT NULL
        CHECK (task_type IN ('Map', 'Reduce')),
    status             VARCHAR(16) NOT NULL
        CHECK (status IN ('Idle', 'In-Progress', 'Completed', 'Failed')),
    replica_index      INTEGER     NOT NULL CHECK (replica_index >= 0),
    current_attempt_id UUID
);

-- ---------------------------------------------------------------------------
-- TASK_INPUTS
-- ---------------------------------------------------------------------------
-- Byte-range input split assignments. Surrogate BIGSERIAL key preserves the
-- insertion order used by QueryGetTaskInputs (ORDER BY input_assignment_id).
CREATE TABLE IF NOT EXISTS TASK_INPUTS (
    input_assignment_id BIGSERIAL PRIMARY KEY,
    task_id             UUID NOT NULL
        REFERENCES TASKS(task_id) ON DELETE CASCADE,
    input_uri           TEXT    NOT NULL,
    byte_start          BIGINT  NOT NULL CHECK (byte_start >= 0),
    byte_end            BIGINT  NOT NULL,
    split_checksum      TEXT    NOT NULL DEFAULT '',
    CHECK (byte_end >= byte_start)
);

-- ---------------------------------------------------------------------------
-- TASK_ATTEMPTS
-- ---------------------------------------------------------------------------
-- Per-execution lease + fencing record. Lease expiry is computed at runtime
-- as last_renewed_at + lease_ttl seconds; never pre-computed.
CREATE TABLE IF NOT EXISTS TASK_ATTEMPTS (
    attempt_id      UUID        PRIMARY KEY,
    task_id         UUID        NOT NULL
        REFERENCES TASKS(task_id) ON DELETE CASCADE,
    worker_id       TEXT        NOT NULL,
    lease_id        UUID        NOT NULL,
    last_renewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_ttl       INTEGER     NOT NULL CHECK (lease_ttl > 0),
    start_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    end_time        TIMESTAMPTZ,
    status          VARCHAR(8)  NOT NULL
        CHECK (status IN ('Running', 'Success', 'Failed'))
);

-- Now that TASK_ATTEMPTS exists, attach the fence pointer FK from TASKS.
-- ON DELETE SET NULL preserves the task row when an attempt row is reaped.
ALTER TABLE TASKS
    ADD CONSTRAINT tasks_current_attempt_fk
    FOREIGN KEY (current_attempt_id)
    REFERENCES TASK_ATTEMPTS(attempt_id)
    ON DELETE SET NULL
    DEFERRABLE INITIALLY DEFERRED;

-- ---------------------------------------------------------------------------
-- TASK_OUTPUTS
-- ---------------------------------------------------------------------------
-- Worker-produced output shards (1NF). Reduce tasks have NULL partition_index
-- because their output is the final job result rather than a shuffle bucket.
CREATE TABLE IF NOT EXISTS TASK_OUTPUTS (
    output_id       BIGSERIAL PRIMARY KEY,
    task_id         UUID NOT NULL
        REFERENCES TASKS(task_id) ON DELETE CASCADE,
    partition_index INTEGER CHECK (partition_index IS NULL OR partition_index >= 0),
    output_uri      TEXT NOT NULL,
    checksum        TEXT NOT NULL DEFAULT ''
);

-- ---------------------------------------------------------------------------
-- SYSTEM_CONFIG
-- ---------------------------------------------------------------------------
-- Cluster-wide quota and resource limits. The single config_id = 1 row is
-- seeded with conservative defaults so QueryGetSystemConfig always finds a
-- row on a fresh database. Admin CLI updates mutate this row in place.
CREATE TABLE IF NOT EXISTS SYSTEM_CONFIG (
    config_id            INTEGER     PRIMARY KEY CHECK (config_id = 1),
    max_concurrent_pods  INTEGER     NOT NULL CHECK (max_concurrent_pods >  0),
    cpu_limit            TEXT        NOT NULL DEFAULT '500m',
    memory_limit         TEXT        NOT NULL DEFAULT '512Mi',
    worker_replicas      INTEGER     NOT NULL DEFAULT 1
        CHECK (worker_replicas > 0),
    max_jobs_per_node    INTEGER     NOT NULL DEFAULT 4
        CHECK (max_jobs_per_node > 0),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO SYSTEM_CONFIG (
    config_id, max_concurrent_pods, cpu_limit, memory_limit,
    worker_replicas, max_jobs_per_node
) VALUES (
    1, 16, '500m', '512Mi', 1, 4
) ON CONFLICT (config_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------
-- These indexes back the hot-path scheduler and reaper queries:
--   - QuerySelectIdleTask, QueryCountPendingTasksByType  -> TASKS(job_id, status)
--   - QueryCountRunningAttempts                          -> TASK_ATTEMPTS(status)
--   - QuerySelectStaleTasks                              -> TASK_ATTEMPTS(task_id)
--   - QueryGetMapOutputs / QueryGetReduceOutputs join    -> TASK_OUTPUTS(task_id)
CREATE INDEX IF NOT EXISTS idx_tasks_job_id          ON TASKS(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status          ON TASKS(status);
CREATE INDEX IF NOT EXISTS idx_task_attempts_task_id ON TASK_ATTEMPTS(task_id);
CREATE INDEX IF NOT EXISTS idx_task_attempts_status  ON TASK_ATTEMPTS(status);
CREATE INDEX IF NOT EXISTS idx_task_outputs_task_id  ON TASK_OUTPUTS(task_id);
CREATE INDEX IF NOT EXISTS idx_task_inputs_task_id   ON TASK_INPUTS(task_id);

COMMIT;
