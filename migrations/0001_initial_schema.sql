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

COMMIT;
