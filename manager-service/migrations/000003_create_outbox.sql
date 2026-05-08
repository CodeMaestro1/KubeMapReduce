-- Migration: Create EVENT_OUTBOX table for transactional outbox pattern
-- Phase 0 of Event-Driven Coordinator Decoupling (Issue #270)
-- Created: 2026-05-08

-- The outbox table stores events that need to be published to the message broker.
-- Events are inserted in the same DB transaction as the domain state change,
-- ensuring atomicity. A background relay service drains this table and publishes
-- events to NATS/Kafka.

CREATE TABLE IF NOT EXISTS EVENT_OUTBOX (
    event_id       UUID PRIMARY KEY,
    event_type     VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(32) NOT NULL,
    aggregate_id   UUID NOT NULL,
    job_id         UUID NOT NULL,
    attempt_id     UUID,
    lease_id       UUID,
    sequence       BIGINT NOT NULL DEFAULT 0,
    emitted_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    producer       VARCHAR(64) NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,
    payload        JSONB NOT NULL,
    -- Delivery tracking
    delivered      BOOLEAN NOT NULL DEFAULT FALSE,
    delivered_at   TIMESTAMP WITH TIME ZONE,
    retry_count    INT NOT NULL DEFAULT 0,
    last_error     TEXT,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for relay service to find undelivered events efficiently
CREATE INDEX IF NOT EXISTS idx_outbox_undelivered
    ON EVENT_OUTBOX (delivered, created_at)
    WHERE delivered = FALSE;

-- Index for querying by aggregate (for replay/debugging)
CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON EVENT_OUTBOX (aggregate_type, aggregate_id, sequence);

-- Index for querying by job_id
CREATE INDEX IF NOT EXISTS idx_outbox_job
    ON EVENT_OUTBOX (job_id, emitted_at);

-- Comments for operational clarity
COMMENT ON TABLE EVENT_OUTBOX IS 'Transactional outbox for event-driven coordinator decoupling (Phase 0)';
COMMENT ON COLUMN EVENT_OUTBOX.event_id IS 'Unique identifier for the event (UUID)';
COMMENT ON COLUMN EVENT_OUTBOX.event_type IS 'Event type string (e.g., tasks.assigned)';
COMMENT ON COLUMN EVENT_OUTBOX.aggregate_type IS 'Type of aggregate: job, task, system';
COMMENT ON COLUMN EVENT_OUTBOX.aggregate_id IS 'UUID of the aggregate root (job_id or task_id)';
COMMENT ON COLUMN EVENT_OUTBOX.sequence IS 'Monotonic sequence number per aggregate for ordering';
COMMENT ON COLUMN EVENT_OUTBOX.payload IS 'JSONB payload containing event-specific data';
COMMENT ON COLUMN EVENT_OUTBOX.delivered IS 'Whether the event has been successfully published to the broker';
COMMENT ON COLUMN EVENT_OUTBOX.retry_count IS 'Number of failed delivery attempts';
COMMENT ON COLUMN EVENT_OUTBOX.last_error IS 'Last error message if delivery failed';
