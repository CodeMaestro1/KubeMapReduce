-- Migration 000004: harden EVENT_OUTBOX for multi-replica relay safety,
-- per-row retry scheduling, and dead-letter type preservation.
--
-- Addresses review findings C1 (multi-replica duplication), C2 (broken
-- batch backoff), C3 (deterministic ordering), C4 (DLQ replay loses
-- original event_type) and M4 (parameterised LIMIT only — no schema impact).
--
-- This migration is forward-only and idempotent.

-- 1. Monotonic ordering column: BIGSERIAL backfills existing rows and
--    becomes the canonical ORDER BY for the relay. The previous
--    aggregate_type/aggregate_id/sequence ordering is undefined when
--    `sequence` is uniformly 0 (which it currently is — see review C3).
ALTER TABLE EVENT_OUTBOX
    ADD COLUMN IF NOT EXISTS id BIGSERIAL;

-- 2. Per-row next-attempt timestamp drives the relay's retry backoff.
--    Defaulting to NOW() means freshly inserted rows are immediately
--    eligible for delivery, preserving today's behaviour.
ALTER TABLE EVENT_OUTBOX
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();

-- 3. Preserve the original event_type when a row is dead-lettered so that
--    ReprocessEvent can restore it on replay. Without this, replayed events
--    would all surface as `tasks.attempt.failed` regardless of their
--    original semantics (see review C4).
ALTER TABLE EVENT_OUTBOX
    ADD COLUMN IF NOT EXISTS original_event_type VARCHAR(64);

-- 4. Replace the old undelivered index with one tailored to the new claim
--    query: SELECT ... WHERE delivered = FALSE AND next_attempt_at <= NOW()
--    ORDER BY id FOR UPDATE SKIP LOCKED.
DROP INDEX IF EXISTS idx_outbox_undelivered;

CREATE INDEX IF NOT EXISTS idx_outbox_pending_ready
    ON EVENT_OUTBOX (next_attempt_at, id)
    WHERE delivered = FALSE;

-- 5. Index for DLQ scans (used by ReplayDeadLetteredEvents).
CREATE INDEX IF NOT EXISTS idx_outbox_dlq
    ON EVENT_OUTBOX (event_type, retry_count DESC, emitted_at)
    WHERE event_type = 'events.deadletter';

COMMENT ON COLUMN EVENT_OUTBOX.id IS 'Monotonic insertion order; canonical ORDER BY for the relay';
COMMENT ON COLUMN EVENT_OUTBOX.next_attempt_at IS 'Earliest time the relay may attempt delivery; updated by RecordDeliveryFailure';
COMMENT ON COLUMN EVENT_OUTBOX.original_event_type IS 'Set when row is dead-lettered; restored by ReprocessEvent';
