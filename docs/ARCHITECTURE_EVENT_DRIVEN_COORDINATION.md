# Event-Driven Coordinator Decoupling Proposal (NATS/Kafka)

## Goal

Decouple coordinator state propagation from the current tightly coupled push model by introducing an event backbone (NATS JetStream or Kafka). This enables independent horizontal scaling of schedulers, API instances, and workers while preserving existing correctness guarantees (attempt fencing, lease TTL, and deterministic task transitions).

## Current Constraints

- Scheduler, task assignment, and worker lifecycle updates are tightly bound to synchronous request/response paths.
- Fan-out of state changes is coupled to manager internals, making independent component scaling harder.
- Recovery depends primarily on database state and in-process control loops, with limited replay semantics for state-change notifications.

## Proposed Event Topology

### Topic / Stream Families

1. **Job lifecycle**
   - `jobs.submitted`
   - `jobs.state.changed`
   - `jobs.cancel.requested`
2. **Task lifecycle**
   - `tasks.idle.detected`
   - `tasks.assigned`
   - `tasks.heartbeat.received`
   - `tasks.attempt.completed`
   - `tasks.attempt.failed`
   - `tasks.reaped`
3. **Control / configuration**
   - `system.config.updated`
   - `system.reconcile.tick`
4. **Audit / DLQ**
   - `events.deadletter`
   - `events.replay.requested`

### Event Envelope (all topics)

```json
{
  "event_id": "uuid",
  "event_type": "tasks.assigned",
  "aggregate_type": "task",
  "aggregate_id": "task_id",
  "job_id": "job_id",
  "attempt_id": "attempt_id_or_null",
  "lease_id": "lease_id_or_null",
  "sequence": 12345,
  "emitted_at": "2026-05-08T00:00:00Z",
  "producer": "manager-scheduler",
  "schema_version": 1,
  "payload": {}
}
```

## Ordering and Consistency Model

### Ordering

- **Per-aggregate ordering is required** (job or task key).
- Partition key:
  - job-level events keyed by `job_id`
  - task-level events keyed by `task_id`
- No global ordering requirement across all jobs/tasks.

### Consistency

- Source of truth remains PostgreSQL during migration.
- Event publication uses **transactional outbox**:
  1. Update DDS state in the same DB transaction.
  2. Insert corresponding outbox row.
  3. Background relay publishes outbox to broker.
  4. Relay marks outbox row delivered (idempotent).
- Consumers are **at-least-once** and must be idempotent using `event_id` and monotonic `(task_id, attempt_id, sequence)` guards.
- Existing fencing remains authoritative:
  - reject stale `attempt_id`
  - enforce lease TTL before commit

## Failure Handling and Replay

### Broker / Consumer Failures

- Durable subscriptions and explicit ACKs.
- Retries with bounded exponential backoff.
- Poison events routed to `events.deadletter` with failure metadata.

### Replay

- Replay supported from:
  - Outbox table (authoritative, finite retention by policy), and/or
  - Broker retention window (JetStream/Kafka retention policy).
- Replay is keyed by time range + topic + aggregate filter (`job_id`/`task_id`).
- Consumers must tolerate duplicates and out-of-order cross-aggregate delivery.

### Disaster Recovery

- If broker is temporarily unavailable, outbox accumulates and drains once restored.
- If consumer state is lost, rebuild from DDS snapshot plus replay from last committed consumer offset/checkpoint.

## Phased Migration Plan

### Phase 0 — Design and Contracts

- Define canonical event schemas and versioning policy.
- Add outbox table + relay service behind feature flags.
- No behavior change to existing control paths.

### Phase 1 — Dual Write (Shadow Publish)

- Continue current synchronous orchestration.
- Emit lifecycle events from outbox in parallel.
- Validate event completeness/latency with dashboards and parity checks.

### Phase 2 — Read Side Adoption

- Move non-critical consumers first (metrics/audit/notifications/reconciler hints).
- Keep scheduler decisions authoritative in existing manager logic.

### Phase 3 — Scheduling Decoupling

- Introduce event-driven scheduler workers consuming `tasks.idle.detected`, `jobs.state.changed`, and heartbeat/reaper events.
- Gate rollout with canary partitions and per-topic feature flags.

### Phase 4 — Full Coordinator Decomposition

- Split responsibilities into independently scalable services:
  - API ingest
  - scheduler
  - lease/reaper controller
  - state projector(s)
- Retain DDS invariants and fencing checks as hard safety boundaries.

### Phase 5 — Decommission Legacy Push Paths

- Remove direct push-based fan-out once SLOs are met.
- Keep replay + DLQ operational runbooks as permanent controls.

## Operational Considerations

- **SLOs**: publish latency, consumer lag, DLQ rate, replay completion time.
- **Capacity planning**: topic partitions/streams sized by active jobs/tasks and heartbeat rate.
- **Schema evolution**: backward-compatible envelope + explicit `schema_version`.
- **Security**:
  - mTLS and authN/Z between producers/consumers and broker
  - tenant/user context in payload only when required, minimize sensitive fields
- **Observability**:
  - propagate request/event correlation IDs
  - trace path: API request -> outbox row -> broker offset -> consumer action
- **Runbooks**:
  - broker outage handling
  - DLQ triage and replay procedure
  - consumer rebootstrap procedure

## Initial Success Criteria

- Event coverage for all task/job state transitions reaches >= 99.9% parity with DDS transitions.
- No regression in attempt-fencing correctness or lease-expiry enforcement.
- Coordinator components can be scaled independently without increasing stale-attempt commit risk.
