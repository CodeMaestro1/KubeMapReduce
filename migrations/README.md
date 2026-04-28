# Database Migrations

Numbered SQL migration files that define the schema of the
**Distributed Data Store (DDS)** — the PostgreSQL instance backing the
Manager service.

## Conventions

- Files are named `NNNN_short_description.sql`, where `NNNN` is a
  zero-padded, monotonically increasing integer (`0001`, `0002`, …).
- Each migration must be **idempotent on a fresh database** and applied
  in lexicographic order.
- Schema changes go through new migration files only; never mutate an
  applied file and never alter schema from application code.
- Target engine: **PostgreSQL 15+**.

## Required tables

| Table           | Purpose                                                    |
|-----------------|------------------------------------------------------------|
| `JOBS`          | Root entity for each MapReduce submission                  |
| `JOB_CONFIGS`   | Immutable user-supplied configuration                      |
| `TASKS`         | Logical Map/Reduce work units                              |
| `TASK_INPUTS`   | Byte-range input split assignments                         |
| `TASK_ATTEMPTS` | Per-execution lease + fencing records                      |
| `TASK_OUTPUTS`  | Worker-produced output shards (1NF)                        |
| `SYSTEM_CONFIG` | Cluster-wide quota and resource limits                     |

## Applying

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/0001_initial_schema.sql
```

Acceptance criteria for the initial migration are tracked in issue #85.
