# ADR 0018: PostgreSQL Metadata Store

## Status

Accepted.

## Context

FerrumQ's durable append-only message log is well-suited for reliable
at-least-once delivery, but it is not designed for ad-hoc queries, search,
dashboards, or operational tooling. Users and operators need to answer
questions like:

- What topics exist and when were they last used?
- How many messages were published per topic?
- Find messages by source, event type, or time range.
- Inspect message headers and metadata without reading raw segment files.

Adding a PostgreSQL metadata/projection layer provides a familiar query
interface without compromising the append-only log's role as the source of
truth.

## Decision

### PostgreSQL is optional

PostgreSQL must not be required for broker correctness. The broker must start,
publish, consume, ACK, NACK, and recover without a running PostgreSQL
instance. Existing tests must pass without a database.

### Append-only log remains source of truth

The message log under `<data-dir>/messages/` remains the single authoritative
record of published messages. PostgreSQL stores derived metadata only:

- Topic names, partition counts, and timestamps.
- Message metadata fields (ID, event type, source, content type, headers,
  timestamps, optional keys).
- SHA-256 payload hash (not the payload itself).

Delivery state, consumer cursors, pending deliveries, and DLQ entries are not
projected into PostgreSQL. They remain in the broker-state JSONL log and
in-memory broker state.

### Offline rebuild, not continuous projection

The metadata projection is built by an explicit offline command
(`brokerd postgres rebuild`), not by a continuous daemon or live hook. This
keeps the projection layer independent from the broker's hot path and avoids
making broker correctness depend on PostgreSQL availability.

Continuous projection is deferred to a future milestone.

### Repeatable and idempotent

Running the rebuild twice produces the same result without duplicating rows.
Message upserts use
`ON CONFLICT (topic, partition_id, message_offset) DO NOTHING`.
Message-bearing topic timestamps are deterministic minimum/maximum message
timestamps. Empty-topic timestamps are assigned only on first insertion and
remain stable.

### Message_id uniqueness enforced

The `ferrumq_messages` table has a named `UNIQUE (topic, message_id)`
constraint. If the same `message_id` appears at a different
`(partition, message_offset)` than
previously recorded, the rebuild fails with `MessageIdConflict`. This
prevents silent data corruption and keeps the projection faithful to the
append-only log.

### sqlx with runtime-checked queries

`sqlx` (v0.8) with `runtime-tokio` and `postgres` features is used for async
PostgreSQL access. Queries use runtime `query()` / `query_as()` rather than
compile-time `query!()` / `query_as!()` macros, avoiding a build-time
dependency on a live database.

### Existing recovery rules are reused

The rebuild opens `DurableBroker` to validate broker-state JSONL and obtain
authoritative topic partition counts. It validates filesystem partition IDs
and reads expected partitions through `msg-storage::PartitionLog`. This keeps
the documented final-incomplete-line and final-segment repair behavior instead
of introducing a second permissive parser.

### Migrations and projection runs are explicit

Migration execution is serialized, version/name metadata is rechecked while
serialized, and migration SQL plus its tracking row commit atomically.
Projection runs start as `in_progress` and finish as `success` or `error` while
the database remains reachable. A disconnect can prevent the final update.

### Minimal metrics

Metrics are not added in this milestone. The rebuild is an offline admin
command, not a hot-path operation, and the overhead of process-local counters
is not justified without a clear operational use case.

## Consequences

### Positive

- Operators can query message metadata using standard SQL.
- The append-only log remains the single source of truth.
- Broker correctness does not depend on PostgreSQL availability.
- The rebuild is repeatable and auditable through `ferrumq_projection_runs`.
- No raw payload bytes leave the local filesystem.
- Database credentials are never logged (URL is sanitized).
- Migration, query, storage, and projection errors are sanitized.
- Existing tests and CI pass without PostgreSQL.

### Negative

- The projection is a point-in-time snapshot, not live data. Continuous
  updates require a future milestone.
- The `message_id` uniqueness constraint means pre-existing corrupt data
  (same ID at different offsets) will fail the rebuild. This is by design.
- Rebuild time is proportional to the total number of retained message
  records, since every record must be read and hashed.
- The projection schema is v1 and may require migrations as new metadata
  fields are added.
- Rebuild recovery scans durable data once for broker validation and again to
  produce projection rows.

## Alternatives Considered

### PostgreSQL as broker source of truth

Rejected. Making PostgreSQL the source of truth would introduce a single point
of failure, require database transactions on the hot publish path, and
contradict the append-only log architecture.

### Continuous projection daemon

Deferred. A live projection worker would add complexity, require crash-safe
checkpointing, and risk making the broker depend on PostgreSQL. The offline
rebuild command covers the initial use cases.

### Embedded SQLite instead of PostgreSQL

Rejected. SQLite is simpler to deploy but harder to query from external tools,
does not support JSONB natively, and lacks the ecosystem of PostgreSQL for
future search and dashboard work.

### Compile-time SQL macros (`sqlx::query!`)

Rejected. `sqlx::query!` requires a running database at build time, which
complicates CI and developer setup. Runtime-checked queries are simpler and
sufficient for this use case.

## References

- [docs/POSTGRES.md](../POSTGRES.md)
- [docs/ARCHITECTURE.md](../ARCHITECTURE.md)
- [docs/STORAGE_FORMAT.md](../STORAGE_FORMAT.md)
- [docs/BROKER_STATE_FORMAT.md](../BROKER_STATE_FORMAT.md)
- [msg-storage crate](../../crates/msg-storage)
- [msg-postgres crate](../../crates/msg-postgres)
