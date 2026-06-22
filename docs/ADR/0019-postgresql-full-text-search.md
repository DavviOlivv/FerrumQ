# ADR 0019: PostgreSQL Full-Text Search Foundation

## Status

Accepted.

## Context

The PostgreSQL metadata projection from Milestone 15 stores safe projected
metadata for topics and messages. Operators and users need a way to find
messages by metadata fields (message ID, event type, source, subject,
content type) without writing custom SQL or reading raw segment files.

A full-text search (FTS) index over the projected metadata provides a
familiar, efficient query interface while keeping the append-only message
log as the single source of truth.

## Decision

### Search is optional and PostgreSQL-backed

Full-text search lives entirely inside the existing PostgreSQL projection.
If PostgreSQL is unavailable, broker publish, consume, ACK, NACK,
recovery, and `brokerd serve-all` continue to work normally. Search is
explicitly invoked via `brokerd postgres search` and never appears on the
hot path.

### Append-only log remains source of truth

The durable append-only message log under `<data-dir>/messages/` is the
single authoritative record of published messages. PostgreSQL stores
derived metadata only, and the FTS index is derived from that projection.

### Migration 002 adds a safe FTS index

A new append-only migration (`migrations/002_full_text_search.sql`) adds:

- `ferrumq_messages.search_text TEXT NOT NULL DEFAULT ''`
- `ferrumq_messages.search_vector TSVECTOR NOT NULL DEFAULT ''::tsvector`
- `idx_messages_search_vector` GIN index on `search_vector`

Migration 001 is not modified. All DDL uses `IF NOT EXISTS` and the
backfill is guarded so the migration is safe to rerun.

Existing rows are backfilled in the migration from safe metadata columns
using `concat_ws(' ', message_id, topic, event_type, source, subject,
content_type)`. `concat_ws` skips `NULL` subject, matching the Rust
`compute_search_text` order.

### Searchable fields

Search covers safe projected metadata only:

- `message_id`
- `topic`
- `event_type`
- `source`
- `subject` (optional)
- `content_type`

### Explicitly NOT searched

- Raw payload bytes (never stored in PostgreSQL)
- `payload_sha256` (hex hash, not meaningful for FTS)
- `idempotency_key` and `partition_key`
- Header keys and header values
- `time_unix_ms`, `indexed_at`
- Database URLs, filesystem paths, delivery IDs, consumer IDs

### Text search configuration: `simple`

The PostgreSQL `simple` text search configuration is used because FerrumQ
metadata is technical and multilingual. `simple` performs no stemming
and no stop-word removal, preserving technical identifiers (message IDs,
event types, source paths) exactly as they appear.

The `simple` parser tokenizes on whitespace and certain non-word
characters (e.g. hyphens, underscores) but treats characters like `/` and
`+` as part of tokens (e.g. `application/cloudevents+json` is not split
on those characters). This is acceptable for FerrumQ metadata because
searchable fields are typically hyphenated identifiers, event types, and
source paths that tokenize cleanly.

### Query function: `websearch_to_tsquery`

User-facing search uses `websearch_to_tsquery('simple', $query)` because
it supports a natural search syntax (quoted phrases, `OR`, `-` exclusion)
while remaining safe with bind parameters. `plainto_tsquery` and
`to_tsquery` are not used because they require careful escaping of
special characters.

### Shared search text derivation

A single Rust function `compute_search_text(&MessageRow)` derives the
search text from safe metadata fields. The SQL backfill in migration 002
uses the equivalent `concat_ws` expression. Both are verified to produce
the same text for rows with and without subject via a parity test in
`models.rs` and a real PostgreSQL test.

### Rust computes `search_text`, SQL computes `search_vector`

`search_text` is bound as a parameter and `search_vector` is computed in
SQL via `to_tsvector('simple', $search_text)`. This avoids duplicating
the `compute_search_text` logic across migration, repository, and tests.

### Query validation

The runtime and repository validate the query string before reaching the
database:

- Empty or blank strings are rejected with `EmptySearchQuery`.
- Strings containing no alphanumeric characters (punctuation-only,
  operator-only) are rejected with `EmptySearchQuery` because they would
  normalize to an empty `tsquery` and produce confusing empty results.
- Limits outside `1..=100` are rejected with `InvalidSearchLimit`.

### Deterministic ordering

Search results are ordered:

1. `rank DESC` (ts_rank with the cover-density ranking)
2. `time_unix_ms DESC`
3. `topic ASC`
4. `partition_id ASC`
5. `message_offset ASC`

This ordering is stable across rebuilds and concurrent calls.

### Search results exclude sensitive fields

`SearchResult` does not include `idempotency_key`, `partition_key`,
`headers`, or raw payload bytes. Only safe projected metadata, payload
length, payload SHA-256, and the FTS rank are returned. The JSON output
verifies this via a dedicated integration test.

### No metrics added

The `postgres search` command is an offline admin query, not a hot-path
operation. No new metrics are introduced.

## Consequences

### Positive

- Operators can find messages by metadata using a familiar SQL-like
  interface.
- The append-only log remains the single source of truth.
- Broker correctness does not depend on PostgreSQL availability.
- The FTS index is a derived projection; rebuilding the index from the
  log is always possible via `brokerd postgres rebuild`.
- Raw payload bytes, idempotency keys, and partition keys are never
  indexed or returned.
- Database credentials are never logged.
- Migration, query, storage, and projection errors are sanitized.
- Existing tests and CI pass without PostgreSQL.
- A real PostgreSQL integration test proves the upgrade path from
  Milestone 15 (migration 001 only) to Milestone 16 (migration 002 with
  backfilled search columns) works correctly.

### Negative

- The `simple` text search configuration does not tokenize on `/` or `+`.
  Content types like `application/cloudevents+json` are kept as a single
  token. Operators searching for `cloudevents` will not match
  `application/cloudevents+json`. This is acceptable because
  FerrumQ metadata is primarily hyphenated identifiers and event types.
- Search is limited to the `brokerd postgres search` command. HTTP,
  gRPC, SDK, CLI, TUI, and chat search are deferred to a future
  milestone.
- Search results are a point-in-time snapshot, not live data. Fresh
  publishes require a rebuild to become searchable.

## Alternatives Considered

### Generated columns for `search_vector`

Using `GENERATED ALWAYS AS (to_tsvector('simple', search_text)) STORED`
would automate the vector computation but couples the schema to the
`simple` configuration and prevents future migration to a different
configuration without a destructive migration. Explicit columns with
SQL-side computation are more flexible.

### Indexing header keys and values

Header values may contain sensitive application metadata. Indexing them
by default would risk leaking sensitive data through search results.
Header keys are also deferred because they are application-specific and
not consistently structured.

### `pg_trgm` or `unaccent` extensions

These would provide fuzzy matching and accent-insensitive search but
add extension dependencies and change the schema. Deferred to a future
milestone if needed.

### Semantic/vector embeddings

Out of scope for this milestone. The foundation is keyword-based FTS
only.

## References

- [docs/POSTGRES.md](../POSTGRES.md)
- [docs/ARCHITECTURE.md](../ARCHITECTURE.md)
- [ADR 0018: PostgreSQL Metadata Store](0018-postgresql-metadata-store.md)
- [ADR 0017: Topic-Scoped Durable Publish Idempotency](0017-topic-scoped-durable-publish-idempotency.md)
- [PostgreSQL Full Text Search documentation](https://www.postgresql.org/docs/current/textsearch.html)
- [msg-postgres crate](../../crates/msg-postgres)
