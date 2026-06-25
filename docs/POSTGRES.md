# PostgreSQL Metadata Store

FerrumQ includes an **optional** PostgreSQL metadata/projection store for
metadata queries, future search, audit views, and operational tooling.

PostgreSQL is **not** required for broker operation. The append-only message
log remains the source of truth. PostgreSQL stores derived metadata only:
no raw payload bytes, no delivery state, no cursor/offset state.

## Architecture

```text
Append-only message log = source of truth (msg-storage)
       |
       |  (offline rebuild)
       v
PostgreSQL metadata store = derived projection (msg-postgres)
  - ferrumq_topics
  - ferrumq_messages
  - ferrumq_projection_runs
```

Broker publish, consume, ACK, and NACK never depend on PostgreSQL. If
PostgreSQL is unavailable, the broker continues working normally.

## Prerequisites

- A running PostgreSQL server (local or remote).
- `psql` client or `docker` for local setup.
- The `FERRUMQ_DATABASE_URL` environment variable or `--database-url` flag.

### Local PostgreSQL with Docker

The optional Make targets manage a disposable PostgreSQL 16 container for
local development:

```sh
make postgres-up
make postgres-wait
```

The defaults are:

- `POSTGRES_CONTAINER=ferrumq-postgres`
- `POSTGRES_PORT=5432`
- `POSTGRES_PASSWORD=ferrumq`

Override them consistently for each target when the defaults conflict with
another local service:

```sh
make postgres-up POSTGRES_CONTAINER=ferrumq-dev POSTGRES_PORT=55432 \
  POSTGRES_PASSWORD=local-only
make postgres-wait POSTGRES_CONTAINER=ferrumq-dev
```

Set the matching connection URL for runtime commands:

```sh
export FERRUMQ_DATABASE_URL=postgres://postgres:ferrumq@localhost:5432/postgres
```

`make postgres-down` forcibly removes the configured container and its
container-local data. These PostgreSQL targets are optional local-development
helpers and are not part of `make ci`.

## Commands

### `brokerd postgres migrate`

Runs schema migrations against the configured PostgreSQL database.

```sh
brokerd postgres migrate \
  --database-url "$FERRUMQ_DATABASE_URL"
```

Creates the following tables if they do not exist:

- `ferrumq_topics` — topic metadata (name, partition count, timestamps).
- `ferrumq_messages` — message metadata only (no raw payload bytes).
- `ferrumq_projection_runs` — tracks rebuild runs for operational visibility.
- `_ferrumq_migrations` — tracks applied migrations.

Migrations are idempotent. Running `migrate` multiple times is safe. Execution
is serialized with a PostgreSQL advisory transaction lock. Applied versions and
names are rechecked while serialized, schema SQL and tracking rows commit
atomically, and failed migrations are not registered.

### `brokerd postgres search`

Searches projected message metadata using PostgreSQL full-text search.

```sh
brokerd postgres search \
  --database-url "$FERRUMQ_DATABASE_URL" \
  --query "order created"

# Optional topic filter
brokerd postgres search \
  --database-url "$FERRUMQ_DATABASE_URL" \
  --query "payment" \
  --topic orders \
  --limit 20

# JSON output
brokerd postgres search \
  --database-url "$FERRUMQ_DATABASE_URL" \
  --query "order" \
  --json
```

The search:

1. Runs migrations to ensure search columns exist.
2. Validates the query (non-empty, contains alphanumeric characters) and
   limit (1..=100).
3. Executes the search using `websearch_to_tsquery('simple', $query)` with
   bind parameters only.
4. Returns results in human-readable or JSON format.

**Searchable fields**: `message_id`, `topic`, `event_type`, `source`,
`subject` (optional), `content_type`.

**Explicitly NOT searched**: raw payload bytes, `payload_sha256`,
`idempotency_key`, `partition_key`, header keys/values, `time_unix_ms`.

**Search results do not expose**: `idempotency_key`, `partition_key`,
`headers`, or raw payload bytes. Only safe projected metadata, payload
length, payload SHA-256, and the FTS rank are returned.

**Ordering**: `rank DESC, time_unix_ms DESC, topic ASC, partition_id ASC,
message_offset ASC`.

**Text search configuration**: `simple` (no stemming, preserves technical
identifiers exactly).

See [ADR 0019](ADR/0019-postgresql-full-text-search.md) for the full
design rationale.

### `brokerd postgres rebuild`

Rebuilds the PostgreSQL metadata projection from the local durable message log.
This is an **offline** operation that reads the data directory and upserts
metadata into PostgreSQL.

```sh
brokerd postgres rebuild \
  --data-dir ./.ferrumq \
  --database-url "$FERRUMQ_DATABASE_URL"
```

The rebuild:

1. Runs existing durable-broker recovery over broker-state metadata. Complete
   malformed records fail; only the documented final incomplete JSONL line is
   tolerated and truncated.
2. Uses recovered topic metadata as the authoritative partition count,
   including topics with no messages.
3. Validates filesystem topic and partition IDs against that metadata.
4. Reads every expected partition through existing storage recovery, including
   rolled segments and records written before publish idempotency.
5. Computes `payload_sha256` (SHA-256 hex digest of raw payload bytes).
6. Upserts topic and message metadata rows.
7. Records the run in `ferrumq_projection_runs` with status and counts.

**Repeatability:** Running `rebuild` twice does not duplicate rows. Message
rows use `ON CONFLICT (topic, partition_id, message_offset) DO NOTHING` for
idempotency.

**Data integrity:** If the same `message_id` appears at a different
`(partition, message_offset)` than previously recorded, the rebuild fails with a
`message_id conflict` error. This prevents silent data corruption.

**Empty topics:** Topics that exist in broker metadata but have no message
records are still projected into `ferrumq_topics` with their partition count.
Their timestamps are assigned only on first insertion and remain stable.
Message-bearing topics use deterministic minimum and maximum message
timestamps.

Each invocation creates one `in_progress` run and normally finishes it as
`success` or `error`. If database connectivity is lost, the final status update
may also be impossible, leaving the row `in_progress`.

## Database URL Resolution

1. `--database-url` CLI flag (takes precedence).
2. `FERRUMQ_DATABASE_URL` environment variable.
3. Missing URL produces a clear error for `postgres` commands only.

The `database-url` must be a valid PostgreSQL connection URI:

```text
postgres://[user[:password]@][host][:port][/database][?options]
```

### Enabling search on the unified runtime

`brokerd serve-all` accepts a `--postgres-database-url <URL>` flag that
takes precedence over `FERRUMQ_DATABASE_URL`. When neither is set, the
server starts with search disabled (`POST /v1/search/messages` returns
`503 SEARCH_UNAVAILABLE`). When set, the runtime:

1. Connects via `PostgresRepository::connect_with_pool_size(&cfg, 4)` —
   pool size 4, vs. the offline CLI tools' `connect()` which keeps pool
   size 1.
2. Calls `run_migrations(pool)` at startup. Migrations are
   advisory-lock serialized and idempotent. Failures fail startup with a
   sanitized `PostgreSQL setup failed: ...` message that does not
   include the URL or password.
3. Wraps the repository in an `Arc<dyn MessageSearch>` and wires it
   into `AppState` so the HTTP control plane can answer search
   requests.

A startup failure with an unreachable database URL **fails startup**
with a sanitized error when a URL is explicitly configured. The
`PostgresConfig::sanitized_url()` helper masks the password and drops
query parameters, so startup logs never include credentials.

## Security

- Database passwords are **never logged**. The connection URL is sanitized
  before logging: `postgres://user:***@host:5432/db`.
- Raw message payload bytes are **never stored** in PostgreSQL. Only metadata
  and a SHA-256 payload hash are projected.
- No payload, idempotency key, message ID, or topic values appear as metric
  labels.
- Connection, migration, query, storage, and projection failures return
  sanitized messages without credentials, payloads, keys, or filesystem paths.

## Schema

### `ferrumq_topics`

| Column | Type | Description |
|--------|------|-------------|
| `name` | `TEXT PRIMARY KEY` | Topic name |
| `partitions` | `INTEGER NOT NULL` | Number of partitions |
| `first_seen_at` | `TIMESTAMPTZ NOT NULL` | Earliest message timestamp (or projection time) |
| `last_seen_at` | `TIMESTAMPTZ NOT NULL` | Latest message timestamp (or projection time) |

### `ferrumq_messages`

| Column | Type | Description |
|--------|------|-------------|
| `topic` | `TEXT NOT NULL` | Topic name |
| `partition_id` | `INTEGER NOT NULL` | Partition ID |
| `message_offset` | `BIGINT NOT NULL` | Offset within partition |
| `message_id` | `TEXT NOT NULL` | Unique message identifier |
| `idempotency_key` | `TEXT` | Optional idempotency key |
| `partition_key` | `TEXT` | Optional partition key |
| `payload_len` | `BIGINT NOT NULL` | Raw payload byte length |
| `payload_sha256` | `TEXT NOT NULL` | SHA-256 hex digest of payload |
| `content_type` | `TEXT NOT NULL` | Message content type |
| `event_type` | `TEXT NOT NULL` | Message event type |
| `source` | `TEXT NOT NULL` | Event source identifier |
| `subject` | `TEXT` | Optional event subject |
| `headers` | `JSONB NOT NULL` | Message headers as key-value pairs |
| `time_unix_ms` | `BIGINT NOT NULL` | Message timestamp in Unix milliseconds |
| `indexed_at` | `TIMESTAMPTZ NOT NULL` | When this row was projected |
| `search_text` | `TEXT NOT NULL` | Derived search text (Milestone 16) |
| `search_vector` | `TSVECTOR NOT NULL` | PostgreSQL FTS vector (Milestone 16) |

**Full-text search index** (Milestone 16):

| Index | Table | Columns | Method |
|-------|-------|---------|--------|
| `idx_messages_search_vector` | `ferrumq_messages` | `search_vector` | GIN |

**Primary key:** `(topic, partition_id, message_offset)`.
**Unique constraint:** `(topic, message_id)` — same `message_id` at a different
message offset is rejected as a data integrity issue.

The initial schema enforces positive topic partition counts, non-negative
partition IDs, message offsets, payload lengths, and run counts, lowercase
64-character SHA-256 text, valid run statuses, and status-consistent
completion/error fields.

### `ferrumq_projection_runs`

| Column | Type | Description |
|--------|------|-------------|
| `id` | `BIGSERIAL PRIMARY KEY` | Auto-incrementing run ID |
| `started_at` | `TIMESTAMPTZ NOT NULL` | When the rebuild started |
| `completed_at` | `TIMESTAMPTZ` | When the rebuild completed |
| `topics_count` | `INTEGER NOT NULL` | Number of topics upserted |
| `messages_count` | `INTEGER NOT NULL` | Number of messages upserted |
| `status` | `TEXT NOT NULL` | `in_progress`, `success`, or `error` |
| `error_message` | `TEXT` | Sanitized error message on failure |

## Testing

The local workflow starts the disposable container, waits for readiness, runs
both PostgreSQL test suites, and removes the container afterward:

```sh
make postgres-up
make postgres-test
make postgres-down
```

`postgres-test` depends on `postgres-wait` and runs:

```sh
cargo test -p msg-postgres
cargo nextest run -p msg-postgres --no-fail-fast
```

Both commands receive a generated `FERRUMQ_POSTGRES_TEST_URL` using
`POSTGRES_PORT` and `POSTGRES_PASSWORD`. When using overrides, pass the same
values to `postgres-up` and `postgres-test`:

```sh
make postgres-up POSTGRES_CONTAINER=ferrumq-dev POSTGRES_PORT=55432 \
  POSTGRES_PASSWORD=local-only
make postgres-test POSTGRES_CONTAINER=ferrumq-dev POSTGRES_PORT=55432 \
  POSTGRES_PASSWORD=local-only
make postgres-down POSTGRES_CONTAINER=ferrumq-dev
```

Each test creates a unique PostgreSQL schema (e.g. `test_migration_0`) and
drops it after completion. Tests are skipped gracefully when the environment
variable is absent:

```sh
# Tests skip with an informative message
cargo test -p msg-postgres
```

## Limitations and Deferred Work

- **Not a source of truth.** PostgreSQL does not own publish, delivery, cursor,
  retry, DLQ, idempotency, or recovery state.
- **Search is exposed through `POST /v1/search/messages`, the `ferrumq search`
  CLI command, and the TUI `4 search` view.** See [ADR
  0020](ADR/0020-search-http-cli-tui-exposure.md). The search surface
  requires the broker to be started with `--postgres-database-url` or
  `FERRUMQ_DATABASE_URL`; otherwise the HTTP endpoint returns
  `503 SEARCH_UNAVAILABLE`, the CLI surfaces a `SEARCH_UNAVAILABLE`
  error, and the TUI shows a friendly unavailable state. The HTTP
  request body carries the query (privacy-first: no query in HTTP URLs,
  access logs, proxies, or HTTP client logs; FerrumQ logs and traces do
  not persist the raw query). The `ferrumq search "<query>"` CLI command
  still receives the query as a CLI argument, so it may appear in shell
  history and process argv; avoid secrets as CLI search queries when
  local shell history or process visibility matters. The gRPC data plane,
  SDK workflows, and chat search remain deferred.
- **Search covers safe metadata only.** Raw payload bytes, `idempotency_key`,
  `partition_key`, header keys/values, and `payload_sha256` are not indexed or
  returned. Payload search and file/blob search are deferred.
- **`simple` text search configuration.** The `simple` parser does not
  tokenize on `/` or `+` (e.g. `application/cloudevents+json` is kept as a
  single token). `pg_trgm`, `unaccent`, and semantic/vector embeddings are
  deferred.
- **No continuous projection worker.** The rebuild is currently an offline
  command. Live metadata streaming from the broker is deferred. Search results
  are a point-in-time snapshot; fresh publishes require a rebuild to become
  searchable.
- **No automatic cleanup or retention.** Projection data grows with the
  message log. Retention policies are deferred.
- **No raw payload storage.** Payload contents are never stored in PostgreSQL.
  File/blob storage is deferred to a future milestone.
- **No web dashboard.** PostgreSQL stores metadata that could power a
  dashboard, but the dashboard itself is deferred.
- **No FerrumQ-managed auth or TLS policy.** PostgreSQL authentication and
  transport settings come only from the configured URL/server.
- **No clustering, tenancy, or exactly-once behavior.** The projection does
  not change FerrumQ's local durable at-least-once contract.

## Related Documents

- [ADR 0018: PostgreSQL Metadata Store](ADR/0018-postgresql-metadata-store.md)
- [ADR 0019: PostgreSQL Full-Text Search Foundation](ADR/0019-postgresql-full-text-search.md)
- [Architecture](ARCHITECTURE.md)
- [SDD](SDD.md)
- [Storage Format](STORAGE_FORMAT.md)
