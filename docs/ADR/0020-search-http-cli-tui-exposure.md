# ADR 0020: HTTP, CLI, and TUI Search Exposure

## Status

Accepted. Implements the deferred `HTTP/gRPC/SDK/CLI/TUI/chat` search exposure
from [ADR 0019](0019-postgresql-full-text-search.md) and supersedes the
"deferred to M17" deferral in [ADR 0018](0018-postgresql-metadata-store.md).

## Context

[ADR 0019](0019-postgresql-full-text-search.md) established a PostgreSQL
FTS foundation that is only reachable today through the offline
`brokerd postgres search` admin command. Users cannot search the broker
from running client tooling. The append-only log, durable broker,
HTTP/gRPC split, and chat demo are all reachable, but the projected
search index is not.

Operators and developers need a way to issue a search from:

- a running `brokerd serve-all` process over HTTP;
- the `ferrumq` CLI (mirroring the `topic`/`dlq`/`publish`/... family);
- the read-only `ferrumq-tui` dashboard (key `4`).

This milestone exposes the M16 FTS index through these user-facing
surfaces without changing the broker core, the gRPC data plane, or the
append-only log invariant.

## Decision

### Search endpoint uses `POST` with a JSON body

The search endpoint is `POST /v1/search/messages` with a JSON body of
shape `{ "query": "...", "topic": "...", "limit": 20 }`. The HTTP
response body is `{ "items": [ ... ] }`.

`POST` with a JSON body is chosen over `GET` with query-string
parameters for a privacy-first reason: search query text is arbitrary
user input and may contain secrets, customer values, credentials, or
payload fragments. Query strings can be persisted in access logs,
reverse proxies, browser history, shell history, HTTP client logs, and
tracing layers. Sending the query in the request body keeps raw user
input out of HTTP URLs and out of URL-derived logs.

The HTTP response never echoes the raw query (no `query` field in the
response envelope) and the handler does not include the query in
tracing spans (`#[tracing::instrument(skip_all)]` plus an explicit
sanitized field set).

### Search remains optional and PostgreSQL-backed

If PostgreSQL is not configured, `POST /v1/search/messages` returns
`503 SEARCH_UNAVAILABLE` with a sanitized envelope:

```json
{
  "error": {
    "code": "SEARCH_UNAVAILABLE",
    "message": "search is not configured",
    "details": {},
    "statusCode": 503
  }
}
```

No HTTP route, schema, or gRPC contract changes. Broker publish,
consume, ACK, NACK, recovery, and `brokerd serve-all` continue to work
without PostgreSQL.

### Hexagonal seam: optional `msg-postgres` dep in `msg-control-api`

`msg-control-api` adds an optional `msg-postgres` dependency behind a
`postgres` Cargo feature (default-on, mirroring `msg-runtime`). The
feature gates:

- A new `MessageSearch` trait that returns
  `Result<Vec<SearchResult>, String>` (sanitized).
- A `PostgresRepository` impl of the trait (postgres-feature gated).
- A new `AppState::with_search(broker, Option<Arc<dyn MessageSearch>>)`
  constructor and a new `open_state_with_search(config, search)` entry
  point.

`AppState` holds `Option<Arc<dyn MessageSearch>>` (postgres-feature
gated). The handler returns `503 SEARCH_UNAVAILABLE` when this is
`None`. The trait is kept narrow (single async method returning a
boxed `Send + 'static` future) so it composes with the rest of the
async runtime and so tests can inject a fake implementation.

No `sqlx`, `PgPool`, or `PostgresError` type leaks across the
`msg-control-api` boundary. The only cross-boundary surface is
`msg_postgres::SearchQuery` (already-validated input carrier),
`msg_postgres::SearchResult` (already projected safe metadata), and the
sanitized `String` error from the trait. The HTTP DTOs are camelCase
Rust structs that map from `SearchResult` to the HTTP wire format.

### Decimal-string contract for `uint64` fields

Two fields on `SearchResult` are `i64` in Rust but are emitted as
decimal strings in the HTTP JSON response to preserve precision across
the JSON boundary:

- `offset`
- `timeUnixMs`

`payloadLen` remains a JSON number because it always fits in
`Number.MAX_SAFE_INTEGER` and matches the existing `topic`/`dlq` JSON
shape convention. `rank` is a JSON number (it is a `f32` rank, not an
integer).

The TypeScript protocol package exposes a `decimalStringSchema` and
requires `offset` and `timeUnixMs` to be strings. An HTTP
regression test (`reject_malformed_response_with_numeric_offset`)
asserts the protocol side rejects numeric `offset` values.

### Runtime configuration: `--postgres-database-url` and `FERRUMQ_DATABASE_URL`

`brokerd serve-all` accepts a new `--postgres-database-url <URL>` flag
that takes precedence. When neither the flag nor the `FERRUMQ_DATABASE_URL`
environment variable is set, the server starts normally with search
disabled (503 on the search endpoint).

When a URL is configured:

- The runtime calls `PostgresRepository::connect_with_pool_size(&cfg, 4)`
  with a pool size of 4 (the existing offline `brokerd postgres ...`
  subcommands keep `connect()` at pool size 1 via a
  `connect_with_pool_size(cfg, 1)` delegation).
- The runtime calls `run_migrations(pool)` at startup. Migrations are
  advisory-lock serialized and idempotent. If migrations fail, startup
  fails with a sanitized `RuntimeError::PostgresSetup` message.
- The repository is wrapped in an `Arc<dyn MessageSearch>` and stored
  on `AppState` via `open_state_with_search`.

If the URL is provided but the connection fails, startup fails with a
sanitized `RuntimeError::PostgresSetup("PostgreSQL setup failed: ...")`
message. The URL, password, and SQL details are never logged; only
`PostgresConfig::sanitized_url()` (which masks the password and drops
query parameters) is ever emitted.

### Connection-pool sizing for serving workloads

`PostgresRepository::connect_with_pool_size(config, max_connections)` is
the new configurable constructor. `connect()` is preserved as
`connect_with_pool_size(config, 1)` to keep the offline CLI tools
unchanged. The serving path uses pool size 4, which is small enough to
not overwhelm a local PostgreSQL and large enough to avoid serializing
concurrent HTTP search requests on a single connection. A real-PG
integration test (`connect_with_pool_size_supports_serving_workload`)
exercises two concurrent `search_messages` calls through the same
repository to confirm pool-size 4 does not deadlock.

### Privacy and observability rules

The search handler logs only the following sanitized fields:

- `operation = "search_messages"`
- `method = "POST"`
- `route = "/v1/search/messages"`
- `outcome` ("search_request" / "search_completed" / "search_unavailable" /
  "search_backend_failed")
- `result_count` (integer only)
- `limit` (the bounded numeric limit, not the query)
- `topic_filter_present` (boolean, never the topic value)
- `postgres_configured` (boolean)
- `status` (HTTP status code)

The handler does **not** log:

- the raw search query (no `query` field anywhere in spans);
- a hash of the query (no `query_hash`);
- the raw topic value (only its presence);
- message IDs returned in the result set;
- the idempotency key (never present in search results);
- raw payload bytes (never present in search results);
- the database URL or password.

A `tracing::instrument(skip_all)` attribute on the handler and a
dedicated `search_messages_does_not_log_raw_query_or_topic` test
enforce this. The HTTP, TUI, and CLI request paths send the query in the
POST body rather than an HTTP URL. The `ferrumq search "<query>"` CLI
command still receives the query as a CLI argument, so it may appear in
shell history and process argv; operators should avoid secrets as CLI
search queries when local shell history or process visibility matters.
The TUI sends the query in the same POST body and does not log the query
to stderr.

### TUI search view: read-only, Unicode-safe, minimal

The TUI adds a `4` key that switches to a `search` view. The view
contains an inline query input built on `useState` + `useInput` (no new
dependencies). The input accepts normal Unicode (Ink's `useInput`
returns a JavaScript string, which is Unicode-safe by default). A
defensive `MAX_SEARCH_QUERY_LENGTH` of 256 characters caps the input
size to keep the rendered view and request body bounded.

The TUI search view is read-only with respect to broker data: it only
issues `POST /v1/search/messages` against the HTTP control plane and
never calls the gRPC data plane. It mirrors the existing read-only
contract of the dashboard, topics, and DLQ views.

Deferred TUI polish (cursor/scroll/copy/paste) is explicitly out of
scope and is documented in the "Deferred scope" section of this ADR.

### Search response shape

The HTTP response is `{ "items": [ ... ] }` where each item is a
camelCase object with the following fields:

| Field              | Type             | Notes                                              |
| ------------------ | ---------------- | -------------------------------------------------- |
| `topic`            | string           | Safe metadata                                      |
| `partitionId`      | number (int)     | Non-negative integer                               |
| `offset`           | string           | **Decimal string** (`"12"`, not `12`)              |
| `messageId`        | string           | Safe metadata                                      |
| `eventType`        | string           | Safe metadata                                      |
| `source`           | string           | Safe metadata                                      |
| `subject`          | string \| null   | Optional metadata; `null` when absent              |
| `contentType`      | string           | Safe metadata                                      |
| `timeUnixMs`       | string           | **Decimal string** (`"1700000000000"`)              |
| `payloadLen`       | number (int)     | Non-negative integer; raw bytes never returned     |
| `payloadSha256`    | string           | 64-character hex hash of payload bytes             |
| `rank`             | number (float)   | PostgreSQL `ts_rank` value                         |

The response explicitly excludes `idempotencyKey`, `partitionKey`,
`headers`, and raw payload bytes. The TypeScript protocol `SearchResult`
schema is a closed `z.object` without those fields, so the contract is
enforced at the schema level.

### CLI: `ferrumq search "<query>" --topic <topic> --limit <n> --json`

The CLI adds a top-level `search` command that internally issues the
same `POST /v1/search/messages` call as the TUI. The CLI never opens a
direct PostgreSQL connection. Human output shows safe metadata and a
shortened 12-character `payloadSha256` prefix (`1234567890ab…`). JSON
output uses the full 64-character hash under the existing CLI
top-level wrapper key convention (`{ "search": { "items": [...] } }`).

The CLI's `--json` mode emits decimal-string `offset` and `timeUnixMs`
to match the protocol contract.

### Search does not run on the publish/consume/ACK/NACK hot path

`brokerd serve-all` runs migrations once at startup, then the search
endpoint reuses the same pool. Publish/consume/ACK/NACK do not touch
the search pool and do not block on search queries. This preserves the
"search is derived, never on the hot path" invariant from
[ADR 0019](0019-postgresql-full-text-search.md).

## Consequences

### Positive

- Operators and developers can issue search requests from the running
  broker over HTTP, the CLI, and the TUI without writing custom SQL.
- The append-only message log remains the single source of truth. The
  FTS index is still a derived projection; `brokerd postgres rebuild`
  continues to work.
- The `POST` body design keeps raw user input out of HTTP URLs, access
  logs, proxies, and HTTP client logs. CLI queries may still appear in
  shell history and process argv because they are typed as command
  arguments.
- Broker correctness does not depend on PostgreSQL availability. The
  search endpoint returns 503 when the database is not configured.
- Decimal-string `offset` and `timeUnixMs` preserve full precision
  across the JSON boundary; the TypeScript protocol and CLI both
  enforce this contract.
- The TypeScript protocol package exposes `SearchMessagesRequest`,
  `SearchMessagesResponse`, and `SearchResult` types that explicitly
  exclude `idempotencyKey`, `partitionKey`, `headers`, and raw payload
  bytes.
- The CLI and TUI connect to the HTTP control plane and never open
  direct PostgreSQL connections.
- A log no-leak regression test enforces the privacy contract.

### Negative

- Search results are still a point-in-time snapshot. Fresh publishes
  require `brokerd postgres rebuild` to become searchable. Live
  projection on publish is out of scope.
- The search endpoint's pool size (4) is small. A noisy neighbour
  with many concurrent searches could still queue; operators can scale
  PostgreSQL connection limits if needed.
- The TUI search view is a minimal foundation. Cursor, scroll, copy,
  paste, and history are deferred polish items.
- The TypeScript protocol requires `decimalStringSchema` for `offset`
  and `timeUnixMs`. Any future client that forgets this will see a
  schema validation error; this is intentional and tested.
- The TUI input is capped at 256 characters. Very long queries must
  be split or run via the CLI.

## Privacy Checklist

Before declaring this milestone complete, the following must be
verified by tests and manual inspection:

- [x] No raw search query in any log line, tracing span, or error
  message.
- [x] No raw search query in HTTP response body.
- [x] No raw payload bytes in HTTP response, CLI output, or TUI render.
- [x] No idempotency key in HTTP response, CLI output, or TUI render.
- [x] No database password in any log line, error message, or HTTP
  response.
- [x] User query is bind-parameterized in the SQL `search_messages`
  call (no string interpolation).
- [x] HTTP endpoint does not echo the query.
- [x] TypeScript CLI and TUI do not persist the query to logs.
- [x] No high-cardinality metric labels are introduced. The
  `/v1/search/messages` route is a low-cardinality label; no `topic`,
  `query`, or `message_id` is ever a label.

## Deferred scope

- Live projection on publish (continuous worker that re-indexes new
  messages).
- Search on the gRPC data plane.
- Search via `pg_trgm` or `unaccent` extensions.
- Semantic / vector embeddings.
- Search over header keys and values.
- TUI input cursor, scroll, copy/paste, history, autosuggest.
- Saved searches and per-user dashboards.
- Pagination beyond a bounded limit (operators run `--limit 100` and
  page manually if needed).
- Auth, API keys, mTLS, rate limiting.
- Search in the chat demo.
- gRPC search RPC.

## References

- [docs/API.md](../API.md)
- [docs/CLI.md](../CLI.md)
- [docs/TUI.md](../TUI.md)
- [docs/POSTGRES.md](../POSTGRES.md)
- [docs/ARCHITECTURE.md](../ARCHITECTURE.md)
- [docs/OBSERVABILITY.md](../OBSERVABILITY.md)
- [docs/FAILURE_MODEL.md](../FAILURE_MODEL.md)
- [ADR 0019: PostgreSQL Full-Text Search Foundation](0019-postgresql-full-text-search.md)
- [ADR 0018: PostgreSQL Metadata Store](0018-postgresql-metadata-store.md)
- [ADR 0015: Unified Runtime Single-Process Broker](0015-unified-runtime-single-process-broker.md)
- [PostgreSQL FTS documentation](https://www.postgresql.org/docs/current/textsearch.html)
- [msg-postgres crate](../../crates/msg-postgres)
- [msg-control-api crate](../../crates/msg-control-api)
- [msg-runtime crate](../../crates/msg-runtime)
- [packages/protocol](../../packages/protocol)
- [packages/cli](../../packages/cli)
- [packages/tui](../../packages/tui)
