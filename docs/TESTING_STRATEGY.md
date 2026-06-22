# Testing Strategy

FerrumQ uses Harness Engineering from the first commit. The current harness
covers Rust formatting, checking, linting, unit and integration tests, nextest,
dependency policy checks, TypeScript formatting, linting, typechecking, tests,
builds, built CLI/TUI smoke checks, `brokerd --version`, and repository
hygiene. Historical milestone sections below explain how coverage accumulated
across the domain, broker, storage, HTTP, gRPC, CLI, TUI, and observability
surfaces.

## Unit Tests

Every Rust crate and TypeScript package should keep focused unit tests for local behavior. Milestone 1 includes `msg-core` unit tests for validated names and identifiers, topic partition configuration, message envelope construction, consumer groups, consumers, subscriptions, delivery attempts, ACK/NACK commands, retry policy validation, dead-letter reasons, delivery states, and serde round trips. Milestone 2 adds `msg-broker` tests for create topic, publish partition assignment, consume pending behavior, ACK, NACK, retry backoff, lease expiry, DLQ routing, offset uniqueness, and no-redelivery invariants. Milestone 3 keeps `msg-storage` unit coverage minimal and emphasizes integration tests because correctness depends on filesystem layout and byte-level recovery behavior. Milestone 4 adds `msg-broker` durable integration tests for reopen and failure behavior. Milestone 5 keeps control API behavior in integration tests and retains crate smoke tests. Milestone 6 adds `msg-protocol` compile/exposure tests for generated protobuf types and service symbols. Milestone 9 adds `msg-observability` unit tests for Prometheus text rendering, label escaping, counter lookup, and absence of payload-like values. Other crates and TypeScript packages still keep their smoke coverage.

## Integration Tests

Milestone 2 uses Rust integration-style tests against the public `msg-broker` API while keeping storage and runtime adapters deferred. Milestone 3 uses Rust integration tests against the public `msg-storage` API with `tempfile` roots for first append offset, monotonic and gapless offsets, bounded reads, read-past-end behavior, failed append preservation, reopen recovery, topic and partition isolation, segment rolling, fixed-width segment naming, reads across segment boundaries, invalid configuration, and validated topic path safety.

Storage recovery tests directly mutate local segment files to cover truncated final length, checksum header, and payload bytes; extra final trailing bytes; final checksum mismatch; final invalid JSON; final metadata mismatch; middle checksum mismatch; middle invalid JSON; empty segment handling; and out-of-sequence segment bases. Milestone 4 durable broker tests use `tempfile` roots to cover publish/reopen recovery, ACK/reopen no-redelivery, duplicate and stale delivery IDs, in-flight/reopen redelivery with incremented attempts, NACK/reopen backoff, retry attempt preservation, DLQ/reopen recovery and metadata, partition/offset determinism, failed message append without phantom visibility, broker-state corruption handling, message-log corruption propagation, and segment/recovery integration through `msg-storage`.

Milestone 5 control API integration tests call the Axum router through Tower services, not fixed ports. They cover health, readiness, readiness failure through unavailable broker state, topic creation, duplicate topic `409 Conflict`, invalid names, zero partitions, supported topic punctuation, URL-encoded topic lookup, deterministic topic listing, topic lookup, missing topics, persistence across `open_state` with the same temp directory, duplicate behavior after reopen, status counts, durable DLQ item envelopes seeded through Rust APIs, topic-filtered DLQ inspection, malformed JSON, missing fields, wrong field types, missing JSON content type, unsupported routes, unsupported methods, JSON content type, and sanitized internal storage failures.

Milestone 6 data-plane tests call the tonic service directly with `tonic::Request`, not fixed ports. They cover publish success, unknown and invalid topics, monotonic offsets, deterministic same-key partitioning, empty and large opaque payloads, consume metadata mapping, invalid consumer groups and consumers, invalid `max_messages`, invalid `lease_ms`, empty consumes, max-limit ordering, in-flight lease no-redelivery, deterministic lease-expiry redelivery without sleeps, retry maintenance before DLQ, ACK success and stale/unknown/wrong-consumer errors, duplicate ACK/NACK, ACK-after-NACK, NACK-after-ACK, ACK durability after reopen, NACK retry after reopen, DLQ routing, DLQ reason durability, no redelivery after DLQ, sanitized internal status mapping, unavailable poisoned-state mapping, and a full publish/consume/ACK durable reopen flow. Milestone 14 extends this coverage with producer deduplication, conflict mapping, append-failure non-mutation, crash/reopen index reconstruction, protobuf compatibility fixtures, exact isolated metric deltas, concurrency barriers, and bounded property tests.

Milestone 9 observability integration tests extend existing seams. Control API
tests verify `GET /metrics`, Prometheus content type, known metric names, topic
creation metrics, duplicate and validation error metrics, payload absence, and
unchanged health/readiness/status behavior. Data-plane tests verify publish,
consume, delivery, ACK, NACK, storage append, and sanitized RPC error counters.
Broker/storage coverage verifies DLQ transition counters and final trailing
repair counters without fixed ports or sleeps.

Milestone 11 runtime integration tests start the unified local runtime on
pre-bound ephemeral HTTP and gRPC listeners. They create a topic through real
HTTP, publish/consume/ACK/NACK through real gRPC, fetch HTTP status and DLQ
state, scrape `/metrics`, and assert that topic creation plus gRPC publish,
consume, ACK, NACK, and DLQ counters are present in one process-local registry.
Runtime CLI tests also cover `serve-all --help`, invalid HTTP and gRPC listen
addresses, invalid data directories, invalid tracing format behavior, and
HTTP/gRPC bind failures.

## End-to-End Tests

Future E2E tests will launch the broker runtime and use CLI/SDK flows for create topic, publish, consume, ACK/NACK, retries, and DLQ inspection. Milestone 5 adds runtime smoke coverage for `brokerd --version`, `brokerd serve --help`, and invalid listen-address parsing. Milestone 6 adds `brokerd serve-grpc --help`, invalid gRPC listen-address coverage, and invalid gRPC data-directory failure coverage. Milestone 11 adds a Rust process-level unified runtime test through real sockets without fixed ports.

## Property-Based Tests

Use `proptest` for domain invariants such as offset ordering, partition selection, retry attempt bounds, cursor advancement, and envelope validation. Milestone 1 adds focused property tests for topic-name validation and offset ordering. Milestone 2 adds loop-style broker tests for unique offsets, ACKed messages never returning, and messages being externally observable as available, pending, ACKed, retry-scheduled, or DLQ.

## Concurrency Tests

Use `loom` for concurrency-sensitive broker and storage logic once shared state, workers, or async coordination are introduced.

## Fuzzing

Use `cargo-fuzz` for protocol parsing, storage record parsing, recovery, and corrupted input handling.

## Crash and Recovery Tests

Durable storage milestones must include tests for restart, partial segment write, corrupted record, cursor restoration, and DLQ recovery. Milestone 3 covers storage-local reopen recovery, partial trailing-frame truncation, final trailing-frame checksum, JSON, and metadata repair, and middle-of-segment checksum and JSON errors. Milestone 4 covers broker restart for successfully published messages, successfully ACKed messages, unACKed in-flight messages, NACK retry state, attempt counts, DLQ entries, failed append visibility, broker-state append failure boundaries, recoverable final incomplete state lines, fatal malformed complete state lines, inconsistent state events, and message-log corruption propagation. Consumers must still be idempotent because at-least-once redelivery is expected.

## Benchmarks

Use `criterion` for publish, append, read, ACK/NACK, retry scheduling, and recovery benchmarks after behavior exists.

## CLI and TUI Tests

Use `vitest` for TypeScript unit tests and lightweight built-entrypoint smoke tests. Milestone 7 CLI tests cover command parsing for all command families, command-specific help, config precedence, URL validation, topic and numeric validation, human and JSON output shapes, mocked `fetch` success and FerrumQ error envelopes, malformed HTTP responses, network failure formatting, mocked gRPC publish/consume/ACK/NACK behavior, gRPC status formatting, dynamic proto-load failures, malformed gRPC responses, and non-zero expected failure returns. Built CLI version/root-help/topic-help/publish-help smoke tests run after build.

Milestone 8 protocol tests cover the shared HTTP control-plane client, including endpoint-specific success mapping, FerrumQ error envelopes, malformed non-2xx bodies, network failures, invalid JSON, and schema mismatches. TUI tests cover config defaults/env/flags, URL validation, stackless CLI errors, loader success and refresh failures, dashboard/topics/DLQ/help rendering, `r`, `R`, `1`, `2`, `3`, `?`, `q`, `Q`, unsupported keyboard interactions, stale refresh protection, and built `ferrumq-tui --version` and `--help` smoke checks.

Milestone 12 SDK tests cover client configuration validation, URL validation,
payload encoding for strings, binary, and JSON values, JSON serialization
failure wrapping, HTTP success parsing and error-envelope mapping, network
failure handling, gRPC error mapping, timeout/deadline rejection, idempotent
`close()`, publish with auto-generated defaults, consume with consumer ID
defaults, ACK/NACK, and public exports. Tests mock the fetch transport and do
not require a running broker.

The SDK process-level integration suite reserves explicit loopback ports,
builds or reuses `target/debug/brokerd`, launches `brokerd serve-all`, imports
the built SDK package entry point, and covers health, readiness, topic creation,
publish, unary consume, ACK, status, metrics, and cleanup. Only explicit
loopback permission failures are skipped; startup and correctness failures
remain test failures.

Milestone 13 chat tests include domain unit tests (room/name/text validation,
message encoding/decoding, fatal UTF-8, 32 KiB payload limit, canonical/future
timestamps, ANSI/OSC/control/bidi sanitization, Unicode joiner preservation,
fingerprinted LRU deduplication and conflicts, identity generation, topic and
consumer group naming), application unit tests with mocked SDK
(idle/starting/connected/failed/stopped lifecycle, duplicate start, publish
gating, topic-already-exists handling, transient versus permanent gRPC errors,
backoff recovery, malformed ACK, callback acceptance, and shutdown), and
terminal UI tests with `ink-testing-library` (slow publish serialization,
editing during publish, failure preservation, stale generation isolation,
500-message memory and 200-message render bounds).

The real chat integration suites start `brokerd serve-all` on ephemeral ports
with temporary data directories. They cover two-way chat, three clients,
identical display names, room isolation, simultaneous publish, concurrent topic
creation, isolated client shutdown, and history replay from offset 0. No fixed
ports, arbitrary sleeps, or committed state are used. Explicit loopback
permission failures are skipped; startup and correctness failures remain test
failures.

## PostgreSQL Integration Tests

Milestone 15 adds optional PostgreSQL integration tests in
`crates/msg-postgres`. These tests require a running PostgreSQL instance
accessible via `FERRUMQ_POSTGRES_TEST_URL`. Each test creates a unique
PostgreSQL schema (e.g. `test_migration_0`) for isolation and drops it after
completion. Tests are skipped gracefully when the environment variable is
absent.

Covered scenarios:

- Concurrent migration serialization, rerun safety, exact migration records,
  hardened constraints, `message_offset` presence, and reserved `offset`
  absence.
- Failed migration non-registration and inconsistent migration metadata.
- Repeatable topic/message upserts, named message-ID conflict translation,
  topic isolation, JSONB headers, nullable keys/subjects, and sanitized
  non-conflict database errors.
- Projection run transitions, target validation, stale error clearing, counts,
  and timestamps.
- Two rebuilds with stable topics, unchanged message count, and one successful
  run row per invocation.
- Empty topics, authoritative partition counts, rolled segments, multiple
  partitions, legacy unkeyed records, keyed records, and deduplicated retries.
- Malformed complete broker-state records, storage corruption, invalid
  partition layouts, sanitized failures, and no catchable lingering
  `in_progress` rows.

Milestone 16 extends the PostgreSQL integration suite with full-text
search coverage:

- Migration 002 creates `search_text`, `search_vector`, and the GIN index;
  existing rows are backfilled in place; the migration is safe to rerun.
- New projected rows receive a non-empty `search_vector`.
- Search finds `message_id`, `event_type`, `source`, `subject`, and
  `content_type`.
- Exact topic filtering returns only rows from the requested topic.
- Empty, blank, and punctuation-only queries are rejected with
  `EmptySearchQuery` before reaching the database.
- Limits outside `1..=100` are rejected with `InvalidSearchLimit`.
- Ordering is deterministic for ties.
- Raw payload-only content is not searchable.
- Search results do not expose `idempotency_key`, `partition_key`,
  `headers`, or raw payload bytes.
- Rebuild twice does not duplicate rows and search still works.
- Empty topics do not produce fake message results.
- A pre-Milestone-16 database (migration 001 only) upgraded in place to
  migration 002 gets searchable rows.
- The Rust `compute_search_text` function matches the SQL `concat_ws`
  backfill expression for rows with and without subject.
- Runtime CLI smoke tests cover `brokerd postgres search --help`, missing
  query, punctuation-only query, and invalid limit; all error paths
  sanitize credentials.

No unit test or existing test depends on PostgreSQL. The crate's unit tests
(config precedence, URL sanitization, payload hash computation, row mapping,
search text computation, query validation, limit validation) run without
any database.

For release-facing validation, use PostgreSQL 16, wait for readiness with
`pg_isready`, and run both Cargo and nextest suites with
`FERRUMQ_POSTGRES_TEST_URL` set.

## CI Gates

The local and CI release gate is `make ci`, which runs:

- `cargo fmt --all --check`.
- `cargo check --workspace`.
- `cargo clippy --workspace --all-targets -- -D warnings`.
- `cargo test --workspace`.
- `cargo nextest run --workspace`.
- `cargo deny check`.
- `pnpm install --frozen-lockfile`.
- `pnpm format:check`.
- `pnpm lint`.
- `pnpm typecheck`.
- `pnpm run examples:typecheck`.
- `pnpm test`.
- `pnpm build`.
- `node packages/cli/dist/cli.js --version`.
- `node packages/cli/dist/cli.js --help`.
- `node packages/cli/dist/cli.js topic --help`.
- `node packages/cli/dist/cli.js publish --help`.
- `node packages/tui/dist/cli.js --version`.
- `node packages/tui/dist/cli.js --help`.
- `node packages/chat/dist/cli.js --help`.
- `cargo run -p msg-runtime --bin brokerd -- --version`.
- `git diff --check`.

`cargo deny check` can emit a duplicate `hashbrown` warning. It is non-fatal
only when the command exits successfully.

The Windows-focused CI job separately validates frozen dependency installation,
SDK/chat typecheck, SDK/chat tests, SDK/chat builds, automatic `brokerd.exe`
preparation, and the built chat help entry point. This gate is intentionally
narrower than the Linux `make ci` job and does not imply unrestricted Windows
terminal support.
