# Milestones

## Milestone 0: Project Skeleton, SDD, Harness

- Cargo workspace.
- pnpm workspace.
- Documentation and ADRs.
- Makefile and CI.
- Minimal Rust `brokerd --version` binary.
- Minimal TypeScript CLI/TUI/SDK/protocol packages.
- Validation commands pass.

Status: implemented.

## Milestone 1: Core Domain

- Message envelope.
- Topics.
- Partitions.
- Offsets.
- Consumer groups.
- ACK/NACK models.
- Domain errors.
- Unit tests.

Status: implemented in `crates/msg-core` as a pure Rust domain layer.

Implemented scope:

- Validated newtypes for message IDs, topic names, partition IDs, offsets, consumer group IDs, consumer IDs, subscription IDs, delivery IDs, idempotency keys, and partition keys.
- CloudEvents-inspired `MessageEnvelope` with typed source, type, optional subject, content type, timestamp, headers, payload, optional partition key, and optional idempotency key.
- Topic, partition, consumer group, consumer, subscription, delivery, ACK/NACK, retry policy, and dead-letter reason domain models.
- Typed `DomainError`/`DomainResult<T>`.
- Serde support for core value types and domain models.
- Unit tests and focused property tests for core invariants.

Deferred from Milestone 1:

- Broker runtime, storage, HTTP/gRPC APIs, TypeScript broker semantics, workers, retry scheduling, and DLQ persistence.

## Milestone 2: In-Memory Broker

- Create topic.
- Publish.
- Consume.
- Ack.
- Nack.
- Basic retry.
- In-memory DLQ.

Status: implemented in `crates/msg-broker` as a synchronous deterministic in-memory broker.

Implemented scope:

- `BrokerService` with owned in-memory state and public create topic, publish, consume, ACK, NACK, retry maintenance, and DLQ query APIs.
- `BrokerConfig` with `RetryPolicy` and validated delivery lease duration.
- Append-only in-memory vectors per topic partition with zero-based monotonic offsets.
- Deterministic partition assignment: FNV-1a 64-bit for keyed messages, per-topic round-robin for unkeyed messages.
- Independent consumer group state with pending deliveries, contiguous ACK cursor advancement, retry scheduling, lease expiry, and DLQ routing.
- Deterministic delivery IDs derived from consumer group, topic, partition, offset, and attempt number.
- Focused Rust tests for topic creation, publish, consume, ACK, NACK, retry, lease expiry, DLQ metadata, offset uniqueness, no-redelivery, and externally observable delivery states.

Deferred from Milestone 2:

- Durable storage, append-only segment files, HTTP/gRPC adapters, runtime workers, background schedulers, TypeScript broker semantics, DLQ persistence, and broker daemon behavior.

## Milestone 3: Append-Only Log

- Segmented log.
- Append.
- Read from offset.
- Checksum.
- Recovery after restart.
- Corruption tests.

Status: implemented in `crates/msg-storage` as an independent synchronous local segment-backed append-only message log.

Implemented scope:

- Message-record persistence only; broker runtime behavior remains the Milestone 2 in-memory implementation.
- `LogConfig`, `PartitionLog`, `StoredMessageRecord`, and typed `StorageError` public API.
- Per-topic-partition segment layout at `<root>/topics/<topic>/partitions/<partition-id>/<20-digit-base-offset>.log`.
- Framed records with `u32_le record_length`, `u32_le crc32(payload)`, and compact JSON payloads containing `format_version = 1`, topic, partition, offset, and `MessageEnvelope`.
- Zero-based, monotonic, gapless offset assignment for successful appends per partition.
- Read-past-end and read-from-next-offset behavior returning empty results.
- Failed append behavior that preserves the in-memory next offset and rolls back write/flush failures to the previous segment length when possible.
- Strict fixed 20-digit segment file naming with invalid unpadded names rejected.
- Segment rolling by `max_segment_bytes` as a roll threshold, including support for a single oversized record in an empty segment.
- Reopen recovery that validates segment ordering, topic, partition, offset continuity, JSON decoding, frame length, and checksums.
- Final-segment trailing-record repair for truncated frames, extra trailing bytes, checksum mismatches, invalid JSON, and metadata mismatches, with corruption errors for non-final or middle-of-segment corruption.
- Integration tests using `tempfile` for append/read behavior, append failure, segment rolling, recovery, isolation, corruption handling, invalid config, and topic path validation.

Deferred from Milestone 3:

- Durable ACK/NACK state, retry state, consumer cursors, pending delivery state, and DLQ persistence.
- Broker/storage wiring.
- Indexes, retention, compaction, fsync policy tuning, APIs, runtime workers, and TypeScript behavior.

## Milestone 4: Delivery Semantics

- At-least-once behavior.
- Pending deliveries.
- Retry with backoff.
- Max attempts.
- Persistent DLQ.
- Idempotent consumer expectation.
- Durable broker delivery cursors and broker/storage wiring.

Status: implemented as a local durable delivery foundation in `crates/msg-broker`.

Implemented scope:

- Public `DurableBroker`, `DurableBrokerConfig`, `DurableBrokerError`, and `DurableBrokerResult` API alongside the unchanged in-memory `BrokerService`.
- Local durable at-least-once delivery using `msg-storage::PartitionLog` for message records under `<root>/messages`.
- Append-only compact JSONL broker-state log under `<root>/broker-state/events.jsonl` for topic metadata, consumed delivery batches, ACKs, NACK retry/DLQ outcomes, and retry maintenance batches.
- Durable publish recovery: successfully published messages are recoverable after broker reopen.
- Durable ACK recovery: successfully ACKed messages are not redelivered after broker reopen.
- Crash recovery for unACKed pending deliveries: remaining pending work is released for immediate at-least-once redelivery with the next attempt number.
- Durable NACK, retry schedule, retry-ready, attempt-count, and DLQ recovery.
- Shared deterministic broker helpers for FNV-1a keyed partitioning, round-robin partition selection, delivery ID generation, and timestamp addition.
- Integration tests for publish/reopen, ACK/reopen, in-flight/reopen, NACK/reopen, retry attempts/reopen, DLQ/reopen, duplicate/stale delivery operations, partition/offset recovery, broker-state corruption, failed append visibility, and segment/recovery integration.
- Crate-local persistence-boundary tests for consumed-delivery, ACK, NACK/retry, retry maintenance, and DLQ state-log append failures.
- Broker-state format documentation in [BROKER_STATE_FORMAT.md](BROKER_STATE_FORMAT.md).

Deferred from Milestone 4:

- HTTP/gRPC API, CLI/TUI broker semantics, runtime daemon behavior, background retry workers, clustering, replication, consensus, exactly-once delivery, retention, compaction, indexes, and fsync policy tuning.
- Replicated durability. Milestone 4 durability is local filesystem durability only.
- Producer or consumer idempotency enforcement. Consumers must be idempotent under at-least-once delivery.

## Milestone 5: Control Plane API

- Axum HTTP API.
- Topic admin.
- Partition inspection.
- Consumer group inspection.
- DLQ inspection.
- Health and readiness.

Status: implemented and hardened as a local control-plane HTTP adapter in `crates/msg-control-api` and `brokerd serve`.

Implemented scope:

- Axum router with `GET /health`, `GET /ready`, `GET /v1/status`, `POST /v1/topics`, `GET /v1/topics`, `GET /v1/topics/{topicName}`, and `GET /v1/dlq`.
- Local durable backing state using `DurableBroker` opened from `ControlApiConfig.data_dir`.
- Read-only durable broker inspection APIs for deterministic topic listing, topic lookup, and local durable status.
- Explicit camelCase JSON DTOs rather than exposing raw domain structs.
- Stable error envelope with `code`, `message`, `details`, and `statusCode`, including explicit unsupported route and unsupported method responses.
- Duplicate topic creation maps to `409 Conflict` through the existing `TopicAlreadyExists` broker contract.
- `brokerd serve --data-dir ./.ferrumq --listen 127.0.0.1:8080` with no daemonization.
- Router integration tests using `tempfile` and Tower calls instead of fixed ports, covering JSON shape errors, validation errors, deterministic ordering, persistence after reopen, durable DLQ inspection, readiness failure, content type behavior, and sanitized internal failures.
- Runtime smoke tests for `brokerd --version`, `brokerd serve --help`, and invalid listen-address parsing.

Deferred from Milestone 5:

- HTTP publish, consume, ACK, and NACK data-plane endpoints.
- Consumer group inspection beyond DLQ entries already tied to consumer groups.
- gRPC/TCP data plane APIs.
- Auth/RBAC, TLS, rate limiting, observability export/metrics dashboards, clustering, replication, consensus, exactly-once semantics, background workers, config files, and TypeScript CLI/TUI integration.

## Milestone 6: Data Plane API

- gRPC with tonic/prost.
- Publish RPC.
- Unary consume RPC.
- ACK/NACK RPC.
- Runtime wiring.

Status: implemented as a local unary gRPC data-plane foundation in `crates/msg-protocol`, `crates/msg-data-plane`, and `brokerd serve-grpc`.

Implemented scope:

- Protobuf contract at `crates/msg-protocol/proto/ferrumq/dataplane/v1/dataplane.proto` with package `ferrumq.dataplane.v1`.
- Generated tonic/prost Rust DTOs, client, and server trait exposed from `msg-protocol`.
- `FerrumQDataPlane` unary `Publish`, `Consume`, `Ack`, and `Nack` RPCs.
- Explicit publish fields for topic, message ID, key, payload, content type, type, source, subject, idempotency key, and Unix-millisecond time.
- Explicit consume fields for topic, consumer group, consumer ID, max messages, lease milliseconds, and Unix-millisecond now.
- Explicit ACK/NACK delivery ownership fields through delivery ID and consumer ID, plus optional NACK reason.
- Local durable at-least-once delivery through `DurableBroker`; consumers must be idempotent.
- `idempotency_key` is carried as publish/consume metadata only and is not enforced for deduplication.
- Per-request consume lease support in `ConsumeCommand`, with existing broker-config leases preserved for older callers.
- `msg-data-plane` adapter backed by `Arc<Mutex<DurableBroker>>`, explicit protobuf-to-domain mapping, public broker API calls only, and sanitized gRPC status mapping.
- `brokerd serve-grpc --data-dir ./.ferrumq --listen 127.0.0.1:9090` runtime wiring, while `brokerd --version` and `brokerd serve` remain unchanged.
- Protocol exposure tests, in-process tonic adapter tests for publish, consume, ACK, NACK, retry/DLQ, sanitized status mapping, and durable reopen flows, and runtime smoke tests for the gRPC subcommand including invalid data-directory handling.

Deferred from Milestone 6:

- Streaming consume.
- Generated TypeScript clients and SDK integration.
- Idempotency-key enforcement and exactly-once semantics.
- Auth/RBAC, TLS, rate limiting, observability export, dashboards, clustering, replication, consensus, MaaS/multi-tenancy, background workers, and production daemon hardening.

## Milestone 7: TypeScript CLI

- Production-grade CLI commands.
- Validation.
- Error formatting.
- E2E tests against broker.

Status: implemented as the first usable TypeScript CLI foundation.

Implemented scope:

- `ferrumq` public binary with `msg` compatibility alias.
- Hand-rolled async parser and command runner split across parsing, config, HTTP/gRPC clients, command handlers, output formatting, and expected-error handling.
- Global `--control-url`, `--grpc-url`, and `--json`, plus `FERRUMQ_CONTROL_URL` and `FERRUMQ_GRPC_URL`, resolved as flag over environment over default.
- Command-specific help for broker, topic, DLQ, publish, consume, ACK, and NACK without HTTP/gRPC client calls.
- HTTP control-plane commands for health, readiness, status, topic create/get/list, and DLQ list.
- Unary gRPC data-plane commands for publish, consume, ACK, and NACK.
- Stable JSON wrappers and human-readable default output, including consume attempt numbers in human output.
- `broker version` as a thin `brokerd --version` wrapper, with broker process management intentionally deferred.
- `@ferrumq/protocol` HTTP DTO schemas, FerrumQ error-envelope schema, gRPC URL normalization, dynamic proto loading, and a tiny CLI-facing data-plane client helper.
- Vitest coverage for parser/config/validation/output/HTTP/gRPC/error behavior and built CLI version/root-help/topic-help/publish-help smoke checks.

Deferred from Milestone 7:

- Public SDK surface, generated TypeScript clients, process-level TypeScript gRPC integration without fixed ports, streaming consume, auth/RBAC, TLS, rate limiting, broker process supervision, observability dashboards/export, clustering, replication, exactly-once semantics, and MaaS/multi-tenancy.

## Milestone 8: TypeScript TUI

- Ink dashboard.
- Broker status.
- Topics.
- DLQ.

Status: implemented as the first read-only TypeScript TUI foundation.

Implemented scope:

- `ferrumq-tui` public binary in `@ferrumq/tui`, separate from the hardened `ferrumq` CLI command surface.
- Ink/React dashboard over the HTTP control plane with local `--help`, `--version`, `--control-url`, and `--grpc-url` parsing.
- Configuration precedence of CLI flag, then `FERRUMQ_CONTROL_URL` or `FERRUMQ_GRPC_URL`, then defaults `http://127.0.0.1:8080` and `http://127.0.0.1:9090`.
- Shared `@ferrumq/protocol` HTTP control-plane client for health, readiness, status, topics, and DLQ inspection, with structured network, HTTP envelope, malformed error body, invalid JSON, and schema-validation failures.
- TUI state for active view, last successful snapshot, loading/error, refresh count, and last refresh timestamp.
- Manual refresh and keyboard navigation: `r`, `q`, `1`, `2`, `3`, and `?`.
- Read-only dashboard, topics, DLQ, help, and footer views. The configured gRPC URL is displayed as state only.
- Vitest coverage for config precedence, loader success/failure behavior, rendering, interactions, and built TUI help/version smoke checks.
- `make ci` safe smoke checks for `node packages/tui/dist/cli.js --version` and `--help`.

Deferred from Milestone 8:

- TUI publish, consume, ACK, NACK, retry, cursor, lag, log streaming, broker process supervision, data-plane gRPC calls, auth/RBAC, TLS, rate limiting, observability dashboards/export, public SDK workflows, streaming consume, clustering, replication, exactly-once semantics, and MaaS/multi-tenancy.

## Milestone 9: Observability

- Structured tracing.
- Metrics endpoint.
- Process-local Prometheus text counters.
- Safe logging and low-cardinality metric labels.

Status: implemented as a focused Rust observability foundation.

Implemented scope:

- `msg-observability` shared crate with tracing initialization, stable metric
  names, process-local counters, helper recording functions, and Prometheus text
  rendering.
- `brokerd serve` and `brokerd serve-grpc` initialize tracing from `RUST_LOG`
  with optional `FERRUMQ_LOG_FORMAT=compact|json`.
- Startup spans/events for HTTP and gRPC runtime modes with operation and listen
  address only.
- `GET /metrics` on the HTTP control plane with Prometheus text content type.
- HTTP control-plane handler spans and request/error counters.
- gRPC data-plane spans and counters for `Publish`, `Consume`, `Ack`, `Nack`,
  delivered messages, and sanitized RPC errors.
- Durable broker spans/events and counters for open/recovery, topic creation,
  publish, consume, delivery creation, ACK, NACK, retry maintenance, and DLQ
  transitions.
- Storage spans/events and counters for partition log open/recovery, append,
  final trailing repair, and sanitized storage error kinds.
- Low-cardinality metric labels limited to `method`, `route`, `status`, `code`,
  and `kind`.
- Tests for metric rendering and escaping, `/metrics`, control-plane create and
  error counters, data-plane success/error counters, DLQ transition metrics, and
  storage repair metrics.
- Documentation in [OBSERVABILITY.md](OBSERVABILITY.md) and ADR
  [0014](ADR/0014-observability-foundation.md).

Deferred from Milestone 9:

- Grafana dashboards, Prometheus/Grafana compose files, OpenTelemetry
  collector/export pipeline, hosted telemetry, auth/TLS/rate limiting for
  metrics, separate data-plane metrics listener, advanced TUI observability
  panels, clustering/replication metrics, exactly-once telemetry, and
  MaaS/multi-tenancy telemetry.

## Milestone 10: Hardening Review

- Crash/recovery tests.
- Fuzzing.
- Property tests.
- Concurrency tests.
- Dependency audit.
- Benchmarks.
- Docs reconciliation.

## Milestone 11: Unified Runtime / Single-Process Broker

- `brokerd serve-all`.
- Shared HTTP control plane and gRPC data plane in one process.
- One process-local `DurableBroker` and metrics registry for local demos.

Status: implemented as the recommended local demo/development runtime in
`crates/msg-runtime`.

Implemented scope:

- `brokerd serve-all --data-dir ./.ferrumq --http-listen 127.0.0.1:8080 --grpc-listen 127.0.0.1:9090`.
- `msg-runtime` library serving functions that accept pre-bound HTTP and gRPC
  listeners for ephemeral-port tests.
- Both HTTP and gRPC listeners are bound before serving starts, so bind
  failures are reported before long-running tasks are left behind.
- Durable state is opened once through `msg_control_api::open_state`.
- The HTTP router uses that `AppState`.
- The gRPC data plane uses `DataPlaneService::from_shared(state.broker())`.
- The synchronization model remains `Arc<Mutex<DurableBroker>>`, matching the
  existing adapters and synchronous local-filesystem broker.
- `brokerd serve` remains HTTP-only, `brokerd serve-grpc` remains gRPC-only,
  and `brokerd --version` remains a no-tracing local command.
- Runtime tests cover `serve-all --help`, invalid HTTP and gRPC listen
  addresses, invalid data directories, invalid log-format behavior, bind
  failures, and a real ephemeral-listener HTTP+gRPC shared-state flow.
- Documentation in [LOCAL_DEMO.md](LOCAL_DEMO.md), [CLI.md](CLI.md),
  [TUI.md](TUI.md), [API.md](API.md), [PROTOCOL.md](PROTOCOL.md),
  [OBSERVABILITY.md](OBSERVABILITY.md), and ADR
  [0015](ADR/0015-unified-runtime-single-process-broker.md).

Deferred from Milestone 11:

- Cross-process live reload, distributed locking, cluster mode, replication,
  shared metrics aggregation across processes, auth/TLS/rate limiting, web
  dashboard, OpenTelemetry collector integration, hosted or SaaS telemetry,
  exactly-once delivery, protobuf changes, HTTP/gRPC error shape changes, and
  storage format changes.

## Milestone 12: TypeScript SDK + Examples

- Reusable `FerrumQClient` for HTTP control plane and gRPC data plane.
- Payload encoding for strings, binary, and JSON-compatible values.
- Typed error model (`FerrumQError`) distinguishing HTTP, gRPC, and SDK errors.
- Per-request timeout support.
- Idempotent `close()` with gRPC channel cleanup.
- Executable examples for basic flow, NACK/DLQ flow, and status/metrics.
- Unit tests with mocked transports.
- SDK documentation.

Status: implemented as `@ferrumq/sdk` with reexports from `@ferrumq/protocol`.

Implemented scope:

- `FerrumQClient` constructor accepting `httpUrl`, `grpcUrl`, and optional
  `timeoutMs` with early validation.
- HTTP control-plane methods: `health()`, `readiness()`, `status()`,
  `createTopic()`, `listTopics()`, `getTopic()`, `listDlq()`, `metrics()`.
- gRPC data-plane methods: `publish()`, `consume()`, `ack()`, `nack()`.
- `publish()` encodes `string` as UTF-8, `Uint8Array`/`Buffer` as binary,
  JSON-compatible values via `JSON.stringify`. Auto-generates `messageId`,
  `type`, `source`, `contentType`, and `timeUnixMs` defaults.
- `consume()` normalizes empty optional proto fields to `null`. Defaults
  `consumerId` to `"ferrumq-sdk"`, `maxMessages` to `1`, `leaseMs` to `30000`.
- `FerrumQError` with `code`, `status`, `transport` (`"http"` | `"grpc"` |
  `"sdk"`), and `cause` fields.
- `ControlPlaneRequestError` from `@ferrumq/protocol` is wrapped into
  `FerrumQError` with transport `"http"`.
- gRPC status codes are converted to string names for the `code` field.
- `close()` is idempotent and closes the gRPC channel.
- `timeoutMs` uses HTTP transport aborts and grpc-js unary deadlines.
- Stable SDK error codes, operation/context metadata, deterministic in-flight
  cancellation, and copied binary payload ownership.
- Real built-package SDK integration coverage against `brokerd serve-all`.
- `@ferrumq/protocol` `DataPlaneClient` interface extended with `close()`.
- `@ferrumq/protocol` `FetchLike` type extended with optional `signal`.
- Unit tests with vitest covering config validation, payload encoding,
  HTTP success/error/network, timeout, close idempotency, and public exports.
- Three executable examples: `basic-flow.ts`, `nack-dlq-flow.ts`,
  `status-metrics.ts`.
- Documentation in [SDK.md](SDK.md) and updated README, LOCAL_DEMO.md,
  ARCHITECTURE.md, PROTOCOL.md, and TESTING_STRATEGY.md.

Deferred from Milestone 12:

- PostgreSQL metadata and projections.
- File payloads and blob storage.
- Authentication and API keys.
- TLS/mTLS.
- Automatic retry policies.
- Browser support.
- Streaming consume.
- Cluster and replication.
- Exactly-once delivery.
- `idempotency_key` enforcement for publish deduplication.

## Milestone 13: Multi-Terminal Chat Example

- Terminal chat application using `@ferrumq/sdk`.
- Multi-client integration test against `brokerd serve-all`.
- Room-per-topic mapping with independent consumer groups per session.
- Session-local deduplication and ACK policy.
- Bounded unary polling with backoff and cancellation.
- Ink/React terminal UI with message display, input, and status.
- CLI with `--name`, `--room`, `--http-url`, `--grpc-url` options.
- Sanitized terminal rendering against ANSI escape and control characters.
- Documentation and ADR for broadcast emulation through consumer groups.

Status: implemented as `@ferrumq/chat` in `packages/chat`.

Implemented scope:

- `ChatApp` application service with join, publish, poll, ACK, and graceful
  shutdown over the public `@ferrumq/sdk` API.
- `ChatUi` Ink/React terminal component with scrollable message display,
  text input, multi-key quit, status indicator, and bounded message history.
- `ChatMessageV1` versioned JSON envelope with sender identity, room,
  session, text, and UTC timestamp fields, validated at the application
  boundary with control-character and ANSI sanitization.
- Room-to-topic mapping (`room "general"` → `chat.general`) with single
  partition for ordered chat display.
- Independent consumer group per participant session to emulate broadcast
  delivery without native broker fan-out.
- Session-local LRU deduplication cache keyed by application message ID.
- ACK for valid and malformed messages (no NACK loops).
- Self-messages received back from broker for confirmed display.
- Bounded polling with configurable interval and exponential backoff on
  transient errors, with AbortController-based cancellation.
- CLI parsing with `--name`, `--room`, `--http-url`, `--grpc-url`,
  `--timeout-ms`, `--poll-interval-ms` and environment variable overrides.
- Unit tests for domain validation, message parsing, sanitization,
  deduplication, identity generation, and application lifecycle.
- Terminal UI tests with mocked SDK for initial render, header, empty state,
  and prompts.
- Real multi-client integration test: spawns `brokerd serve-all` with
  ephemeral ports, creates two independent SDK clients in the same room,
  verifies both receive each other's messages, ACKs deliveries, and confirms
  no redelivery after ACK.
- Documentation in [CHAT.md](CHAT.md) and ADR
  [0016](ADR/0016-chat-broadcast-emulation.md).
- README, LOCAL_DEMO, ARCHITECTURE, TESTING_STRATEGY, and MILESTONES updates.

Deferred from Milestone 13:

- Native broker fan-out subscriptions and consumer-group redesign.
- Streaming gRPC consume.
- WebSockets.
- Presence protocol and typing indicators.
- Private messages and moderation.
- Message editing, deletion, and full-text search.
- History replay control (`--history`, `--from` flags).
- Non-interactive send/receive CLI commands.
- Authentication, authorization, TLS, and encryption.
- Web UI.
- File upload and blob storage.
- PostgreSQL history and search.
- Cluster mode and replication.
- Exactly-once delivery.

### Milestone 13 Hardening Summary

**Multi-client reliability**: Tested with 3+ clients, identical display names,
multiple rooms, concurrent topic creation, client shutdown isolation, and
history-from-offset-0 replay. All participants receive expected messages through
independent consumer groups. Session-local deduplication prevents double display.

**Outage and recovery**: Broker unavailable at startup → error state, no polling.
Timeout, non-shutdown cancellation, and transient gRPC failures during consume
→ exponential backoff from `pollIntervalMs` to
`max(30s, pollIntervalMs)`, warnings coalesced and cleared on recovery.
Permanent configuration, validation, authorization, and invalid-response errors
→ polling stops and the application enters error state. Publish failures → no
automatic retry, unsent input preserved. Shutdown during backoff → immediate,
no timer leaks.

**Polling and timer correctness**: One consume RPC per session, normal and
backoff delays bounded by Node.js safe timer limits, AbortController-based
cancellation, timers cleared on shutdown, fake-timer tests isolate state.

**Lifecycle safety**: Equivalent config rerenders preserve the active session.
Genuine config changes stop old generation before starting new one. Cleanup runs
once per generation. Stale async callbacks cannot mutate the current generation.
Shutdown is idempotent. Publish is allowed only while connected. Duplicate start
calls share one startup attempt. Signal listeners are removed.

**Terminal and payload safety**: Payloads are capped at 32 KiB before fatal
UTF-8 decoding. OSC sequences, ANSI CSI, C0/C1 controls, DEL, bidi controls,
BOM, zero-width space, and word joiner are stripped. ZWJ/ZWNJ remain when
accompanied by visible content. Timestamps must be canonical UTC ISO 8601 and at
most five minutes in the future. Fields empty after sanitization follow the
malformed-message path.

**Input and configuration**: CLI flags take precedence over env vars over defaults.
Only exact `--flag value` and `--flag=value` forms are accepted. Duplicate
flags, missing values, `--flag==value`, unknown flags, and non-decimal integers
are rejected. SDK URL validation runs before UI rendering.

**Deduplication and UI bounds**: The strict 2048-entry LRU stores message ID plus
SHA-256 fingerprint only after display acceptance. Identical content is
deduplicated; conflicting content is warned, suppressed, and ACKed. The UI keeps
500 messages in memory and renders the newest 200.

**Portability**: A shared Node helper detects `brokerd`/`brokerd.exe`, builds the
runtime when absent, and propagates cargo failure. Integration tests select the
platform-specific suffix. Focused Windows CI validates install, SDK/chat
typecheck, tests, build, and chat help smoke; broader Windows terminal support
remains deferred.

Broker semantics and protocol/storage formats were unchanged. No Rust crate
changes. No new release tag is part of this milestone. `.ferrumq/` remains
ignored and untracked. Native fan-out, streaming consume, group cleanup,
history controls, presence, authentication, and exactly-once delivery remain
future work.

## Milestone 14: Durable Publish Idempotency

- Producer-side publish deduplication via `idempotency_key`.
- SHA-256 deterministic intent fingerprint.
- Equivalent retry returns original publish identity.
- Conflicting reuse rejected with `IDEMPOTENCY_KEY_CONFLICT`.
- Recovery-time index rebuild from durable message log.
- Historical duplicate handling.
- Protocol extension: `bool deduplicated = 5` on `PublishResponse`.
- SDK `IDEMPOTENCY_KEY_CONFLICT` normalization.
- CLI deduplication indicator and conflict error.
- Observability counters for dedup and conflict.
- ADR 0017.

Status: implemented.

Implemented scope:

- `msg-broker/src/idempotency.rs`: `PublishFingerprint` (SHA-256 over canonical
  length-prefixed encoding of topic, partition_key, payload, content_type,
  event_type, source, subject, headers), `IdempotencyRecord`, and shared
  helpers used by both the in-memory broker and the durable broker.
- `BrokerError::IdempotencyKeyConflict { topic }` mapped to gRPC `ALREADY_EXISTS`.
- `PublishedMessage` extended with `deduplicated()` accessor.
- In-memory `BrokerService` checks idempotency before partition selection;
  duplicate retries return the original identity without appending or advancing
  round-robin state.
- `DurableBroker` checks idempotency before partition selection and storage
  append. Idempotency index is rebuilt from the message log during `open`
  (canonical order: topic, then partition ID, then offset). Historical
  equivalent duplicates keep the earliest record as canonical; conflicting
  duplicates fail open with `DurableBrokerError::Corruption`.
- Protobuf `PublishResponse.deduplicated` field 5.
- `msg-data-plane` maps `IdempotencyKeyConflict` to
  `Status::already_exists("idempotency key conflict")`.
- Two labelless metrics: `ferrumq_broker_publish_deduplicated_total` and
  `ferrumq_broker_publish_idempotency_conflicts_total`.
- `ferrumq_broker_messages_published_total` counts actual appends only
  (deduplicated retries do not increment it).
- TypeScript protocol `DataPlanePublishResponse.deduplicated: boolean`.
- SDK `PublishResult.deduplicated` and conflict normalization to
  `FerrumQError.code === "IDEMPOTENCY_KEY_CONFLICT"` (preserving
  `grpcStatus === "ALREADY_EXISTS"`).
- CLI human output `(deduplicated)` suffix, JSON `deduplicated` field, and
  `IDEMPOTENCY_KEY_CONFLICT` error on stderr for conflicts.
- Help text explains topic-scoped idempotency key lifetime.
- Integration tests for in-memory dedup, durable dedup, recovery, gRPC
  adapter, SDK, and CLI.
- Documentation updates across README, ARCHITECTURE, FAILURE_MODEL,
  PROTOCOL, SDK, CLI, OBSERVABILITY, BROKER_STATE_FORMAT, RELEASE_CHECKLIST,
  STORAGE_FORMAT, TESTING_STRATEGY, and ADR 0017.

Deferred from Milestone 14:

- Idempotency TTL or retention windows.
- Idempotency record deletion.
- Log compaction.
- Exactly-once delivery.
- Automatic SDK publish retries.
- Clustering or replicated idempotency.
- PostgreSQL or Redis-backed idempotency.

## Milestone 15: PostgreSQL Metadata Store

- Optional PostgreSQL metadata/projection layer.
- Append-only log remains source of truth.
- Schema v1: `ferrumq_topics`, `ferrumq_messages`, `ferrumq_projection_runs`.
- Custom migration runner with runtime SQL files (no compile-time DB).
- `msg-postgres` crate with async `sqlx` repository and offline projection
  rebuild.
- `brokerd postgres migrate` — run schema migrations.
- `brokerd postgres rebuild` — offline rebuild from durable message log.
- Database URL resolution: `--database-url` flag, then `FERRUMQ_DATABASE_URL`.
- Repeatable rebuild:
  `ON CONFLICT (topic, partition_id, message_offset) DO NOTHING`.
- `message_id` uniqueness enforced: same ID at a different location → error.
- Projection runs tracked in `ferrumq_projection_runs` (success/error).
- Broker-state metadata is authoritative for partition counts; durable broker
  and storage recovery rules validate source data.
- Stable message-derived topic timestamps and first-insert-only empty-topic
  timestamps.
- Serialized transactional migrations with validated tracking metadata.
- Schema constraints for ranges, hashes, statuses, and completion fields.
- Repeatable rebuild, empty topics, segment-rolled logs, multiple partitions,
  keyed records, deduplicated retries, and pre-idempotency messages.
- Integration tests behind `FERRUMQ_POSTGRES_TEST_URL`.
- Documentation: `docs/POSTGRES.md`, ADR 0018.
- No broker correctness dependency on PostgreSQL.
- `make ci` passes without a running database.
- No new public gRPC or HTTP contracts.
- No continuous projection worker (deferred).
- No full-text search (deferred).
- No file/blob storage (deferred).
- No web dashboard (deferred).
- No metrics added (offline admin command).
- Sanitized connection, migration, query, source-data, and projection errors.

Status: implemented.

Deferred from Milestone 15:

- Continuous projection daemon/worker.
- Full-text search (`tsvector`, `pg_trgm`, `unaccent`).
- File upload and blob store.
- Web dashboard.
- Authenticated/encrypted PostgreSQL connections.
- Projection retention or cleanup.
- New public gRPC or HTTP data-plane contracts.

## Milestone 16: PostgreSQL Full-Text Search Foundation

- Optional PostgreSQL full-text search over projected metadata.
- Append-only log remains source of truth; FTS is a derived index.
- Append-only migration 002 adds `search_text`, `search_vector` (TSVECTOR),
  and a GIN index on `search_vector`. Migration 001 is not modified.
- Search covers safe projected metadata only: `message_id`, `topic`,
  `event_type`, `source`, `subject` (optional), `content_type`.
- Search does NOT cover: raw payload bytes, `payload_sha256`,
  `idempotency_key`, `partition_key`, header keys/values, `time_unix_ms`.
- `simple` text search configuration (no stemming, preserves technical
  identifiers).
- `websearch_to_tsquery('simple', $query)` with bind parameters for safe
  user-facing query syntax.
- Shared `compute_search_text` function in Rust, matching the SQL
  `concat_ws` backfill expression. Parity verified by unit and
  integration tests.
- `brokerd postgres search --query <QUERY> [--topic <TOPIC>] [--limit N] [--json]`.
- Query validation: empty, blank, and punctuation-only queries are
  rejected with `EmptySearchQuery` before reaching the database. Limits
  outside `1..=100` are rejected with `InvalidSearchLimit`.
- Deterministic ordering: `rank DESC, time_unix_ms DESC, topic ASC,
  partition_id ASC, message_offset ASC`.
- Search results exclude `idempotency_key`, `partition_key`, `headers`,
  and raw payload bytes.
- Upgrade-path test: a pre-Milestone-16 database (migration 001 only with
  existing rows) is upgraded in-place to migration 002 and becomes
  searchable.
- Real PostgreSQL integration tests behind `FERRUMQ_POSTGRES_TEST_URL`.
- Runtime CLI smoke tests for `brokerd postgres search --help`, missing
  query, punctuation-only query, and invalid limit.
- `make ci` passes without PostgreSQL.
- No new public gRPC or HTTP contracts.
- No continuous projection worker (search is offline, requires rebuild).
- No metrics added.
- Sanitized connection, migration, query, source-data, and search errors.
- Documentation: `docs/POSTGRES.md`, ADR 0019.

Status: implemented.

Deferred from Milestone 16:

- HTTP/gRPC/SDK/CLI/TUI/chat search interfaces (planned for M17).
- Semantic/vector embeddings.
- Header key/value search.
- `pg_trgm`, `unaccent` extensions.
- File/blob payload search.
- Continuous live indexing.
- Search metrics.

## Milestone 17: Search HTTP, CLI, and TUI Exposure

Exposes the M16 PostgreSQL full-text search foundation through
user-facing surfaces without changing the broker core, the gRPC data
plane, or the append-only log invariant. The append-only log remains
the source of truth; PostgreSQL is still a derived projection.

Done shape:

- `POST /v1/search/messages` HTTP control-plane endpoint backed by an
  optional `Arc<dyn MessageSearch>` on `AppState`. The endpoint is
  disabled (returns `503 SEARCH_UNAVAILABLE`) when the broker is
  started without a PostgreSQL configuration.
- The endpoint accepts a JSON body
  `{ "query": "...", "topic": "...", "limit": 20 }`, so raw query text
  is not placed in HTTP URLs, access logs, proxies, or HTTP client logs.
  FerrumQ logs and traces do not persist the raw query. CLI queries may
  still appear in shell history and process argv because they are typed
  as command arguments. The response does not echo the query.
- Decimal-string `offset` and `timeUnixMs` in the JSON response
  (preserves full precision across the JSON boundary). `payloadLen`
  and `rank` remain JSON numbers. The TypeScript protocol enforces the
  decimal-string contract via a strict regex schema.
- The response excludes `idempotencyKey`, `partitionKey`, `headers`,
  and raw payload bytes. The TypeScript protocol schema is a closed
  `z.object` without those fields.
- The handler logs only sanitized fields via
  `#[tracing::instrument(skip_all)]`: `operation`, `method`, `route`,
  `outcome`, `result_count`, `limit`, `topic_filter_present` (boolean),
  `postgres_configured` (boolean). The HTTP status code is recorded
  by the shared `observe_http_result` helper, which emits the actual
  status (200, 400, 503) rather than a hardcoded value. No raw query,
  no query hash, no raw topic value, no message IDs, no idempotency
  key, no payload bytes, no database URL.
- `brokerd serve-all` accepts a new `--postgres-database-url <URL>`
  flag with `FERRUMQ_DATABASE_URL` as the environment fallback. The
  flag takes precedence. When neither is set, search is disabled
  (`503 SEARCH_UNAVAILABLE`). When set, the runtime calls
  `PostgresRepository::connect_with_pool_size(&cfg, 4)` (pool size 4
  vs. the offline CLI tools' pool size 1) and runs migrations at
  startup. Migration failures fail startup with a sanitized
  `RuntimeError::PostgresSetup` message. The URL/credentials are
  never logged; only `PostgresConfig::sanitized_url()` is emitted.
- New `PostgresRepository::connect_with_pool_size(config,
  max_connections)` constructor. The existing `connect()` is
  preserved as `connect_with_pool_size(config, 1)` to keep the
  offline CLI tools unchanged. A real-PG integration test
  (`connect_with_pool_size_supports_serving_workload`) exercises two
  concurrent `search_messages` calls through the same repository.
- New `MessageSearch` trait in `msg-control-api` with a single async
  method that returns `Result<Vec<SearchResult>, String>` (sanitized
  error). `PostgresRepository` impls the trait (postgres-feature
  gated). `AppState` holds `Option<Arc<dyn MessageSearch>>`. The
  adapter uses `sqlx::Executor::execute` / `Executor::fetch_all`
  directly on `&mut *tx` to avoid `Send` lifetime issues with
  SQLx's generic async convenience methods. A dedicated
  `migrations_future_is_send` regression test enforces `Send` for
  the `run_migrations` future.
- The TypeScript protocol package adds a `SearchMessagesRequest`
  schema, a `SearchMessagesResponse` schema, a `SearchResult` schema
  (no `idempotencyKey`, `partitionKey`, `headers`, or payload fields),
  a `decimalStringSchema` for `uint64` fields, and a
  `searchMessages(request, options?)` method on `ControlPlaneClient`
  that posts the JSON body and parses the response.
- The `ferrumq search "<query>" --topic <topic> --limit <n> --json`
  CLI command. The CLI connects to the HTTP control plane and never
  opens a direct PostgreSQL connection. Human output shows safe
  metadata and a shortened `payloadSha256` (first 12 hex chars + `…`).
  `--json` output uses the full 64-character hash and the existing
  CLI top-level wrapper key (`{ "search": { "items": [...] } }`).
- The TUI adds a `4 search` view (Unicode-safe inline input on
  `useState` + `useInput`, Enter to submit, Backspace to edit, capped
  at 256 characters as a defensive limit). The outer global
  `useInput` handler is gated on `activeView !== "search"` so global
  navigation keys (`q`, `r`, `1`–`4`, `?`) do not fire while the
  user is editing the search query. The view shows
  idle/loading/empty/error/unavailable states. The TUI does not call
  the gRPC data plane and does not log the query. TUI-side topic
  filtering, cursor, scroll, copy/paste, autosuggest, and saved
  searches are deferred polish.
- Tests:
  - 16 control-API tests cover success, decimal-string fields,
    503 unavailable with no search dependency, validation
    (empty/whitespace/punctuation-only query, out-of-range limit,
    invalid topic), validation-before-availability precedence
    (no PG + invalid input returns 400, not 503), schema
    (malformed body, missing content type, GET instead of POST,
    sanitized backend failure), and a log no-leak regression test
    that the capture contains no raw query or raw topic value.
  - 6 runtime tests cover `serve-all --help`, 503 with no PG,
    sanitized startup failure on unreachable PG, sanitized startup
    failure on invalid `FERRUMQ_DATABASE_URL`, sanitized startup
    failure on invalid `--postgres-database-url`, and the existing
    unified runtime test.
  - 2 compact/json format tests assert that the search
    `log_search_event` field set does not include the raw query or
    raw topic value.
  - 1 msg-postgres send-regression test covers
    `migrations_future_is_send`.
  - 1 msg-postgres integration test covers
    `connect_with_pool_size_supports_serving_workload` (skipped
    without `FERRUMQ_POSTGRES_TEST_URL`).
  - 4 protocol tests cover POST with JSON body, decimal-string
    parsing, 503 `SEARCH_UNAVAILABLE` mapping, and schema rejection
    of numeric `offset`.
  - 9 CLI tests cover parser, human output, JSON output, validation
    (empty query, invalid limit, invalid topic), 503 propagation,
    privacy (no payload bytes / idempotency key in output), the
    accurate `search --help` wording (no shell-history claim), and
    built CLI help.
  - 10 TUI tests cover view rendering, search submission with
    Unicode input, unavailable state, backend error state, backspace
    editing, and regression coverage that printable characters
    (`q`, `r`, digits, `?`) type into the search box instead of
    triggering global navigation.
- Documentation: ADR 0020, `docs/API.md`, `docs/CLI.md`,
  `docs/TUI.md`, `docs/POSTGRES.md`, `docs/ARCHITECTURE.md`,
  `docs/OBSERVABILITY.md`, `docs/FAILURE_MODEL.md`, `README.md`.

Status: implemented.

Deferred from Milestone 17:

- Live projection on publish (continuous worker that re-indexes new
  messages).
- Search on the gRPC data plane.
- Search via `pg_trgm` or `unaccent` extensions.
- Semantic / vector embeddings.
- Search over header keys and values.
- TUI search input cursor, scroll, copy/paste, history, autosuggest,
  saved searches, and topic filter editing.
- Pagination beyond the bounded `1..=100` limit.
- Auth, API keys, mTLS, rate limiting.
- Search in the chat demo.
- gRPC search RPC.
