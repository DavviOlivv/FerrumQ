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
Outage during consume → exponential backoff cap at 30s, warnings coalesced,
cleared on recovery. Publish failures → no automatic retry, unsent input
preserved. Shutdown during backoff → immediate, no timer leaks. Permanent SDK
errors → backoff applied to prevent busy loops.

**Polling and timer correctness**: One consume RPC per session, normal and
backoff delays bounded by Node.js safe timer limits, AbortController-based
cancellation, timers cleared on shutdown, fake-timer tests isolate state.

**Lifecycle safety**: Equivalent config rerenders preserve the active session.
Genuine config changes stop old generation before starting new one. Cleanup runs
once per generation. Stale async callbacks cannot mutate the current generation.
Shutdown is idempotent. Signal listeners are removed.

**Terminal safety**: OSC sequences, ANSI CSI, C0/C1 controls, bidirectional text
override characters, and zero-width characters are stripped. Ordinary Unicode,
Portuguese text, and emoji are preserved. Fields empty after sanitization follow
the malformed-message path.

**Input and configuration**: CLI flags take precedence over env vars over defaults.
Duplicate flags are rejected. Equals-form values are handled correctly including
edge cases (`--name==foo` → `foo`). Unknown flags are flagged.

**Portability**: Build scripts use Node.js APIs for chmod and binary detection
(macOS compatible, graceful no-op on Windows). SIGTERM behavior is
platform-dependent; Esc, Ctrl+C, unmount, and normal exit cleanup are correct
on every supported platform.

Broker semantics and protocol/storage formats were unchanged. No Rust crate
changes. No release tag created. `.ferrumq/` remains ignored and untracked.
