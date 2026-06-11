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

- TypeScript TUI, public SDK surface, generated TypeScript clients, process-level TypeScript gRPC integration without fixed ports, streaming consume, auth/RBAC, TLS, rate limiting, broker process supervision, observability dashboards/export, clustering, replication, exactly-once semantics, and MaaS/multi-tenancy.

## Milestone 8: TypeScript TUI

- Ink dashboard.
- Broker status.
- Topics.
- Lag.
- DLQ.
- Logs.

## Milestone 9: Observability

- Structured tracing.
- Metrics endpoint.
- Prometheus/Grafana compose.
- OpenTelemetry integration.

## Milestone 10: Hardening Review

- Crash/recovery tests.
- Fuzzing.
- Property tests.
- Concurrency tests.
- Dependency audit.
- Benchmarks.
- Docs reconciliation.
