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
- Idempotency key support.
- Durable broker delivery cursors and broker/storage wiring.

## Milestone 5: Control Plane API

- Axum HTTP API.
- Topic admin.
- Partition inspection.
- Consumer group inspection.
- DLQ inspection.
- Health and readiness.

## Milestone 6: Data Plane API

- gRPC with tonic/prost.
- Publish RPC.
- Consume stream.
- ACK/NACK RPC.
- Rust client.

## Milestone 7: TypeScript CLI

- Production-grade CLI commands.
- Validation.
- Error formatting.
- E2E tests against broker.

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
