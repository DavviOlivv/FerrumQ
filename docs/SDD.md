# Software Design Document

FerrumQ is a real broker/event bus foundation inspired by Kafka, RabbitMQ, NATS JetStream, and Pulsar. This document is the central specification. Milestone 0 implemented the repository skeleton, documentation, CI, and compile-tested placeholders. Milestone 1 implements the pure Rust `msg-core` domain layer. Milestone 2 implements synchronous deterministic in-memory broker orchestration in `msg-broker`. Milestone 3 implements the independent local append-only message-record log foundation in `msg-storage`. Milestone 4 adds a local durable `DurableBroker` with at-least-once delivery across broker reopen. Milestone 5 adds a local Axum HTTP control-plane API backed by `DurableBroker`.

## 1. Product Vision

FerrumQ should become a local-first, testable, observable messaging engine with durable publish/consume workflows, explicit delivery semantics, and a terminal-first developer experience.

## 2. Scope

The planned product scope includes topics, partitions, publish/consume flows, ACK/NACK, retry with backoff, DLQ routing, idempotency support, consumer cursors, append-only storage, control plane APIs, data plane APIs, and observability.

## 3. Non-Goals

- No exactly-once delivery promise in the initial version.
- No microservice split in early milestones.
- No TypeScript-owned broker semantics.
- No production broker daemon behavior in Milestone 2.
- No durable storage implementation in Milestone 2.
- No durable broker delivery state, ACK/NACK cursor persistence, retry persistence, pending delivery persistence, or DLQ persistence in Milestone 3.
- No broker/storage wiring in Milestone 3.
- No HTTP/gRPC API, CLI/TUI broker semantics, clustering, replication, consensus, or exactly-once behavior in Milestone 4.
- No HTTP publish, consume, ACK, or NACK endpoints in Milestone 5.
- No auth/RBAC, TLS, rate limiting, clustering, replication, consensus, or daemonization in Milestone 5.
- No HTTP/gRPC control or data plane adapters in Milestone 2.
- No retry scheduling workers or DLQ persistence in Milestone 2.

## 4. Architecture Summary

FerrumQ uses a Rust modular monolith with hexagonal architecture. Rust owns domain, broker, storage, runtime, protocol, control API, and observability modules. TypeScript owns CLI, TUI, SDK, and protocol-package surfaces for developer tooling.

## 5. Domain Model

Milestone 1 implements core domain types in `crates/msg-core`, including `MessageId`, `TopicName`, `PartitionId`, `Offset`, `ConsumerGroupId`, `ConsumerId`, `SubscriptionId`, `DeliveryId`, `IdempotencyKey`, `PartitionKey`, `MessageEnvelope`, `Topic`, `Partition`, `ConsumerGroup`, `Consumer`, `Subscription`, `DeliveryAttempt`, `Delivery`, `Ack`, `Nack`, `RetryPolicy`, `DeadLetterReason`, and typed domain errors. Milestone 2 reuses those types from `msg-broker` and does not add broker-specific domain duplicates.

Important identifiers use strong newtypes instead of raw strings or integers. Constructors trim string input and enforce length/character invariants where required. Fields remain private where direct mutation could create invalid state, and serde deserialization uses the same validations for the core value types.

## 6. Message Envelope

Messages use a CloudEvents-inspired envelope with stable metadata: ID, source, type, optional subject, time, content type, headers, optional partition key, optional idempotency key, and opaque payload bytes. Milestone 1 implements the in-memory domain representation and builder-style construction. Full CloudEvents compatibility and external protocol DTOs are future work.

## 7. Topics, Partitions, Offsets

Topics are logical streams. Partitions are ordered append sequences inside a topic. Offsets identify records within a partition. Milestone 2 stores each broker topic partition as an append-only in-memory vector. Milestone 3 adds a separate `msg-storage` partition log that persists message records to local segment files with the same zero-based monotonic and gapless successful-append offset model. Milestone 4 wires `DurableBroker` publish and consume to `msg-storage` partition logs under `<root>/messages`. Messages with a partition key use deterministic FNV-1a 64-bit hashing over key bytes modulo partition count. Messages without a key use a deterministic per-topic round-robin counter. Ordering is guaranteed only within the same topic partition, not globally.

## 8. Consumer Groups and Cursors

Consumer groups coordinate consumption and track cursors. Milestone 2 maintains independent in-memory group state per topic partition: contiguous ACK cursors, pending deliveries, scheduled retries, ACKed offsets, and DLQ offsets. Cursors advance only after contiguous ACKed offsets. A delivered but unacked message is not redelivered until a NACK schedules it and `retry_ready(now)` makes it available, or until lease expiry is processed by `retry_ready(now)`. Milestone 4 persists `DurableBroker` delivery transitions in a local append-only JSONL state log so successful ACKs are not redelivered after reopen, NACK/retry/DLQ state survives reopen, and crash-recovered unACKed pending deliveries become immediately eligible for at-least-once redelivery with the next attempt number. Consumers must be idempotent.

## 9. Publish Flow

Milestone 2 publish flow validates the topic exists, selects a partition deterministically, appends the envelope to the in-memory partition log, and returns topic, partition, offset, and message ID metadata. Milestone 4 keeps `BrokerService` as the in-memory implementation and adds `DurableBroker`, whose publish path appends the envelope to `msg-storage::PartitionLog` before returning. A successfully published durable message is recoverable after broker reopen. A failed durable append returns an error and must not expose a phantom message.

## 10. Consume Flow

Milestone 2 consume flow scans partitions in stable partition-id order and offsets in ascending order within each partition. A consumed message becomes pending for that consumer group with a deterministic delivery ID, attempt number, delivered-at timestamp, and lease expiry timestamp. Pending, retry-scheduled, ACKed, and DLQ messages are not returned by normal consume. Milestone 4 persists durable consume batches before exposing deliveries to callers.

## 11. ACK/NACK Flow

ACK confirms successful processing and can advance a group cursor only through contiguous ACKed offsets. NACK records failure and schedules retry using the configured backoff, or routes to DLQ when the next delivery would exceed max attempts. `DurableBroker` appends and flushes ACK/NACK outcome events before mutating pending, retry, cursor, or DLQ state. Wrong-consumer ACK/NACK attempts fail explicitly. Unknown, stale, already ACKed, retry-scheduled, NACKed, or DLQ delivery IDs fail as not found.

## 12. Retry Policy

Retries use bounded attempts and optional backoff. Milestone 2 has no background scheduler; time is injected through command timestamps and `retry_ready(now)`. That maintenance call moves ready retries back to available and expires pending leases through the same retry or DLQ decision.

## 13. DLQ Policy

A message exceeding max delivery attempts moves to the DLQ for that consumer group. DLQ entries include topic, partition, offset, message ID, original envelope, consumer group, reason, attempt count, and timestamp. `BrokerService` keeps DLQ entries in memory. `DurableBroker` persists DLQ transitions in its broker-state log and reconstructs DLQ entries from durable message records on reopen.

## 14. Idempotency and Deduplication

At-least-once delivery allows duplicates. Milestone 1 models message IDs and idempotency keys as validated values. Deduplication windows and producer/consumer behavior remain future work.

## 15. Backpressure Model

Backpressure applies when memory, storage, partition depth, consumer lag, or retry queues exceed limits. Future APIs should return explicit backpressure errors or readiness state rather than accepting unbounded work.

## 16. Storage Model

The target storage model is an append-only log per topic partition. Milestone 2 uses append-only in-memory vectors local to `msg-broker`. Milestone 3 implements `msg-storage` as a synchronous local durable storage adapter/foundation with segment files at `<root>/topics/<topic>/partitions/<partition-id>/<20-digit-base-offset>.log`. Milestone 4 uses that storage under `<root>/messages` for `DurableBroker` message records and writes broker delivery state under `<root>/broker-state/events.jsonl`.

Each Milestone 3 storage record is framed as `u32_le record_length`, `u32_le crc32(payload)`, and a compact deterministic JSON payload containing `format_version = 1`, topic, partition, offset, and `MessageEnvelope`. Segment names are fixed 20-digit base offsets. Recovery scans segment files in parsed base-offset order, validates checksums, JSON, topic, partition, and offset continuity, and repairs only trailing damage in the final segment by truncating to the start of the damaged record.

Message records and delivery state are separate durable concerns. `msg-storage` segment records are the source of message envelopes and offsets. The Milestone 4 broker-state JSONL log is the source of topic metadata and delivery transitions: consumed batches, ACKs, NACK retry/DLQ outcomes, and retry maintenance batches. The executable broker-state contract is documented in [BROKER_STATE_FORMAT.md](BROKER_STATE_FORMAT.md). Indexes, retention, compaction, fsync policy tuning, APIs, clustering, replication, consensus, and TypeScript behavior remain deferred.

## 17. Crash and Recovery Expectations

A message appended through `msg-storage` must be recoverable according to the local segment recovery rules. In Milestone 4, a `DurableBroker` reopen replays topic metadata and delivery transitions from the broker-state JSONL log, reopens all message partition logs, reconstructs round-robin state from recovered unkeyed message count, preserves successful ACK/NACK/retry/DLQ transitions, and releases any remaining pending deliveries for immediate at-least-once redelivery. A final incomplete broker-state JSONL line may be truncated and ignored; malformed complete broker-state events are durable broker corruption errors. Durability is local filesystem durability, not replicated cluster durability.

## 18. Control Plane

The control plane manages topics, partition inspection, consumer group inspection, DLQ inspection, health, readiness, and configuration visibility. Milestone 5 implements the first HTTP control-plane adapter in `msg-control-api` using Axum. It is backed by a local `DurableBroker` opened from `ControlApiConfig.data_dir`, uses explicit camelCase DTOs, and returns stable error envelopes:

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "...",
    "details": {},
    "statusCode": 400
  }
}
```

`brokerd serve --data-dir ./.ferrumq --listen 127.0.0.1:8080` opens the local durable broker and serves the control router. The Milestone 5 API is control-plane only: it exposes health, readiness, broker status, topic creation/listing/lookup, and DLQ inspection. It does not expose data-plane publish, consume, ACK, or NACK behavior over HTTP.

## 19. Data Plane

The data plane handles publish, consume, ACK, and NACK. gRPC with `tonic` and `prost` is planned for later data plane APIs. Milestone 5 deliberately keeps HTTP data-plane endpoints out of scope.

## 20. Observability

Broker internals must be observable through structured logs and later metrics/traces. Use `tracing`, `tracing-subscriber`, and future OpenTelemetry integration.

## 21. Security Assumptions

Early milestones assume local development. Authentication, authorization, multi-tenant isolation, and secret management are future work and must not be implied by Milestone 2 code.

## 22. Testing Strategy Summary

The harness starts with compile checks, unit tests, TypeScript tests, linting, formatting, and CI. Milestone 1 adds focused Rust unit tests and property tests for core domain invariants. Milestone 2 adds `msg-broker` integration-style Rust tests for topic creation, publish, consume, ACK, NACK, retry, lease expiry, DLQ, offset uniqueness, and no-redelivery invariants. Milestone 3 adds `msg-storage` filesystem integration tests for append/read behavior, segment rolling, reopen recovery, truncation repair, checksum repair for the final trailing frame, and corruption errors. Milestone 4 adds `DurableBroker` reopen, duplicate/stale operation, retry/DLQ, partition/offset, corruption, and persistence-boundary tests for the local durable contract. Milestone 5 adds Tower/Axum router integration tests for health, readiness, topic admin, deterministic listing, persistence, status, DLQ inspection, malformed JSON, and stable error envelopes. Later milestones add E2E tests, broader property tests, concurrency tests, crash/recovery tests, fuzzing, and benchmarks.

## 23. Milestone Roadmap

The roadmap is defined in [MILESTONES.md](MILESTONES.md), from project skeleton through hardening review.

## 24. Invariants

- A message appended through durable storage must be recoverable according to the configured durability policy.
- A delivered but unacked message may be delivered again.
- At-least-once delivery allows duplicates.
- Consumers are expected to be idempotent.
- Offsets and cursors must only advance according to ACK or commit semantics.
- A message exceeding max delivery attempts must move to DLQ.
- Partition key determines partition selection when provided.
- Ordering is guaranteed only within the same topic partition, not globally.
- Control plane changes must not corrupt data plane message flow.
- Broker internals must be observable through structured logs and later metrics/traces.
- Domain constructors must reject invalid core names, identifiers, partition counts, retry attempts, and retry backoff values before adapters or runtime code can depend on them.
