# Software Design Document

FerrumQ is a real broker/event bus foundation inspired by Kafka, RabbitMQ, NATS JetStream, and Pulsar. This document is the central specification. Milestone 0 implemented the repository skeleton, documentation, CI, and compile-tested placeholders. Milestone 1 implements the pure Rust `msg-core` domain layer. Milestone 2 implements synchronous deterministic in-memory broker orchestration in `msg-broker`. Milestone 3 implements the independent local append-only message-record log foundation in `msg-storage`. Milestone 4 adds a local durable `DurableBroker` with at-least-once delivery across broker reopen. Milestone 5 adds a local Axum HTTP control-plane API backed by `DurableBroker`. Milestone 6 adds a unary tonic/prost gRPC data-plane API backed by `DurableBroker`. Milestone 7 adds the first usable TypeScript CLI as an adapter over those Rust-owned APIs. Milestone 8 adds the first read-only TypeScript TUI over the HTTP control plane. Milestone 9 adds structured tracing and process-local Prometheus metrics.

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
- No streaming consume, generated TypeScript gRPC clients, SDK integration, auth/RBAC, TLS, rate limiting, clustering, replication, consensus, MaaS/multi-tenancy, idempotency-key enforcement, exactly-once semantics, or daemonization in Milestone 6.
- No TypeScript broker process supervision, public SDK surface, streaming consume, auth/RBAC, TLS, rate limiting, clustering, replication, consensus, MaaS/multi-tenancy, idempotency-key enforcement, or exactly-once semantics in Milestone 7.
- No TUI publish, consume, ACK, NACK, broker process supervision, data-plane gRPC calls, streaming consume, auth/RBAC, TLS, rate limiting, clustering, replication, consensus, MaaS/multi-tenancy, idempotency-key enforcement, or exactly-once semantics in Milestone 8.
- No dashboards, OpenTelemetry collector/export pipeline, hosted telemetry, auth/TLS/rate limiting for metrics, advanced TUI observability panels, clustering/replication metrics, exactly-once telemetry, or MaaS/multi-tenancy telemetry in Milestone 9.
- No HTTP/gRPC control or data plane adapters in Milestone 2.
- No retry scheduling workers or DLQ persistence in Milestone 2.

## 4. Architecture Summary

FerrumQ uses a Rust modular monolith with hexagonal architecture. Rust owns domain, broker, storage, runtime, protocol, control API, and observability modules. TypeScript owns CLI, TUI, SDK, and protocol-package surfaces for developer tooling.

## 5. Domain Model

Milestone 1 implements core domain types in `crates/msg-core`, including `MessageId`, `TopicName`, `PartitionId`, `Offset`, `ConsumerGroupId`, `ConsumerId`, `SubscriptionId`, `DeliveryId`, `IdempotencyKey`, `PartitionKey`, `MessageEnvelope`, `Topic`, `Partition`, `ConsumerGroup`, `Consumer`, `Subscription`, `DeliveryAttempt`, `Delivery`, `Ack`, `Nack`, `RetryPolicy`, `DeadLetterReason`, and typed domain errors. Milestone 2 reuses those types from `msg-broker` and does not add broker-specific domain duplicates.

Important identifiers use strong newtypes instead of raw strings or integers. Constructors trim string input and enforce length/character invariants where required. Fields remain private where direct mutation could create invalid state, and serde deserialization uses the same validations for the core value types.

## 6. Message Envelope

Messages use a CloudEvents-inspired envelope with stable metadata: ID, source, type, optional subject, time, content type, headers, optional partition key, optional idempotency key, and opaque payload bytes. Milestone 1 implements the in-memory domain representation and builder-style construction. Milestone 6 exposes the core data-plane subset over protobuf fields named `topic`, `message_id`, `key`, `payload`, `content_type`, `type`, `source`, `subject`, `idempotency_key`, and `time_unix_ms`. Full CloudEvents compatibility remains future work.

## 7. Topics, Partitions, Offsets

Topics are logical streams. Partitions are ordered append sequences inside a topic. Offsets identify records within a partition. Milestone 2 stores each broker topic partition as an append-only in-memory vector. Milestone 3 adds a separate `msg-storage` partition log that persists message records to local segment files with the same zero-based monotonic and gapless successful-append offset model. Milestone 4 wires `DurableBroker` publish and consume to `msg-storage` partition logs under `<root>/messages`. Messages with a partition key use deterministic FNV-1a 64-bit hashing over key bytes modulo partition count. Messages without a key use a deterministic per-topic round-robin counter. Ordering is guaranteed only within the same topic partition, not globally.

## 8. Consumer Groups and Cursors

Consumer groups coordinate consumption and track cursors. Milestone 2 maintains independent in-memory group state per topic partition: contiguous ACK cursors, pending deliveries, scheduled retries, ACKed offsets, and DLQ offsets. Cursors advance only after contiguous ACKed offsets. A delivered but unacked message is not redelivered until a NACK schedules it and `retry_ready(now)` makes it available, or until lease expiry is processed by `retry_ready(now)`. Milestone 4 persists `DurableBroker` delivery transitions in a local append-only JSONL state log so successful ACKs are not redelivered after reopen, NACK/retry/DLQ state survives reopen, and crash-recovered unACKed pending deliveries become immediately eligible for at-least-once redelivery with the next attempt number. Consumers must be idempotent.

## 9. Publish Flow

Milestone 2 publish flow validates the topic exists, selects a partition deterministically, appends the envelope to the in-memory partition log, and returns topic, partition, offset, and message ID metadata. Milestone 4 keeps `BrokerService` as the in-memory implementation and adds `DurableBroker`, whose publish path appends the envelope to `msg-storage::PartitionLog` before returning. A successfully published durable message is recoverable after broker reopen. A failed durable append returns an error and must not expose a phantom message.

## 10. Consume Flow

Milestone 2 consume flow scans partitions in stable partition-id order and offsets in ascending order within each partition. A consumed message becomes pending for that consumer group with a deterministic delivery ID, attempt number, delivered-at timestamp, and lease expiry timestamp. Pending, retry-scheduled, ACKed, and DLQ messages are not returned by normal consume. Milestone 4 persists durable consume batches before exposing deliveries to callers. Milestone 6 makes consume leases explicit per gRPC request through `lease_ms`; existing broker callers without an explicit lease continue to use broker configuration.

## 11. ACK/NACK Flow

ACK confirms successful processing and can advance a group cursor only through contiguous ACKed offsets. NACK records failure and schedules retry using the configured backoff, or routes to DLQ when the next delivery would exceed max attempts. `DurableBroker` appends and flushes ACK/NACK outcome events before mutating pending, retry, cursor, or DLQ state. Wrong-consumer ACK/NACK attempts fail explicitly. Unknown, stale, already ACKed, retry-scheduled, NACKed, or DLQ delivery IDs fail as not found.

## 12. Retry Policy

Retries use bounded attempts and optional backoff. Milestone 2 has no background scheduler; time is injected through command timestamps and `retry_ready(now)`. That maintenance call moves ready retries back to available and expires pending leases through the same retry or DLQ decision.

## 13. DLQ Policy

A message exceeding max delivery attempts moves to the DLQ for that consumer group. DLQ entries include topic, partition, offset, message ID, original envelope, consumer group, reason, attempt count, and timestamp. `BrokerService` keeps DLQ entries in memory. `DurableBroker` persists DLQ transitions in its broker-state log and reconstructs DLQ entries from durable message records on reopen.

## 14. Idempotency and Deduplication

At-least-once delivery allows duplicates. Milestone 1 models message IDs and idempotency keys as validated values. Milestone 6 carries `idempotency_key` through the gRPC data plane as metadata only; it does not enforce producer deduplication. Deduplication windows and producer/consumer behavior remain future work.

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

`brokerd serve --data-dir ./.ferrumq --listen 127.0.0.1:8080` opens the local durable broker and serves the control router. The Milestone 5 API is control-plane only: it exposes health, readiness, broker status, topic creation/listing/lookup, and DLQ inspection. It does not expose data-plane publish, consume, ACK, or NACK behavior over HTTP. Duplicate topic creation returns `409 TOPIC_ALREADY_EXISTS`; valid but unknown topic lookups return `404 TOPIC_NOT_FOUND`; malformed JSON and wrong request shapes return `400 INVALID_REQUEST`; domain validation failures return `400 VALIDATION_ERROR`; unavailable broker state returns `503 BROKER_UNAVAILABLE`; internal broker/storage failures return sanitized `500 INTERNAL_ERROR`. Unsupported routes and methods also use the JSON envelope.

## 19. Data Plane

The data plane handles publish, consume, ACK, and NACK. Milestone 6 implements the first data-plane adapter in `msg-data-plane` using tonic/prost and protobuf contracts generated from `crates/msg-protocol/proto/ferrumq/dataplane/v1/dataplane.proto`.

`brokerd serve-grpc --data-dir ./.ferrumq --listen 127.0.0.1:9090` opens a local `DurableBroker` and serves `ferrumq.dataplane.v1.FerrumQDataPlane`. The service is unary-only:

- `Publish(PublishRequest) -> PublishResponse`.
- `Consume(ConsumeRequest) -> ConsumeResponse`.
- `Ack(AckRequest) -> AckResponse`.
- `Nack(NackRequest) -> NackResponse`.

`msg-data-plane` owns explicit protobuf-to-domain mapping, keeps `DurableBroker` behind `Arc<Mutex<_>>`, calls public broker APIs only, and returns sanitized gRPC statuses. Delivery is local durable at-least-once; consumers must be idempotent. Validation and malformed request values map to `INVALID_ARGUMENT`; unknown topics and unknown, duplicate, or stale deliveries map to `NOT_FOUND`; wrong delivery ownership maps to `FAILED_PRECONDITION`; duplicate topics map to `ALREADY_EXISTS` if encountered; poisoned broker state maps to `UNAVAILABLE`; storage, corruption, serialization, and unexpected broker failures map to `INTERNAL`.

## 20. TypeScript CLI

Milestone 7 implements `ferrumq` as the first usable TypeScript CLI and keeps
`msg` as a compatibility alias. The CLI owns command parsing, config
resolution, boundary validation, output formatting, and expected-error
formatting. It does not own broker behavior.

Global `--control-url`, `--grpc-url`, and `--json` flags are supported.
`FERRUMQ_CONTROL_URL` and `FERRUMQ_GRPC_URL` are also supported. Precedence is
flag, then environment, then default. Defaults are
`http://127.0.0.1:8080` for HTTP and `http://127.0.0.1:9090` for gRPC.
URLs are validated client-side as API origins; control URLs reject credentials,
paths, queries, and fragments, and gRPC URLs additionally require an explicit
port and reject HTTPS/TLS until that scope is implemented.

Control-plane CLI commands call HTTP: health, ready, status, topic create/get/list,
and DLQ list. Data-plane CLI commands call unary gRPC: publish, consume, ACK,
and NACK. `broker version` shells out to `brokerd --version`; start/supervise
commands remain deferred. Help and version commands are local and do not call
HTTP or gRPC clients.

Human-readable output is the default. `--json` uses stable wrappers and renders
gRPC `uint64` response fields as decimal strings. Human consume output includes
delivery ID, message ID, topic, partition, offset, attempt number, and payload.
Expected errors are short human text on stderr even when `--json` is set. The
shared `@ferrumq/protocol` HTTP control-plane client owns DTO validation and
distinguishes network failures, FerrumQ error envelopes, malformed non-2xx
responses, invalid JSON, and schema mismatches.

## 21. TypeScript TUI

Milestone 8 implements `ferrumq-tui` as the first usable TypeScript terminal UI.
The TUI is an adapter over the HTTP control plane and does not own broker
behavior. It displays the configured gRPC URL only as state; it does not call
the gRPC data plane.

The TUI supports local `--help` and `--version`, plus `--control-url` and
`--grpc-url`. `FERRUMQ_CONTROL_URL` and `FERRUMQ_GRPC_URL` are supported with
the same precedence as the CLI: flag, then environment, then default. Defaults
are `http://127.0.0.1:8080` for HTTP and `http://127.0.0.1:9090` for gRPC.

On startup and manual refresh, the TUI concurrently fetches `/health`,
`/ready`, `/v1/status`, `/v1/topics`, and `/v1/dlq`. It keeps active view, last
successful snapshot, loading/error, refresh count, and last refresh timestamp in
local UI state. Views are dashboard, topics, DLQ, and help. Key bindings are
`r` refresh, `q` quit, `1` dashboard, `2` topics, `3` DLQ, and `?` help.

Refresh failures are expected user-facing states, not stack traces. If a
partial refresh fails, the TUI renders one short error message and keeps any
last successful snapshot visible.

## 22. Observability

Milestone 9 implements a focused local observability foundation.

`brokerd serve` and `brokerd serve-grpc` initialize `tracing` from `RUST_LOG`.
`FERRUMQ_LOG_FORMAT=json` selects JSON logs; unset or `compact` uses compact
text. Other values fail startup for commands that initialize tracing; commands
such as `brokerd --version` do not initialize tracing. Startup logs include
operation and listen address only.

HTTP control handlers, gRPC data-plane handlers, durable broker operations, and
partition log storage paths emit stable spans/events with safe metadata:
operation, topic, partition, offset, message ID, delivery ID, consumer group,
consumer ID, status, and sanitized error kind. Message payloads, full
filesystem paths, backtraces, secrets, and debug dumps must not be logged.

`msg-observability` owns stable metric names, an internal process-local counter
registry, helper recording functions, and Prometheus text rendering. The HTTP
control plane exposes `GET /metrics` with content type
`text/plain; version=0.0.4; charset=utf-8`. Metrics use only `method`, `route`,
`status`, `code`, and `kind` labels. Topic names, payloads, message IDs,
delivery IDs, consumer IDs, and filesystem paths are not metric labels.
Each successful `/metrics` scrape is counted once as
`ferrumq_control_http_requests_total{method="GET",route="/metrics",status="200"}`
before rendering.

Metrics are process-local. If HTTP and gRPC run as separate processes,
`GET /metrics` on the HTTP process reports only the HTTP process. Data-plane
metrics aggregation, dashboards, OpenTelemetry export, hosted telemetry, and
advanced TUI observability panels remain deferred.

## 23. Security Assumptions

Early milestones assume local development. Authentication, authorization, multi-tenant isolation, and secret management are future work and must not be implied by Milestone 2 code.

## 24. Testing Strategy Summary

The harness starts with compile checks, unit tests, TypeScript tests, linting, formatting, and CI. Milestone 1 adds focused Rust unit tests and property tests for core domain invariants. Milestone 2 adds `msg-broker` integration-style Rust tests for topic creation, publish, consume, ACK, NACK, retry, lease expiry, DLQ, offset uniqueness, and no-redelivery invariants. Milestone 3 adds `msg-storage` filesystem integration tests for append/read behavior, segment rolling, reopen recovery, truncation repair, checksum repair for the final trailing frame, and corruption errors. Milestone 4 adds `DurableBroker` reopen, duplicate/stale operation, retry/DLQ, partition/offset, corruption, and persistence-boundary tests for the local durable contract. Milestone 5 adds Tower/Axum router integration tests for health, readiness, topic admin, deterministic listing, persistence, status, DLQ inspection, malformed JSON, and stable error envelopes. Milestone 6 adds protocol generation/exposure tests, in-process tonic service tests for publish/consume/ACK/NACK and durability, and runtime smoke tests for `serve-grpc`. Milestone 7 adds Vitest coverage for CLI parsing, config precedence, validation, JSON output, HTTP success/error handling, network failures, mocked gRPC data-plane commands, gRPC status formatting, and built CLI smoke tests. Milestone 8 adds Vitest coverage for the shared HTTP control client, TUI config precedence, loader success/failures, Ink rendering, keyboard interactions, and built TUI help/version smoke tests. Milestone 9 adds observability crate tests for Prometheus rendering, escaping, label policy, and payload absence; control API metrics endpoint tests; data-plane counter tests; and focused broker/storage metric coverage. Later milestones add E2E tests, broader property tests, concurrency tests, crash/recovery tests, fuzzing, and benchmarks.

## 25. Milestone Roadmap

The roadmap is defined in [MILESTONES.md](MILESTONES.md), from project skeleton through hardening review.

## 26. Invariants

- A message appended through durable storage must be recoverable according to the configured durability policy.
- A delivered but unacked message may be delivered again.
- At-least-once delivery allows duplicates.
- Consumers are expected to be idempotent.
- Offsets and cursors must only advance according to ACK or commit semantics.
- A message exceeding max delivery attempts must move to DLQ.
- Partition key determines partition selection when provided.
- Ordering is guaranteed only within the same topic partition, not globally.
- Control plane changes must not corrupt data plane message flow.
- Broker internals must be observable through structured logs and process-local metrics without exporting payloads or secrets.
- Domain constructors must reject invalid core names, identifiers, partition counts, retry attempts, and retry backoff values before adapters or runtime code can depend on them.
