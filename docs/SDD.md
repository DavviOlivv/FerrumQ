# Software Design Document

FerrumQ is a real broker/event bus foundation inspired by Kafka, RabbitMQ, NATS JetStream, and Pulsar. This document is the central specification. Milestone 0 implements only the repository skeleton, documentation, CI, and compile-tested placeholders.

## 1. Product Vision

FerrumQ should become a local-first, testable, observable messaging engine with durable publish/consume workflows, explicit delivery semantics, and a terminal-first developer experience.

## 2. Scope

The planned product scope includes topics, partitions, publish/consume flows, ACK/NACK, retry with backoff, DLQ routing, idempotency support, consumer cursors, append-only storage, control plane APIs, data plane APIs, and observability.

## 3. Non-Goals

- No exactly-once delivery promise in the initial version.
- No microservice split in early milestones.
- No TypeScript-owned broker semantics.
- No production broker runtime in Milestone 0.
- No durable storage implementation in Milestone 0.

## 4. Architecture Summary

FerrumQ uses a Rust modular monolith with hexagonal architecture. Rust owns domain, broker, storage, runtime, protocol, control API, and observability modules. TypeScript owns CLI, TUI, SDK, and protocol-package surfaces for developer tooling.

## 5. Domain Model

Future domain types include `MessageId`, `TopicName`, `PartitionId`, `Offset`, `ConsumerGroupId`, `SubscriptionId`, `MessageEnvelope`, `DeliveryAttempt`, `Ack`, `Nack`, and domain errors. Important identifiers should use strong newtypes instead of raw strings or integers.

## 6. Message Envelope

Messages use a CloudEvents-inspired envelope with stable metadata: ID, source, type, subject, time, content type, partition key, and data. Full CloudEvents compatibility is future work.

## 7. Topics, Partitions, Offsets

Topics are logical streams. Partitions are ordered append sequences inside a topic. Offsets identify records within a partition. Ordering is guaranteed only within the same topic partition, not globally.

## 8. Consumer Groups and Cursors

Consumer groups coordinate consumption and track cursors. Cursors must only advance according to ACK or commit semantics. A delivered but unacked message remains eligible for redelivery.

## 9. Publish Flow

Future publish flow validates the envelope, selects a partition, appends the message according to durability policy, and returns success only after the configured publish condition is met.

## 10. Consume Flow

Future consume flow selects available records from topic partitions, records delivery attempts, and returns messages with enough metadata for ACK/NACK handling.

## 11. ACK/NACK Flow

ACK confirms successful processing and can advance a cursor. NACK records failure and may schedule retry, apply backoff, or route to DLQ depending on attempts and policy.

## 12. Retry Policy

Retries use bounded attempts and backoff. Retry state should be observable and must not allow a failed message to block unrelated partitions indefinitely.

## 13. DLQ Policy

A message exceeding max delivery attempts must move to DLQ. DLQ entries should keep original message metadata, attempt count, failure context, and timestamps.

## 14. Idempotency and Deduplication

At-least-once delivery allows duplicates. Producers and consumers should use idempotency keys, message IDs, and deduplication windows where configured.

## 15. Backpressure Model

Backpressure applies when memory, storage, partition depth, consumer lag, or retry queues exceed limits. Future APIs should return explicit backpressure errors or readiness state rather than accepting unbounded work.

## 16. Storage Model

The target storage model is an append-only log per topic partition. Early milestones may use in-memory storage. Durable milestones add segment files, checksums, indexes, fsync policy, crash recovery, and corruption handling.

## 17. Crash and Recovery Expectations

A successfully published message must be recoverable according to the configured durability policy. Recovery must rebuild broker state from durable records without advancing unacked cursors incorrectly.

## 18. Control Plane

The control plane manages topics, partition inspection, consumer group inspection, DLQ inspection, health, readiness, and configuration visibility. Axum is the preferred future HTTP framework.

## 19. Data Plane

The data plane handles publish, consume, ACK, and NACK. gRPC with `tonic` and `prost` is planned for later data plane APIs.

## 20. Observability

Broker internals must be observable through structured logs and later metrics/traces. Use `tracing`, `tracing-subscriber`, and future OpenTelemetry integration.

## 21. Security Assumptions

Early milestones assume local development. Authentication, authorization, multi-tenant isolation, and secret management are future work and must not be implied by Milestone 0 code.

## 22. Testing Strategy Summary

The harness starts with compile checks, unit tests, TypeScript tests, linting, formatting, and CI. Later milestones add integration tests, E2E tests, property tests, concurrency tests, crash/recovery tests, fuzzing, and benchmarks.

## 23. Milestone Roadmap

The roadmap is defined in [MILESTONES.md](MILESTONES.md), from project skeleton through hardening review.

## 24. Invariants

- A successfully published message must be recoverable according to the configured durability policy.
- A delivered but unacked message may be delivered again.
- At-least-once delivery allows duplicates.
- Consumers are expected to be idempotent.
- Offsets and cursors must only advance according to ACK or commit semantics.
- A message exceeding max delivery attempts must move to DLQ.
- Partition key determines partition selection when provided.
- Ordering is guaranteed only within the same topic partition, not globally.
- Control plane changes must not corrupt data plane message flow.
- Broker internals must be observable through structured logs and later metrics/traces.
