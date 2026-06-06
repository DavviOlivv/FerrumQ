# ADR 0006: Core Domain Newtypes and Invariants

## Status

Accepted.

## Context

FerrumQ needs a pure Rust domain foundation before broker orchestration, storage, runtime adapters, HTTP/gRPC APIs, or TypeScript tooling can depend on message semantics. Raw strings and integers would make invalid topic names, consumer identifiers, partition counts, retry policies, and delivery metadata easy to construct accidentally.

## Decision

`msg-core` owns the Milestone 1 domain model as validated Rust types with private fields and explicit constructors.

Core names and identifiers are strong newtypes, including message IDs, topic names, partition IDs, offsets, consumer group IDs, consumer IDs, subscription IDs, delivery IDs, idempotency keys, and partition keys. Topic names enforce the stricter topic grammar. Consumer group IDs enforce their own ASCII grammar. Other string identifiers enforce trimming and bounded non-empty length.

`MessageEnvelope` uses builder-style construction and typed metadata for source, event type, optional subject, content type, timestamp, headers, payload, optional partition key, and optional idempotency key.

Topics, partitions, consumer groups, consumers, subscriptions, deliveries, attempts, ACK/NACK commands, retry policies, delivery states, and dead-letter reasons are modeled as pure domain values. Constructors and serde deserialization enforce invariants where invalid state would otherwise be possible.

## Consequences

- Later crates and adapters can depend on validated domain values instead of repeating low-level checks.
- `msg-core` remains independent of runtime, storage, HTTP, gRPC, terminal UI, worker scheduling, and process management concerns.
- Serde support is part of the domain model, but external protocol compatibility remains a separate future concern.
- Tests can exercise domain invariants without starting a broker daemon or using durable storage.

## Non-Decisions

Milestone 1 does not implement publish/consume orchestration, partition selection, cursor advancement, retry scheduling, DLQ persistence, append-only storage, control-plane APIs, data-plane APIs, SDK behavior, or TUI screens.
