# ADR 0003: Append-Only Log

## Status

Accepted.

## Decision

FerrumQ targets an append-only log per topic partition as its durable persistence model.

## Rationale

An append-only log supports ordered reads within a partition, replay, recovery, and consumer offset semantics. It matches the needs of event streaming and durable messaging while keeping the storage model inspectable and testable.

## Consequences

Milestone 0 does not implement storage. Early milestones may use in-memory storage while APIs and invariants are developed. Durable storage work must converge on partition logs, segment files, checksums, recovery, and corruption handling.
