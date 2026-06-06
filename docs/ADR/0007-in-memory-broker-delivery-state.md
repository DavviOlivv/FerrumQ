# ADR 0007: In-Memory Broker Delivery State

## Status

Accepted.

## Decision

Milestone 2 implements `msg-broker` as a synchronous deterministic in-memory broker service. Broker state lives in owned Rust data structures inside `msg-broker`, not in `msg-core`, `msg-storage`, a runtime worker, an async task, or TypeScript packages.

The in-memory message log is an append-only vector per topic partition. Offsets are zero-based and monotonic within each partition. Messages with a partition key use FNV-1a 64-bit over the key bytes modulo partition count. Messages without a partition key use a deterministic per-topic round-robin counter.

Consumer group state is independent per group and partition. A consumed message becomes pending with a deterministic delivery ID derived from group, topic, partition, offset, and attempt number. ACK removes the pending delivery and advances the group cursor only across contiguous ACKed offsets. NACK removes the pending delivery and schedules retry using the configured backoff, or routes to DLQ when the next delivery would exceed max attempts.

No hidden clock is used. Publish and consume commands carry timestamps where needed, and `retry_ready(now)` explicitly processes retry-ready entries and expired leases.

## Rationale

This gives FerrumQ executable broker semantics before durable storage or network adapters exist. Keeping the broker synchronous and in-memory makes publish, consume, ACK, NACK, retry, lease expiry, and DLQ behavior deterministic and directly testable.

The state layout keeps the target append-only storage model visible: the current vectors can later be replaced by segment-backed partition logs without changing the public service semantics.

## Consequences

Milestone 2 is not durable. A process restart loses topics, partition logs, consumer group state, pending deliveries, scheduled retries, and DLQ entries.

There is no background scheduler. Callers must invoke `retry_ready(now)` to make scheduled retries and expired leases available.

Delivery IDs are stable for tests and stale delivery tokens cannot ACK newer attempts because retry attempts use new delivery IDs.
