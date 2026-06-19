# ADR 0017: Topic-Scoped Durable Publish Idempotency

## Status

Accepted.

## Context

`idempotency_key` has been carried in the `PublishRequest` and `MessageEnvelope`
since Milestone 1, but it was metadata-only — the broker never checked it and
duplicate publishes always created new messages at new offsets. Producers
retrying after ambiguous transport failures could not know whether their
publish succeeded, and there was no way to retry without duplicating messages.

The project explicitly deferred producer-side idempotency in earlier
milestones (see Milestones 2, 4, 6, 12). This ADR records the design
decisions for the first durable publish idempotency implementation.

## Decision

### Scope

Idempotency is scoped by `(topic, idempotency_key)`. The same key may be used
independently on different topics. No consumer-group, process, partition, or
instance scoping.

### First publish wins

The first successful publish for a given `(topic, idempotency_key)` owns the
durable message identity (partition, offset, message ID). Equivalent retries
return the original identity with `deduplicated = true` without appending
another message or advancing partition state.

### Fingerprint

A deterministic SHA-256 fingerprint of the semantic publish intent defines
equivalence. The fingerprint is computed from a canonical length-prefixed
encoding of these fields (in order):

1. `topic` — UTF-8 bytes.
2. `partition_key` — presence tag (`0x00` absent, `0x01` present) plus UTF-8
   bytes if present.
3. `payload` — raw bytes (no text decoding).
4. `content_type` — UTF-8 bytes.
5. `event_type` — UTF-8 bytes.
6. `source` — UTF-8 bytes.
7. `subject` — presence tag plus UTF-8 bytes if present.
8. `headers` — entry count, then each `(name, value)` pair in `BTreeMap` order
   (deterministic).

Each byte string is prefixed with a `u64` little-endian length for unambiguous
delimiting.

Fields excluded from the fingerprint: `message_id` (transport-generated),
`timestamp` / `time_unix_ms` (transport-generated), `idempotency_key`
(already part of the lookup key).

SHA-256 was chosen because it is a widely audited cryptographic hash with
negligible collision probability for this use case. The `sha2` crate
(RustCrypto, MIT OR Apache-2.0) was added as a dependency of `msg-broker` —
the narrowest crate that owns the dedup logic.

### Recovery

The in-memory idempotency index is rebuilt from the durable message log on
every broker open. The message log is the single source of truth — no
separate dedup ledger, no broker-state event for publishes (consistent with
the existing design, where publishes write to the message log only).

Recovery scans all message records in deterministic canonical order: topics in
`BTreeMap` key order, then partitions in `PartitionId` order (ascending), then
offsets in ascending order within each partition. This triple defines the
canonical "earliest" record because no global cross-partition append order
exists.

### Conflict behavior

When an idempotency key is reused with a different fingerprint (different
payload, headers, or other semantic field), the broker rejects the publish with
`BrokerError::IdempotencyKeyConflict`. The gRPC adapter maps this to
`ALREADY_EXISTS` with a sanitized message. The SDK normalizes this to
`FerrumQError.code === "IDEMPOTENCY_KEY_CONFLICT"` while preserving
`grpcStatus === "ALREADY_EXISTS"` as transport metadata. The CLI exposes the
stable code in its error message.

### Historical duplicate handling

Existing data may contain repeated idempotency keys from before this milestone.
On recovery:

- Equivalent historical duplicates (same fingerprint) keep the earliest record
  by canonical order as canonical. Later records remain in the log but are
  shadowed.
- Conflicting historical duplicates (different fingerprints) fail broker open
  with `DurableBrokerError::Corruption`. The error message identifies the
  topic, partitions, and offsets but not the key values or payloads.

No automatic repair or rewriting of existing data is performed.

### Lifetime

Idempotency records live for the lifetime of retained local broker data. No
TTL, no bounded expiration window, no cleanup API, no compaction, no deletion
worker. Disk and memory growth are honest and documented.

### Not exactly-once

This is producer-side publish deduplication, not exactly-once delivery.
Consumer-side processing remains at-least-once. A deduplicated retry returns
the original publish result but does not prevent consumer redelivery of
already-delivered messages.

## Consequences

### Positive

- Producers can safely retry publishes after ambiguous transport failures by
  using a stable idempotency key and equivalent intent.
- The single source of truth (message log) avoids two-file divergence on
  recovery.
- Fingerprint is deterministic, platform-independent, and handles binary
  payloads.
- Backward-compatible: publishes without keys are unchanged; proto field
  `deduplicated = 5` defaults to false for old clients.

### Negative

- Idempotency index grows unboundedly with the number of unique idempotency
  keys. For a local broker this is acceptable; production deployments would
  need a retention policy.
- Recovery scans all message records to rebuild the index, adding startup cost
  proportional to the data set. Round-robin recovery already performs a full
  scan, so the additional cost is limited to building the in-memory map.
- The first-publish-wins model means a producer that changes its intent (e.g.,
  corrected payload) cannot reuse the same key; it must use a new key or omit
  the key.

## Alternatives Considered

### Separate dedup ledger

Writing idempotency records to a separate append-only file (or as
`BrokerStateEvent` variants in `events.jsonl`) would avoid the full scan on
recovery. Rejected because it introduces a two-file protocol that can diverge,
and the full scan already exists for round-robin recovery.

### Non-cryptographic fingerprint (FNV-1a, CRC32)

Already available in the workspace (FNV-1a in `msg-broker`, CRC32 in
`msg-storage`). Rejected because these are checksums, not cryptographic
digitests, and have non-trivial collision probabilities for adversarial or
high-volume inputs.

### Scoping by consumer group

Scoping the key by consumer group in addition to topic would add complexity for
a use case that doesn't exist. Rejected in favor of the simpler topic-only
scope.

## References

- [PROTOCOL.md](../PROTOCOL.md)
- [BROKER_STATE_FORMAT.md](../BROKER_STATE_FORMAT.md)
- [FAILURE_MODEL.md](../FAILURE_MODEL.md)
- [ADR 0004: At-Least-Once Delivery](0004-at-least-once-delivery.md)
