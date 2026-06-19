# Failure Model

This document describes target and current failure behavior for the local
broker foundation.

## Producer Retry

Producers may retry publish requests on transient failures. When a non-empty
`idempotency_key` is provided, an equivalent retry returns the original publish
result without appending or delivering another message. The key is scoped by
`(topic, idempotency_key)`. A deterministic SHA-256 fingerprint of the semantic
publish intent (excluding `message_id`, timestamp, and the key itself) defines
equivalence. See ADR 0017 for the full design.

## Consumer Crash

If a consumer crashes after delivery but before ACK, the message may be delivered again. Offsets and cursors must not advance beyond acknowledged or committed work.

## Broker Crash

After a broker crash, durable messages must be recoverable according to the
configured durability policy. The in-memory broker can lose messages, while the
durable broker documents and tests recovery expectations for local state.

## Storage Write Failure

A failed append must not be reported as a successful publish. Partial writes must be detected during recovery and either repaired, truncated, or quarantined according to the storage policy.

## Partial Segment Write

Segment records include length, checksum, and offset metadata so recovery can
identify incomplete records. Recovery stops at the last valid final-segment
record for repairable trailing damage.

## Corrupted Record

Corruption handling must protect valid records and avoid silently delivering corrupted data. Future storage must expose structured diagnostics and tests for corruption scenarios.

## Duplicate Publish

At-least-once producer retry means duplicate publish requests are possible. When
a non-empty `idempotency_key` is provided, the broker deduplicates equivalent
retries to the original publish result without appending another message.
Conflicting reuse of the same key with a different semantic intent is rejected
with `IDEMPOTENCY_KEY_CONFLICT`. Idempotency records live for the lifetime of
retained local broker data.

## Duplicate Delivery

Duplicate delivery is allowed by the reliability model. Consumers must be idempotent, and SDKs should make this expectation clear.

## Poison Message

A poison message that repeatedly fails processing must move to DLQ after max delivery attempts. DLQ records should preserve original metadata, failure context, and attempt count.

## Backpressure Conditions

Backpressure should activate when memory, storage, partition depth, or consumer lag exceeds configured thresholds. Future APIs should return explicit errors or readiness signals instead of silently accepting unbounded work.

## Graceful Shutdown

Future graceful shutdown should stop accepting new work, flush accepted writes according to durability policy, allow in-flight delivery handling within a timeout, and expose shutdown progress through structured logs.

## Chat Application Failure Behavior

The `@ferrumq/chat` application handles failures at the application layer on top of the public SDK:

### Broker Unavailable at Startup

If the broker cannot be reached during `start()` (health check, readiness, or topic creation fails), the `ChatApp` transitions to `error` state, closes the SDK client, and does not start polling. No background retry loop runs. The user must restart the chat.

### Broker Outage During Operation

When consume times out, is cancelled independently of shutdown, or fails with a
transient gRPC status, exponential backoff is applied and a warning is
displayed. Repeated identical outage warnings are coalesced. A successful
consume resets the backoff and clears the warning. The first retry uses
`pollIntervalMs`; backoff caps at `max(30_000, pollIntervalMs)` ms.

Shutdown during backoff is immediate: pending timer is cleared, AbortController is signaled, and the SDK client is closed.

Publish failures are not retried — an error is displayed and unsent input is preserved in the buffer. The user may retry manually.

### Permanent Errors

Configuration, validation, authorization, invalid-response, and other
non-transient errors stop polling, close the SDK client, and move the
application to an error state. They are not retried.

### Transparent Reconnection

The chat does **not** transparently reconnect after a broker restart. If the broker restarts while clients are open, the consumer-group state may be out of sync. Users must restart the chat.

### Malformed Messages

Messages that fail parsing or validation are ACKed (not NACKed) and a concise warning is emitted. This prevents infinite redelivery loops. Malformed messages never enter the deduplication cache and never appear as React keys.

Payloads are limited to 32 KiB before fatal UTF-8 decoding. Accepted timestamps
must be canonical UTC ISO 8601 and no more than five minutes in the future.
Duplicate IDs are compared with a SHA-256 fingerprint: identical content is
deduplicated, while conflicting content is warned, suppressed, and ACKed.
