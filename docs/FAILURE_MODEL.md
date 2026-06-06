# Failure Model

This document describes target behavior. Milestone 0 only creates placeholders and documentation.

## Producer Retry

Future producers may retry publish requests on transient failures. Retries can create duplicate publish attempts, so publish APIs must support idempotency keys or deduplication metadata.

## Consumer Crash

If a consumer crashes after delivery but before ACK, the message may be delivered again. Offsets and cursors must not advance beyond acknowledged or committed work.

## Broker Crash

After a broker crash, durable messages must be recoverable according to the configured durability policy. In-memory milestone behavior can lose messages, but durable milestones must document and test recovery expectations.

## Storage Write Failure

A failed append must not be reported as a successful publish. Partial writes must be detected during recovery and either repaired, truncated, or quarantined according to the storage policy.

## Partial Segment Write

Future segment records require length, checksum, and offset metadata so recovery can identify incomplete records. Recovery should stop at the last valid record and preserve evidence for inspection.

## Corrupted Record

Corruption handling must protect valid records and avoid silently delivering corrupted data. Future storage must expose structured diagnostics and tests for corruption scenarios.

## Duplicate Publish

At-least-once producer retry means duplicate publish requests are possible. Future idempotency support should detect duplicate IDs or idempotency keys within a documented retention window.

## Duplicate Delivery

Duplicate delivery is allowed by the reliability model. Consumers must be idempotent, and SDKs should make this expectation clear.

## Poison Message

A poison message that repeatedly fails processing must move to DLQ after max delivery attempts. DLQ records should preserve original metadata, failure context, and attempt count.

## Backpressure Conditions

Backpressure should activate when memory, storage, partition depth, or consumer lag exceeds configured thresholds. Future APIs should return explicit errors or readiness signals instead of silently accepting unbounded work.

## Graceful Shutdown

Future graceful shutdown should stop accepting new work, flush accepted writes according to durability policy, allow in-flight delivery handling within a timeout, and expose shutdown progress through structured logs.
