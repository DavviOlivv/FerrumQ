# ADR 0004: At-Least-Once Delivery

## Status

Accepted.

## Decision

FerrumQ starts with at-least-once delivery.

## Rationale

At-least-once delivery is a practical and honest reliability model for a first broker. It allows redelivery after crashes, storage failures, consumer failures, or ACK loss. It also forces the design to support idempotent consumers, deduplication keys, retry policies, DLQ routing, and explicit cursor advancement.

## Consequences

Duplicates are allowed and expected. Consumers must be idempotent. Exactly-once delivery is not promised in the initial version and remains future research.
