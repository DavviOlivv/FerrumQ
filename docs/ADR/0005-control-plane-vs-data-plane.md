# ADR 0005: Control Plane vs Data Plane

## Status

Accepted.

## Decision

FerrumQ separates the control plane from the data plane.

## Rationale

Administrative operations and message flow have different performance, reliability, authorization, and observability characteristics. Separating them keeps APIs and internal modules clearer and helps prevent management activity from corrupting or starving broker data flow.

## Consequences

Future control plane APIs will manage topics, partitions, consumer groups, DLQ inspection, health, readiness, and configuration visibility. Future data plane APIs will publish, consume, ACK, and NACK messages. The two planes can share core domain types but should keep separate adapter boundaries.
