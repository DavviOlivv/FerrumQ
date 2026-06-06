# Observability

This document describes target observability. Milestone 0 does not start a broker process.

## Structured Logs

Rust diagnostics should use `tracing` rather than ad-hoc printing. Log records should include stable fields such as topic, partition, offset, message ID, consumer group, delivery attempt, request ID, and operation.

## Span Strategy

Future spans should wrap publish, append, consume, ACK, NACK, retry scheduling, DLQ routing, storage recovery, and control plane requests. Spans should preserve correlation between external API calls and internal broker work.

## Correlation IDs

Control plane and data plane requests should carry or generate correlation IDs. Message IDs and idempotency keys should be logged as fields, not embedded in unstructured text.

## Future Metrics

The project should expose broker metrics once runtime behavior exists. Required metric directions include:

- Published messages total.
- Delivered messages total.
- Acked messages total.
- Nacked messages total.
- Retry count.
- DLQ count.
- Consumer lag.
- Partition depth.
- Storage append latency.
- Delivery latency.

## Future Traces

OpenTelemetry integration is planned after local broker semantics are stable. Traces should show producer request handling, durable append, delivery, ACK/NACK handling, retries, and DLQ movement.

## Operational Rules

Observability must not corrupt message flow. Control plane inspection must be bounded so it cannot starve the data plane. Sensitive payload data should not be logged by default.
