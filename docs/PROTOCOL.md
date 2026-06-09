# Protocol

This document describes protocol contracts. Milestone 6 adds the first protobuf/gRPC data-plane contract while the TypeScript protocol package remains a placeholder surface.

## Message Envelope

FerrumQ uses a CloudEvents-inspired envelope. The project does not implement full CloudEvents compatibility in Milestone 0, but future message metadata should align with standard event concepts:

```json
{
  "id": "msg_01J00000000000000000000000",
  "source": "orders-service",
  "type": "order.created",
  "subject": "order/123",
  "time": "2026-06-06T10:00:00Z",
  "datacontenttype": "application/json",
  "partitionKey": "customer-456",
  "data": {}
}
```

## JSON Boundary

Initial HTTP/control plane APIs should use JSON DTOs with explicit versioning. TypeScript and Rust protocol packages should validate external input and avoid implicit broker semantics.

## gRPC Data Plane

Milestone 6 defines `ferrumq.dataplane.v1.FerrumQDataPlane` in `crates/msg-protocol/proto/ferrumq/dataplane/v1/dataplane.proto` and exposes generated Rust types from `msg-protocol`.

The service is unary-only:

- `Publish(PublishRequest) -> PublishResponse`.
- `Consume(ConsumeRequest) -> ConsumeResponse`.
- `Ack(AckRequest) -> AckResponse`.
- `Nack(NackRequest) -> NackResponse`.

`PublishRequest` carries `topic`, `message_id`, `key`, `payload`, `content_type`, `type`, `source`, `subject`, `idempotency_key`, and `time_unix_ms`. Empty `key`, `subject`, and `idempotency_key` mean absent optional metadata. Empty payloads are valid opaque payload bytes.

`ConsumeRequest` carries `topic`, `consumer_group`, `consumer_id`, `max_messages`, `lease_ms`, and `now_unix_ms`. `max_messages` and `lease_ms` must be greater than zero. Consume responses include delivery ID, topic, partition, offset, envelope metadata, consumer ownership, attempt number, delivery timestamp, and lease deadline.

`AckRequest` carries `delivery_id` and `consumer_id`. `NackRequest` carries `delivery_id`, `consumer_id`, and optional `reason`.

## Versioning Strategy

Protocol versions should be explicit in API paths, protobuf packages, or schema metadata. Breaking changes require a new version. Compatible additions should prefer optional fields with documented defaults. The Milestone 6 protobuf package version is `ferrumq.dataplane.v1`.

## Compatibility Rules

- Unknown fields should not change broker behavior unless a version says they do.
- Message IDs must remain stable across retries and redeliveries.
- Ordering guarantees apply only within a topic partition.
- Exactly-once delivery is not part of the initial contract.

## Error Contract Direction

Data-plane gRPC errors use stable tonic status codes and sanitized messages. Validation and malformed request values map to `INVALID_ARGUMENT`; unknown topics and unknown, duplicate, or stale deliveries map to `NOT_FOUND`; wrong consumer ownership maps to `FAILED_PRECONDITION`; duplicate topics map to `ALREADY_EXISTS` if surfaced; unavailable broker state maps to `UNAVAILABLE`; storage, corruption, serialization, and unexpected failures map to `INTERNAL`.
