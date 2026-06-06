# Protocol

This document describes target protocol contracts. Milestone 0 only includes a placeholder TypeScript health schema.

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

## Future gRPC Data Plane

The data plane is expected to move toward protobuf/gRPC with `tonic` and `prost` after the broker core is stable. The gRPC surface should provide publish, consume stream, ACK, and NACK operations.

## Versioning Strategy

Protocol versions should be explicit in API paths, protobuf packages, or schema metadata. Breaking changes require a new version. Compatible additions should prefer optional fields with documented defaults.

## Compatibility Rules

- Unknown fields should not change broker behavior unless a version says they do.
- Message IDs must remain stable across retries and redeliveries.
- Ordering guarantees apply only within a topic partition.
- Exactly-once delivery is not part of the initial contract.

## Error Contract Direction

Future APIs should return structured errors with stable codes, human-readable messages, and machine-readable context. Error contracts must distinguish validation errors, backpressure, storage failures, not-found resources, and delivery-state conflicts.
