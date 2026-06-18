# Protocol

This document describes protocol contracts. FerrumQ currently has a
protobuf/gRPC data-plane contract and a TypeScript protocol package with HTTP
Zod schemas, FerrumQ error-envelope schemas, gRPC URL normalization, dynamic
proto loading, and a unary data-plane client helper.

## Message Envelope

FerrumQ uses a CloudEvents-inspired envelope. The project does not implement
full CloudEvents compatibility, but future message metadata should align with
standard event concepts:

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

## TypeScript Protocol Helper

`@ferrumq/protocol` is not a public SDK. It exports the small contract helpers
needed by `@ferrumq/cli` and `@ferrumq/sdk`:

- Zod schemas for HTTP control-plane success DTOs and FerrumQ error envelopes.
- DTO types for topic, status, DLQ, and data-plane command responses.
- `createGrpcDataPlaneClient` using `@grpc/grpc-js` and
  `@grpc/proto-loader` against the protobuf definition packaged in
  `@ferrumq/protocol`, with a source-tree fallback during development.
- Optional HTTP `AbortSignal` forwarding and grpc-js unary deadlines. Active
  unary calls are cancelled by idempotent client cleanup.
- `normalizeGrpcTarget`, which accepts `http://host:port` only and rejects
  credentials, missing ports, paths, queries, fragments, and HTTPS/TLS because
  auth/TLS are deferred.
- gRPC status formatting helpers for short CLI expected errors.

Dynamic gRPC loading uses decimal strings for `uint64` response values so CLI
JSON output does not lose precision for offsets or timestamps.

TypeScript tests use mocked raw gRPC clients and proto-loading failure seams.
The SDK integration suite also imports the built package entry point and runs
against a real `brokerd serve-all` process on reserved loopback ports.

## gRPC Data Plane

`ferrumq.dataplane.v1.FerrumQDataPlane` is defined in
`crates/msg-protocol/proto/ferrumq/dataplane/v1/dataplane.proto`, and generated
Rust types are exposed from `msg-protocol`.

The service is unary-only:

- `Publish(PublishRequest) -> PublishResponse`.
- `Consume(ConsumeRequest) -> ConsumeResponse`.
- `Ack(AckRequest) -> AckResponse`.
- `Nack(NackRequest) -> NackResponse`.

`PublishRequest` carries `topic`, `message_id`, `key`, `payload`, `content_type`, `type`, `source`, `subject`, `idempotency_key`, and `time_unix_ms`. `topic`, `message_id`, `content_type`, `type`, and `source` are required by validation. Empty `key`, `subject`, and `idempotency_key` mean absent optional metadata. Empty payloads are valid opaque payload bytes. `time_unix_ms` is a Unix timestamp in milliseconds. `idempotency_key` is metadata-only in `ferrumq.dataplane.v1`; the adapter does not deduplicate publishes by key.

`ConsumeRequest` carries `topic`, `consumer_group`, `consumer_id`, `max_messages`, `lease_ms`, and `now_unix_ms`. `topic`, `consumer_group`, and `consumer_id` are required by validation. `max_messages` and `lease_ms` must be greater than zero. `now_unix_ms` is a caller-supplied Unix millisecond timestamp used for deterministic consume, retry, and lease-expiry decisions. Consume responses include delivery ID, topic, partition, offset, envelope metadata, consumer ownership, attempt number, delivery timestamp, and lease deadline.

`AckRequest` carries required `delivery_id` and `consumer_id` strings. `NackRequest` carries required `delivery_id` and `consumer_id` strings plus optional `reason`; empty or whitespace-only reasons use the broker default.

For local coherent demos and development, run the protobuf service through the
unified runtime:

```sh
cargo run -p msg-runtime --bin brokerd -- serve-all \
  --data-dir ./.ferrumq \
  --http-listen 127.0.0.1:8080 \
  --grpc-listen 127.0.0.1:9090
```

`serve-all` shares one process-local `DurableBroker` between HTTP topic
creation/status/DLQ/metrics and gRPC publish/consume/ACK/NACK. `brokerd
serve-grpc` remains a gRPC-only runtime. In split-process mode, `brokerd serve`
and `brokerd serve-grpc` each load durable state at startup, do not live-reload
peer process mutations, and keep separate process-local metrics.

## Observability Boundary

Observability does not change protobuf messages or service methods. The
`msg-data-plane` adapter records local structured spans and process-local
counters for `Publish`, `Consume`, `Ack`, and `Nack`. Metrics use sanitized
gRPC code strings such as `ok`, `invalid_argument`, `not_found`, and
`failed_precondition`; message payloads, topics, message IDs, delivery IDs, and
consumer IDs are not metric labels. With `serve-all`, HTTP `/metrics` exposes
these gRPC counters because both adapters run in one process. With
`serve-grpc` alone, there is no HTTP metrics endpoint in that gRPC-only process.

## Versioning Strategy

Protocol versions should be explicit in API paths, protobuf packages, or schema
metadata. Breaking changes require a new version. Compatible additions should
prefer optional fields with documented defaults. The current protobuf package
version is `ferrumq.dataplane.v1`.

## Compatibility Rules

- Unknown fields should not change broker behavior unless a version says they do.
- Message IDs must remain stable across retries and redeliveries.
- Ordering guarantees apply only within a topic partition.
- Delivery is local durable at-least-once; consumers must be idempotent.
- Exactly-once delivery is not part of the initial contract.

## Error Contract Direction

Data-plane gRPC errors use stable tonic status codes and sanitized messages. Validation and malformed request values map to `INVALID_ARGUMENT`; unknown topics and unknown, duplicate, or stale deliveries map to `NOT_FOUND`; wrong consumer ownership maps to `FAILED_PRECONDITION`; duplicate topics map to `ALREADY_EXISTS` if surfaced; unavailable broker state maps to `UNAVAILABLE`; storage, corruption, serialization, and unexpected failures map to `INTERNAL`.
