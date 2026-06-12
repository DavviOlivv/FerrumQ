# HTTP Control API

Milestone 5 exposes a local HTTP control plane backed by `DurableBroker`.
Milestone 7 uses this API from the TypeScript `ferrumq` CLI for control-plane
commands only. Milestone 9 adds `GET /metrics` as an operational Prometheus
text endpoint for process-local counters.

Start the server:

```sh
cargo run -p msg-runtime --bin brokerd -- serve --data-dir ./.ferrumq --listen 127.0.0.1:8080
```

This API is control-plane only. It manages and inspects local durable broker state. It does not provide HTTP publish, consume, ACK, or NACK endpoints. The TypeScript CLI uses unary gRPC for those data-plane commands. TypeScript TUI behavior, auth, TLS, rate limiting, observability export, clustering, replication, and exactly-once semantics are intentionally deferred.

All JSON responses, including API-owned error responses, use `application/json`. Endpoints with JSON request bodies require `Content-Type: application/json`. `GET /metrics` is the exception: it returns Prometheus text, not JSON.

## Error Envelope

All API-owned errors use this stable envelope:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "topic_name contains invalid characters; allowed: ASCII letters, digits, '.', '_', '-'",
    "details": {},
    "statusCode": 400
  }
}
```

Current public error codes:

- `INVALID_REQUEST`: malformed JSON, missing JSON content type, missing required fields, or wrong JSON field types.
- `VALIDATION_ERROR`: syntactically valid request values that violate domain validation, such as invalid topic names or zero partitions.
- `TOPIC_ALREADY_EXISTS`: duplicate topic creation.
- `TOPIC_NOT_FOUND`: valid topic name or DLQ topic filter with no matching topic.
- `BROKER_UNAVAILABLE`: local durable broker state cannot be accessed by the API.
- `INTERNAL_ERROR`: sanitized internal broker, storage, recovery, or serialization failure.
- `METHOD_NOT_ALLOWED`: known route called with an unsupported HTTP method.
- `NOT_FOUND`: unknown route.

Internal broker, storage, recovery, serialization, and corruption failures are sanitized as:

```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "internal server error",
    "details": {},
    "statusCode": 500
  }
}
```

Public error messages must not include filesystem paths, Rust type names, backtraces, or debug dumps.

## Health

### `GET /health`

Returns process liveness. It does not inspect broker state.

Response `200 OK`:

```json
{ "status": "ok" }
```

## Metrics

### `GET /metrics`

Returns process-local Prometheus text exposition for counters recorded in this
HTTP control-plane process.

Response `200 OK` content type:

```txt
text/plain; version=0.0.4; charset=utf-8
```

The output includes `# HELP` and `# TYPE` lines for known FerrumQ counters and
sample lines for counters observed in the current process. Metric labels are
limited to `method`, `route`, `status`, `code`, and `kind`. Metrics do not
include topic names, message payloads, message IDs, delivery IDs, consumer IDs,
full filesystem paths, backtraces, or debug dumps.

When HTTP and gRPC are run as separate `brokerd` processes, this endpoint
reports only the HTTP process. Cross-process aggregation, dashboards,
OpenTelemetry export, auth, TLS, and rate limiting for metrics are deferred.

## Readiness

### `GET /ready`

Returns readiness when the local durable broker state is accessible through the API.

Response `200 OK`:

```json
{ "status": "ready" }
```

If broker state cannot be accessed, the API returns `503 Service Unavailable` with code `BROKER_UNAVAILABLE`.

## Status

### `GET /v1/status`

Returns stable local durable broker status. This is operational control-plane data, not data-plane message content.

Response `200 OK`:

```json
{
  "mode": "local-durable",
  "dataDir": "./.ferrumq",
  "topics": 2,
  "dlqEntries": 1
}
```

Status codes:

- `200 OK`: status returned.
- `503 Service Unavailable`: broker state is unavailable.
- `500 Internal Server Error`: sanitized internal failure.

## Topics

Topic names are validated by `msg-core::TopicName`: after trimming, they must be non-empty, at most 255 characters, contain only ASCII letters, digits, `.`, `_`, and `-`, must not start or end with `.`, and must not contain `..`.

Path topic names are percent-decoded by Axum before validation. URL-encoded names work when the decoded value remains one path segment and satisfies the same `TopicName` rules.

### `POST /v1/topics`

Creates topic metadata and opens durable partition logs.

Request:

```json
{
  "name": "orders",
  "partitions": 3
}
```

Response `201 Created`:

```json
{
  "name": "orders",
  "partitions": 3
}
```

Duplicate creation is not idempotent success. It preserves the broker contract `TopicAlreadyExists`, including after reopening the API with the same data directory.

Partition count must be at least `1`. There is currently no API-specific partition maximum beyond the `u32` request type and practical local filesystem/resource limits.

Status codes:

- `201 Created`: topic created.
- `400 Bad Request`: malformed JSON, missing content type, missing fields, wrong field types, invalid topic name, or invalid partition count.
- `409 Conflict`: topic already exists.
- `503 Service Unavailable`: broker state is unavailable.
- `500 Internal Server Error`: sanitized internal failure.

### `GET /v1/topics`

Lists topics in deterministic topic-name order.

Response `200 OK`:

```json
{
  "items": [
    { "name": "orders", "partitions": 3 },
    { "name": "payments", "partitions": 1 }
  ]
}
```

Status codes:

- `200 OK`: topic list returned.
- `503 Service Unavailable`: broker state is unavailable.
- `500 Internal Server Error`: sanitized internal failure.

### `GET /v1/topics/{topicName}`

Returns topic metadata.

Response `200 OK`:

```json
{
  "name": "orders",
  "partitions": 3
}
```

Status codes:

- `200 OK`: topic found.
- `400 Bad Request`: invalid decoded `topicName`.
- `404 Not Found`: valid topic name but no such topic exists.
- `503 Service Unavailable`: broker state is unavailable.
- `500 Internal Server Error`: sanitized internal failure.

## DLQ

### `GET /v1/dlq`

Lists dead-letter entries in stable broker order.

Response `200 OK`:

```json
{
  "items": [
    {
      "topic": "orders",
      "partition": 0,
      "offset": 42,
      "messageId": "message-1",
      "consumerGroupId": "group.1",
      "reason": "poison",
      "attemptCount": 3,
      "timestamp": 1700000000000
    }
  ]
}
```

### `GET /v1/dlq?topic=orders`

Lists dead-letter entries for one topic.

Status codes:

- `200 OK`: DLQ query succeeded.
- `400 Bad Request`: invalid topic filter.
- `404 Not Found`: valid topic filter but no such topic exists.
- `503 Service Unavailable`: broker state is unavailable.
- `500 Internal Server Error`: sanitized internal failure.

Reason strings are stable lower snake case for built-in reasons (`max_attempts_exceeded`, `expired`, `rejected`, `poisoned`) and the manual reason text for manual NACK reasons.

## Unsupported HTTP Surface

Known routes called with unsupported methods return `405 Method Not Allowed` with code `METHOD_NOT_ALLOWED`.

Unknown routes return `404 Not Found` with code `NOT_FOUND`.

These unsupported-surface responses use the same error envelope and `application/json` content type as endpoint-owned errors.
