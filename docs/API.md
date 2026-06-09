# HTTP Control API

Milestone 5 exposes a local HTTP control plane backed by `DurableBroker`.

Start the server:

```sh
cargo run -p msg-runtime --bin brokerd -- serve --data-dir ./.ferrumq --listen 127.0.0.1:8080
```

This API is control-plane only. It does not provide HTTP publish, consume, ACK, or NACK endpoints.

## Error Envelope

All API-owned errors use this envelope:

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "topic_name contains invalid characters; allowed: ASCII letters, digits, '.', '_', '-'",
    "details": {},
    "statusCode": 400
  }
}
```

Internal storage, recovery, serialization, or lock errors are sanitized as:

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

## Health

### `GET /health`

Returns process health.

Response `200 OK`:

```json
{ "status": "ok" }
```

## Readiness

### `GET /ready`

Returns readiness when the durable broker state is accessible.

Response `200 OK`:

```json
{ "status": "ready" }
```

If broker state cannot be accessed, the API returns `503 Service Unavailable` with code `NOT_READY`.

## Status

### `GET /v1/status`

Returns local durable broker status.

Response `200 OK`:

```json
{
  "mode": "local-durable",
  "dataDir": "./.ferrumq",
  "topics": 2,
  "dlqEntries": 1
}
```

## Topics

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

Status codes:

- `201 Created`: topic created.
- `400 Bad Request`: invalid topic name, invalid partition count, malformed JSON, or wrong JSON shape.
- `409 Conflict`: topic already exists. Duplicate creation is not idempotent success; it preserves the broker contract `TopicAlreadyExists`.
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
- `400 Bad Request`: invalid `topicName`.
- `404 Not Found`: valid topic name but no such topic exists.

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

Reason strings are stable lower snake case for built-in reasons (`max_attempts_exceeded`, `expired`, `rejected`, `poisoned`) and the manual reason text for manual NACK reasons.
