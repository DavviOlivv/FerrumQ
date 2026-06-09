# ADR 0010: Control Plane HTTP API

## Status

Accepted.

## Decision

Milestone 5 adds `msg-control-api` as an Axum-based HTTP adapter for local control-plane operations. The adapter is backed by `DurableBroker`, opened from `ControlApiConfig.data_dir`, and served by:

```sh
brokerd serve --data-dir ./.ferrumq --listen 127.0.0.1:8080
```

The API exposes only:

- `GET /health`
- `GET /ready`
- `GET /v1/status`
- `POST /v1/topics`
- `GET /v1/topics`
- `GET /v1/topics/{topicName}`
- `GET /v1/dlq`

`DurableBroker` gains read-only inspection APIs for deterministic topic listing, topic lookup, and local durable status. The HTTP layer uses explicit camelCase DTOs and does not serialize raw domain structs as its external contract.

API-owned errors use a stable envelope:

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "...",
    "details": {},
    "statusCode": 400
  }
}
```

The public error code set includes `INVALID_REQUEST`, `VALIDATION_ERROR`, `TOPIC_NOT_FOUND`, `TOPIC_ALREADY_EXISTS`, `BROKER_UNAVAILABLE`, `INTERNAL_ERROR`, `METHOD_NOT_ALLOWED`, and route-level `NOT_FOUND`.

Duplicate topic creation maps the existing broker contract `TopicAlreadyExists` to `409 Conflict`. It is not treated as idempotent success, including after reopening the API with the same durable data directory.

Internal broker, storage, serialization, or corruption failures are sanitized as `500 INTERNAL_ERROR` without filesystem paths, Rust debug output, backtraces, or type dumps. If shared broker state cannot be accessed by the API, readiness and other broker-backed routes return `503 BROKER_UNAVAILABLE`.

Unsupported routes and unsupported methods are API-owned HTTP errors and use the same JSON envelope.

## Rationale

Axum fits the current Rust modular monolith: it gives a small typed routing layer, direct Tower-based tests, and no separate service framework. Keeping the adapter in `msg-control-api` preserves the hexagonal boundary: broker semantics remain in `msg-broker`, runtime wiring remains in `msg-runtime`, and HTTP-specific DTO/error behavior remains in the adapter.

Using `DurableBroker` as the backing state ensures the control plane reports the same local durable topics and DLQ entries that survive broker reopen. Deterministic topic ordering follows the broker's `BTreeMap` state and keeps API responses stable for tests and operators.

The control/data-plane split remains intentional. Topic admin and inspection belong in the HTTP control plane. Publish, consume, ACK, and NACK have different latency, streaming, and client semantics and remain deferred to the future data-plane milestone.

## Consequences

`brokerd serve` now runs a local HTTP control-plane server with no daemonization, config file, TLS, auth, rate limiting, background workers, observability export, or clustering.

The first HTTP API is intentionally narrow but durable. It can create and inspect topics and DLQ entries, but it cannot publish or consume messages.

The error envelope, duplicate-topic `409`, topic-not-found `404`, readiness `503`, and unsupported-surface envelope behavior are now part of the external API contract and should remain stable unless a future ADR replaces them.

Future work can add consumer group inspection, authentication, TLS, richer readiness checks, process-level E2E tests, and separate data-plane APIs without changing the Milestone 5 separation.
