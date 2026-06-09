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

Duplicate topic creation maps the existing broker contract `TopicAlreadyExists` to `409 Conflict`. It is not treated as idempotent success.

Internal broker, storage, serialization, corruption, or lock failures are sanitized as `500 INTERNAL_ERROR` without Rust debug output.

## Rationale

Axum fits the current Rust modular monolith: it gives a small typed routing layer, direct Tower-based tests, and no separate service framework. Keeping the adapter in `msg-control-api` preserves the hexagonal boundary: broker semantics remain in `msg-broker`, runtime wiring remains in `msg-runtime`, and HTTP-specific DTO/error behavior remains in the adapter.

Using `DurableBroker` as the backing state ensures the control plane reports the same local durable topics and DLQ entries that survive broker reopen. Deterministic topic ordering follows the broker's `BTreeMap` state and keeps API responses stable for tests and operators.

The control/data-plane split remains intentional. Topic admin and inspection belong in the HTTP control plane. Publish, consume, ACK, and NACK have different latency, streaming, and client semantics and remain deferred to the future data-plane milestone.

## Consequences

`brokerd serve` now runs a local HTTP control-plane server with no daemonization, config file, TLS, auth, rate limiting, background workers, or clustering.

The first HTTP API is intentionally narrow but durable. It can create and inspect topics and DLQ entries, but it cannot publish or consume messages.

The error envelope is now part of the external API contract and should remain stable unless a future ADR replaces it.

Future work can add consumer group inspection, authentication, TLS, richer readiness checks, process-level E2E tests, and separate data-plane APIs without changing the Milestone 5 separation.
