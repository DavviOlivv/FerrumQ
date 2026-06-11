# ADR 0013: TypeScript TUI Foundation

## Status

Accepted.

## Context

Milestone 7 established the TypeScript CLI as an adapter over Rust-owned HTTP
and gRPC APIs. The TUI package was still a placeholder. FerrumQ needs a
terminal dashboard for local broker inspection, but it must not duplicate broker
semantics in TypeScript or widen the CLI command surface.

The existing HTTP control plane already exposes health, readiness, status,
topics, and DLQ inspection. Those endpoints are enough for the first useful TUI
screen set.

## Decision

Implement `@ferrumq/tui` as an Ink/React terminal app with a separate
`ferrumq-tui` binary. TypeScript owns terminal tooling only; Rust remains the
source of truth for publish, consume, ACK, NACK, retry, DLQ, storage, and
recovery behavior.

The first TUI is a read-only adapter over the HTTP control plane. It fetches:

- `GET /health`.
- `GET /ready`.
- `GET /v1/status`.
- `GET /v1/topics`.
- `GET /v1/dlq`.

The configured gRPC URL is displayed as state only. The TUI does not call the
data plane.

Configuration precedence matches the CLI: flag, then environment variable, then
default. Defaults are `http://127.0.0.1:8080` for the HTTP control plane and
`http://127.0.0.1:9090` for the gRPC data plane.

Move the reusable HTTP control-plane request logic into `@ferrumq/protocol` so
CLI and TUI share schema validation and expected error classification. Expected
errors render as short messages without stack traces.

## Consequences

The TUI now has a real dashboard, topic list, DLQ list, help view, manual
refresh, and keyboard navigation without changing broker behavior. It can be
tested with mocked clients and Ink rendering tests rather than process-level
broker E2E tests.

Keeping `ferrumq-tui` separate avoids changing the already-hardened `ferrumq`
CLI command surface.

Deferred scope remains explicit: public TypeScript SDK, auth/RBAC, TLS,
streaming consume, data-plane TUI workflows, publish/consume/ACK/NACK from the
TUI, broker process supervision, observability dashboards/export, clustering,
replication, exactly-once semantics, and MaaS/multi-tenancy.
