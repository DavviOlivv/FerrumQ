# ADR 0012: TypeScript CLI Foundation

## Status

Accepted.

## Context

Milestones 5 and 6 established separate Rust-owned HTTP control-plane and gRPC
data-plane adapters. The TypeScript CLI was still a placeholder, so developers
had no supported command surface for local broker workflows.

The CLI must improve developer ergonomics without moving broker behavior into
TypeScript. Rust remains the source of truth for topic, delivery, storage,
retry, ACK/NACK, DLQ, and recovery semantics.

## Decision

Implement `@ferrumq/cli` as an async TypeScript command runner with a small
hand-rolled parser. The public binary is `ferrumq`; `msg` remains a
compatibility alias. The CLI resolves configuration from flags, environment,
and defaults in that order.

Control-plane commands call the HTTP API:

- `health`, `ready`, `status`.
- `topic create`, `topic get`, `topic list`.
- `dlq list`.

Data-plane commands call unary gRPC through a tiny `@ferrumq/protocol` helper:

- `publish`.
- `consume`.
- `ack`.
- `nack`.

Human-readable output is the default. `--json` returns stable wrapper objects
for command families, and gRPC `uint64` response values are rendered as decimal
strings to avoid unsafe JavaScript integer conversion.

`broker version` remains a thin `brokerd --version` wrapper. Broker process
management commands are not implemented in TypeScript.

## Consequences

TypeScript now owns developer CLI tooling while Rust continues to own broker
behavior. The protocol package gains only the small DTO/schema and gRPC client
helpers needed by the CLI; it is not a public SDK.

The CLI can be tested through parser/config units, mocked HTTP fetch calls,
mocked gRPC clients, and built-entrypoint smoke tests without launching fixed
ports for normal unit coverage.

Deferred scope remains explicit: TUI, public SDK, auth/RBAC, TLS, streaming
consume, rate limiting, observability dashboards/export, clustering,
replication, exactly-once semantics, process supervision, and MaaS/multi-tenancy.
