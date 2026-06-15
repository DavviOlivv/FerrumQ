# FerrumQ

FerrumQ is a local-first messaging broker foundation. Rust owns the broker
domain model, durable storage, HTTP control plane, unary gRPC data plane, and
runtime. TypeScript owns the developer-facing CLI, read-only TUI, protocol
helpers, and placeholder SDK package.

This repository is a portfolio-oriented release candidate for the broker
foundation, not a production broker service. The current goal is to make the
local durable messaging path easy to inspect, run, and validate without
overclaiming distributed-system guarantees.

## Problem

FerrumQ explores the parts of a broker that are easiest to blur in early
systems work:

- Clear separation between broker semantics and adapters.
- Durable append-only local state with deterministic recovery behavior.
- Explicit control-plane and data-plane boundaries.
- At-least-once delivery with visible ACK/NACK and DLQ state.
- Terminal tooling that exercises the broker without reimplementing it.

## Architecture

- `msg-core`: validated domain types and message envelopes.
- `msg-storage`: framed append-only partition logs with CRC32 recovery checks.
- `msg-broker`: in-memory and local durable broker services.
- `msg-control-api`: Axum HTTP control plane for health, readiness, status,
  topics, DLQ inspection, and metrics.
- `msg-protocol` and `msg-data-plane`: protobuf contracts and tonic gRPC
  data-plane adapter for publish, consume, ACK, and NACK.
- `msg-runtime`: `brokerd`, the local runtime binary for HTTP and gRPC serving.
- `packages/cli`: `ferrumq` command-line adapter over HTTP and gRPC.
- `packages/tui`: `ferrumq-tui`, a read-only Ink dashboard over HTTP.

The design is a modular monolith with hexagonal boundaries. Broker behavior
stays in Rust; TypeScript packages are adapters or client-side helpers.

## Current Capabilities

- Local durable topic creation and deterministic topic listing.
- Durable publish through unary gRPC.
- Unary consume with leases and at-least-once delivery.
- ACK, NACK, retry maintenance, and DLQ transitions.
- Reopen recovery for published, ACKed, and in-flight local state.
- HTTP health, readiness, broker status, topic, DLQ, and `/metrics` endpoints.
- Structured `tracing` logs and process-local Prometheus counters.
- TypeScript CLI smoke path for local broker interaction.
- Read-only TUI for health, readiness, topic, DLQ, and status inspection.

## Explicit Non-Goals

FerrumQ currently provides local durable at-least-once delivery only. Consumers
must be idempotent. The project does not provide exactly-once delivery,
producer deduplication, clustering, replication, consensus, auth/RBAC, TLS,
rate limiting, hosted telemetry, dashboards, multi-tenancy, MaaS behavior, or
production daemon hardening.

`idempotency_key` is metadata-only and is not enforced for publish
deduplication. Message payloads are not logged by default and are not exported
as metric labels.

## Quickstart

Install dependencies and build the workspace:

```sh
pnpm install --frozen-lockfile
pnpm build
cargo build --workspace
```

Use a local data directory:

```sh
mkdir -p ./.ferrumq
```

Start the recommended local demo/dev runtime. It serves HTTP and gRPC in one
OS process backed by one shared local `DurableBroker`:

```sh
cargo run -p msg-runtime --bin brokerd -- serve-all \
  --data-dir ./.ferrumq \
  --http-listen 127.0.0.1:8080 \
  --grpc-listen 127.0.0.1:9090
```

In another shell, create and inspect a topic:

```sh
node packages/cli/dist/cli.js topic create orders --partitions 3
node packages/cli/dist/cli.js topic list
node packages/cli/dist/cli.js topic get orders
```

Publish and consume a message through gRPC:

```sh
node packages/cli/dist/cli.js publish orders --data '{"orderId":1}' --key account-1
node packages/cli/dist/cli.js consume orders --group workers --max 1
```

ACK or NACK the returned delivery ID:

```sh
node packages/cli/dist/cli.js ack <delivery-id>
node packages/cli/dist/cli.js nack <delivery-id> --reason poison
```

Inspect process-local metrics from the shared runtime:

```sh
curl http://127.0.0.1:8080/metrics
```

`brokerd serve-all` is recommended for coherent local demos and development:
HTTP topic creation, gRPC publish/consume/ACK/NACK, HTTP status/DLQ, and HTTP
`/metrics` all observe the same live process-local state. `brokerd serve`
remains HTTP-only, and `brokerd serve-grpc` remains gRPC-only. In that
split-process mode, each process opens state at startup, does not live-reload
peer process mutations, and has its own process-local metrics. `serve-all`
solves live state and metrics coherence only inside one process; cross-process
reload, locking, and metrics aggregation remain deferred.

For a fuller walkthrough, see [docs/LOCAL_DEMO.md](docs/LOCAL_DEMO.md).

## Validation

The local release gate is:

```sh
make ci
```

Focused checks are available through Make targets:

```sh
make rust-fmt-check
make rust-check
make rust-clippy
make rust-test
make rust-nextest
make rust-deny
make ts-format-check
make ts-lint
make ts-typecheck
make ts-test
make ts-build
make smoke
make hygiene
```

`cargo deny check` can emit a known duplicate `hashbrown` warning. Treat it as
non-fatal only when the command exits successfully.

## Project Structure

```txt
crates/              Rust broker, storage, protocol, API, runtime, and tests
packages/            TypeScript CLI, TUI, protocol helpers, and SDK placeholder
docs/                Architecture, protocol, operation, release, and API docs
.github/workflows/   CI entrypoint that runs the local harness
```

## Docs Index

- [Architecture](docs/ARCHITECTURE.md)
- [HTTP Control API](docs/API.md)
- [Protocol](docs/PROTOCOL.md)
- [CLI](docs/CLI.md)
- [TUI](docs/TUI.md)
- [Observability](docs/OBSERVABILITY.md)
- [Failure Model](docs/FAILURE_MODEL.md)
- [Storage Format](docs/STORAGE_FORMAT.md)
- [Broker State Format](docs/BROKER_STATE_FORMAT.md)
- [Testing Strategy](docs/TESTING_STRATEGY.md)
- [Local Demo](docs/LOCAL_DEMO.md)
- [Release Checklist](docs/RELEASE_CHECKLIST.md)
- [Milestones](docs/MILESTONES.md)
- [ADRs](docs/ADR/)

## Status

Current local release status is `0.1.0`. Rust workspace packages, root
TypeScript package metadata, `@ferrumq/*` package metadata, CLI version output,
TUI version output, and `brokerd --version` are expected to remain aligned at
`0.1.0` for this release candidate.
