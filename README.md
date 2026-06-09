# FerrumQ

FerrumQ is a milestone-driven messaging engine foundation. The core broker, domain, runtime, and storage semantics are owned by Rust. TypeScript owns the developer-facing CLI, TUI, SDK, and protocol package surfaces.

Milestone 0 created the project skeleton, SDD documentation, architecture records, validation harness, and compile-tested placeholders. Milestone 1 added the pure Rust `msg-core` domain layer: validated identifiers and names, message envelopes, topics and partitions, consumer groups and subscriptions, delivery attempts, ACK/NACK commands, retry policy values, DLQ reason values, typed domain errors, serde support, and focused unit/property tests.

Milestone 2 adds `msg-broker` as a synchronous deterministic in-memory broker service. It supports topic creation, publish, consume, ACK, NACK, injected-time retry processing, lease expiry, and in-memory DLQ inspection. The broker has no async runtime, background worker, durable storage, HTTP/gRPC API, or TypeScript-owned broker semantics.

Milestone 3 adds `msg-storage` as an independent synchronous local append-only log for durable message records. It uses framed JSON records, CRC32 checksums, fixed 20-digit segment names, zero-based gapless successful-append offsets, reopen recovery, and final-segment trailing-record repair. Broker delivery durability, ACK/NACK state, retry state, consumer cursors, pending delivery state, DLQ persistence, broker/storage wiring, APIs, retention, compaction, and fsync policy tuning remain deferred.

Milestone 4 adds `DurableBroker` in `msg-broker` as a local durable at-least-once delivery foundation while keeping `BrokerService` unchanged as the in-memory broker. Durable messages are stored through `msg-storage` under `<root>/messages`; topic metadata and delivery transitions are stored in `<root>/broker-state/events.jsonl`. Successfully published messages are recoverable after broker reopen, successfully ACKed messages are not redelivered after reopen, unACKed messages may be redelivered after reopen, and duplicate or stale delivery IDs fail as not found. The broker-state format and recovery rules are documented in [docs/BROKER_STATE_FORMAT.md](docs/BROKER_STATE_FORMAT.md). Consumers must be idempotent. This is local filesystem durability only, not clustering, replication, consensus, HTTP/gRPC API behavior, CLI/TUI broker semantics, or exactly-once delivery.

Milestone 5 adds `msg-control-api`, an Axum HTTP control-plane adapter backed by local durable `DurableBroker` state, and wires `brokerd serve`. The API exposes health, readiness, broker status, topic creation/listing/lookup, and DLQ inspection only. It intentionally does not expose HTTP publish, consume, ACK, or NACK data-plane endpoints. Endpoint shapes, deterministic topic ordering, duplicate topic behavior, unsupported route/method behavior, readiness semantics, and stable JSON error envelopes are documented in [docs/API.md](docs/API.md).

Milestone 6 adds a unary gRPC data-plane foundation. Protobuf contracts live in `msg-protocol` under `ferrumq.dataplane.v1`, `msg-data-plane` maps those DTOs to public `DurableBroker` publish, consume, ACK, and NACK APIs, and `brokerd serve-grpc` serves the adapter from local durable state. Streaming consume, generated TypeScript clients, auth, TLS, rate limiting, and distributed broker behavior remain deferred.

Start the local control-plane server:

```sh
cargo run -p msg-runtime --bin brokerd -- serve --data-dir ./.ferrumq --listen 127.0.0.1:8080
```

Start the local data-plane gRPC server:

```sh
cargo run -p msg-runtime --bin brokerd -- serve-grpc --data-dir ./.ferrumq --listen 127.0.0.1:9090
```

## Architecture Direction

- Modular monolith first, with explicit crate and package boundaries.
- Hexagonal architecture with Rust domain logic isolated from future adapters.
- Append-only log as the target persistence model.
- At-least-once delivery as the initial reliability model.
- Separate control plane and data plane.
- Harness Engineering from the first commit.

## Local Validation

Run the full local harness:

```sh
make ci
```

Useful focused checks:

```sh
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo nextest run --workspace
cargo deny check
pnpm install --frozen-lockfile
pnpm typecheck
pnpm test
pnpm build
cargo run -p msg-runtime --bin brokerd -- --version
git diff --check
```

`cargo nextest run --workspace` and `cargo deny check` require local optional tools. `make audit` runs `cargo deny check` when `cargo-deny` is installed. Missing global audit tooling remains a non-breaking documented follow-up.
