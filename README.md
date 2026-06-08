# FerrumQ

FerrumQ is a milestone-driven messaging engine foundation. The core broker, domain, runtime, and storage semantics are owned by Rust. TypeScript owns the developer-facing CLI, TUI, SDK, and protocol package surfaces.

Milestone 0 created the project skeleton, SDD documentation, architecture records, validation harness, and compile-tested placeholders. Milestone 1 added the pure Rust `msg-core` domain layer: validated identifiers and names, message envelopes, topics and partitions, consumer groups and subscriptions, delivery attempts, ACK/NACK commands, retry policy values, DLQ reason values, typed domain errors, serde support, and focused unit/property tests.

Milestone 2 adds `msg-broker` as a synchronous deterministic in-memory broker service. It supports topic creation, publish, consume, ACK, NACK, injected-time retry processing, lease expiry, and in-memory DLQ inspection. The broker has no async runtime, background worker, durable storage, HTTP/gRPC API, or TypeScript-owned broker semantics.

Milestone 3 adds `msg-storage` as an independent synchronous local append-only log for durable message records. It uses framed JSON records, CRC32 checksums, fixed 20-digit segment names, zero-based gapless successful-append offsets, reopen recovery, and final-segment trailing-record repair. Broker delivery durability, ACK/NACK state, retry state, consumer cursors, pending delivery state, DLQ persistence, broker/storage wiring, APIs, retention, compaction, and fsync policy tuning remain deferred.

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
