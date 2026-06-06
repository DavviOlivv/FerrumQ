# FerrumQ

FerrumQ is a milestone-driven messaging engine foundation. The core broker, domain, runtime, and storage semantics are owned by Rust. TypeScript owns the developer-facing CLI, TUI, SDK, and protocol package surfaces.

Milestone 0 created the project skeleton, SDD documentation, architecture records, validation harness, and compile-tested placeholders. Milestone 1 adds the pure Rust `msg-core` domain layer: validated identifiers and names, message envelopes, topics and partitions, consumer groups and subscriptions, delivery attempts, ACK/NACK commands, retry policy values, DLQ reason values, typed domain errors, serde support, and focused unit/property tests.

Milestone 1 still does not implement broker runtime behavior, publish/consume orchestration, durable storage, HTTP/gRPC APIs, retry scheduling workers, DLQ persistence, or TUI screens.

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
pnpm install --frozen-lockfile
pnpm typecheck
pnpm test
pnpm build
```

`make audit` runs `cargo deny check` when `cargo-deny` is installed. Missing global audit tooling remains a non-breaking documented follow-up.
