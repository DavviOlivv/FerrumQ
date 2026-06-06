# FerrumQ

FerrumQ is a milestone-driven messaging engine foundation. The core broker, domain, runtime, and storage semantics are owned by Rust. TypeScript owns the developer-facing CLI, TUI, SDK, and protocol package surfaces.

Milestone 0 creates only the project skeleton, SDD documentation, architecture records, validation harness, and compile-tested placeholders. It does not implement broker runtime behavior, message delivery, persistence, HTTP/gRPC APIs, or TUI screens.

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

`make audit` runs `cargo deny check` when `cargo-deny` is installed. In Milestone 0, missing global audit tooling is a non-breaking documented follow-up.
