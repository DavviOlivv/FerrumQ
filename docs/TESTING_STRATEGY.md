# Testing Strategy

FerrumQ uses Harness Engineering from the first commit. Milestone 0 established the validation harness. Milestone 1 keeps those commands green and adds focused Rust coverage for the pure `msg-core` domain layer.

## Unit Tests

Every Rust crate and TypeScript package should keep focused unit tests for local behavior. Milestone 1 includes `msg-core` unit tests for validated names and identifiers, topic partition configuration, message envelope construction, consumer groups, consumers, subscriptions, delivery attempts, ACK/NACK commands, retry policy validation, dead-letter reasons, delivery states, and serde round trips. Other crates and TypeScript packages still keep their Milestone 0 smoke coverage.

## Integration Tests

Future integration tests will exercise crate boundaries, storage ports, broker orchestration, and runtime wiring without relying on external services. Milestone 1 does not add integration tests because broker orchestration and storage ports remain deferred.

## End-to-End Tests

Future E2E tests will launch the broker runtime and use CLI/SDK flows for create topic, publish, consume, ACK/NACK, retries, and DLQ inspection. Milestone 1 does not add E2E tests because no runtime daemon behavior is implemented.

## Property-Based Tests

Use `proptest` for domain invariants such as offset ordering, partition selection, retry attempt bounds, cursor advancement, and envelope validation. Milestone 1 adds focused property tests for topic-name validation and offset ordering.

## Concurrency Tests

Use `loom` for concurrency-sensitive broker and storage logic once shared state, workers, or async coordination are introduced.

## Fuzzing

Use `cargo-fuzz` for protocol parsing, storage record parsing, recovery, and corrupted input handling.

## Crash and Recovery Tests

Durable storage milestones must include tests for broker restart, partial segment write, corrupted record, cursor restoration, and DLQ recovery.

## Benchmarks

Use `criterion` for publish, append, read, ACK/NACK, retry scheduling, and recovery benchmarks after behavior exists.

## CLI and TUI Tests

Use `vitest` for TypeScript unit tests and `execa` for future process-level CLI tests. TUI behavior should be tested at component and command-boundary levels without reimplementing Rust broker semantics.

## CI Gates

The local and CI gates are:

- `cargo fmt --all --check`.
- `cargo check --workspace`.
- `cargo clippy --workspace --all-targets -- -D warnings`.
- `cargo test --workspace` or `cargo nextest run --workspace` when available.
- `cargo build --workspace`.
- `pnpm install --frozen-lockfile`.
- `pnpm format:check`.
- `pnpm lint`.
- `pnpm typecheck`.
- `pnpm test`.
- `pnpm build`.

`cargo-deny` is recommended. Missing global audit tooling is a non-breaking fallback until the project standardizes required local tool installation.
