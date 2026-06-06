# Testing Strategy

FerrumQ uses Harness Engineering from the first commit. Milestone 0 established the validation harness. Milestone 1 keeps those commands green and adds focused Rust coverage for the pure `msg-core` domain layer. Milestone 2 adds broker behavior coverage for synchronous in-memory delivery semantics. Milestone 3 adds filesystem-backed storage coverage for the local append-only log foundation.

## Unit Tests

Every Rust crate and TypeScript package should keep focused unit tests for local behavior. Milestone 1 includes `msg-core` unit tests for validated names and identifiers, topic partition configuration, message envelope construction, consumer groups, consumers, subscriptions, delivery attempts, ACK/NACK commands, retry policy validation, dead-letter reasons, delivery states, and serde round trips. Milestone 2 adds `msg-broker` tests for create topic, publish partition assignment, consume pending behavior, ACK, NACK, retry backoff, lease expiry, DLQ routing, offset uniqueness, and no-redelivery invariants. Milestone 3 keeps `msg-storage` unit coverage minimal and emphasizes integration tests because correctness depends on filesystem layout and byte-level recovery behavior. Other crates and TypeScript packages still keep their smoke coverage.

## Integration Tests

Milestone 2 uses Rust integration-style tests against the public `msg-broker` API while keeping storage and runtime adapters deferred. Milestone 3 uses Rust integration tests against the public `msg-storage` API with `tempfile` roots for first append offset, monotonic offsets, bounded reads, future-offset reads, reopen recovery, topic and partition isolation, segment rolling, reads across segment boundaries, invalid configuration, and validated topic path safety.

Storage recovery tests directly mutate local segment files to cover truncated final frames, checksum mismatch in the final trailing frame, and checksum mismatch in the middle of a segment. Milestone 3 persists message records only; durable ACK/NACK state, retry state, consumer cursors, DLQ persistence, broker/storage wiring, indexes, retention, compaction, fsync policy tuning, APIs, and TypeScript behavior are deferred. Future integration tests will exercise those areas without relying on external services.

## End-to-End Tests

Future E2E tests will launch the broker runtime and use CLI/SDK flows for create topic, publish, consume, ACK/NACK, retries, and DLQ inspection. Milestone 2 does not add E2E tests because no runtime daemon behavior or network API is implemented.

## Property-Based Tests

Use `proptest` for domain invariants such as offset ordering, partition selection, retry attempt bounds, cursor advancement, and envelope validation. Milestone 1 adds focused property tests for topic-name validation and offset ordering. Milestone 2 adds loop-style broker tests for unique offsets, ACKed messages never returning, and messages being externally observable as available, pending, ACKed, retry-scheduled, or DLQ.

## Concurrency Tests

Use `loom` for concurrency-sensitive broker and storage logic once shared state, workers, or async coordination are introduced.

## Fuzzing

Use `cargo-fuzz` for protocol parsing, storage record parsing, recovery, and corrupted input handling.

## Crash and Recovery Tests

Durable storage milestones must include tests for restart, partial segment write, corrupted record, cursor restoration, and DLQ recovery. Milestone 3 covers storage-local reopen recovery, partial trailing-frame truncation, final trailing-frame checksum repair, and middle-of-segment checksum errors. Broker restart, cursor restoration, ACK/NACK state, retry state, and DLQ recovery remain future Milestone 4+ responsibilities.

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
