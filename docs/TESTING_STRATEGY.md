# Testing Strategy

FerrumQ uses Harness Engineering from the first commit. Milestone 0 established the validation harness. Milestone 1 keeps those commands green and adds focused Rust coverage for the pure `msg-core` domain layer. Milestone 2 adds broker behavior coverage for synchronous in-memory delivery semantics. Milestone 3 adds filesystem-backed storage coverage for the local append-only log foundation. Milestone 4 adds durable broker reopen coverage for local at-least-once delivery. Milestone 5 adds control-plane HTTP adapter coverage against Axum routers without fixed ports. Milestone 6 adds protobuf generation exposure tests and in-process tonic coverage for the local gRPC data plane. Milestone 7 adds TypeScript CLI coverage against parser/config seams plus mocked HTTP and gRPC clients. Milestone 8 adds TypeScript TUI coverage against config, loader, rendering, and keyboard seams.

## Unit Tests

Every Rust crate and TypeScript package should keep focused unit tests for local behavior. Milestone 1 includes `msg-core` unit tests for validated names and identifiers, topic partition configuration, message envelope construction, consumer groups, consumers, subscriptions, delivery attempts, ACK/NACK commands, retry policy validation, dead-letter reasons, delivery states, and serde round trips. Milestone 2 adds `msg-broker` tests for create topic, publish partition assignment, consume pending behavior, ACK, NACK, retry backoff, lease expiry, DLQ routing, offset uniqueness, and no-redelivery invariants. Milestone 3 keeps `msg-storage` unit coverage minimal and emphasizes integration tests because correctness depends on filesystem layout and byte-level recovery behavior. Milestone 4 adds `msg-broker` durable integration tests for reopen and failure behavior. Milestone 5 keeps control API behavior in integration tests and retains crate smoke tests. Milestone 6 adds `msg-protocol` compile/exposure tests for generated protobuf types and service symbols. Other crates and TypeScript packages still keep their smoke coverage.

## Integration Tests

Milestone 2 uses Rust integration-style tests against the public `msg-broker` API while keeping storage and runtime adapters deferred. Milestone 3 uses Rust integration tests against the public `msg-storage` API with `tempfile` roots for first append offset, monotonic and gapless offsets, bounded reads, read-past-end behavior, failed append preservation, reopen recovery, topic and partition isolation, segment rolling, fixed-width segment naming, reads across segment boundaries, invalid configuration, and validated topic path safety.

Storage recovery tests directly mutate local segment files to cover truncated final length, checksum header, and payload bytes; extra final trailing bytes; final checksum mismatch; final invalid JSON; final metadata mismatch; middle checksum mismatch; middle invalid JSON; empty segment handling; and out-of-sequence segment bases. Milestone 4 durable broker tests use `tempfile` roots to cover publish/reopen recovery, ACK/reopen no-redelivery, duplicate and stale delivery IDs, in-flight/reopen redelivery with incremented attempts, NACK/reopen backoff, retry attempt preservation, DLQ/reopen recovery and metadata, partition/offset determinism, failed message append without phantom visibility, broker-state corruption handling, message-log corruption propagation, and segment/recovery integration through `msg-storage`.

Milestone 5 control API integration tests call the Axum router through Tower services, not fixed ports. They cover health, readiness, readiness failure through unavailable broker state, topic creation, duplicate topic `409 Conflict`, invalid names, zero partitions, supported topic punctuation, URL-encoded topic lookup, deterministic topic listing, topic lookup, missing topics, persistence across `open_state` with the same temp directory, duplicate behavior after reopen, status counts, durable DLQ item envelopes seeded through Rust APIs, topic-filtered DLQ inspection, malformed JSON, missing fields, wrong field types, missing JSON content type, unsupported routes, unsupported methods, JSON content type, and sanitized internal storage failures.

Milestone 6 data-plane tests call the tonic service directly with `tonic::Request`, not fixed ports. They cover publish success, unknown and invalid topics, monotonic offsets, deterministic same-key partitioning, metadata-only idempotency keys, empty and large opaque payloads, consume metadata mapping, invalid consumer groups and consumers, invalid `max_messages`, invalid `lease_ms`, empty consumes, max-limit ordering, in-flight lease no-redelivery, deterministic lease-expiry redelivery without sleeps, retry maintenance before DLQ, ACK success and stale/unknown/wrong-consumer errors, duplicate ACK/NACK, ACK-after-NACK, NACK-after-ACK, ACK durability after reopen, NACK retry after reopen, DLQ routing, DLQ reason durability, no redelivery after DLQ, sanitized internal status mapping, unavailable poisoned-state mapping, and a full publish/consume/ACK durable reopen flow.

## End-to-End Tests

Future E2E tests will launch the broker runtime and use CLI/SDK flows for create topic, publish, consume, ACK/NACK, retries, and DLQ inspection. Milestone 5 adds runtime smoke coverage for `brokerd --version`, `brokerd serve --help`, and invalid listen-address parsing. Milestone 6 adds `brokerd serve-grpc --help`, invalid gRPC listen-address coverage, and invalid gRPC data-directory failure coverage, but it does not add process-level gRPC E2E tests yet.

## Property-Based Tests

Use `proptest` for domain invariants such as offset ordering, partition selection, retry attempt bounds, cursor advancement, and envelope validation. Milestone 1 adds focused property tests for topic-name validation and offset ordering. Milestone 2 adds loop-style broker tests for unique offsets, ACKed messages never returning, and messages being externally observable as available, pending, ACKed, retry-scheduled, or DLQ.

## Concurrency Tests

Use `loom` for concurrency-sensitive broker and storage logic once shared state, workers, or async coordination are introduced.

## Fuzzing

Use `cargo-fuzz` for protocol parsing, storage record parsing, recovery, and corrupted input handling.

## Crash and Recovery Tests

Durable storage milestones must include tests for restart, partial segment write, corrupted record, cursor restoration, and DLQ recovery. Milestone 3 covers storage-local reopen recovery, partial trailing-frame truncation, final trailing-frame checksum, JSON, and metadata repair, and middle-of-segment checksum and JSON errors. Milestone 4 covers broker restart for successfully published messages, successfully ACKed messages, unACKed in-flight messages, NACK retry state, attempt counts, DLQ entries, failed append visibility, broker-state append failure boundaries, recoverable final incomplete state lines, fatal malformed complete state lines, inconsistent state events, and message-log corruption propagation. Consumers must still be idempotent because at-least-once redelivery is expected.

## Benchmarks

Use `criterion` for publish, append, read, ACK/NACK, retry scheduling, and recovery benchmarks after behavior exists.

## CLI and TUI Tests

Use `vitest` for TypeScript unit tests and lightweight built-entrypoint smoke tests. Milestone 7 CLI tests cover command parsing for all command families, command-specific help, config precedence, URL validation, topic and numeric validation, human and JSON output shapes, mocked `fetch` success and FerrumQ error envelopes, malformed HTTP responses, network failure formatting, mocked gRPC publish/consume/ACK/NACK behavior, gRPC status formatting, dynamic proto-load failures, malformed gRPC responses, and non-zero expected failure returns. Built CLI version/root-help/topic-help/publish-help smoke tests run after build.

Milestone 8 protocol tests cover the shared HTTP control-plane client, including endpoint-specific success mapping, FerrumQ error envelopes, malformed non-2xx bodies, network failures, invalid JSON, and schema mismatches. TUI tests cover config defaults/env/flags, URL validation, stackless CLI errors, loader success and refresh failures, dashboard/topics/DLQ/help rendering, `r`, `R`, `1`, `2`, `3`, `?`, `q`, `Q`, unsupported keyboard interactions, stale refresh protection, and built `ferrumq-tui --version` and `--help` smoke checks.

Process-level TypeScript gRPC integration is deferred because `brokerd serve-grpc --listen 127.0.0.1:0` does not expose the selected port; Rust in-process gRPC tests cover the service semantics. TUI behavior should remain component and command-boundary coverage without reimplementing Rust broker semantics.

## CI Gates

The local and CI gates are:

- `cargo fmt --all --check`.
- `cargo check --workspace`.
- `cargo clippy --workspace --all-targets -- -D warnings`.
- `cargo test --workspace` or `cargo nextest run --workspace` when available.
- `cargo build --workspace`.
- `cargo run -p msg-runtime --bin brokerd -- --version`.
- `cargo run -p msg-runtime --bin brokerd -- serve-grpc --help`.
- `pnpm install --frozen-lockfile`.
- `pnpm format:check`.
- `pnpm lint`.
- `pnpm typecheck`.
- `pnpm test`.
- `pnpm build`.
- `node packages/tui/dist/cli.js --version`.
- `node packages/tui/dist/cli.js --help`.
- `pnpm --filter @ferrumq/cli build`.
- `node packages/cli/dist/cli.js --version`.
- `node packages/cli/dist/cli.js --help`.

`cargo-deny` is recommended. Missing global audit tooling is a non-breaking fallback until the project standardizes required local tool installation.
