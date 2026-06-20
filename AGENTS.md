# AGENTS.md

This file is the operating guide for coding agents working in FerrumQ. It
applies to the entire repository unless a deeper `AGENTS.md` overrides it.

## Project Snapshot

FerrumQ is a local-first messaging broker foundation and portfolio-oriented
release candidate. It is a modular monolith:

- Rust owns domain invariants, broker semantics, durable storage, recovery,
  HTTP/gRPC adapters, observability, and the runtime.
- TypeScript owns developer-facing clients and terminal applications.
- The current reliability contract is local durable at-least-once delivery.
- Producer idempotency deduplicates equivalent publishes by
  `(topic, idempotency_key)`; it is not exactly-once delivery.
- `brokerd serve-all` is the recommended local development/demo runtime because
  HTTP and gRPC share one in-process `DurableBroker`.

The current release version is `0.1.0`.

`INSTRUCTION.md` is the original repository bootstrap brief. Its architectural
decisions remain relevant, but its “Milestone 0 only” task constraint is
historical. The current implementation and contracts are described by the code,
`README.md`, `docs/`, and the accepted ADRs.

## Non-Negotiable Boundaries

1. Rust is the source of truth for broker behavior.
   TypeScript may validate, transport, format, render, and apply
   application-specific policy, but it must not reimplement broker delivery,
   retry, cursor, DLQ, storage, or recovery semantics.
2. Preserve the modular-monolith and hexagonal boundaries.
   Domain and broker logic must not depend on HTTP, gRPC, terminal UI, or
   process-management concerns.
3. Preserve at-least-once semantics.
   Delivered but unacknowledged messages may be delivered again. Consumers must
   remain idempotent. Never describe publish idempotency as exactly-once.
4. Preserve the control-plane/data-plane split.
   HTTP owns health, readiness, status, topics, DLQ inspection, and metrics.
   gRPC owns publish, consume, ACK, and NACK.
5. Keep local-runtime limitations explicit.
   Separate `brokerd serve` and `brokerd serve-grpc` processes do not live-reload
   each other, share a process-local broker, coordinate concurrent writes, or
   aggregate metrics.
6. Do not expose message payloads, idempotency keys, secrets, full filesystem
   paths, or high-cardinality identifiers in metrics. Logs may contain safe
   operational identifiers, but never payload bytes or secrets.
7. Keep future work honest. Do not claim clustering, replication, consensus,
   auth/RBAC, TLS, rate limiting, multi-tenancy, hosted telemetry, browser SDK
   support, streaming consume, or production daemon hardening.

## Repository Map

### Rust workspace

- `crates/msg-core`: pure domain values and invariants. No filesystem, HTTP,
  gRPC, terminal, or runtime behavior.
- `crates/msg-broker`: synchronous in-memory and durable broker orchestration,
  delivery state, retry/DLQ behavior, partitioning, and publish idempotency.
- `crates/msg-storage`: synchronous append-only partition log, segment framing,
  checksums, reads, rolling, recovery, and corruption handling.
- `crates/msg-protocol`: protobuf definition and generated tonic/prost Rust
  types for `ferrumq.dataplane.v1`.
- `crates/msg-data-plane`: tonic adapter mapping protobuf requests to
  `DurableBroker`.
- `crates/msg-control-api`: Axum HTTP control-plane adapter and stable JSON
  error envelopes.
- `crates/msg-observability`: tracing setup, stable metric names, process-local
  counters, and Prometheus rendering.
- `crates/msg-runtime`: `brokerd` CLI and unified/split runtime wiring.
- `crates/msg-test-harness`: shared test helpers as they are introduced.

Expected dependency direction:

```text
runtime / HTTP / gRPC / storage adapters
                ↓
        broker orchestration
                ↓
        core domain values
```

### TypeScript workspace

- `packages/protocol`: Zod HTTP contracts and dynamic unary gRPC transport.
  This is a protocol helper, not the public SDK.
- `packages/sdk`: public Node.js client combining HTTP and gRPC.
- `packages/cli`: `ferrumq` command-line adapter; `msg` is a compatibility
  alias.
- `packages/tui`: read-only Ink dashboard over the HTTP control plane.
- `packages/chat`: local multi-terminal chat example built only on the SDK.
- `examples`: executable SDK flows.
- `scripts/ensure-brokerd.mjs`: builds/fetches the platform-specific local
  `brokerd` test binary when absent.

### Contracts and decision records

- `docs/SDD.md`: central behavioral specification.
- `docs/ARCHITECTURE.md`: boundaries and dependency direction.
- `docs/PROTOCOL.md`, `docs/API.md`: gRPC and HTTP contracts.
- `docs/STORAGE_FORMAT.md`, `docs/BROKER_STATE_FORMAT.md`: durable formats and
  recovery rules.
- `docs/FAILURE_MODEL.md`: expected behavior under failures.
- `docs/TESTING_STRATEGY.md`: test seams and release gates.
- `docs/OBSERVABILITY.md`: logging and metric privacy/label policy.
- `docs/MILESTONES.md`: implemented scope and deferred work.
- `docs/ADR/`: accepted architectural decisions. Some older filenames are
  compatibility aliases; follow their canonical links.

## Toolchain and Setup

Required local tools:

- Stable Rust with `rustfmt` and `clippy` (`rust-version = 1.85`,
  edition 2024).
- `cargo-nextest`.
- `cargo-deny`.
- Node.js 18+; CI uses current Node LTS.
- pnpm `11.5.2`, as pinned by the root `packageManager`.

Install TypeScript dependencies with:

```sh
pnpm install --frozen-lockfile
```

Use pnpm only. Do not create npm or Yarn lockfiles. The local pnpm store is
`.pnpm-store/`.

## Primary Commands

The full local and CI release gate is:

```sh
make ci
```

It runs Rust formatting/checking/clippy/tests/nextest/dependency policy,
TypeScript frozen install/format/lint/typecheck/tests/build, built-entrypoint
smoke checks, `brokerd --version`, and `git diff --check`.

Focused Make targets:

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

Useful focused commands:

```sh
cargo test -p msg-core
cargo test -p msg-broker --test in_memory
cargo test -p msg-broker --test durable
cargo test -p msg-storage --test partition_log
cargo test -p msg-control-api --test control_api
cargo test -p msg-data-plane --test data_plane
cargo test -p msg-runtime --test brokerd
cargo nextest run -p msg-broker

pnpm --filter @ferrumq/protocol test
pnpm --filter @ferrumq/sdk test
pnpm --filter @ferrumq/cli test
pnpm --filter @ferrumq/tui test
pnpm --filter @ferrumq/chat test
pnpm --filter @ferrumq/sdk exec vitest run tests/sdk.test.ts -t "<name>"
```

`cargo deny check` currently emits a known duplicate `hashbrown` warning. Treat
it as non-fatal only when the command exits successfully.

Some benchmark-style diagnostics and global-metrics tests are deliberately
ignored by the default Rust test run. Do not silently convert them into normal
parallel tests without addressing isolation and runtime cost.

## Coding Rules

### Rust

- Use strong domain newtypes from `msg-core`; do not replace validated
  identifiers with raw `String`/integer values at broker boundaries.
- Keep fields private where direct mutation could violate invariants.
  Constructors and serde deserialization must enforce equivalent validation.
- Prefer `Result<T, E>` and typed `thiserror` errors. Panic/`expect` is
  acceptable only for proven internal invariants or test setup.
- Keep broker and storage logic synchronous and deterministic. Runtime adapters
  may be async around the synchronous broker.
- Pass timestamps explicitly through commands when deterministic delivery,
  retry, or lease behavior depends on time. Avoid hidden clocks in domain and
  broker code.
- Preserve deterministic ordering. `BTreeMap`, ordered partition iteration,
  stable DTO order, and canonical recovery order are intentional.
- Persist/flush the corresponding durable record before mutating in-memory
  delivery state or returning successful mutation results.
- Do not silently repair non-final or middle-of-log corruption. Only the
  documented final trailing damage may be truncated.
- Use `tracing`, not `println!`, for runtime diagnostics.
- Avoid `unsafe`. The scoped `PROTOC` environment mutation in
  `msg-protocol/build.rs` is an existing build-script exception; new unsafe code
  requires strong justification and normally an ADR.
- Keep public API docs accurate when behavior changes.

### TypeScript

- Use strict ESM TypeScript with explicit `.js` suffixes for relative imports.
- Preserve `strict`, `exactOptionalPropertyTypes`, and
  `noUncheckedIndexedAccess`. Do not introduce `any`.
- Validate external inputs and responses at boundaries. Use Zod where the
  package already uses it.
- Use conditional object spreads instead of assigning `undefined` to optional
  properties when exact optional types apply.
- Keep dependency-injection seams (`fetchImpl`, client factories, clocks,
  UUIDs, renderers, runtimes) so tests remain deterministic.
- Expected CLI/TUI failures must be short, non-zero, stackless user-facing
  errors. Do not print internal debug objects.
- Preserve gRPC `uint64` values as decimal strings in TypeScript.
- Copy binary payload data across public SDK boundaries; do not expose mutable
  transport-owned buffers.
- SDK and protocol clients must keep cleanup/cancellation idempotent.
- CLI and TUI must remain adapters. The TUI is read-only and does not call the
  data plane.
- Chat-specific sanitization, payload limits, timestamp validation, bounded
  caches/history, stale-generation guards, and shutdown behavior are security
  and reliability contracts, not incidental UI details.

### Formatting

- Rust: `cargo fmt --all`.
- TypeScript/JSON and other files recognized by Biome:
  `pnpm format` or `pnpm format:check`.
- Biome uses 2 spaces, double quotes, semicolons, trailing commas, LF endings,
  and an 80-column target.
- Markdown is not processed by the current Biome configuration; keep it
  readable, wrap prose consistently, and verify whitespace with
  `git diff --check`.
- Do not hand-edit generated `target/`, `node_modules/`, or `packages/*/dist/`
  output.

## Core Behavioral Invariants

- Successful durable publishes are recoverable according to the current local
  flush-based policy.
- Successful appends receive zero-based, monotonic, gapless offsets per
  partition.
- Ordering is guaranteed only within one topic partition.
- Partition keys use deterministic FNV-1a routing; unkeyed publishes use
  per-topic round robin.
- A failed append must not advance offsets, round-robin state, or reserve an
  idempotency key.
- A delivered but unacknowledged message may be redelivered.
- ACK/NACK ownership is bound to both delivery ID and consumer ID.
- Duplicate, stale, ACK-after-NACK, and NACK-after-ACK delivery IDs fail as not
  found.
- Cursors advance only through contiguous acknowledged offsets.
- Retry and lease-expiry maintenance is explicit; there is no hidden background
  scheduler.
- Messages exceeding max attempts move to DLQ and are not delivered again.
- Durable state events are appended and flushed before their in-memory
  transitions are applied.
- Reopen releases still-pending deliveries for a later attempt while preserving
  attempt counts.
- Idempotency is scoped by `(topic, idempotency_key)`. Equivalent semantic
  intent returns the original partition, offset, and message ID with
  `deduplicated = true`.
- Conflicting idempotency-key reuse returns
  `IDEMPOTENCY_KEY_CONFLICT`. Deduplicated retries do not append records,
  advance partition state, create another visible delivery, or increment the
  actual-publish metric.
- The durable message log is the idempotency source of truth. Recovery rebuilds
  the index in topic/partition/offset order; equivalent historical duplicates
  keep the canonical earliest record and conflicts fail broker open.

## External Contract Rules

### Protobuf/gRPC

- The current package is `ferrumq.dataplane.v1`.
- Never renumber or reuse existing protobuf field numbers.
- Prefer additive compatible fields with documented defaults.
- Empty strings represent absent optional string metadata in the current proto.
- Unknown protobuf fields must remain tolerated.
- Validation failures map to `INVALID_ARGUMENT`; unknown topics/deliveries to
  `NOT_FOUND`; invalid ownership to `FAILED_PRECONDITION`; idempotency conflicts
  to `ALREADY_EXISTS`; unavailable shared state to `UNAVAILABLE`; internal
  storage/corruption details to sanitized `INTERNAL`.
- A proto change normally requires updates to:
  `dataplane.proto`, Rust protocol/data-plane code, wire compatibility tests,
  TypeScript protocol mappings, SDK/CLI behavior, and protocol documentation.
- Rust protobuf output is generated during build and is not committed.
  `@ferrumq/protocol` copies the source proto into its `dist/` during build.

### HTTP

- Keep HTTP control-plane DTOs explicit and camelCase; do not expose raw Rust
  domain structs.
- All API-owned JSON errors use the stable
  `{ error: { code, message, details, statusCode } }` envelope.
- Internal failures must remain sanitized.
- `GET /metrics` is Prometheus text, not JSON.
- Unsupported routes and methods use the same API-owned JSON error contract.

### Storage

- Message segments use
  `<root>/messages/topics/<topic>/partitions/<partition>/<20-digit-base>.log`.
- Frames are `u32_le length`, `u32_le CRC32`, then compact JSON payload with
  `format_version = 1`.
- Broker state is append-only compact JSONL at
  `<root>/broker-state/events.jsonl`.
- Storage or broker-state format changes require explicit compatibility and
  recovery reasoning, focused corruption/reopen tests, documentation updates,
  and usually an ADR. Do not casually rewrite retained data.

### Observability

- Metric names and allowed labels are stable public operational contracts.
- Allowed labels are only `method`, `route`, `status`, `code`, and `kind`.
- Metrics are process-local. Tests that reset/assert the global registry must
  remain serialized or otherwise isolated.
- Never include payloads, topic names, message IDs, delivery IDs, consumer IDs,
  idempotency keys, secrets, or filesystem paths as metric labels.

## Testing Expectations

- Place pure invariant tests close to the Rust module; use integration tests for
  filesystem, adapter, runtime, and cross-crate behavior.
- Use `tempfile` for durable tests. Do not commit broker state.
- Use injected timestamps instead of sleeps for retry and lease behavior.
- Use Tower calls for Axum route tests and direct tonic service calls for most
  gRPC adapter tests.
- Runtime and SDK/chat integration tests must use reserved or pre-bound
  ephemeral loopback ports, temporary data directories, explicit readiness
  checks, and deterministic cleanup.
- Do not introduce fixed ports or arbitrary sleeps into automated tests.
- Loopback tests may skip only explicit environment permission failures;
  startup and correctness failures must still fail.
- Cover both success and failure atomicity for durable mutations.
- Any recovery or persistence change needs reopen tests.
- Any parser, protocol, or external DTO change needs malformed-input and
  compatibility tests.
- Any user-facing CLI/TUI/chat behavior change needs built-entrypoint or Ink
  rendering coverage where applicable.

## Change Impact Matrix

| Change area | Minimum focused validation and review |
| --- | --- |
| `msg-core` domain/invariants | `cargo test -p msg-core`; inspect broker/adapters and TS boundary validation for duplicated assumptions |
| In-memory broker | `cargo test -p msg-broker --test in_memory`; verify deterministic state and idempotency parity |
| Durable broker/state log | durable broker tests plus data-plane tests; add reopen and append-failure atomicity coverage; update broker-state/failure docs |
| `msg-storage` | partition-log tests plus durable broker tests; test final repair and middle/non-final corruption; update storage docs |
| Protobuf/data plane | protocol and data-plane tests, TS protocol/SDK/CLI tests, compatibility fixture, `docs/PROTOCOL.md` |
| HTTP control API | control API tests, protocol client tests, CLI/TUI/SDK tests as affected, `docs/API.md` |
| Observability | observability/control/data/broker/storage metric tests; check label privacy and process-local wording |
| Runtime commands | runtime tests and relevant real HTTP+gRPC integration flow; keep `serve-all`/split-process docs aligned |
| `packages/protocol` | protocol tests, SDK/CLI/TUI dependants, build to verify packaged proto path |
| SDK | SDK unit and integration tests; chat tests for lifecycle/transport effects; update `docs/SDK.md` |
| CLI | CLI parser/output/built-smoke tests and `docs/CLI.md`; keep human and JSON contracts stable |
| TUI | TUI config/loader/Ink/built-smoke tests and `docs/TUI.md`; preserve read-only behavior and stale-refresh protection |
| Chat | domain/app/UI/integration/multi-client tests and `docs/CHAT.md`; preserve terminal sanitization and shutdown bounds |
| Version change | align Cargo workspace, root/package manifests, CLI/TUI/chat outputs, docs, builds, and release checklist |

Run `make ci` for cross-cutting changes, release work, and before declaring a PR
ready. For a narrow edit, run focused checks first, then expand validation in
proportion to risk.

## Documentation and ADR Discipline

- Keep documentation aligned with implemented behavior in the same change.
- Update the relevant public docs whenever commands, DTOs, errors, metrics,
  storage/recovery behavior, defaults, or limitations change.
- Add or amend an ADR when changing a durable format, delivery guarantee,
  process/ownership model, protocol compatibility rule, or major boundary.
- Preserve explicit non-goals and local-first wording.
- Avoid duplicating milestone history as current behavior; use current sections
  for current contracts and `docs/MILESTONES.md` for chronology.
- Follow `docs/RELEASE_CHECKLIST.md` for release-facing work.

## Runtime and Manual Smoke Flow

Build and start the coherent local runtime:

```sh
pnpm build
cargo build --workspace
cargo run -p msg-runtime --bin brokerd -- serve-all \
  --data-dir ./.ferrumq \
  --http-listen 127.0.0.1:8080 \
  --grpc-listen 127.0.0.1:9090
```

Useful built entrypoints:

```sh
node packages/cli/dist/cli.js --help
node packages/cli/dist/cli.js topic create orders --partitions 3
node packages/cli/dist/cli.js publish orders --data '{"id":1}'
node packages/cli/dist/cli.js consume orders --group workers
node packages/tui/dist/cli.js --help
node packages/chat/dist/cli.js --name davi --room general
```

Use disposable data directories for manual and automated work. Do not treat
split HTTP/gRPC processes as a safe concurrent-write topology.

## Git and Generated-File Hygiene

- Keep changes scoped; preserve unrelated user work.
- Do not commit `target/`, `node_modules/`, `packages/*/dist/`, `.pnpm-store/`,
  `.ferrumq/`, coverage output, or local logs.
- Update `Cargo.lock` or `pnpm-lock.yaml` only when dependency changes require
  it. Use frozen installs for validation.
- Do not regenerate broad dependency or formatting churn unrelated to the task.
- Run `git diff --check` before handoff.
- Do not use destructive Git commands unless the user explicitly requests them.

## Definition of Done

A change is complete when:

1. It respects the architecture and behavioral invariants above.
2. Relevant focused tests pass.
3. Cross-cutting changes pass `make ci`.
4. External contracts and compatibility behavior are tested.
5. Documentation and ADRs are updated where required.
6. Public errors/logs/metrics remain sanitized.
7. No generated, dependency, broker-state, or unrelated files are included.
8. The final report states exactly which checks ran, their results, and any
   remaining limitations or intentionally deferred work.
