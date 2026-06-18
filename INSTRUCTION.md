# INSTRUCTION.md

## Purpose

You are Codex working on a complex messaging engine project. Your first job is to create the project foundation, SDD documentation, architecture documentation, and Harness Engineering validation layer that all future implementation must obey.

This project is not a toy queue. Treat it as the foundation of a real broker/event bus inspired by systems such as Kafka, RabbitMQ, NATS JetStream, and Pulsar, while keeping the first implementation small, local-first, testable, and milestone-driven.

The user has already made the core technical decisions. Do not re-litigate them unless an existing repository already makes one of them impossible.

---

## Non-negotiable decisions

### Language split

- Core messaging engine: **Rust**
- TUI/CLI and developer-facing terminal tooling: **TypeScript**
- Rust owns the broker/domain/runtime/storage semantics.
- TypeScript owns the human-facing terminal experience.
- TypeScript must not become the source of truth for broker behavior.

### Architectural style

Use:

- **Modular monolith first**
- **Hexagonal architecture / ports and adapters**
- **Event-driven core**
- **Append-only log as the target persistence model**
- **Clear separation between control plane and data plane**
- **SDD-first development**
- **Harness Engineering from the first commit**

Do not start with microservices. The project should be designed so it can evolve into distributed components later, but the initial system must be a well-factored modular monolith.

### Delivery semantics

The initial reliable delivery model is:

- **At-least-once delivery**
- ACK/NACK
- Retry with backoff
- Dead Letter Queue
- Idempotency and deduplication support
- Consumer offsets/cursors
- Partition keys
- Backpressure
- Observability from early milestones

Do not promise exactly-once delivery in the first version. If exactly-once appears in docs, it must be explicitly listed as a future/non-goal.

### Message envelope

Use a CloudEvents-inspired envelope. The project does not need to implement the full CloudEvents spec in Milestone 0, but the design must align with the idea of standard event metadata.

Example conceptual envelope:

```json
{
  "id": "msg_01J00000000000000000000000",
  "source": "orders-service",
  "type": "order.created",
  "subject": "order/123",
  "time": "2026-06-06T10:00:00Z",
  "datacontenttype": "application/json",
  "partitionKey": "customer-456",
  "data": {}
}
```

---

## Project shape

Create or adapt the repository toward this structure:

```txt
messaging-engine/
  crates/
    msg-core/
    msg-protocol/
    msg-storage/
    msg-broker/
    msg-runtime/
    msg-control-api/
    msg-observability/
    msg-test-harness/

  packages/
    cli/
    tui/
    sdk/
    protocol/

  docs/
    SDD.md
    ARCHITECTURE.md
    TESTING_STRATEGY.md
    STORAGE_FORMAT.md
    PROTOCOL.md
    FAILURE_MODEL.md
    OBSERVABILITY.md
    MILESTONES.md
    ADR/
      0001-rust-core-typescript-cli.md
      0002-modular-monolith-hexagonal.md
      0003-append-only-log.md
      0004-at-least-once-delivery.md
      0005-control-plane-vs-data-plane.md

  .github/
    workflows/
      ci.yml

  Makefile
  Cargo.toml
  pnpm-workspace.yaml
  package.json
  README.md
```

If an existing repository already has a different name or partial structure, preserve what is reasonable and adapt carefully. Prefer additive, low-risk changes.

---

## Rust workspace requirements

Use a Cargo workspace.

Suggested crates and responsibilities:

### `msg-core`

Pure domain types and invariants.

Should eventually contain:

- `MessageId`
- `TopicName`
- `PartitionId`
- `Offset`
- `ConsumerGroupId`
- `SubscriptionId`
- `MessageEnvelope`
- `DeliveryAttempt`
- `Ack`
- `Nack`
- domain errors

Rules:

- No database dependency.
- No HTTP dependency.
- No Tokio dependency unless truly necessary.
- Prefer pure, deterministic logic.
- Use strong newtypes instead of raw strings/integers for important concepts.
- Encode invariants in types where practical.

### `msg-protocol`

Shared protocol models and schemas.

Should eventually contain:

- API request/response DTOs
- protobuf definitions if/when gRPC is introduced
- serialization boundaries
- compatibility/versioning notes

### `msg-storage`

Storage ports and implementations.

Target design:

- Start with in-memory storage in early milestones.
- Then implement append-only log by topic/partition.
- Later add segment files, checksums, recovery, indexes, and compaction research.
- Storage APIs should expose operations such as:
  - append message
  - read from offset
  - persist cursor/offset
  - recover state
  - move to DLQ

### `msg-broker`

Broker orchestration.

Should eventually contain:

- topic routing
- partition selection
- publish flow
- consume flow
- ACK/NACK handling
- retry scheduling rules
- DLQ routing
- consumer group coordination in local mode

### `msg-runtime`

Runtime process and daemon.

Should eventually contain:

- broker boot
- config loading
- Tokio runtime usage
- graceful shutdown
- worker orchestration
- background retry workers

### `msg-control-api`

Control plane API.

Use **Axum** when HTTP is introduced.

Control plane responsibilities:

- create/list topics
- inspect topic metadata
- inspect partitions
- inspect consumer groups
- inspect DLQ
- health/readiness endpoints
- configuration visibility

### `msg-observability`

Observability helpers.

Use:

- `tracing`
- `tracing-subscriber`
- OpenTelemetry later
- metrics later

### `msg-test-harness`

Test helpers and harness utilities.

Should contain utilities for:

- temporary broker setup
- deterministic test clocks later
- crash/restart tests later
- test storage directories
- fake producers/consumers
- eventual chaos/failure scenarios

---

## TypeScript workspace requirements

Use pnpm workspaces.

Suggested packages:

### `packages/cli`

Main CLI package.

Use:

- `oclif` for larger CLI structure
- `zod` for validation at boundaries
- `execa` to invoke local Rust daemon/process when needed
- `vitest` for tests
- `tsup` for builds

Future commands should look like:

```txt
msg broker start
msg broker status
msg topic create <name>
msg topic list
msg publish <topic>
msg consume <topic>
msg ack <message-id>
msg nack <message-id>
msg dlq list
msg inspect
```

For Milestone 0, it is enough to provide a minimal CLI with version/help and test/build wiring.

### `packages/tui`

Interactive terminal UI.

Use:

- `ink`
- `@inkjs/ui`
- `zod`
- `vitest`

Future TUI responsibilities:

- dashboard of topics
- broker status
- throughput
- consumer lag
- retry counts
- DLQ inspection
- recent logs/events

For Milestone 0, it is enough to scaffold the package and make build/test pass.

### `packages/sdk`

TypeScript client SDK for the broker APIs.

Future responsibilities:

- typed client for control plane
- typed client for data plane
- schema validation
- retries/timeouts at client boundary

For Milestone 0, it can be a placeholder package with strict TS config and a minimal exported function/type.

### `packages/protocol`

TypeScript representation of protocol contracts.

Future responsibilities:

- shared DTO schemas
- generated protobuf TS outputs if used later
- Zod schemas for runtime validation

For Milestone 0, it can define initial placeholder schemas and compile.

---

## Required Rust dependencies

Do not add every dependency immediately if it is not used. However, the architecture and docs must clearly choose the following libraries as preferred defaults.

Core/common:

- `serde`
- `serde_json`
- `thiserror`
- `anyhow`
- `uuid` or `ulid`
- `time`
- `bytes`

Runtime/API:

- `tokio`
- `axum`
- `tower`
- `tower-http`
- `tonic`
- `prost`
- `clap`
- `tracing`
- `tracing-subscriber`
- `opentelemetry`
- `tracing-opentelemetry`

Storage/testing/failure:

- `crc32fast`
- `tempfile`
- `proptest`
- `loom`
- `criterion`
- `cargo-nextest`
- `cargo-deny`
- `cargo-fuzz`

Guidelines:

- Use `thiserror` for library/domain errors.
- Use `anyhow` for binaries and top-level application boundaries.
- Use `tracing` instead of ad-hoc println logging.
- Avoid `unsafe` entirely unless a future ADR justifies it.
- Use `cargo fmt`, `clippy`, and `nextest`.

---

## Required TypeScript dependencies

Do not add every dependency immediately if it is not used. However, the architecture and docs must clearly choose the following libraries as preferred defaults.

- `typescript`
- `pnpm`
- `tsx`
- `tsup`
- `vitest`
- `@biomejs/biome`
- `zod`
- `oclif`
- `ink`
- `@inkjs/ui`
- `execa`
- `changesets`

Guidelines:

- TypeScript must run with `strict: true`.
- Validate runtime inputs with Zod.
- Keep protocol/client types explicit.
- Do not use `any` unless justified locally with a comment.
- CLI/TUI code must not own broker semantics.
- CLI/TUI must call APIs or process boundaries exposed by the Rust core.

---

## Documentation deliverables

Create or update all docs below.

### `docs/SDD.md`

This is the central spec.

It must include:

1. Product vision
2. Scope
3. Explicit non-goals
4. Architecture summary
5. Domain model
6. Message envelope
7. Topics, partitions, offsets
8. Consumer groups and cursors
9. Publish flow
10. Consume flow
11. ACK/NACK flow
12. Retry policy
13. DLQ policy
14. Idempotency/deduplication model
15. Backpressure model
16. Storage model
17. Crash/recovery expectations
18. Control plane
19. Data plane
20. Observability
21. Security assumptions
22. Testing strategy summary
23. Milestone roadmap
24. Invariants

Important invariants to include:

- A successfully published message must be recoverable according to the configured durability policy.
- A delivered but unacked message may be delivered again.
- At-least-once delivery allows duplicates.
- Consumers are expected to be idempotent.
- Offsets/cursors must only advance according to ACK/commit semantics.
- A message exceeding max delivery attempts must move to DLQ.
- Partition key determines partition selection when provided.
- Ordering is guaranteed only within the same topic partition, not globally.
- Control plane changes must not corrupt data plane message flow.
- Broker internals must be observable through structured logs and later metrics/traces.

### `docs/ARCHITECTURE.md`

Must describe:

- modular monolith decision
- hexagonal architecture
- ports and adapters
- crate/package boundaries
- dependency direction
- control plane vs data plane
- why microservices are not used initially
- future distributed evolution path

### `docs/TESTING_STRATEGY.md`

Must describe:

- unit tests
- integration tests
- E2E tests
- property-based tests with `proptest`
- concurrency testing with `loom`
- fuzzing with `cargo-fuzz`
- crash/recovery tests
- benchmark tests with `criterion`
- CLI/TUI tests with `vitest` and `execa`
- CI gates

### `docs/STORAGE_FORMAT.md`

Must describe the target storage design:

- in-memory first
- append-only log later
- topic/partition directory layout
- segment files
- offsets
- checksums
- indexes
- fsync/durability policy
- crash recovery
- corruption handling
- compaction as future work

### `docs/PROTOCOL.md`

Must describe:

- CloudEvents-inspired envelope
- JSON boundary for initial HTTP/control API
- protobuf/gRPC as future data plane
- versioning strategy
- compatibility rules
- error contract direction

### `docs/FAILURE_MODEL.md`

Must describe expected behavior for:

- producer retry
- consumer crash
- broker crash
- storage write failure
- partial segment write
- corrupted record
- duplicate publish
- duplicate delivery
- poison message
- backpressure conditions
- graceful shutdown

### `docs/OBSERVABILITY.md`

Must describe:

- structured logs using `tracing`
- span strategy
- correlation/request IDs
- future metrics
- future traces
- key broker metrics:
  - published messages total
  - delivered messages total
  - acked messages total
  - nacked messages total
  - retry count
  - DLQ count
  - consumer lag
  - partition depth
  - storage append latency
  - delivery latency

### `docs/MILESTONES.md`

Must define the roadmap:

#### Milestone 0: Project Skeleton, SDD, Harness

- Cargo workspace
- pnpm workspace
- docs created
- Makefile
- CI
- minimal Rust binary with `--version`
- minimal TypeScript CLI/TUI packages
- all validation commands pass

#### Milestone 1: Core Domain

- message envelope
- topics
- partitions
- offsets
- consumer groups
- ACK/NACK models
- domain errors
- unit tests

#### Milestone 2: In-Memory Broker

- create topic
- publish
- consume
- ack
- nack
- basic retry
- in-memory DLQ

#### Milestone 3: Append-Only Log

- segmented log
- append
- read from offset
- checksum
- recovery after restart
- corruption tests

#### Milestone 4: Delivery Semantics

- at-least-once behavior
- pending deliveries
- retry with backoff
- max attempts
- persistent DLQ
- idempotency key support

#### Milestone 5: Control Plane API

- Axum HTTP API
- topic admin
- partition inspection
- consumer group inspection
- DLQ inspection
- health/readiness

#### Milestone 6: Data Plane API

- gRPC with tonic/prost
- publish RPC
- consume stream
- ack/nack RPC
- Rust client

#### Milestone 7: TypeScript CLI

- production-grade CLI commands
- validation
- error formatting
- E2E tests against broker

#### Milestone 8: TypeScript TUI

- Ink dashboard
- broker status
- topics
- lag
- DLQ
- logs

#### Milestone 9: Observability

- structured tracing
- metrics endpoint
- Prometheus/Grafana compose
- OpenTelemetry integration

#### Milestone 10: Hardening Review

- crash/recovery tests
- fuzzing
- property tests
- concurrency tests
- dependency audit
- benchmarks
- docs reconciliation

---

## ADR deliverables

Create ADRs with concise but real rationale.

### `docs/ADR/0001-rust-core-typescript-cli.md`

Decision:

- Rust for core engine.
- TypeScript for TUI/CLI.

Rationale:

- Rust gives memory safety, performance, explicit error handling, and strong concurrency tools for the engine.
- TypeScript gives strong developer experience for terminal tooling and package distribution.
- This mirrors the pattern of terminal-first developer tools that combine a systems core with a polished CLI/TUI layer.

### `docs/ADR/0002-modular-monolith-hexagonal.md`

Decision:

- Start as modular monolith.
- Use hexagonal architecture.

Rationale:

- A messaging engine is already complex and distributed at its boundaries.
- Microservices would multiply operational complexity too early.
- Hexagonal architecture keeps domain logic isolated from HTTP, gRPC, filesystem, CLI, and TUI adapters.

### `docs/ADR/0003-append-only-log.md`

Decision:

- Target append-only log per topic/partition.

Rationale:

- Enables replay, ordered reads per partition, recovery, and future consumer offset semantics.
- Matches the needs of event streaming and durable messaging.

### `docs/ADR/0004-at-least-once-delivery.md`

Decision:

- Start with at-least-once delivery.

Rationale:

- Practical and honest reliability model.
- Allows redelivery after crash/failure.
- Requires idempotent consumers and duplicate-aware design.
- Exactly-once is a future research topic, not an initial promise.

### `docs/ADR/0005-control-plane-vs-data-plane.md`

Decision:

- Separate control plane from data plane.

Rationale:

- Admin operations and message flow have different performance and reliability characteristics.
- This keeps APIs and internal modules clearer.
- It prepares the project for future distributed evolution.

---

## Harness Engineering requirements

Create a `Makefile` with these targets where practical:

```txt
make fmt
make lint
make typecheck
make test
make test-rust
make test-ts
make test-integration
make test-e2e
make build
make audit
make ci
```

The exact internals can evolve, but `make ci` must be the main local verification command.

### Rust validation

`make ci` should include, at minimum:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace
```

If `cargo-nextest` is available, prefer:

```sh
cargo nextest run --workspace
```

If not available, keep `cargo test --workspace` as fallback and document the nextest recommendation.

### TypeScript validation

`make ci` should include, at minimum:

```sh
pnpm install --frozen-lockfile
pnpm format:check
pnpm lint
pnpm typecheck
pnpm test
pnpm build
```

Use package scripts to support these commands.

### Audit validation

If feasible in Milestone 0:

```sh
cargo deny check
```

If `cargo-deny` is not installed/configured yet, create a placeholder documented path for adding it in a follow-up milestone. Do not break CI by requiring unavailable global tooling.

### GitHub Actions

Create `.github/workflows/ci.yml`.

It must validate:

- Rust format
- Rust clippy
- Rust tests
- Rust build
- pnpm install
- TypeScript lint
- TypeScript typecheck
- TypeScript tests
- TypeScript build

Use stable Rust and a recent Node.js LTS.

---

## Initial implementation constraints

For this instruction task, implement **Milestone 0 only**.

Do not implement the full broker yet.

Milestone 0 must produce:

- repository skeleton
- SDD docs
- ADRs
- Makefile
- CI
- minimal Rust crates
- minimal TypeScript packages
- validation commands that pass

Minimal Rust implementation can include:

- workspace crates with basic modules
- a simple binary command such as `brokerd --version`
- placeholder domain types only if useful
- smoke tests

Minimal TypeScript implementation can include:

- CLI package with `--version` or `help`
- TUI package placeholder that builds
- SDK/protocol package placeholders
- smoke tests

Do not add fake complex behavior that is not actually implemented.

Docs may describe future milestones, but code should not pretend those milestones exist.

---

## Code quality rules

### Rust

- Prefer explicit types.
- Prefer newtypes for domain identifiers.
- Prefer `Result<T, E>` over panic.
- Use `thiserror` for library errors.
- Use `anyhow` only at application/binary edges.
- Use `tracing` for diagnostics.
- Keep modules small.
- Keep domain logic independent from infrastructure.
- Avoid `unsafe`.

### TypeScript

- Use `strict: true`.
- Avoid `any`.
- Validate external data with Zod.
- Keep CLI/TUI logic separate from protocol/client logic.
- Use clear error messages.
- Prefer small modules.
- Build packages with tsup.
- Test with vitest.

### Documentation

- Do not write vague docs.
- Include explicit invariants, non-goals, and acceptance criteria.
- Whenever a feature is future work, label it clearly as future work.
- Keep docs aligned with actual code state.

---

## Acceptance criteria for this Codex task

The task is complete only when:

1. `docs/SDD.md` exists and documents the messaging engine spec.
2. `docs/ARCHITECTURE.md` exists and documents the chosen architecture.
3. `docs/TESTING_STRATEGY.md` exists and documents the harness/testing approach.
4. `docs/STORAGE_FORMAT.md` exists.
5. `docs/PROTOCOL.md` exists.
6. `docs/FAILURE_MODEL.md` exists.
7. `docs/OBSERVABILITY.md` exists.
8. `docs/MILESTONES.md` exists.
9. All ADR files listed above exist.
10. Rust workspace exists and validates.
11. TypeScript workspace exists and validates.
12. `Makefile` exists.
13. `.github/workflows/ci.yml` exists.
14. `make ci` passes locally or any missing external tool is clearly documented with a non-breaking fallback.
15. README explains project purpose and local validation.

---

## Final response required from Codex

When finished, summarize:

- files created/changed
- architecture choices encoded
- validation commands run
- whether they passed
- any known gaps or follow-up work

Do not claim success for commands that were not run.
Do not hide failing validation.
Do not implement beyond Milestone 0 unless explicitly asked later.
