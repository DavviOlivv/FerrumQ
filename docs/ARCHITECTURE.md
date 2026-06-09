# Architecture

FerrumQ starts as a modular monolith with hexagonal architecture. The codebase is one repository and one Rust workspace for the broker core, with a separate pnpm workspace for human-facing TypeScript tooling.

## Modular Monolith

The initial system is intentionally not split into microservices. Broker behavior, storage semantics, delivery guarantees, and recovery rules are complex enough without introducing distributed deployment and service-to-service coordination. A modular monolith lets the project prove domain boundaries first.

The monolith is modular through crate boundaries:

- `msg-core`: pure domain types and invariants. Milestone 1 implements validated core newtypes, message envelopes, topics, partitions, consumer groups, subscriptions, delivery attempts, ACK/NACK commands, retry policy values, dead-letter reason values, typed domain errors, and serde support here.
- `msg-protocol`: shared protocol DTOs and serialization boundaries.
- `msg-storage`: local durable storage adapter/foundation. Milestone 3 implements a synchronous segment-backed append-only log per topic partition with framed JSON message records, CRC32 checksums, zero-based gapless offset assignment for successful appends, segment rolling, reopen recovery, and final-segment trailing-record repair.
- `msg-broker`: broker orchestration and delivery flow. Milestone 2 implements `BrokerService` as synchronous deterministic in-memory state with topic creation, publish, consume, ACK, NACK, retry maintenance, lease expiry, and in-memory DLQ. Milestone 4 adds `DurableBroker`, a synchronous local durable broker that uses `msg-storage` for message records and a JSONL broker-state log for topic metadata and delivery transitions.
- `msg-runtime`: daemon entrypoints, configuration, and runtime wiring. Milestone 5 wires `brokerd serve` to the local control-plane HTTP router.
- `msg-control-api`: Axum control plane adapter. Milestone 5 implements health, readiness, status, topic admin, topic inspection, and DLQ inspection endpoints backed by `DurableBroker`.
- `msg-observability`: tracing, metrics, and telemetry helpers.
- `msg-test-harness`: deterministic test and failure-simulation helpers.

## Hexagonal Architecture

The core domain does not depend on HTTP, gRPC, filesystem layout, terminal rendering, or process management. Those concerns are adapters around domain ports. This keeps publish, consume, ACK/NACK, retry, DLQ, offset, and storage invariants testable without a running daemon.

Milestone 2 keeps `msg-core` pure and places broker mutation state in `msg-broker`. The broker has no async runtime, shared mutex state, persistence, runtime workers, HTTP/gRPC adapters, or TypeScript-owned broker behavior. Retry and lease processing are explicit service calls driven by injected timestamps.

Milestone 3 keeps durable storage independent from broker orchestration. `msg-storage` depends on `msg-core` domain types and stores validated `TopicName`, `PartitionId`, `Offset`, and `MessageEnvelope` values in local segment files with fixed 20-digit segment names and final-segment-only repair. It persists message records only; durable ACK/NACK state, retry state, consumer cursors, pending delivery state, DLQ persistence, broker/storage wiring, indexes, retention, compaction, fsync policy tuning, APIs, and TypeScript behavior are deferred. `msg-broker` behavior remains unchanged until a later milestone wires broker publish, consume, ACK/NACK, retry, cursor, and DLQ state to durable adapters.

Milestone 4 keeps `BrokerService` unchanged and adds `DurableBroker` as a separate public API. Durable message records live under `<root>/messages` through `msg-storage::PartitionLog`; durable topic and delivery state lives under `<root>/broker-state/events.jsonl` as append-only compact JSONL events. Successfully published messages are recoverable after reopen, successfully ACKed messages are not redelivered after reopen, unACKed in-flight deliveries may be redelivered after reopen, and duplicate or stale delivery IDs fail as not found. The broker-state format is specified in [BROKER_STATE_FORMAT.md](BROKER_STATE_FORMAT.md). This is local filesystem durability only, not replicated cluster durability. There is still no HTTP/gRPC API, CLI/TUI broker behavior, clustering, replication, consensus, or exactly-once delivery.

Milestone 5 adds an HTTP adapter without changing broker semantics. `msg-control-api` owns Axum routing, DTOs, deterministic JSON response shapes, unsupported route/method fallbacks, and public error envelopes; `msg-runtime` owns process entrypoints and the TCP listener. The adapter opens a local `DurableBroker` from a configured data directory and stores it behind shared application state for synchronous control-plane calls. It exposes only health, readiness, broker status, topic creation/listing/lookup, and DLQ inspection. HTTP publish, consume, ACK, and NACK are not implemented in Milestone 5.

Planned dependency direction:

```txt
adapters/runtime/api/storage
  depend on
broker orchestration
  depends on
core domain and ports
```

TypeScript packages must not become an alternate broker implementation. They present state, validate user input at boundaries, and call Rust-owned APIs or processes.

## Control Plane and Data Plane

The control plane manages topics, partitions, consumer groups, DLQ inspection, health, readiness, and configuration visibility. The data plane handles publish, consume, ACK, and NACK. Separating the two avoids mixing admin operations with latency-sensitive message flow.

Milestone 5 implements the first control-plane adapter with Axum and local durable backing state. It uses explicit JSON DTOs and a stable error envelope, including `409 Conflict` for duplicate topic creation, `404` for valid unknown topics, `503` when broker state is unavailable, and JSON-envelope responses for unknown routes or unsupported methods. Data-plane adapters remain deferred.

## Future Distributed Evolution

FerrumQ can evolve into distributed components once local semantics are proven. Candidate future splits include runtime nodes, replicated storage, remote data plane APIs, and operator-facing control services. Those splits must come after stable ports, tests, and failure models exist.
