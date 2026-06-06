# Architecture

FerrumQ starts as a modular monolith with hexagonal architecture. The codebase is one repository and one Rust workspace for the broker core, with a separate pnpm workspace for human-facing TypeScript tooling.

## Modular Monolith

The initial system is intentionally not split into microservices. Broker behavior, storage semantics, delivery guarantees, and recovery rules are complex enough without introducing distributed deployment and service-to-service coordination. A modular monolith lets the project prove domain boundaries first.

The monolith is modular through crate boundaries:

- `msg-core`: pure domain types and invariants. Milestone 1 implements validated core newtypes, message envelopes, topics, partitions, consumer groups, subscriptions, delivery attempts, ACK/NACK commands, retry policy values, dead-letter reason values, typed domain errors, and serde support here.
- `msg-protocol`: shared protocol DTOs and serialization boundaries.
- `msg-storage`: storage ports and future implementations.
- `msg-broker`: broker orchestration and delivery flow. Milestone 2 implements this as synchronous deterministic in-memory state with topic creation, publish, consume, ACK, NACK, retry maintenance, lease expiry, and in-memory DLQ.
- `msg-runtime`: daemon entrypoints, configuration, and runtime wiring.
- `msg-control-api`: future Axum control plane adapter.
- `msg-observability`: tracing, metrics, and telemetry helpers.
- `msg-test-harness`: deterministic test and failure-simulation helpers.

## Hexagonal Architecture

The core domain does not depend on HTTP, gRPC, filesystem layout, terminal rendering, or process management. Those concerns are adapters around domain ports. This keeps publish, consume, ACK/NACK, retry, DLQ, offset, and storage invariants testable without a running daemon.

Milestone 2 keeps `msg-core` pure and places broker mutation state in `msg-broker`. The broker has no async runtime, shared mutex state, persistence, runtime workers, HTTP/gRPC adapters, or TypeScript-owned broker behavior. Retry and lease processing are explicit service calls driven by injected timestamps.

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

Milestone 2 provides shared Rust domain values and an in-memory broker service that later control-plane and data-plane adapters can call. The adapters themselves remain deferred.

## Future Distributed Evolution

FerrumQ can evolve into distributed components once local semantics are proven. Candidate future splits include runtime nodes, replicated storage, remote data plane APIs, and operator-facing control services. Those splits must come after stable ports, tests, and failure models exist.
