# Architecture

FerrumQ starts as a modular monolith with hexagonal architecture. The codebase is one repository and one Rust workspace for the broker core, with a separate pnpm workspace for human-facing TypeScript tooling.

## Modular Monolith

The initial system is intentionally not split into microservices. Broker behavior, storage semantics, delivery guarantees, and recovery rules are complex enough without introducing distributed deployment and service-to-service coordination. A modular monolith lets the project prove domain boundaries first.

The monolith is modular through crate boundaries:

- `msg-core`: pure domain types and invariants. Milestone 1 implements validated core newtypes, message envelopes, topics, partitions, consumer groups, subscriptions, delivery attempts, ACK/NACK commands, retry policy values, dead-letter reason values, typed domain errors, and serde support here.
- `msg-protocol`: shared protocol DTOs and serialization boundaries. Milestone 6 adds protobuf definitions and generated tonic/prost Rust types for `ferrumq.dataplane.v1`.
- `msg-storage`: local durable storage adapter/foundation. Milestone 3 implements a synchronous segment-backed append-only log per topic partition with framed JSON message records, CRC32 checksums, zero-based gapless offset assignment for successful appends, segment rolling, reopen recovery, and final-segment trailing-record repair.
- `msg-broker`: broker orchestration and delivery flow. Milestone 2 implements `BrokerService` as synchronous deterministic in-memory state with topic creation, publish, consume, ACK, NACK, retry maintenance, lease expiry, and in-memory DLQ. Milestone 4 adds `DurableBroker`, a synchronous local durable broker that uses `msg-storage` for message records and a JSONL broker-state log for topic metadata and delivery transitions.
- `msg-runtime`: daemon entrypoints, configuration, and runtime wiring. Milestone 5 wires `brokerd serve` to the local control-plane HTTP router. Milestone 6 wires `brokerd serve-grpc` to the local data-plane gRPC service. Milestone 11 adds `brokerd serve-all`, which serves both adapters in one process with one shared local durable broker.
- `msg-control-api`: Axum control plane adapter. Milestone 5 implements health, readiness, status, topic admin, topic inspection, and DLQ inspection endpoints backed by `DurableBroker`.
- `msg-data-plane`: tonic gRPC data-plane adapter. Milestone 6 implements unary publish, consume, ACK, and NACK RPCs backed by `DurableBroker`.
- `msg-observability`: shared observability helpers. Milestone 9 implements
  tracing initialization, stable metric names, a process-local counter registry,
  and Prometheus text rendering.
- `msg-test-harness`: deterministic test and failure-simulation helpers.

## Hexagonal Architecture

The core domain does not depend on HTTP, gRPC, filesystem layout, terminal rendering, or process management. Those concerns are adapters around domain ports. This keeps publish, consume, ACK/NACK, retry, DLQ, offset, and storage invariants testable without a running daemon.

Milestone 2 keeps `msg-core` pure and places broker mutation state in `msg-broker`. The broker has no async runtime, shared mutex state, persistence, runtime workers, HTTP/gRPC adapters, or TypeScript-owned broker behavior. Retry and lease processing are explicit service calls driven by injected timestamps.

Milestone 3 keeps durable storage independent from broker orchestration. `msg-storage` depends on `msg-core` domain types and stores validated `TopicName`, `PartitionId`, `Offset`, and `MessageEnvelope` values in local segment files with fixed 20-digit segment names and final-segment-only repair. It persists message records only; durable ACK/NACK state, retry state, consumer cursors, pending delivery state, DLQ persistence, broker/storage wiring, indexes, retention, compaction, fsync policy tuning, APIs, and TypeScript behavior are deferred. `msg-broker` behavior remains unchanged until a later milestone wires broker publish, consume, ACK/NACK, retry, cursor, and DLQ state to durable adapters.

Milestone 4 keeps `BrokerService` unchanged and adds `DurableBroker` as a separate public API. Durable message records live under `<root>/messages` through `msg-storage::PartitionLog`; durable topic and delivery state lives under `<root>/broker-state/events.jsonl` as append-only compact JSONL events. Successfully published messages are recoverable after reopen, successfully ACKed messages are not redelivered after reopen, unACKed in-flight deliveries may be redelivered after reopen, and duplicate or stale delivery IDs fail as not found. The broker-state format is specified in [BROKER_STATE_FORMAT.md](BROKER_STATE_FORMAT.md). This is local filesystem durability only, not replicated cluster durability. There is still no HTTP/gRPC API, CLI/TUI broker behavior, clustering, replication, consensus, or exactly-once delivery.

Milestone 5 adds an HTTP adapter without changing broker semantics. `msg-control-api` owns Axum routing, DTOs, deterministic JSON response shapes, unsupported route/method fallbacks, and public error envelopes; `msg-runtime` owns process entrypoints and the TCP listener. The adapter opens a local `DurableBroker` from a configured data directory and stores it behind shared application state for synchronous control-plane calls. It exposes only health, readiness, broker status, topic creation/listing/lookup, and DLQ inspection. HTTP publish, consume, ACK, and NACK are not implemented in Milestone 5.

Milestone 6 adds a gRPC data-plane adapter without moving broker semantics into the protocol layer. `msg-protocol` owns protobuf contracts and generated Rust service/types, while `msg-data-plane` owns protobuf-to-domain mapping, mutex-backed access to the synchronous `DurableBroker`, and sanitized gRPC status mapping. The adapter calls public broker APIs only. Consume remains unary and explicitly carries `max_messages`, `lease_ms`, and `now_unix_ms`; streaming consumption, SDK integration, auth/TLS, clustering/replication, exactly-once semantics, and background retry workers remain deferred.

Milestone 7 adds the first TypeScript CLI foundation without creating a TypeScript broker. `@ferrumq/cli` validates command input, resolves configuration, formats human and JSON output, and calls the Rust-owned adapters: HTTP for control-plane commands and unary gRPC for data-plane commands. `@ferrumq/protocol` provides only the small schemas and dynamic gRPC helper needed by the CLI, not a public SDK. Broker process supervision, streaming consume, auth/TLS, and distributed behavior remain deferred.

Milestone 8 adds the first TypeScript TUI foundation under the same boundary. `@ferrumq/tui` renders an Ink dashboard from the HTTP control plane through the shared `@ferrumq/protocol` control-plane client. It is read-only, keeps the gRPC URL as configured display state only, and does not publish, consume, ACK, NACK, supervise processes, or call the data plane.

Milestone 13 adds the first user-facing application (`@ferrumq/chat`), a
multi-terminal chat built entirely on the public SDK. Each participant uses an
independent consumer group to emulate broadcast delivery, and the Ink/React
terminal UI exercises the full HTTP/gRPC stack. See [CHAT.md](CHAT.md).

Milestone 12 adds the TypeScript SDK (`@ferrumq/sdk`) as a reusable typed client
that wraps the HTTP control plane and gRPC data plane into a single coherent API.
It reuses `@ferrumq/protocol` for transport and protocol-level contracts while
adding copied payload encoding, stable error normalization into
`FerrumQError`, HTTP aborts, gRPC deadlines, and deterministic lifecycle
management. The SDK is a Node.js-only, unary client layer and does not own
broker semantics or automatic retries.

Milestone 9 adds observability without moving broker semantics into tooling.
`msg-observability` is a shared Rust helper crate for `tracing` setup, stable
metric names, process-local counters, and Prometheus text rendering. The HTTP
control plane exposes `GET /metrics` as an operational endpoint; broker,
storage, HTTP, and gRPC adapters record counters when they run in the same
process. Metrics use only low-cardinality labels and do not export payloads,
topic labels, delivery labels, full filesystem paths, or secrets.

Milestone 11 adds a unified local runtime without changing broker, HTTP,
gRPC, storage, or protobuf contracts. `brokerd serve-all` opens durable state
once, builds the HTTP router from that `AppState`, and builds the gRPC service
from the same `Arc<Mutex<DurableBroker>>`. This is the recommended local demo
and development runtime because HTTP topic creation, gRPC
publish/consume/ACK/NACK, HTTP status/DLQ, and HTTP `/metrics` observe one live
process-local state. `brokerd serve` and `brokerd serve-grpc` remain
split-process modes with startup-loaded state and separate process-local
metrics.

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

Milestone 5 implements the first control-plane adapter with Axum and local durable backing state. It uses explicit JSON DTOs and a stable error envelope, including `409 Conflict` for duplicate topic creation, `404` for valid unknown topics, `503` when broker state is unavailable, and JSON-envelope responses for unknown routes or unsupported methods.

Milestone 6 implements the first data-plane adapter with tonic/prost and the local durable broker. The gRPC service exposes unary `Publish`, `Consume`, `Ack`, and `Nack` calls. Delivery remains local durable at-least-once and `idempotency_key` is metadata-only, so consumers must be idempotent and producers do not get deduplication guarantees yet. It maps validation failures to `INVALID_ARGUMENT`, unknown topics and stale deliveries to `NOT_FOUND`, invalid delivery ownership to `FAILED_PRECONDITION`, duplicate topics to `ALREADY_EXISTS` if surfaced through broker APIs, unavailable broker state to `UNAVAILABLE`, and storage/corruption/unexpected failures to sanitized `INTERNAL` statuses.

Milestone 7 exposes both planes through `ferrumq`: health, readiness, status, topic, and DLQ commands use HTTP; publish, consume, ACK, and NACK commands use unary gRPC. Help and version commands are local. JSON CLI output wraps each command family in stable top-level keys and represents gRPC `uint64` values as decimal strings; expected errors remain short human text on stderr.

Milestone 8 exposes a read-only `ferrumq-tui` view of the control plane. It fetches health, readiness, status, topic list, and DLQ list concurrently on startup and manual refresh. Partial refresh failures become short user-facing error text while the last successful snapshot remains visible.

Milestone 9 exposes process-local metrics through the HTTP control plane at
`GET /metrics`. This keeps metrics operational and read-only while preserving
the data-plane API for publish, consume, ACK, and NACK. Milestone 11 makes those
metrics locally coherent for demos when both adapters run under `serve-all`.
If HTTP and gRPC run as separate processes, the HTTP metrics endpoint reports
only the HTTP process; data-plane metrics aggregation is deferred.

`brokerd serve-all` is now the recommended local runtime shape. `brokerd serve`
and `brokerd serve-grpc` are still valid separate local processes with separate
in-memory broker instances. Each opens `DurableBroker` state from `--data-dir`
at startup. Sharing a data directory persists state across process restarts,
but it does not provide live reload, cross-process synchronization, or live
HTTP/TUI inspection of mutations made by an already-running gRPC process.
`serve-all` solves live state and metrics coherence only inside one process;
cross-process reload/sync remains deferred.

## Future Distributed Evolution

FerrumQ can evolve into distributed components once local semantics are proven. Candidate future splits include runtime nodes, replicated storage, remote data plane APIs, and operator-facing control services. Those splits must come after stable ports, tests, and failure models exist.
