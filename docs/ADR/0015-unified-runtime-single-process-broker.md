# ADR 0015: Unified Runtime Single-Process Broker

## Status

Accepted.

## Context

FerrumQ has two local runtime commands:

- `brokerd serve` for the HTTP control plane.
- `brokerd serve-grpc` for the gRPC data plane.

Those commands are useful for preserving the control-plane/data-plane boundary,
but they are separate OS processes. Each opens durable state at startup and owns
its own in-memory `DurableBroker`. A shared `--data-dir` persists state across
restarts, but it does not make already-running processes live-reload each
other's mutations. Metrics are also process-local, so HTTP `/metrics` in the
split setup reports only the HTTP process.

This made local demos awkward: creating a topic through HTTP, publishing through
gRPC, and then checking HTTP status, DLQ, or `/metrics` required stop/start
sequencing or careful explanation of split-process limitations.

## Decision

Add `brokerd serve-all` as the recommended local demo and development runtime:

```sh
brokerd serve-all \
  --data-dir ./.ferrumq \
  --http-listen 127.0.0.1:8080 \
  --grpc-listen 127.0.0.1:9090
```

`serve-all` binds the HTTP and gRPC listeners before serving starts. It opens
durable state once through `msg_control_api::open_state`, builds the HTTP router
from that `AppState`, and builds the gRPC service with
`DataPlaneService::from_shared(state.broker())`.

The ownership model remains the existing adapter model:

```txt
Arc<Mutex<DurableBroker>>
```

This is acceptable for the local single-process runtime because `DurableBroker`
is synchronous, local-filesystem backed, and already used behind a mutex by the
HTTP and gRPC adapters. The unified runtime does not introduce an actor system,
unsafe code, a new storage format, protobuf changes, or public HTTP/gRPC error
shape changes.

`brokerd serve` remains HTTP-only. `brokerd serve-grpc` remains gRPC-only.
`brokerd --version` remains a local command that does not initialize tracing.

## Metrics Behavior

FerrumQ metrics remain process-local. Under `serve-all`, the one process
contains both adapters, so HTTP `/metrics` includes counters from HTTP topic
creation and gRPC publish, consume, ACK, and NACK calls.

In split-process mode, each process still owns its own metrics registry. HTTP
`/metrics` does not expose counters from a separate gRPC process.

## Non-Decision

This ADR does not implement cross-process state sharing. It does not add file
watching, distributed locks, database-backed coordination, broker actors,
replication, or shared metrics aggregation.

Cross-process live reload would need an explicit consistency model for topic,
delivery, retry, DLQ, and metric state. That is larger than the local demo
problem and would blur the current local durable broker contract.

## Consequences

Local demos become coherent: HTTP topic creation, gRPC publish/consume/ACK/NACK,
HTTP status/DLQ, and HTTP `/metrics` all observe one live broker.

The runtime remains simple and compatible with existing adapters and tests.
Tests can bind ephemeral listeners and drive real HTTP and gRPC servers without
fixed ports.

The split commands keep their existing limitations. They are useful for testing
individual adapters and preserving command compatibility, but not recommended
for coherent local demos.

Deferred scope includes cross-process live reload, cluster mode, replication,
distributed locking, shared metrics aggregation across processes, auth/TLS/rate
limiting, web dashboards, OpenTelemetry collector integration, hosted or SaaS
telemetry, and exactly-once delivery.
