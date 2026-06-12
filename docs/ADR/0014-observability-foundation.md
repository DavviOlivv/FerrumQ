# ADR 0014: Observability Foundation

## Status

Accepted.

## Context

FerrumQ now has durable broker behavior, an HTTP control plane, a gRPC data
plane, a CLI, and a read-only TUI. Operators need enough local visibility to
debug startup, storage recovery, publish/consume flow, ACK/NACK behavior,
retry/DLQ transitions, and adapter errors.

The project is still local-first. It does not yet have authentication, TLS,
remote telemetry, clustering, replication, or a process topology for hosted
observability infrastructure.

## Decision

Use Rust `tracing` for structured spans and events across runtime startup,
HTTP handlers, gRPC handlers, durable broker operations, and storage recovery.
`brokerd` initializes tracing from `RUST_LOG`; `FERRUMQ_LOG_FORMAT=json` selects
JSON logs and the default is compact text.

Add a small internal metrics registry in `msg-observability`. The registry owns
process-local counters and renders Prometheus text directly. It avoids global
Prometheus recorder lifecycle issues in tests, keeps dependency and license
churn small, and gives the Rust adapters deterministic in-process assertions.

Expose metrics through the HTTP control plane at `GET /metrics` with:

```txt
text/plain; version=0.0.4; charset=utf-8
```

Metrics use low-cardinality labels only: `method`, `route`, `status`, `code`,
and `kind`. Topic names, message IDs, delivery IDs, consumer IDs, payloads,
full filesystem paths, backtraces, and debug dumps are not exported as metric
labels. Logs may include safe operational identifiers such as topic, partition,
offset, message ID, delivery ID, and consumer group, but never message payloads
or full filesystem paths.

Preserve the control-plane/data-plane split. The metrics endpoint is an
operational HTTP endpoint. HTTP control-plane handlers and gRPC data-plane
handlers both record counters when they run in the same process; a separate
data-plane metrics listener is deferred.

## Consequences

FerrumQ gets useful local observability without adding a collector, dashboard,
remote exporter, or hosted telemetry assumptions. Tests can assert metrics
through the same process-local registry used by the adapters.

Metrics are not cross-process aggregated. When `brokerd serve` and
`brokerd serve-grpc` run as separate processes, `/metrics` on the HTTP process
reports only that HTTP process.

Dashboards, OpenTelemetry export, collectors, hosted telemetry, auth/TLS for
metrics, rate limiting, clustering/replication metrics, exactly-once telemetry,
multi-tenant telemetry, and advanced TUI observability panels remain deferred.
