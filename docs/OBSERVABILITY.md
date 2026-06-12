# Observability

Milestone 9 adds the first concrete observability foundation. It is local and
process-scoped: structured logs use `tracing`, counters live in each process,
and the HTTP control plane exposes Prometheus text at `GET /metrics`.

This is operational data for a local broker process. It is not hosted telemetry,
not a dashboard, not a collector pipeline, and not an authentication boundary.

## Logging

`brokerd serve` and `brokerd serve-grpc` initialize tracing before opening
runtime services.

Filtering uses `RUST_LOG`:

```sh
RUST_LOG=info cargo run -p msg-runtime --bin brokerd -- serve
RUST_LOG=msg_broker=debug,msg_storage=debug cargo run -p msg-runtime --bin brokerd -- serve-grpc
```

Log formatting is selected with `FERRUMQ_LOG_FORMAT`:

- unset or `compact`: compact text logs.
- `json`: JSON logs.

Examples:

```sh
FERRUMQ_LOG_FORMAT=compact RUST_LOG=info brokerd serve
FERRUMQ_LOG_FORMAT=json RUST_LOG=info brokerd serve-grpc
```

Startup logs include only the operation and listen address. Broker and storage
events use safe metadata fields such as operation, topic, partition, offset,
message ID, delivery ID, consumer group, consumer ID, status, and sanitized
error kind. Logs must not include message payloads, full filesystem paths,
backtraces, or debug dumps.

## Metrics Endpoint

The HTTP control plane exposes:

```txt
GET /metrics
```

Response content type:

```txt
text/plain; version=0.0.4; charset=utf-8
```

Example:

```sh
curl http://127.0.0.1:8080/metrics
```

The endpoint returns Prometheus text exposition for the current process only.
It includes `# HELP` and `# TYPE` lines for known counters and sample lines for
counters observed in that process.

## Metric Labels

Metrics intentionally use low-cardinality labels only:

- `method`: HTTP method or gRPC method name.
- `route`: stable HTTP route template, such as `/v1/topics`.
- `status`: HTTP status code or operation outcome such as `success`.
- `code`: stable HTTP error code or sanitized gRPC code.
- `kind`: sanitized storage, broker, repair, or DLQ kind.

Topic names, message payloads, delivery IDs, message IDs, consumer IDs, and
filesystem paths are not metric labels.

## Metric Names

Control-plane counters:

- `ferrumq_control_http_requests_total`: HTTP requests by `method`, `route`, and
  numeric `status`.
- `ferrumq_control_http_errors_total`: HTTP error responses by `method`,
  `route`, numeric `status`, and stable FerrumQ error `code`.
- `ferrumq_control_topics_created_total`: topic creation attempts by
  `status=success|error`.

Data-plane counters:

- `ferrumq_data_rpc_requests_total`: gRPC requests by `method` and sanitized
  `status`.
- `ferrumq_data_rpc_errors_total`: gRPC error responses by `method` and
  sanitized `code`.
- `ferrumq_data_publishes_total`: publish attempts by `status`.
- `ferrumq_data_consumes_total`: consume attempts by `status`.
- `ferrumq_data_messages_delivered_total`: messages returned by successful
  consume responses.
- `ferrumq_data_acks_total`: ACK attempts by `status`.
- `ferrumq_data_nacks_total`: NACK attempts by `status`.

Broker counters:

- `ferrumq_broker_opens_total`: durable broker open attempts by `status`.
- `ferrumq_broker_recoveries_total`: broker-state recovery passes by `status`.
- `ferrumq_broker_topics_created_total`: durable topic creation attempts by
  `status`.
- `ferrumq_broker_messages_published_total`: durable publish attempts by
  `status`.
- `ferrumq_broker_consumes_total`: durable consume attempts by `status`.
- `ferrumq_broker_deliveries_created_total`: delivery records created.
- `ferrumq_broker_acks_total`: durable ACK attempts by `status`.
- `ferrumq_broker_nacks_total`: durable NACK attempts by `status`.
- `ferrumq_broker_retry_maintenance_total`: retry maintenance passes by
  `status`.
- `ferrumq_broker_dlq_transitions_total`: DLQ transitions by `kind`, currently
  `nack` or `expired`.

Storage counters:

- `ferrumq_storage_partition_log_opens_total`: partition log open attempts by
  `status`.
- `ferrumq_storage_partition_log_recoveries_total`: partition log recovery
  passes by `status`.
- `ferrumq_storage_appends_total`: storage append attempts by `status`.
- `ferrumq_storage_trailing_repairs_total`: final trailing-record repairs by
  sanitized `kind`.
- `ferrumq_storage_errors_total`: storage errors by sanitized `kind`.

## Limitations

Metrics are process-local. If HTTP control and gRPC data-plane servers run as
separate processes, `GET /metrics` on the HTTP process reports only the HTTP
process counters. Rust in-process tests can inspect one registry because they
run adapters in a single process. A separate data-plane metrics listener,
remote scraping topology, and cross-process aggregation are deferred.

Deferred observability scope includes Grafana dashboards, OpenTelemetry
collector/export pipelines, hosted telemetry, auth/TLS/rate limiting for
metrics, clustering/replication metrics, exactly-once telemetry, multi-tenant
telemetry, and advanced TUI observability panels.
