# FerrumQ CLI

The public TypeScript CLI binary is `ferrumq`; `msg` remains a compatibility
alias. The CLI is an adapter only: control-plane commands call the HTTP API and
data-plane commands call unary gRPC. Broker behavior remains owned by Rust.

The separate `ferrumq-tui` binary provides read-only terminal inspection. It is
documented in [TUI.md](TUI.md) and does not change the CLI command surface.

## Defaults

- Control-plane URL: `http://127.0.0.1:8080`.
- gRPC data-plane URL: `http://127.0.0.1:9090`.
- Consumer ID: `ferrumq-cli`.
- Publish content type: `application/json`.
- Publish type: `ferrumq.cli.message`.
- Publish source: `ferrumq-cli`.

Configuration precedence is CLI flag, then environment variable, then default:

```sh
ferrumq --control-url http://127.0.0.1:8080 --grpc-url http://127.0.0.1:9090 status
FERRUMQ_CONTROL_URL=http://127.0.0.1:8080 ferrumq health
FERRUMQ_GRPC_URL=http://127.0.0.1:9090 ferrumq consume orders --group workers
```

Control URLs must be HTTP or HTTPS origins without credentials, paths, queries,
or fragments. gRPC URLs must be HTTP origins with an explicit port and no
credentials, paths, queries, or fragments; gRPC TLS/HTTPS remains deferred.

## Commands

```sh
ferrumq --version
ferrumq --help
ferrumq broker --help
ferrumq topic --help
ferrumq publish --help
ferrumq broker version
```

Help and version commands are local, exit `0`, and do not call the HTTP or gRPC
clients.

`broker version` runs `brokerd --version`. If `brokerd` is not on `PATH`, the
CLI reports a short expected error. Broker process supervision commands are not
implemented; start `brokerd serve` and `brokerd serve-grpc` directly.

Those runtime commands start separate local processes. Each process opens its
own `DurableBroker` state at startup. A shared `--data-dir` persists state
across restarts, but it does not provide live shared in-memory state or
live-reload between running HTTP and gRPC processes. For local demos, create
topics through `brokerd serve`, stop that process, then start
`brokerd serve-grpc` against the same data directory for publish, consume, ACK,
and NACK commands.

Control-plane commands:

```sh
ferrumq health
ferrumq ready
ferrumq status
ferrumq topic create orders --partitions 3
ferrumq topic get orders
ferrumq topic list
ferrumq dlq list
ferrumq dlq list --topic orders
```

`GET /metrics` is available on the HTTP control plane as an operational
Prometheus endpoint. The CLI does not wrap it as a command; use HTTP tooling
such as `curl` when metrics text is needed. In the split-process setup, HTTP
`/metrics` reports only HTTP-process counters, not gRPC counters.

Data-plane commands:

```sh
ferrumq publish orders --data '{"id":1}'
ferrumq publish orders --data '{"id":1}' --key account-1 --message-id msg-custom
ferrumq consume orders --group workers --max 10 --lease-ms 30000
ferrumq ack delivery-1
ferrumq nack delivery-1 --reason poison
```

Publish generates `message_id` as `msg_${crypto.randomUUID()}` unless
`--message-id` is provided. `--idempotency-key` is sent as message metadata only;
the broker does not deduplicate publishes by that key yet. `--data` must be
non-empty. Topic names, consumer groups, bounded identifiers, partition counts,
consume limits, and lease values are validated before requests are sent.

## Output

Human-readable output is the default. `--json` writes a stable single JSON
object:

```json
{ "health": { "status": "ok" } }
```

Wrappers are:

- `{ "health": ... }`, `{ "ready": ... }`, `{ "status": ... }`.
- `{ "topic": ... }`, `{ "topics": [...] }`.
- `{ "dlq": { "items": [...] } }`.
- `{ "message": { "id", "topic", "partition", "offset" } }`.
- `{ "messages": [...] }`.
- `{ "ack": { "deliveryId", "consumerId" } }`.
- `{ "nack": { "deliveryId", "consumerId", "reason" } }`.

gRPC `uint64` response fields are rendered as decimal strings in JSON so large
offsets and timestamps are not truncated by JavaScript number limits.

Human `consume` output includes delivery ID, message ID, topic, partition,
offset, attempt number, and payload.

## Errors

Expected failures exit non-zero without stack traces. HTTP non-2xx responses
surface the FerrumQ error envelope code and message, for example:

```txt
HTTP 400 VALIDATION_ERROR: topic_name must not be empty
```

Network failures and gRPC status failures are also short expected errors:

```txt
Network request failed for GET http://127.0.0.1:8080/ready: connection refused
gRPC INVALID_ARGUMENT (3): topic_name must not be empty
```

Error output is currently human text on stderr even when `--json` is set.

## Deferred Scope

The TypeScript CLI does not start, supervise, or embed the broker. Public SDK,
auth/RBAC, TLS, streaming consume, rate limiting, metrics commands,
observability dashboards/export, clustering, replication, exactly-once
semantics, and MaaS/multi-tenancy remain deferred. TypeScript process-level
gRPC integration tests are also deferred because
`brokerd serve-grpc --listen 127.0.0.1:0` does not expose the selected port;
the project relies on Rust in-process gRPC tests and mocked TypeScript client
seams for that boundary.
