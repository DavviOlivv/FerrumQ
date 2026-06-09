# FerrumQ CLI

Milestone 7 introduces the first usable TypeScript CLI. The public binary is
`ferrumq`; `msg` remains a compatibility alias. The CLI is an adapter only:
control-plane commands call the HTTP API and data-plane commands call unary
gRPC. Broker behavior remains owned by Rust.

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

## Commands

```sh
ferrumq --version
ferrumq --help
ferrumq broker --help
ferrumq broker version
```

`broker version` runs `brokerd --version`. If `brokerd` is not on `PATH`, the
CLI reports a short expected error. Broker process supervision commands are not
implemented in Milestone 7; start `brokerd serve` and `brokerd serve-grpc`
directly.

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

Data-plane commands:

```sh
ferrumq publish orders --data '{"id":1}'
ferrumq publish orders --data '{"id":1}' --key account-1 --message-id msg-custom
ferrumq consume orders --group workers --max 10 --lease-ms 30000
ferrumq ack delivery-1
ferrumq nack delivery-1 --reason poison
```

Publish generates `message_id` as `msg_${crypto.randomUUID()}` unless
`--message-id` is provided. `--data` must be non-empty. Topic names, consumer
groups, bounded identifiers, partition counts, consume limits, and lease values
are validated before requests are sent.

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

## Deferred Scope

The TypeScript CLI does not start, supervise, or embed the broker. TUI,
public SDK, auth/RBAC, TLS, streaming consume, rate limiting, observability
dashboards/export, clustering, replication, exactly-once semantics, and
MaaS/multi-tenancy remain deferred.
