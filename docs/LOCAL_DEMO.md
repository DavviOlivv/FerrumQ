# Local Demo

This flow exercises the current FerrumQ local broker surface without requiring
published packages or a remote service.

## Build

```sh
pnpm install --frozen-lockfile
pnpm build
cargo build --workspace
```

Use a disposable local data directory:

```sh
rm -rf ./.ferrumq-demo
mkdir -p ./.ferrumq-demo
```

## Recommended Runtime

Use `brokerd serve-all` for coherent local demos and development. It starts the
HTTP control plane and gRPC data plane in one OS process backed by one shared
`DurableBroker`.

```sh
cargo run -p msg-runtime --bin brokerd -- serve-all \
  --data-dir ./.ferrumq-demo \
  --http-listen 127.0.0.1:8080 \
  --grpc-listen 127.0.0.1:9090
```

In another shell, create and inspect a topic through HTTP:

```sh
node packages/cli/dist/cli.js health
node packages/cli/dist/cli.js ready
node packages/cli/dist/cli.js status
node packages/cli/dist/cli.js topic create orders --partitions 3
node packages/cli/dist/cli.js topic list
node packages/cli/dist/cli.js topic get orders
```

Publish through gRPC:

```sh
node packages/cli/dist/cli.js publish orders --data '{"orderId":1,"status":"created"}' --key account-1 --idempotency-key demo-1
```

Consume through gRPC:

```sh
node packages/cli/dist/cli.js consume orders --group workers --consumer-id worker-1 --max 1 --lease-ms 30000
```

The consume output includes a delivery ID. ACK it:

```sh
node packages/cli/dist/cli.js ack <delivery-id> --consumer-id worker-1
```

To see NACK and DLQ inspection, publish another message and NACK each returned
delivery ID until the default retry policy moves it to the DLQ:

```sh
node packages/cli/dist/cli.js publish orders --data '{"orderId":2,"status":"reject"}'
node packages/cli/dist/cli.js consume orders --group workers --consumer-id worker-1 --max 1
node packages/cli/dist/cli.js nack <delivery-id> --consumer-id worker-1 --reason poison
sleep 1
node packages/cli/dist/cli.js consume orders --group workers --consumer-id worker-1 --max 1
node packages/cli/dist/cli.js nack <delivery-id> --consumer-id worker-1 --reason poison
sleep 1
node packages/cli/dist/cli.js consume orders --group workers --consumer-id worker-1 --max 1
node packages/cli/dist/cli.js nack <delivery-id> --consumer-id worker-1 --reason poison
node packages/cli/dist/cli.js dlq list --topic orders
```

Equivalent direct HTTP checks:

```sh
curl http://127.0.0.1:8080/v1/status
curl http://127.0.0.1:8080/metrics
```

With `serve-all`, `/metrics` is still process-local, but the one process has
both HTTP and gRPC counters. A scrape after the flow above includes topic
creation plus publish, consume, ACK, and NACK counters.

## TUI

You can inspect the same live HTTP process with the read-only TUI while
`serve-all` is running:

```sh
node packages/tui/dist/cli.js --control-url http://127.0.0.1:8080 --grpc-url http://127.0.0.1:9090
```

Useful keys:

- `r`: refresh.
- `1`: dashboard.
- `2`: topics.
- `3`: DLQ.
- `?`: help.
- `q`: quit.

The TUI reads HTTP state only. With `serve-all`, that HTTP state is backed by
the same in-process broker that gRPC mutates.

## Split-Process Compatibility

`brokerd serve` and `brokerd serve-grpc` remain valid, but they are intentionally
split local processes:

```sh
cargo run -p msg-runtime --bin brokerd -- serve --data-dir ./.ferrumq-demo --listen 127.0.0.1:8080
cargo run -p msg-runtime --bin brokerd -- serve-grpc --data-dir ./.ferrumq-demo --listen 127.0.0.1:9090
```

Each process opens its own `DurableBroker` state at startup. A shared
`--data-dir` persists state across restarts, but running processes do not
live-reload each other's mutations or share in-memory state. In this mode, an
already-running HTTP process or TUI will not show live gRPC-process changes.
`/metrics` is also process-local; HTTP `/metrics` does not expose counters from
a separate gRPC process. `serve-all` solves local live state and metrics
coherence only inside one process. Cross-process live reload, distributed
locking, and metrics aggregation remain deferred.

## Safety Notes

FerrumQ currently provides local durable at-least-once delivery only. Consumers
must be idempotent. `idempotency_key` is metadata-only and is not enforced for
producer deduplication. Exactly-once delivery, clustering, replication,
auth/RBAC, TLS, MaaS/multi-tenancy, dashboards, and production daemon hardening
are outside the current release scope.

Message payloads are not logged by default and are not exported as metric
labels.
