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

## Split-Process Model

`brokerd serve` and `brokerd serve-grpc` are separate local processes. Each
opens its own `DurableBroker` state at startup. Using the same `--data-dir`
persists state across restarts, but it does not provide live shared in-memory
state and does not live-reload changes between running processes.

Run the HTTP control-plane demo first, stop it, then run the gRPC data-plane
demo against the same data directory. Do not expect an already-running HTTP
process or TUI to show live gRPC-process changes. `/metrics` is process-local;
HTTP `/metrics` does not expose gRPC counters in this split-process setup. A
combined runtime or reload/sync mechanism is deferred.

## HTTP Control Plane

Start the HTTP control plane:

```sh
cargo run -p msg-runtime --bin brokerd -- serve --data-dir ./.ferrumq-demo --listen 127.0.0.1:8080
```

In another shell, create and inspect a topic:

```sh
node packages/cli/dist/cli.js health
node packages/cli/dist/cli.js ready
node packages/cli/dist/cli.js status
node packages/cli/dist/cli.js topic create orders --partitions 3
node packages/cli/dist/cli.js topic list
node packages/cli/dist/cli.js topic get orders
```

Equivalent direct HTTP checks:

```sh
curl http://127.0.0.1:8080/v1/status
curl http://127.0.0.1:8080/metrics
```

You can inspect this HTTP process with the read-only TUI while the HTTP server
is running:

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

Stop the HTTP process before continuing. The `orders` topic remains on disk.

## gRPC Data Plane

Start the gRPC data plane:

```sh
cargo run -p msg-runtime --bin brokerd -- serve-grpc --data-dir ./.ferrumq-demo --listen 127.0.0.1:9090
```

The server loads the `orders` topic created by the prior HTTP process when it
opens `./.ferrumq-demo`.

Publish:

```sh
node packages/cli/dist/cli.js publish orders --data '{"orderId":1,"status":"created"}' --key account-1 --idempotency-key demo-1
```

Consume:

```sh
node packages/cli/dist/cli.js consume orders --group workers --consumer-id worker-1 --max 1 --lease-ms 30000
```

The consume output includes a delivery ID. ACK it:

```sh
node packages/cli/dist/cli.js ack <delivery-id> --consumer-id worker-1
```

To see NACK and DLQ inspection, publish and consume another message, then NACK
the returned delivery ID:

```sh
node packages/cli/dist/cli.js publish orders --data '{"orderId":2,"status":"reject"}'
node packages/cli/dist/cli.js consume orders --group workers --consumer-id worker-1 --max 1
node packages/cli/dist/cli.js nack <delivery-id> --consumer-id worker-1 --reason poison
```

To inspect the resulting DLQ state through HTTP, stop the gRPC process and
restart the HTTP process against the same data directory:

```sh
cargo run -p msg-runtime --bin brokerd -- serve --data-dir ./.ferrumq-demo --listen 127.0.0.1:8080
```

Then query the reopened HTTP process from another shell:

```sh
node packages/cli/dist/cli.js dlq list --topic orders
```

## Metrics

```sh
curl http://127.0.0.1:8080/metrics
```

Metrics are process-local. In the split-process demo, this endpoint reports
HTTP-process counters only. It does not include publish, consume, ACK, or NACK
counters from the earlier gRPC process.

## Safety Notes

FerrumQ currently provides local durable at-least-once delivery only. Consumers
must be idempotent. `idempotency_key` is metadata-only and is not enforced for
producer deduplication. Exactly-once delivery, clustering, replication,
auth/RBAC, TLS, MaaS/multi-tenancy, dashboards, and production daemon hardening
are outside the current release scope.

Message payloads are not logged by default and are not exported as metric
labels.
