# TypeScript SDK

`@ferrumq/sdk` is a reusable TypeScript SDK for interacting with FerrumQ brokers.
It wraps the HTTP control plane and gRPC data plane into a single typed client
with payload encoding, error handling, timeout support, and lifecycle
management.

## Installation / Workspace Usage

The SDK is part of the pnpm monorepo:

```json
{
  "dependencies": {
    "@ferrumq/sdk": "workspace:*"
  }
}
```

Build the protocol dependency and SDK:

```sh
pnpm install
pnpm --filter @ferrumq/protocol run build
pnpm --filter @ferrumq/sdk run build
```

## Quickstart

```ts
import { FerrumQClient } from "@ferrumq/sdk";

const client = new FerrumQClient({
  httpUrl: "http://127.0.0.1:8080",
  grpcUrl: "http://127.0.0.1:9090",
});

await client.createTopic({ name: "orders", partitions: 3 });

const published = await client.publish({
  topic: "orders",
  key: "account-1",
  payload: { orderId: 1, status: "created" },
});

const deliveries = await client.consume({
  topic: "orders",
  group: "workers",
  maxMessages: 1,
});

for (const delivery of deliveries) {
  await client.ack({
    deliveryId: delivery.deliveryId,
  });
}

console.log(published);
await client.close();
```

## Client Construction

```ts
import { FerrumQClient } from "@ferrumq/sdk";

const client = new FerrumQClient({
  httpUrl: "http://127.0.0.1:8080",
  grpcUrl: "http://127.0.0.1:9090",
  timeoutMs: 10_000,
});
```

- `httpUrl` (required): HTTP control plane URL. Supports `http://` and `https://`.
- `grpcUrl` (required): gRPC data plane URL. Must be `http://host:port` (no TLS, no path).
- `timeoutMs` (optional): Per-request timeout in milliseconds. When set, operations that
  exceed this duration reject with a `FerrumQError` with transport `"sdk"`. Omitted or zero
  means no timeout.

Configuration is validated at construction time. Invalid or missing URLs throw
`FerrumQError`.

## Control Plane (HTTP)

All control-plane methods return typed responses matching the broker's HTTP API DTOs.

### `health()`

```ts
const status: HealthStatus = await client.health();
// { status: "ok" }
```

### `readiness()`

```ts
const status: HealthStatus = await client.readiness();
// { status: "ready" }
```

### `status()`

```ts
const status: BrokerStatus = await client.status();
// { mode: "local-durable", dataDir: "./.ferrumq", topics: 2, dlqEntries: 1 }
```

### `metrics()`

```ts
const text: string = await client.metrics();
// Prometheus text exposition format
```

### `createTopic(request)`

```ts
const topic: Topic = await client.createTopic({
  name: "orders",
  partitions: 3,
});
// { name: "orders", partitions: 3 }
```

Returns `Topic`. Duplicate topic creation throws `FerrumQError` with code
`"TOPIC_ALREADY_EXISTS"` and status `409`.

### `listTopics()`

```ts
const topics: Topic[] = await client.listTopics();
// [{ name: "orders", partitions: 3 }, ...]
```

### `getTopic(name)`

```ts
const topic: Topic = await client.getTopic("orders");
// { name: "orders", partitions: 3 }
```

### `listDlq(topic?)`

```ts
const entries: DlqEntry[] = await client.listDlq("orders");
// filtered to one topic

const all: DlqEntry[] = await client.listDlq();
// all topics
```

Each `DlqEntry` has: `topic`, `partition`, `offset`, `messageId`,
`consumerGroupId`, `reason`, `attemptCount`, `timestamp`.

## Data Plane (gRPC)

The gRPC client is lazily created on first data-plane call. It uses an insecure
channel matching the repository's existing `@grpc/grpc-js` setup.

### `publish(request)`

```ts
const result: PublishResult = await client.publish({
  topic: "orders",
  key: "account-1",
  payload: { orderId: 1, status: "created" },
});
// { topic: "orders", partition: 0, offset: "0", messageId: "..." }
```

- `topic` (required): Name of an existing topic.
- `payload` (required): String, `Uint8Array`, `Buffer`, or JSON-compatible value.
- `key` (optional): Partition key for deterministic routing.
- `messageId` (optional): Auto-generated UUID if omitted.
- `contentType` (optional): Auto-derived from payload encoding if omitted.
- `type` (optional): Defaults to `"ferrumq.sdk.message"`.
- `source` (optional): Defaults to `"ferrumq-sdk"`.
- `subject` (optional): Event subject.
- `idempotencyKey` (optional): Metadata only; deduplication is not enforced.
- `timeUnixMs` (optional): Defaults to `Date.now()`.

### `consume(request)`

```ts
const messages: ConsumedMessage[] = await client.consume({
  topic: "orders",
  group: "workers",
  consumerId: "worker-1",
  maxMessages: 5,
  leaseMs: 30_000,
});
```

- `topic` (required): Topic to consume from.
- `group` (required): Consumer group name.
- `consumerId` (optional): Defaults to `"ferrumq-sdk"`. Must match ACK/NACK `consumerId`.
- `maxMessages` (optional): Defaults to `1`.
- `leaseMs` (optional): Defaults to `30000` (30 seconds).

Each `ConsumedMessage` includes: `deliveryId`, `topic`, `partition`, `offset`
(decimal string), `messageId`, `key` (nullable), `payload` (`Uint8Array`),
`contentType`, `type`, `source`, `subject` (nullable), `idempotencyKey`
(nullable), `timeUnixMs` (decimal string), `consumerGroup`, `consumerId`,
`attemptNumber`, `deliveredAtUnixMs` (decimal string), and
`leaseExpiresAtUnixMs` (decimal string).

Empty optional proto fields are normalized to `null` (`key`, `subject`,
`idempotencyKey`).

### `ack(request)`

```ts
await client.ack({
  deliveryId: "del-1",
  consumerId: "worker-1",
});
```

- `consumerId` (optional): Defaults to `"ferrumq-sdk"`. Must own the delivery.

### `nack(request)`

```ts
await client.nack({
  deliveryId: "del-1",
  consumerId: "worker-1",
  reason: "poison",
});
```

- `reason` (optional): NACK reason. Broker default is used when omitted.

## Payload Encoding

The SDK encodes payloads deterministically:

| Payload Type                                                            | Encoding                          | Content-Type               |
| ----------------------------------------------------------------------- | --------------------------------- | -------------------------- |
| `string`                                                                | UTF-8 bytes                       | `text/plain`               |
| `Uint8Array`                                                            | As-is                             | `application/octet-stream` |
| `Buffer`                                                                | As-is (Buffer extends Uint8Array) | `application/octet-stream` |
| JSON-compatible values (`object`, `array`, `number`, `boolean`, `null`) | `JSON.stringify` → UTF-8 bytes    | `application/json`         |

Unsupported types (`function`, `symbol`, `undefined`) throw `FerrumQError` with
transport `"sdk"`. Circular references that fail `JSON.stringify` are wrapped as
SDK errors.

When `contentType` is omitted from `PublishRequest`, the SDK derives it from the
encoded payload. An explicit `contentType` overrides the auto-derived value.

## Errors

All errors thrown by the SDK are instances of `FerrumQError`:

```ts
class FerrumQError extends Error {
  readonly code?: string; // FerrumQ error code or gRPC status name
  readonly status?: number; // HTTP status code (HTTP errors only)
  readonly transport: "http" | "grpc" | "sdk";
  readonly cause?: unknown; // Original error
}
```

### HTTP Errors

- `transport: "http"`
- `status`: HTTP status code (e.g., 404)
- `code`: FerrumQ error code (e.g., `"TOPIC_NOT_FOUND"`, `"INVALID_REQUEST"`, `"INTERNAL_ERROR"`)
- `cause`: Original `ControlPlaneRequestError`

### gRPC Errors

- `transport: "grpc"`
- `code`: gRPC status name (e.g., `"NOT_FOUND"`, `"INVALID_ARGUMENT"`, `"INTERNAL"`)
- `cause`: Original gRPC error object

### SDK Errors

- `transport: "sdk"`
- Raised for configuration validation, payload serialization failures, timeouts,
  and calls on a closed client.

## Timeouts

When `timeoutMs` is set, every operation is bounded by a `Promise.race` with a
rejecting timeout. Timeout rejections are `FerrumQError` with transport `"sdk"`
and a message indicating the timeout duration.

Automatic retries are not implemented. Retries without an idempotency policy can
duplicate non-idempotent operations such as publish. Callers remain responsible
for application-level retry logic.

## Cleanup

```ts
await client.close();
```

- Closes the gRPC channel.
- Marks the client as closed.
- Subsequent operations reject with `FerrumQError`.
- `close()` is idempotent.

## Examples

See the `examples/` directory for executable flows:

- `examples/basic-flow.ts`: connect, create topic, publish, consume, ACK, status.
- `examples/nack-dlq-flow.ts`: publish, consume, NACK, retry, DLQ inspection.
- `examples/status-metrics.ts`: broker status, topic listing, metrics, DLQ.

Run with:

```sh
tsx examples/basic-flow.ts
```

Requires `brokerd serve-all` running on `http://127.0.0.1:8080` and gRPC on
`http://127.0.0.1:9090`.

## Node.js Support

The SDK targets the same Node.js versions as the rest of the FerrumQ monorepo
(ES2022, Node.js 18+). It uses:

- `node:crypto` for UUID generation.
- `TextEncoder` and `TextDecoder` (Node.js global).
- `fetch` (Node.js 18+ global).
- `@grpc/grpc-js` (via `@ferrumq/protocol`).

Browser support is not implemented.

## Limitations

The following are explicitly deferred and not provided by this SDK:

- PostgreSQL metadata and projections.
- File payloads and blob storage.
- Authentication and API keys.
- TLS/mTLS.
- Automatic retries.
- Browser support.
- Streaming consume.
- Cluster and replication.
- Exactly-once delivery.
- `idempotency_key` enforcement for publish deduplication.
