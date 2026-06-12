# FerrumQ TUI

Milestone 8 introduces `ferrumq-tui`, the first usable TypeScript terminal UI.
It is an Ink dashboard over the HTTP control plane. Rust remains the source of
broker behavior.

## Usage

Start the HTTP control plane:

```sh
cargo run -p msg-runtime --bin brokerd -- serve --data-dir ./.ferrumq --listen 127.0.0.1:8080
```

Run the TUI:

```sh
ferrumq-tui
ferrumq-tui --control-url http://127.0.0.1:8080 --grpc-url http://127.0.0.1:9090
ferrumq-tui --help
ferrumq-tui --version
```

The built development entrypoint can be smoke-tested directly:

```sh
pnpm --filter @ferrumq/tui build
node packages/tui/dist/cli.js --help
node packages/tui/dist/cli.js --version
```

## Configuration

Defaults:

- Control-plane URL: `http://127.0.0.1:8080`.
- gRPC data-plane URL: `http://127.0.0.1:9090`.

Precedence is CLI flag, then environment variable, then default:

```sh
ferrumq-tui --control-url http://control.local:8080
FERRUMQ_CONTROL_URL=http://control.local:8080 ferrumq-tui
FERRUMQ_GRPC_URL=http://broker.local:9090 ferrumq-tui
```

The control URL must be an HTTP or HTTPS origin without credentials, path,
query, or fragment. The gRPC URL must be an HTTP origin with an explicit port;
gRPC TLS/HTTPS remains deferred.

## Screens

Dashboard:

- FerrumQ version.
- Control URL.
- Configured gRPC URL.
- Health and readiness status.
- Broker mode.
- Topic count.
- DLQ count.
- Last refresh timestamp.
- Last error, if any.

Topics:

- Topic names and partition counts in API order.
- Empty state when no topics exist.

DLQ:

- Topic, partition, offset, message ID, consumer group, reason, attempt count,
  and timestamp.
- Empty state when no DLQ entries exist.

Help:

- In-app key bindings.

## Keys

- `r` or `R`: refresh.
- `q` or `Q`: quit.
- `1`: dashboard.
- `2`: topics.
- `3`: DLQ.
- `?`: help.

There is no auto-refresh in Milestone 8. Refresh is manual.

## Error Behavior

On startup and refresh, the TUI concurrently fetches:

- `GET /health`.
- `GET /ready`.
- `GET /v1/status`.
- `GET /v1/topics`.
- `GET /v1/dlq`.

`GET /metrics` exists on the HTTP control plane as an operational Prometheus
endpoint, but the TUI does not fetch it or render charts/live metric panels in
Milestone 9.

Expected failures render as short messages without stack traces. The shared
HTTP client distinguishes network failures, FerrumQ non-2xx error envelopes,
malformed non-2xx responses, invalid JSON, and schema validation failures.
Before the first successful snapshot, startup loading and error states are
visible from Dashboard, Topics, DLQ, and Help. Topics and DLQ only show their
empty states after the API has returned a loaded empty list.

If a refresh partially fails, the TUI shows one user-facing error message and
keeps the last successful snapshot visible, including stale Topics and DLQ rows.

## Limitations

The TUI is read-only in Milestone 8. It does not publish, consume, ACK, NACK,
retry messages, inspect lag, stream logs, fetch `/metrics`, call the gRPC data
plane, start or supervise broker processes, or implement broker semantics.
Public SDK workflows, auth/RBAC, TLS, rate limiting, observability
dashboards/export, clustering, replication, exactly-once semantics, and
MaaS/multi-tenancy remain deferred.
