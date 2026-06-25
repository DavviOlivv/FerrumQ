# FerrumQ TUI

`ferrumq-tui` is the TypeScript terminal UI. It is an Ink dashboard over the
HTTP control plane. Rust remains the source of broker behavior.

## Usage

Start the recommended local runtime:

```sh
cargo run -p msg-runtime --bin brokerd -- serve-all \
  --data-dir ./.ferrumq \
  --http-listen 127.0.0.1:8080 \
  --grpc-listen 127.0.0.1:9090
```

The TUI reads from the HTTP control plane only. With `serve-all`, that HTTP
control plane shares one in-process broker with the gRPC data plane, so manual
refreshes show live gRPC-process mutations made through the same runtime.
`brokerd serve` remains HTTP-only, and `brokerd serve-grpc` remains gRPC-only.
If those split processes use the same `--data-dir`, the TUI does not show live
gRPC-process changes until the HTTP process is stopped and restarted.

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

Search:

- Inline query input (press `4` to switch to this view). Type the query,
  press `Enter` to issue the request, and press `Backspace` to edit.
  Input is Unicode-safe and capped at 256 characters as a defensive
  limit. While the Search view is active, global navigation keys (`q`,
  `r`, `1`–`4`, `?`) are suppressed so the query can contain any
  printable character; the keys resume normal behavior after the
  user leaves the view.
- Topic filtering from the TUI is **deferred in M17**. Use the
  `ferrumq search <query> --topic <topic>` CLI command or
  `POST /v1/search/messages` with an explicit `topic` body field for
  topic-scoped searches.
- Result rows show topic, partition, offset, message ID, event type,
  source, subject, content type, time, payload length, shortened
  payload SHA-256 (first 12 characters + `…`), and FTS rank.
- Empty / loading / error / unavailable states are rendered
  explicitly. Raw payload bytes and idempotency keys are never
  rendered.
- The query is sent in the `POST /v1/search/messages` JSON body. The
  TUI does not log the query and does not call the gRPC data plane.

Help:

- In-app key bindings.

## Keys

- `r` or `R`: refresh.
- `q` or `Q`: quit.
- `1`: dashboard.
- `2`: topics.
- `3`: DLQ.
- `4`: search.
- `?`: help.

There is no auto-refresh. Refresh is manual.

## Error Behavior

On startup and refresh, the TUI concurrently fetches:

- `GET /health`.
- `GET /ready`.
- `GET /v1/status`.
- `GET /v1/topics`.
- `GET /v1/dlq`.

`GET /metrics` exists on the HTTP control plane as an operational Prometheus
endpoint, but the TUI does not fetch it or render charts/live metric panels in
the current release.

Expected failures render as short messages without stack traces. The shared
HTTP client distinguishes network failures, FerrumQ non-2xx error envelopes,
malformed non-2xx responses, invalid JSON, and schema validation failures.
Before the first successful snapshot, startup loading and error states are
visible from Dashboard, Topics, DLQ, and Help. Topics and DLQ only show their
empty states after the API has returned a loaded empty list.

If a refresh partially fails, the TUI shows one user-facing error message and
keeps the last successful snapshot visible, including stale Topics and DLQ rows.

## Limitations

The TUI is read-only. It does not publish, consume, ACK, NACK, retry messages,
inspect lag, stream logs, fetch `/metrics`, call the gRPC data plane, start or
supervise broker processes, or implement broker semantics.
The configured gRPC URL is displayed for operator context only; it is not used
for direct data-plane inspection.
The Search view is a minimal foundation: it exposes an inline query input
with Enter to submit and Backspace to edit. Topic filtering from the TUI
itself is deferred in M17; cursor, scroll, copy/paste, search history,
autosuggest, and saved searches are also deferred polish.
Public SDK workflows, auth/RBAC, TLS, rate limiting, observability
dashboards/export, clustering, replication, exactly-once semantics, and
MaaS/multi-tenancy remain deferred.
