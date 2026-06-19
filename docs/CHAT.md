# FerrumQ Chat

The `@ferrumq/chat` package is the first complete user-facing application built
on the FerrumQ messaging engine. It demonstrates multi-terminal chat where
participants connected to the same `brokerd serve-all` runtime exchange messages
in real time.

This is a **local demonstration only**, not a secure production chat system.

## Purpose

- Exercise the full FerrumQ stack (HTTP control plane, gRPC data plane, SDK).
- Prove that independent consumer groups can emulate broadcast behavior.
- Demonstrate at-least-once delivery, ACK, and deduplication in practice.
- Provide a reference architecture for application-layer use of FerrumQ.

## Architecture

```text
Terminal A (Ink/React) ─┐
                         ├─ FerrumQClient (SDK) ─ HTTP/gRPC ─ brokerd serve-all
Terminal B (Ink/React) ─┘
```

Each terminal runs a `ChatApp` instance backed by `@ferrumq/sdk`. The chat
application is strictly an adapter layer — it does not own broker semantics.

Separation of concerns:

```text
Chat domain       → message schema, room/name validation, display model
Chat application  → join, publish, poll, ACK, shutdown
Terminal UI       → rendering and input (Ink/React)
FerrumQ SDK       → HTTP/gRPC transport
FerrumQ broker    → durable messaging engine
```

## How to Run

### 1. Start the broker

```bash
cargo run -p msg-runtime --bin brokerd -- serve-all \
  --data-dir ./.ferrumq \
  --http-listen 127.0.0.1:8080 \
  --grpc-listen 127.0.0.1:9090
```

### 2. Build the chat package

```bash
pnpm install --frozen-lockfile
pnpm build
```

### 3. Open multiple terminals

```bash
pnpm --filter @ferrumq/chat exec node dist/cli.js --name davi --room general
```

```bash
pnpm --filter @ferrumq/chat exec node dist/cli.js --name alice --room general
```

Or use the installed binary (after `pnpm build`):

```bash
node packages/chat/dist/cli.js --name davi --room general
```

### CLI Options

| Option | Required | Default | Description |
|---|---|---|---|
| `--name` | Yes | — | Display name |
| `--room` | Yes | — | Room name |
| `--http-url` | No | `FERRUMQ_HTTP_URL` or `http://127.0.0.1:8080` | HTTP control plane URL |
| `--grpc-url` | No | `FERRUMQ_GRPC_URL` or `http://127.0.0.1:9090` | gRPC data plane URL |
| `--timeout-ms` | No | `10000` | Request timeout in ms |
| `--poll-interval-ms` | No | `500` | Poll interval in ms |

Each URL is resolved independently. Precedence is the explicit CLI flag,
then its corresponding environment variable (`FERRUMQ_HTTP_URL` or
`FERRUMQ_GRPC_URL`), then the built-in default.

### Controls

| Key | Action |
|---|---|
| Enter | Send message |
| Esc | Quit |
| Ctrl+C | Quit (handled by Ink) |

Only one publish is allowed in flight per UI generation. Repeated Enter presses
are ignored until it settles. A successful publish removes only the submitted
input snapshot, preserving characters typed while it was pending; a failed
publish leaves the buffer intact for manual retry.

## Room/Topic Mapping

Each room maps to a deterministic FerrumQ topic:

```text
room "general" → topic "chat.general"
```

Room names are validated and normalized:

- Lowercased and trimmed.
- Must contain 1–64 ASCII letters, digits, dots, hyphens, or underscores.
- Must not start or end with a dot.
- Must not contain consecutive dots (`..`).
- May start or end with a hyphen or underscore.

A single partition per room is used because chat display order is easier
to explain and test with ordered delivery.

Display names are trimmed and must contain 1–32 ASCII alphanumeric characters,
dots, hyphens, or underscores. A one-character name must be alphanumeric;
longer names must start and end with an alphanumeric character.

## Participant Identity and Consumer-Group Strategy

Each participant generates:

- A stable **participant ID** (UUID).
- A unique **session ID** per chat process instance.
- A unique **consumer group** per session: `chat.{room}.session.{sessionId}`.
- A unique **consumer ID**: `chat-session-{sessionId}`.

Two terminals using the same display name have different session IDs, consumer
groups, and consumer IDs. No collision occurs.

## Why Independent Consumer Groups?

FerrumQ's current consumer-group semantics are **competing-consumer**: messages
in a group are distributed among group members. This is appropriate for work
queues but would cause only one chat participant to receive each message.

To emulate broadcast behavior without native fan-out subscriptions, each
participant uses its own consumer group. This means:

- Every connected participant has an independent offset cursor.
- Every participant sees every message published to the room topic.
- No change to broker consumer-group semantics is required.

### Limitations

- New participants joining a room with existing messages will see the full
  topic history (all messages from offset 0) on first consume, because their
  consumer group starts fresh at offset 0.
- Replay is intentionally incremental: each consume requests at most five
  messages. Ignoring transport latency, replaying `history` messages therefore
  takes approximately `ceil(history / 5) × pollIntervalMs`.
- Session-local deduplication prevents duplicate display of the same
  application message ID, but does not provide exactly-once delivery.
- There is no presence protocol — participants cannot see who is online.
- History visibility on join is a known limitation. A `--history` flag or
  cursor fast-forward mechanism is not implemented in this version.
- Every chat session creates a **durable consumer group** that persists across
  broker restarts. Repeated connections accumulate durable consumer-group
  state. There is no consumer-group deletion, retention, or cleanup mechanism.
  This is not an issue for local demos and manual testing with a handful of
  sessions, but would need broker-side lifecycle management for production use.
- Transparent reconnection after broker restart is **not** supported. If the
  broker restarts while a chat client is open, the client's consumer-group
  state may be out of sync and the user should restart the chat.

## Message Envelope

Chat messages use a versioned JSON application payload:

```ts
interface ChatMessageV1 {
  version: 1;
  id: string;          // Application-level message UUID
  room: string;        // Room name
  sender: {
    id: string;        // Stable participant UUID
    name: string;      // Display name (sanitized)
    sessionId: string; // Per-process session UUID
  };
  text: string;        // Message text (sanitized, max 4096 chars)
  sentAt: string;      // ISO 8601 UTC timestamp
}
```

Published to the broker with `type: "ferrumq.chat.message.v1"` and
`source: "ferrumq-chat"`.

## Delivery Semantics

### At-Least-Once and Deduplication

FerrumQ provides at-least-once delivery. Duplicate deliveries are possible.
The chat application implements session-local deduplication:

- In-memory LRU cache keyed by application message ID and SHA-256 content
  fingerprint.
- Bounded to 2048 entries.
- The same ID with the same accepted content is a duplicate and is ACKed
  without display.
- The same ID with different accepted content is treated as a conflict: a
  warning is emitted, the delivery is ACKed, and it is not rendered.
- A message enters the cache only after the display callback accepts it.
- This is **not exactly-once delivery** — it is a best-effort display guard.

### ACK Policy

- **Valid chat messages**: ACK after the message is accepted for display.
- **Malformed chat messages**: ACK immediately (do not NACK; malformed
  messages are not retried). A concise warning is emitted.
- **Duplicate messages**: ACK immediately.

Malformed messages are ACKed (not NACKed) to prevent infinite redelivery
loops. A malformed message from a non-chat source will not crash the UI.

### Self-Messages

Self-published messages are received back from the broker and displayed.
This demonstrates the actual message path through publish → storage → consume
and avoids optimistic-delivery inconsistencies.

### Polling

The current data plane is unary (no streaming consume). The chat uses bounded
polling:

- Configurable poll interval (default: 500 ms).
- Exponential backoff on timeouts, non-shutdown cancellation, and transient
  gRPC statuses. The first retry uses `pollIntervalMs`; the cap is
  `max(30 seconds, pollIntervalMs)`.
- AbortController-based cancellation on shutdown.
- Only one consume request per session is in flight at a time.
- No tight busy loops, no unbounded recursion, no arbitrary fixed sleeps.
- Configuration, validation, authorization, and other permanent errors stop
  polling and move the application to an error state.

### Shutdown

- SIGINT (Ctrl+C) or SIGTERM triggers clean shutdown.
- Poll timer is cleared.
- Active HTTP/gRPC requests are aborted.
- SDK client is closed.
- State transitions to `disconnected`.

## Terminal Safety

All external input is sanitized:

- Broker payloads larger than 32 KiB are rejected before decoding, and UTF-8 is
  decoded in fatal mode.
- ANSI CSI sequences (`ESC[`) and OSC sequences (`ESC]...BEL`/`ESC]...ST`)
  are stripped from all received fields.
- The complete C0 range (U+0000–U+001F), U+007F (DEL), and the complete C1
  range (U+0080–U+009F) are stripped.
- Unicode bidirectional controls (including U+061C, U+200E–U+200F,
  U+202A–U+202E, and U+2066–U+2069), BOM, zero-width space, and word joiner
  are stripped to prevent misleading terminal output.
- ZWNJ and ZWJ are preserved only when accompanied by visible content, so
  normal scripts and joined emoji remain intact while invisible-only fields
  are rejected.
- Embedded newlines, carriage returns, and tabs are removed from external
  sender names and message text.
- Oversized fields are rejected rather than truncated.
- Any required field that becomes empty after sanitization triggers the
  malformed-message path.
- `sentAt` must be canonical UTC ISO 8601 (`Date#toISOString()` form) and no
  more than five minutes in the future.
- Terminal control sequences injected by other participants cannot affect
  the local display.
- Unicode, including Portuguese text and emoji, is preserved where the
  terminal supports it.

## Error Behavior

- **Broker unavailable at startup**: Error displayed, state set to `error`,
  client closed, no polling started. The user must restart the chat.
- **Broker unavailable during operation**: Warning displayed, polling continues
  with backoff. Repeated identical outage warnings are coalesced (only the
  first warning is shown). The warning is cleared automatically after the
  first successful consume following recovery.
- **Shutdown during outage/backoff**: Cancellation is immediate — backoff
  timers are cleared, no further polls are scheduled, and the client is closed.
- **Connection failure on startup**: Error displayed, state set to `error`.
  The user must restart the chat to attempt reconnection.
- **Message send failure**: Error displayed, user can retry. Unsent input is
  preserved in the input buffer.
- **Malformed messages from broker**: Acked immediately, warning logged.
- **Topic creation race**: `TOPIC_ALREADY_EXISTS` / `ALREADY_EXISTS` treated
  as success.
- **Permanent errors (configuration, validation, authorization, invalid
  response)**: Polling stops, the SDK client closes, and the application moves
  to an error state.
- **Transparent reconnection is not supported**: If the broker goes down and
  restarts, the chat session does not automatically recover its consumer-group
  state. The user must restart the chat (`Esc`, then re-launch).

## Security

This is a **local demonstration only**. The following are explicitly absent:

- No authentication or authorization.
- No TLS/mTLS encryption.
- No end-to-end encryption.
- Display names are not trusted identities.
- Messages are durably stored by the broker on local disk.
- No message editing or deletion.
- No file upload.
- No PostgreSQL history or search.
- No presence protocol.
- No rate limiting.

Do not use this chat for sensitive communication.

## Current Limitations

- Unary polling is not real-time streaming (gRPC streaming consume is deferred).
- New participants see full topic history (offset 0) on join.
- No `--history` flag to control history visibility.
- No message editing, deletion, or search.
- No presence protocol or typing indicators.
- No private messages.
- No authentication or encryption.
- No web UI.
- Session-local deduplication is not exactly-once delivery.
- No non-interactive send/receive mode.
- No transparent reconnection after broker restart.
- Consumer groups accumulate indefinitely; no cleanup mechanism.
- The UI retains at most 500 messages in memory and renders only the latest 200.
- Platform support: Linux remains the primary development environment. CI also
  validates Windows dependency installation, SDK/chat typecheck, tests, build,
  and chat help smoke. This is a focused portability gate, not a claim of
  unrestricted Windows terminal compatibility.

## Testing

- `pnpm --filter @ferrumq/chat test` runs unit, UI, and integration tests.
- The integration test starts `brokerd serve-all` with ephemeral ports and a
  temporary data directory, creates two independent SDK clients, and verifies
  multi-client message delivery.
- Unit tests cover domain validation, message parsing, sanitization,
  deduplication conflicts, identity generation, lifecycle states, transient
  versus permanent polling failures, and shutdown.
- UI tests verify terminal rendering, slow publish serialization, input
  preservation, stale-generation protection, and memory/render limits with a
  mocked SDK.
