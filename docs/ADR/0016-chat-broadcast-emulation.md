# ADR 0016: Chat Broadcast Emulation Through Independent Consumer Groups

## Status

Accepted.

## Context

The FerrumQ multi-terminal chat example requires that every connected
participant sees every message published to a room. FerrumQ's current
consumer-group semantics are competing-consumer: messages in a group are
distributed among group members. This is appropriate for work queues but
would cause only one chat participant to receive each message if all
participants shared one consumer group.

FerrumQ does not have native fan-out subscriptions or topic-level
broadcast delivery. Adding them would be a broker-level protocol and
storage change that crosses multiple crates and modifies the protobuf
and HTTP contracts.

## Decision

Each chat participant uses a unique, independently-named consumer group
per session:

```text
consumer group = chat.{room}.session.{sessionId}
```

This means:

- Every connected participant has an independent offset cursor per room.
- Every participant consumes from offset 0 onward independently.
- Every participant sees every message published to the room topic.
- No change to broker consumer-group semantics is required.

The chat application handles:

- Session-local deduplication via an in-memory LRU cache keyed by application
  message ID with a SHA-256 content fingerprint.
- ACK after a message is accepted for display, advancing the per-group cursor.
- ACK for malformed messages to prevent redelivery loops.

## Consequences

### Positive

- The chat example works with the current broker without modification.
- The broker's at-least-once delivery, retry, DLQ, offset, and partition-key
  contracts remain unchanged.
- The consumer-group-per-participant pattern is explicitly documented as an
  application-layer decision, not a broker feature.
- If fan-out subscriptions are added later, the chat can migrate without
  breaking existing broker users.

### Negative

- A new participant joining a room with existing messages will see the full
  topic history from offset 0, because their consumer group starts fresh.
  This is visible to the user and documented honestly.
- Replay is bounded to five messages per unary consume. Without transport
  latency, replay duration is approximately
  `ceil(history / 5) × pollIntervalMs`.
- No `--history` or `--from` flag has been implemented to control history
  visibility on join.
- The number of consumer groups scales linearly with the number of unique
  chat sessions. For a local demo this is negligible; for a production
  deployment it would require broker-side consumer-group lifecycle management.
- Session groups are persistent and currently have no deletion, retention, or
  cleanup mechanism, so repeated chat launches grow broker state.
- Session-local deduplication is not exactly-once delivery. Duplicates
  delivered across broker restarts or after cache eviction will be displayed
  again.
- Reuse of one application message ID with different accepted content is
  treated as a conflict, warned, suppressed, and ACKed.

### Mitigations

- Document the history-on-join limitation in CHAT.md.
- Keep deduplication bounded (2048 entries) and session-local.
- Schedule history control (`--history`, cursor fast-forward) as future work
  and link it to fan-out subscription development.

## Alternatives Considered

### Native fan-out subscriptions

Adding a broadcast subscription type to the protobuf contract and broker
would be the correct long-term architecture. It was rejected for this
milestone because:

- It crosses Rust crates (`msg-core`, `msg-broker`, `msg-protocol`,
  `msg-data-plane`, `msg-control-api`, `msg-runtime`).
- It changes the protobuf contract and gRPC service definition.
- It requires storage format changes for durable subscriber state.
- The milestone scope is the first application-layer example, not a broker
  feature.

This remains the preferred long-term direction and should be reconsidered
when streaming consume is implemented.

### Shared consumer group with partition-key routing

Routing messages to specific consumers via partition keys would break chat
broadcast semantics and is not the right tool for this use case.

### Temporary topic per message

Creating a topic per message or per participant would be extremely inefficient
and violate topic lifecycle expectations.

## References

- [CHAT.md](../CHAT.md)
- [ADR 0004: At-Least-Once Delivery](0004-at-least-once-delivery.md)
- [ADR 0005: Control Plane vs Data Plane](0005-control-plane-vs-data-plane.md)
