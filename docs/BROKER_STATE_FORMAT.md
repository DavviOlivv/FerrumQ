# Broker State Format

`DurableBroker` uses two local durable stores under its configured root:

```txt
<root>/
  messages/
    topics/<topic>/partitions/<partition-id>/<20-digit-base-offset>.log
  broker-state/
    events.jsonl
```

`<root>/messages` is owned by `msg-storage::PartitionLog`. It stores immutable message records with topic, partition, offset, and envelope data. `<root>/broker-state/events.jsonl` is owned by `msg-broker::DurableBroker`. It stores topic metadata and consumer-group delivery transitions as compact JSON Lines.

## Broker-State Events

Each complete line in `events.jsonl` is one JSON object with a `type` field. The current record types are:

- `topic_created`: serialized `Topic` metadata.
- `messages_consumed`: a batch of delivery records containing delivery ID, consumer ID, topic, partition, offset, consumer group, attempt number, delivered-at timestamp, and lease expiry timestamp.
- `message_acked`: delivery ID, consumer ID, topic, partition, offset, consumer group, and ACK timestamp.
- `message_nacked`: delivery metadata, attempt number, timestamp, reason, and an outcome.
- `retry_maintenance_applied`: expired pending delivery outcomes and retry offsets made available.

`message_nacked` and expired delivery outcomes use one of these outcome records:

- `retry_scheduled`: the offset remains unavailable until `ready_at`.
- `dead_lettered`: the offset is moved to DLQ with reason and attempt count.

## Recovery Order

Reopen recovery is deterministic:

1. Read and validate complete broker-state JSONL lines.
2. Truncate and ignore one final incomplete broker-state line without a trailing newline.
3. Replay topic metadata and delivery transitions.
4. Reopen all message partition logs under `<root>/messages`.
5. Reconstruct per-topic round-robin state from recovered unkeyed messages.
6. Rebuild the in-memory idempotency index from durable message records (see
   ADR 0017). Historical duplicates with equivalent intent keep the earliest
   record by canonical order as canonical; conflicting duplicates fail open
   with `DurableBrokerError::Corruption`.
7. Release any remaining pending deliveries so they can be redelivered with the next attempt number.

Message records remain the source of envelopes, offsets, and idempotency state. Broker-state events are the source of topic metadata, pending deliveries, ACKs, retry state, and DLQ state.

## Operation Boundaries

Durable operations persist before exposing or mutating state:

- `publish` succeeds only after the message-log append succeeds.
- `consume` succeeds only after the consumed-delivery event is appended and flushed.
- `ack`, `nack`, and `retry_ready` append and flush their state events before mutating in-memory delivery state.

If a state event append fails, the corresponding in-memory delivery transition is not applied.

## Duplicate And Stale Operations

Delivery IDs identify the current pending delivery attempt. Unknown, duplicate, stale, ACK-after-NACK, and NACK-after-ACK delivery IDs return `BrokerError::DeliveryNotFound`.

Wrong-consumer ACK/NACK attempts still fail as invalid consumer operations while the delivery is pending.

## Retry And DLQ

NACK and lease expiry use the configured retry policy. If the next attempt is within `max_attempts`, the message is retry-scheduled and remains unavailable until `retry_ready(now)` makes it available. If the next delivery would exceed `max_attempts`, the message is moved to DLQ.

DLQ entries include topic, partition, offset, message ID, original envelope, consumer group, reason, attempt count, and timestamp. DLQ offsets are not delivered again and are reconstructed from broker-state plus message records after reopen.

## Corruption Handling

A final incomplete broker-state line is recoverable: it is truncated and ignored. A malformed complete broker-state line is fatal and opens as `DurableBrokerError::StateCorruption`.

Complete but inconsistent state events, such as a duplicate recovered `topic_created` event, open as `DurableBrokerError::Corruption`. Message-log corruption is surfaced through `DurableBrokerError::Storage` using the `msg-storage` recovery contract.

## Known Limitations

- Durability is local and flush-based; explicit fsync policy tuning is deferred.
- There is no broker-state compaction.
- There is no replication, clustering, or consensus.
- Publish-idempotency checks are synchronized only within one broker process;
  there is no cross-process or distributed lock.
- Rebuilding the idempotency index requires a full retained-message-log scan,
  and index memory grows without bound while unique keyed records are retained.
- There is no exactly-once delivery. Publish idempotency via `idempotency_key`
  provides producer-side deduplication of equivalent retries, not consumer-side
  exactly-once processing.
- Consumers must be idempotent because at-least-once redelivery is expected.
