# ADR 0009: Durable Broker Delivery State

## Status

Accepted.

## Decision

Milestone 4 adds `DurableBroker` as a separate synchronous broker API in `msg-broker`. `BrokerService` remains the in-memory implementation and is not generalized or replaced.

`DurableBroker` stores durable message records through `msg-storage::PartitionLog` under:

```txt
<root>/messages/topics/<topic>/partitions/<partition-id>/<20-digit-base-offset>.log
```

`DurableBroker` stores topic metadata and delivery transitions in an append-only compact JSONL log under:

```txt
<root>/broker-state/events.jsonl
```

The broker-state log records topic creation, consumed delivery batches, ACKs, NACK retry/DLQ outcomes, and retry maintenance batches. Each event is flushed before `DurableBroker` mutates in-memory delivery state or returns success for that operation.

Recovery replays the broker-state log, reopens all message partition logs, reconstructs round-robin state from recovered unkeyed message count, and rebuilds consumer group delivery state. Successfully published messages are recoverable after broker reopen. Successfully ACKed messages are not redelivered after broker reopen. Remaining pending deliveries are treated as crash-recovered unACKed work: they are removed from pending state, their attempt count is retained, and they are immediately eligible for at-least-once redelivery with a new deterministic delivery ID.

A final incomplete broker-state JSONL line without a trailing newline may be truncated and ignored. Any malformed complete broker-state event is a typed durable broker corruption error. Message-log corruption follows the `msg-storage` contract.

## Rationale

Message records and delivery state have different durability lifecycles. Message records are immutable partition facts keyed by topic, partition, and offset. Delivery state is consumer-group-specific and changes as messages are consumed, ACKed, NACKed, retried, expired, or dead-lettered.

Keeping delivery transitions in a small append-only broker-state log lets Milestone 4 add local durable at-least-once delivery without changing the Milestone 3 message segment format or replacing the Milestone 2 in-memory broker. It also keeps recovery deterministic and inspectable while indexes, compaction, fsync policy tuning, and distributed replication remain deferred.

## Consequences

`DurableBroker` provides local filesystem durable at-least-once delivery only. Consumers must be idempotent because unACKed messages may be redelivered after reopen and duplicates are allowed.

This milestone does not include HTTP/gRPC APIs, CLI/TUI broker semantics, runtime daemon behavior, background retry workers, clustering, replication, consensus, or exactly-once delivery.

The JSONL broker-state log may grow until future retention or compaction work is added. Broker-state fsync policy remains tied to the current flush-based local write path.
