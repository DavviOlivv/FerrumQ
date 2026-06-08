# Storage Format

This document describes the storage design, the Milestone 3 on-disk message-record format implemented in `msg-storage`, and the Milestone 4 local durable broker-state log used by `DurableBroker`. The formats are internal to FerrumQ while early milestones are still evolving; `format_version` exists for message records so future migrations can be explicit rather than accidental.

## In-Memory First

Early broker milestones may use in-memory storage to prove domain behavior and delivery semantics. `BrokerService` remains the in-memory broker and is not durable. Milestone 4 adds `DurableBroker` as a separate API for local filesystem durable at-least-once delivery.

## Append-Only Log Target

Durable storage targets an append-only log per topic partition. Appends are the primary write path. Reads occur by offset. Milestone 3 implements append and read-from-offset for message records in `msg-storage`.

Message records and delivery state are separate durable concerns. Milestone 4 stores durable message records in `msg-storage` logs under `<root>/messages` and stores topic/delivery transitions in a separate JSONL broker-state log under `<root>/broker-state/events.jsonl`. This provides local durable at-least-once delivery after broker reopen, not replicated cluster durability.

## Directory Layout

Milestone 4 durable broker storage uses:

```txt
data/
  messages/
    topics/
      <topic>/
        partitions/
          <partition-id>/
            00000000000000000000.log
            00000000000000000128.log
  broker-state/
    events.jsonl
```

Index files, cursor files, retention metadata, compaction metadata, and replicated consensus metadata are future work.

## Segment Files

Segments group contiguous records. Segment names encode fixed 20-digit base offsets and must use the `.log` extension. Names that are not exactly 20 ASCII digits, including unpadded names such as `2.log` or `10.log`, are invalid.

Milestone 3 discovers segment files by parsing their numeric base offsets and sorting by that parsed value. Recovery rejects gaps or out-of-sequence bases. Empty final segments are allowed and can be reused by the next append; empty non-final segments are corruption.

Milestone 3 rotates by `max_segment_bytes` as a roll threshold: if a single record exceeds the threshold, it is still written to an empty segment.

## Frame Layout

Each record frame is:

```txt
u32_le record_length
u32_le crc32(payload)
payload
```

`record_length` is the byte length of the JSON payload. The CRC32 is computed over the payload bytes. The payload is compact JSON with:

- `format_version = 1`.
- `topic`.
- `partition`.
- `offset`.
- `envelope`.

## Offsets

The first successful append within a partition receives offset `0`. Successful appends are monotonically increasing and gapless within that partition. Offset assignment happens at append time and is part of the durable record identity.

`read_from(next_offset, _)`, reads past the end, and `limit == 0` return an empty result. A failed append must not advance the in-memory next offset or the next offset recovered from disk. The implementation attempts to truncate back to the pre-append segment length when a write or flush reports failure after bytes may have reached the file.

## Checksums

Milestone 3 records include CRC32 checksums over each JSON payload using `crc32fast`. Recovery validates checksums before exposing records.

## Indexes

Indexes are future work for faster offset lookup. Index rebuild should be possible from log segments so indexes do not become the source of truth.

## Fsync and Durability Policy

Durability policy must be explicit. Milestone 3 calls `flush()` after writing each frame. Explicit fsync policy, group commit, and latency/durability tuning remain deferred. A future publish response can only claim durable success after the configured write and flush requirements are met.

Milestone 4 broker-state events are written as compact JSON objects followed by `\n` and flushed before `DurableBroker` mutates in-memory delivery state or returns success for the corresponding operation. This is still local filesystem durability and does not imply replication or power-loss guarantees beyond the current flush policy.

## Broker-State JSONL

`DurableBroker` stores broker metadata and delivery transitions in `<root>/broker-state/events.jsonl`. Each complete line is one compact JSON object with a `type` field. Event types include:

- `topic_created` with a serialized `Topic`.
- `messages_consumed` with a batch of delivery records.
- `message_acked` with delivery, consumer, topic, partition, offset, group, and timestamp metadata.
- `message_nacked` with delivery metadata, attempt number, reason, timestamp, and either a `retry_scheduled` or `dead_lettered` outcome.
- `retry_maintenance_applied` with expired pending delivery outcomes and retry entries made available.

On reopen, `DurableBroker` replays the broker-state log, reopens message partition logs, reconstructs round-robin state from recovered unkeyed message count, and releases any still-pending delivery for immediate at-least-once redelivery while preserving its attempt count. Successfully ACKed messages are not redelivered after reopen. UnACKed messages may be redelivered after reopen, so consumers must be idempotent.

## Crash Recovery

Recovery scans segment files in base-offset order, validates record boundaries, checksums, topic, partition, offset continuity, and JSON decoding, and truncates only trailing damage in the final segment. Repair always truncates to the start byte of the damaged record, preserving earlier valid records and discarding the damaged trailing bytes. `DurableBroker` may also truncate and ignore one final incomplete broker-state JSONL line without a trailing newline. Any malformed complete broker-state event is a typed durable broker corruption error.

## Corruption Handling

Corruption must not be silently ignored. Truncated final length, checksum header, or payload bytes; extra trailing bytes after a valid final record; final checksum mismatch; final invalid JSON; and final record metadata mismatch are treated as repairable final trailing damage. Checksum mismatches, invalid JSON, invalid metadata, empty segments, or offset discontinuities in non-final segments or in the middle of the final segment return typed storage errors and do not expose later records as valid.

The broker should identify the affected partition and segment, preserve valid records where possible, and emit structured diagnostics once broker/runtime wiring exists.

## Compaction

Compaction is future work. It must not undermine at-least-once delivery, cursor correctness, or auditability.
