# ADR 0008: Append-Only Log Storage Foundation

## Status

Accepted.

## Decision

Milestone 3 implements `msg-storage` as an independent synchronous local append-only log for durable message records. It is not wired into `msg-broker` yet.

Each topic partition uses segment files under:

```txt
<root>/topics/<topic>/partitions/<partition-id>/<20-digit-base-offset>.log
```

`TopicName` comes from `msg-core` validation and is used directly for path components; raw unsanitized topic strings are not accepted by the storage API.

Each record frame is:

```txt
u32_le record_length
u32_le crc32(payload)
payload
```

The payload is compact deterministic JSON with `format_version = 1`, topic, partition, offset, and the `MessageEnvelope`. Core message headers already use ordered maps, preserving stable serialized header order.

Segments roll when the active non-empty segment would exceed `max_segment_bytes`. The value is a roll threshold, not a hard record-size limit: a single record larger than the threshold is written to an empty segment.

Recovery scans segment files in base-offset order and validates segment names, topic, partition, offset continuity, frame lengths, JSON decoding, and CRC32 checksums. Only a corrupted or truncated trailing record in the final segment may be truncated and ignored. Corruption in non-final segments or the middle of the final segment returns a storage error.

## Rationale

The project needs durable message-record storage before durable broker delivery semantics. Keeping this milestone as a storage-only crate proves the file layout, record format, checksum policy, segment rolling, and recovery behavior without destabilizing the Milestone 2 in-memory broker.

A simple framed JSON payload keeps the format inspectable while the domain model is still evolving. CRC32 catches torn writes and byte corruption cheaply. Segment base offsets make recovery deterministic and leave room for future indexes that can be rebuilt from the log.

## Consequences

Message records appended through `msg-storage` are durable according to the local filesystem write path and recovery policy. Milestone 3 persists message records only. `msg-broker` still uses its Milestone 2 in-memory vectors until a later milestone wires it to storage.

Durable ACK/NACK state, retry state, consumer cursors, DLQ persistence, broker/storage wiring, indexes, retention, compaction, fsync policy tuning, APIs, background workers, and TypeScript behavior are deferred.
