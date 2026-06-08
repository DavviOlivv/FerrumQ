# Storage Format

This document describes the storage design and the Milestone 3 on-disk message-record format implemented in `msg-storage`. The format is internal to FerrumQ while early milestones are still evolving; `format_version` exists so future migrations can be explicit rather than accidental.

## In-Memory First

Early broker milestones may use in-memory storage to prove domain behavior and delivery semantics. Milestone 2 `msg-broker` behavior remains in-memory after Milestone 3. In-memory storage is not durable and must be documented as such wherever exposed.

## Append-Only Log Target

Durable storage targets an append-only log per topic partition. Appends are the primary write path. Reads occur by offset. Milestone 3 implements append and read-from-offset for message records in `msg-storage`.

Milestone 3 persists message records only. Durable ACK/NACK state, retry state, consumer cursors, pending delivery state, DLQ persistence, broker/storage wiring, indexes, retention, compaction, fsync policy tuning, APIs, and TypeScript behavior are deferred.

## Directory Layout

Future durable storage should use a layout similar to:

```txt
data/
  topics/
    <topic>/
      partitions/
        <partition-id>/
          00000000000000000000.log
          00000000000000000128.log
          cursors/
            <consumer-group>.cursor
```

Milestone 3 implements only the `.log` files. Index files and cursor files are future work.

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

## Crash Recovery

Recovery scans segment files in base-offset order, validates record boundaries, checksums, topic, partition, offset continuity, and JSON decoding, and truncates only trailing damage in the final segment. Repair always truncates to the start byte of the damaged record, preserving earlier valid records and discarding the damaged trailing bytes. Index rebuild and cursor restoration are future work.

## Corruption Handling

Corruption must not be silently ignored. Truncated final length, checksum header, or payload bytes; extra trailing bytes after a valid final record; final checksum mismatch; final invalid JSON; and final record metadata mismatch are treated as repairable final trailing damage. Checksum mismatches, invalid JSON, invalid metadata, empty segments, or offset discontinuities in non-final segments or in the middle of the final segment return typed storage errors and do not expose later records as valid.

The broker should identify the affected partition and segment, preserve valid records where possible, and emit structured diagnostics once broker/runtime wiring exists.

## Compaction

Compaction is future work. It must not undermine at-least-once delivery, cursor correctness, or auditability.
