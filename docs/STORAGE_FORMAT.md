# Storage Format

This document describes target storage design. Milestone 3 implements the first durable message-record storage foundation in `msg-storage`.

## In-Memory First

Early broker milestones may use in-memory storage to prove domain behavior and delivery semantics. Milestone 2 `msg-broker` behavior remains in-memory after Milestone 3. In-memory storage is not durable and must be documented as such wherever exposed.

## Append-Only Log Target

Durable storage targets an append-only log per topic partition. Appends are the primary write path. Reads occur by offset. Milestone 3 implements append and read-from-offset for message records in `msg-storage`.

Milestone 3 persists message records only. Durable ACK/NACK state, retry state, consumer cursors, DLQ persistence, broker/storage wiring, indexes, retention, compaction, fsync policy tuning, APIs, and TypeScript behavior are deferred.

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

Segments group contiguous records. Segment names encode 20-digit base offsets. Milestone 3 rotates by `max_segment_bytes` as a roll threshold: if a single record exceeds the threshold, it is still written to an empty segment.

## Offsets

Offsets are monotonically increasing within a partition. Offset assignment happens at append time and is part of the durable record identity.

## Checksums

Milestone 3 records include CRC32 checksums over each JSON payload using `crc32fast`. Recovery validates checksums before exposing records.

## Indexes

Indexes are future work for faster offset lookup. Index rebuild should be possible from log segments so indexes do not become the source of truth.

## Fsync and Durability Policy

Durability policy must be explicit. A publish response can only claim durable success after the configured write and flush requirements are met.

## Crash Recovery

Recovery scans segment files in base-offset order, validates record boundaries, checksums, topic, partition, offset continuity, and JSON decoding, and truncates only a corrupted or truncated trailing record in the final segment. Index rebuild and cursor restoration are future work.

## Corruption Handling

Corruption must not be silently ignored. The broker should identify the affected partition and segment, preserve valid records where possible, and emit structured diagnostics.

## Compaction

Compaction is future work. It must not undermine at-least-once delivery, cursor correctness, or auditability.
