# Storage Format

This document describes target storage design. Milestone 0 does not implement storage.

## In-Memory First

Early broker milestones may use in-memory storage to prove domain behavior and delivery semantics. In-memory storage is not durable and must be documented as such wherever exposed.

## Append-Only Log Target

Durable storage targets an append-only log per topic partition. Appends are the primary write path. Reads occur by offset.

## Directory Layout

Future durable storage should use a layout similar to:

```txt
data/
  topics/
    <topic>/
      partitions/
        <partition-id>/
          segments/
            00000000000000000000.log
            00000000000000000000.index
          cursors/
            <consumer-group>.cursor
```

## Segment Files

Segments group contiguous records. Segment names should encode base offsets. Segment rotation should be based on size, age, or explicit maintenance policy.

## Offsets

Offsets are monotonically increasing within a partition. Offset assignment happens at append time and is part of the durable record identity.

## Checksums

Future records should include checksums, such as `crc32fast`, to detect corruption and partial writes. Recovery must validate checksums before exposing records.

## Indexes

Indexes are future work for faster offset lookup. Index rebuild should be possible from log segments so indexes do not become the source of truth.

## Fsync and Durability Policy

Durability policy must be explicit. A publish response can only claim durable success after the configured write and flush requirements are met.

## Crash Recovery

Recovery should scan segment files, validate record boundaries and checksums, rebuild indexes, restore cursors, and truncate or quarantine partial records according to policy.

## Corruption Handling

Corruption must not be silently ignored. The broker should identify the affected partition and segment, preserve valid records where possible, and emit structured diagnostics.

## Compaction

Compaction is future work. It must not undermine at-least-once delivery, cursor correctness, or auditability.
