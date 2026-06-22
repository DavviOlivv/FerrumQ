-- Migration 001: Initial PostgreSQL metadata schema for FerrumQ.
--
-- The append-only message log remains the source of truth. This schema
-- stores derived metadata and projections for query, search, dashboards,
-- and operational tools.
--
-- PostgreSQL is optional. Broker correctness never depends on it.

CREATE TABLE IF NOT EXISTS ferrumq_topics (
    name            TEXT PRIMARY KEY,
    partitions      INTEGER NOT NULL CHECK (partitions > 0),
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ferrumq_messages (
    topic           TEXT NOT NULL,
    partition_id    INTEGER NOT NULL CHECK (partition_id >= 0),
    message_offset  BIGINT NOT NULL CHECK (message_offset >= 0),
    message_id      TEXT NOT NULL,
    idempotency_key TEXT,
    partition_key   TEXT,
    payload_len     BIGINT NOT NULL CHECK (payload_len >= 0),
    payload_sha256  TEXT NOT NULL
        CHECK (
            char_length(payload_sha256) = 64
            AND payload_sha256 ~ '^[0-9a-f]{64}$'
        ),
    content_type    TEXT NOT NULL DEFAULT '',
    event_type      TEXT NOT NULL DEFAULT '',
    source          TEXT NOT NULL DEFAULT '',
    subject         TEXT,
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    time_unix_ms    BIGINT NOT NULL,
    indexed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition_id, message_offset),
    -- A message_id must be unique within a topic. If a projection encounters
    -- the same message_id at a different (partition, message_offset), that is
    -- a data integrity issue and the projection must fail rather than
    -- silently choosing one row.
    CONSTRAINT ferrumq_messages_topic_message_id_key
        UNIQUE (topic, message_id)
);

CREATE INDEX IF NOT EXISTS idx_ferrumq_messages_topic
    ON ferrumq_messages (topic);
CREATE INDEX IF NOT EXISTS idx_ferrumq_messages_time
    ON ferrumq_messages (time_unix_ms);

CREATE TABLE IF NOT EXISTS ferrumq_projection_runs (
    id              BIGSERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    topics_count    INTEGER NOT NULL DEFAULT 0 CHECK (topics_count >= 0),
    messages_count  INTEGER NOT NULL DEFAULT 0 CHECK (messages_count >= 0),
    status          TEXT NOT NULL DEFAULT 'in_progress'
        CHECK (status IN ('in_progress', 'success', 'error')),
    error_message   TEXT,
    CHECK (
        (
            status = 'in_progress'
            AND completed_at IS NULL
            AND error_message IS NULL
        )
        OR (
            status = 'success'
            AND completed_at IS NOT NULL
            AND error_message IS NULL
        )
        OR (
            status = 'error'
            AND completed_at IS NOT NULL
            AND error_message IS NOT NULL
        )
    )
);
