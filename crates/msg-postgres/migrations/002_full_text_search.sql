-- Migration 002: Full-text search over projected metadata.
--
-- The append-only message log remains the source of truth. This migration
-- adds a derived full-text search index over safe projected metadata
-- columns of `ferrumq_messages`.
--
-- Search covers: message_id, topic, event_type, source, subject (optional),
-- and content_type. The field order matches `compute_search_text` in
-- `crates/msg-postgres/src/models.rs`.
--
-- Search does NOT cover: raw payload bytes, payload_sha256, idempotency_key,
-- partition_key, header values, header keys, delivery IDs, consumer IDs,
-- database URLs, or filesystem paths.
--
-- The migration is append-only: migration 001 is not modified, all DDL uses
-- IF NOT EXISTS, and backfill is guarded so it is safe to rerun.

ALTER TABLE ferrumq_messages
  ADD COLUMN IF NOT EXISTS search_text TEXT NOT NULL DEFAULT '';

ALTER TABLE ferrumq_messages
  ADD COLUMN IF NOT EXISTS search_vector TSVECTOR NOT NULL DEFAULT ''::tsvector;

CREATE INDEX IF NOT EXISTS idx_messages_search_vector
  ON ferrumq_messages
  USING GIN (search_vector);

-- Backfill existing rows from safe metadata columns. The `concat_ws` order
-- matches the Rust `compute_search_text` order and skips NULL subject, so
-- rows with and without subject produce equivalent text.
--
-- The WHERE clauses make the backfill safe to rerun: only rows with empty
-- search_text receive a new value, and only rows with an empty vector and
-- non-empty text receive a vector.
UPDATE ferrumq_messages
  SET search_text = concat_ws(' ', message_id, topic, event_type, source, subject, content_type)
  WHERE search_text = '';

UPDATE ferrumq_messages
  SET search_vector = to_tsvector('simple', search_text)
  WHERE search_vector = ''::tsvector AND search_text <> '';
