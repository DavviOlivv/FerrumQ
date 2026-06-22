use std::collections::BTreeMap;

use msg_storage::StoredMessageRecord;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::PostgresError;

/// Row projected into `ferrumq_topics`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRow {
    pub name: String,
    pub partitions: i32,
    pub first_seen_at: chrono::DateTime<chrono::Utc>,
    pub last_seen_at: chrono::DateTime<chrono::Utc>,
}

/// Row projected into `ferrumq_messages`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRow {
    pub topic: String,
    pub partition_id: i32,
    pub offset: i64,
    pub message_id: String,
    pub idempotency_key: Option<String>,
    pub partition_key: Option<String>,
    pub payload_len: i64,
    pub payload_sha256: String,
    pub content_type: String,
    pub event_type: String,
    pub source: String,
    pub subject: Option<String>,
    pub headers: serde_json::Value,
    pub time_unix_ms: i64,
}

/// Result of a projection run.
#[derive(Debug, Clone)]
pub struct ProjectionResult {
    pub topics_count: usize,
    pub messages_count: usize,
}

/// Query parameters for a full-text search over projected message metadata.
///
/// The query string is validated before reaching the database: empty, blank,
/// and punctuation-only inputs are rejected. `limit` must be in `1..=100`.
///
/// Fields are private so invariants established by [`SearchQuery::new`] cannot
/// be bypassed by direct construction. Use the accessors to read individual
/// fields.
#[derive(Debug, Clone)]
pub struct SearchQuery {
    query: String,
    topic: Option<String>,
    limit: u32,
}

impl SearchQuery {
    /// Creates a validated search query. Returns an error for empty, blank,
    /// or punctuation-only query strings, and for limits outside `1..=100`.
    ///
    /// This is the only constructor. Direct struct literal construction is
    /// prevented by the private fields.
    pub fn new(
        query: impl Into<String>,
        topic: Option<String>,
        limit: u32,
    ) -> Result<Self, crate::PostgresError> {
        let query = query.into();
        validate_search_query_text(&query)?;
        if !(1..=100).contains(&limit) {
            return Err(crate::PostgresError::InvalidSearchLimit);
        }
        Ok(Self {
            query,
            topic,
            limit,
        })
    }

    /// Returns the validated query string.
    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Returns the optional topic filter.
    #[must_use]
    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    /// Returns the validated result limit.
    #[must_use]
    pub fn limit(&self) -> u32 {
        self.limit
    }
}

/// A single row returned by `PostgresRepository::search_messages`.
///
/// The struct intentionally does not expose `idempotency_key` or raw payload
/// bytes. Only safe projected metadata and the FTS `rank` are returned.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SearchResult {
    pub topic: String,
    pub partition_id: i32,
    #[sqlx(rename = "message_offset")]
    pub offset: i64,
    pub message_id: String,
    pub event_type: String,
    pub source: String,
    pub subject: Option<String>,
    pub content_type: String,
    pub time_unix_ms: i64,
    pub payload_len: i64,
    pub payload_sha256: String,
    pub rank: f32,
}

/// Serializes search results to pretty-printed JSON for CLI output.
///
/// Returns `SearchResultSerializationFailed` if the result set cannot be
/// serialized. The current `SearchResult` field set always serializes
/// successfully, but the helper is defined so the CLI can return a
/// non-zero exit code if that contract ever changes.
pub fn serialize_search_results_json(results: &[SearchResult]) -> Result<String, PostgresError> {
    serde_json::to_string_pretty(results).map_err(PostgresError::SearchResultSerializationFailed)
}

/// Computes the SHA-256 hex digest of payload bytes.
#[must_use]
pub fn compute_payload_sha256(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hex::encode(hasher.finalize())
}

/// Converts a `StoredMessageRecord` into a `MessageRow`.
pub fn record_to_message_row(record: &StoredMessageRecord) -> Result<MessageRow, PostgresError> {
    let envelope = &record.envelope;
    let headers = envelope_headers_to_json(envelope.headers());

    Ok(MessageRow {
        topic: record.topic.as_str().to_owned(),
        partition_id: i32::try_from(record.partition.value()).map_err(|_| {
            PostgresError::ProjectionValueOutOfRange {
                field: "partition_id",
            }
        })?,
        offset: i64::try_from(record.offset.value()).map_err(|_| {
            PostgresError::ProjectionValueOutOfRange {
                field: "message_offset",
            }
        })?,
        message_id: envelope.id().as_str().to_owned(),
        idempotency_key: envelope.idempotency_key().map(|k| k.as_str().to_owned()),
        partition_key: envelope.partition_key().map(|k| k.as_str().to_owned()),
        payload_len: i64::try_from(envelope.payload().as_bytes().len()).map_err(|_| {
            PostgresError::ProjectionValueOutOfRange {
                field: "payload_len",
            }
        })?,
        payload_sha256: compute_payload_sha256(envelope.payload().as_bytes()),
        content_type: envelope.content_type().as_str().to_owned(),
        event_type: envelope.event_type().as_str().to_owned(),
        source: envelope.source().as_str().to_owned(),
        subject: envelope.subject().map(|s| s.as_str().to_owned()),
        headers,
        time_unix_ms: i64::try_from(envelope.timestamp().as_unix_millis()).map_err(|_| {
            PostgresError::ProjectionValueOutOfRange {
                field: "time_unix_ms",
            }
        })?,
    })
}

fn envelope_headers_to_json(headers: &msg_core::MessageHeaders) -> serde_json::Value {
    let mut map = BTreeMap::new();
    for (name, value) in headers.iter() {
        map.insert(
            name.as_str().to_owned(),
            serde_json::Value::String(value.as_str().to_owned()),
        );
    }
    serde_json::Value::Object(serde_json::Map::from_iter(map))
}

/// Derives safe search text from projected metadata fields.
///
/// The field order matches the SQL backfill expression in
/// `migrations/002_full_text_search.sql`:
/// `concat_ws(' ', message_id, topic, event_type, source, subject, content_type)`.
///
/// `concat_ws` skips NULL `subject`, matching the conditional push below.
/// `content_type` is always included (it is non-null in the schema).
///
/// Searched fields:
/// - `message_id`, `topic`, `event_type`, `source`, `subject` (optional),
///   `content_type`.
///
/// Explicitly NOT searched:
/// - raw payload bytes, `payload_sha256`, `idempotency_key`, `partition_key`,
///   header keys/values, `time_unix_ms`.
#[must_use]
pub fn compute_search_text(row: &MessageRow) -> String {
    let mut parts: Vec<&str> = vec![&row.message_id, &row.topic, &row.event_type, &row.source];
    if let Some(subject) = &row.subject {
        parts.push(subject.as_str());
    }
    parts.push(&row.content_type);
    parts.join(" ")
}

/// Validates a user-provided search query string. Rejects empty, blank,
/// and inputs that contain no alphanumeric characters (e.g. punctuation-only
/// or operator-only strings) because they would normalize to an empty
/// `websearch_to_tsquery` and produce confusing empty results or database
/// warnings.
pub fn validate_search_query_text(query: &str) -> Result<(), crate::PostgresError> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Err(crate::PostgresError::EmptySearchQuery);
    }
    let has_alphanumeric = trimmed.chars().any(|c| c.is_alphanumeric());
    if !has_alphanumeric {
        return Err(crate::PostgresError::EmptySearchQuery);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use msg_core::{
        ContentType, EventSource, EventSubject, EventType, IdempotencyKey, MessageEnvelope,
        MessageId, MessagePayload, MessageTimestamp, Offset, PartitionId, PartitionKey, TopicName,
    };

    use super::*;

    fn sample_envelope() -> MessageEnvelope {
        MessageEnvelope::builder(
            MessageId::new("msg-1").unwrap(),
            EventSource::new("/tests").unwrap(),
            EventType::new("order.created").unwrap(),
            ContentType::new("application/json").unwrap(),
            MessageTimestamp::from_unix_millis(1_700_000_000_000),
            MessagePayload::from_bytes(br#"{"ok":true}"#),
        )
        .partition_key(PartitionKey::new("key-1").unwrap())
        .idempotency_key(IdempotencyKey::new("idem-1").unwrap())
        .subject(EventSubject::new("order/123").unwrap())
        .build()
    }

    fn sample_record() -> StoredMessageRecord {
        StoredMessageRecord {
            topic: TopicName::new("orders").unwrap(),
            partition: PartitionId::new(0),
            offset: Offset::new(42),
            envelope: sample_envelope(),
        }
    }

    #[test]
    fn computes_sha256_hex() {
        let hash = compute_payload_sha256(br#"hello"#);
        assert_eq!(hash.len(), 64);
        // known SHA-256 of "hello"
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn maps_record_to_row() {
        let row = record_to_message_row(&sample_record()).unwrap();
        assert_eq!(row.topic, "orders");
        assert_eq!(row.partition_id, 0);
        assert_eq!(row.offset, 42);
        assert_eq!(row.message_id, "msg-1");
        assert_eq!(row.idempotency_key.as_deref(), Some("idem-1"));
        assert_eq!(row.partition_key.as_deref(), Some("key-1"));
        assert_eq!(row.payload_len, 11);
        assert_eq!(row.payload_sha256.len(), 64);
        assert_eq!(row.content_type, "application/json");
        assert_eq!(row.event_type, "order.created");
        assert_eq!(row.source, "/tests");
        assert_eq!(row.subject.as_deref(), Some("order/123"));
        assert_eq!(row.time_unix_ms, 1_700_000_000_000);
    }

    #[test]
    fn maps_record_without_optional_fields() {
        let envelope = MessageEnvelope::builder(
            MessageId::new("msg-2").unwrap(),
            EventSource::new("/test").unwrap(),
            EventType::new("event").unwrap(),
            ContentType::new("text/plain").unwrap(),
            MessageTimestamp::from_unix_millis(1000),
            MessagePayload::from_bytes(b"data"),
        )
        .build();

        let record = StoredMessageRecord {
            topic: TopicName::new("test-topic").unwrap(),
            partition: PartitionId::new(1),
            offset: Offset::new(0),
            envelope,
        };

        let row = record_to_message_row(&record).unwrap();
        assert!(row.idempotency_key.is_none());
        assert!(row.partition_key.is_none());
        assert!(row.subject.is_none());
        assert_eq!(row.headers, serde_json::json!({}));
    }

    #[test]
    fn payload_sha256_is_deterministic() {
        let payload = b"some deterministic payload";
        let a = compute_payload_sha256(payload);
        let b = compute_payload_sha256(payload);
        assert_eq!(a, b);
    }

    #[test]
    fn different_payloads_different_hashes() {
        let a = compute_payload_sha256(b"payload-a");
        let b = compute_payload_sha256(b"payload-b");
        assert_ne!(a, b);
    }

    fn build_test_row(subject: Option<&str>) -> MessageRow {
        MessageRow {
            topic: "orders".to_owned(),
            partition_id: 0,
            offset: 0,
            message_id: "msg-1".to_owned(),
            idempotency_key: Some("idem-1".to_owned()),
            partition_key: Some("key-1".to_owned()),
            payload_len: 11,
            payload_sha256: compute_payload_sha256(br#"{"ok":true}"#),
            content_type: "application/json".to_owned(),
            event_type: "order.created".to_owned(),
            source: "/tests".to_owned(),
            subject: subject.map(str::to_owned),
            headers: serde_json::json!({}),
            time_unix_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn compute_search_text_with_subject_matches_sql_concat_ws_order() {
        let row = build_test_row(Some("order/123"));
        assert_eq!(
            compute_search_text(&row),
            "msg-1 orders order.created /tests order/123 application/json"
        );
    }

    #[test]
    fn compute_search_text_without_subject_matches_sql_concat_ws_order() {
        let row = build_test_row(None);
        assert_eq!(
            compute_search_text(&row),
            "msg-1 orders order.created /tests application/json"
        );
    }

    #[test]
    fn compute_search_text_excludes_idempotency_key_and_payload_metadata() {
        let row = build_test_row(Some("order/123"));
        let text = compute_search_text(&row);
        assert!(!text.contains("idem-1"));
        assert!(!text.contains("key-1"));
        assert!(!text.contains("payload-secret"));
        assert!(!text.contains(&row.payload_sha256));
    }

    #[test]
    fn validate_search_query_text_rejects_empty_and_blank() {
        assert!(matches!(
            validate_search_query_text(""),
            Err(crate::PostgresError::EmptySearchQuery)
        ));
        assert!(matches!(
            validate_search_query_text("   "),
            Err(crate::PostgresError::EmptySearchQuery)
        ));
        assert!(matches!(
            validate_search_query_text("\t\n  "),
            Err(crate::PostgresError::EmptySearchQuery)
        ));
    }

    #[test]
    fn validate_search_query_text_rejects_punctuation_only() {
        for input in [
            "...", "!!!", "@@@", "---", "+++", "((()))", ".. ..", "@ - +",
        ] {
            assert!(
                matches!(
                    validate_search_query_text(input),
                    Err(crate::PostgresError::EmptySearchQuery)
                ),
                "expected {input:?} to be rejected"
            );
        }
    }

    #[test]
    fn validate_search_query_text_accepts_alphanumeric_queries() {
        for input in [
            "order",
            "order created",
            "payment-123",
            "OR order AND created",
            "user@host query",
            "café",
        ] {
            assert!(
                validate_search_query_text(input).is_ok(),
                "expected {input:?} to be accepted"
            );
        }
    }

    #[test]
    fn search_query_new_validates_query_and_limit() {
        assert!(SearchQuery::new("order", None, 20).is_ok());
        assert!(matches!(
            SearchQuery::new("", None, 20),
            Err(crate::PostgresError::EmptySearchQuery)
        ));
        assert!(matches!(
            SearchQuery::new("!!!", None, 20),
            Err(crate::PostgresError::EmptySearchQuery)
        ));
        assert!(matches!(
            SearchQuery::new("order", None, 0),
            Err(crate::PostgresError::InvalidSearchLimit)
        ));
        assert!(matches!(
            SearchQuery::new("order", None, 101),
            Err(crate::PostgresError::InvalidSearchLimit)
        ));
    }

    #[test]
    fn search_query_accessors_return_validated_values() {
        let query = SearchQuery::new("order created", Some("orders".to_owned()), 25).unwrap();
        assert_eq!(query.query(), "order created");
        assert_eq!(query.topic(), Some("orders"));
        assert_eq!(query.limit(), 25);
    }

    fn sample_search_result() -> SearchResult {
        SearchResult {
            topic: "orders".to_owned(),
            partition_id: 0,
            offset: 42,
            message_id: "msg-1".to_owned(),
            event_type: "order.created".to_owned(),
            source: "/tests".to_owned(),
            subject: Some("order/1".to_owned()),
            content_type: "application/json".to_owned(),
            time_unix_ms: 1_700_000_000_000,
            payload_len: 11,
            payload_sha256: "0".repeat(64),
            rank: 0.5,
        }
    }

    #[test]
    fn serialize_search_results_json_succeeds_for_normal_results() {
        let results = vec![sample_search_result()];
        let text = serialize_search_results_json(&results).unwrap();
        assert!(text.contains("\"topic\": \"orders\""));
        assert!(text.contains("\"message_id\": \"msg-1\""));
        assert!(!text.contains("idempotency_key"));
    }

    #[test]
    fn search_result_serialization_failure_error_has_sanitized_message() {
        let source = serde_json::Error::io(std::io::Error::other("simulated"));
        let error = crate::PostgresError::SearchResultSerializationFailed(source);
        let rendered = error.to_string();
        assert!(rendered.contains("search result serialization failed"));
        assert!(!rendered.contains("idempotency"));
        assert!(!rendered.contains("password"));
    }
}
