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
}
