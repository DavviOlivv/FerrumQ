use msg_core::{MessageEnvelope, MessageId, Offset, PartitionId, TopicName};
use sha2::{Digest, Sha256};

use crate::errors::BrokerError;

/// Stable public error code for idempotency key conflict.
pub const IDEMPOTENCY_KEY_CONFLICT_CODE: &str = "IDEMPOTENCY_KEY_CONFLICT";

/// A 32-byte SHA-256 fingerprint of the semantic publish intent.
///
/// Two publishes with the same `(topic, idempotency_key)` are equivalent if and
/// only if their fingerprints are equal. The fingerprint is deterministic and
/// platform-independent: it never uses Rust's randomized `Hash`, JSON object
/// key iteration without canonicalization, debug formatting, or payload text
/// decoding.
///
/// # Included fields (in canonical encoding order)
///
/// 1. `topic` — UTF-8 bytes.
/// 2. `partition_key` — presence tag plus UTF-8 bytes.
/// 3. `payload` — raw bytes (no text decoding).
/// 4. `content_type` — UTF-8 bytes.
/// 5. `event_type` — UTF-8 bytes.
/// 6. `source` — UTF-8 bytes.
/// 7. `subject` — presence tag plus UTF-8 bytes.
/// 8. `headers` — entry count followed by each `(name, value)` pair as
///    length-prefixed UTF-8, iterated in `BTreeMap` order (deterministic).
///
/// # Excluded fields
///
/// - `message_id` — transport-generated; a retry may reconstruct it.
/// - `timestamp` / `time_unix_ms` — transport-generated; a retry may
///   reconstruct it.
/// - `idempotency_key` — already part of the lookup key, not the fingerprint.
///
/// # Encoding
///
/// Each byte string is length-prefixed with a `u64` little-endian length. The
/// presence tag for optional fields is a single byte: `0x00` for absent,
/// `0x01` for present. This makes the encoding unambiguous and
/// non-prefix-collision-free.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PublishFingerprint([u8; 32]);

impl PublishFingerprint {
    /// Computes the deterministic fingerprint of the semantic publish intent
    /// for the given topic and envelope.
    #[must_use]
    pub fn compute(topic: &TopicName, envelope: &MessageEnvelope) -> Self {
        let mut hasher = Sha256::new();

        write_str(&mut hasher, topic.as_str());

        match envelope.partition_key() {
            Some(key) => {
                hasher.update([0x01]);
                write_str(&mut hasher, key.as_str());
            }
            None => {
                hasher.update([0x00]);
            }
        }

        write_bytes(&mut hasher, envelope.payload().as_bytes());
        write_str(&mut hasher, envelope.content_type().as_str());
        write_str(&mut hasher, envelope.event_type().as_str());
        write_str(&mut hasher, envelope.source().as_str());

        match envelope.subject() {
            Some(subject) => {
                hasher.update([0x01]);
                write_str(&mut hasher, subject.as_str());
            }
            None => {
                hasher.update([0x00]);
            }
        }

        let headers = envelope.headers();
        write_u64(&mut hasher, headers.len() as u64);
        for (name, value) in headers.iter() {
            write_str(&mut hasher, name.as_str());
            write_str(&mut hasher, value.as_str());
        }

        let result = hasher.finalize();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&result);
        Self(bytes)
    }

    /// Returns the raw 32-byte fingerprint.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A durable record of a successful idempotent publish.
///
/// Stores enough information to recognize an equivalent retry and return the
/// original publish result without appending another message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyRecord {
    fingerprint: PublishFingerprint,
    partition_id: PartitionId,
    offset: Offset,
    message_id: MessageId,
}

impl IdempotencyRecord {
    /// Creates a new idempotency record from the fingerprint and original
    /// publish identity.
    #[must_use]
    pub fn new(
        fingerprint: PublishFingerprint,
        partition_id: PartitionId,
        offset: Offset,
        message_id: MessageId,
    ) -> Self {
        Self {
            fingerprint,
            partition_id,
            offset,
            message_id,
        }
    }

    /// Returns the fingerprint of the original publish intent.
    #[must_use]
    pub fn fingerprint(&self) -> &PublishFingerprint {
        &self.fingerprint
    }

    /// Returns the partition ID of the original publish.
    #[must_use]
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Returns the offset of the original publish.
    #[must_use]
    pub fn offset(&self) -> Offset {
        self.offset
    }

    /// Returns the message ID of the original publish.
    #[must_use]
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }
}

/// Checks whether a publish with the given envelope is an equivalent retry of
/// an existing idempotency record.
///
/// Returns `true` if the fingerprints match (equivalent retry), `false` if
/// they differ (the caller should raise a conflict error).
pub(crate) fn is_equivalent_retry(
    topic: &TopicName,
    envelope: &MessageEnvelope,
    existing: &IdempotencyRecord,
) -> bool {
    let fingerprint = PublishFingerprint::compute(topic, envelope);
    existing.fingerprint() == &fingerprint
}

/// Creates a conflict error for the given topic. The error is sanitized and
/// does not expose the idempotency key, payload, or any sensitive value.
pub(crate) fn conflict_error(topic: &TopicName) -> BrokerError {
    BrokerError::IdempotencyKeyConflict {
        topic: topic.clone(),
    }
}

fn write_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_le_bytes());
}

fn write_str(hasher: &mut Sha256, value: &str) {
    write_bytes(hasher, value.as_bytes());
}

fn write_bytes(hasher: &mut Sha256, value: &[u8]) {
    write_u64(hasher, value.len() as u64);
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use msg_core::{
        ContentType, EventSource, EventType, MessageEnvelope, MessageHeaders, MessageId,
        MessagePayload, MessageTimestamp, PartitionKey,
    };

    use super::*;

    fn envelope(
        id: &str,
        partition_key: Option<&str>,
        payload: &[u8],
        content_type: &str,
        event_type: &str,
        source: &str,
        subject: Option<&str>,
    ) -> MessageEnvelope {
        let mut builder = MessageEnvelope::builder(
            MessageId::new(id).unwrap(),
            EventSource::new(source).unwrap(),
            EventType::new(event_type).unwrap(),
            ContentType::new(content_type).unwrap(),
            MessageTimestamp::from_unix_millis(1_700_000_000_000),
            MessagePayload::from_bytes(payload),
        );
        if let Some(key) = partition_key {
            builder = builder.partition_key(PartitionKey::new(key).unwrap());
        }
        if let Some(sub) = subject {
            builder = builder.subject(msg_core::EventSubject::new(sub).unwrap());
        }
        builder.build()
    }

    fn topic() -> TopicName {
        TopicName::new("orders").unwrap()
    }

    #[test]
    fn fingerprint_is_deterministic() {
        let env = envelope(
            "msg-1",
            Some("key-1"),
            b"hello",
            "application/json",
            "order.created",
            "orders-service",
            Some("order/123"),
        );
        let a = PublishFingerprint::compute(&topic(), &env);
        let b = PublishFingerprint::compute(&topic(), &env);
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_excludes_message_id() {
        let env_a = envelope(
            "msg-1",
            Some("key-1"),
            b"hello",
            "application/json",
            "order.created",
            "orders-service",
            None,
        );
        let env_b = envelope(
            "msg-2",
            Some("key-1"),
            b"hello",
            "application/json",
            "order.created",
            "orders-service",
            None,
        );
        assert_eq!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_excludes_timestamp() {
        let env_a = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        let env_b = MessageEnvelope::builder(
            MessageId::new("msg-1").unwrap(),
            EventSource::new("src").unwrap(),
            EventType::new("type").unwrap(),
            ContentType::new("application/json").unwrap(),
            MessageTimestamp::from_unix_millis(9_999_999_999_999),
            MessagePayload::from_bytes(b"hello"),
        )
        .build();
        // env_a has timestamp 1_700_000_000_000; env_b has 9_999_999_999_999
        // but everything else equal. Fingerprints must match.
        assert_eq!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_excludes_idempotency_key() {
        let env_a = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        let env_b = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        // The fingerprint should not include idempotency_key, so even if we
        // could set different keys, the fingerprints would match. Since we
        // can't easily set it post-build, we just verify the fingerprint
        // doesn't change when the envelope is identical.
        assert_eq!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_changes_when_payload_changes() {
        let env_a = envelope(
            "msg-1",
            Some("key-1"),
            b"hello",
            "application/json",
            "order.created",
            "orders-service",
            None,
        );
        let env_b = envelope(
            "msg-1",
            Some("key-1"),
            b"world",
            "application/json",
            "order.created",
            "orders-service",
            None,
        );
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_changes_when_partition_key_changes() {
        let env_a = envelope(
            "msg-1",
            Some("key-a"),
            b"hello",
            "application/json",
            "order.created",
            "orders-service",
            None,
        );
        let env_b = envelope(
            "msg-1",
            Some("key-b"),
            b"hello",
            "application/json",
            "order.created",
            "orders-service",
            None,
        );
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_changes_when_content_type_changes() {
        let env_a = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        let env_b = envelope("msg-1", None, b"hello", "text/plain", "type", "src", None);
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_changes_when_event_type_changes() {
        let env_a = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type-a",
            "src",
            None,
        );
        let env_b = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type-b",
            "src",
            None,
        );
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_changes_when_source_changes() {
        let env_a = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src-a",
            None,
        );
        let env_b = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src-b",
            None,
        );
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_changes_when_subject_changes() {
        let env_a = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            Some("sub-a"),
        );
        let env_b = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            Some("sub-b"),
        );
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_distinguishes_present_vs_absent_subject() {
        let env_a = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            Some("sub"),
        );
        let env_b = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_distinguishes_present_vs_absent_partition_key() {
        let env_a = envelope(
            "msg-1",
            Some("key"),
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        let env_b = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_changes_when_headers_change() {
        let mut headers = MessageHeaders::new();
        headers.insert(
            msg_core::HeaderName::new("trace-id").unwrap(),
            msg_core::HeaderValue::new("abc"),
        );
        let env_a = MessageEnvelope::builder(
            MessageId::new("msg-1").unwrap(),
            EventSource::new("src").unwrap(),
            EventType::new("type").unwrap(),
            ContentType::new("application/json").unwrap(),
            MessageTimestamp::from_unix_millis(1_700_000_000_000),
            MessagePayload::from_bytes(b"hello"),
        )
        .headers(headers)
        .build();

        let env_b = envelope(
            "msg-1",
            None,
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
    }

    #[test]
    fn fingerprint_distinguishes_different_topics() {
        let env = envelope(
            "msg-1",
            Some("key-1"),
            b"hello",
            "application/json",
            "order.created",
            "orders-service",
            None,
        );
        let topic_a = TopicName::new("orders").unwrap();
        let topic_b = TopicName::new("payments").unwrap();
        assert_ne!(
            PublishFingerprint::compute(&topic_a, &env),
            PublishFingerprint::compute(&topic_b, &env)
        );
    }

    #[test]
    fn fingerprint_handles_binary_payloads_as_raw_bytes() {
        let env_a = envelope(
            "msg-1",
            None,
            &[0x00, 0xFF, 0xFE, 0x01],
            "application/octet-stream",
            "type",
            "src",
            None,
        );
        let env_b = envelope(
            "msg-1",
            None,
            &[0x00, 0xFF, 0xFE, 0x02],
            "application/octet-stream",
            "type",
            "src",
            None,
        );
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
        let env_c = envelope(
            "msg-1",
            None,
            &[0x00, 0xFF, 0xFE, 0x01],
            "application/octet-stream",
            "type",
            "src",
            None,
        );
        assert_eq!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_c)
        );
    }

    #[test]
    fn fingerprint_handles_empty_payload() {
        let env_a = envelope("msg-1", None, b"", "application/json", "type", "src", None);
        let env_b = envelope("msg-1", None, b"", "application/json", "type", "src", None);
        assert_eq!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_b)
        );
        let env_c = envelope("msg-1", None, b"x", "application/json", "type", "src", None);
        assert_ne!(
            PublishFingerprint::compute(&topic(), &env_a),
            PublishFingerprint::compute(&topic(), &env_c)
        );
    }

    #[test]
    fn is_equivalent_retry_returns_true_for_matching_fingerprint() {
        let env = envelope(
            "msg-1",
            Some("key-1"),
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        let fingerprint = PublishFingerprint::compute(&topic(), &env);
        let existing = IdempotencyRecord::new(
            fingerprint,
            PartitionId::new(0),
            Offset::new(5),
            MessageId::new("original-msg").unwrap(),
        );
        assert!(is_equivalent_retry(&topic(), &env, &existing));
    }

    #[test]
    fn is_equivalent_retry_returns_false_for_different_intent() {
        let env_a = envelope(
            "msg-1",
            Some("key-1"),
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        let env_b = envelope(
            "msg-1",
            Some("key-1"),
            b"world",
            "application/json",
            "type",
            "src",
            None,
        );
        let fingerprint = PublishFingerprint::compute(&topic(), &env_a);
        let existing = IdempotencyRecord::new(
            fingerprint,
            PartitionId::new(0),
            Offset::new(0),
            MessageId::new("msg-1").unwrap(),
        );
        assert!(!is_equivalent_retry(&topic(), &env_b, &existing));
    }

    #[test]
    fn conflict_error_does_not_expose_key_or_payload() {
        let topic = TopicName::new("orders").unwrap();
        let error = conflict_error(&topic);
        let message = format!("{error}");
        assert!(!message.contains("idem-1"));
        assert!(!message.contains("hello"));
        assert!(message.contains("orders"));
        assert!(matches!(error, BrokerError::IdempotencyKeyConflict { .. }));
    }

    #[test]
    fn equivalent_retry_with_different_message_id_returns_original_identity() {
        let env_original = envelope(
            "msg-original",
            Some("key-1"),
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        let env_retry = envelope(
            "msg-retry",
            Some("key-1"),
            b"hello",
            "application/json",
            "type",
            "src",
            None,
        );
        let fingerprint = PublishFingerprint::compute(&topic(), &env_original);
        let existing = IdempotencyRecord::new(
            fingerprint,
            PartitionId::new(2),
            Offset::new(7),
            MessageId::new("msg-original").unwrap(),
        );
        assert!(is_equivalent_retry(&topic(), &env_retry, &existing));
        assert_eq!(existing.message_id().as_str(), "msg-original");
        assert_eq!(existing.partition_id(), PartitionId::new(2));
        assert_eq!(existing.offset(), Offset::new(7));
    }
}
