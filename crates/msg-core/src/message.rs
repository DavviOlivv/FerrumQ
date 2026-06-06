use std::collections::{BTreeMap, btree_map};

use serde::{Deserialize, Serialize};

use crate::identifiers::{IdempotencyKey, MessageId, PartitionKey};
use crate::validation::validated_string_type;

validated_string_type!(EventSource, "event_source", bounded_text);
validated_string_type!(EventType, "event_type", bounded_text);
validated_string_type!(EventSubject, "event_subject", bounded_text);
validated_string_type!(ContentType, "content_type", bounded_text);
validated_string_type!(HeaderName, "header_name", bounded_text);

/// Milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MessageTimestamp(u64);

impl MessageTimestamp {
    #[must_use]
    pub const fn from_unix_millis(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_unix_millis(self) -> u64 {
        self.0
    }
}

/// Header value carried with a message envelope.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeaderValue(String);

impl HeaderValue {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<&str> for HeaderValue {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for HeaderValue {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Display for HeaderValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for HeaderValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for HeaderValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(Self(value))
    }
}

/// Message headers keyed by validated header names.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageHeaders(BTreeMap<HeaderName, HeaderValue>);

impl MessageHeaders {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: HeaderName, value: HeaderValue) -> Option<HeaderValue> {
        self.0.insert(name, value)
    }

    #[must_use]
    pub fn get(&self, name: &HeaderName) -> Option<&HeaderValue> {
        self.0.get(name)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> btree_map::Iter<'_, HeaderName, HeaderValue> {
        self.0.iter()
    }
}

/// Opaque message payload bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessagePayload(Vec<u8>);

impl MessagePayload {
    #[must_use]
    pub fn from_bytes(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for MessagePayload {
    fn from(value: Vec<u8>) -> Self {
        Self::from_bytes(value)
    }
}

impl From<&[u8]> for MessagePayload {
    fn from(value: &[u8]) -> Self {
        Self::from_bytes(value)
    }
}

/// CloudEvents-inspired message metadata plus opaque payload bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageEnvelope {
    id: MessageId,
    source: EventSource,
    event_type: EventType,
    subject: Option<EventSubject>,
    content_type: ContentType,
    timestamp: MessageTimestamp,
    headers: MessageHeaders,
    payload: MessagePayload,
    partition_key: Option<PartitionKey>,
    idempotency_key: Option<IdempotencyKey>,
}

impl MessageEnvelope {
    #[must_use]
    pub fn builder(
        id: MessageId,
        source: EventSource,
        event_type: EventType,
        content_type: ContentType,
        timestamp: MessageTimestamp,
        payload: MessagePayload,
    ) -> MessageEnvelopeBuilder {
        MessageEnvelopeBuilder {
            id,
            source,
            event_type,
            subject: None,
            content_type,
            timestamp,
            headers: MessageHeaders::new(),
            payload,
            partition_key: None,
            idempotency_key: None,
        }
    }

    #[must_use]
    pub fn id(&self) -> &MessageId {
        &self.id
    }

    #[must_use]
    pub fn source(&self) -> &EventSource {
        &self.source
    }

    #[must_use]
    pub fn event_type(&self) -> &EventType {
        &self.event_type
    }

    #[must_use]
    pub fn subject(&self) -> Option<&EventSubject> {
        self.subject.as_ref()
    }

    #[must_use]
    pub fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    #[must_use]
    pub fn timestamp(&self) -> MessageTimestamp {
        self.timestamp
    }

    #[must_use]
    pub fn headers(&self) -> &MessageHeaders {
        &self.headers
    }

    #[must_use]
    pub fn payload(&self) -> &MessagePayload {
        &self.payload
    }

    #[must_use]
    pub fn partition_key(&self) -> Option<&PartitionKey> {
        self.partition_key.as_ref()
    }

    #[must_use]
    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.idempotency_key.as_ref()
    }
}

/// Builder for optional envelope metadata.
#[derive(Debug, Clone)]
pub struct MessageEnvelopeBuilder {
    id: MessageId,
    source: EventSource,
    event_type: EventType,
    subject: Option<EventSubject>,
    content_type: ContentType,
    timestamp: MessageTimestamp,
    headers: MessageHeaders,
    payload: MessagePayload,
    partition_key: Option<PartitionKey>,
    idempotency_key: Option<IdempotencyKey>,
}

impl MessageEnvelopeBuilder {
    #[must_use]
    pub fn subject(mut self, subject: EventSubject) -> Self {
        self.subject = Some(subject);
        self
    }

    #[must_use]
    pub fn partition_key(mut self, partition_key: PartitionKey) -> Self {
        self.partition_key = Some(partition_key);
        self
    }

    #[must_use]
    pub fn idempotency_key(mut self, idempotency_key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(idempotency_key);
        self
    }

    #[must_use]
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }

    #[must_use]
    pub fn headers(mut self, headers: MessageHeaders) -> Self {
        self.headers = headers;
        self
    }

    #[must_use]
    pub fn build(self) -> MessageEnvelope {
        MessageEnvelope {
            id: self.id,
            source: self.source,
            event_type: self.event_type,
            subject: self.subject,
            content_type: self.content_type,
            timestamp: self.timestamp,
            headers: self.headers,
            payload: self.payload,
            partition_key: self.partition_key,
            idempotency_key: self.idempotency_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required_envelope_builder() -> MessageEnvelopeBuilder {
        MessageEnvelope::builder(
            MessageId::new("msg-1").unwrap(),
            EventSource::new("/tests").unwrap(),
            EventType::new("example.created").unwrap(),
            ContentType::new("application/json").unwrap(),
            MessageTimestamp::from_unix_millis(1_700_000_000_000),
            MessagePayload::from_bytes(br#"{"ok":true}"#.to_vec()),
        )
    }

    #[test]
    fn creates_message_envelope_with_optional_metadata() {
        let trace_header = HeaderName::new("trace-id").unwrap();
        let envelope = required_envelope_builder()
            .subject(EventSubject::new("subject-1").unwrap())
            .partition_key(PartitionKey::new("account-1").unwrap())
            .idempotency_key(IdempotencyKey::new("idem-1").unwrap())
            .header(trace_header.clone(), HeaderValue::new("trace-1"))
            .build();

        assert_eq!(envelope.id().as_str(), "msg-1");
        assert_eq!(envelope.source().as_str(), "/tests");
        assert_eq!(envelope.event_type().as_str(), "example.created");
        assert_eq!(envelope.subject().unwrap().as_str(), "subject-1");
        assert_eq!(envelope.content_type().as_str(), "application/json");
        assert_eq!(envelope.timestamp().as_unix_millis(), 1_700_000_000_000);
        assert_eq!(
            envelope.headers().get(&trace_header).unwrap().as_str(),
            "trace-1"
        );
        assert_eq!(envelope.payload().as_bytes(), br#"{"ok":true}"#);
        assert_eq!(envelope.partition_key().unwrap().as_str(), "account-1");
        assert_eq!(envelope.idempotency_key().unwrap().as_str(), "idem-1");
    }

    #[test]
    fn creates_message_envelope_without_optional_metadata() {
        let envelope = required_envelope_builder().build();

        assert!(envelope.subject().is_none());
        assert!(envelope.partition_key().is_none());
        assert!(envelope.idempotency_key().is_none());
        assert!(envelope.headers().is_empty());
    }

    #[test]
    fn rejects_invalid_required_envelope_fields() {
        assert!(MessageId::new("").is_err());
        assert!(EventSource::new(" ").is_err());
        assert!(EventType::new("a".repeat(256)).is_err());
        assert!(ContentType::new("").is_err());
    }

    #[test]
    fn serde_round_trips_message_envelope() {
        let envelope = required_envelope_builder()
            .subject(EventSubject::new("subject-1").unwrap())
            .partition_key(PartitionKey::new("account-1").unwrap())
            .idempotency_key(IdempotencyKey::new("idem-1").unwrap())
            .header(
                HeaderName::new("trace-id").unwrap(),
                HeaderValue::new("trace-1"),
            )
            .build();

        let serialized = serde_json::to_string(&envelope).unwrap();
        let deserialized: MessageEnvelope = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized, envelope);
    }

    #[test]
    fn serde_round_trips_representative_key_types() {
        let topic = crate::TopicName::new("orders.created").unwrap();
        let serialized = serde_json::to_string(&topic).unwrap();
        let deserialized: crate::TopicName = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, topic);

        let offset = crate::Offset::new(42);
        let serialized = serde_json::to_string(&offset).unwrap();
        let deserialized: crate::Offset = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, offset);

        let consumer_group = crate::ConsumerGroupId::new("group.1").unwrap();
        let serialized = serde_json::to_string(&consumer_group).unwrap();
        let deserialized: crate::ConsumerGroupId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, consumer_group);
    }
}
