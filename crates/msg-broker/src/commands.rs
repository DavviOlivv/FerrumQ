use msg_core::{
    ConsumerGroupId, ConsumerId, DeliveryId, MessageEnvelope, MessageTimestamp, TopicConfig,
    TopicName,
};

/// Create a topic with the validated name and topic configuration from `msg-core`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicCommand {
    name: TopicName,
    config: TopicConfig,
}

impl CreateTopicCommand {
    #[must_use]
    pub fn new(name: TopicName, config: TopicConfig) -> Self {
        Self { name, config }
    }

    #[must_use]
    pub fn name(&self) -> &TopicName {
        &self.name
    }

    #[must_use]
    pub fn config(&self) -> TopicConfig {
        self.config
    }
}

/// Publish an already validated message envelope to a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishCommand {
    topic: TopicName,
    envelope: MessageEnvelope,
}

impl PublishCommand {
    #[must_use]
    pub fn new(topic: TopicName, envelope: MessageEnvelope) -> Self {
        Self { topic, envelope }
    }

    #[must_use]
    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    #[must_use]
    pub fn envelope(&self) -> &MessageEnvelope {
        &self.envelope
    }

    pub(crate) fn into_parts(self) -> (TopicName, MessageEnvelope) {
        (self.topic, self.envelope)
    }
}

/// Consume up to `max_messages` messages for one consumer in one consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeCommand {
    topic: TopicName,
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
    max_messages: usize,
    timestamp: MessageTimestamp,
}

impl ConsumeCommand {
    #[must_use]
    pub fn new(
        topic: TopicName,
        consumer_group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        max_messages: usize,
        timestamp: MessageTimestamp,
    ) -> Self {
        Self {
            topic,
            consumer_group_id,
            consumer_id,
            max_messages,
            timestamp,
        }
    }

    #[must_use]
    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    #[must_use]
    pub fn consumer_group_id(&self) -> &ConsumerGroupId {
        &self.consumer_group_id
    }

    #[must_use]
    pub fn consumer_id(&self) -> &ConsumerId {
        &self.consumer_id
    }

    #[must_use]
    pub fn max_messages(&self) -> usize {
        self.max_messages
    }

    #[must_use]
    pub fn timestamp(&self) -> MessageTimestamp {
        self.timestamp
    }
}

/// ACK a pending delivery for the consumer that received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckCommand {
    delivery_id: DeliveryId,
    consumer_id: ConsumerId,
    timestamp: MessageTimestamp,
}

impl AckCommand {
    #[must_use]
    pub fn new(
        delivery_id: DeliveryId,
        consumer_id: ConsumerId,
        timestamp: MessageTimestamp,
    ) -> Self {
        Self {
            delivery_id,
            consumer_id,
            timestamp,
        }
    }

    #[must_use]
    pub fn delivery_id(&self) -> &DeliveryId {
        &self.delivery_id
    }

    #[must_use]
    pub fn consumer_id(&self) -> &ConsumerId {
        &self.consumer_id
    }

    #[must_use]
    pub fn timestamp(&self) -> MessageTimestamp {
        self.timestamp
    }
}

/// NACK a pending delivery for the consumer that received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NackCommand {
    delivery_id: DeliveryId,
    consumer_id: ConsumerId,
    reason: Option<String>,
    timestamp: MessageTimestamp,
}

impl NackCommand {
    #[must_use]
    pub fn new(
        delivery_id: DeliveryId,
        consumer_id: ConsumerId,
        timestamp: MessageTimestamp,
    ) -> Self {
        Self {
            delivery_id,
            consumer_id,
            reason: None,
            timestamp,
        }
    }

    #[must_use]
    pub fn with_reason(
        delivery_id: DeliveryId,
        consumer_id: ConsumerId,
        reason: impl AsRef<str>,
        timestamp: MessageTimestamp,
    ) -> Self {
        let reason = reason.as_ref().trim();

        Self {
            delivery_id,
            consumer_id,
            reason: (!reason.is_empty()).then(|| reason.to_owned()),
            timestamp,
        }
    }

    #[must_use]
    pub fn delivery_id(&self) -> &DeliveryId {
        &self.delivery_id
    }

    #[must_use]
    pub fn consumer_id(&self) -> &ConsumerId {
        &self.consumer_id
    }

    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    #[must_use]
    pub fn timestamp(&self) -> MessageTimestamp {
        self.timestamp
    }
}

/// Query dead-letter entries, optionally narrowed to one topic or consumer group.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DlqQuery {
    topic: Option<TopicName>,
    consumer_group_id: Option<ConsumerGroupId>,
}

impl DlqQuery {
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn for_topic(topic: TopicName) -> Self {
        Self {
            topic: Some(topic),
            consumer_group_id: None,
        }
    }

    #[must_use]
    pub fn for_consumer_group(consumer_group_id: ConsumerGroupId) -> Self {
        Self {
            topic: None,
            consumer_group_id: Some(consumer_group_id),
        }
    }

    #[must_use]
    pub fn for_topic_and_consumer_group(
        topic: TopicName,
        consumer_group_id: ConsumerGroupId,
    ) -> Self {
        Self {
            topic: Some(topic),
            consumer_group_id: Some(consumer_group_id),
        }
    }

    #[must_use]
    pub fn topic(&self) -> Option<&TopicName> {
        self.topic.as_ref()
    }

    #[must_use]
    pub fn consumer_group_id(&self) -> Option<&ConsumerGroupId> {
        self.consumer_group_id.as_ref()
    }
}
