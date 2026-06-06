use msg_core::{
    ConsumerGroupId, ConsumerId, DeadLetterReason, DeliveryId, MessageEnvelope, MessageId,
    MessageTimestamp, Offset, PartitionId, TopicName,
};

/// Metadata returned after appending a message to the in-memory partition log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedMessage {
    topic: TopicName,
    partition_id: PartitionId,
    offset: Offset,
    message_id: MessageId,
}

impl PublishedMessage {
    #[must_use]
    pub(crate) fn new(
        topic: TopicName,
        partition_id: PartitionId,
        offset: Offset,
        message_id: MessageId,
    ) -> Self {
        Self {
            topic,
            partition_id,
            offset,
            message_id,
        }
    }

    #[must_use]
    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    #[must_use]
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    #[must_use]
    pub fn offset(&self) -> Offset {
        self.offset
    }

    #[must_use]
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }
}

/// Message delivered to a consumer group with retry and lease metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedMessage {
    delivery_id: DeliveryId,
    topic: TopicName,
    partition_id: PartitionId,
    offset: Offset,
    envelope: MessageEnvelope,
    consumer_group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
    attempt_number: u32,
    delivered_at: MessageTimestamp,
    lease_expires_at: MessageTimestamp,
}

impl ConsumedMessage {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub(crate) fn new(
        delivery_id: DeliveryId,
        topic: TopicName,
        partition_id: PartitionId,
        offset: Offset,
        envelope: MessageEnvelope,
        consumer_group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        attempt_number: u32,
        delivered_at: MessageTimestamp,
        lease_expires_at: MessageTimestamp,
    ) -> Self {
        Self {
            delivery_id,
            topic,
            partition_id,
            offset,
            envelope,
            consumer_group_id,
            consumer_id,
            attempt_number,
            delivered_at,
            lease_expires_at,
        }
    }

    #[must_use]
    pub fn delivery_id(&self) -> &DeliveryId {
        &self.delivery_id
    }

    #[must_use]
    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    #[must_use]
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    #[must_use]
    pub fn offset(&self) -> Offset {
        self.offset
    }

    #[must_use]
    pub fn envelope(&self) -> &MessageEnvelope {
        &self.envelope
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
    pub fn attempt_number(&self) -> u32 {
        self.attempt_number
    }

    #[must_use]
    pub fn delivered_at(&self) -> MessageTimestamp {
        self.delivered_at
    }

    #[must_use]
    pub fn lease_expires_at(&self) -> MessageTimestamp {
        self.lease_expires_at
    }
}

/// Dead-lettered message with enough context for inspection and debugging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterEntry {
    topic: TopicName,
    partition_id: PartitionId,
    offset: Offset,
    message_id: MessageId,
    envelope: MessageEnvelope,
    consumer_group_id: ConsumerGroupId,
    reason: DeadLetterReason,
    attempt_count: u32,
    timestamp: MessageTimestamp,
}

impl DeadLetterEntry {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub(crate) fn new(
        topic: TopicName,
        partition_id: PartitionId,
        offset: Offset,
        message_id: MessageId,
        envelope: MessageEnvelope,
        consumer_group_id: ConsumerGroupId,
        reason: DeadLetterReason,
        attempt_count: u32,
        timestamp: MessageTimestamp,
    ) -> Self {
        Self {
            topic,
            partition_id,
            offset,
            message_id,
            envelope,
            consumer_group_id,
            reason,
            attempt_count,
            timestamp,
        }
    }

    #[must_use]
    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    #[must_use]
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    #[must_use]
    pub fn offset(&self) -> Offset {
        self.offset
    }

    #[must_use]
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }

    #[must_use]
    pub fn envelope(&self) -> &MessageEnvelope {
        &self.envelope
    }

    #[must_use]
    pub fn consumer_group_id(&self) -> &ConsumerGroupId {
        &self.consumer_group_id
    }

    #[must_use]
    pub fn reason(&self) -> &DeadLetterReason {
        &self.reason
    }

    #[must_use]
    pub fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    #[must_use]
    pub fn timestamp(&self) -> MessageTimestamp {
        self.timestamp
    }
}

/// Summary of a retry maintenance pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetrySummary {
    retry_scheduled: usize,
    lease_expired: usize,
    made_available: usize,
    dead_lettered: usize,
}

impl RetrySummary {
    #[must_use]
    pub(crate) fn new(
        retry_scheduled: usize,
        lease_expired: usize,
        made_available: usize,
        dead_lettered: usize,
    ) -> Self {
        Self {
            retry_scheduled,
            lease_expired,
            made_available,
            dead_lettered,
        }
    }

    #[must_use]
    pub fn retry_scheduled(&self) -> usize {
        self.retry_scheduled
    }

    #[must_use]
    pub fn lease_expired(&self) -> usize {
        self.lease_expired
    }

    #[must_use]
    pub fn made_available(&self) -> usize {
        self.made_available
    }

    #[must_use]
    pub fn dead_lettered(&self) -> usize {
        self.dead_lettered
    }
}
