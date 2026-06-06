use std::num::{NonZeroU32, NonZeroU64};

use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{DomainError, DomainResult};
use crate::identifiers::{ConsumerId, DeliveryId, MessageId, Offset, PartitionId, TopicName};
use crate::message::MessageTimestamp;

/// Reason a delivery is eligible for dead-letter handling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeadLetterReason {
    MaxAttemptsExceeded,
    Expired,
    Rejected,
    Poisoned,
    Manual(String),
}

/// State of a delivery record in the pure domain model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryState {
    Pending,
    Delivered,
    Acked,
    Nacked,
    RetryScheduled,
    DeadLettered(DeadLetterReason),
}

/// Delivery attempt metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryAttempt {
    delivery_id: DeliveryId,
    attempt_number: NonZeroU32,
    timestamp: MessageTimestamp,
}

impl DeliveryAttempt {
    pub fn new(
        delivery_id: DeliveryId,
        attempt_number: u32,
        timestamp: MessageTimestamp,
    ) -> DomainResult<Self> {
        let attempt_number = NonZeroU32::new(attempt_number).ok_or(DomainError::TooSmall {
            field: "attempt_number",
            min: 1,
            actual: u64::from(attempt_number),
        })?;

        Ok(Self {
            delivery_id,
            attempt_number,
            timestamp,
        })
    }

    #[must_use]
    pub fn delivery_id(&self) -> &DeliveryId {
        &self.delivery_id
    }

    #[must_use]
    pub fn attempt_number(&self) -> u32 {
        self.attempt_number.get()
    }

    #[must_use]
    pub fn timestamp(&self) -> MessageTimestamp {
        self.timestamp
    }
}

/// Positive bounded retry policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    max_attempts: NonZeroU32,
    backoff_millis: Option<NonZeroU64>,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, backoff_millis: Option<u64>) -> DomainResult<Self> {
        let max_attempts = NonZeroU32::new(max_attempts).ok_or(DomainError::TooSmall {
            field: "max_attempts",
            min: 1,
            actual: u64::from(max_attempts),
        })?;

        let backoff_millis = match backoff_millis {
            Some(0) => {
                return Err(DomainError::TooSmall {
                    field: "backoff_millis",
                    min: 1,
                    actual: 0,
                });
            }
            Some(value) => NonZeroU64::new(value),
            None => None,
        };

        Ok(Self {
            max_attempts,
            backoff_millis,
        })
    }

    #[must_use]
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts.get()
    }

    #[must_use]
    pub fn backoff_millis(&self) -> Option<u64> {
        self.backoff_millis.map(NonZeroU64::get)
    }
}

/// ACK command for a delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    delivery_id: DeliveryId,
    consumer_id: ConsumerId,
    timestamp: MessageTimestamp,
}

impl Ack {
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

/// NACK command for a delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Nack {
    delivery_id: DeliveryId,
    consumer_id: ConsumerId,
    reason: Option<String>,
    timestamp: MessageTimestamp,
}

impl Nack {
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

/// Delivery record for a message assigned to a consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Delivery {
    id: DeliveryId,
    message_id: MessageId,
    consumer_id: ConsumerId,
    topic_name: TopicName,
    partition_id: PartitionId,
    offset: Offset,
    state: DeliveryState,
    attempts: Vec<DeliveryAttempt>,
}

impl Delivery {
    #[must_use]
    pub fn new(
        id: DeliveryId,
        message_id: MessageId,
        consumer_id: ConsumerId,
        topic_name: TopicName,
        partition_id: PartitionId,
        offset: Offset,
    ) -> Self {
        Self {
            id,
            message_id,
            consumer_id,
            topic_name,
            partition_id,
            offset,
            state: DeliveryState::Pending,
            attempts: Vec::new(),
        }
    }

    #[must_use]
    pub fn id(&self) -> &DeliveryId {
        &self.id
    }

    #[must_use]
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }

    #[must_use]
    pub fn consumer_id(&self) -> &ConsumerId {
        &self.consumer_id
    }

    #[must_use]
    pub fn topic_name(&self) -> &TopicName {
        &self.topic_name
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
    pub fn state(&self) -> &DeliveryState {
        &self.state
    }

    #[must_use]
    pub fn attempts(&self) -> &[DeliveryAttempt] {
        &self.attempts
    }

    pub fn add_attempt(&mut self, attempt: DeliveryAttempt) -> DomainResult<()> {
        if attempt.delivery_id() != &self.id {
            return Err(DomainError::InvalidReference {
                field: "delivery_attempt.delivery_id",
                expected: self.id.to_string(),
                actual: attempt.delivery_id().to_string(),
            });
        }

        self.attempts.push(attempt);
        self.state = DeliveryState::Delivered;
        Ok(())
    }

    pub fn apply_ack(&mut self, ack: &Ack) -> DomainResult<()> {
        self.ensure_command_matches(ack.delivery_id(), ack.consumer_id())?;
        self.state = DeliveryState::Acked;
        Ok(())
    }

    pub fn apply_nack(&mut self, nack: &Nack) -> DomainResult<()> {
        self.ensure_command_matches(nack.delivery_id(), nack.consumer_id())?;
        self.state = DeliveryState::Nacked;
        Ok(())
    }

    pub fn schedule_retry(&mut self) {
        self.state = DeliveryState::RetryScheduled;
    }

    pub fn move_to_dead_letter(&mut self, reason: DeadLetterReason) {
        self.state = DeliveryState::DeadLettered(reason);
    }

    fn ensure_command_matches(
        &self,
        delivery_id: &DeliveryId,
        consumer_id: &ConsumerId,
    ) -> DomainResult<()> {
        if delivery_id != &self.id {
            return Err(DomainError::InvalidReference {
                field: "delivery_id",
                expected: self.id.to_string(),
                actual: delivery_id.to_string(),
            });
        }

        if consumer_id != &self.consumer_id {
            return Err(DomainError::InvalidReference {
                field: "consumer_id",
                expected: self.consumer_id.to_string(),
                actual: consumer_id.to_string(),
            });
        }

        Ok(())
    }
}

impl<'de> Deserialize<'de> for Delivery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawDelivery {
            id: DeliveryId,
            message_id: MessageId,
            consumer_id: ConsumerId,
            topic_name: TopicName,
            partition_id: PartitionId,
            offset: Offset,
            state: DeliveryState,
            attempts: Vec<DeliveryAttempt>,
        }

        let raw = RawDelivery::deserialize(deserializer)?;
        for attempt in &raw.attempts {
            if attempt.delivery_id() != &raw.id {
                return Err(serde::de::Error::custom(format!(
                    "delivery attempt id mismatch: expected {}, got {}",
                    raw.id,
                    attempt.delivery_id()
                )));
            }
        }

        Ok(Self {
            id: raw.id,
            message_id: raw.message_id,
            consumer_id: raw.consumer_id,
            topic_name: raw.topic_name,
            partition_id: raw.partition_id,
            offset: raw.offset,
            state: raw.state,
            attempts: raw.attempts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp() -> MessageTimestamp {
        MessageTimestamp::from_unix_millis(42)
    }

    fn delivery_id() -> DeliveryId {
        DeliveryId::new("delivery-1").unwrap()
    }

    fn consumer_id() -> ConsumerId {
        ConsumerId::new("consumer-1").unwrap()
    }

    fn delivery() -> Delivery {
        Delivery::new(
            delivery_id(),
            MessageId::new("message-1").unwrap(),
            consumer_id(),
            TopicName::new("orders").unwrap(),
            PartitionId::new(0),
            Offset::new(7),
        )
    }

    #[test]
    fn creates_delivery_attempts() {
        let attempt = DeliveryAttempt::new(delivery_id(), 1, timestamp()).unwrap();

        assert_eq!(attempt.delivery_id().as_str(), "delivery-1");
        assert_eq!(attempt.attempt_number(), 1);
        assert_eq!(attempt.timestamp(), timestamp());
        assert!(DeliveryAttempt::new(delivery_id(), 0, timestamp()).is_err());
    }

    #[test]
    fn creates_ack_and_nack() {
        let ack = Ack::new(delivery_id(), consumer_id(), timestamp());
        let nack = Nack::with_reason(delivery_id(), consumer_id(), " transient ", timestamp());

        assert_eq!(ack.delivery_id().as_str(), "delivery-1");
        assert_eq!(ack.consumer_id().as_str(), "consumer-1");
        assert_eq!(ack.timestamp(), timestamp());
        assert_eq!(nack.reason(), Some("transient"));
        assert_eq!(nack.timestamp(), timestamp());
    }

    #[test]
    fn validates_retry_policy() {
        let immediate = RetryPolicy::new(1, None).unwrap();
        let backoff = RetryPolicy::new(3, Some(250)).unwrap();

        assert_eq!(immediate.max_attempts(), 1);
        assert_eq!(immediate.backoff_millis(), None);
        assert_eq!(backoff.max_attempts(), 3);
        assert_eq!(backoff.backoff_millis(), Some(250));
        assert!(RetryPolicy::new(0, None).is_err());
        assert!(RetryPolicy::new(1, Some(0)).is_err());
    }

    #[test]
    fn models_delivery_states_and_dead_letter_reasons() {
        let mut delivery = delivery();
        assert_eq!(delivery.state(), &DeliveryState::Pending);

        delivery
            .add_attempt(DeliveryAttempt::new(delivery_id(), 1, timestamp()).unwrap())
            .unwrap();
        assert_eq!(delivery.state(), &DeliveryState::Delivered);
        assert_eq!(delivery.attempts().len(), 1);

        delivery.schedule_retry();
        assert_eq!(delivery.state(), &DeliveryState::RetryScheduled);

        delivery.move_to_dead_letter(DeadLetterReason::MaxAttemptsExceeded);
        assert_eq!(
            delivery.state(),
            &DeliveryState::DeadLettered(DeadLetterReason::MaxAttemptsExceeded)
        );

        let manual = DeadLetterReason::Manual("operator override".to_owned());
        delivery.move_to_dead_letter(manual.clone());
        assert_eq!(delivery.state(), &DeliveryState::DeadLettered(manual));
    }

    #[test]
    fn applies_ack_and_nack_to_matching_deliveries() {
        let mut delivery = delivery();

        delivery
            .apply_ack(&Ack::new(delivery_id(), consumer_id(), timestamp()))
            .unwrap();
        assert_eq!(delivery.state(), &DeliveryState::Acked);

        delivery
            .apply_nack(&Nack::new(delivery_id(), consumer_id(), timestamp()))
            .unwrap();
        assert_eq!(delivery.state(), &DeliveryState::Nacked);
    }
}
