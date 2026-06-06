mod validation;

pub mod consumers;
pub mod delivery;
pub mod error;
pub mod identifiers;
pub mod message;
pub mod topics;

pub use consumers::{Consumer, ConsumerGroup, Subscription, SubscriptionConfig};
pub use delivery::{
    Ack, DeadLetterReason, Delivery, DeliveryAttempt, DeliveryState, Nack, RetryPolicy,
};
pub use error::{DomainError, DomainResult};
pub use identifiers::{
    ConsumerGroupId, ConsumerId, DeliveryId, IdempotencyKey, MessageId, Offset, PartitionId,
    PartitionKey, SubscriptionId, TopicName,
};
pub use message::{
    ContentType, EventSource, EventSubject, EventType, HeaderName, HeaderValue, MessageEnvelope,
    MessageEnvelopeBuilder, MessageHeaders, MessagePayload, MessageTimestamp,
};
pub use topics::{Partition, PartitionConfig, Topic, TopicConfig};

/// Returns this crate's package name.
pub fn crate_name() -> &'static str {
    "msg-core"
}

#[cfg(test)]
mod tests {
    use super::crate_name;

    #[test]
    fn reports_crate_name() {
        assert_eq!(crate_name(), "msg-core");
    }
}
