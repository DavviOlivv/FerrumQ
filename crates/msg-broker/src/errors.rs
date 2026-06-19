use msg_core::{ConsumerId, DeliveryId, DomainError, TopicName};
use thiserror::Error;

/// Result type used by broker orchestration.
pub type BrokerResult<T> = Result<T, BrokerError>;

/// Errors raised by the in-memory broker service.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BrokerError {
    #[error(transparent)]
    Domain(#[from] DomainError),

    #[error("topic already exists: {topic}")]
    TopicAlreadyExists { topic: TopicName },

    #[error("topic not found: {topic}")]
    TopicNotFound { topic: TopicName },

    #[error("delivery not found: {delivery_id}")]
    DeliveryNotFound { delivery_id: DeliveryId },

    #[error("delivery {delivery_id} belongs to consumer {expected}, not {actual}")]
    InvalidConsumer {
        delivery_id: DeliveryId,
        expected: ConsumerId,
        actual: ConsumerId,
    },

    #[error("invalid broker config for {field}: {reason}")]
    InvalidConfig {
        field: &'static str,
        reason: &'static str,
    },

    #[error("idempotency key conflict for topic {topic}")]
    IdempotencyKeyConflict { topic: TopicName },
}
