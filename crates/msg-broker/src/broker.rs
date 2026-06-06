use msg_core::{MessageTimestamp, RetryPolicy, Topic};

use crate::{
    commands::{
        AckCommand, ConsumeCommand, CreateTopicCommand, DlqQuery, NackCommand, PublishCommand,
    },
    delivery::{ConsumedMessage, DeadLetterEntry, PublishedMessage, RetrySummary},
    errors::{BrokerError, BrokerResult},
    memory::InMemoryBrokerState,
};

/// Runtime-free broker configuration for deterministic in-memory tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerConfig {
    retry_policy: RetryPolicy,
    delivery_lease_millis: u64,
}

impl BrokerConfig {
    pub fn new(retry_policy: RetryPolicy, delivery_lease_millis: u64) -> BrokerResult<Self> {
        if delivery_lease_millis == 0 {
            return Err(BrokerError::InvalidConfig {
                field: "delivery_lease_millis",
                reason: "must be greater than zero",
            });
        }

        Ok(Self {
            retry_policy,
            delivery_lease_millis,
        })
    }

    #[must_use]
    pub fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }

    #[must_use]
    pub fn delivery_lease_millis(&self) -> u64 {
        self.delivery_lease_millis
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::new(3, Some(1_000)).expect("default retry policy is valid"),
            delivery_lease_millis: 30_000,
        }
    }
}

/// Synchronous, deterministic in-memory broker service.
///
/// Partition assignment is deterministic: messages with a partition key use
/// FNV-1a 64-bit over the key bytes modulo the topic partition count; messages
/// without a key use a per-topic round-robin counter.
#[derive(Debug, Clone)]
pub struct BrokerService {
    config: BrokerConfig,
    state: InMemoryBrokerState,
}

impl BrokerService {
    #[must_use]
    pub fn new(config: BrokerConfig) -> Self {
        Self {
            config,
            state: InMemoryBrokerState::default(),
        }
    }

    pub fn create_topic(&mut self, command: CreateTopicCommand) -> BrokerResult<Topic> {
        self.state.create_topic(command)
    }

    pub fn publish(&mut self, command: PublishCommand) -> BrokerResult<PublishedMessage> {
        let (topic, envelope) = command.into_parts();
        self.state.publish(topic, envelope)
    }

    pub fn consume(&mut self, command: ConsumeCommand) -> BrokerResult<Vec<ConsumedMessage>> {
        self.state
            .consume(command, self.config.delivery_lease_millis())
    }

    pub fn ack(&mut self, command: AckCommand) -> BrokerResult<()> {
        self.state.ack(command)
    }

    pub fn nack(&mut self, command: NackCommand) -> BrokerResult<()> {
        self.state.nack(command, self.config.retry_policy())
    }

    pub fn retry_ready(&mut self, now: MessageTimestamp) -> BrokerResult<RetrySummary> {
        self.state.retry_ready(now, self.config.retry_policy())
    }

    pub fn list_dlq(&self, query: DlqQuery) -> BrokerResult<Vec<DeadLetterEntry>> {
        self.state.list_dlq(query)
    }
}
