use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{DomainError, DomainResult};
use crate::identifiers::{ConsumerGroupId, ConsumerId, SubscriptionId, TopicName};

/// Consumer group identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroup {
    id: ConsumerGroupId,
}

impl ConsumerGroup {
    #[must_use]
    pub fn new(id: ConsumerGroupId) -> Self {
        Self { id }
    }

    #[must_use]
    pub fn id(&self) -> &ConsumerGroupId {
        &self.id
    }
}

/// Consumer identity bound to a consumer group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Consumer {
    id: ConsumerId,
    group_id: ConsumerGroupId,
}

impl Consumer {
    #[must_use]
    pub fn new(id: ConsumerId, group_id: ConsumerGroupId) -> Self {
        Self { id, group_id }
    }

    #[must_use]
    pub fn id(&self) -> &ConsumerId {
        &self.id
    }

    #[must_use]
    pub fn group_id(&self) -> &ConsumerGroupId {
        &self.group_id
    }
}

/// Subscription configuration for one or more topics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SubscriptionConfig {
    topics: Vec<TopicName>,
}

impl SubscriptionConfig {
    pub fn new(topics: Vec<TopicName>) -> DomainResult<Self> {
        if topics.is_empty() {
            return Err(DomainError::EmptyCollection { field: "topics" });
        }

        Ok(Self { topics })
    }

    #[must_use]
    pub fn topics(&self) -> &[TopicName] {
        &self.topics
    }
}

impl<'de> Deserialize<'de> for SubscriptionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawSubscriptionConfig {
            topics: Vec<TopicName>,
        }

        let raw = RawSubscriptionConfig::deserialize(deserializer)?;
        Self::new(raw.topics).map_err(serde::de::Error::custom)
    }
}

/// Consumer group subscription to one or more topics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscription {
    id: SubscriptionId,
    group_id: ConsumerGroupId,
    config: SubscriptionConfig,
}

impl Subscription {
    #[must_use]
    pub fn new(id: SubscriptionId, group_id: ConsumerGroupId, config: SubscriptionConfig) -> Self {
        Self {
            id,
            group_id,
            config,
        }
    }

    #[must_use]
    pub fn id(&self) -> &SubscriptionId {
        &self.id
    }

    #[must_use]
    pub fn group_id(&self) -> &ConsumerGroupId {
        &self.group_id
    }

    #[must_use]
    pub fn config(&self) -> &SubscriptionConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_consumer_group_and_consumer() {
        let group_id = ConsumerGroupId::new("group.1").unwrap();
        let group = ConsumerGroup::new(group_id.clone());
        let consumer = Consumer::new(ConsumerId::new("consumer-1").unwrap(), group_id.clone());

        assert_eq!(group.id(), &group_id);
        assert_eq!(consumer.id().as_str(), "consumer-1");
        assert_eq!(consumer.group_id(), &group_id);
    }

    #[test]
    fn creates_one_topic_subscription() {
        let config = SubscriptionConfig::new(vec![TopicName::new("orders").unwrap()]).unwrap();
        let subscription = Subscription::new(
            SubscriptionId::new("sub-1").unwrap(),
            ConsumerGroupId::new("group.1").unwrap(),
            config,
        );

        assert_eq!(subscription.id().as_str(), "sub-1");
        assert_eq!(subscription.config().topics().len(), 1);
        assert_eq!(subscription.config().topics()[0].as_str(), "orders");
    }

    #[test]
    fn creates_multi_topic_subscription() {
        let config = SubscriptionConfig::new(vec![
            TopicName::new("orders").unwrap(),
            TopicName::new("payments").unwrap(),
        ])
        .unwrap();

        assert_eq!(config.topics().len(), 2);
        assert_eq!(config.topics()[1].as_str(), "payments");
    }

    #[test]
    fn rejects_invalid_subscription_inputs() {
        assert!(ConsumerGroupId::new("group/1").is_err());
        assert!(ConsumerId::new("").is_err());
        assert!(SubscriptionId::new(" ").is_err());
        assert!(TopicName::new("bad topic").is_err());
        assert!(SubscriptionConfig::new(Vec::new()).is_err());
    }
}
