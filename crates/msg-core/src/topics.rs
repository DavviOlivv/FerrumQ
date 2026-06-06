use std::num::NonZeroU32;

use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{DomainError, DomainResult};
use crate::identifiers::{PartitionId, TopicName};

/// Topic-level configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfig {
    partition_count: NonZeroU32,
}

impl TopicConfig {
    pub fn new(partition_count: u32) -> DomainResult<Self> {
        let partition_count = NonZeroU32::new(partition_count).ok_or(DomainError::TooSmall {
            field: "partition_count",
            min: 1,
            actual: u64::from(partition_count),
        })?;

        Ok(Self { partition_count })
    }

    #[must_use]
    pub fn partition_count(&self) -> u32 {
        self.partition_count.get()
    }
}

/// Partition-level configuration placeholder for future broker/runtime settings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionConfig;

/// A topic partition identified by a stable non-negative id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partition {
    id: PartitionId,
    config: PartitionConfig,
}

impl Partition {
    #[must_use]
    pub fn new(id: PartitionId, config: PartitionConfig) -> Self {
        Self { id, config }
    }

    #[must_use]
    pub fn id(&self) -> PartitionId {
        self.id
    }

    #[must_use]
    pub fn config(&self) -> PartitionConfig {
        self.config
    }
}

/// Logical stream made of one or more ordered partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Topic {
    name: TopicName,
    config: TopicConfig,
    partitions: Vec<Partition>,
}

impl Topic {
    #[must_use]
    pub fn new(name: TopicName, config: TopicConfig) -> Self {
        let partitions = (0..config.partition_count())
            .map(|id| Partition::new(PartitionId::new(id), PartitionConfig))
            .collect();

        Self {
            name,
            config,
            partitions,
        }
    }

    #[must_use]
    pub fn name(&self) -> &TopicName {
        &self.name
    }

    #[must_use]
    pub fn config(&self) -> TopicConfig {
        self.config
    }

    #[must_use]
    pub fn partition_count(&self) -> u32 {
        self.config.partition_count()
    }

    #[must_use]
    pub fn partitions(&self) -> &[Partition] {
        &self.partitions
    }

    pub fn partition_ids(&self) -> impl Iterator<Item = PartitionId> + '_ {
        self.partitions.iter().map(Partition::id)
    }
}

impl<'de> Deserialize<'de> for Topic {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawTopic {
            name: TopicName,
            config: TopicConfig,
            partitions: Vec<Partition>,
        }

        let raw = RawTopic::deserialize(deserializer)?;
        let expected_count = raw.config.partition_count() as usize;
        if raw.partitions.len() != expected_count {
            return Err(serde::de::Error::custom(format!(
                "partition count mismatch: expected {expected_count}, got {}",
                raw.partitions.len()
            )));
        }

        for (expected, partition) in raw.partitions.iter().enumerate() {
            if partition.id().value() != expected as u32 {
                return Err(serde::de::Error::custom(format!(
                    "partition id mismatch: expected {expected}, got {}",
                    partition.id().value()
                )));
            }
        }

        Ok(Self {
            name: raw.name,
            config: raw.config,
            partitions: raw.partitions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_one_partition_topic() {
        let topic = Topic::new(
            TopicName::new("orders").unwrap(),
            TopicConfig::new(1).unwrap(),
        );

        assert_eq!(topic.name().as_str(), "orders");
        assert_eq!(topic.partition_count(), 1);
        assert_eq!(topic.partitions()[0].id(), PartitionId::new(0));
    }

    #[test]
    fn creates_multi_partition_topic_with_stable_ids() {
        let topic = Topic::new(
            TopicName::new("orders").unwrap(),
            TopicConfig::new(3).unwrap(),
        );

        let ids: Vec<_> = topic.partition_ids().collect();
        assert_eq!(
            ids,
            vec![
                PartitionId::new(0),
                PartitionId::new(1),
                PartitionId::new(2)
            ]
        );
    }

    #[test]
    fn rejects_zero_partition_topics() {
        assert!(TopicConfig::new(0).is_err());
    }
}
