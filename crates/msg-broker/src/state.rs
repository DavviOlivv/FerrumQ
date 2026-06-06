use std::collections::{BTreeMap, BTreeSet};

use msg_core::{
    ConsumerGroupId, ConsumerId, DeliveryId, MessageEnvelope, MessageTimestamp, Offset,
    PartitionId, Topic, TopicName,
};

use crate::delivery::DeadLetterEntry;

#[derive(Debug, Clone)]
pub(crate) struct StoredTopic {
    topic: Topic,
    partitions: BTreeMap<PartitionId, PartitionLog>,
    next_round_robin_partition: u32,
}

impl StoredTopic {
    pub(crate) fn new(topic: Topic) -> Self {
        let partitions = topic
            .partition_ids()
            .map(|partition_id| (partition_id, PartitionLog::default()))
            .collect();

        Self {
            topic,
            partitions,
            next_round_robin_partition: 0,
        }
    }

    pub(crate) fn partition_log(&self, partition_id: PartitionId) -> Option<&PartitionLog> {
        self.partitions.get(&partition_id)
    }

    pub(crate) fn partition_log_mut(
        &mut self,
        partition_id: PartitionId,
    ) -> Option<&mut PartitionLog> {
        self.partitions.get_mut(&partition_id)
    }

    pub(crate) fn partition_ids(&self) -> impl Iterator<Item = PartitionId> + '_ {
        self.partitions.keys().copied()
    }

    pub(crate) fn partition_count(&self) -> u32 {
        self.topic.partition_count()
    }

    pub(crate) fn select_round_robin_partition(&mut self) -> PartitionId {
        let partition_id = PartitionId::new(self.next_round_robin_partition);
        self.next_round_robin_partition =
            (self.next_round_robin_partition + 1) % self.partition_count();
        partition_id
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PartitionLog {
    entries: Vec<MessageEnvelope>,
}

impl PartitionLog {
    pub(crate) fn append(&mut self, envelope: MessageEnvelope) -> Offset {
        let offset = Offset::new(self.entries.len() as u64);
        self.entries.push(envelope);
        offset
    }

    pub(crate) fn get(&self, offset: Offset) -> Option<&MessageEnvelope> {
        self.entries.get(offset.value() as usize)
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MessageRef {
    pub(crate) topic: TopicName,
    pub(crate) partition_id: PartitionId,
    pub(crate) offset: Offset,
}

impl MessageRef {
    pub(crate) fn new(topic: TopicName, partition_id: PartitionId, offset: Offset) -> Self {
        Self {
            topic,
            partition_id,
            offset,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PendingDelivery {
    pub(crate) message_ref: MessageRef,
    pub(crate) consumer_group_id: ConsumerGroupId,
    pub(crate) consumer_id: ConsumerId,
    pub(crate) attempt_number: u32,
    pub(crate) lease_expires_at: MessageTimestamp,
}

#[derive(Debug, Clone)]
pub(crate) struct RetryEntry {
    pub(crate) message_ref: MessageRef,
    pub(crate) ready_at: MessageTimestamp,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct GroupPartitionState {
    pub(crate) cursor: u64,
    pub(crate) acked_offsets: BTreeSet<Offset>,
    pub(crate) pending_by_offset: BTreeMap<Offset, DeliveryId>,
    pub(crate) retry_by_offset: BTreeMap<Offset, RetryEntry>,
    pub(crate) dead_lettered_offsets: BTreeSet<Offset>,
    pub(crate) attempt_counts: BTreeMap<Offset, u32>,
}

impl GroupPartitionState {
    pub(crate) fn is_available(&self, offset: Offset) -> bool {
        !self.acked_offsets.contains(&offset)
            && !self.pending_by_offset.contains_key(&offset)
            && !self.retry_by_offset.contains_key(&offset)
            && !self.dead_lettered_offsets.contains(&offset)
    }

    pub(crate) fn next_attempt_number(&self, offset: Offset) -> u32 {
        self.attempt_counts
            .get(&offset)
            .copied()
            .unwrap_or_default()
            + 1
    }

    pub(crate) fn mark_pending(&mut self, offset: Offset, delivery_id: DeliveryId) {
        self.pending_by_offset.insert(offset, delivery_id);
    }

    pub(crate) fn mark_acked(&mut self, offset: Offset) {
        self.acked_offsets.insert(offset);
        self.attempt_counts.remove(&offset);
        self.advance_cursor();
    }

    pub(crate) fn mark_retry_scheduled(&mut self, offset: Offset, retry_entry: RetryEntry) {
        self.retry_by_offset.insert(offset, retry_entry);
    }

    pub(crate) fn mark_retry_available(&mut self, offset: Offset) {
        self.retry_by_offset.remove(&offset);
    }

    pub(crate) fn mark_dead_lettered(&mut self, offset: Offset) {
        self.retry_by_offset.remove(&offset);
        self.attempt_counts.remove(&offset);
        self.dead_lettered_offsets.insert(offset);
    }

    fn advance_cursor(&mut self) {
        while self.acked_offsets.contains(&Offset::new(self.cursor)) {
            self.cursor += 1;
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct GroupState {
    partitions: BTreeMap<(TopicName, PartitionId), GroupPartitionState>,
}

impl GroupState {
    pub(crate) fn partition_state_mut(
        &mut self,
        topic: &TopicName,
        partition_id: PartitionId,
    ) -> &mut GroupPartitionState {
        self.partitions
            .entry((topic.clone(), partition_id))
            .or_default()
    }

    pub(crate) fn partition_state(
        &self,
        topic: &TopicName,
        partition_id: PartitionId,
    ) -> Option<&GroupPartitionState> {
        self.partitions.get(&(topic.clone(), partition_id))
    }

    pub(crate) fn partition_states(
        &self,
    ) -> impl Iterator<Item = (&(TopicName, PartitionId), &GroupPartitionState)> {
        self.partitions.iter()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BrokerState {
    pub(crate) topics: BTreeMap<TopicName, StoredTopic>,
    pub(crate) groups: BTreeMap<ConsumerGroupId, GroupState>,
    pub(crate) pending: BTreeMap<DeliveryId, PendingDelivery>,
    pub(crate) dead_letters: Vec<DeadLetterEntry>,
}
