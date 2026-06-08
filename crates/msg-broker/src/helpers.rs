use msg_core::{
    ConsumerGroupId, DeliveryId, MessageTimestamp, Offset, PartitionId, PartitionKey, TopicName,
};

use crate::{
    errors::{BrokerError, BrokerResult},
    state::StoredTopic,
};

pub(crate) fn keyed_partition(partition_key: &PartitionKey, partition_count: u32) -> PartitionId {
    PartitionId::new(
        (fnv1a_64(partition_key.as_str().as_bytes()) % u64::from(partition_count)) as u32,
    )
}

pub(crate) fn round_robin_partition(next_round_robin_partition: u32) -> PartitionId {
    PartitionId::new(next_round_robin_partition)
}

pub(crate) fn advance_round_robin_partition(
    next_round_robin_partition: u32,
    partition_count: u32,
) -> u32 {
    (next_round_robin_partition + 1) % partition_count
}

pub(crate) fn select_partition(
    stored_topic: &StoredTopic,
    envelope: &msg_core::MessageEnvelope,
) -> PartitionId {
    if let Some(partition_key) = envelope.partition_key() {
        return keyed_partition(partition_key, stored_topic.partition_count());
    }

    round_robin_partition(stored_topic.next_round_robin_partition())
}

/// Deterministic FNV-1a 64-bit hash for keyed partition selection.
pub(crate) fn fnv1a_64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

pub(crate) fn deterministic_delivery_id(
    consumer_group_id: &ConsumerGroupId,
    topic: &TopicName,
    partition_id: PartitionId,
    offset: Offset,
    attempt_number: u32,
) -> BrokerResult<DeliveryId> {
    let input = format!(
        "{}:{}:{}:{}:{}",
        consumer_group_id,
        topic,
        partition_id.value(),
        offset.value(),
        attempt_number
    );

    DeliveryId::new(format!(
        "delivery:{:016x}:{}:{}:{}",
        fnv1a_64(input.as_bytes()),
        partition_id.value(),
        offset.value(),
        attempt_number
    ))
    .map_err(BrokerError::from)
}

pub(crate) fn add_millis(
    timestamp: MessageTimestamp,
    millis: u64,
    field: &'static str,
) -> BrokerResult<MessageTimestamp> {
    timestamp
        .as_unix_millis()
        .checked_add(millis)
        .map(MessageTimestamp::from_unix_millis)
        .ok_or(BrokerError::InvalidConfig {
            field,
            reason: "timestamp overflow",
        })
}
