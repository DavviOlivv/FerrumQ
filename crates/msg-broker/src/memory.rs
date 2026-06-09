use msg_core::{
    ConsumerGroupId, DeadLetterReason, DeliveryId, MessageEnvelope, MessageTimestamp, Offset,
    RetryPolicy, Topic, TopicName,
};

use crate::{
    commands::{AckCommand, ConsumeCommand, CreateTopicCommand, DlqQuery, NackCommand},
    delivery::{ConsumedMessage, DeadLetterEntry, PublishedMessage, RetrySummary},
    errors::{BrokerError, BrokerResult},
    helpers::{add_millis, deterministic_delivery_id, select_partition},
    state::{
        BrokerState, GroupPartitionState, MessageRef, PendingDelivery, RetryEntry, StoredTopic,
    },
};

/// Explicitly in-memory broker state and mutation helpers.
#[derive(Debug, Clone, Default)]
pub(crate) struct InMemoryBrokerState {
    state: BrokerState,
}

impl InMemoryBrokerState {
    pub(crate) fn create_topic(&mut self, command: CreateTopicCommand) -> BrokerResult<Topic> {
        let topic = Topic::new(command.name().clone(), command.config());
        if self.state.topics.contains_key(topic.name()) {
            return Err(BrokerError::TopicAlreadyExists {
                topic: topic.name().clone(),
            });
        }

        self.state
            .topics
            .insert(topic.name().clone(), StoredTopic::new(topic.clone()));

        Ok(topic)
    }

    pub(crate) fn publish(
        &mut self,
        topic: TopicName,
        envelope: MessageEnvelope,
    ) -> BrokerResult<PublishedMessage> {
        let stored_topic =
            self.state
                .topics
                .get_mut(&topic)
                .ok_or_else(|| BrokerError::TopicNotFound {
                    topic: topic.clone(),
                })?;

        let should_advance_round_robin = envelope.partition_key().is_none();
        let partition_id = select_partition(stored_topic, &envelope);
        let message_id = envelope.id().clone();
        let offset = stored_topic
            .partition_log_mut(partition_id)
            .expect("topic partition logs are created from Topic partition ids")
            .append(envelope);
        if should_advance_round_robin {
            stored_topic.advance_round_robin_partition();
        }

        Ok(PublishedMessage::new(
            topic,
            partition_id,
            offset,
            message_id,
        ))
    }

    pub(crate) fn consume(
        &mut self,
        command: ConsumeCommand,
        delivery_lease_millis: u64,
        lease_field: &'static str,
    ) -> BrokerResult<Vec<ConsumedMessage>> {
        let topic_name = command.topic().clone();
        let stored_topic =
            self.state
                .topics
                .get(&topic_name)
                .ok_or_else(|| BrokerError::TopicNotFound {
                    topic: topic_name.clone(),
                })?;

        if command.max_messages() == 0 {
            return Ok(Vec::new());
        }

        let mut selected = Vec::new();
        for partition_id in stored_topic.partition_ids() {
            let Some(partition_log) = stored_topic.partition_log(partition_id) else {
                continue;
            };
            let Some(group_state) = self.state.groups.get(command.consumer_group_id()) else {
                for offset in 0..partition_log.len() as u64 {
                    selected.push((partition_id, Offset::new(offset)));
                    if selected.len() == command.max_messages() {
                        break;
                    }
                }
                if selected.len() == command.max_messages() {
                    break;
                }
                continue;
            };

            let partition_state = group_state.partition_state(&topic_name, partition_id);
            for offset in 0..partition_log.len() as u64 {
                let offset = Offset::new(offset);
                if partition_state.is_none_or(|state| state.is_available(offset)) {
                    selected.push((partition_id, offset));
                    if selected.len() == command.max_messages() {
                        break;
                    }
                }
            }

            if selected.len() == command.max_messages() {
                break;
            }
        }

        let mut consumed = Vec::with_capacity(selected.len());
        for (partition_id, offset) in selected {
            let envelope = self
                .state
                .topics
                .get(&topic_name)
                .and_then(|topic| topic.partition_log(partition_id))
                .and_then(|partition| partition.get(offset))
                .expect("selected offsets come from existing partition logs")
                .clone();
            let group_state = self
                .state
                .groups
                .entry(command.consumer_group_id().clone())
                .or_default();
            let partition_state = group_state.partition_state_mut(command.topic(), partition_id);
            let attempt_number = partition_state.next_attempt_number(offset);
            let delivery_id = deterministic_delivery_id(
                command.consumer_group_id(),
                command.topic(),
                partition_id,
                offset,
                attempt_number,
            )?;
            let lease_expires_at =
                add_millis(command.timestamp(), delivery_lease_millis, lease_field)?;
            let message_ref = MessageRef::new(topic_name.clone(), partition_id, offset);

            partition_state
                .attempt_counts
                .insert(offset, attempt_number);
            partition_state.mark_pending(offset, delivery_id.clone());
            self.state.pending.insert(
                delivery_id.clone(),
                PendingDelivery {
                    message_ref,
                    consumer_group_id: command.consumer_group_id().clone(),
                    consumer_id: command.consumer_id().clone(),
                    attempt_number,
                    lease_expires_at,
                },
            );

            consumed.push(ConsumedMessage::new(
                delivery_id,
                topic_name.clone(),
                partition_id,
                offset,
                envelope,
                command.consumer_group_id().clone(),
                command.consumer_id().clone(),
                attempt_number,
                command.timestamp(),
                lease_expires_at,
            ));
        }

        Ok(consumed)
    }

    pub(crate) fn ack(&mut self, command: AckCommand) -> BrokerResult<()> {
        let pending = self
            .pending_for_command(command.delivery_id(), command.consumer_id())?
            .clone();

        self.state.pending.remove(command.delivery_id());
        let partition_state =
            self.group_partition_state_mut(&pending.consumer_group_id, &pending.message_ref);
        partition_state
            .pending_by_offset
            .remove(&pending.message_ref.offset);
        partition_state.mark_acked(pending.message_ref.offset);

        Ok(())
    }

    pub(crate) fn nack(
        &mut self,
        command: NackCommand,
        retry_policy: RetryPolicy,
    ) -> BrokerResult<()> {
        let pending = self
            .pending_for_command(command.delivery_id(), command.consumer_id())?
            .clone();

        self.state.pending.remove(command.delivery_id());
        self.schedule_pending_retry_or_dlq(
            pending,
            retry_policy,
            command.timestamp(),
            DeadLetterReason::Manual(
                command
                    .reason()
                    .map_or_else(|| "nack".to_owned(), ToOwned::to_owned),
            ),
        )
    }

    pub(crate) fn retry_ready(
        &mut self,
        now: MessageTimestamp,
        retry_policy: RetryPolicy,
    ) -> BrokerResult<RetrySummary> {
        let expired_delivery_ids: Vec<_> = self
            .state
            .pending
            .iter()
            .filter(|(_delivery_id, pending)| pending.lease_expires_at <= now)
            .map(|(delivery_id, _pending)| delivery_id.clone())
            .collect();

        let lease_expired = expired_delivery_ids.len();
        let mut dead_lettered = 0;
        for delivery_id in expired_delivery_ids {
            let pending = self
                .state
                .pending
                .remove(&delivery_id)
                .expect("delivery id was collected from pending deliveries");
            let before = self.state.dead_letters.len();
            self.schedule_pending_retry_or_dlq(
                pending,
                retry_policy,
                now,
                DeadLetterReason::Expired,
            )?;
            dead_lettered += self.state.dead_letters.len() - before;
        }

        let ready_entries = self.ready_retry_entries(now);
        let retry_scheduled = ready_entries.len();
        let mut made_available = 0;
        for (consumer_group_id, message_ref) in ready_entries {
            let partition_state = self.group_partition_state_mut(&consumer_group_id, &message_ref);
            if partition_state
                .retry_by_offset
                .get(&message_ref.offset)
                .is_some_and(|entry| entry.ready_at <= now)
            {
                partition_state.mark_retry_available(message_ref.offset);
                made_available += 1;
            }
        }

        Ok(RetrySummary::new(
            retry_scheduled,
            lease_expired,
            made_available,
            dead_lettered,
        ))
    }

    pub(crate) fn list_dlq(&self, query: DlqQuery) -> BrokerResult<Vec<DeadLetterEntry>> {
        if let Some(topic) = query.topic()
            && !self.state.topics.contains_key(topic)
        {
            return Err(BrokerError::TopicNotFound {
                topic: topic.clone(),
            });
        }

        Ok(self
            .state
            .dead_letters
            .iter()
            .filter(|entry| query.topic().is_none_or(|topic| entry.topic() == topic))
            .filter(|entry| {
                query
                    .consumer_group_id()
                    .is_none_or(|group_id| entry.consumer_group_id() == group_id)
            })
            .cloned()
            .collect())
    }

    fn pending_for_command(
        &self,
        delivery_id: &DeliveryId,
        consumer_id: &msg_core::ConsumerId,
    ) -> BrokerResult<&PendingDelivery> {
        let pending =
            self.state
                .pending
                .get(delivery_id)
                .ok_or_else(|| BrokerError::DeliveryNotFound {
                    delivery_id: delivery_id.clone(),
                })?;

        if &pending.consumer_id != consumer_id {
            return Err(BrokerError::InvalidConsumer {
                delivery_id: delivery_id.clone(),
                expected: pending.consumer_id.clone(),
                actual: consumer_id.clone(),
            });
        }

        Ok(pending)
    }

    fn schedule_pending_retry_or_dlq(
        &mut self,
        pending: PendingDelivery,
        retry_policy: RetryPolicy,
        timestamp: MessageTimestamp,
        reason: DeadLetterReason,
    ) -> BrokerResult<()> {
        let partition_state =
            self.group_partition_state_mut(&pending.consumer_group_id, &pending.message_ref);
        partition_state
            .pending_by_offset
            .remove(&pending.message_ref.offset);

        let next_attempt = pending.attempt_number + 1;
        if next_attempt > retry_policy.max_attempts() {
            partition_state.mark_dead_lettered(pending.message_ref.offset);
            self.push_dead_letter(pending, reason, timestamp)?;
            return Ok(());
        }

        let ready_at = match retry_policy.backoff_millis() {
            Some(backoff) => add_millis(timestamp, backoff, "retry_policy.backoff_millis")?,
            None => timestamp,
        };
        partition_state.mark_retry_scheduled(
            pending.message_ref.offset,
            RetryEntry {
                message_ref: pending.message_ref.clone(),
                ready_at,
            },
        );

        Ok(())
    }

    fn push_dead_letter(
        &mut self,
        pending: PendingDelivery,
        reason: DeadLetterReason,
        timestamp: MessageTimestamp,
    ) -> BrokerResult<()> {
        let envelope = self.envelope_for(&pending.message_ref)?.clone();
        self.state.dead_letters.push(DeadLetterEntry::new(
            pending.message_ref.topic,
            pending.message_ref.partition_id,
            pending.message_ref.offset,
            envelope.id().clone(),
            envelope,
            pending.consumer_group_id,
            reason,
            pending.attempt_number,
            timestamp,
        ));

        Ok(())
    }

    fn ready_retry_entries(&self, now: MessageTimestamp) -> Vec<(ConsumerGroupId, MessageRef)> {
        self.state
            .groups
            .iter()
            .flat_map(|(consumer_group_id, group_state)| {
                group_state
                    .partition_states()
                    .filter_map(move |((_topic, _partition_id), partition_state)| {
                        let entries: Vec<_> = partition_state
                            .retry_by_offset
                            .values()
                            .filter(|entry| entry.ready_at <= now)
                            .map(|entry| (consumer_group_id.clone(), entry.message_ref.clone()))
                            .collect();
                        (!entries.is_empty()).then_some(entries)
                    })
                    .flatten()
            })
            .collect()
    }

    fn envelope_for(&self, message_ref: &MessageRef) -> BrokerResult<&MessageEnvelope> {
        self.state
            .topics
            .get(&message_ref.topic)
            .and_then(|topic| topic.partition_log(message_ref.partition_id))
            .and_then(|partition| partition.get(message_ref.offset))
            .ok_or_else(|| BrokerError::TopicNotFound {
                topic: message_ref.topic.clone(),
            })
    }

    fn group_partition_state_mut(
        &mut self,
        consumer_group_id: &ConsumerGroupId,
        message_ref: &MessageRef,
    ) -> &mut GroupPartitionState {
        self.state
            .groups
            .entry(consumer_group_id.clone())
            .or_default()
            .partition_state_mut(&message_ref.topic, message_ref.partition_id)
    }
}
