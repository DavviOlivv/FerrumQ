use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use msg_core::{
    ConsumerGroupId, ConsumerId, DeadLetterReason, DeliveryId, MessageEnvelope, MessageTimestamp,
    Offset, PartitionId, RetryPolicy, Topic, TopicName,
};
use msg_observability::metrics;
use msg_storage::{LogConfig, PartitionLog as StoragePartitionLog, StoredMessageRecord};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{info, info_span, warn};

use crate::{
    broker::BrokerConfig,
    commands::{
        AckCommand, ConsumeCommand, CreateTopicCommand, DlqQuery, NackCommand, PublishCommand,
    },
    delivery::{ConsumedMessage, DeadLetterEntry, PublishedMessage, RetrySummary},
    errors::{BrokerError, BrokerResult},
    helpers::{
        add_millis, advance_round_robin_partition, deterministic_delivery_id, keyed_partition,
        round_robin_partition,
    },
    state::{GroupPartitionState, GroupState, MessageRef, PendingDelivery, RetryEntry},
};

/// Result type used by the local durable broker.
pub type DurableBrokerResult<T> = Result<T, DurableBrokerError>;

/// Local durable broker configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableBrokerConfig {
    /// Root directory for durable broker files.
    pub root_dir: PathBuf,
    /// Delivery and retry configuration shared with the in-memory broker.
    pub broker_config: BrokerConfig,
    /// Segment roll threshold for durable message partition logs.
    pub max_segment_bytes: u64,
}

/// Read-only summary of the local durable broker state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableBrokerStatus {
    mode: &'static str,
    topic_count: usize,
    dlq_count: usize,
    root_dir: PathBuf,
}

impl DurableBrokerStatus {
    #[must_use]
    pub fn mode(&self) -> &'static str {
        self.mode
    }

    #[must_use]
    pub fn topic_count(&self) -> usize {
        self.topic_count
    }

    #[must_use]
    pub fn dlq_count(&self) -> usize {
        self.dlq_count
    }

    #[must_use]
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }
}

impl DurableBrokerConfig {
    #[must_use]
    pub fn new(
        root_dir: impl Into<PathBuf>,
        broker_config: BrokerConfig,
        max_segment_bytes: u64,
    ) -> Self {
        Self {
            root_dir: root_dir.into(),
            broker_config,
            max_segment_bytes,
        }
    }
}

/// Errors raised by durable broker orchestration and recovery.
#[derive(Debug, Error)]
pub enum DurableBrokerError {
    #[error(transparent)]
    Broker(#[from] BrokerError),

    #[error(transparent)]
    Storage(#[from] msg_storage::StorageError),

    #[error("broker state I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("broker state serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("broker state corruption in {path} at line {line}: {reason}")]
    StateCorruption {
        path: PathBuf,
        line: usize,
        reason: String,
    },

    #[error("broker state corruption: {reason}")]
    Corruption { reason: String },
}

/// Synchronous local durable broker service.
///
/// Messages are persisted in `msg-storage` partition logs under
/// `<root>/messages`. Topic metadata and delivery state transitions are
/// persisted in an append-only JSONL state log under
/// `<root>/broker-state/events.jsonl`.
///
/// The durable broker provides local filesystem at-least-once semantics:
///
/// - [`DurableBroker::publish`] returns success only after the message record
///   has been appended to the message log.
/// - [`DurableBroker::consume`] returns deliveries only after their delivery
///   state event has been appended and flushed.
/// - [`DurableBroker::ack`], [`DurableBroker::nack`], and
///   [`DurableBroker::retry_ready`] append and flush their state events before
///   mutating in-memory delivery state.
/// - Reopen recovery releases any unACKed in-flight deliveries so they can be
///   redelivered with the next deterministic attempt number.
/// - Unknown, duplicate, stale, ACK-after-NACK, and NACK-after-ACK delivery IDs
///   return [`BrokerError::DeliveryNotFound`].
/// - Complete malformed broker-state log lines are fatal
///   [`DurableBrokerError::StateCorruption`] errors. A final incomplete state
///   line without a trailing newline is truncated and ignored during recovery.
///
/// This is not exactly-once delivery, deduplication enforcement, replication,
/// compaction, or an fsync-tuned storage policy. Consumers must be idempotent.
#[derive(Debug)]
pub struct DurableBroker {
    config: DurableBrokerConfig,
    topics: BTreeMap<TopicName, DurableTopic>,
    state: DurableDeliveryState,
    state_log: StateLog,
}

#[derive(Debug, Default)]
struct DurableDeliveryState {
    groups: BTreeMap<ConsumerGroupId, GroupState>,
    pending: BTreeMap<DeliveryId, PendingDelivery>,
    dead_letters: Vec<DeadLetterEntry>,
}

#[derive(Debug)]
struct DurableTopic {
    topic: Topic,
    partitions: BTreeMap<PartitionId, StoragePartitionLog>,
    next_round_robin_partition: u32,
}

#[derive(Debug)]
struct StateLog {
    file: File,
    #[cfg(test)]
    fail_next_append: bool,
}

#[derive(Debug, Clone)]
struct SelectedDelivery {
    event: DeliveryEvent,
    message: ConsumedMessage,
}

#[derive(Debug, Clone)]
struct ComputedOutcome {
    outcome: DeliveryOutcome,
    dead_letter: Option<DeadLetterEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BrokerStateEvent {
    TopicCreated {
        topic: Topic,
    },
    MessagesConsumed {
        deliveries: Vec<DeliveryEvent>,
    },
    MessageAcked {
        delivery_id: DeliveryId,
        consumer_id: ConsumerId,
        topic: TopicName,
        partition_id: PartitionId,
        offset: Offset,
        consumer_group_id: ConsumerGroupId,
        timestamp: MessageTimestamp,
    },
    MessageNacked {
        delivery_id: DeliveryId,
        consumer_id: ConsumerId,
        topic: TopicName,
        partition_id: PartitionId,
        offset: Offset,
        consumer_group_id: ConsumerGroupId,
        attempt_number: u32,
        timestamp: MessageTimestamp,
        reason: DeadLetterReason,
        outcome: DeliveryOutcome,
    },
    RetryMaintenanceApplied {
        timestamp: MessageTimestamp,
        expired_outcomes: Vec<ExpiredDeliveryOutcome>,
        made_available: Vec<RetryAvailableEvent>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeliveryEvent {
    delivery_id: DeliveryId,
    consumer_id: ConsumerId,
    topic: TopicName,
    partition_id: PartitionId,
    offset: Offset,
    consumer_group_id: ConsumerGroupId,
    attempt_number: u32,
    delivered_at: MessageTimestamp,
    lease_expires_at: MessageTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExpiredDeliveryOutcome {
    delivery_id: DeliveryId,
    consumer_id: ConsumerId,
    topic: TopicName,
    partition_id: PartitionId,
    offset: Offset,
    consumer_group_id: ConsumerGroupId,
    attempt_number: u32,
    timestamp: MessageTimestamp,
    reason: DeadLetterReason,
    outcome: DeliveryOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryAvailableEvent {
    consumer_group_id: ConsumerGroupId,
    topic: TopicName,
    partition_id: PartitionId,
    offset: Offset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DeliveryOutcome {
    RetryScheduled {
        ready_at: MessageTimestamp,
    },
    DeadLettered {
        reason: DeadLetterReason,
        attempt_count: u32,
    },
}

impl DurableBroker {
    /// Opens or creates a durable broker rooted at `config.root_dir`.
    ///
    /// Recovery replays `<root>/broker-state/events.jsonl`, opens the message
    /// logs under `<root>/messages`, reconstructs round-robin partition
    /// selection, and releases recovered pending deliveries for at-least-once
    /// redelivery. Complete malformed state lines fail open with
    /// [`DurableBrokerError::StateCorruption`]; one final incomplete line is
    /// truncated and ignored.
    pub fn open(config: DurableBrokerConfig) -> DurableBrokerResult<Self> {
        let span = info_span!("durable_broker.open", operation = "open");
        let _guard = span.enter();

        let result = (|| {
            fs::create_dir_all(&config.root_dir)?;

            let state_dir = config.root_dir.join("broker-state");
            fs::create_dir_all(&state_dir)?;
            let state_path = state_dir.join("events.jsonl");
            let events = recover_state_events(&state_path)?;
            let recovered_events = events.len();
            let state_log = StateLog::open(&state_path)?;

            let mut broker = Self {
                config,
                topics: BTreeMap::new(),
                state: DurableDeliveryState::default(),
                state_log,
            };

            for event in events {
                broker.apply_recovered_event(event)?;
            }
            broker.recover_round_robin_state()?;
            broker.release_recovered_pending();

            info!(
                operation = "open",
                status = "success",
                topics = broker.topics.len(),
                dlq_entries = broker.state.dead_letters.len(),
                recovered_events
            );
            Ok(broker)
        })();

        match &result {
            Ok(_) => metrics::record_broker_open("success"),
            Err(error) => {
                metrics::record_broker_open("error");
                warn!(
                    operation = "open",
                    status = "error",
                    kind = durable_error_kind(error)
                );
            }
        }

        result
    }

    /// Creates topic metadata and opens its durable partition logs.
    ///
    /// The topic-created event is appended and flushed to broker state before
    /// the topic becomes visible in memory. Recreating a recovered topic returns
    /// [`BrokerError::TopicAlreadyExists`].
    pub fn create_topic(&mut self, command: CreateTopicCommand) -> DurableBrokerResult<Topic> {
        let topic_name = command.name().clone();
        let partitions = command.config().partition_count();
        let span = info_span!(
            "durable_broker.create_topic",
            operation = "create_topic",
            topic = %topic_name,
            partitions
        );
        let _guard = span.enter();

        let result = (|| {
            let topic = Topic::new(command.name().clone(), command.config());
            if self.topics.contains_key(topic.name()) {
                return Err(BrokerError::TopicAlreadyExists {
                    topic: topic.name().clone(),
                }
                .into());
            }

            let durable_topic = self.open_topic_logs(topic.clone())?;
            self.state_log.append(&BrokerStateEvent::TopicCreated {
                topic: topic.clone(),
            })?;
            self.topics.insert(topic.name().clone(), durable_topic);

            Ok(topic)
        })();

        match &result {
            Ok(topic) => {
                metrics::record_broker_topic_create("success");
                info!(
                    operation = "create_topic",
                    status = "success",
                    topic = %topic.name(),
                    partitions = topic.partition_count()
                );
            }
            Err(error) => {
                metrics::record_broker_topic_create("error");
                warn!(
                    operation = "create_topic",
                    status = "error",
                    topic = %topic_name,
                    kind = durable_error_kind(error)
                );
            }
        }

        result
    }

    /// Lists topics in deterministic topic-name order.
    #[must_use]
    pub fn list_topics(&self) -> Vec<Topic> {
        self.topics
            .values()
            .map(|durable_topic| durable_topic.topic.clone())
            .collect()
    }

    /// Returns topic metadata for a known topic.
    pub fn get_topic(&self, name: &TopicName) -> DurableBrokerResult<Topic> {
        self.topics
            .get(name)
            .map(|durable_topic| durable_topic.topic.clone())
            .ok_or_else(|| {
                BrokerError::TopicNotFound {
                    topic: name.clone(),
                }
                .into()
            })
    }

    /// Returns a read-only summary of the local durable broker state.
    #[must_use]
    pub fn status(&self) -> DurableBrokerStatus {
        DurableBrokerStatus {
            mode: "local-durable",
            topic_count: self.topics.len(),
            dlq_count: self.state.dead_letters.len(),
            root_dir: self.config.root_dir.clone(),
        }
    }

    /// Publishes one envelope to a topic partition.
    ///
    /// Success means the message record append completed in `msg-storage`. If
    /// append fails, no durable broker metadata is advanced and no phantom
    /// message is exposed by recovery.
    pub fn publish(&mut self, command: PublishCommand) -> DurableBrokerResult<PublishedMessage> {
        let topic_for_log = command.topic().clone();
        let message_id_for_log = command.envelope().id().clone();
        let span = info_span!(
            "durable_broker.publish",
            operation = "publish",
            topic = %topic_for_log,
            message_id = %message_id_for_log
        );
        let _guard = span.enter();

        let result = (|| {
            let (topic_name, envelope) = command.into_parts();
            let durable_topic =
                self.topics
                    .get_mut(&topic_name)
                    .ok_or_else(|| BrokerError::TopicNotFound {
                        topic: topic_name.clone(),
                    })?;

            let should_advance_round_robin = envelope.partition_key().is_none();
            let partition_id = select_durable_partition(durable_topic, &envelope);
            let message_id = envelope.id().clone();
            let offset = durable_topic
                .partition_log_mut(partition_id)
                .expect("durable topic logs are opened from Topic partition ids")
                .append(envelope)?;

            if should_advance_round_robin {
                durable_topic.advance_round_robin_partition();
            }

            Ok(PublishedMessage::new(
                topic_name,
                partition_id,
                offset,
                message_id,
            ))
        })();

        match &result {
            Ok(message) => {
                metrics::record_broker_publish("success");
                info!(
                    operation = "publish",
                    status = "success",
                    topic = %message.topic(),
                    partition = message.partition_id().value(),
                    offset = message.offset().value(),
                    message_id = %message.message_id()
                );
            }
            Err(error) => {
                metrics::record_broker_publish("error");
                warn!(
                    operation = "publish",
                    status = "error",
                    topic = %topic_for_log,
                    message_id = %message_id_for_log,
                    kind = durable_error_kind(error)
                );
            }
        }

        result
    }

    /// Consumes up to `max_messages` available messages for a consumer group.
    ///
    /// Selected deliveries are appended and flushed to broker state before they
    /// are marked pending in memory or returned to the caller. Pending,
    /// retry-scheduled, ACKed, and DLQ offsets are not delivered. Recovered
    /// unACKed pending deliveries are made available again and redelivered with
    /// incremented attempts.
    pub fn consume(
        &mut self,
        command: ConsumeCommand,
    ) -> DurableBrokerResult<Vec<ConsumedMessage>> {
        let topic_for_log = command.topic().clone();
        let consumer_group_for_log = command.consumer_group_id().clone();
        let consumer_for_log = command.consumer_id().clone();
        let max_messages = command.max_messages();
        let span = info_span!(
            "durable_broker.consume",
            operation = "consume",
            topic = %topic_for_log,
            consumer_group = %consumer_group_for_log,
            consumer_id = %consumer_for_log,
            max_messages
        );
        let _guard = span.enter();

        let result = (|| {
            if !self.topics.contains_key(command.topic()) {
                return Err(BrokerError::TopicNotFound {
                    topic: command.topic().clone(),
                }
                .into());
            }

            if command.max_messages() == 0 {
                return Ok(Vec::new());
            }

            let selected = self.select_available_deliveries(&command)?;
            if selected.is_empty() {
                return Ok(Vec::new());
            }

            let deliveries = selected
                .iter()
                .map(|selection| selection.event.clone())
                .collect();
            self.state_log
                .append(&BrokerStateEvent::MessagesConsumed { deliveries })?;

            for selection in &selected {
                self.apply_consumed_delivery(&selection.event);
            }

            Ok(selected
                .into_iter()
                .map(|selection| selection.message)
                .collect())
        })();

        match &result {
            Ok(messages) => {
                metrics::record_broker_consume("success");
                metrics::record_broker_deliveries_created(messages.len());
                info!(
                    operation = "consume",
                    status = "success",
                    topic = %topic_for_log,
                    consumer_group = %consumer_group_for_log,
                    consumer_id = %consumer_for_log,
                    delivered = messages.len()
                );
                for message in messages {
                    info!(
                        operation = "delivery_created",
                        topic = %message.topic(),
                        partition = message.partition_id().value(),
                        offset = message.offset().value(),
                        message_id = %message.envelope().id(),
                        delivery_id = %message.delivery_id(),
                        consumer_group = %message.consumer_group_id(),
                        consumer_id = %message.consumer_id()
                    );
                }
            }
            Err(error) => {
                metrics::record_broker_consume("error");
                warn!(
                    operation = "consume",
                    status = "error",
                    topic = %topic_for_log,
                    consumer_group = %consumer_group_for_log,
                    consumer_id = %consumer_for_log,
                    kind = durable_error_kind(error)
                );
            }
        }

        result
    }

    /// ACKs a pending delivery.
    ///
    /// The ACK state event is appended and flushed before pending state is
    /// removed and the partition cursor is advanced. Unknown, duplicate, stale,
    /// ACK-after-NACK, and ACK-after-DLQ delivery IDs return
    /// [`BrokerError::DeliveryNotFound`].
    pub fn ack(&mut self, command: AckCommand) -> DurableBrokerResult<()> {
        let delivery_id_for_log = command.delivery_id().clone();
        let consumer_for_log = command.consumer_id().clone();
        let span = info_span!(
            "durable_broker.ack",
            operation = "ack",
            delivery_id = %delivery_id_for_log,
            consumer_id = %consumer_for_log
        );
        let _guard = span.enter();

        let result = (|| {
            let pending = self
                .pending_for_command(command.delivery_id(), command.consumer_id())?
                .clone();
            let event = BrokerStateEvent::MessageAcked {
                delivery_id: command.delivery_id().clone(),
                consumer_id: command.consumer_id().clone(),
                topic: pending.message_ref.topic.clone(),
                partition_id: pending.message_ref.partition_id,
                offset: pending.message_ref.offset,
                consumer_group_id: pending.consumer_group_id.clone(),
                timestamp: command.timestamp(),
            };

            self.state_log.append(&event)?;
            self.apply_ack(command.delivery_id(), pending);

            Ok(())
        })();

        match &result {
            Ok(()) => {
                metrics::record_broker_ack("success");
                info!(
                    operation = "ack",
                    status = "success",
                    delivery_id = %delivery_id_for_log,
                    consumer_id = %consumer_for_log
                );
            }
            Err(error) => {
                metrics::record_broker_ack("error");
                warn!(
                    operation = "ack",
                    status = "error",
                    delivery_id = %delivery_id_for_log,
                    consumer_id = %consumer_for_log,
                    kind = durable_error_kind(error)
                );
            }
        }

        result
    }

    /// NACKs a pending delivery and schedules retry or DLQ routing.
    ///
    /// The NACK outcome event is appended and flushed before pending state is
    /// removed. If the next attempt is within the retry policy the message is
    /// retry-scheduled; otherwise it is dead-lettered. Unknown, duplicate,
    /// stale, NACK-after-ACK, and NACK-after-DLQ delivery IDs return
    /// [`BrokerError::DeliveryNotFound`].
    pub fn nack(&mut self, command: NackCommand) -> DurableBrokerResult<()> {
        let delivery_id_for_log = command.delivery_id().clone();
        let consumer_for_log = command.consumer_id().clone();
        let span = info_span!(
            "durable_broker.nack",
            operation = "nack",
            delivery_id = %delivery_id_for_log,
            consumer_id = %consumer_for_log
        );
        let _guard = span.enter();

        let result = (|| {
            let pending = self
                .pending_for_command(command.delivery_id(), command.consumer_id())?
                .clone();
            let reason = DeadLetterReason::Manual(
                command
                    .reason()
                    .map_or_else(|| "nack".to_owned(), ToOwned::to_owned),
            );
            let computed = self.compute_pending_outcome(
                &pending,
                self.config.broker_config.retry_policy(),
                command.timestamp(),
                reason.clone(),
            )?;
            let event = BrokerStateEvent::MessageNacked {
                delivery_id: command.delivery_id().clone(),
                consumer_id: command.consumer_id().clone(),
                topic: pending.message_ref.topic.clone(),
                partition_id: pending.message_ref.partition_id,
                offset: pending.message_ref.offset,
                consumer_group_id: pending.consumer_group_id.clone(),
                attempt_number: pending.attempt_number,
                timestamp: command.timestamp(),
                reason,
                outcome: computed.outcome.clone(),
            };
            let dead_lettered = computed.dead_letter.is_some();

            self.state_log.append(&event)?;
            self.apply_pending_outcome(
                command.delivery_id(),
                pending,
                &computed.outcome,
                computed.dead_letter,
            )?;

            Ok(dead_lettered)
        })();

        match &result {
            Ok(dead_lettered) => {
                metrics::record_broker_nack("success");
                if *dead_lettered {
                    metrics::record_broker_dlq_transition("nack", 1);
                }
                info!(
                    operation = "nack",
                    status = "success",
                    delivery_id = %delivery_id_for_log,
                    consumer_id = %consumer_for_log,
                    dead_lettered = *dead_lettered
                );
            }
            Err(error) => {
                metrics::record_broker_nack("error");
                warn!(
                    operation = "nack",
                    status = "error",
                    delivery_id = %delivery_id_for_log,
                    consumer_id = %consumer_for_log,
                    kind = durable_error_kind(error)
                );
            }
        }

        result.map(|_dead_lettered| ())
    }

    /// Applies deterministic retry maintenance at the injected timestamp.
    ///
    /// The maintenance event records lease-expiry outcomes and ready retry
    /// offsets, then is appended and flushed before in-memory retry, pending,
    /// or DLQ state is mutated.
    pub fn retry_ready(&mut self, now: MessageTimestamp) -> DurableBrokerResult<RetrySummary> {
        let span = info_span!(
            "durable_broker.retry_ready",
            operation = "retry_ready",
            now_unix_ms = now.as_unix_millis()
        );
        let _guard = span.enter();

        let result = (|| {
            let expired_pending: Vec<_> = self
                .state
                .pending
                .iter()
                .filter(|(_delivery_id, pending)| pending.lease_expires_at <= now)
                .map(|(delivery_id, pending)| (delivery_id.clone(), pending.clone()))
                .collect();
            let lease_expired = expired_pending.len();

            let mut expired_outcomes = Vec::with_capacity(expired_pending.len());
            let mut computed_outcomes = Vec::with_capacity(expired_pending.len());
            let mut made_available: Vec<_> = self
                .ready_retry_entries(now)
                .into_iter()
                .map(|(consumer_group_id, message_ref)| RetryAvailableEvent {
                    consumer_group_id,
                    topic: message_ref.topic,
                    partition_id: message_ref.partition_id,
                    offset: message_ref.offset,
                })
                .collect();

            let mut dead_lettered = 0;
            for (delivery_id, pending) in expired_pending {
                let reason = DeadLetterReason::Expired;
                let computed = self.compute_pending_outcome(
                    &pending,
                    self.config.broker_config.retry_policy(),
                    now,
                    reason.clone(),
                )?;
                if matches!(&computed.outcome, DeliveryOutcome::DeadLettered { .. }) {
                    dead_lettered += 1;
                }
                if let DeliveryOutcome::RetryScheduled { ready_at } = &computed.outcome
                    && *ready_at <= now
                {
                    made_available.push(RetryAvailableEvent {
                        consumer_group_id: pending.consumer_group_id.clone(),
                        topic: pending.message_ref.topic.clone(),
                        partition_id: pending.message_ref.partition_id,
                        offset: pending.message_ref.offset,
                    });
                }

                expired_outcomes.push(ExpiredDeliveryOutcome {
                    delivery_id: delivery_id.clone(),
                    consumer_id: pending.consumer_id.clone(),
                    topic: pending.message_ref.topic.clone(),
                    partition_id: pending.message_ref.partition_id,
                    offset: pending.message_ref.offset,
                    consumer_group_id: pending.consumer_group_id.clone(),
                    attempt_number: pending.attempt_number,
                    timestamp: now,
                    reason,
                    outcome: computed.outcome.clone(),
                });
                computed_outcomes.push((delivery_id, pending, computed));
            }

            self.state_log
                .append(&BrokerStateEvent::RetryMaintenanceApplied {
                    timestamp: now,
                    expired_outcomes,
                    made_available: made_available.clone(),
                })?;

            for (delivery_id, pending, computed) in computed_outcomes {
                self.apply_pending_outcome(
                    &delivery_id,
                    pending,
                    &computed.outcome,
                    computed.dead_letter,
                )?;
            }

            let mut made_available_count = 0;
            for event in &made_available {
                if self.apply_retry_available(event, now)? {
                    made_available_count += 1;
                }
            }

            Ok(RetrySummary::new(
                made_available.len(),
                lease_expired,
                made_available_count,
                dead_lettered,
            ))
        })();

        match &result {
            Ok(summary) => {
                metrics::record_broker_retry_maintenance("success");
                metrics::record_broker_dlq_transition("expired", summary.dead_lettered());
                info!(
                    operation = "retry_ready",
                    status = "success",
                    retry_scheduled = summary.retry_scheduled(),
                    lease_expired = summary.lease_expired(),
                    made_available = summary.made_available(),
                    dead_lettered = summary.dead_lettered()
                );
            }
            Err(error) => {
                metrics::record_broker_retry_maintenance("error");
                warn!(
                    operation = "retry_ready",
                    status = "error",
                    kind = durable_error_kind(error)
                );
            }
        }

        result
    }

    /// Lists recovered and in-memory dead-letter entries matching `query`.
    ///
    /// DLQ entries are durable broker-state outcomes and are not delivered
    /// again by normal consume after reopen.
    pub fn list_dlq(&self, query: DlqQuery) -> DurableBrokerResult<Vec<DeadLetterEntry>> {
        if let Some(topic) = query.topic()
            && !self.topics.contains_key(topic)
        {
            return Err(BrokerError::TopicNotFound {
                topic: topic.clone(),
            }
            .into());
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

    fn select_available_deliveries(
        &self,
        command: &ConsumeCommand,
    ) -> DurableBrokerResult<Vec<SelectedDelivery>> {
        let topic_name = command.topic().clone();
        let durable_topic =
            self.topics
                .get(&topic_name)
                .ok_or_else(|| BrokerError::TopicNotFound {
                    topic: topic_name.clone(),
                })?;

        let mut selected = Vec::new();
        for partition_id in durable_topic.partition_ids() {
            let partition_log = durable_topic
                .partition_log(partition_id)
                .expect("durable topic logs are opened from Topic partition ids");
            let records = read_all_records(partition_log)?;

            for record in records {
                let partition_state = self
                    .state
                    .groups
                    .get(command.consumer_group_id())
                    .and_then(|group_state| group_state.partition_state(&topic_name, partition_id));
                if !partition_state.is_none_or(|state| state.is_available(record.offset)) {
                    continue;
                }

                let attempt_number =
                    partition_state.map_or(1, |state| state.next_attempt_number(record.offset));
                let delivery_id = deterministic_delivery_id(
                    command.consumer_group_id(),
                    command.topic(),
                    partition_id,
                    record.offset,
                    attempt_number,
                )?;
                let (delivery_lease_millis, lease_field) =
                    command.delivery_lease_millis().map_or_else(
                        || {
                            (
                                self.config.broker_config.delivery_lease_millis(),
                                "delivery_lease_millis",
                            )
                        },
                        |lease_millis| (lease_millis, "lease_ms"),
                    );
                let lease_expires_at =
                    add_millis(command.timestamp(), delivery_lease_millis, lease_field)?;
                let event = DeliveryEvent {
                    delivery_id: delivery_id.clone(),
                    consumer_id: command.consumer_id().clone(),
                    topic: topic_name.clone(),
                    partition_id,
                    offset: record.offset,
                    consumer_group_id: command.consumer_group_id().clone(),
                    attempt_number,
                    delivered_at: command.timestamp(),
                    lease_expires_at,
                };
                let message = ConsumedMessage::new(
                    delivery_id,
                    topic_name.clone(),
                    partition_id,
                    record.offset,
                    record.envelope,
                    command.consumer_group_id().clone(),
                    command.consumer_id().clone(),
                    attempt_number,
                    command.timestamp(),
                    lease_expires_at,
                );
                selected.push(SelectedDelivery { event, message });

                if selected.len() == command.max_messages() {
                    break;
                }
            }

            if selected.len() == command.max_messages() {
                break;
            }
        }

        Ok(selected)
    }

    fn apply_recovered_event(&mut self, event: BrokerStateEvent) -> DurableBrokerResult<()> {
        match event {
            BrokerStateEvent::TopicCreated { topic } => self.replay_topic_created(topic),
            BrokerStateEvent::MessagesConsumed { deliveries } => {
                for delivery in deliveries {
                    self.replay_consumed_delivery(delivery)?;
                }
                Ok(())
            }
            BrokerStateEvent::MessageAcked {
                delivery_id,
                consumer_id,
                topic,
                partition_id,
                offset,
                consumer_group_id,
                timestamp: _,
            } => {
                let message_ref = MessageRef::new(topic, partition_id, offset);
                let pending = self.pending_for_replay(
                    &delivery_id,
                    &consumer_id,
                    &consumer_group_id,
                    &message_ref,
                    None,
                )?;
                self.apply_ack(&delivery_id, pending);
                Ok(())
            }
            BrokerStateEvent::MessageNacked {
                delivery_id,
                consumer_id,
                topic,
                partition_id,
                offset,
                consumer_group_id,
                attempt_number,
                timestamp,
                reason: _,
                outcome,
            } => {
                let message_ref = MessageRef::new(topic, partition_id, offset);
                let pending = self.pending_for_replay(
                    &delivery_id,
                    &consumer_id,
                    &consumer_group_id,
                    &message_ref,
                    Some(attempt_number),
                )?;
                let dead_letter = self.dead_letter_for_outcome(&pending, &outcome, timestamp)?;
                self.apply_pending_outcome(&delivery_id, pending, &outcome, dead_letter)
            }
            BrokerStateEvent::RetryMaintenanceApplied {
                timestamp,
                expired_outcomes,
                made_available,
            } => {
                for expired in expired_outcomes {
                    self.replay_expired_outcome(expired)?;
                }
                for available in made_available {
                    self.apply_retry_available(&available, timestamp)?;
                }
                Ok(())
            }
        }
    }

    fn replay_topic_created(&mut self, topic: Topic) -> DurableBrokerResult<()> {
        if self.topics.contains_key(topic.name()) {
            return Err(corruption(format!(
                "duplicate topic_created event for {}",
                topic.name()
            )));
        }

        let durable_topic = self.open_topic_logs(topic.clone())?;
        self.topics.insert(topic.name().clone(), durable_topic);
        Ok(())
    }

    fn replay_consumed_delivery(&mut self, delivery: DeliveryEvent) -> DurableBrokerResult<()> {
        self.ensure_message_exists(&delivery.topic, delivery.partition_id, delivery.offset)?;

        if self.state.pending.contains_key(&delivery.delivery_id) {
            return Err(corruption(format!(
                "duplicate pending delivery {}",
                delivery.delivery_id
            )));
        }

        let is_available = self
            .state
            .groups
            .get(&delivery.consumer_group_id)
            .and_then(|group_state| {
                group_state.partition_state(&delivery.topic, delivery.partition_id)
            })
            .is_none_or(|state| state.is_available(delivery.offset));
        if !is_available {
            return Err(corruption(format!(
                "delivery {} consumed unavailable offset {}:{}:{}",
                delivery.delivery_id,
                delivery.topic,
                delivery.partition_id.value(),
                delivery.offset.value()
            )));
        }

        self.apply_consumed_delivery(&delivery);
        Ok(())
    }

    fn replay_expired_outcome(
        &mut self,
        expired: ExpiredDeliveryOutcome,
    ) -> DurableBrokerResult<()> {
        let message_ref = MessageRef::new(expired.topic, expired.partition_id, expired.offset);
        let pending = self.pending_for_replay(
            &expired.delivery_id,
            &expired.consumer_id,
            &expired.consumer_group_id,
            &message_ref,
            Some(expired.attempt_number),
        )?;
        let dead_letter =
            self.dead_letter_for_outcome(&pending, &expired.outcome, expired.timestamp)?;
        self.apply_pending_outcome(&expired.delivery_id, pending, &expired.outcome, dead_letter)
    }

    fn apply_consumed_delivery(&mut self, delivery: &DeliveryEvent) {
        let message_ref = MessageRef::new(
            delivery.topic.clone(),
            delivery.partition_id,
            delivery.offset,
        );
        let partition_state =
            self.group_partition_state_mut(&delivery.consumer_group_id, &message_ref);
        partition_state
            .attempt_counts
            .insert(delivery.offset, delivery.attempt_number);
        partition_state.mark_pending(delivery.offset, delivery.delivery_id.clone());
        self.state.pending.insert(
            delivery.delivery_id.clone(),
            PendingDelivery {
                message_ref,
                consumer_group_id: delivery.consumer_group_id.clone(),
                consumer_id: delivery.consumer_id.clone(),
                attempt_number: delivery.attempt_number,
                lease_expires_at: delivery.lease_expires_at,
            },
        );
    }

    fn apply_ack(&mut self, delivery_id: &DeliveryId, pending: PendingDelivery) {
        self.state.pending.remove(delivery_id);
        let partition_state =
            self.group_partition_state_mut(&pending.consumer_group_id, &pending.message_ref);
        partition_state
            .pending_by_offset
            .remove(&pending.message_ref.offset);
        partition_state.mark_acked(pending.message_ref.offset);
    }

    fn apply_pending_outcome(
        &mut self,
        delivery_id: &DeliveryId,
        pending: PendingDelivery,
        outcome: &DeliveryOutcome,
        dead_letter: Option<DeadLetterEntry>,
    ) -> DurableBrokerResult<()> {
        self.state.pending.remove(delivery_id);
        let partition_state =
            self.group_partition_state_mut(&pending.consumer_group_id, &pending.message_ref);
        partition_state
            .pending_by_offset
            .remove(&pending.message_ref.offset);

        match outcome {
            DeliveryOutcome::RetryScheduled { ready_at } => {
                partition_state.mark_retry_scheduled(
                    pending.message_ref.offset,
                    RetryEntry {
                        message_ref: pending.message_ref,
                        ready_at: *ready_at,
                    },
                );
            }
            DeliveryOutcome::DeadLettered { .. } => {
                partition_state.mark_dead_lettered(pending.message_ref.offset);
                let dead_letter = dead_letter.ok_or_else(|| {
                    corruption("dead-letter outcome missing dead-letter entry".to_owned())
                })?;
                info!(
                    operation = "dlq_transition",
                    topic = %dead_letter.topic(),
                    partition = dead_letter.partition_id().value(),
                    offset = dead_letter.offset().value(),
                    message_id = %dead_letter.message_id(),
                    consumer_group = %dead_letter.consumer_group_id()
                );
                self.state.dead_letters.push(dead_letter);
            }
        }

        Ok(())
    }

    fn apply_retry_available(
        &mut self,
        event: &RetryAvailableEvent,
        timestamp: MessageTimestamp,
    ) -> DurableBrokerResult<bool> {
        let message_ref = MessageRef::new(event.topic.clone(), event.partition_id, event.offset);
        let partition_state =
            self.group_partition_state_mut(&event.consumer_group_id, &message_ref);
        let Some(entry) = partition_state.retry_by_offset.get(&event.offset) else {
            return Err(corruption(format!(
                "retry availability event for unscheduled offset {}:{}:{}",
                event.topic,
                event.partition_id.value(),
                event.offset.value()
            )));
        };
        if entry.ready_at > timestamp {
            return Err(corruption(format!(
                "retry availability event before ready_at for offset {}:{}:{}",
                event.topic,
                event.partition_id.value(),
                event.offset.value()
            )));
        }

        partition_state.mark_retry_available(event.offset);
        Ok(true)
    }

    fn compute_pending_outcome(
        &self,
        pending: &PendingDelivery,
        retry_policy: RetryPolicy,
        timestamp: MessageTimestamp,
        reason: DeadLetterReason,
    ) -> DurableBrokerResult<ComputedOutcome> {
        let next_attempt =
            pending
                .attempt_number
                .checked_add(1)
                .ok_or(BrokerError::InvalidConfig {
                    field: "attempt_number",
                    reason: "attempt number overflow",
                })?;

        if next_attempt > retry_policy.max_attempts() {
            let dead_letter = self.build_dead_letter_entry(
                pending,
                reason.clone(),
                pending.attempt_number,
                timestamp,
            )?;
            return Ok(ComputedOutcome {
                outcome: DeliveryOutcome::DeadLettered {
                    reason,
                    attempt_count: pending.attempt_number,
                },
                dead_letter: Some(dead_letter),
            });
        }

        let ready_at = match retry_policy.backoff_millis() {
            Some(backoff) => add_millis(timestamp, backoff, "retry_policy.backoff_millis")?,
            None => timestamp,
        };
        Ok(ComputedOutcome {
            outcome: DeliveryOutcome::RetryScheduled { ready_at },
            dead_letter: None,
        })
    }

    fn dead_letter_for_outcome(
        &self,
        pending: &PendingDelivery,
        outcome: &DeliveryOutcome,
        timestamp: MessageTimestamp,
    ) -> DurableBrokerResult<Option<DeadLetterEntry>> {
        match outcome {
            DeliveryOutcome::RetryScheduled { .. } => Ok(None),
            DeliveryOutcome::DeadLettered {
                reason,
                attempt_count,
            } => Ok(Some(self.build_dead_letter_entry(
                pending,
                reason.clone(),
                *attempt_count,
                timestamp,
            )?)),
        }
    }

    fn build_dead_letter_entry(
        &self,
        pending: &PendingDelivery,
        reason: DeadLetterReason,
        attempt_count: u32,
        timestamp: MessageTimestamp,
    ) -> DurableBrokerResult<DeadLetterEntry> {
        let envelope = self.envelope_for(&pending.message_ref)?;
        Ok(DeadLetterEntry::new(
            pending.message_ref.topic.clone(),
            pending.message_ref.partition_id,
            pending.message_ref.offset,
            envelope.id().clone(),
            envelope,
            pending.consumer_group_id.clone(),
            reason,
            attempt_count,
            timestamp,
        ))
    }

    fn envelope_for(&self, message_ref: &MessageRef) -> DurableBrokerResult<MessageEnvelope> {
        let durable_topic =
            self.topics
                .get(&message_ref.topic)
                .ok_or_else(|| BrokerError::TopicNotFound {
                    topic: message_ref.topic.clone(),
                })?;
        let partition_log = durable_topic
            .partition_log(message_ref.partition_id)
            .ok_or_else(|| {
                corruption(format!(
                    "missing partition {} for topic {}",
                    message_ref.partition_id.value(),
                    message_ref.topic
                ))
            })?;
        let records = partition_log.read_from(message_ref.offset, 1)?;
        let record = records.into_iter().next().ok_or_else(|| {
            corruption(format!(
                "missing message record {}:{}:{}",
                message_ref.topic,
                message_ref.partition_id.value(),
                message_ref.offset.value()
            ))
        })?;
        if record.offset != message_ref.offset {
            return Err(corruption(format!(
                "read offset {} for requested offset {}",
                record.offset.value(),
                message_ref.offset.value()
            )));
        }

        Ok(record.envelope)
    }

    fn pending_for_command(
        &self,
        delivery_id: &DeliveryId,
        consumer_id: &ConsumerId,
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

    fn pending_for_replay(
        &self,
        delivery_id: &DeliveryId,
        consumer_id: &ConsumerId,
        consumer_group_id: &ConsumerGroupId,
        message_ref: &MessageRef,
        attempt_number: Option<u32>,
    ) -> DurableBrokerResult<PendingDelivery> {
        let pending = self
            .state
            .pending
            .get(delivery_id)
            .ok_or_else(|| corruption(format!("missing pending delivery {delivery_id}")))?;

        if &pending.consumer_id != consumer_id {
            return Err(corruption(format!(
                "delivery {delivery_id} consumer mismatch: expected {}, got {}",
                pending.consumer_id, consumer_id
            )));
        }
        if &pending.consumer_group_id != consumer_group_id {
            return Err(corruption(format!(
                "delivery {delivery_id} consumer group mismatch"
            )));
        }
        if &pending.message_ref != message_ref {
            return Err(corruption(format!(
                "delivery {delivery_id} message reference mismatch"
            )));
        }
        if let Some(attempt_number) = attempt_number
            && pending.attempt_number != attempt_number
        {
            return Err(corruption(format!(
                "delivery {delivery_id} attempt mismatch: expected {}, got {attempt_number}",
                pending.attempt_number
            )));
        }

        Ok(pending.clone())
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

    fn ensure_message_exists(
        &self,
        topic: &TopicName,
        partition_id: PartitionId,
        offset: Offset,
    ) -> DurableBrokerResult<()> {
        let durable_topic = self
            .topics
            .get(topic)
            .ok_or_else(|| corruption(format!("state event references unknown topic {topic}")))?;
        let partition_log = durable_topic.partition_log(partition_id).ok_or_else(|| {
            corruption(format!(
                "state event references unknown partition {} for topic {topic}",
                partition_id.value()
            ))
        })?;
        if partition_log.next_offset() <= offset {
            return Err(corruption(format!(
                "state event references missing offset {}:{}:{}",
                topic,
                partition_id.value(),
                offset.value()
            )));
        }

        Ok(())
    }

    fn recover_round_robin_state(&mut self) -> DurableBrokerResult<()> {
        for durable_topic in self.topics.values_mut() {
            let mut unkeyed_message_count = 0_u64;
            for partition_id in durable_topic.partition_ids().collect::<Vec<_>>() {
                let partition_log = durable_topic
                    .partition_log(partition_id)
                    .expect("durable topic logs are opened from Topic partition ids");
                let records = read_all_records(partition_log)?;
                unkeyed_message_count += records
                    .iter()
                    .filter(|record| record.envelope.partition_key().is_none())
                    .count() as u64;
            }

            durable_topic.set_next_round_robin_partition(
                (unkeyed_message_count % u64::from(durable_topic.partition_count())) as u32,
            );
        }

        Ok(())
    }

    fn release_recovered_pending(&mut self) {
        let recovered_pending: Vec<_> = self.state.pending.values().cloned().collect();
        self.state.pending.clear();

        for pending in recovered_pending {
            let partition_state =
                self.group_partition_state_mut(&pending.consumer_group_id, &pending.message_ref);
            partition_state
                .pending_by_offset
                .remove(&pending.message_ref.offset);
        }
    }

    fn open_topic_logs(&self, topic: Topic) -> DurableBrokerResult<DurableTopic> {
        DurableTopic::open(self.storage_log_config(), topic)
    }

    fn storage_log_config(&self) -> LogConfig {
        LogConfig {
            root_dir: self.config.root_dir.join("messages"),
            max_segment_bytes: self.config.max_segment_bytes,
        }
    }

    #[cfg(test)]
    fn fail_next_state_log_append(&mut self) {
        self.state_log.fail_next_append();
    }
}

impl DurableTopic {
    fn open(config: LogConfig, topic: Topic) -> DurableBrokerResult<Self> {
        let mut partitions = BTreeMap::new();
        for partition_id in topic.partition_ids() {
            let partition_log =
                StoragePartitionLog::open(config.clone(), topic.name(), partition_id)?;
            partitions.insert(partition_id, partition_log);
        }

        Ok(Self {
            topic,
            partitions,
            next_round_robin_partition: 0,
        })
    }

    fn partition_log(&self, partition_id: PartitionId) -> Option<&StoragePartitionLog> {
        self.partitions.get(&partition_id)
    }

    fn partition_log_mut(&mut self, partition_id: PartitionId) -> Option<&mut StoragePartitionLog> {
        self.partitions.get_mut(&partition_id)
    }

    fn partition_ids(&self) -> impl Iterator<Item = PartitionId> + '_ {
        self.partitions.keys().copied()
    }

    fn partition_count(&self) -> u32 {
        self.topic.partition_count()
    }

    fn next_round_robin_partition(&self) -> u32 {
        self.next_round_robin_partition
    }

    fn set_next_round_robin_partition(&mut self, next_round_robin_partition: u32) {
        self.next_round_robin_partition = next_round_robin_partition % self.partition_count();
    }

    fn advance_round_robin_partition(&mut self) {
        self.next_round_robin_partition =
            advance_round_robin_partition(self.next_round_robin_partition, self.partition_count());
    }
}

impl StateLog {
    fn open(path: &Path) -> DurableBrokerResult<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file,
            #[cfg(test)]
            fail_next_append: false,
        })
    }

    fn append(&mut self, event: &BrokerStateEvent) -> DurableBrokerResult<()> {
        #[cfg(test)]
        if self.fail_next_append {
            self.fail_next_append = false;
            return Err(std::io::Error::other("injected broker-state append failure").into());
        }

        let bytes = serde_json::to_vec(event)?;
        self.file.write_all(&bytes)?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        Ok(())
    }

    #[cfg(test)]
    fn fail_next_append(&mut self) {
        self.fail_next_append = true;
    }
}

fn select_durable_partition(topic: &DurableTopic, envelope: &MessageEnvelope) -> PartitionId {
    if let Some(partition_key) = envelope.partition_key() {
        return keyed_partition(partition_key, topic.partition_count());
    }

    round_robin_partition(topic.next_round_robin_partition())
}

fn read_all_records(
    partition_log: &StoragePartitionLog,
) -> DurableBrokerResult<Vec<StoredMessageRecord>> {
    let limit = usize::try_from(partition_log.next_offset().value()).map_err(|_| {
        corruption("partition offset exceeds this platform's addressable read limit".to_owned())
    })?;
    partition_log
        .read_from(Offset::new(0), limit)
        .map_err(Into::into)
}

fn recover_state_events(path: &Path) -> DurableBrokerResult<Vec<BrokerStateEvent>> {
    let span = info_span!("durable_broker.recover_state", operation = "recover_state");
    let _guard = span.enter();

    let result = (|| {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let mut bytes = fs::read(path)?;
        let complete_len = if bytes.last().is_some_and(|byte| *byte == b'\n') {
            bytes.len()
        } else {
            bytes
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map_or(0, |position| position + 1)
        };

        if complete_len != bytes.len() {
            OpenOptions::new()
                .write(true)
                .open(path)?
                .set_len(complete_len as u64)?;
            bytes.truncate(complete_len);
        }

        let mut events = Vec::new();
        for (line_index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }

            let event = serde_json::from_slice(line).map_err(|error| {
                DurableBrokerError::StateCorruption {
                    path: path.to_path_buf(),
                    line: line_index + 1,
                    reason: error.to_string(),
                }
            })?;
            events.push(event);
        }

        Ok(events)
    })();

    match &result {
        Ok(events) => {
            metrics::record_broker_recovery("success");
            info!(
                operation = "recover_state",
                status = "success",
                recovered_events = events.len()
            );
        }
        Err(error) => {
            metrics::record_broker_recovery("error");
            warn!(
                operation = "recover_state",
                status = "error",
                kind = durable_error_kind(error)
            );
        }
    }

    result
}

fn corruption(reason: impl Into<String>) -> DurableBrokerError {
    DurableBrokerError::Corruption {
        reason: reason.into(),
    }
}

fn durable_error_kind(error: &DurableBrokerError) -> &'static str {
    match error {
        DurableBrokerError::Broker(BrokerError::Domain(_)) => "domain",
        DurableBrokerError::Broker(BrokerError::TopicAlreadyExists { .. }) => {
            "topic_already_exists"
        }
        DurableBrokerError::Broker(BrokerError::TopicNotFound { .. }) => "topic_not_found",
        DurableBrokerError::Broker(BrokerError::DeliveryNotFound { .. }) => "delivery_not_found",
        DurableBrokerError::Broker(BrokerError::InvalidConsumer { .. }) => "invalid_consumer",
        DurableBrokerError::Broker(BrokerError::InvalidConfig { .. }) => "invalid_config",
        DurableBrokerError::Storage(_) => "storage",
        DurableBrokerError::Io(_) => "io",
        DurableBrokerError::Serde(_) => "serde",
        DurableBrokerError::StateCorruption { .. } => "state_corruption",
        DurableBrokerError::Corruption { .. } => "corruption",
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use msg_core::{ContentType, EventSource, EventType, MessageId, MessagePayload, TopicConfig};

    use super::*;

    const MAX_SEGMENT_BYTES: u64 = 1024 * 1024;

    fn timestamp(value: u64) -> MessageTimestamp {
        MessageTimestamp::from_unix_millis(value)
    }

    fn topic_name() -> TopicName {
        TopicName::new("orders").unwrap()
    }

    fn group_id() -> ConsumerGroupId {
        ConsumerGroupId::new("group.1").unwrap()
    }

    fn consumer_id() -> ConsumerId {
        ConsumerId::new("consumer-1").unwrap()
    }

    fn broker_config(max_attempts: u32, backoff_millis: Option<u64>) -> BrokerConfig {
        BrokerConfig::new(
            RetryPolicy::new(max_attempts, backoff_millis).unwrap(),
            1_000,
        )
        .unwrap()
    }

    fn durable_config(
        root: &TempDir,
        max_attempts: u32,
        backoff_millis: Option<u64>,
    ) -> DurableBrokerConfig {
        DurableBrokerConfig::new(
            root.path(),
            broker_config(max_attempts, backoff_millis),
            MAX_SEGMENT_BYTES,
        )
    }

    fn open_broker(
        root: &TempDir,
        max_attempts: u32,
        backoff_millis: Option<u64>,
    ) -> DurableBroker {
        DurableBroker::open(durable_config(root, max_attempts, backoff_millis)).unwrap()
    }

    fn envelope(id: impl AsRef<str>) -> MessageEnvelope {
        MessageEnvelope::builder(
            MessageId::new(id.as_ref()).unwrap(),
            EventSource::new("/tests").unwrap(),
            EventType::new("order.created").unwrap(),
            ContentType::new("application/json").unwrap(),
            timestamp(1),
            MessagePayload::from_bytes(br#"{"ok":true}"#.to_vec()),
        )
        .build()
    }

    fn create_topic(broker: &mut DurableBroker) {
        broker
            .create_topic(CreateTopicCommand::new(
                topic_name(),
                TopicConfig::new(1).unwrap(),
            ))
            .unwrap();
    }

    fn publish(broker: &mut DurableBroker, id: impl AsRef<str>) {
        broker
            .publish(PublishCommand::new(topic_name(), envelope(id)))
            .unwrap();
    }

    fn consume(broker: &mut DurableBroker, at: u64) -> Vec<ConsumedMessage> {
        broker
            .consume(ConsumeCommand::new(
                topic_name(),
                group_id(),
                consumer_id(),
                10,
                timestamp(at),
            ))
            .unwrap()
    }

    fn assert_injected_io_error(error: DurableBrokerError) {
        assert!(matches!(error, DurableBrokerError::Io(_)));
    }

    #[test]
    fn consume_state_log_append_failure_does_not_mark_deliveries_pending() {
        let root = TempDir::new().unwrap();
        let mut broker = open_broker(&root, 3, Some(100));
        create_topic(&mut broker);
        publish(&mut broker, "message-1");

        broker.fail_next_state_log_append();
        let error = broker
            .consume(ConsumeCommand::new(
                topic_name(),
                group_id(),
                consumer_id(),
                10,
                timestamp(10),
            ))
            .unwrap_err();
        assert_injected_io_error(error);

        let consumed = consume(&mut broker, 11);
        assert_eq!(consumed.len(), 1);
        assert_eq!(consumed[0].attempt_number(), 1);
        assert_eq!(consumed[0].envelope().id().as_str(), "message-1");
    }

    #[test]
    fn ack_state_log_append_failure_leaves_message_pending_and_not_recovered_as_acked() {
        let root = TempDir::new().unwrap();
        let delivery_id = {
            let mut broker = open_broker(&root, 3, Some(100));
            create_topic(&mut broker);
            publish(&mut broker, "message-1");
            let consumed = consume(&mut broker, 10);

            broker.fail_next_state_log_append();
            let error = broker
                .ack(AckCommand::new(
                    consumed[0].delivery_id().clone(),
                    consumer_id(),
                    timestamp(11),
                ))
                .unwrap_err();
            assert_injected_io_error(error);
            assert!(consume(&mut broker, 12).is_empty());

            consumed[0].delivery_id().clone()
        };

        let mut reopened = open_broker(&root, 3, Some(100));
        let redelivered = consume(&mut reopened, 20);
        assert_eq!(redelivered.len(), 1);
        assert_eq!(redelivered[0].attempt_number(), 2);
        assert_ne!(redelivered[0].delivery_id(), &delivery_id);
    }

    #[test]
    fn nack_retry_state_log_append_failure_leaves_pending_delivery_intact() {
        let root = TempDir::new().unwrap();
        {
            let mut broker = open_broker(&root, 3, Some(100));
            create_topic(&mut broker);
            publish(&mut broker, "message-1");
            let consumed = consume(&mut broker, 10);

            broker.fail_next_state_log_append();
            let error = broker
                .nack(NackCommand::with_reason(
                    consumed[0].delivery_id().clone(),
                    consumer_id(),
                    "transient",
                    timestamp(11),
                ))
                .unwrap_err();
            assert_injected_io_error(error);
            assert!(consume(&mut broker, 12).is_empty());
        }

        let mut reopened = open_broker(&root, 3, Some(100));
        let redelivered = consume(&mut reopened, 20);
        assert_eq!(redelivered.len(), 1);
        assert_eq!(redelivered[0].attempt_number(), 2);
        assert_eq!(redelivered[0].envelope().id().as_str(), "message-1");
    }

    #[test]
    fn retry_maintenance_state_log_append_failure_leaves_pending_delivery_intact() {
        let root = TempDir::new().unwrap();
        {
            let mut broker = open_broker(&root, 3, Some(100));
            create_topic(&mut broker);
            publish(&mut broker, "message-1");
            let consumed = consume(&mut broker, 10);

            broker.fail_next_state_log_append();
            let error = broker.retry_ready(timestamp(1_010)).unwrap_err();
            assert_injected_io_error(error);
            assert!(consume(&mut broker, 1_011).is_empty());
            assert_eq!(consumed[0].attempt_number(), 1);
        }

        let mut reopened = open_broker(&root, 3, Some(100));
        let redelivered = consume(&mut reopened, 20);
        assert_eq!(redelivered.len(), 1);
        assert_eq!(redelivered[0].attempt_number(), 2);
    }

    #[test]
    fn dlq_transition_state_log_append_failure_does_not_drop_message() {
        let root = TempDir::new().unwrap();
        {
            let mut broker = open_broker(&root, 1, None);
            create_topic(&mut broker);
            publish(&mut broker, "message-1");
            let consumed = consume(&mut broker, 10);

            broker.fail_next_state_log_append();
            let error = broker
                .nack(NackCommand::with_reason(
                    consumed[0].delivery_id().clone(),
                    consumer_id(),
                    "poison",
                    timestamp(11),
                ))
                .unwrap_err();
            assert_injected_io_error(error);
            assert!(broker.list_dlq(DlqQuery::all()).unwrap().is_empty());
            assert!(consume(&mut broker, 12).is_empty());
        }

        let mut reopened = open_broker(&root, 1, None);
        let redelivered = consume(&mut reopened, 20);
        assert_eq!(redelivered.len(), 1);
        assert_eq!(redelivered[0].attempt_number(), 2);
        assert_eq!(redelivered[0].envelope().id().as_str(), "message-1");
        assert!(reopened.list_dlq(DlqQuery::all()).unwrap().is_empty());
    }
}
