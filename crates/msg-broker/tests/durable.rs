use std::fs;
use std::path::{Path, PathBuf};

use msg_broker::{
    AckCommand, BrokerConfig, BrokerError, ConsumeCommand, CreateTopicCommand, DlqQuery,
    DurableBroker, DurableBrokerConfig, DurableBrokerError, NackCommand, PublishCommand,
};
use msg_core::{
    ConsumerGroupId, ConsumerId, ContentType, DeadLetterReason, EventSource, EventType, MessageId,
    MessageTimestamp, Offset, RetryPolicy, TopicConfig, TopicName,
};
use tempfile::TempDir;

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

fn broker_config(
    max_attempts: u32,
    backoff_millis: Option<u64>,
    lease_millis: u64,
) -> BrokerConfig {
    BrokerConfig::new(
        RetryPolicy::new(max_attempts, backoff_millis).unwrap(),
        lease_millis,
    )
    .unwrap()
}

fn durable_config(
    root: &TempDir,
    max_attempts: u32,
    backoff_millis: Option<u64>,
    lease_millis: u64,
    max_segment_bytes: u64,
) -> DurableBrokerConfig {
    DurableBrokerConfig::new(
        root.path(),
        broker_config(max_attempts, backoff_millis, lease_millis),
        max_segment_bytes,
    )
}

fn open_broker(
    root: &TempDir,
    max_attempts: u32,
    backoff_millis: Option<u64>,
    lease_millis: u64,
    max_segment_bytes: u64,
) -> DurableBroker {
    DurableBroker::open(durable_config(
        root,
        max_attempts,
        backoff_millis,
        lease_millis,
        max_segment_bytes,
    ))
    .unwrap()
}

fn envelope(id: impl AsRef<str>) -> msg_core::MessageEnvelope {
    msg_core::MessageEnvelope::builder(
        MessageId::new(id.as_ref()).unwrap(),
        EventSource::new("/tests").unwrap(),
        EventType::new("order.created").unwrap(),
        ContentType::new("application/json").unwrap(),
        timestamp(1),
        msg_core::MessagePayload::from_bytes(br#"{"ok":true}"#.to_vec()),
    )
    .build()
}

fn create_topic(broker: &mut DurableBroker, partitions: u32) {
    broker
        .create_topic(CreateTopicCommand::new(
            topic_name(),
            TopicConfig::new(partitions).unwrap(),
        ))
        .unwrap();
}

fn publish(broker: &mut DurableBroker, id: impl AsRef<str>) -> msg_broker::PublishedMessage {
    broker
        .publish(PublishCommand::new(topic_name(), envelope(id)))
        .unwrap()
}

fn consume(
    broker: &mut DurableBroker,
    max_messages: usize,
    at: u64,
) -> Vec<msg_broker::ConsumedMessage> {
    broker
        .consume(ConsumeCommand::new(
            topic_name(),
            group_id(),
            consumer_id(),
            max_messages,
            timestamp(at),
        ))
        .unwrap()
}

fn segment_path(root: &Path, partition: u32, base_offset: u64) -> PathBuf {
    root.join("messages")
        .join("topics")
        .join(topic_name().as_str())
        .join("partitions")
        .join(partition.to_string())
        .join(format!("{base_offset:020}.log"))
}

fn consumed_ids(messages: &[msg_broker::ConsumedMessage]) -> Vec<String> {
    messages
        .iter()
        .map(|message| message.envelope().id().as_str().to_owned())
        .collect()
}

fn assert_delivery_not_found(error: DurableBrokerError) {
    assert!(matches!(
        error,
        DurableBrokerError::Broker(BrokerError::DeliveryNotFound { .. })
    ));
}

#[test]
fn publish_reopen_recovers_message() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "message-1");
    }

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    let consumed = consume(&mut broker, 10, 10);

    assert_eq!(consumed.len(), 1);
    assert_eq!(consumed[0].offset(), Offset::new(0));
    assert_eq!(consumed[0].envelope().id().as_str(), "message-1");
}

#[test]
fn ack_reopen_prevents_redelivery_and_duplicate_ack_fails() {
    let root = TempDir::new().unwrap();
    let delivery_id = {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "message-1");
        let consumed = consume(&mut broker, 10, 10);
        let delivery_id = consumed[0].delivery_id().clone();
        broker
            .ack(AckCommand::new(
                delivery_id.clone(),
                consumer_id(),
                timestamp(11),
            ))
            .unwrap();
        delivery_id
    };

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    assert!(consume(&mut broker, 10, 20).is_empty());

    let duplicate = broker
        .ack(AckCommand::new(delivery_id, consumer_id(), timestamp(21)))
        .unwrap_err();
    assert_delivery_not_found(duplicate);
}

#[test]
fn in_flight_reopen_redelivers_with_next_attempt() {
    let root = TempDir::new().unwrap();
    let first_delivery_id = {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "message-1");
        let consumed = consume(&mut broker, 10, 10);
        assert_eq!(consumed[0].attempt_number(), 1);
        consumed[0].delivery_id().clone()
    };

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    let redelivered = consume(&mut broker, 10, 20);

    assert_eq!(redelivered.len(), 1);
    assert_eq!(redelivered[0].attempt_number(), 2);
    assert_ne!(redelivered[0].delivery_id(), &first_delivery_id);
    assert_eq!(redelivered[0].offset(), Offset::new(0));
}

#[test]
fn nack_reopen_preserves_backoff_and_retries_when_ready() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "message-1");
        let consumed = consume(&mut broker, 10, 10);
        broker
            .nack(NackCommand::with_reason(
                consumed[0].delivery_id().clone(),
                consumer_id(),
                "transient",
                timestamp(20),
            ))
            .unwrap();
    }

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    let early = broker.retry_ready(timestamp(119)).unwrap();
    assert_eq!(early.made_available(), 0);
    assert!(consume(&mut broker, 10, 119).is_empty());

    let ready = broker.retry_ready(timestamp(120)).unwrap();
    assert_eq!(ready.made_available(), 1);
    let retry = consume(&mut broker, 10, 121);
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].attempt_number(), 2);
}

#[test]
fn retry_attempts_survive_reopen_and_continue_incrementing() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 5, None, 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "message-1");
        let first = consume(&mut broker, 10, 10);
        broker
            .nack(NackCommand::new(
                first[0].delivery_id().clone(),
                consumer_id(),
                timestamp(11),
            ))
            .unwrap();
        broker.retry_ready(timestamp(11)).unwrap();
        let second = consume(&mut broker, 10, 12);
        assert_eq!(second[0].attempt_number(), 2);
    }

    let mut broker = open_broker(&root, 5, None, 1_000, 1024 * 1024);
    let third = consume(&mut broker, 10, 20);

    assert_eq!(third.len(), 1);
    assert_eq!(third[0].attempt_number(), 3);
}

#[test]
fn dlq_reopen_preserves_entry_and_prevents_redelivery() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 2, None, 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "message-1");

        let first = consume(&mut broker, 10, 10);
        broker
            .nack(NackCommand::new(
                first[0].delivery_id().clone(),
                consumer_id(),
                timestamp(11),
            ))
            .unwrap();
        broker.retry_ready(timestamp(11)).unwrap();

        let second = consume(&mut broker, 10, 12);
        broker
            .nack(NackCommand::with_reason(
                second[0].delivery_id().clone(),
                consumer_id(),
                "poison",
                timestamp(13),
            ))
            .unwrap();
    }

    let mut broker = open_broker(&root, 2, None, 1_000, 1024 * 1024);
    let dlq = broker.list_dlq(DlqQuery::all()).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].message_id().as_str(), "message-1");
    assert_eq!(dlq[0].attempt_count(), 2);
    assert_eq!(
        dlq[0].reason(),
        &DeadLetterReason::Manual("poison".to_owned())
    );
    assert!(consume(&mut broker, 10, 20).is_empty());
}

#[test]
fn failed_message_append_returns_error_without_phantom_message() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1);
        create_topic(&mut broker, 1);
        publish(&mut broker, "message-0");

        let blocker = segment_path(root.path(), 0, 1);
        fs::create_dir(&blocker).unwrap();
        let error = broker
            .publish(PublishCommand::new(topic_name(), envelope("message-1")))
            .unwrap_err();
        assert!(matches!(
            error,
            DurableBrokerError::Storage(msg_storage::StorageError::Io(_))
        ));
    }

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1);
    let consumed = consume(&mut broker, 10, 10);

    assert_eq!(consumed_ids(&consumed), vec!["message-0"]);
}

#[test]
fn segment_recovery_consumes_all_messages_in_deterministic_order() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1);
        create_topic(&mut broker, 3);
        for index in 0..8 {
            publish(&mut broker, format!("message-{index}"));
        }
    }

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1);
    let after_reopen = publish(&mut broker, "message-8");
    assert_eq!(after_reopen.partition_id().value(), 2);

    let consumed = consume(&mut broker, 20, 10);
    assert_eq!(
        consumed_ids(&consumed),
        vec![
            "message-0",
            "message-3",
            "message-6",
            "message-1",
            "message-4",
            "message-7",
            "message-2",
            "message-5",
            "message-8",
        ]
    );
}
