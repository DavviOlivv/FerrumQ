use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use msg_broker::{
    AckCommand, BrokerConfig, BrokerError, ConsumeCommand, CreateTopicCommand, DlqQuery,
    DurableBroker, DurableBrokerConfig, DurableBrokerError, NackCommand, PublishCommand,
};
use msg_core::{
    ConsumerGroupId, ConsumerId, ContentType, DeadLetterReason, DeliveryId, EventSource, EventType,
    MessageId, MessagePayload, MessageTimestamp, Offset, PartitionId, PartitionKey, RetryPolicy,
    TopicConfig, TopicName,
};
use tempfile::TempDir;

fn timestamp(value: u64) -> MessageTimestamp {
    MessageTimestamp::from_unix_millis(value)
}

fn topic_name() -> TopicName {
    TopicName::new("orders").unwrap()
}

fn other_topic_name() -> TopicName {
    TopicName::new("payments").unwrap()
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

fn envelope_builder(id: impl AsRef<str>) -> msg_core::MessageEnvelopeBuilder {
    msg_core::MessageEnvelope::builder(
        MessageId::new(id.as_ref()).unwrap(),
        EventSource::new("/tests").unwrap(),
        EventType::new("order.created").unwrap(),
        ContentType::new("application/json").unwrap(),
        timestamp(1),
        MessagePayload::from_bytes(br#"{"ok":true}"#.to_vec()),
    )
}

fn envelope(id: impl AsRef<str>) -> msg_core::MessageEnvelope {
    envelope_builder(id).build()
}

fn keyed_envelope(id: impl AsRef<str>, key: impl AsRef<str>) -> msg_core::MessageEnvelope {
    envelope_builder(id)
        .partition_key(PartitionKey::new(key.as_ref()).unwrap())
        .build()
}

fn create_topic_named(broker: &mut DurableBroker, name: TopicName, partitions: u32) {
    broker
        .create_topic(CreateTopicCommand::new(
            name,
            TopicConfig::new(partitions).unwrap(),
        ))
        .unwrap();
}

fn create_topic(broker: &mut DurableBroker, partitions: u32) {
    create_topic_named(broker, topic_name(), partitions);
}

fn publish_to(
    broker: &mut DurableBroker,
    topic: TopicName,
    envelope: msg_core::MessageEnvelope,
) -> msg_broker::PublishedMessage {
    broker
        .publish(PublishCommand::new(topic, envelope))
        .unwrap()
}

fn publish(broker: &mut DurableBroker, id: impl AsRef<str>) -> msg_broker::PublishedMessage {
    publish_to(broker, topic_name(), envelope(id))
}

fn consume_from(
    broker: &mut DurableBroker,
    topic: TopicName,
    group: ConsumerGroupId,
    consumer: ConsumerId,
    max_messages: usize,
    at: u64,
) -> Vec<msg_broker::ConsumedMessage> {
    broker
        .consume(ConsumeCommand::new(
            topic,
            group,
            consumer,
            max_messages,
            timestamp(at),
        ))
        .unwrap()
}

fn consume(
    broker: &mut DurableBroker,
    max_messages: usize,
    at: u64,
) -> Vec<msg_broker::ConsumedMessage> {
    consume_from(
        broker,
        topic_name(),
        group_id(),
        consumer_id(),
        max_messages,
        at,
    )
}

fn segment_path(root: &Path, partition: u32, base_offset: u64) -> PathBuf {
    root.join("messages")
        .join("topics")
        .join(topic_name().as_str())
        .join("partitions")
        .join(partition.to_string())
        .join(format!("{base_offset:020}.log"))
}

fn state_log_path(root: &Path) -> PathBuf {
    root.join("broker-state").join("events.jsonl")
}

fn frame_starts(path: &Path) -> Vec<u64> {
    let mut file = fs::File::open(path).unwrap();
    let file_len = file.metadata().unwrap().len();
    let mut position = 0;
    let mut starts = Vec::new();

    while position < file_len {
        starts.push(position);
        let mut length_bytes = [0_u8; 4];
        file.read_exact(&mut length_bytes).unwrap();
        let record_length = u64::from(u32::from_le_bytes(length_bytes));
        position += 8 + record_length;
        file.seek(SeekFrom::Start(position)).unwrap();
    }

    starts
}

fn flip_checksum_byte(path: &Path, frame_start: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(frame_start + 4)).unwrap();

    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(frame_start + 4)).unwrap();
    file.write_all(&byte).unwrap();
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
fn empty_broker_directory_opens_cleanly() {
    let root = TempDir::new().unwrap();

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);

    create_topic(&mut broker, 1);
    assert!(consume(&mut broker, 10, 10).is_empty());
}

#[test]
fn topic_metadata_survives_reopen_and_duplicate_create_fails() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 2);
    }

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    let duplicate = broker
        .create_topic(CreateTopicCommand::new(
            topic_name(),
            TopicConfig::new(2).unwrap(),
        ))
        .unwrap_err();
    assert!(matches!(
        duplicate,
        DurableBrokerError::Broker(BrokerError::TopicAlreadyExists { topic })
            if topic == topic_name()
    ));

    let published = publish(&mut broker, "message-1");
    assert_eq!(published.topic(), &topic_name());
}

#[test]
fn existing_broker_storage_reopens_repeatedly_with_deterministic_consumption() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 3);
        for index in 0..6 {
            publish(&mut broker, format!("message-{index}"));
        }
    }

    for _ in 0..3 {
        let broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        drop(broker);
    }

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    let consumed = consume(&mut broker, 10, 10);
    assert_eq!(
        consumed_ids(&consumed),
        vec![
            "message-0",
            "message-3",
            "message-1",
            "message-4",
            "message-2",
            "message-5",
        ]
    );
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
fn ack_unknown_delivery_returns_delivery_not_found() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    create_topic(&mut broker, 1);

    let error = broker
        .ack(AckCommand::new(
            DeliveryId::new("unknown-delivery").unwrap(),
            consumer_id(),
            timestamp(10),
        ))
        .unwrap_err();

    assert_delivery_not_found(error);
}

#[test]
fn ack_after_nack_returns_delivery_not_found() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");
    let consumed = consume(&mut broker, 10, 10);
    let delivery_id = consumed[0].delivery_id().clone();
    broker
        .nack(NackCommand::new(
            delivery_id.clone(),
            consumer_id(),
            timestamp(11),
        ))
        .unwrap();

    let error = broker
        .ack(AckCommand::new(delivery_id, consumer_id(), timestamp(12)))
        .unwrap_err();

    assert_delivery_not_found(error);
}

#[test]
fn nack_unknown_delivery_returns_delivery_not_found() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    create_topic(&mut broker, 1);

    let error = broker
        .nack(NackCommand::new(
            DeliveryId::new("unknown-delivery").unwrap(),
            consumer_id(),
            timestamp(10),
        ))
        .unwrap_err();

    assert_delivery_not_found(error);
}

#[test]
fn duplicate_nack_and_nack_after_ack_return_delivery_not_found() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, None, 1_000, 1024 * 1024);
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");
    publish(&mut broker, "message-2");

    let first = consume(&mut broker, 1, 10);
    let first_delivery_id = first[0].delivery_id().clone();
    broker
        .nack(NackCommand::new(
            first_delivery_id.clone(),
            consumer_id(),
            timestamp(11),
        ))
        .unwrap();
    let duplicate_nack = broker
        .nack(NackCommand::new(
            first_delivery_id,
            consumer_id(),
            timestamp(12),
        ))
        .unwrap_err();
    assert_delivery_not_found(duplicate_nack);

    let second = consume(&mut broker, 1, 13);
    let second_delivery_id = second[0].delivery_id().clone();
    broker
        .ack(AckCommand::new(
            second_delivery_id.clone(),
            consumer_id(),
            timestamp(14),
        ))
        .unwrap();
    let nack_after_ack = broker
        .nack(NackCommand::new(
            second_delivery_id,
            consumer_id(),
            timestamp(15),
        ))
        .unwrap_err();
    assert_delivery_not_found(nack_after_ack);
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
fn consume_empty_topic_and_while_leased_returns_empty() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    create_topic(&mut broker, 1);
    assert!(consume(&mut broker, 10, 10).is_empty());

    publish(&mut broker, "message-1");
    let first = consume(&mut broker, 10, 11);
    assert_eq!(first.len(), 1);
    assert!(consume(&mut broker, 10, 12).is_empty());
}

#[test]
fn lease_expiry_redelivery_increments_attempts_without_inconsistent_state() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, None, 100, 1024 * 1024);
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");

    let first = consume(&mut broker, 10, 10);
    assert_eq!(first[0].attempt_number(), 1);
    let summary = broker.retry_ready(timestamp(110)).unwrap();
    assert_eq!(summary.lease_expired(), 1);
    assert_eq!(summary.made_available(), 1);

    let redelivered = consume(&mut broker, 10, 111);
    assert_eq!(redelivered.len(), 1);
    assert_eq!(redelivered[0].attempt_number(), 2);
    assert_ne!(redelivered[0].delivery_id(), first[0].delivery_id());
    assert!(consume(&mut broker, 10, 112).is_empty());
}

#[test]
fn acking_redelivery_completes_message_and_stale_first_delivery_cannot_ack() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, None, 100, 1024 * 1024);
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");

    let first = consume(&mut broker, 10, 10);
    broker.retry_ready(timestamp(110)).unwrap();
    let redelivered = consume(&mut broker, 10, 111);
    broker
        .ack(AckCommand::new(
            redelivered[0].delivery_id().clone(),
            consumer_id(),
            timestamp(112),
        ))
        .unwrap();

    let stale_ack = broker
        .ack(AckCommand::new(
            first[0].delivery_id().clone(),
            consumer_id(),
            timestamp(113),
        ))
        .unwrap_err();
    assert_delivery_not_found(stale_ack);
    assert!(consume(&mut broker, 10, 114).is_empty());
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
fn below_max_attempts_retries_without_dead_lettering() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, None, 1_000, 1024 * 1024);
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
    assert!(broker.list_dlq(DlqQuery::all()).unwrap().is_empty());

    let summary = broker.retry_ready(timestamp(11)).unwrap();
    assert_eq!(summary.made_available(), 1);
    let retried = consume(&mut broker, 10, 12);
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].attempt_number(), 2);
    assert!(broker.list_dlq(DlqQuery::all()).unwrap().is_empty());
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
fn max_attempts_dead_letters_without_infinite_retry() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 1, None, 1_000, 1024 * 1024);
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");

    let first = consume(&mut broker, 10, 10);
    broker
        .nack(NackCommand::with_reason(
            first[0].delivery_id().clone(),
            consumer_id(),
            "poison",
            timestamp(11),
        ))
        .unwrap();

    let summary = broker.retry_ready(timestamp(12)).unwrap();
    assert_eq!(summary.made_available(), 0);
    assert_eq!(summary.dead_lettered(), 0);
    assert_eq!(broker.list_dlq(DlqQuery::all()).unwrap().len(), 1);
    assert!(consume(&mut broker, 10, 13).is_empty());
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
    assert_eq!(dlq[0].topic(), &topic_name());
    assert_eq!(dlq[0].partition_id(), PartitionId::new(0));
    assert_eq!(dlq[0].offset(), Offset::new(0));
    assert_eq!(dlq[0].message_id().as_str(), "message-1");
    assert_eq!(dlq[0].envelope().id().as_str(), "message-1");
    assert_eq!(dlq[0].consumer_group_id(), &group_id());
    assert_eq!(dlq[0].attempt_count(), 2);
    assert_eq!(dlq[0].timestamp(), timestamp(13));
    assert_eq!(
        dlq[0].reason(),
        &DeadLetterReason::Manual("poison".to_owned())
    );
    assert!(consume(&mut broker, 10, 20).is_empty());

    drop(broker);
    let mut broker = open_broker(&root, 2, None, 1_000, 1024 * 1024);
    assert_eq!(broker.list_dlq(DlqQuery::all()).unwrap(), dlq);
    assert!(consume(&mut broker, 10, 21).is_empty());
}

#[test]
fn delivery_states_remain_stable_across_consecutive_reopens() {
    let acked_root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&acked_root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "acked");
        let consumed = consume(&mut broker, 10, 10);
        broker
            .ack(AckCommand::new(
                consumed[0].delivery_id().clone(),
                consumer_id(),
                timestamp(11),
            ))
            .unwrap();
    }
    for at in [20, 21] {
        let mut broker = open_broker(&acked_root, 3, Some(100), 1_000, 1024 * 1024);
        assert!(consume(&mut broker, 10, at).is_empty());
    }

    let retry_root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&retry_root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "retry");
        let consumed = consume(&mut broker, 10, 10);
        broker
            .nack(NackCommand::new(
                consumed[0].delivery_id().clone(),
                consumer_id(),
                timestamp(11),
            ))
            .unwrap();
    }
    for at in [50, 51] {
        let mut broker = open_broker(&retry_root, 3, Some(100), 1_000, 1024 * 1024);
        assert!(consume(&mut broker, 10, at).is_empty());
    }
    let mut broker = open_broker(&retry_root, 3, Some(100), 1_000, 1024 * 1024);
    broker.retry_ready(timestamp(111)).unwrap();
    let retried = consume(&mut broker, 10, 112);
    assert_eq!(retried[0].attempt_number(), 2);

    let in_flight_root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&in_flight_root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "in-flight");
        let consumed = consume(&mut broker, 10, 10);
        assert_eq!(consumed[0].attempt_number(), 1);
    }
    for _ in 0..2 {
        let broker = open_broker(&in_flight_root, 3, Some(100), 1_000, 1024 * 1024);
        drop(broker);
    }
    let mut broker = open_broker(&in_flight_root, 3, Some(100), 1_000, 1024 * 1024);
    let redelivered = consume(&mut broker, 10, 20);
    assert_eq!(redelivered[0].attempt_number(), 2);

    let dlq_root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&dlq_root, 1, None, 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
        publish(&mut broker, "dlq");
        let consumed = consume(&mut broker, 10, 10);
        broker
            .nack(NackCommand::new(
                consumed[0].delivery_id().clone(),
                consumer_id(),
                timestamp(11),
            ))
            .unwrap();
    }
    for at in [20, 21] {
        let mut broker = open_broker(&dlq_root, 1, None, 1_000, 1024 * 1024);
        assert_eq!(broker.list_dlq(DlqQuery::all()).unwrap().len(), 1);
        assert!(consume(&mut broker, 10, at).is_empty());
    }
}

#[test]
fn offsets_are_monotonic_within_each_partition() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    create_topic(&mut broker, 2);

    let published: Vec<_> = (0..6)
        .map(|index| publish(&mut broker, format!("message-{index}")))
        .collect();

    let partition_zero_offsets: Vec<_> = published
        .iter()
        .filter(|message| message.partition_id() == PartitionId::new(0))
        .map(|message| message.offset())
        .collect();
    let partition_one_offsets: Vec<_> = published
        .iter()
        .filter(|message| message.partition_id() == PartitionId::new(1))
        .map(|message| message.offset())
        .collect();

    assert_eq!(
        partition_zero_offsets,
        vec![Offset::new(0), Offset::new(1), Offset::new(2)]
    );
    assert_eq!(
        partition_one_offsets,
        vec![Offset::new(0), Offset::new(1), Offset::new(2)]
    );
}

#[test]
fn partition_selection_remains_deterministic_across_reopen_for_keyed_and_unkeyed_messages() {
    let root = TempDir::new().unwrap();
    let keyed_partition = {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 3);
        let keyed = publish_to(
            &mut broker,
            topic_name(),
            keyed_envelope("keyed-1", "customer-42"),
        );
        assert_eq!(
            publish(&mut broker, "unkeyed-1").partition_id(),
            PartitionId::new(0)
        );
        assert_eq!(
            publish(&mut broker, "unkeyed-2").partition_id(),
            PartitionId::new(1)
        );
        keyed.partition_id()
    };

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    let keyed_after_reopen = publish_to(
        &mut broker,
        topic_name(),
        keyed_envelope("keyed-2", "customer-42"),
    );
    let unkeyed_after_reopen = publish(&mut broker, "unkeyed-3");

    assert_eq!(keyed_after_reopen.partition_id(), keyed_partition);
    assert_eq!(unkeyed_after_reopen.partition_id(), PartitionId::new(2));
}

#[test]
fn multiple_topics_and_partitions_recover_independently() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic_named(&mut broker, topic_name(), 2);
        create_topic_named(&mut broker, other_topic_name(), 3);

        assert_eq!(
            publish(&mut broker, "orders-0").partition_id(),
            PartitionId::new(0)
        );
        assert_eq!(
            publish(&mut broker, "orders-1").partition_id(),
            PartitionId::new(1)
        );
        assert_eq!(
            publish_to(&mut broker, other_topic_name(), envelope("payments-0")).partition_id(),
            PartitionId::new(0)
        );
        assert_eq!(
            publish_to(&mut broker, other_topic_name(), envelope("payments-1")).partition_id(),
            PartitionId::new(1)
        );
    }

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    assert_eq!(
        publish(&mut broker, "orders-2").partition_id(),
        PartitionId::new(0)
    );
    assert_eq!(
        publish_to(&mut broker, other_topic_name(), envelope("payments-2")).partition_id(),
        PartitionId::new(2)
    );

    let orders = consume(&mut broker, 10, 10);
    let payments = consume_from(
        &mut broker,
        other_topic_name(),
        group_id(),
        consumer_id(),
        10,
        10,
    );

    assert_eq!(
        consumed_ids(&orders),
        vec!["orders-0", "orders-2", "orders-1"]
    );
    assert_eq!(
        consumed_ids(&payments),
        vec!["payments-0", "payments-1", "payments-2"]
    );
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

#[test]
fn final_incomplete_broker_state_line_is_truncated_and_ignored() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
    }
    OpenOptions::new()
        .append(true)
        .open(state_log_path(root.path()))
        .unwrap()
        .write_all(br#"{"type":"message_ack"#)
        .unwrap();

    let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
    let duplicate = broker
        .create_topic(CreateTopicCommand::new(
            topic_name(),
            TopicConfig::new(1).unwrap(),
        ))
        .unwrap_err();
    assert!(matches!(
        duplicate,
        DurableBrokerError::Broker(BrokerError::TopicAlreadyExists { .. })
    ));

    let state_log = fs::read_to_string(state_log_path(root.path())).unwrap();
    assert!(state_log.ends_with('\n'));
    assert!(!state_log.contains("message_ack"));
}

#[test]
fn malformed_complete_broker_state_line_returns_state_corruption() {
    let root = TempDir::new().unwrap();
    fs::create_dir_all(root.path().join("broker-state")).unwrap();
    fs::write(state_log_path(root.path()), b"not-json\n").unwrap();

    let error =
        DurableBroker::open(durable_config(&root, 3, Some(100), 1_000, 1024 * 1024)).unwrap_err();

    assert!(matches!(error, DurableBrokerError::StateCorruption { .. }));
}

#[test]
fn inconsistent_complete_state_event_returns_corruption() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1024 * 1024);
        create_topic(&mut broker, 1);
    }

    let line = fs::read_to_string(state_log_path(root.path())).unwrap();
    OpenOptions::new()
        .append(true)
        .open(state_log_path(root.path()))
        .unwrap()
        .write_all(line.as_bytes())
        .unwrap();

    let error =
        DurableBroker::open(durable_config(&root, 3, Some(100), 1_000, 1024 * 1024)).unwrap_err();

    assert!(matches!(error, DurableBrokerError::Corruption { .. }));
}

#[test]
fn message_log_corruption_surfaces_as_storage_error() {
    let root = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&root, 3, Some(100), 1_000, 1);
        create_topic(&mut broker, 1);
        publish(&mut broker, "message-0");
        publish(&mut broker, "message-1");
    }

    let first_segment = segment_path(root.path(), 0, 0);
    let starts = frame_starts(&first_segment);
    flip_checksum_byte(&first_segment, starts[0]);

    let error = DurableBroker::open(durable_config(&root, 3, Some(100), 1_000, 1)).unwrap_err();
    assert!(matches!(error, DurableBrokerError::Storage(_)));
}
