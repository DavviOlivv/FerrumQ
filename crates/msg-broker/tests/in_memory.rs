use std::collections::BTreeSet;

use msg_broker::{
    AckCommand, BrokerConfig, BrokerError, BrokerService, ConsumeCommand, CreateTopicCommand,
    DlqQuery, NackCommand, PublishCommand,
};
use msg_core::{
    ConsumerGroupId, ConsumerId, ContentType, DeadLetterReason, EventSource, EventType, MessageId,
    MessageTimestamp, Offset, PartitionKey, RetryPolicy, TopicConfig, TopicName,
};

fn timestamp(value: u64) -> MessageTimestamp {
    MessageTimestamp::from_unix_millis(value)
}

fn topic_name() -> TopicName {
    TopicName::new("orders").unwrap()
}

fn group_id() -> ConsumerGroupId {
    ConsumerGroupId::new("group.1").unwrap()
}

fn other_group_id() -> ConsumerGroupId {
    ConsumerGroupId::new("group.2").unwrap()
}

fn consumer_id() -> ConsumerId {
    ConsumerId::new("consumer-1").unwrap()
}

fn other_consumer_id() -> ConsumerId {
    ConsumerId::new("consumer-2").unwrap()
}

fn test_config(max_attempts: u32, backoff_millis: Option<u64>, lease_millis: u64) -> BrokerConfig {
    BrokerConfig::new(
        RetryPolicy::new(max_attempts, backoff_millis).unwrap(),
        lease_millis,
    )
    .unwrap()
}

fn broker() -> BrokerService {
    BrokerService::new(test_config(3, Some(100), 1_000))
}

fn create_topic(broker: &mut BrokerService, partitions: u32) {
    broker
        .create_topic(CreateTopicCommand::new(
            topic_name(),
            TopicConfig::new(partitions).unwrap(),
        ))
        .unwrap();
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

fn keyed_envelope(id: impl AsRef<str>, key: impl AsRef<str>) -> msg_core::MessageEnvelope {
    msg_core::MessageEnvelope::builder(
        MessageId::new(id.as_ref()).unwrap(),
        EventSource::new("/tests").unwrap(),
        EventType::new("order.created").unwrap(),
        ContentType::new("application/json").unwrap(),
        timestamp(1),
        msg_core::MessagePayload::from_bytes(br#"{"ok":true}"#.to_vec()),
    )
    .partition_key(PartitionKey::new(key.as_ref()).unwrap())
    .build()
}

fn publish(broker: &mut BrokerService, id: impl AsRef<str>) -> msg_broker::PublishedMessage {
    broker
        .publish(PublishCommand::new(topic_name(), envelope(id)))
        .unwrap()
}

fn consume(
    broker: &mut BrokerService,
    group_id: ConsumerGroupId,
    consumer_id: ConsumerId,
    max_messages: usize,
    at: u64,
) -> Vec<msg_broker::ConsumedMessage> {
    broker
        .consume(ConsumeCommand::new(
            topic_name(),
            group_id,
            consumer_id,
            max_messages,
            timestamp(at),
        ))
        .unwrap()
}

#[test]
fn topic_creation_succeeds_and_duplicate_fails() {
    let mut broker = broker();
    let topic = broker
        .create_topic(CreateTopicCommand::new(
            topic_name(),
            TopicConfig::new(3).unwrap(),
        ))
        .unwrap();

    assert_eq!(topic.name(), &topic_name());
    assert_eq!(topic.partition_count(), 3);

    let error = broker
        .create_topic(CreateTopicCommand::new(
            topic_name(),
            TopicConfig::new(3).unwrap(),
        ))
        .unwrap_err();
    assert!(matches!(error, BrokerError::TopicAlreadyExists { topic } if topic == topic_name()));
}

#[test]
fn invalid_topic_inputs_fail_through_domain_validation() {
    assert!(TopicName::new("bad topic").is_err());
    assert!(TopicConfig::new(0).is_err());
}

#[test]
fn long_valid_topic_and_group_still_generate_valid_delivery_ids() {
    let mut broker = broker();
    let long_topic = TopicName::new("a".repeat(255)).unwrap();
    let long_group = ConsumerGroupId::new("b".repeat(255)).unwrap();

    broker
        .create_topic(CreateTopicCommand::new(
            long_topic.clone(),
            TopicConfig::new(1).unwrap(),
        ))
        .unwrap();
    broker
        .publish(PublishCommand::new(
            long_topic.clone(),
            envelope("message-1"),
        ))
        .unwrap();

    let consumed = broker
        .consume(ConsumeCommand::new(
            long_topic,
            long_group,
            consumer_id(),
            1,
            timestamp(1),
        ))
        .unwrap();

    assert_eq!(consumed.len(), 1);
    assert!(consumed[0].delivery_id().as_str().len() <= 255);
}

#[test]
fn publish_requires_existing_topic_and_assigns_monotonic_offsets() {
    let mut broker = broker();
    let missing = broker
        .publish(PublishCommand::new(topic_name(), envelope("missing")))
        .unwrap_err();
    assert!(matches!(missing, BrokerError::TopicNotFound { topic } if topic == topic_name()));

    create_topic(&mut broker, 1);

    for index in 0..10 {
        let published = publish(&mut broker, format!("message-{index}"));
        assert_eq!(published.partition_id().value(), 0);
        assert_eq!(published.offset(), Offset::new(index));
    }
}

#[test]
fn partition_assignment_is_deterministic_for_keys_and_round_robin_without_keys() {
    let mut keyed = broker();
    create_topic(&mut keyed, 4);

    let first = keyed
        .publish(PublishCommand::new(
            topic_name(),
            keyed_envelope("keyed-1", "account-1"),
        ))
        .unwrap();
    let second = keyed
        .publish(PublishCommand::new(
            topic_name(),
            keyed_envelope("keyed-2", "account-1"),
        ))
        .unwrap();
    assert_eq!(first.partition_id(), second.partition_id());

    let mut round_robin = broker();
    create_topic(&mut round_robin, 3);
    let assigned: Vec<_> = (0..5)
        .map(|index| {
            publish(&mut round_robin, format!("rr-{index}"))
                .partition_id()
                .value()
        })
        .collect();
    assert_eq!(assigned, vec![0, 1, 2, 0, 1]);
}

#[test]
fn consumer_groups_receive_independent_pending_deliveries() {
    let mut broker = broker();
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");

    let empty = consume(&mut broker, group_id(), consumer_id(), 10, 1);
    assert_eq!(empty.len(), 1);

    let pending_not_redelivered = consume(&mut broker, group_id(), consumer_id(), 10, 2);
    assert!(pending_not_redelivered.is_empty());

    let other_group = consume(&mut broker, other_group_id(), other_consumer_id(), 10, 3);
    assert_eq!(other_group.len(), 1);
    assert_eq!(other_group[0].offset(), Offset::new(0));
}

#[test]
fn empty_topics_return_empty_consumes() {
    let mut broker = broker();
    create_topic(&mut broker, 1);

    assert!(consume(&mut broker, group_id(), consumer_id(), 10, 1).is_empty());
}

#[test]
fn ack_valid_pending_succeeds_and_acked_messages_do_not_redeliver() {
    let mut broker = broker();
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");

    let consumed = consume(&mut broker, group_id(), consumer_id(), 10, 1);
    broker
        .ack(AckCommand::new(
            consumed[0].delivery_id().clone(),
            consumer_id(),
            timestamp(2),
        ))
        .unwrap();

    assert!(consume(&mut broker, group_id(), consumer_id(), 10, 3).is_empty());

    let stale_ack = broker
        .ack(AckCommand::new(
            consumed[0].delivery_id().clone(),
            consumer_id(),
            timestamp(4),
        ))
        .unwrap_err();
    assert!(matches!(stale_ack, BrokerError::DeliveryNotFound { .. }));
}

#[test]
fn ack_unknown_and_wrong_consumer_fail() {
    let mut broker = broker();
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");

    let unknown = broker
        .ack(AckCommand::new(
            msg_core::DeliveryId::new("unknown").unwrap(),
            consumer_id(),
            timestamp(1),
        ))
        .unwrap_err();
    assert!(matches!(unknown, BrokerError::DeliveryNotFound { .. }));

    let consumed = consume(&mut broker, group_id(), consumer_id(), 10, 2);
    let wrong_consumer = broker
        .ack(AckCommand::new(
            consumed[0].delivery_id().clone(),
            other_consumer_id(),
            timestamp(3),
        ))
        .unwrap_err();
    assert!(matches!(
        wrong_consumer,
        BrokerError::InvalidConsumer { expected, actual, .. }
            if expected == consumer_id() && actual == other_consumer_id()
    ));
}

#[test]
fn nack_honors_backoff_and_redelivers_with_incremented_attempt() {
    let mut broker = BrokerService::new(test_config(3, Some(100), 1_000));
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");

    let first = consume(&mut broker, group_id(), consumer_id(), 10, 10);
    assert_eq!(first[0].attempt_number(), 1);
    broker
        .nack(NackCommand::with_reason(
            first[0].delivery_id().clone(),
            consumer_id(),
            "transient",
            timestamp(20),
        ))
        .unwrap();

    let early = broker.retry_ready(timestamp(119)).unwrap();
    assert_eq!(early.made_available(), 0);
    assert!(consume(&mut broker, group_id(), consumer_id(), 10, 119).is_empty());

    let ready = broker.retry_ready(timestamp(120)).unwrap();
    assert_eq!(ready.retry_scheduled(), 1);
    assert_eq!(ready.made_available(), 1);

    let second = consume(&mut broker, group_id(), consumer_id(), 10, 121);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].attempt_number(), 2);
    assert_ne!(first[0].delivery_id(), second[0].delivery_id());
    assert_eq!(second[0].offset(), Offset::new(0));
}

#[test]
fn lease_expiry_makes_unacked_messages_retryable_without_sleeps() {
    let mut broker = BrokerService::new(test_config(3, None, 50));
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");

    let first = consume(&mut broker, group_id(), consumer_id(), 10, 10);
    assert_eq!(first[0].lease_expires_at(), timestamp(60));
    assert!(consume(&mut broker, group_id(), consumer_id(), 10, 59).is_empty());

    let summary = broker.retry_ready(timestamp(60)).unwrap();
    assert_eq!(summary.lease_expired(), 1);
    assert_eq!(summary.made_available(), 1);

    let second = consume(&mut broker, group_id(), consumer_id(), 10, 61);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].attempt_number(), 2);
}

#[test]
fn max_attempts_routes_to_dlq_and_dlq_entries_are_not_redelivered() {
    let mut broker = BrokerService::new(test_config(2, None, 1_000));
    create_topic(&mut broker, 1);
    publish(&mut broker, "message-1");

    let first = consume(&mut broker, group_id(), consumer_id(), 10, 10);
    broker
        .nack(NackCommand::new(
            first[0].delivery_id().clone(),
            consumer_id(),
            timestamp(11),
        ))
        .unwrap();
    broker.retry_ready(timestamp(11)).unwrap();

    let second = consume(&mut broker, group_id(), consumer_id(), 10, 12);
    assert_eq!(second[0].attempt_number(), 2);
    broker
        .nack(NackCommand::with_reason(
            second[0].delivery_id().clone(),
            consumer_id(),
            "poison",
            timestamp(13),
        ))
        .unwrap();

    let dlq = broker.list_dlq(DlqQuery::all()).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].topic(), &topic_name());
    assert_eq!(dlq[0].partition_id().value(), 0);
    assert_eq!(dlq[0].offset(), Offset::new(0));
    assert_eq!(dlq[0].message_id().as_str(), "message-1");
    assert_eq!(dlq[0].consumer_group_id(), &group_id());
    assert_eq!(
        dlq[0].reason(),
        &DeadLetterReason::Manual("poison".to_owned())
    );
    assert_eq!(dlq[0].attempt_count(), 2);
    assert_eq!(dlq[0].timestamp(), timestamp(13));

    assert!(consume(&mut broker, group_id(), consumer_id(), 10, 14).is_empty());
}

#[test]
fn published_offsets_are_unique_in_a_partition() {
    let mut broker = broker();
    create_topic(&mut broker, 1);

    let offsets: Vec<_> = (0..100)
        .map(|index| publish(&mut broker, format!("message-{index}")).offset())
        .collect();
    let unique: BTreeSet<_> = offsets.iter().copied().collect();

    assert_eq!(unique.len(), offsets.len());
    assert_eq!(offsets.first(), Some(&Offset::new(0)));
    assert_eq!(offsets.last(), Some(&Offset::new(99)));
}

#[test]
fn acked_messages_never_return_again_in_looped_flow() {
    let mut broker = BrokerService::new(test_config(5, None, 1_000));
    create_topic(&mut broker, 1);
    for index in 0..25 {
        publish(&mut broker, format!("message-{index}"));
    }

    for round in 0..5 {
        let consumed = consume(&mut broker, group_id(), consumer_id(), 5, round);
        assert_eq!(consumed.len(), 5);
        for message in consumed {
            broker
                .ack(AckCommand::new(
                    message.delivery_id().clone(),
                    consumer_id(),
                    timestamp(round + 100),
                ))
                .unwrap();
        }
    }

    assert!(consume(&mut broker, group_id(), consumer_id(), 10, 1_000).is_empty());
}

#[test]
fn public_observations_cover_available_pending_acked_retry_and_dlq_states() {
    let mut broker = BrokerService::new(test_config(2, Some(100), 1_000));
    create_topic(&mut broker, 1);
    for index in 0..5 {
        publish(&mut broker, format!("message-{index}"));
    }

    let initial = consume(&mut broker, group_id(), consumer_id(), 4, 10);
    assert_eq!(initial.len(), 4);

    broker
        .ack(AckCommand::new(
            initial[0].delivery_id().clone(),
            consumer_id(),
            timestamp(11),
        ))
        .unwrap();

    broker
        .nack(NackCommand::new(
            initial[1].delivery_id().clone(),
            consumer_id(),
            timestamp(20),
        ))
        .unwrap();

    broker
        .nack(NackCommand::new(
            initial[2].delivery_id().clone(),
            consumer_id(),
            timestamp(13),
        ))
        .unwrap();
    broker.retry_ready(timestamp(113)).unwrap();
    let retry = consume(&mut broker, group_id(), consumer_id(), 1, 114);
    assert_eq!(retry[0].offset(), Offset::new(2));
    assert_eq!(retry[0].attempt_number(), 2);
    broker
        .nack(NackCommand::new(
            retry[0].delivery_id().clone(),
            consumer_id(),
            timestamp(115),
        ))
        .unwrap();

    let visible = consume(&mut broker, group_id(), consumer_id(), 10, 116);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].offset(), Offset::new(4));

    let dlq = broker.list_dlq(DlqQuery::all()).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].offset(), Offset::new(2));
}
