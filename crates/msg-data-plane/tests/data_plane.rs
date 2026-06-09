use msg_broker::{BrokerConfig, CreateTopicCommand, DlqQuery, DurableBroker, DurableBrokerConfig};
use msg_core::{DeadLetterReason, RetryPolicy, TopicConfig, TopicName};
use msg_data_plane::DataPlaneService;
use msg_protocol::ferrumq::dataplane::v1::{
    AckRequest, ConsumeRequest, ConsumedMessage, NackRequest, PublishRequest, PublishResponse,
    ferrum_q_data_plane_server::FerrumQDataPlane,
};
use tempfile::TempDir;
use tonic::{Code, Request};

const MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const FAR_FUTURE_MS: u64 = u64::MAX - 1_000_000;

fn broker_config(max_attempts: u32, backoff_millis: Option<u64>) -> BrokerConfig {
    BrokerConfig::new(
        RetryPolicy::new(max_attempts, backoff_millis).unwrap(),
        30_000,
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

fn open_broker(root: &TempDir, max_attempts: u32, backoff_millis: Option<u64>) -> DurableBroker {
    DurableBroker::open(durable_config(root, max_attempts, backoff_millis)).unwrap()
}

fn topic_name() -> TopicName {
    TopicName::new("orders").unwrap()
}

fn create_topic_with_partitions(broker: &mut DurableBroker, partitions: u32) {
    broker
        .create_topic(CreateTopicCommand::new(
            topic_name(),
            TopicConfig::new(partitions).unwrap(),
        ))
        .unwrap();
}

fn create_topic(broker: &mut DurableBroker) {
    create_topic_with_partitions(broker, 1);
}

fn service_with_topic(
    max_attempts: u32,
    backoff_millis: Option<u64>,
) -> (TempDir, DataPlaneService) {
    service_with_topic_and_partitions(max_attempts, backoff_millis, 1)
}

fn service_with_topic_and_partitions(
    max_attempts: u32,
    backoff_millis: Option<u64>,
    partitions: u32,
) -> (TempDir, DataPlaneService) {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, max_attempts, backoff_millis);
    create_topic_with_partitions(&mut broker, partitions);
    (root, DataPlaneService::new(broker))
}

fn publish_request(message_id: &str) -> PublishRequest {
    PublishRequest {
        topic: "orders".to_owned(),
        message_id: message_id.to_owned(),
        key: "account-1".to_owned(),
        payload: br#"{"ok":true}"#.to_vec(),
        content_type: "application/json".to_owned(),
        r#type: "order.created".to_owned(),
        source: "/tests".to_owned(),
        subject: "subject-1".to_owned(),
        idempotency_key: "idem-1".to_owned(),
        time_unix_ms: 1_700_000_000_000,
    }
}

fn consume_request(now_unix_ms: u64) -> ConsumeRequest {
    ConsumeRequest {
        topic: "orders".to_owned(),
        consumer_group: "group.1".to_owned(),
        consumer_id: "consumer-1".to_owned(),
        max_messages: 10,
        lease_ms: 50,
        now_unix_ms,
    }
}

fn ack_request(delivery_id: impl Into<String>, consumer_id: impl Into<String>) -> AckRequest {
    AckRequest {
        delivery_id: delivery_id.into(),
        consumer_id: consumer_id.into(),
    }
}

fn nack_request(
    delivery_id: impl Into<String>,
    consumer_id: impl Into<String>,
    reason: impl Into<String>,
) -> NackRequest {
    NackRequest {
        delivery_id: delivery_id.into(),
        consumer_id: consumer_id.into(),
        reason: reason.into(),
    }
}

async fn publish_response(service: &DataPlaneService, request: PublishRequest) -> PublishResponse {
    service
        .publish(Request::new(request))
        .await
        .unwrap()
        .into_inner()
}

async fn publish(service: &DataPlaneService, message_id: &str) {
    publish_response(service, publish_request(message_id)).await;
}

async fn consume_messages(
    service: &DataPlaneService,
    request: ConsumeRequest,
) -> Vec<ConsumedMessage> {
    service
        .consume(Request::new(request))
        .await
        .unwrap()
        .into_inner()
        .messages
}

fn assert_status<T: std::fmt::Debug>(result: Result<T, tonic::Status>, code: Code, message: &str) {
    let error = result.unwrap_err();
    assert_eq!(error.code(), code);
    assert_eq!(error.message(), message);
}

fn assert_invalid_contains<T: std::fmt::Debug>(
    result: Result<T, tonic::Status>,
    expected_message: &str,
) {
    let error = result.unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains(expected_message),
        "expected {:?} to contain {:?}",
        error.message(),
        expected_message
    );
}

#[tokio::test]
async fn publish_success_returns_storage_positions_and_monotonic_offsets() {
    let (_root, service) = service_with_topic(3, Some(1_000));

    let responses: Vec<_> = ["message-1", "message-2", "message-3"]
        .into_iter()
        .map(publish_request)
        .collect();

    for (index, request) in responses.into_iter().enumerate() {
        let response = publish_response(&service, request).await;
        assert_eq!(response.topic, "orders");
        assert_eq!(response.partition, 0);
        assert_eq!(response.offset, index as u64);
        assert_eq!(response.message_id, format!("message-{}", index + 1));
    }
}

#[tokio::test]
async fn publish_rejects_empty_and_unknown_topics() {
    let (_root, service) = service_with_topic(3, Some(1_000));

    let mut empty_topic = publish_request("message-1");
    empty_topic.topic.clear();
    assert_status(
        service.publish(Request::new(empty_topic)).await,
        Code::InvalidArgument,
        "topic_name must not be empty",
    );

    let mut unknown = publish_request("message-2");
    unknown.topic = "payments".to_owned();
    assert_status(
        service.publish(Request::new(unknown)).await,
        Code::NotFound,
        "topic not found",
    );
}

#[tokio::test]
async fn publish_uses_deterministic_key_partition_and_keeps_idempotency_metadata_only() {
    let (_root, service) = service_with_topic_and_partitions(3, Some(1_000), 4);

    let mut first = publish_request("message-1");
    first.key = "same-account".to_owned();
    first.idempotency_key = "same-idempotency-key".to_owned();
    let mut second = publish_request("message-2");
    second.key = "same-account".to_owned();
    second.idempotency_key = "same-idempotency-key".to_owned();

    let first = publish_response(&service, first).await;
    let second = publish_response(&service, second).await;

    assert_eq!(first.partition, second.partition);
    assert_eq!(first.offset, 0);
    assert_eq!(second.offset, 1);

    let messages = consume_messages(&service, consume_request(10)).await;
    let ids: Vec<_> = messages
        .iter()
        .map(|message| message.message_id.as_str())
        .collect();
    assert_eq!(ids, ["message-1", "message-2"]);
    assert!(
        messages
            .iter()
            .all(|message| message.idempotency_key == "same-idempotency-key")
    );
}

#[tokio::test]
async fn publish_allows_empty_payload_and_optional_metadata_round_trip() {
    let (_root, service) = service_with_topic(3, Some(1_000));
    let mut request = publish_request("message-1");
    request.key.clear();
    request.payload.clear();
    request.subject.clear();
    request.idempotency_key.clear();
    request.content_type = "application/octet-stream".to_owned();
    request.r#type = "opaque.bytes".to_owned();
    request.source = "/binary-tests".to_owned();
    request.time_unix_ms = 42;

    publish_response(&service, request).await;
    let messages = consume_messages(&service, consume_request(10)).await;

    assert_eq!(messages.len(), 1);
    let message = &messages[0];
    assert_eq!(message.key, "");
    assert!(message.payload.is_empty());
    assert_eq!(message.subject, "");
    assert_eq!(message.idempotency_key, "");
    assert_eq!(message.content_type, "application/octet-stream");
    assert_eq!(message.r#type, "opaque.bytes");
    assert_eq!(message.source, "/binary-tests");
    assert_eq!(message.time_unix_ms, 42);
}

#[tokio::test]
async fn large_payload_is_durable_after_reopen() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, Some(1_000));
    create_topic(&mut broker);
    let service = DataPlaneService::new(broker);
    let payload: Vec<_> = (0..(128 * 1024)).map(|index| (index % 251) as u8).collect();
    let mut request = publish_request("large-message");
    request.payload = payload.clone();

    publish_response(&service, request).await;
    drop(service);

    let reopened = DataPlaneService::new(open_broker(&root, 3, Some(1_000)));
    let messages = consume_messages(&reopened, consume_request(10)).await;

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, "large-message");
    assert_eq!(messages[0].payload.len(), payload.len());
    assert_eq!(messages[0].payload, payload);
}

#[tokio::test]
async fn consume_after_publish_returns_payload_and_metadata() {
    let (_root, service) = service_with_topic(3, Some(1_000));
    publish(&service, "message-1").await;

    let messages = consume_messages(&service, consume_request(10)).await;

    assert_eq!(messages.len(), 1);
    let message = &messages[0];
    assert!(!message.delivery_id.is_empty());
    assert_eq!(message.topic, "orders");
    assert_eq!(message.partition, 0);
    assert_eq!(message.offset, 0);
    assert_eq!(message.message_id, "message-1");
    assert_eq!(message.key, "account-1");
    assert_eq!(message.payload, br#"{"ok":true}"#);
    assert_eq!(message.content_type, "application/json");
    assert_eq!(message.r#type, "order.created");
    assert_eq!(message.source, "/tests");
    assert_eq!(message.subject, "subject-1");
    assert_eq!(message.idempotency_key, "idem-1");
    assert_eq!(message.time_unix_ms, 1_700_000_000_000);
    assert_eq!(message.consumer_group, "group.1");
    assert_eq!(message.consumer_id, "consumer-1");
    assert_eq!(message.attempt_number, 1);
    assert_eq!(message.delivered_at_unix_ms, 10);
    assert_eq!(message.lease_expires_at_unix_ms, 60);
}

#[tokio::test]
async fn consume_validation_unknown_topic_invalid_identifiers_and_limits() {
    let (_root, service) = service_with_topic(3, Some(1_000));

    let mut unknown = consume_request(10);
    unknown.topic = "payments".to_owned();
    assert_status(
        service.consume(Request::new(unknown)).await,
        Code::NotFound,
        "topic not found",
    );

    let mut empty_topic = consume_request(10);
    empty_topic.topic.clear();
    assert_status(
        service.consume(Request::new(empty_topic)).await,
        Code::InvalidArgument,
        "topic_name must not be empty",
    );

    let mut invalid_group = consume_request(10);
    invalid_group.consumer_group = "group one".to_owned();
    assert_invalid_contains(
        service.consume(Request::new(invalid_group)).await,
        "consumer_group_id",
    );

    let mut invalid_consumer = consume_request(10);
    invalid_consumer.consumer_id.clear();
    assert_invalid_contains(
        service.consume(Request::new(invalid_consumer)).await,
        "consumer_id",
    );

    let mut invalid_max = consume_request(10);
    invalid_max.max_messages = 0;
    assert_status(
        service.consume(Request::new(invalid_max)).await,
        Code::InvalidArgument,
        "max_messages must be greater than zero",
    );

    let mut invalid_lease = consume_request(10);
    invalid_lease.lease_ms = 0;
    assert_status(
        service.consume(Request::new(invalid_lease)).await,
        Code::InvalidArgument,
        "lease_ms must be greater than zero",
    );

    let empty = consume_messages(&service, consume_request(10)).await;
    assert!(empty.is_empty());
}

#[tokio::test]
async fn consume_honors_max_limit_and_stable_ordering() {
    let (_root, service) = service_with_topic(3, Some(1_000));
    publish(&service, "message-1").await;
    publish(&service, "message-2").await;
    publish(&service, "message-3").await;
    let mut request = consume_request(10);
    request.max_messages = 2;

    let messages = consume_messages(&service, request).await;
    let ids: Vec<_> = messages
        .iter()
        .map(|message| message.message_id.as_str())
        .collect();
    let offsets: Vec<_> = messages.iter().map(|message| message.offset).collect();

    assert_eq!(ids, ["message-1", "message-2"]);
    assert_eq!(offsets, [0, 1]);
}

#[tokio::test]
async fn leased_in_flight_delivery_is_not_immediately_redelivered() {
    let (_root, service) = service_with_topic(3, Some(1_000));
    publish(&service, "message-1").await;

    let first = consume_messages(&service, consume_request(10)).await;
    assert_eq!(first.len(), 1);

    let second = consume_messages(&service, consume_request(11)).await;
    assert!(second.is_empty());
}

#[tokio::test]
async fn lease_expiry_redelivers_deterministically_without_sleep() {
    let (_root, service) = service_with_topic(3, None);
    publish(&service, "message-1").await;

    let first = consume_messages(&service, consume_request(10)).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].attempt_number, 1);

    let redelivered = consume_messages(&service, consume_request(60)).await;
    assert_eq!(redelivered.len(), 1);
    assert_eq!(redelivered[0].message_id, "message-1");
    assert_eq!(redelivered[0].attempt_number, 2);
    assert_ne!(redelivered[0].delivery_id, first[0].delivery_id);
    assert_eq!(redelivered[0].delivered_at_unix_ms, 60);
    assert_eq!(redelivered[0].lease_expires_at_unix_ms, 110);
}

#[tokio::test]
async fn retry_maintenance_does_not_dlq_before_max_attempts() {
    let (_root, service) = service_with_topic(2, None);
    publish(&service, "message-1").await;
    let first = consume_messages(&service, consume_request(10)).await;

    service
        .nack(Request::new(nack_request(
            first[0].delivery_id.clone(),
            "consumer-1",
            "transient",
        )))
        .await
        .unwrap();

    let retried = consume_messages(&service, consume_request(FAR_FUTURE_MS)).await;
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].attempt_number, 2);

    let broker = service.broker();
    let dlq = broker.lock().unwrap().list_dlq(DlqQuery::all()).unwrap();
    assert!(dlq.is_empty());
}

#[tokio::test]
async fn ack_success_errors_and_durability() {
    let (root, service) = service_with_topic(3, Some(1_000));
    publish(&service, "message-1").await;
    let consumed = consume_messages(&service, consume_request(10)).await;
    let delivery_id = consumed[0].delivery_id.clone();

    assert_status(
        service
            .ack(Request::new(ack_request("missing-delivery", "consumer-1")))
            .await,
        Code::NotFound,
        "delivery not found",
    );

    assert_status(
        service
            .ack(Request::new(ack_request(delivery_id.clone(), "consumer-2")))
            .await,
        Code::FailedPrecondition,
        "invalid delivery ownership",
    );

    service
        .ack(Request::new(ack_request(delivery_id.clone(), "consumer-1")))
        .await
        .unwrap();

    assert_status(
        service
            .ack(Request::new(ack_request(delivery_id, "consumer-1")))
            .await,
        Code::NotFound,
        "delivery not found",
    );

    drop(service);
    let reopened = DataPlaneService::new(open_broker(&root, 3, Some(1_000)));
    let empty = consume_messages(&reopened, consume_request(1_000)).await;
    assert!(empty.is_empty());
}

#[tokio::test]
async fn nack_errors_duplicate_state_transitions_and_ack_after_nack() {
    let (_root, service) = service_with_topic(3, None);
    publish(&service, "message-1").await;
    let first = consume_messages(&service, consume_request(10)).await;
    let first_delivery = first[0].delivery_id.clone();

    assert_status(
        service
            .nack(Request::new(nack_request(
                first_delivery.clone(),
                "consumer-2",
                "wrong-owner",
            )))
            .await,
        Code::FailedPrecondition,
        "invalid delivery ownership",
    );

    service
        .nack(Request::new(nack_request(
            first_delivery.clone(),
            "consumer-1",
            "transient",
        )))
        .await
        .unwrap();

    assert_status(
        service
            .nack(Request::new(nack_request(
                first_delivery.clone(),
                "consumer-1",
                "duplicate",
            )))
            .await,
        Code::NotFound,
        "delivery not found",
    );
    assert_status(
        service
            .ack(Request::new(ack_request(first_delivery, "consumer-1")))
            .await,
        Code::NotFound,
        "delivery not found",
    );

    let retried = consume_messages(&service, consume_request(FAR_FUTURE_MS)).await;
    assert_eq!(retried.len(), 1);
    service
        .ack(Request::new(ack_request(
            retried[0].delivery_id.clone(),
            "consumer-1",
        )))
        .await
        .unwrap();

    assert_status(
        service
            .nack(Request::new(nack_request(
                retried[0].delivery_id.clone(),
                "consumer-1",
                "after-ack",
            )))
            .await,
        Code::NotFound,
        "delivery not found",
    );
}

#[tokio::test]
async fn nack_unknown_delivery_and_retry_persistence_across_reopen() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, None);
    create_topic(&mut broker);
    let service = DataPlaneService::new(broker);
    publish(&service, "message-1").await;
    let consumed = consume_messages(&service, consume_request(10)).await;

    assert_status(
        service
            .nack(Request::new(nack_request(
                "missing-delivery",
                "consumer-1",
                "transient",
            )))
            .await,
        Code::NotFound,
        "delivery not found",
    );

    service
        .nack(Request::new(nack_request(
            consumed[0].delivery_id.clone(),
            "consumer-1",
            "transient",
        )))
        .await
        .unwrap();
    drop(service);

    let reopened = DataPlaneService::new(open_broker(&root, 3, None));
    let retried = consume_messages(&reopened, consume_request(FAR_FUTURE_MS)).await;
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].message_id, "message-1");
    assert_eq!(retried[0].attempt_number, 2);
}

#[tokio::test]
async fn dlq_durability_reason_preservation_and_no_redelivery() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 2, None);
    create_topic(&mut broker);
    let service = DataPlaneService::new(broker);
    publish(&service, "message-1").await;

    let first = consume_messages(&service, consume_request(10)).await;
    service
        .nack(Request::new(nack_request(
            first[0].delivery_id.clone(),
            "consumer-1",
            "transient",
        )))
        .await
        .unwrap();
    let retried = consume_messages(&service, consume_request(FAR_FUTURE_MS)).await;
    service
        .nack(Request::new(nack_request(
            retried[0].delivery_id.clone(),
            "consumer-1",
            "poison",
        )))
        .await
        .unwrap();
    drop(service);

    let reopened = DataPlaneService::new(open_broker(&root, 2, None));
    let broker = reopened.broker();
    let dlq = broker.lock().unwrap().list_dlq(DlqQuery::all()).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].message_id().as_str(), "message-1");
    assert_eq!(dlq[0].attempt_count(), 2);
    assert_eq!(
        dlq[0].reason(),
        &DeadLetterReason::Manual("poison".to_owned())
    );

    let empty = consume_messages(&reopened, consume_request(FAR_FUTURE_MS + 100)).await;
    assert!(empty.is_empty());
}

#[tokio::test]
async fn publish_consume_ack_full_durability_flow() {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, 3, Some(1_000));
    create_topic(&mut broker);
    let service = DataPlaneService::new(broker);

    publish(&service, "message-1").await;
    drop(service);

    let reopened = DataPlaneService::new(open_broker(&root, 3, Some(1_000)));
    let consumed = consume_messages(&reopened, consume_request(10)).await;
    assert_eq!(consumed.len(), 1);
    reopened
        .ack(Request::new(ack_request(
            consumed[0].delivery_id.clone(),
            "consumer-1",
        )))
        .await
        .unwrap();
    drop(reopened);

    let reopened = DataPlaneService::new(open_broker(&root, 3, Some(1_000)));
    let empty = consume_messages(&reopened, consume_request(1_000)).await;
    assert!(empty.is_empty());
}
