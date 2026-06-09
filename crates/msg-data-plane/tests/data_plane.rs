use msg_broker::{BrokerConfig, CreateTopicCommand, DlqQuery, DurableBroker, DurableBrokerConfig};
use msg_core::{RetryPolicy, TopicConfig, TopicName};
use msg_data_plane::DataPlaneService;
use msg_protocol::ferrumq::dataplane::v1::{
    AckRequest, ConsumeRequest, NackRequest, PublishRequest,
    ferrum_q_data_plane_server::FerrumQDataPlane,
};
use tempfile::TempDir;
use tonic::{Code, Request};

const MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

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

fn create_topic(broker: &mut DurableBroker) {
    broker
        .create_topic(CreateTopicCommand::new(
            topic_name(),
            TopicConfig::new(1).unwrap(),
        ))
        .unwrap();
}

fn service_with_topic(
    max_attempts: u32,
    backoff_millis: Option<u64>,
) -> (TempDir, DataPlaneService) {
    let root = TempDir::new().unwrap();
    let mut broker = open_broker(&root, max_attempts, backoff_millis);
    create_topic(&mut broker);
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

async fn publish(service: &DataPlaneService, message_id: &str) {
    service
        .publish(Request::new(publish_request(message_id)))
        .await
        .unwrap();
}

fn assert_status<T: std::fmt::Debug>(result: Result<T, tonic::Status>, code: Code, message: &str) {
    let error = result.unwrap_err();
    assert_eq!(error.code(), code);
    assert_eq!(error.message(), message);
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
async fn publish_success_returns_storage_position() {
    let (_root, service) = service_with_topic(3, Some(1_000));

    let response = service
        .publish(Request::new(publish_request("message-1")))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.topic, "orders");
    assert_eq!(response.partition, 0);
    assert_eq!(response.offset, 0);
    assert_eq!(response.message_id, "message-1");
}

#[tokio::test]
async fn publish_maps_unknown_and_invalid_topics_and_allows_empty_payloads() {
    let (_root, service) = service_with_topic(3, Some(1_000));

    let mut unknown = publish_request("message-1");
    unknown.topic = "payments".to_owned();
    assert_status(
        service.publish(Request::new(unknown)).await,
        Code::NotFound,
        "topic not found",
    );

    let mut invalid = publish_request("message-2");
    invalid.topic = "bad topic".to_owned();
    let error = service.publish(Request::new(invalid)).await.unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("topic_name"));

    let mut empty_payload = publish_request("message-3");
    empty_payload.payload = Vec::new();
    service.publish(Request::new(empty_payload)).await.unwrap();
}

#[tokio::test]
async fn consume_after_publish_returns_payload_and_metadata() {
    let (_root, service) = service_with_topic(3, Some(1_000));
    publish(&service, "message-1").await;

    let response = service
        .consume(Request::new(consume_request(10)))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.messages.len(), 1);
    let message = &response.messages[0];
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
async fn consume_validation_unknown_topic_and_empty_topic_behavior() {
    let (_root, service) = service_with_topic(3, Some(1_000));

    let mut unknown = consume_request(10);
    unknown.topic = "payments".to_owned();
    assert_status(
        service.consume(Request::new(unknown)).await,
        Code::NotFound,
        "topic not found",
    );

    let mut invalid_group = consume_request(10);
    invalid_group.consumer_group = "group one".to_owned();
    let error = service
        .consume(Request::new(invalid_group))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("consumer_group_id"));

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

    let empty = service
        .consume(Request::new(consume_request(10)))
        .await
        .unwrap()
        .into_inner();
    assert!(empty.messages.is_empty());
}

#[tokio::test]
async fn leased_in_flight_delivery_is_not_immediately_redelivered() {
    let (_root, service) = service_with_topic(3, Some(1_000));
    publish(&service, "message-1").await;

    let first = service
        .consume(Request::new(consume_request(10)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.messages.len(), 1);

    let second = service
        .consume(Request::new(consume_request(11)))
        .await
        .unwrap()
        .into_inner();
    assert!(second.messages.is_empty());
}

#[tokio::test]
async fn ack_success_errors_and_durability() {
    let (root, service) = service_with_topic(3, Some(1_000));
    publish(&service, "message-1").await;
    let consumed = service
        .consume(Request::new(consume_request(10)))
        .await
        .unwrap()
        .into_inner();
    let delivery_id = consumed.messages[0].delivery_id.clone();

    assert_status(
        service
            .ack(Request::new(AckRequest {
                delivery_id: "missing-delivery".to_owned(),
                consumer_id: "consumer-1".to_owned(),
            }))
            .await,
        Code::NotFound,
        "delivery not found",
    );

    assert_status(
        service
            .ack(Request::new(AckRequest {
                delivery_id: delivery_id.clone(),
                consumer_id: "consumer-2".to_owned(),
            }))
            .await,
        Code::FailedPrecondition,
        "invalid delivery ownership",
    );

    service
        .ack(Request::new(AckRequest {
            delivery_id: delivery_id.clone(),
            consumer_id: "consumer-1".to_owned(),
        }))
        .await
        .unwrap();

    assert_status(
        service
            .ack(Request::new(AckRequest {
                delivery_id,
                consumer_id: "consumer-1".to_owned(),
            }))
            .await,
        Code::NotFound,
        "delivery not found",
    );

    drop(service);
    let reopened = DataPlaneService::new(open_broker(&root, 3, Some(1_000)));
    let empty = reopened
        .consume(Request::new(consume_request(1_000)))
        .await
        .unwrap()
        .into_inner();
    assert!(empty.messages.is_empty());
}

#[tokio::test]
async fn nack_retries_unknown_delivery_and_routes_to_dlq() {
    let (_root, service) = service_with_topic(2, None);
    publish(&service, "message-1").await;
    let consumed = service
        .consume(Request::new(consume_request(10)))
        .await
        .unwrap()
        .into_inner();
    let first_delivery = consumed.messages[0].delivery_id.clone();

    assert_status(
        service
            .nack(Request::new(NackRequest {
                delivery_id: "missing-delivery".to_owned(),
                consumer_id: "consumer-1".to_owned(),
                reason: "transient".to_owned(),
            }))
            .await,
        Code::NotFound,
        "delivery not found",
    );

    service
        .nack(Request::new(NackRequest {
            delivery_id: first_delivery,
            consumer_id: "consumer-1".to_owned(),
            reason: "transient".to_owned(),
        }))
        .await
        .unwrap();

    let retried = service
        .consume(Request::new(consume_request(now_ms() + 1)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retried.messages.len(), 1);
    assert_eq!(retried.messages[0].attempt_number, 2);

    service
        .nack(Request::new(NackRequest {
            delivery_id: retried.messages[0].delivery_id.clone(),
            consumer_id: "consumer-1".to_owned(),
            reason: "poison".to_owned(),
        }))
        .await
        .unwrap();

    let empty = service
        .consume(Request::new(consume_request(now_ms() + 1)))
        .await
        .unwrap()
        .into_inner();
    assert!(empty.messages.is_empty());

    let broker = service.broker();
    let dlq = broker.lock().unwrap().list_dlq(DlqQuery::all()).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].message_id().as_str(), "message-1");
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
    let consumed = reopened
        .consume(Request::new(consume_request(10)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(consumed.messages.len(), 1);
    reopened
        .ack(Request::new(AckRequest {
            delivery_id: consumed.messages[0].delivery_id.clone(),
            consumer_id: "consumer-1".to_owned(),
        }))
        .await
        .unwrap();
    drop(reopened);

    let reopened = DataPlaneService::new(open_broker(&root, 3, Some(1_000)));
    let empty = reopened
        .consume(Request::new(consume_request(1_000)))
        .await
        .unwrap()
        .into_inner();
    assert!(empty.messages.is_empty());
}
