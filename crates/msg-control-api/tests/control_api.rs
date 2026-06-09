use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use msg_broker::{
    BrokerConfig, ConsumeCommand, CreateTopicCommand, DurableBroker, DurableBrokerConfig,
    NackCommand, PublishCommand,
};
use msg_control_api::{AppState, ControlApiConfig, build_router, open_state};
use msg_core::{
    ConsumerGroupId, ConsumerId, ContentType, EventSource, EventType, MessageId, MessagePayload,
    MessageTimestamp, RetryPolicy, TopicConfig, TopicName,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

fn timestamp(value: u64) -> MessageTimestamp {
    MessageTimestamp::from_unix_millis(value)
}

fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn empty_request(method: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&body).unwrap();
    (status, body)
}

fn router_with_temp_state(root: &TempDir) -> Router {
    let state = open_state(ControlApiConfig::new(root.path())).unwrap();
    build_router(state)
}

fn envelope(id: impl AsRef<str>) -> msg_core::MessageEnvelope {
    msg_core::MessageEnvelope::builder(
        MessageId::new(id.as_ref()).unwrap(),
        EventSource::new("/tests").unwrap(),
        EventType::new("order.created").unwrap(),
        ContentType::new("application/json").unwrap(),
        timestamp(1),
        MessagePayload::from_bytes(br#"{"ok":true}"#.to_vec()),
    )
    .build()
}

fn seed_dlq_router() -> Router {
    let root = TempDir::new().unwrap();
    let mut broker = DurableBroker::open(DurableBrokerConfig::new(
        root.path(),
        BrokerConfig::new(RetryPolicy::new(1, None).unwrap(), 1_000).unwrap(),
        1024 * 1024,
    ))
    .unwrap();
    let topic = TopicName::new("orders").unwrap();
    let group = ConsumerGroupId::new("group.1").unwrap();
    let consumer = ConsumerId::new("consumer-1").unwrap();

    broker
        .create_topic(CreateTopicCommand::new(
            topic.clone(),
            TopicConfig::new(1).unwrap(),
        ))
        .unwrap();
    broker
        .publish(PublishCommand::new(topic.clone(), envelope("message-1")))
        .unwrap();
    let consumed = broker
        .consume(ConsumeCommand::new(
            topic,
            group,
            consumer.clone(),
            1,
            timestamp(10),
        ))
        .unwrap();
    broker
        .nack(NackCommand::with_reason(
            consumed[0].delivery_id().clone(),
            consumer,
            "poison",
            timestamp(11),
        ))
        .unwrap();

    build_router(AppState::new(broker))
}

#[tokio::test]
async fn health_and_readiness_return_ok() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    let (status, body) = send(&router, empty_request("GET", "/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "status": "ok" }));

    let (status, body) = send(&router, empty_request("GET", "/ready")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "status": "ready" }));
}

#[tokio::test]
async fn topic_creation_and_duplicate_conflict_use_error_envelope() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);
    let request = json!({ "name": "orders", "partitions": 2 });

    let (status, body) = send(&router, json_request("POST", "/v1/topics", request.clone())).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body, json!({ "name": "orders", "partitions": 2 }));

    let (status, body) = send(&router, json_request("POST", "/v1/topics", request)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], "TOPIC_ALREADY_EXISTS");
    assert_eq!(body["error"]["statusCode"], 409);
    assert_eq!(body["error"]["details"], json!({}));
}

#[tokio::test]
async fn invalid_topic_requests_return_bad_request() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    let (status, body) = send(
        &router,
        json_request(
            "POST",
            "/v1/topics",
            json!({ "name": "bad topic", "partitions": 1 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "INVALID_REQUEST");
    assert_eq!(body["error"]["statusCode"], 400);

    let (status, body) = send(
        &router,
        json_request(
            "POST",
            "/v1/topics",
            json!({ "name": "orders", "partitions": 0 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "INVALID_REQUEST");
    assert_eq!(body["error"]["statusCode"], 400);
}

#[tokio::test]
async fn list_topics_is_deterministic_and_get_topic_reports_missing_topics() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    for name in ["zeta", "alpha", "orders"] {
        let (status, _body) = send(
            &router,
            json_request(
                "POST",
                "/v1/topics",
                json!({ "name": name, "partitions": 1 }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (status, body) = send(&router, empty_request("GET", "/v1/topics")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({
            "items": [
                { "name": "alpha", "partitions": 1 },
                { "name": "orders", "partitions": 1 },
                { "name": "zeta", "partitions": 1 }
            ]
        })
    );

    let (status, body) = send(&router, empty_request("GET", "/v1/topics/orders")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "name": "orders", "partitions": 1 }));

    let (status, body) = send(&router, empty_request("GET", "/v1/topics/missing")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
    assert_eq!(body["error"]["statusCode"], 404);
}

#[tokio::test]
async fn topic_metadata_survives_reopened_state() {
    let root = TempDir::new().unwrap();
    {
        let router = router_with_temp_state(&root);
        let (status, _body) = send(
            &router,
            json_request(
                "POST",
                "/v1/topics",
                json!({ "name": "orders", "partitions": 3 }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let router = router_with_temp_state(&root);
    let (status, body) = send(&router, empty_request("GET", "/v1/topics")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({ "items": [{ "name": "orders", "partitions": 3 }] })
    );

    let (status, body) = send(&router, empty_request("GET", "/v1/topics/orders")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "name": "orders", "partitions": 3 }));
}

#[tokio::test]
async fn status_reports_local_durable_counts() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);
    let (status, _body) = send(
        &router,
        json_request(
            "POST",
            "/v1/topics",
            json!({ "name": "orders", "partitions": 1 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = send(&router, empty_request("GET", "/v1/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["mode"], "local-durable");
    assert_eq!(body["topics"], 1);
    assert_eq!(body["dlqEntries"], 0);
    assert!(
        body["dataDir"]
            .as_str()
            .unwrap()
            .contains(root.path().to_str().unwrap())
    );
}

#[tokio::test]
async fn dlq_returns_stable_items_envelope() {
    let router = seed_dlq_router();

    let (status, body) = send(&router, empty_request("GET", "/v1/dlq")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({
            "items": [{
                "topic": "orders",
                "partition": 0,
                "offset": 0,
                "messageId": "message-1",
                "consumerGroupId": "group.1",
                "reason": "poison",
                "attemptCount": 1,
                "timestamp": 11
            }]
        })
    );

    let (status, body) = send(&router, empty_request("GET", "/v1/dlq?topic=orders")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn malformed_json_returns_stable_bad_request_envelope() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);
    let request = Request::builder()
        .method("POST")
        .uri("/v1/topics")
        .header("content-type", "application/json")
        .body(Body::from("{"))
        .unwrap();

    let (status, body) = send(&router, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "INVALID_REQUEST");
    assert_eq!(
        body["error"]["message"],
        "request body must be valid JSON for this endpoint"
    );
    assert_eq!(body["error"]["details"], json!({}));
    assert_eq!(body["error"]["statusCode"], 400);
}
