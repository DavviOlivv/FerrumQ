use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode, header},
};
use msg_broker::{
    BrokerConfig, ConsumeCommand, CreateTopicCommand, DurableBroker, DurableBrokerConfig,
    NackCommand, PublishCommand,
};
use msg_control_api::{ControlApiConfig, build_router, open_state};
use msg_core::{
    ConsumerGroupId, ConsumerId, ContentType, EventSource, EventType, MessageId, MessagePayload,
    MessageTimestamp, RetryPolicy, TopicConfig, TopicName,
};
use msg_observability::{PROMETHEUS_CONTENT_TYPE, metric_names, metrics};
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

async fn send_response(router: &Router, request: Request<Body>) -> (StatusCode, HeaderMap, Value) {
    let (status, headers, body) = send_raw(router, request).await;
    let body = serde_json::from_slice(&body).unwrap();
    (status, headers, body)
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let (status, _headers, body) = send_response(router, request).await;
    (status, body)
}

async fn send_raw(router: &Router, request: Request<Body>) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, headers, body.to_vec())
}

fn assert_json_content_type(headers: &HeaderMap) {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    assert!(content_type.starts_with("application/json"));
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

fn seed_dlq_router(root: &TempDir) -> Router {
    {
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
    }

    router_with_temp_state(root)
}

#[tokio::test]
async fn health_and_readiness_return_ok() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    let (status, headers, body) = send_response(&router, empty_request("GET", "/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_json_content_type(&headers);
    assert_eq!(body, json!({ "status": "ok" }));

    let (status, body) = send(&router, empty_request("GET", "/ready")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "status": "ready" }));
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_text_with_known_names() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    let (status, headers, body) = send_raw(&router, empty_request("GET", "/metrics")).await;

    assert_eq!(status, StatusCode::OK);
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    assert_eq!(content_type, PROMETHEUS_CONTENT_TYPE);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains(metric_names::CONTROL_HTTP_REQUESTS_TOTAL));
    assert!(body.contains(metric_names::CONTROL_TOPICS_CREATED_TOTAL));
    assert!(body.contains(metric_names::BROKER_TOPICS_CREATED_TOTAL));
}

#[tokio::test]
async fn topic_creation_and_errors_update_control_metrics() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);
    let created_before = metrics::counter_value(
        metric_names::CONTROL_TOPICS_CREATED_TOTAL,
        &[("status", "success")],
    );
    let conflict_before = metrics::counter_value(
        metric_names::CONTROL_HTTP_ERRORS_TOTAL,
        &[
            ("method", "POST"),
            ("route", "/v1/topics"),
            ("status", "409"),
            ("code", "TOPIC_ALREADY_EXISTS"),
        ],
    );
    let validation_before = metrics::counter_value(
        metric_names::CONTROL_HTTP_ERRORS_TOTAL,
        &[
            ("method", "POST"),
            ("route", "/v1/topics"),
            ("status", "400"),
            ("code", "VALIDATION_ERROR"),
        ],
    );

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

    let (status, _body) = send(
        &router,
        json_request(
            "POST",
            "/v1/topics",
            json!({ "name": "orders", "partitions": 1 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, _body) = send(
        &router,
        json_request(
            "POST",
            "/v1/topics",
            json!({ "name": "bad topic", "partitions": 1 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let created_after = metrics::counter_value(
        metric_names::CONTROL_TOPICS_CREATED_TOTAL,
        &[("status", "success")],
    );
    let conflict_after = metrics::counter_value(
        metric_names::CONTROL_HTTP_ERRORS_TOTAL,
        &[
            ("method", "POST"),
            ("route", "/v1/topics"),
            ("status", "409"),
            ("code", "TOPIC_ALREADY_EXISTS"),
        ],
    );
    let validation_after = metrics::counter_value(
        metric_names::CONTROL_HTTP_ERRORS_TOTAL,
        &[
            ("method", "POST"),
            ("route", "/v1/topics"),
            ("status", "400"),
            ("code", "VALIDATION_ERROR"),
        ],
    );

    assert!(created_after > created_before);
    assert!(conflict_after > conflict_before);
    assert!(validation_after > validation_before);
}

#[tokio::test]
async fn metrics_output_does_not_include_message_payloads() {
    let root = TempDir::new().unwrap();
    let router = seed_dlq_router(&root);

    let (status, _headers, body) = send_raw(&router, empty_request("GET", "/metrics")).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    assert!(!body.contains(r#"{"ok":true}"#));
    assert!(!body.contains("payload"));
}

#[tokio::test]
async fn readiness_failure_is_sanitized_when_broker_state_is_unavailable() {
    let root = TempDir::new().unwrap();
    let state = open_state(ControlApiConfig::new(root.path())).unwrap();
    let broker = state.broker();
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = broker.lock().unwrap();
        panic!("poison broker lock");
    }));
    let router = build_router(state);

    let (status, body) = send(&router, empty_request("GET", "/ready")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        body,
        json!({
            "error": {
                "code": "BROKER_UNAVAILABLE",
                "message": "durable broker state is not accessible",
                "details": {},
                "statusCode": 503
            }
        })
    );
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
    assert_eq!(body["error"]["message"], "topic already exists: orders");
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
    assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
    assert_eq!(
        body["error"]["message"],
        "topic_name contains invalid characters; allowed: ASCII letters, digits, '.', '_', '-'"
    );
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
    assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
    assert_eq!(
        body["error"]["message"],
        "partition_count must be at least 1; got 0"
    );
    assert_eq!(body["error"]["statusCode"], 400);
}

#[tokio::test]
async fn topic_names_with_supported_punctuation_and_encoded_paths_work() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    for name in ["orders.2026", "orders-west", "orders_west"] {
        let (status, body) = send(
            &router,
            json_request(
                "POST",
                "/v1/topics",
                json!({ "name": name, "partitions": 1 }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body, json!({ "name": name, "partitions": 1 }));
    }

    let (status, body) = send(&router, empty_request("GET", "/v1/topics/orders%2E2026")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "name": "orders.2026", "partitions": 1 }));
}

#[tokio::test]
async fn valid_multi_partition_topics_return_stable_shape() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    let (status, body) = send(
        &router,
        json_request(
            "POST",
            "/v1/topics",
            json!({ "name": "wide-topic", "partitions": 16 }),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body, json!({ "name": "wide-topic", "partitions": 16 }));
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
    assert_eq!(body["error"]["code"], "TOPIC_NOT_FOUND");
    assert_eq!(body["error"]["message"], "topic not found: missing");
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

    let (status, body) = send(
        &router,
        json_request(
            "POST",
            "/v1/topics",
            json!({ "name": "orders", "partitions": 3 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], "TOPIC_ALREADY_EXISTS");

    let (status, body) = send(&router, empty_request("GET", "/v1/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["topics"], 1);
    assert_eq!(body["dlqEntries"], 0);
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
    for key in body.as_object().unwrap().keys() {
        let key = key.to_ascii_lowercase();
        assert!(!key.contains("password"));
        assert!(!key.contains("secret"));
        assert!(!key.contains("token"));
    }
    assert!(
        body["dataDir"]
            .as_str()
            .unwrap()
            .contains(root.path().to_str().unwrap())
    );
}

#[tokio::test]
async fn dlq_returns_stable_items_envelope() {
    let root = TempDir::new().unwrap();
    let router = seed_dlq_router(&root);

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

    let (status, body) = send(&router, empty_request("GET", "/v1/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["topics"], 1);
    assert_eq!(body["dlqEntries"], 1);
}

#[tokio::test]
async fn dlq_topic_filter_errors_use_stable_envelopes() {
    let root = TempDir::new().unwrap();
    let router = seed_dlq_router(&root);

    let (status, body) = send(&router, empty_request("GET", "/v1/dlq?topic=missing")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "TOPIC_NOT_FOUND");
    assert_eq!(body["error"]["message"], "topic not found: missing");

    let (status, body) = send(&router, empty_request("GET", "/v1/dlq?topic=bad%20topic")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
    assert_eq!(
        body["error"]["message"],
        "topic_name contains invalid characters; allowed: ASCII letters, digits, '.', '_', '-'"
    );
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
    assert_eq!(body["error"]["message"], "request body must be valid JSON");
    assert_eq!(body["error"]["details"], json!({}));
    assert_eq!(body["error"]["statusCode"], 400);
}

#[tokio::test]
async fn json_shape_errors_return_stable_bad_request_envelopes() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    for body in [
        json!({ "name": "orders" }),
        json!({ "name": "orders", "partitions": "one" }),
    ] {
        let (status, response_body) = send(&router, json_request("POST", "/v1/topics", body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(response_body["error"]["code"], "INVALID_REQUEST");
        assert_eq!(
            response_body["error"]["message"],
            "request JSON must include the required fields with valid types"
        );
        assert_eq!(response_body["error"]["details"], json!({}));
        assert_eq!(response_body["error"]["statusCode"], 400);
    }
}

#[tokio::test]
async fn json_content_type_is_required_for_create_topic() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);
    let request = Request::builder()
        .method("POST")
        .uri("/v1/topics")
        .body(Body::from(
            json!({ "name": "orders", "partitions": 1 }).to_string(),
        ))
        .unwrap();

    let (status, body) = send(&router, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "INVALID_REQUEST");
    assert_eq!(
        body["error"]["message"],
        "content-type must be application/json for this endpoint"
    );
    assert_eq!(body["error"]["statusCode"], 400);
}

#[tokio::test]
async fn unsupported_routes_and_methods_use_error_envelope() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    let (status, headers, body) = send_response(&router, empty_request("GET", "/v1/missing")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_json_content_type(&headers);
    assert_eq!(
        body,
        json!({
            "error": {
                "code": "NOT_FOUND",
                "message": "route not found",
                "details": {},
                "statusCode": 404
            }
        })
    );

    let (status, headers, body) = send_response(&router, empty_request("POST", "/health")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_json_content_type(&headers);
    assert_eq!(
        body,
        json!({
            "error": {
                "code": "METHOD_NOT_ALLOWED",
                "message": "method not allowed",
                "details": {},
                "statusCode": 405
            }
        })
    );
}

#[tokio::test]
async fn internal_storage_errors_are_sanitized() {
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);
    std::fs::create_dir_all(root.path().join("messages/topics")).unwrap();
    std::fs::write(
        root.path().join("messages/topics/orders"),
        b"not a directory",
    )
    .unwrap();

    let (status, body) = send(
        &router,
        json_request(
            "POST",
            "/v1/topics",
            json!({ "name": "orders", "partitions": 1 }),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        body,
        json!({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "internal server error",
                "details": {},
                "statusCode": 500
            }
        })
    );
}
