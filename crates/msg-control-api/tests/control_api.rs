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
use std::sync::OnceLock;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tower::ServiceExt;

#[cfg(feature = "postgres")]
use std::sync::Arc as StdArc;

#[cfg(feature = "postgres")]
use msg_control_api::{MessageSearch, open_state_with_search};

#[cfg(feature = "postgres")]
use msg_postgres::{SearchQuery, SearchResult as PgSearchResult};

const ALL_METRIC_NAMES: &[&str] = &[
    metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
    metric_names::CONTROL_HTTP_ERRORS_TOTAL,
    metric_names::CONTROL_TOPICS_CREATED_TOTAL,
    metric_names::DATA_RPC_REQUESTS_TOTAL,
    metric_names::DATA_RPC_ERRORS_TOTAL,
    metric_names::DATA_PUBLISHES_TOTAL,
    metric_names::DATA_CONSUMES_TOTAL,
    metric_names::DATA_MESSAGES_DELIVERED_TOTAL,
    metric_names::DATA_ACKS_TOTAL,
    metric_names::DATA_NACKS_TOTAL,
    metric_names::BROKER_OPENS_TOTAL,
    metric_names::BROKER_RECOVERIES_TOTAL,
    metric_names::BROKER_TOPICS_CREATED_TOTAL,
    metric_names::BROKER_MESSAGES_PUBLISHED_TOTAL,
    metric_names::BROKER_PUBLISH_DEDUPLICATED_TOTAL,
    metric_names::BROKER_PUBLISH_IDEMPOTENCY_CONFLICTS_TOTAL,
    metric_names::BROKER_CONSUMES_TOTAL,
    metric_names::BROKER_DELIVERIES_CREATED_TOTAL,
    metric_names::BROKER_ACKS_TOTAL,
    metric_names::BROKER_NACKS_TOTAL,
    metric_names::BROKER_RETRY_MAINTENANCE_TOTAL,
    metric_names::BROKER_DLQ_TRANSITIONS_TOTAL,
    metric_names::STORAGE_PARTITION_LOG_OPENS_TOTAL,
    metric_names::STORAGE_PARTITION_LOG_RECOVERIES_TOTAL,
    metric_names::STORAGE_APPENDS_TOTAL,
    metric_names::STORAGE_TRAILING_REPAIRS_TOTAL,
    metric_names::STORAGE_ERRORS_TOTAL,
];

const PRIVATE_METRIC_STRINGS: &[&str] = &[
    r#"{"ok":true}"#,
    "payload",
    "idem-1",
    "message-1",
    "delivery-1",
    "consumer-1",
    "group.1",
    "secret",
    "token",
    "password",
];

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

async fn metrics_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
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
    let _guard = metrics_test_guard().await;
    metrics::reset_for_tests();
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
    for name in ALL_METRIC_NAMES {
        assert!(body.contains(&format!("# HELP {name} ")));
        assert!(body.contains(&format!("# TYPE {name} counter")));
    }
    assert!(body.contains(
        "ferrumq_control_http_requests_total{method=\"GET\",route=\"/metrics\",status=\"200\"} 1"
    ));
}

#[tokio::test]
async fn repeated_metrics_scrapes_only_increment_scrape_counter() {
    let _guard = metrics_test_guard().await;
    metrics::reset_for_tests();
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);

    let (_status, _headers, first) = send_raw(&router, empty_request("GET", "/metrics")).await;
    let first_count = metrics::counter_value(
        metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
        &[("method", "GET"), ("route", "/metrics"), ("status", "200")],
    );
    let (_status, _headers, second) = send_raw(&router, empty_request("GET", "/metrics")).await;
    let second_count = metrics::counter_value(
        metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
        &[("method", "GET"), ("route", "/metrics"), ("status", "200")],
    );

    assert_eq!(first_count, 1);
    assert_eq!(second_count, 2);
    let first = String::from_utf8(first).unwrap();
    let second = String::from_utf8(second).unwrap();
    assert_eq!(first.matches("# HELP ").count(), ALL_METRIC_NAMES.len());
    assert_eq!(second.matches("# HELP ").count(), ALL_METRIC_NAMES.len());
    assert!(first.contains("route=\"/metrics\",status=\"200\"} 1"));
    assert!(second.contains("route=\"/metrics\",status=\"200\"} 2"));
}

#[tokio::test]
async fn topic_creation_and_errors_update_control_metrics() {
    let _guard = metrics_test_guard().await;
    metrics::reset_for_tests();
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
    let _guard = metrics_test_guard().await;
    metrics::reset_for_tests();
    let root = TempDir::new().unwrap();
    let router = seed_dlq_router(&root);

    let (status, _headers, body) = send_raw(&router, empty_request("GET", "/metrics")).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    for private in PRIVATE_METRIC_STRINGS {
        assert!(
            !body.contains(private),
            "metrics output leaked private string {private:?}"
        );
    }
    assert!(!body.contains("topic=\"orders\""));
    assert!(!body.contains("consumer_id="));
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
async fn health_ready_status_and_unsupported_routes_update_http_metrics() {
    let _guard = metrics_test_guard().await;
    metrics::reset_for_tests();
    let root = TempDir::new().unwrap();
    let router = router_with_temp_state(&root);
    let health_before = metrics::counter_value(
        metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
        &[("method", "GET"), ("route", "/health"), ("status", "200")],
    );
    let ready_before = metrics::counter_value(
        metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
        &[("method", "GET"), ("route", "/ready"), ("status", "200")],
    );
    let status_before = metrics::counter_value(
        metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
        &[
            ("method", "GET"),
            ("route", "/v1/status"),
            ("status", "200"),
        ],
    );
    let not_found_before = metrics::counter_value(
        metric_names::CONTROL_HTTP_ERRORS_TOTAL,
        &[
            ("method", "UNKNOWN"),
            ("route", "unmatched"),
            ("status", "404"),
            ("code", "NOT_FOUND"),
        ],
    );
    let method_before = metrics::counter_value(
        metric_names::CONTROL_HTTP_ERRORS_TOTAL,
        &[
            ("method", "UNKNOWN"),
            ("route", "method_not_allowed"),
            ("status", "405"),
            ("code", "METHOD_NOT_ALLOWED"),
        ],
    );

    assert_eq!(
        send(&router, empty_request("GET", "/health")).await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(&router, empty_request("GET", "/ready")).await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(&router, empty_request("GET", "/v1/status")).await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(&router, empty_request("GET", "/v1/missing")).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(&router, empty_request("POST", "/health")).await.0,
        StatusCode::METHOD_NOT_ALLOWED
    );

    assert!(
        metrics::counter_value(
            metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
            &[("method", "GET"), ("route", "/health"), ("status", "200")]
        ) > health_before
    );
    assert!(
        metrics::counter_value(
            metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
            &[("method", "GET"), ("route", "/ready"), ("status", "200")]
        ) > ready_before
    );
    assert!(
        metrics::counter_value(
            metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
            &[
                ("method", "GET"),
                ("route", "/v1/status"),
                ("status", "200")
            ]
        ) > status_before
    );
    assert!(
        metrics::counter_value(
            metric_names::CONTROL_HTTP_ERRORS_TOTAL,
            &[
                ("method", "UNKNOWN"),
                ("route", "unmatched"),
                ("status", "404"),
                ("code", "NOT_FOUND"),
            ],
        ) > not_found_before
    );
    assert!(
        metrics::counter_value(
            metric_names::CONTROL_HTTP_ERRORS_TOTAL,
            &[
                ("method", "UNKNOWN"),
                ("route", "method_not_allowed"),
                ("status", "405"),
                ("code", "METHOD_NOT_ALLOWED"),
            ],
        ) > method_before
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

#[cfg(feature = "postgres")]
mod search_endpoint_tests {
    use super::*;

    /// Fake search backend for control API tests. Returns a deterministic
    /// payload that exercises the decimal-string and field-masking contract.
    struct FakeSearch {
        rows: Vec<PgSearchResult>,
        failure: Option<String>,
    }

    impl FakeSearch {
        fn with_rows(rows: Vec<PgSearchResult>) -> Self {
            Self {
                rows,
                failure: None,
            }
        }

        fn with_failure(message: &str) -> Self {
            Self {
                rows: Vec::new(),
                failure: Some(message.to_owned()),
            }
        }
    }

    impl MessageSearch for FakeSearch {
        fn search(
            self: StdArc<Self>,
            _query: SearchQuery,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Vec<PgSearchResult>, String>> + Send>,
        > {
            let failure = self.failure.clone();
            let rows = self.rows.clone();
            Box::pin(async move {
                if let Some(message) = failure {
                    return Err(message);
                }
                Ok(rows)
            })
        }
    }

    fn sample_row() -> PgSearchResult {
        PgSearchResult {
            topic: "orders".to_owned(),
            partition_id: 0,
            offset: 12,
            message_id: "msg-1".to_owned(),
            event_type: "order.created".to_owned(),
            source: "checkout-service".to_owned(),
            subject: Some("order-1".to_owned()),
            content_type: "application/json".to_owned(),
            time_unix_ms: 1_700_000_000_000,
            payload_len: 128,
            payload_sha256: "0".repeat(64),
            rank: 0.25,
        }
    }

    fn router_with_fake_search(
        root: &TempDir,
        search: Option<StdArc<dyn MessageSearch>>,
    ) -> Router {
        let state = open_state_with_search(ControlApiConfig::new(root.path()), search).unwrap();
        build_router(state)
    }

    #[tokio::test]
    async fn search_messages_returns_items_envelope_with_decimal_strings() {
        let root = TempDir::new().unwrap();
        let fake = StdArc::new(FakeSearch::with_rows(vec![sample_row()]));
        let router = router_with_fake_search(&root, Some(fake));

        let (status, body) = send(
            &router,
            json_request(
                "POST",
                "/v1/search/messages",
                json!({ "query": "order", "topic": "orders", "limit": 5 }),
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let items = body["items"].as_array().expect("items array");
        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item["topic"], "orders");
        assert_eq!(item["partitionId"], 0);
        assert_eq!(item["offset"], "12");
        assert_eq!(item["messageId"], "msg-1");
        assert_eq!(item["eventType"], "order.created");
        assert_eq!(item["source"], "checkout-service");
        assert_eq!(item["subject"], "order-1");
        assert_eq!(item["contentType"], "application/json");
        assert_eq!(item["timeUnixMs"], "1700000000000");
        assert_eq!(item["payloadLen"], 128);
        assert_eq!(item["payloadSha256"], "0".repeat(64));
        assert!(item["rank"].is_number());
        assert!(!item.as_object().unwrap().contains_key("idempotencyKey"));
        assert!(!item.as_object().unwrap().contains_key("partitionKey"));
        assert!(!item.as_object().unwrap().contains_key("headers"));
        assert!(!item.as_object().unwrap().contains_key("payload"));
        assert!(!body.as_object().unwrap().contains_key("query"));
    }

    #[tokio::test]
    async fn search_messages_default_limit_is_twenty() {
        let root = TempDir::new().unwrap();
        let rows: Vec<PgSearchResult> = (0..5)
            .map(|index| {
                let mut row = sample_row();
                row.message_id = format!("msg-{index}");
                row
            })
            .collect();
        let fake = StdArc::new(FakeSearch::with_rows(rows));
        let router = router_with_fake_search(&root, Some(fake));

        let (status, body) = send(
            &router,
            json_request("POST", "/v1/search/messages", json!({ "query": "order" })),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"].as_array().unwrap().len(), 5);
    }

    #[tokio::test]
    async fn search_messages_explicit_null_topic_is_accepted() {
        let root = TempDir::new().unwrap();
        let fake = StdArc::new(FakeSearch::with_rows(Vec::new()));
        let router = router_with_fake_search(&root, Some(fake));

        let (status, _body) = send(
            &router,
            json_request(
                "POST",
                "/v1/search/messages",
                json!({ "query": "order", "topic": null }),
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn search_messages_returns_unavailable_when_no_search_dependency() {
        let root = TempDir::new().unwrap();
        let router = router_with_fake_search(&root, None);

        let (status, body) = send(
            &router,
            json_request("POST", "/v1/search/messages", json!({ "query": "order" })),
        )
        .await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["code"], "SEARCH_UNAVAILABLE");
        assert_eq!(body["error"]["message"], "search is not configured");
        assert_eq!(body["error"]["statusCode"], 503);
        assert_eq!(body["error"]["details"], json!({}));
    }

    #[tokio::test]
    async fn search_messages_validates_before_availability_no_pg() {
        let root = TempDir::new().unwrap();
        let router = router_with_fake_search(&root, None);

        let (status, body) = send(
            &router,
            json_request(
                "POST",
                "/v1/search/messages",
                json!({ "query": "order", "limit": 0 }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
    }

    #[tokio::test]
    async fn search_messages_validates_topic_before_availability_no_pg() {
        let root = TempDir::new().unwrap();
        let router = router_with_fake_search(&root, None);

        let (status, body) = send(
            &router,
            json_request(
                "POST",
                "/v1/search/messages",
                json!({ "query": "order", "topic": "bad name" }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
    }

    #[tokio::test]
    async fn search_messages_validates_blank_query_before_availability_no_pg() {
        let root = TempDir::new().unwrap();
        let router = router_with_fake_search(&root, None);

        for query in ["", "   ", "...", "!!!"] {
            let (status, body) = send(
                &router,
                json_request("POST", "/v1/search/messages", json!({ "query": query })),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "query={query:?}");
            assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
        }
    }

    #[tokio::test]
    async fn search_messages_unavailable_after_validation_passes_no_pg() {
        let root = TempDir::new().unwrap();
        let router = router_with_fake_search(&root, None);

        let (status, body) = send(
            &router,
            json_request(
                "POST",
                "/v1/search/messages",
                json!({ "query": "order", "limit": 5 }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["code"], "SEARCH_UNAVAILABLE");
    }

    #[tokio::test]
    async fn search_messages_rejects_empty_query() {
        let root = TempDir::new().unwrap();
        let fake = StdArc::new(FakeSearch::with_rows(Vec::new()));
        let router = router_with_fake_search(&root, Some(fake));

        for query in ["", "   ", "...", "!!!"] {
            let (status, body) = send(
                &router,
                json_request("POST", "/v1/search/messages", json!({ "query": query })),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "query={query:?}");
            assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
            assert!(
                body["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("search query")
            );
        }
    }

    #[tokio::test]
    async fn search_messages_rejects_invalid_topic() {
        let root = TempDir::new().unwrap();
        let fake = StdArc::new(FakeSearch::with_rows(Vec::new()));
        let router = router_with_fake_search(&root, Some(fake));

        let (status, body) = send(
            &router,
            json_request(
                "POST",
                "/v1/search/messages",
                json!({ "query": "order", "topic": "bad name" }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
    }

    #[tokio::test]
    async fn search_messages_rejects_out_of_range_limit() {
        let root = TempDir::new().unwrap();
        let fake = StdArc::new(FakeSearch::with_rows(Vec::new()));
        let router = router_with_fake_search(&root, Some(fake));

        for limit in [0_u32, 101_u32] {
            let (status, body) = send(
                &router,
                json_request(
                    "POST",
                    "/v1/search/messages",
                    json!({ "query": "order", "limit": limit }),
                ),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "limit={limit}");
            assert_eq!(body["error"]["code"], "VALIDATION_ERROR");
            assert!(body["error"]["message"].as_str().unwrap().contains("limit"));
        }
    }

    #[tokio::test]
    async fn search_messages_sanitizes_backend_failure() {
        let root = TempDir::new().unwrap();
        let fake = StdArc::new(FakeSearch::with_failure("database query failed"));
        let router = router_with_fake_search(&root, Some(fake));

        let (status, body) = send(
            &router,
            json_request("POST", "/v1/search/messages", json!({ "query": "order" })),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["code"], "SEARCH_UNAVAILABLE");
        assert_eq!(body["error"]["message"], "database query failed");
    }

    #[tokio::test]
    async fn search_messages_rejects_unsupported_method() {
        let root = TempDir::new().unwrap();
        let router = router_with_fake_search(&root, None);

        let (status, body) = send(&router, empty_request("GET", "/v1/search/messages")).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(body["error"]["code"], "METHOD_NOT_ALLOWED");
    }

    #[tokio::test]
    async fn search_messages_rejects_missing_content_type() {
        let root = TempDir::new().unwrap();
        let router = router_with_fake_search(&root, None);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/search/messages")
            .body(Body::from(json!({ "query": "order" }).to_string()))
            .unwrap();

        let (status, body) = send(&router, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "INVALID_REQUEST");
    }

    #[tokio::test]
    async fn search_messages_rejects_malformed_json() {
        let root = TempDir::new().unwrap();
        let router = router_with_fake_search(&root, None);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/search/messages")
            .header("content-type", "application/json")
            .body(Body::from("{"))
            .unwrap();

        let (status, body) = send(&router, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "INVALID_REQUEST");
    }

    #[tokio::test]
    async fn search_messages_does_not_log_raw_query_or_topic() {
        use std::sync::{Arc as StdArc2, Mutex as StdMutex};
        use tracing::instrument::WithSubscriber;

        struct CaptureWriter(StdArc2<StdMutex<Vec<u8>>>);
        impl std::io::Write for CaptureWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
            type Writer = Self;
            fn make_writer(&'a self) -> Self::Writer {
                Self(self.0.clone())
            }
        }

        let buffer = StdArc2::new(StdMutex::new(Vec::new()));
        let writer = CaptureWriter(buffer.clone());
        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .json()
            .with_writer(writer)
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_level(false)
            .with_current_span(false)
            .with_span_list(false)
            .finish();

        const SENTINEL_QUERY: &str = "super-secret-token-9f8a7b6c5d4e3f2a1b0c";
        const SENTINEL_TOPIC: &str = "sensitive-customer-topic-9f8a7b6c";

        let root = TempDir::new().unwrap();
        let fake = StdArc::new(FakeSearch::with_rows(vec![sample_row()]));
        let router = router_with_fake_search(&root, Some(fake));

        // Attach the capture subscriber to this future on every poll. A
        // thread-local default around `.await` can miss events if the runtime
        // resumes the future on another worker thread.
        let _ = async {
            send(
                &router,
                json_request(
                    "POST",
                    "/v1/search/messages",
                    json!({ "query": SENTINEL_QUERY, "topic": SENTINEL_TOPIC, "limit": 5 }),
                ),
            )
            .await
        }
        .with_subscriber(subscriber)
        .await;

        let output = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        assert!(
            !output.contains(SENTINEL_QUERY),
            "raw search query leaked into log: {output}"
        );
        assert!(
            !output.contains(SENTINEL_TOPIC),
            "raw topic leaked into log: {output}"
        );
    }
}
