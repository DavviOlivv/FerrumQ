use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::JsonRejection},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use msg_broker::{
    BrokerConfig, BrokerError, CreateTopicCommand, DlqQuery, DurableBroker, DurableBrokerConfig,
    DurableBrokerError,
};
use msg_core::{DeadLetterReason, DomainError, Topic, TopicConfig, TopicName};
use msg_observability::{PROMETHEUS_CONTENT_TYPE, metrics};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tracing::{info, warn};

const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Configuration for the local control-plane HTTP API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlApiConfig {
    pub data_dir: PathBuf,
}

impl ControlApiConfig {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }
}

/// Shared Axum application state.
#[derive(Debug, Clone)]
pub struct AppState {
    broker: Arc<Mutex<DurableBroker>>,
}

impl AppState {
    #[must_use]
    pub fn new(broker: DurableBroker) -> Self {
        Self {
            broker: Arc::new(Mutex::new(broker)),
        }
    }

    #[must_use]
    pub fn broker(&self) -> Arc<Mutex<DurableBroker>> {
        Arc::clone(&self.broker)
    }
}

/// Errors raised while opening control API state.
#[derive(Debug, Error)]
pub enum ControlApiError {
    #[error("failed to open durable broker state")]
    OpenState(#[source] DurableBrokerError),
}

/// Opens the durable broker backing state for the control API.
pub fn open_state(config: ControlApiConfig) -> Result<AppState, ControlApiError> {
    let broker = DurableBroker::open(DurableBrokerConfig::new(
        config.data_dir,
        BrokerConfig::default(),
        DEFAULT_MAX_SEGMENT_BYTES,
    ))
    .map_err(ControlApiError::OpenState)?;

    Ok(AppState::new(broker))
}

/// Builds the control-plane HTTP router.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_endpoint))
        .route("/ready", get(ready))
        .route("/v1/status", get(status))
        .route("/v1/topics", post(create_topic).get(list_topics))
        .route("/v1/topics/{topicName}", get(get_topic))
        .route("/v1/dlq", get(list_dlq))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state)
}

#[tracing::instrument(name = "control_api.health", skip_all)]
async fn health() -> Json<StatusResponse> {
    record_http_success("GET", "/health", StatusCode::OK, "health");
    Json(StatusResponse { status: "ok" })
}

#[tracing::instrument(name = "control_api.metrics", skip_all)]
async fn metrics_endpoint() -> Response {
    record_http_success("GET", "/metrics", StatusCode::OK, "metrics");
    (
        [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
        metrics::render_prometheus(),
    )
        .into_response()
}

#[tracing::instrument(name = "control_api.ready", skip_all)]
async fn ready(State(state): State<AppState>) -> Result<Json<StatusResponse>, ApiError> {
    let result = with_broker(&state, |broker| {
        let _status = broker.status();
        Ok(Json(StatusResponse { status: "ready" }))
    })
    .map_err(|error| match error {
        ApiError::BrokerUnavailable(_) | ApiError::Internal => {
            ApiError::broker_unavailable("durable broker state is not accessible")
        }
        other => other,
    });
    observe_http_result("GET", "/ready", StatusCode::OK, "ready", result)
}

#[tracing::instrument(name = "control_api.status", skip_all)]
async fn status(State(state): State<AppState>) -> Result<Json<BrokerStatusResponse>, ApiError> {
    let result = with_broker(&state, |broker| {
        let status = broker.status();
        Ok(Json(BrokerStatusResponse {
            mode: status.mode(),
            data_dir: status.root_dir().to_string_lossy().into_owned(),
            topics: status.topic_count(),
            dlq_entries: status.dlq_count(),
        }))
    });
    observe_http_result("GET", "/v1/status", StatusCode::OK, "status", result)
}

#[tracing::instrument(name = "control_api.create_topic", skip_all)]
async fn create_topic(
    State(state): State<AppState>,
    request: Result<Json<CreateTopicRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<TopicResponse>), ApiError> {
    let result = create_topic_inner(&state, request);
    metrics::record_control_topic_create(if result.is_ok() { "success" } else { "error" });
    observe_http_result(
        "POST",
        "/v1/topics",
        StatusCode::CREATED,
        "create_topic",
        result,
    )
}

fn create_topic_inner(
    state: &AppState,
    request: Result<Json<CreateTopicRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<TopicResponse>), ApiError> {
    let request = request.map_err(ApiError::from_json_rejection)?.0;
    let topic_name = parse_topic_name(&request.name)?;
    let topic_config = TopicConfig::new(request.partitions).map_err(ApiError::from_domain)?;

    with_broker(state, |broker| {
        broker
            .create_topic(CreateTopicCommand::new(topic_name, topic_config))
            .map(topic_response)
            .map(|topic| (StatusCode::CREATED, Json(topic)))
            .map_err(ApiError::from_durable)
    })
}

#[tracing::instrument(name = "control_api.list_topics", skip_all)]
async fn list_topics(State(state): State<AppState>) -> Result<Json<TopicListResponse>, ApiError> {
    let result = with_broker(&state, |broker| {
        Ok(Json(TopicListResponse {
            items: broker
                .list_topics()
                .into_iter()
                .map(topic_response)
                .collect(),
        }))
    });
    observe_http_result("GET", "/v1/topics", StatusCode::OK, "list_topics", result)
}

#[tracing::instrument(name = "control_api.get_topic", skip_all)]
async fn get_topic(
    State(state): State<AppState>,
    Path(topic_name): Path<String>,
) -> Result<Json<TopicResponse>, ApiError> {
    let result = get_topic_inner(&state, &topic_name);
    observe_http_result(
        "GET",
        "/v1/topics/{topicName}",
        StatusCode::OK,
        "get_topic",
        result,
    )
}

fn get_topic_inner(state: &AppState, topic_name: &str) -> Result<Json<TopicResponse>, ApiError> {
    let topic_name = parse_topic_name(topic_name)?;

    with_broker(state, |broker| {
        broker
            .get_topic(&topic_name)
            .map(topic_response)
            .map(Json)
            .map_err(ApiError::from_durable)
    })
}

#[tracing::instrument(name = "control_api.list_dlq", skip_all)]
async fn list_dlq(
    State(state): State<AppState>,
    Query(query): Query<DlqRequest>,
) -> Result<Json<DlqListResponse>, ApiError> {
    let result = list_dlq_inner(&state, query);
    observe_http_result("GET", "/v1/dlq", StatusCode::OK, "list_dlq", result)
}

fn list_dlq_inner(state: &AppState, query: DlqRequest) -> Result<Json<DlqListResponse>, ApiError> {
    let query = match query.topic {
        Some(topic) => DlqQuery::for_topic(parse_topic_name(&topic)?),
        None => DlqQuery::all(),
    };

    with_broker(state, |broker| {
        broker
            .list_dlq(query)
            .map(|entries| DlqListResponse {
                items: entries
                    .iter()
                    .map(|entry| DlqEntryResponse {
                        topic: entry.topic().as_str().to_owned(),
                        partition: entry.partition_id().value(),
                        offset: entry.offset().value(),
                        message_id: entry.message_id().as_str().to_owned(),
                        consumer_group_id: entry.consumer_group_id().as_str().to_owned(),
                        reason: dead_letter_reason(entry.reason()),
                        attempt_count: entry.attempt_count(),
                        timestamp: entry.timestamp().as_unix_millis(),
                    })
                    .collect(),
            })
            .map(Json)
            .map_err(ApiError::from_durable)
    })
}

fn observe_http_result<T>(
    method: &'static str,
    route: &'static str,
    success_status: StatusCode,
    operation: &'static str,
    result: Result<T, ApiError>,
) -> Result<T, ApiError> {
    match &result {
        Ok(_) => record_http_success(method, route, success_status, operation),
        Err(error) => record_http_error(method, route, operation, error),
    }
    result
}

fn record_http_success(
    method: &'static str,
    route: &'static str,
    status: StatusCode,
    operation: &'static str,
) {
    metrics::record_control_http_request(method, route, status.as_u16());
    info!(operation, method, route, status = status.as_u16());
}

fn record_http_error(
    method: &'static str,
    route: &'static str,
    operation: &'static str,
    error: &ApiError,
) {
    let status = error.status_code();
    metrics::record_control_http_request(method, route, status.as_u16());
    metrics::record_control_http_error(method, route, status.as_u16(), error.code());
    warn!(
        operation,
        method,
        route,
        status = status.as_u16(),
        code = error.code()
    );
}

fn with_broker<T>(
    state: &AppState,
    operation: impl FnOnce(&mut DurableBroker) -> Result<T, ApiError>,
) -> Result<T, ApiError> {
    let mut broker = state
        .broker
        .lock()
        .map_err(|_| ApiError::broker_unavailable("durable broker state is not accessible"))?;
    operation(&mut broker)
}

fn parse_topic_name(value: &str) -> Result<TopicName, ApiError> {
    TopicName::new(value).map_err(ApiError::from_domain)
}

fn topic_response(topic: Topic) -> TopicResponse {
    TopicResponse {
        name: topic.name().as_str().to_owned(),
        partitions: topic.partition_count(),
    }
}

fn dead_letter_reason(reason: &DeadLetterReason) -> String {
    match reason {
        DeadLetterReason::MaxAttemptsExceeded => "max_attempts_exceeded".to_owned(),
        DeadLetterReason::Expired => "expired".to_owned(),
        DeadLetterReason::Rejected => "rejected".to_owned(),
        DeadLetterReason::Poisoned => "poisoned".to_owned(),
        DeadLetterReason::Manual(reason) => reason.clone(),
    }
}

async fn not_found() -> ApiError {
    let error = ApiError::RouteNotFound("route not found".to_owned());
    record_http_error("UNKNOWN", "unmatched", "not_found", &error);
    error
}

async fn method_not_allowed() -> ApiError {
    let error = ApiError::MethodNotAllowed("method not allowed".to_owned());
    record_http_error(
        "UNKNOWN",
        "method_not_allowed",
        "method_not_allowed",
        &error,
    );
    error
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusResponse {
    status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct BrokerStatusResponse {
    mode: &'static str,
    data_dir: String,
    topics: usize,
    dlq_entries: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateTopicRequest {
    name: String,
    partitions: u32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct TopicResponse {
    name: String,
    partitions: u32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct TopicListResponse {
    items: Vec<TopicResponse>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DlqRequest {
    topic: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DlqListResponse {
    items: Vec<DlqEntryResponse>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DlqEntryResponse {
    topic: String,
    partition: u32,
    offset: u64,
    message_id: String,
    consumer_group_id: String,
    reason: String,
    attempt_count: u32,
    timestamp: u64,
}

#[derive(Debug, Clone)]
enum ApiError {
    InvalidRequest(String),
    ValidationError(String),
    TopicAlreadyExists(String),
    TopicNotFound(String),
    BrokerUnavailable(String),
    MethodNotAllowed(String),
    RouteNotFound(String),
    Internal,
}

impl ApiError {
    fn from_json_rejection(rejection: JsonRejection) -> Self {
        let message = match rejection {
            JsonRejection::JsonSyntaxError(_) => "request body must be valid JSON",
            JsonRejection::JsonDataError(_) => {
                "request JSON must include the required fields with valid types"
            }
            JsonRejection::MissingJsonContentType(_) => {
                "content-type must be application/json for this endpoint"
            }
            JsonRejection::BytesRejection(_) => "request body could not be read",
            _ => "request body is invalid for this endpoint",
        };

        Self::InvalidRequest(message.to_owned())
    }

    fn from_domain(error: DomainError) -> Self {
        Self::ValidationError(error.to_string())
    }

    fn from_durable(error: DurableBrokerError) -> Self {
        match error {
            DurableBrokerError::Broker(BrokerError::Domain(error)) => Self::from_domain(error),
            DurableBrokerError::Broker(BrokerError::TopicAlreadyExists { topic }) => {
                Self::TopicAlreadyExists(format!("topic already exists: {topic}"))
            }
            DurableBrokerError::Broker(BrokerError::TopicNotFound { topic }) => {
                Self::TopicNotFound(format!("topic not found: {topic}"))
            }
            DurableBrokerError::Broker(BrokerError::InvalidConfig { field, reason }) => {
                Self::ValidationError(format!("invalid broker config for {field}: {reason}"))
            }
            DurableBrokerError::Broker(BrokerError::DeliveryNotFound { .. })
            | DurableBrokerError::Broker(BrokerError::InvalidConsumer { .. })
            | DurableBrokerError::Storage(_)
            | DurableBrokerError::Io(_)
            | DurableBrokerError::Serde(_)
            | DurableBrokerError::StateCorruption { .. }
            | DurableBrokerError::Corruption { .. } => Self::Internal,
        }
    }

    fn broker_unavailable(message: impl Into<String>) -> Self {
        Self::BrokerUnavailable(message.into())
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Self::ValidationError(_) => StatusCode::BAD_REQUEST,
            Self::TopicAlreadyExists(_) => StatusCode::CONFLICT,
            Self::TopicNotFound(_) => StatusCode::NOT_FOUND,
            Self::BrokerUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::MethodNotAllowed(_) => StatusCode::METHOD_NOT_ALLOWED,
            Self::RouteNotFound(_) => StatusCode::NOT_FOUND,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) => "INVALID_REQUEST",
            Self::ValidationError(_) => "VALIDATION_ERROR",
            Self::TopicAlreadyExists(_) => "TOPIC_ALREADY_EXISTS",
            Self::TopicNotFound(_) => "TOPIC_NOT_FOUND",
            Self::BrokerUnavailable(_) => "BROKER_UNAVAILABLE",
            Self::MethodNotAllowed(_) => "METHOD_NOT_ALLOWED",
            Self::RouteNotFound(_) => "NOT_FOUND",
            Self::Internal => "INTERNAL_ERROR",
        }
    }

    fn message(&self) -> String {
        match self {
            Self::InvalidRequest(message)
            | Self::ValidationError(message)
            | Self::TopicAlreadyExists(message)
            | Self::TopicNotFound(message)
            | Self::BrokerUnavailable(message)
            | Self::MethodNotAllowed(message)
            | Self::RouteNotFound(message) => message.clone(),
            Self::Internal => "internal server error".to_owned(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = ErrorEnvelope {
            error: ErrorBody {
                code: self.code(),
                message: self.message(),
                details: json!({}),
                status_code: status.as_u16(),
            },
        };

        (status, Json(body)).into_response()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: String,
    details: Value,
    status_code: u16,
}

/// Returns this crate's package name.
pub fn crate_name() -> &'static str {
    "msg-control-api"
}

#[cfg(test)]
mod tests {
    use super::crate_name;

    #[test]
    fn reports_crate_name() {
        assert_eq!(crate_name(), "msg-control-api");
    }
}
