use std::{
    path::PathBuf,
    sync::{Arc as StdArc, Mutex},
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

#[cfg(feature = "postgres")]
use msg_postgres::{PostgresError, SearchQuery, SearchResult};

const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const SEARCH_DEFAULT_LIMIT: u32 = 20;
const SEARCH_MAX_LIMIT: u32 = 100;
pub(crate) const SEARCH_ROUTE: &str = "/v1/search/messages";

/// Storage-agnostic search interface for the control plane.
///
/// Implemented by `PostgresRepository` when the optional `postgres` feature
/// is enabled, and by a fake test impl in unit/integration tests.
///
/// The error type is a sanitized `String`. `PostgresError::Display` already
/// strips `#[source]` (database details, paths, payloads) and the projection
/// layer routes everything through `sanitize_error`, so the cross-boundary
/// string carries no secrets.
///
/// The future is `Send + 'static` and takes an `Arc<Self>` clone so it can
/// be spawned independently of the caller.
#[cfg(feature = "postgres")]
pub trait MessageSearch: Send + Sync + 'static {
    fn search(
        self: StdArc<Self>,
        query: SearchQuery,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<SearchResult>, String>> + Send>,
    >;
}

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
#[derive(Clone)]
pub struct AppState {
    broker: StdArc<Mutex<DurableBroker>>,
    #[cfg(feature = "postgres")]
    search: Option<StdArc<dyn MessageSearch>>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("AppState");
        debug.field("broker", &"Arc<Mutex<DurableBroker>>");
        #[cfg(feature = "postgres")]
        debug.field(
            "search",
            &self.search.as_ref().map(|_| "Arc<dyn MessageSearch>"),
        );
        debug.finish()
    }
}

impl AppState {
    /// Creates a state instance without search support.
    #[must_use]
    pub fn new(broker: DurableBroker) -> Self {
        Self {
            broker: StdArc::new(Mutex::new(broker)),
            #[cfg(feature = "postgres")]
            search: None,
        }
    }

    /// Creates a state instance with an optional search dependency.
    #[cfg(feature = "postgres")]
    #[must_use]
    pub fn with_search(broker: DurableBroker, search: Option<StdArc<dyn MessageSearch>>) -> Self {
        Self {
            broker: StdArc::new(Mutex::new(broker)),
            search,
        }
    }

    #[must_use]
    pub fn broker(&self) -> StdArc<Mutex<DurableBroker>> {
        StdArc::clone(&self.broker)
    }

    #[cfg(feature = "postgres")]
    #[must_use]
    pub fn search(&self) -> Option<&StdArc<dyn MessageSearch>> {
        self.search.as_ref()
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

/// Marker type used as a no-op search argument when the `postgres` feature
/// is disabled.
pub struct NoopSearchHandle;

impl NoopSearchHandle {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopSearchHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Opens the durable broker backing state with an optional search dependency.
#[cfg(feature = "postgres")]
pub fn open_state_with_search(
    config: ControlApiConfig,
    search: Option<StdArc<dyn MessageSearch>>,
) -> Result<AppState, ControlApiError> {
    let broker = DurableBroker::open(DurableBrokerConfig::new(
        config.data_dir,
        BrokerConfig::default(),
        DEFAULT_MAX_SEGMENT_BYTES,
    ))
    .map_err(ControlApiError::OpenState)?;

    Ok(AppState::with_search(broker, search))
}

/// Opens the durable broker backing state without search support.
#[cfg(not(feature = "postgres"))]
pub fn open_state_with_search(
    config: ControlApiConfig,
    _search: NoopSearchHandle,
) -> Result<AppState, ControlApiError> {
    open_state(config)
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
        .route(SEARCH_ROUTE, post(search_messages))
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

#[tracing::instrument(name = "control_api.search_messages", skip_all)]
async fn search_messages(
    State(state): State<AppState>,
    request: Result<Json<SearchMessagesRequest>, JsonRejection>,
) -> Result<Json<SearchMessagesResponse>, ApiError> {
    let result = search_messages_inner(&state, request).await;
    observe_http_result(
        "POST",
        SEARCH_ROUTE,
        StatusCode::OK,
        "search_messages",
        result,
    )
}

async fn search_messages_inner(
    state: &AppState,
    request: Result<Json<SearchMessagesRequest>, JsonRejection>,
) -> Result<Json<SearchMessagesResponse>, ApiError> {
    let request = request.map_err(ApiError::from_json_rejection)?.0;

    let topic_filter_present = request.topic.is_some();
    let limit = request.limit.unwrap_or(SEARCH_DEFAULT_LIMIT);
    let query = request.query;

    log_search_event(
        "search_request",
        0,
        limit,
        topic_filter_present,
        postgres_configured_in_state(state),
    );

    validate_search_query_text(&query)?;
    if !(1..=SEARCH_MAX_LIMIT).contains(&limit) {
        return Err(ApiError::ValidationError(format!(
            "search limit must be between 1 and {SEARCH_MAX_LIMIT}"
        )));
    }
    if let Some(topic_name) = request.topic.as_deref() {
        parse_topic_name(topic_name)?;
    }

    #[cfg(feature = "postgres")]
    {
        if state.search().is_none() {
            log_search_event("search_unavailable", 0, limit, topic_filter_present, false);
            return Err(ApiError::search_unavailable("search is not configured"));
        }

        let search_query = match SearchQuery::new(query, request.topic, limit) {
            Ok(q) => q,
            Err(PostgresError::EmptySearchQuery) => {
                return Err(ApiError::ValidationError(
                    "search query must contain at least one alphanumeric character".to_owned(),
                ));
            }
            Err(PostgresError::InvalidSearchLimit) => {
                return Err(ApiError::ValidationError(format!(
                    "search limit must be between 1 and {SEARCH_MAX_LIMIT}"
                )));
            }
            Err(other) => {
                let message = sanitize_postgres_message(&other.to_string());
                return Err(ApiError::search_unavailable(message));
            }
        };

        let search = state.search().expect("checked Some above");
        let results = match StdArc::clone(search).search(search_query).await {
            Ok(rows) => rows,
            Err(message) => {
                log_search_event(
                    "search_backend_failed",
                    0,
                    limit,
                    topic_filter_present,
                    true,
                );
                return Err(ApiError::search_unavailable(sanitize_postgres_message(
                    &message,
                )));
            }
        };

        let items: Vec<SearchMessageItem> = results
            .iter()
            .map(|row| SearchMessageItem {
                topic: row.topic.clone(),
                partition_id: row.partition_id,
                offset: row.offset.to_string(),
                message_id: row.message_id.clone(),
                event_type: row.event_type.clone(),
                source: row.source.clone(),
                subject: row.subject.clone(),
                content_type: row.content_type.clone(),
                time_unix_ms: row.time_unix_ms.to_string(),
                payload_len: row.payload_len,
                payload_sha256: row.payload_sha256.clone(),
                rank: row.rank,
            })
            .collect();

        log_search_event(
            "search_completed",
            items.len(),
            limit,
            topic_filter_present,
            true,
        );

        Ok(Json(SearchMessagesResponse { items }))
    }

    #[cfg(not(feature = "postgres"))]
    {
        log_search_event("search_unavailable", 0, limit, topic_filter_present, false);
        Err(ApiError::search_unavailable("search is not configured"))
    }
}

#[cfg(feature = "postgres")]
fn postgres_configured_in_state(state: &AppState) -> bool {
    state.search().is_some()
}

#[cfg(not(feature = "postgres"))]
fn postgres_configured_in_state(_state: &AppState) -> bool {
    false
}

fn log_search_event(
    outcome: &'static str,
    result_count: usize,
    limit: u32,
    topic_filter_present: bool,
    postgres_configured: bool,
) {
    info!(
        operation = "search_messages",
        method = "POST",
        route = SEARCH_ROUTE,
        outcome,
        result_count,
        limit,
        topic_filter_present,
        postgres_configured,
    );
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

fn validate_search_query_text(query: &str) -> Result<(), ApiError> {
    let trimmed = query.trim();
    if trimmed.is_empty() || !trimmed.chars().any(|c| c.is_alphanumeric()) {
        return Err(ApiError::ValidationError(
            "search query must contain at least one alphanumeric character".to_owned(),
        ));
    }
    Ok(())
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchMessagesRequest {
    query: String,
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchMessagesResponse {
    items: Vec<SearchMessageItem>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchMessageItem {
    topic: String,
    partition_id: i32,
    offset: String,
    message_id: String,
    event_type: String,
    source: String,
    subject: Option<String>,
    content_type: String,
    time_unix_ms: String,
    payload_len: i64,
    payload_sha256: String,
    rank: f32,
}

#[derive(Debug, Clone)]
enum ApiError {
    InvalidRequest(String),
    ValidationError(String),
    TopicAlreadyExists(String),
    TopicNotFound(String),
    BrokerUnavailable(String),
    SearchUnavailable(String),
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
            | DurableBrokerError::Broker(BrokerError::IdempotencyKeyConflict { .. })
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

    fn search_unavailable(message: impl Into<String>) -> Self {
        Self::SearchUnavailable(message.into())
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Self::ValidationError(_) => StatusCode::BAD_REQUEST,
            Self::TopicAlreadyExists(_) => StatusCode::CONFLICT,
            Self::TopicNotFound(_) => StatusCode::NOT_FOUND,
            Self::BrokerUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::SearchUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
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
            Self::SearchUnavailable(_) => "SEARCH_UNAVAILABLE",
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
            | Self::SearchUnavailable(message)
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

#[cfg(feature = "postgres")]
fn sanitize_postgres_message(message: &str) -> String {
    if message.is_empty() {
        "search backend is unavailable".to_owned()
    } else {
        message.to_owned()
    }
}

/// Returns this crate's package name.
pub fn crate_name() -> &'static str {
    "msg-control-api"
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{crate_name, log_search_event};

    #[test]
    fn reports_crate_name() {
        assert_eq!(crate_name(), "msg-control-api");
    }

    #[test]
    fn search_log_event_includes_only_sanitized_fields() {
        struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for CaptureWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CaptureWriter {
            type Writer = Self;

            fn make_writer(&'writer self) -> Self::Writer {
                Self(Arc::clone(&self.0))
            }
        }

        const SENTINEL_QUERY: &str = "super-secret-token-9f8a7b6c5d4e3f2a1b0c";
        const SENTINEL_TOPIC: &str = "sensitive-customer-topic-9f8a7b6c";

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = CaptureWriter(Arc::clone(&buffer));
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

        tracing::subscriber::with_default(subscriber, || {
            log_search_event("search_completed", 1, 5, true, true);
        });

        let output = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        assert!(
            output.contains("search_messages"),
            "log should mention operation: {output}"
        );
        assert!(
            output.contains("topic_filter_present"),
            "log should mention only topic filter presence: {output}"
        );
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

#[cfg(feature = "postgres")]
mod postgres_adapter {
    use std::sync::Arc as StdArc;

    use msg_postgres::{PostgresError, PostgresRepository, SearchQuery, SearchResult};

    use crate::MessageSearch;

    /// Adapter implementing `MessageSearch` for `PostgresRepository`.
    ///
    /// All error paths return a sanitized `String`. `PostgresError::Display`
    /// already strips `#[source]` so database details, paths, and payload
    /// fragments do not leak across the hexagonal boundary.
    impl MessageSearch for PostgresRepository {
        fn search(
            self: StdArc<Self>,
            query: SearchQuery,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Vec<SearchResult>, String>> + Send>,
        > {
            Box::pin(async move {
                self.search_messages(&query)
                    .await
                    .map_err(|error| sanitize_postgres_error(&error))
            })
        }
    }

    fn sanitize_postgres_error(error: &PostgresError) -> String {
        let message = error.to_string();
        if message.is_empty() {
            "search backend is unavailable".to_owned()
        } else {
            message
        }
    }
}
