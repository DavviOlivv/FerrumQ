use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use msg_broker::{
    AckCommand, BrokerConfig, BrokerError, ConsumeCommand, DurableBroker, DurableBrokerConfig,
    DurableBrokerError, NackCommand, PublishCommand,
};
use msg_core::{
    ConsumerGroupId, ConsumerId, ContentType, DeliveryId, EventSource, EventSubject, EventType,
    IdempotencyKey, MessageEnvelope, MessageId, MessagePayload, MessageTimestamp, PartitionKey,
    TopicName,
};
use msg_observability::metrics;
use msg_protocol::ferrumq::dataplane::v1::{
    AckRequest, AckResponse, ConsumeRequest, ConsumeResponse, ConsumedMessage, NackRequest,
    NackResponse, PublishRequest, PublishResponse, ferrum_q_data_plane_server::FerrumQDataPlane,
};
use thiserror::Error;
use tonic::{Code, Request, Response, Status};
use tracing::{info, warn};

pub use msg_protocol::ferrumq::dataplane::v1::ferrum_q_data_plane_server::FerrumQDataPlaneServer;

const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Configuration for the local gRPC data-plane API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPlaneConfig {
    pub data_dir: PathBuf,
}

impl DataPlaneConfig {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }
}

/// Errors raised while opening data-plane state.
#[derive(Debug, Error)]
pub enum DataPlaneError {
    #[error("failed to open durable broker state")]
    OpenState(#[source] DurableBrokerError),
}

/// gRPC adapter for the synchronous durable broker.
#[derive(Debug, Clone)]
pub struct DataPlaneService {
    broker: Arc<Mutex<DurableBroker>>,
}

impl DataPlaneService {
    #[must_use]
    pub fn new(broker: DurableBroker) -> Self {
        Self {
            broker: Arc::new(Mutex::new(broker)),
        }
    }

    #[must_use]
    pub fn from_shared(broker: Arc<Mutex<DurableBroker>>) -> Self {
        Self { broker }
    }

    #[must_use]
    pub fn broker(&self) -> Arc<Mutex<DurableBroker>> {
        Arc::clone(&self.broker)
    }

    fn with_broker<T>(
        &self,
        operation: impl FnOnce(&mut DurableBroker) -> Result<T, DurableBrokerError>,
    ) -> Result<T, Status> {
        let mut broker = self
            .broker
            .lock()
            .map_err(|_| Status::unavailable("durable broker state is not currently accessible"))?;
        operation(&mut broker).map_err(status_from_durable)
    }
}

/// Opens the durable broker backing state for the gRPC data-plane API.
pub fn open_service(config: DataPlaneConfig) -> Result<DataPlaneService, DataPlaneError> {
    let broker = DurableBroker::open(DurableBrokerConfig::new(
        config.data_dir,
        BrokerConfig::default(),
        DEFAULT_MAX_SEGMENT_BYTES,
    ))
    .map_err(DataPlaneError::OpenState)?;

    Ok(DataPlaneService::new(broker))
}

#[tonic::async_trait]
impl FerrumQDataPlane for DataPlaneService {
    #[tracing::instrument(name = "data_plane.publish", skip_all)]
    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let result = self.publish_inner(request.into_inner());
        match &result {
            Ok(response) => {
                metrics::record_data_rpc_request("Publish", "ok");
                metrics::record_data_publish("success");
                info!(
                    operation = "Publish",
                    status = "ok",
                    topic = response.topic.as_str(),
                    partition = response.partition,
                    offset = response.offset,
                    message_id = response.message_id.as_str(),
                    deduplicated = response.deduplicated
                );
            }
            Err(status) => {
                record_rpc_error("Publish", "publish", status);
                metrics::record_data_publish("error");
            }
        }

        result.map(Response::new)
    }

    #[tracing::instrument(name = "data_plane.consume", skip_all)]
    async fn consume(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<ConsumeResponse>, Status> {
        let result = self.consume_inner(request.into_inner());
        match &result {
            Ok(response) => {
                metrics::record_data_rpc_request("Consume", "ok");
                metrics::record_data_consume("success");
                metrics::record_data_messages_delivered(response.messages.len());
                info!(
                    operation = "Consume",
                    status = "ok",
                    delivered = response.messages.len()
                );
            }
            Err(status) => {
                record_rpc_error("Consume", "consume", status);
                metrics::record_data_consume("error");
            }
        }

        result.map(Response::new)
    }

    #[tracing::instrument(name = "data_plane.ack", skip_all)]
    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let result = self.ack_inner(request.into_inner());
        match &result {
            Ok(()) => {
                metrics::record_data_rpc_request("Ack", "ok");
                metrics::record_data_ack("success");
                info!(operation = "Ack", status = "ok");
            }
            Err(status) => {
                record_rpc_error("Ack", "ack", status);
                metrics::record_data_ack("error");
            }
        }

        result.map(|()| Response::new(AckResponse {}))
    }

    #[tracing::instrument(name = "data_plane.nack", skip_all)]
    async fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackResponse>, Status> {
        let result = self.nack_inner(request.into_inner());
        match &result {
            Ok(()) => {
                metrics::record_data_rpc_request("Nack", "ok");
                metrics::record_data_nack("success");
                info!(operation = "Nack", status = "ok");
            }
            Err(status) => {
                record_rpc_error("Nack", "nack", status);
                metrics::record_data_nack("error");
            }
        }

        result.map(|()| Response::new(NackResponse {}))
    }
}

impl DataPlaneService {
    #[tracing::instrument(name = "data_plane.publish", skip_all)]
    fn publish_inner(&self, request: PublishRequest) -> Result<PublishResponse, Status> {
        let topic = topic_name(&request.topic)?;
        let envelope = publish_envelope(request)?;

        let published =
            self.with_broker(|broker| broker.publish(PublishCommand::new(topic, envelope)))?;

        Ok(PublishResponse {
            topic: published.topic().as_str().to_owned(),
            partition: published.partition_id().value(),
            offset: published.offset().value(),
            message_id: published.message_id().as_str().to_owned(),
            deduplicated: published.deduplicated(),
        })
    }

    fn consume_inner(&self, request: ConsumeRequest) -> Result<ConsumeResponse, Status> {
        let topic = topic_name(&request.topic)?;
        let consumer_group_id = consumer_group_id(&request.consumer_group)?;
        let consumer_id = consumer_id(&request.consumer_id)?;
        let max_messages = max_messages(request.max_messages)?;
        let lease_ms = lease_ms(request.lease_ms)?;
        let now = MessageTimestamp::from_unix_millis(request.now_unix_ms);
        let command = ConsumeCommand::with_lease_millis(
            topic,
            consumer_group_id,
            consumer_id,
            max_messages,
            now,
            lease_ms,
        )
        .map_err(status_from_broker)?;

        let topic_for_check = command.topic().clone();
        let messages = self.with_broker(|broker| {
            broker.get_topic(&topic_for_check)?;
            broker.retry_ready(now)?;
            broker.consume(command)
        })?;

        Ok(ConsumeResponse {
            messages: messages.into_iter().map(consumed_message).collect(),
        })
    }

    fn ack_inner(&self, request: AckRequest) -> Result<(), Status> {
        let delivery_id = delivery_id(&request.delivery_id)?;
        let consumer_id = consumer_id(&request.consumer_id)?;
        let timestamp = current_unix_millis();

        self.with_broker(|broker| broker.ack(AckCommand::new(delivery_id, consumer_id, timestamp)))
    }

    fn nack_inner(&self, request: NackRequest) -> Result<(), Status> {
        let delivery_id = delivery_id(&request.delivery_id)?;
        let consumer_id = consumer_id(&request.consumer_id)?;
        let timestamp = current_unix_millis();
        let command = if request.reason.trim().is_empty() {
            NackCommand::new(delivery_id, consumer_id, timestamp)
        } else {
            NackCommand::with_reason(delivery_id, consumer_id, request.reason, timestamp)
        };

        self.with_broker(|broker| broker.nack(command))
    }
}

fn record_rpc_error(method: &'static str, operation: &'static str, status: &Status) {
    let code = grpc_code(status.code());
    metrics::record_data_rpc_request(method, code);
    metrics::record_data_rpc_error(method, code);
    warn!(operation, status = code);
}

fn grpc_code(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

fn publish_envelope(request: PublishRequest) -> Result<MessageEnvelope, Status> {
    let id = message_id(&request.message_id)?;
    let source = event_source(&request.source)?;
    let event_type = event_type(&request.r#type)?;
    let content_type = content_type(&request.content_type)?;
    let timestamp = MessageTimestamp::from_unix_millis(request.time_unix_ms);
    let payload = MessagePayload::from_bytes(request.payload);

    let mut builder =
        MessageEnvelope::builder(id, source, event_type, content_type, timestamp, payload);

    if let Some(subject) = optional_subject(&request.subject)? {
        builder = builder.subject(subject);
    }
    if let Some(key) = optional_partition_key(&request.key)? {
        builder = builder.partition_key(key);
    }
    if let Some(idempotency_key) = optional_idempotency_key(&request.idempotency_key)? {
        builder = builder.idempotency_key(idempotency_key);
    }

    Ok(builder.build())
}

fn consumed_message(message: msg_broker::ConsumedMessage) -> ConsumedMessage {
    let envelope = message.envelope();

    ConsumedMessage {
        delivery_id: message.delivery_id().as_str().to_owned(),
        topic: message.topic().as_str().to_owned(),
        partition: message.partition_id().value(),
        offset: message.offset().value(),
        message_id: envelope.id().as_str().to_owned(),
        key: envelope
            .partition_key()
            .map_or_else(String::new, |key| key.as_str().to_owned()),
        payload: envelope.payload().as_bytes().to_vec(),
        content_type: envelope.content_type().as_str().to_owned(),
        r#type: envelope.event_type().as_str().to_owned(),
        source: envelope.source().as_str().to_owned(),
        subject: envelope
            .subject()
            .map_or_else(String::new, |subject| subject.as_str().to_owned()),
        idempotency_key: envelope
            .idempotency_key()
            .map_or_else(String::new, |key| key.as_str().to_owned()),
        time_unix_ms: envelope.timestamp().as_unix_millis(),
        consumer_group: message.consumer_group_id().as_str().to_owned(),
        consumer_id: message.consumer_id().as_str().to_owned(),
        attempt_number: message.attempt_number(),
        delivered_at_unix_ms: message.delivered_at().as_unix_millis(),
        lease_expires_at_unix_ms: message.lease_expires_at().as_unix_millis(),
    }
}

fn topic_name(value: &str) -> Result<TopicName, Status> {
    TopicName::new(value).map_err(invalid_argument)
}

fn message_id(value: &str) -> Result<MessageId, Status> {
    MessageId::new(value).map_err(invalid_argument)
}

fn event_source(value: &str) -> Result<EventSource, Status> {
    EventSource::new(value).map_err(invalid_argument)
}

fn event_type(value: &str) -> Result<EventType, Status> {
    EventType::new(value).map_err(invalid_argument)
}

fn content_type(value: &str) -> Result<ContentType, Status> {
    ContentType::new(value).map_err(invalid_argument)
}

fn consumer_group_id(value: &str) -> Result<ConsumerGroupId, Status> {
    ConsumerGroupId::new(value).map_err(invalid_argument)
}

fn consumer_id(value: &str) -> Result<ConsumerId, Status> {
    ConsumerId::new(value).map_err(invalid_argument)
}

fn delivery_id(value: &str) -> Result<DeliveryId, Status> {
    DeliveryId::new(value).map_err(invalid_argument)
}

fn optional_partition_key(value: &str) -> Result<Option<PartitionKey>, Status> {
    optional(value, |value| PartitionKey::new(value))
}

fn optional_subject(value: &str) -> Result<Option<EventSubject>, Status> {
    optional(value, |value| EventSubject::new(value))
}

fn optional_idempotency_key(value: &str) -> Result<Option<IdempotencyKey>, Status> {
    optional(value, |value| IdempotencyKey::new(value))
}

fn optional<T>(
    value: &str,
    parse: impl FnOnce(&str) -> Result<T, msg_core::DomainError>,
) -> Result<Option<T>, Status> {
    if value.trim().is_empty() {
        Ok(None)
    } else {
        parse(value).map(Some).map_err(invalid_argument)
    }
}

fn max_messages(value: u32) -> Result<usize, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(
            "max_messages must be greater than zero",
        ));
    }

    Ok(value as usize)
}

fn lease_ms(value: u64) -> Result<u64, Status> {
    if value == 0 {
        return Err(Status::invalid_argument(
            "lease_ms must be greater than zero",
        ));
    }

    Ok(value)
}

fn invalid_argument(error: msg_core::DomainError) -> Status {
    Status::invalid_argument(error.to_string())
}

fn status_from_durable(error: DurableBrokerError) -> Status {
    match error {
        DurableBrokerError::Broker(error) => status_from_broker(error),
        DurableBrokerError::Storage(_)
        | DurableBrokerError::Io(_)
        | DurableBrokerError::Serde(_)
        | DurableBrokerError::StateCorruption { .. }
        | DurableBrokerError::Corruption { .. } => Status::internal("internal broker error"),
    }
}

fn status_from_broker(error: BrokerError) -> Status {
    match error {
        BrokerError::Domain(error) => Status::invalid_argument(error.to_string()),
        BrokerError::TopicAlreadyExists { .. } => Status::already_exists("topic already exists"),
        BrokerError::TopicNotFound { .. } => Status::not_found("topic not found"),
        BrokerError::DeliveryNotFound { .. } => Status::not_found("delivery not found"),
        BrokerError::InvalidConsumer { .. } => {
            Status::failed_precondition("invalid delivery ownership")
        }
        BrokerError::InvalidConfig { field, reason } => {
            Status::invalid_argument(format!("{field} {reason}"))
        }
        BrokerError::IdempotencyKeyConflict { .. } => {
            Status::already_exists("idempotency key conflict")
        }
    }
}

fn current_unix_millis() -> MessageTimestamp {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u128::from(u64::MAX)) as u64
        });
    MessageTimestamp::from_unix_millis(millis)
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use msg_storage::StorageError;
    use tempfile::TempDir;
    use tonic::Code;

    use super::*;

    #[test]
    fn durable_storage_and_corruption_errors_are_sanitized_internal_statuses() {
        let cases = [
            DurableBrokerError::Storage(StorageError::InvalidConfig {
                reason: "specific storage detail".to_owned(),
            }),
            DurableBrokerError::StateCorruption {
                path: PathBuf::from("/tmp/private/events.jsonl"),
                line: 17,
                reason: "malformed broker-state line".to_owned(),
            },
            DurableBrokerError::Corruption {
                reason: "duplicate recovered topic event".to_owned(),
            },
        ];

        for error in cases {
            let status = status_from_durable(error);
            assert_eq!(status.code(), Code::Internal);
            assert_eq!(status.message(), "internal broker error");
        }
    }

    #[test]
    fn poisoned_broker_state_maps_to_unavailable() {
        let root = TempDir::new().unwrap();
        let broker = DurableBroker::open(DurableBrokerConfig::new(
            root.path(),
            BrokerConfig::default(),
            DEFAULT_MAX_SEGMENT_BYTES,
        ))
        .unwrap();
        let shared = Arc::new(Mutex::new(broker));
        let poison_target = Arc::clone(&shared);

        let _panic = std::thread::spawn(move || {
            let _guard = poison_target.lock().unwrap();
            panic!("poison durable broker mutex");
        })
        .join();

        let service = DataPlaneService::from_shared(shared);
        let status = service
            .with_broker(|_| Ok::<_, DurableBrokerError>(()))
            .unwrap_err();

        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(
            status.message(),
            "durable broker state is not currently accessible"
        );
    }
}
