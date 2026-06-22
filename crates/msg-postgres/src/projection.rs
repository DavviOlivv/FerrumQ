use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use chrono::{DateTime, Utc};
use msg_broker::{BrokerConfig, DurableBroker, DurableBrokerConfig};
use msg_core::{Offset, PartitionId, Topic, TopicName};
use msg_storage::{LogConfig, PartitionLog, StoredMessageRecord};
use tracing::{info, info_span, warn};

use crate::{
    PostgresError,
    models::{ProjectionResult, TopicRow, record_to_message_row},
    repository::PostgresRepository,
};

const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Rebuilds the PostgreSQL metadata projection from local durable broker data.
///
/// `DurableBroker` recovery validates the complete broker-state log and makes
/// broker-state topic metadata authoritative. Each expected partition is then
/// scanned through `msg-storage` recovery. PostgreSQL remains an offline,
/// derived projection and is never involved in live broker operations.
pub async fn rebuild_projection(
    repo: &PostgresRepository,
    data_dir: &Path,
) -> Result<ProjectionResult, PostgresError> {
    let span = info_span!("projection.rebuild", operation = "rebuild");
    let _guard = span.enter();

    let run_id = repo.start_projection_run().await?;
    info!(run_id, "started projection rebuild");

    match rebuild_inner(repo, data_dir).await {
        Ok(result) => {
            repo.complete_projection_run(run_id, &result).await?;
            info!(
                run_id,
                topics = result.topics_count,
                messages = result.messages_count,
                "projection rebuild complete"
            );
            Ok(result)
        }
        Err(error) => {
            let original = sanitize_error(&error);
            if let Err(recording_error) = repo.fail_projection_run(run_id, &original).await {
                warn!(
                    run_id,
                    error = %sanitize_error(&recording_error),
                    "failed to record projection failure"
                );
            }
            warn!(run_id, error = %original, "projection rebuild failed");
            Err(PostgresError::ProjectionFailed(original))
        }
    }
}

async fn rebuild_inner(
    repo: &PostgresRepository,
    data_dir: &Path,
) -> Result<ProjectionResult, PostgresError> {
    let broker = DurableBroker::open(DurableBrokerConfig::new(
        data_dir,
        BrokerConfig::default(),
        DEFAULT_MAX_SEGMENT_BYTES,
    ))?;
    let topics = broker.list_topics();
    validate_message_layout(data_dir, &topics)?;

    let messages_root = data_dir.join("messages");
    let mut messages_count = 0usize;

    for topic in &topics {
        let mut first_seen_at: Option<DateTime<Utc>> = None;
        let mut last_seen_at: Option<DateTime<Utc>> = None;

        for partition_id in topic.partition_ids() {
            for record in read_partition_records(&messages_root, topic.name(), partition_id)? {
                let row = record_to_message_row(&record)?;
                let timestamp = DateTime::from_timestamp_millis(row.time_unix_ms).ok_or(
                    PostgresError::ProjectionValueOutOfRange {
                        field: "time_unix_ms",
                    },
                )?;
                first_seen_at = Some(first_seen_at.map_or(timestamp, |old| old.min(timestamp)));
                last_seen_at = Some(last_seen_at.map_or(timestamp, |old| old.max(timestamp)));
                repo.upsert_message(&row).await?;
                messages_count += 1;
            }
        }

        let partitions = i32::try_from(topic.partition_count()).map_err(|_| {
            PostgresError::ProjectionValueOutOfRange {
                field: "partitions",
            }
        })?;
        match (first_seen_at, last_seen_at) {
            (Some(first_seen_at), Some(last_seen_at)) => {
                repo.upsert_topic(&TopicRow {
                    name: topic.name().as_str().to_owned(),
                    partitions,
                    first_seen_at,
                    last_seen_at,
                })
                .await?;
            }
            (None, None) => {
                repo.upsert_empty_topic(topic.name().as_str(), partitions)
                    .await?;
            }
            _ => return Err(PostgresError::InvalidProjectionSource),
        }
    }

    Ok(ProjectionResult {
        topics_count: topics.len(),
        messages_count,
    })
}

fn validate_message_layout(data_dir: &Path, topics: &[Topic]) -> Result<(), PostgresError> {
    let messages_dir = data_dir.join("messages").join("topics");
    if !messages_dir.exists() {
        return Ok(());
    }

    let authoritative: BTreeMap<&str, &Topic> = topics
        .iter()
        .map(|topic| (topic.name().as_str(), topic))
        .collect();

    for entry in fs::read_dir(&messages_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            return Err(PostgresError::InvalidProjectionSource);
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| PostgresError::InvalidProjectionSource)?;
        TopicName::new(&name).map_err(|_| PostgresError::InvalidProjectionSource)?;
        let topic = authoritative
            .get(name.as_str())
            .ok_or(PostgresError::InvalidProjectionSource)?;
        validate_partition_layout(&entry.path(), topic)?;
    }

    Ok(())
}

fn validate_partition_layout(topic_dir: &Path, topic: &Topic) -> Result<(), PostgresError> {
    let partitions_dir = topic_dir.join("partitions");
    if !partitions_dir.exists() {
        return Ok(());
    }

    let expected: BTreeSet<u32> = topic.partition_ids().map(PartitionId::value).collect();
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(partitions_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            return Err(PostgresError::InvalidProjectionSource);
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| PostgresError::InvalidProjectionSource)?;
        let partition_id = name
            .parse::<u32>()
            .map_err(|_| PostgresError::InvalidProjectionSource)?;
        if name != partition_id.to_string()
            || !expected.contains(&partition_id)
            || !actual.insert(partition_id)
        {
            return Err(PostgresError::InvalidProjectionSource);
        }
    }

    if actual != expected {
        return Err(PostgresError::InvalidProjectionSource);
    }
    Ok(())
}

fn read_partition_records(
    messages_root: &Path,
    topic: &TopicName,
    partition_id: PartitionId,
) -> Result<Vec<StoredMessageRecord>, PostgresError> {
    let log = PartitionLog::open(
        LogConfig {
            root_dir: messages_root.to_path_buf(),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        },
        topic,
        partition_id,
    )?;
    let limit = usize::try_from(log.next_offset().value()).map_err(|_| {
        PostgresError::ProjectionValueOutOfRange {
            field: "message_offset",
        }
    })?;
    Ok(log.read_from(Offset::new(0), limit)?)
}

fn sanitize_error(error: &PostgresError) -> String {
    match error {
        PostgresError::MessageIdConflict { topic } => {
            format!("message_id conflict for topic '{topic}'")
        }
        PostgresError::BrokerRecovery(_) => "broker recovery failed".to_owned(),
        PostgresError::Storage(_) => "storage recovery failed".to_owned(),
        PostgresError::Io(_) => "projection source I/O failed".to_owned(),
        PostgresError::InvalidProjectionSource => "projection source layout is invalid".to_owned(),
        PostgresError::ConnectionFailed(_) => "database connection failed".to_owned(),
        PostgresError::MigrationFailed { .. } => "database migration failed".to_owned(),
        PostgresError::QueryFailed { operation, .. } => {
            format!("database query failed during {operation}")
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_source_and_database_errors() {
        let io_error = PostgresError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "/tmp/secret/events.jsonl",
        ));
        assert_eq!(sanitize_error(&io_error), "projection source I/O failed");

        let storage_error = PostgresError::Storage(msg_storage::StorageError::InvalidFormat {
            path: "/home/user/private.log".into(),
            reason: "payload secret".to_owned(),
        });
        assert_eq!(sanitize_error(&storage_error), "storage recovery failed");
    }

    #[test]
    fn sanitizes_message_id_conflict_without_message_id_or_location() {
        let error = PostgresError::MessageIdConflict {
            topic: "orders".to_owned(),
        };
        assert_eq!(
            sanitize_error(&error),
            "message_id conflict for topic 'orders'"
        );
    }
}
