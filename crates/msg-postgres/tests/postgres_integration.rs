//! Real PostgreSQL coverage for migrations, repositories, and offline rebuilds.
//!
//! Set `FERRUMQ_POSTGRES_TEST_URL` to run these tests. Each test uses a unique
//! schema and removes it afterward.

use std::{
    env, fs,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    process,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use chrono::{DateTime, Utc};
use msg_broker::{
    BrokerConfig, CreateTopicCommand, DurableBroker, DurableBrokerConfig, PublishCommand,
};
use msg_core::{
    ContentType, EventSource, EventSubject, EventType, HeaderName, HeaderValue, IdempotencyKey,
    MessageEnvelope, MessageId, MessagePayload, MessageTimestamp, PartitionKey, TopicConfig,
    TopicName,
};
use msg_postgres::{
    PostgresConfig, PostgresError,
    migrations::run_migrations,
    models::{MessageRow, ProjectionResult, SearchQuery, TopicRow, compute_payload_sha256},
    projection::rebuild_projection,
    repository::PostgresRepository,
};
use sqlx::PgPool;
use tempfile::TempDir;
use url::Url;

type StoredMessageMetadata = (
    String,
    i32,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    i64,
    String,
    serde_json::Value,
);

fn test_database_url() -> Option<String> {
    env::var("FERRUMQ_POSTGRES_TEST_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn unique_schema(prefix: &str) -> String {
    static COUNTER: OnceLock<AtomicU64> = OnceLock::new();
    let counter = COUNTER.get_or_init(|| AtomicU64::new(0));
    format!(
        "{prefix}_{}_{}",
        process::id(),
        counter.fetch_add(1, Ordering::SeqCst)
    )
}

fn schema_url(base_url: &str, schema: &str) -> String {
    let mut url = Url::parse(base_url).expect("test URL must be a PostgreSQL URL");
    url.query_pairs_mut()
        .append_pair("options", &format!("--search_path={schema}"));
    url.to_string()
}

async fn create_schema(base_url: &str, schema: &str) {
    let config = PostgresConfig::from_url(Some(base_url.to_owned())).unwrap();
    let repo = PostgresRepository::connect(config).await.unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(repo.pool())
        .await
        .unwrap();
}

async fn scoped_repo(base_url: &str, schema: &str) -> PostgresRepository {
    let config = PostgresConfig::from_url(Some(schema_url(base_url, schema))).unwrap();
    PostgresRepository::connect(config).await.unwrap()
}

async fn migrated_repo(base_url: &str, schema: &str) -> PostgresRepository {
    create_schema(base_url, schema).await;
    let repo = scoped_repo(base_url, schema).await;
    run_migrations(repo.pool()).await.unwrap();
    repo
}

async fn drop_schema(base_url: &str, schema: &str) {
    let config = PostgresConfig::from_url(Some(base_url.to_owned())).unwrap();
    let repo = PostgresRepository::connect(config).await.unwrap();
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(repo.pool())
        .await
        .unwrap();
}

fn sample_message_row(topic: &str, message_id: &str) -> MessageRow {
    MessageRow {
        topic: topic.to_owned(),
        partition_id: 0,
        offset: 0,
        message_id: message_id.to_owned(),
        idempotency_key: Some("idem-1".to_owned()),
        partition_key: Some("account-1".to_owned()),
        payload_len: 4,
        payload_sha256: compute_payload_sha256(b"data"),
        content_type: "application/json".to_owned(),
        event_type: "order.created".to_owned(),
        source: "/tests".to_owned(),
        subject: Some("order/1".to_owned()),
        headers: serde_json::json!({"trace-id": "trace-1"}),
        time_unix_ms: 1_700_000_000_000,
    }
}

fn open_broker(root: &TempDir, max_segment_bytes: u64) -> DurableBroker {
    DurableBroker::open(DurableBrokerConfig::new(
        root.path(),
        BrokerConfig::default(),
        max_segment_bytes,
    ))
    .unwrap()
}

fn create_topic(broker: &mut DurableBroker, name: &str, partitions: u32) {
    broker
        .create_topic(CreateTopicCommand::new(
            TopicName::new(name).unwrap(),
            TopicConfig::new(partitions).unwrap(),
        ))
        .unwrap();
}

fn envelope(
    message_id: &str,
    timestamp: u64,
    payload: &[u8],
    partition_key: Option<&str>,
    idempotency_key: Option<&str>,
) -> MessageEnvelope {
    let mut builder = MessageEnvelope::builder(
        MessageId::new(message_id).unwrap(),
        EventSource::new("/postgres-tests").unwrap(),
        EventType::new("order.created").unwrap(),
        ContentType::new("application/json").unwrap(),
        MessageTimestamp::from_unix_millis(timestamp),
        MessagePayload::from_bytes(payload),
    )
    .subject(EventSubject::new("order/1").unwrap())
    .header(
        HeaderName::new("trace-id").unwrap(),
        HeaderValue::new("trace-1"),
    );
    if let Some(key) = partition_key {
        builder = builder.partition_key(PartitionKey::new(key).unwrap());
    }
    if let Some(key) = idempotency_key {
        builder = builder.idempotency_key(IdempotencyKey::new(key).unwrap());
    }
    builder.build()
}

fn publish(broker: &mut DurableBroker, topic: &str, envelope: MessageEnvelope) {
    broker
        .publish(PublishCommand::new(
            TopicName::new(topic).unwrap(),
            envelope,
        ))
        .unwrap();
}

async fn run_row(
    repo: &PostgresRepository,
    run_id: i64,
) -> (String, Option<DateTime<Utc>>, i32, i32, Option<String>) {
    sqlx::query_as(
        "SELECT status, completed_at, topics_count, messages_count, error_message
         FROM ferrumq_projection_runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_one(repo.pool())
    .await
    .unwrap()
}

async fn latest_run_id(repo: &PostgresRepository) -> i64 {
    sqlx::query_scalar("SELECT max(id) FROM ferrumq_projection_runs")
        .fetch_one(repo.pool())
        .await
        .unwrap()
}

fn flip_first_checksum_byte(segment: &Path) {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment)
        .unwrap();
    file.seek(SeekFrom::Start(4)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(4)).unwrap();
    file.write_all(&byte).unwrap();
    file.flush().unwrap();
}

#[tokio::test]
async fn migrations_are_serialized_repeatable_and_create_hardened_schema() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("migration");
    create_schema(&base_url, &schema).await;
    let url = schema_url(&base_url, &schema);
    let pool = PgPool::connect(&url).await.unwrap();

    let (left, right) = tokio::join!(run_migrations(&pool), run_migrations(&pool));
    left.unwrap();
    right.unwrap();
    run_migrations(&pool).await.unwrap();

    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = current_schema() ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        tables,
        vec![
            "_ferrumq_migrations",
            "ferrumq_messages",
            "ferrumq_projection_runs",
            "ferrumq_topics",
        ]
    );

    let migrations: Vec<(i32, String)> =
        sqlx::query_as("SELECT version, name FROM _ferrumq_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        migrations,
        vec![
            (1, "001_initial_schema".to_owned()),
            (2, "002_full_text_search".to_owned()),
        ]
    );

    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = current_schema()
           AND table_name = 'ferrumq_messages'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(columns.contains(&"message_offset".to_owned()));
    assert!(!columns.contains(&"offset".to_owned()));
    assert!(!columns.contains(&"payload".to_owned()));

    let constraints: Vec<String> = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid)
         FROM pg_constraint
         WHERE connamespace = current_schema()::regnamespace
         ORDER BY conname",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let constraints = constraints.join("\n");
    for expected in [
        "partitions > 0",
        "partition_id >= 0",
        "message_offset >= 0",
        "payload_len >= 0",
        "char_length(payload_sha256) = 64",
        "status = ANY",
        "completed_at IS NULL",
        "completed_at IS NOT NULL",
    ] {
        assert!(
            constraints.contains(expected),
            "missing constraint fragment {expected:?} in {constraints}"
        );
    }

    drop(pool);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn failed_migration_is_not_recorded_and_bad_metadata_is_rejected() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };

    let failed_schema = unique_schema("migration_fail");
    create_schema(&base_url, &failed_schema).await;
    let failed_repo = scoped_repo(&base_url, &failed_schema).await;
    sqlx::query("CREATE TABLE ferrumq_messages (topic TEXT NOT NULL)")
        .execute(failed_repo.pool())
        .await
        .unwrap();
    assert!(matches!(
        run_migrations(failed_repo.pool()).await,
        Err(PostgresError::MigrationFailed { .. })
    ));
    let tracking_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('_ferrumq_migrations')::text")
            .fetch_one(failed_repo.pool())
            .await
            .unwrap();
    assert!(
        tracking_table.is_none(),
        "failed migration transaction must not leave tracking metadata"
    );
    drop(failed_repo);
    drop_schema(&base_url, &failed_schema).await;

    let inconsistent_schema = unique_schema("migration_metadata");
    create_schema(&base_url, &inconsistent_schema).await;
    let inconsistent_repo = scoped_repo(&base_url, &inconsistent_schema).await;
    sqlx::query(
        "CREATE TABLE _ferrumq_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            name TEXT NOT NULL
         )",
    )
    .execute(inconsistent_repo.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO _ferrumq_migrations (version, name) VALUES (1, 'wrong_name')")
        .execute(inconsistent_repo.pool())
        .await
        .unwrap();
    assert!(matches!(
        run_migrations(inconsistent_repo.pool()).await,
        Err(PostgresError::InconsistentMigrationMetadata)
    ));
    drop(inconsistent_repo);
    drop_schema(&base_url, &inconsistent_schema).await;
}

#[tokio::test]
async fn repository_upserts_are_repeatable_typed_and_sanitized() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("repository");
    let repo = migrated_repo(&base_url, &schema).await;

    let first_seen_at = DateTime::from_timestamp_millis(1_000).unwrap();
    let last_seen_at = DateTime::from_timestamp_millis(3_000).unwrap();
    let topic = TopicRow {
        name: "orders".to_owned(),
        partitions: 2,
        first_seen_at,
        last_seen_at,
    };
    repo.upsert_topic(&topic).await.unwrap();
    repo.upsert_topic(&topic).await.unwrap();

    let mut row = sample_message_row("orders", "message-1");
    repo.upsert_message(&row).await.unwrap();
    repo.upsert_message(&row).await.unwrap();

    let stored: (
        i64,
        serde_json::Value,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT count(*) OVER (), headers, idempotency_key, partition_key, subject
             FROM ferrumq_messages
             WHERE topic = $1 AND partition_id = $2 AND message_offset = $3",
    )
    .bind(&row.topic)
    .bind(row.partition_id)
    .bind(row.offset)
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(stored.0, 1);
    assert_eq!(stored.1, serde_json::json!({"trace-id": "trace-1"}));
    assert_eq!(stored.2.as_deref(), Some("idem-1"));
    assert_eq!(stored.3.as_deref(), Some("account-1"));
    assert_eq!(stored.4.as_deref(), Some("order/1"));

    row.partition_id = 1;
    row.offset = 7;
    assert!(matches!(
        repo.upsert_message(&row).await,
        Err(PostgresError::MessageIdConflict { .. })
    ));

    let mut other_topic = sample_message_row("payments", "message-1");
    other_topic.idempotency_key = None;
    other_topic.partition_key = None;
    other_topic.subject = None;
    repo.upsert_message(&other_topic).await.unwrap();
    let isolated_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ferrumq_messages WHERE message_id = 'message-1'")
            .fetch_one(repo.pool())
            .await
            .unwrap();
    assert_eq!(isolated_count, 2);
    let absent_optionals: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT idempotency_key, partition_key, subject
         FROM ferrumq_messages WHERE topic = 'payments'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(absent_optionals, (None, None, None));

    let mut invalid = sample_message_row("orders", "message-secret");
    invalid.offset = 8;
    invalid.payload_sha256 = "payload-secret".to_owned();
    let error = repo.upsert_message(&invalid).await.unwrap_err();
    assert!(matches!(error, PostgresError::QueryFailed { .. }));
    let public = error.to_string();
    assert!(!public.contains("payload-secret"));
    assert!(!public.contains("message-secret"));

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn projection_run_updates_validate_targets_and_clear_stale_errors() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("runs");
    let repo = migrated_repo(&base_url, &schema).await;

    let run_id = repo.start_projection_run().await.unwrap();
    let started = run_row(&repo, run_id).await;
    assert_eq!(started.0, "in_progress");
    assert!(started.1.is_none());
    assert!(started.4.is_none());

    repo.fail_projection_run(run_id, "sanitized failure")
        .await
        .unwrap();
    let failed = run_row(&repo, run_id).await;
    assert_eq!(failed.0, "error");
    assert!(failed.1.is_some());
    assert_eq!(failed.4.as_deref(), Some("sanitized failure"));

    repo.complete_projection_run(
        run_id,
        &ProjectionResult {
            topics_count: 2,
            messages_count: 10,
        },
    )
    .await
    .unwrap();
    let succeeded = run_row(&repo, run_id).await;
    assert_eq!(succeeded.0, "success");
    assert_eq!((succeeded.2, succeeded.3), (2, 10));
    assert!(succeeded.4.is_none());

    assert!(matches!(
        repo.complete_projection_run(
            i64::MAX,
            &ProjectionResult {
                topics_count: 0,
                messages_count: 0,
            },
        )
        .await,
        Err(PostgresError::ProjectionRunNotFound { .. })
    ));
    assert!(matches!(
        repo.fail_projection_run(i64::MAX, "safe").await,
        Err(PostgresError::ProjectionRunNotFound { .. })
    ));

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn rebuild_twice_projects_authoritative_topics_and_all_message_metadata() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("rebuild");
    let repo = migrated_repo(&base_url, &schema).await;
    let data = TempDir::new().unwrap();

    {
        let mut broker = open_broker(&data, 1);
        create_topic(&mut broker, "empty-topic", 3);
        create_topic(&mut broker, "orders", 2);

        publish(
            &mut broker,
            "orders",
            envelope("unkeyed-1", 3_000, b"first", None, None),
        );
        publish(
            &mut broker,
            "orders",
            envelope(
                "keyed-original",
                1_000,
                b"second",
                Some("account-1"),
                Some("idem-1"),
            ),
        );
        let retry = broker
            .publish(PublishCommand::new(
                TopicName::new("orders").unwrap(),
                envelope(
                    "keyed-retry",
                    9_000,
                    b"second",
                    Some("account-1"),
                    Some("idem-1"),
                ),
            ))
            .unwrap();
        assert!(retry.deduplicated());
        publish(
            &mut broker,
            "orders",
            envelope("unkeyed-2", 2_000, b"third", None, None),
        );
    }
    fs::OpenOptions::new()
        .append(true)
        .open(data.path().join("broker-state/events.jsonl"))
        .unwrap()
        .write_all(b"{\"type\":\"topic_created\"")
        .unwrap();

    let segment_count = fs::read_dir(data.path().join("messages/topics/orders/partitions/0"))
        .unwrap()
        .count()
        + fs::read_dir(data.path().join("messages/topics/orders/partitions/1"))
            .unwrap()
            .count();
    assert!(
        segment_count >= 3,
        "small segment threshold should roll logs"
    );

    let first = rebuild_projection(&repo, data.path()).await.unwrap();
    assert_eq!((first.topics_count, first.messages_count), (2, 3));

    let topic_before: Vec<(String, i32, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT name, partitions, first_seen_at, last_seen_at
         FROM ferrumq_topics ORDER BY name",
    )
    .fetch_all(repo.pool())
    .await
    .unwrap();
    assert_eq!(topic_before.len(), 2);
    assert_eq!(topic_before[0].0, "empty-topic");
    assert_eq!(topic_before[0].1, 3);
    assert_eq!(topic_before[1].0, "orders");
    assert_eq!(topic_before[1].1, 2);
    assert_eq!(
        topic_before[1].2,
        DateTime::from_timestamp_millis(1_000).unwrap()
    );
    assert_eq!(
        topic_before[1].3,
        DateTime::from_timestamp_millis(3_000).unwrap()
    );

    let messages: Vec<StoredMessageMetadata> = sqlx::query_as(
        "SELECT message_id, partition_id, message_offset, idempotency_key,
                partition_key, subject, payload_len, payload_sha256, headers
         FROM ferrumq_messages ORDER BY message_id",
    )
    .fetch_all(repo.pool())
    .await
    .unwrap();
    assert_eq!(messages.len(), 3);
    assert!(!messages.iter().any(|row| row.0 == "keyed-retry"));
    let keyed = messages
        .iter()
        .find(|row| row.0 == "keyed-original")
        .unwrap();
    assert_eq!(keyed.3.as_deref(), Some("idem-1"));
    assert_eq!(keyed.4.as_deref(), Some("account-1"));
    assert_eq!(keyed.5.as_deref(), Some("order/1"));
    assert_eq!(keyed.6, 6);
    assert_eq!(keyed.7, compute_payload_sha256(b"second"));
    assert_eq!(keyed.8, serde_json::json!({"trace-id": "trace-1"}));
    let legacy = messages.iter().find(|row| row.0 == "unkeyed-1").unwrap();
    assert!(legacy.3.is_none());
    assert!(legacy.4.is_none());

    let second = rebuild_projection(&repo, data.path()).await.unwrap();
    assert_eq!((second.topics_count, second.messages_count), (2, 3));
    let topic_after: Vec<(String, i32, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT name, partitions, first_seen_at, last_seen_at
         FROM ferrumq_topics ORDER BY name",
    )
    .fetch_all(repo.pool())
    .await
    .unwrap();
    assert_eq!(topic_after, topic_before);

    let message_count: i64 = sqlx::query_scalar("SELECT count(*) FROM ferrumq_messages")
        .fetch_one(repo.pool())
        .await
        .unwrap();
    let successful_runs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ferrumq_projection_runs WHERE status = 'success'")
            .fetch_one(repo.pool())
            .await
            .unwrap();
    assert_eq!(message_count, 3);
    assert_eq!(successful_runs, 2);

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn malformed_broker_state_records_a_sanitized_terminal_failure() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("bad_state");
    let repo = migrated_repo(&base_url, &schema).await;
    let data = TempDir::new().unwrap();
    fs::create_dir_all(data.path().join("broker-state")).unwrap();
    fs::write(
        data.path().join("broker-state/events.jsonl"),
        b"{\"type\":\"topic_created\",\"password\":\"db-secret\",\"payload\":\"payload-secret\"}\n",
    )
    .unwrap();

    let error = rebuild_projection(&repo, data.path()).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "projection failed: broker recovery failed"
    );
    let run_id = latest_run_id(&repo).await;
    let run = run_row(&repo, run_id).await;
    assert_eq!(run.0, "error");
    assert!(run.1.is_some());
    assert_eq!(run.4.as_deref(), Some("broker recovery failed"));
    assert!(!run.4.unwrap().contains("secret"));
    let in_progress: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ferrumq_projection_runs WHERE status = 'in_progress'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(in_progress, 0);

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn storage_corruption_and_invalid_partition_layout_fail_safely() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };

    let corruption_schema = unique_schema("corruption");
    let corruption_repo = migrated_repo(&base_url, &corruption_schema).await;
    let corrupted_data = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&corrupted_data, 64 * 1024 * 1024);
        create_topic(&mut broker, "orders", 1);
        publish(
            &mut broker,
            "orders",
            envelope("message-1", 1_000, b"one", None, None),
        );
        publish(
            &mut broker,
            "orders",
            envelope("message-2", 2_000, b"two", None, None),
        );
    }
    flip_first_checksum_byte(
        &corrupted_data
            .path()
            .join("messages/topics/orders/partitions/0/00000000000000000000.log"),
    );
    let error = rebuild_projection(&corruption_repo, corrupted_data.path())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "projection failed: broker recovery failed"
    );
    let corruption_run = run_row(&corruption_repo, latest_run_id(&corruption_repo).await).await;
    assert_eq!(corruption_run.0, "error");
    assert_eq!(corruption_run.4.as_deref(), Some("broker recovery failed"));
    drop(corruption_repo);
    drop_schema(&base_url, &corruption_schema).await;

    let layout_schema = unique_schema("layout");
    let layout_repo = migrated_repo(&base_url, &layout_schema).await;
    let layout_data = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&layout_data, 64 * 1024 * 1024);
        create_topic(&mut broker, "orders", 1);
    }
    fs::create_dir_all(
        layout_data
            .path()
            .join("messages/topics/orders/partitions/9"),
    )
    .unwrap();
    let error = rebuild_projection(&layout_repo, layout_data.path())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "projection failed: projection source layout is invalid"
    );
    let layout_run = run_row(&layout_repo, latest_run_id(&layout_repo).await).await;
    assert_eq!(layout_run.0, "error");
    assert_eq!(
        layout_run.4.as_deref(),
        Some("projection source layout is invalid")
    );
    drop(layout_repo);
    drop_schema(&base_url, &layout_schema).await;
}

// --- Milestone 16: PostgreSQL full-text search foundation ---

fn sample_search_row(
    topic: &str,
    message_id: &str,
    offset: i64,
    subject: Option<&str>,
    content_type: &str,
) -> MessageRow {
    MessageRow {
        topic: topic.to_owned(),
        partition_id: 0,
        offset,
        message_id: message_id.to_owned(),
        idempotency_key: Some(format!("idem-{message_id}")),
        partition_key: Some("key-1".to_owned()),
        payload_len: 4,
        payload_sha256: compute_payload_sha256(b"data"),
        content_type: content_type.to_owned(),
        event_type: "order.created".to_owned(),
        source: "/orders-service".to_owned(),
        subject: subject.map(str::to_owned),
        headers: serde_json::json!({"trace-id": "trace-1"}),
        time_unix_ms: 1_700_000_000_000 + offset,
    }
}

#[tokio::test]
async fn migration_002_adds_search_columns_index_and_backfill() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_migration");
    let repo = migrated_repo(&base_url, &schema).await;

    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = current_schema()
           AND table_name = 'ferrumq_messages'
           AND column_name IN ('search_text', 'search_vector')",
    )
    .fetch_all(repo.pool())
    .await
    .unwrap();
    assert!(columns.contains(&"search_text".to_owned()));
    assert!(columns.contains(&"search_vector".to_owned()));

    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes
         WHERE schemaname = current_schema()
           AND tablename = 'ferrumq_messages'
           AND indexname = 'idx_messages_search_vector'",
    )
    .fetch_all(repo.pool())
    .await
    .unwrap();
    assert_eq!(indexes, vec!["idx_messages_search_vector".to_owned()]);

    let pre_existing = sample_search_row("orders", "pre-1", 0, Some("order/1"), "application/json");
    sqlx::query(
        "INSERT INTO ferrumq_messages
         (topic, partition_id, message_offset, message_id, idempotency_key,
          partition_key, payload_len, payload_sha256, content_type,
          event_type, source, subject, headers, time_unix_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
    )
    .bind(&pre_existing.topic)
    .bind(pre_existing.partition_id)
    .bind(pre_existing.offset)
    .bind(&pre_existing.message_id)
    .bind(&pre_existing.idempotency_key)
    .bind(&pre_existing.partition_key)
    .bind(pre_existing.payload_len)
    .bind(&pre_existing.payload_sha256)
    .bind(&pre_existing.content_type)
    .bind(&pre_existing.event_type)
    .bind(&pre_existing.source)
    .bind(&pre_existing.subject)
    .bind(&pre_existing.headers)
    .bind(pre_existing.time_unix_ms)
    .execute(repo.pool())
    .await
    .unwrap();
    let (before_text, before_vector): (String, String) = sqlx::query_as(
        "SELECT search_text::text, search_vector::text
         FROM ferrumq_messages WHERE message_id = 'pre-1'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(before_text, "");
    assert_eq!(before_vector, "");

    sqlx::raw_sql(include_str!("../migrations/002_full_text_search.sql"))
        .execute(repo.pool())
        .await
        .unwrap();

    let (after_text, after_vector): (String, String) = sqlx::query_as(
        "SELECT search_text, search_vector::text
         FROM ferrumq_messages WHERE message_id = 'pre-1'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert!(after_text.contains("pre-1"));
    assert!(after_text.contains("orders"));
    assert!(after_text.contains("order/1"));
    assert!(after_text.contains("application/json"));
    assert!(!after_text.contains("idem-pre-1"));
    assert!(!after_text.contains("key-1"));
    assert!(!after_vector.is_empty());

    sqlx::raw_sql(include_str!("../migrations/002_full_text_search.sql"))
        .execute(repo.pool())
        .await
        .unwrap();
    let (rerun_text,): (String,) =
        sqlx::query_as("SELECT search_text FROM ferrumq_messages WHERE message_id = 'pre-1'")
            .fetch_one(repo.pool())
            .await
            .unwrap();
    assert_eq!(rerun_text, after_text);

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn projected_rows_get_non_empty_search_vector() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_vector");
    let repo = migrated_repo(&base_url, &schema).await;
    let row = sample_search_row("orders", "msg-1", 0, Some("order/1"), "application/json");
    repo.upsert_message(&row).await.unwrap();
    let stored: String = sqlx::query_scalar(
        "SELECT search_vector::text FROM ferrumq_messages WHERE message_id = 'msg-1'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert!(!stored.is_empty());
    assert!(stored.contains("msg"));
    assert!(stored.contains("order"));

    let no_subject = sample_search_row("orders", "msg-2", 1, None, "application/json");
    repo.upsert_message(&no_subject).await.unwrap();
    let stored_text: String =
        sqlx::query_scalar("SELECT search_text FROM ferrumq_messages WHERE message_id = 'msg-2'")
            .fetch_one(repo.pool())
            .await
            .unwrap();
    assert!(!stored_text.contains("  "));
    assert!(stored_text.contains("msg-2"));
    assert!(stored_text.contains("application/json"));

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn search_finds_safe_metadata_fields() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_meta");
    let repo = migrated_repo(&base_url, &schema).await;
    for (mid, offset, subject, ct) in [
        (
            "unique-order-id-xyz",
            0,
            Some("order/1"),
            "application/json",
        ),
        ("payment-receipt-abc", 1, Some("payment/2"), "text/plain"),
        ("shipment-notice-def", 2, None, "cloudevents-format"),
    ] {
        let row = sample_search_row("orders", mid, offset, subject, ct);
        repo.upsert_message(&row).await.unwrap();
    }
    let other = sample_search_row("audit", "audit-entry-ghi", 0, None, "application/json");
    repo.upsert_message(&other).await.unwrap();

    for (query, expected_mid) in [
        ("unique-order-id-xyz", "unique-order-id-xyz"),
        ("payment-receipt-abc", "payment-receipt-abc"),
        ("shipment-notice-def", "shipment-notice-def"),
    ] {
        let q = SearchQuery::new(query, None, 20).unwrap();
        let results = repo.search_messages(&q).await.unwrap();
        assert!(
            results.iter().any(|r| r.message_id == expected_mid),
            "expected message_id {expected_mid} in results for query {query:?}"
        );
    }

    let q = SearchQuery::new("cloudevents", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert!(
        results
            .iter()
            .any(|r| r.message_id == "shipment-notice-def")
    );

    let q = SearchQuery::new("application/json", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    let json_ids: Vec<&str> = results
        .iter()
        .map(|r| r.message_id.as_str())
        .filter(|id| *id == "unique-order-id-xyz" || *id == "msg-2" || *id == "audit-entry-ghi")
        .collect();
    assert!(json_ids.contains(&"unique-order-id-xyz"));
    assert!(json_ids.contains(&"audit-entry-ghi"));

    let q = SearchQuery::new("/orders-service", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert!(results.iter().all(|r| r.source == "/orders-service"));
    assert!(!results.is_empty());

    let q = SearchQuery::new("payment/2", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert!(
        results
            .iter()
            .any(|r| r.message_id == "payment-receipt-abc")
    );

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn search_filters_by_exact_topic() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_topic");
    let repo = migrated_repo(&base_url, &schema).await;
    for (topic, mid, offset) in [
        ("orders", "order-1", 0),
        ("orders", "order-2", 1),
        ("payments", "payment-1", 0),
    ] {
        let row = sample_search_row(topic, mid, offset, None, "application/json");
        repo.upsert_message(&row).await.unwrap();
    }

    let q = SearchQuery::new("application/json", Some("orders".to_owned()), 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    let ids: Vec<&str> = results.iter().map(|r| r.message_id.as_str()).collect();
    assert_eq!(results.len(), 2);
    assert!(ids.contains(&"order-1"));
    assert!(ids.contains(&"order-2"));

    let q = SearchQuery::new("application/json", Some("payments".to_owned()), 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "payment-1");

    let q = SearchQuery::new("application/json", Some("nonexistent".to_owned()), 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert!(results.is_empty());

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn search_rejects_empty_and_blank_queries() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_empty");
    let repo = migrated_repo(&base_url, &schema).await;

    assert!(matches!(
        SearchQuery::new("", None, 20),
        Err(PostgresError::EmptySearchQuery)
    ));
    assert!(matches!(
        SearchQuery::new("   ", None, 20),
        Err(PostgresError::EmptySearchQuery)
    ));
    assert!(matches!(
        SearchQuery::new("...", None, 20),
        Err(PostgresError::EmptySearchQuery)
    ));
    assert!(matches!(
        SearchQuery::new("!!!", None, 20),
        Err(PostgresError::EmptySearchQuery)
    ));
    assert!(matches!(
        SearchQuery::new("   ...   ", None, 20),
        Err(PostgresError::EmptySearchQuery)
    ));

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn search_limit_is_bounded() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_limit");
    let repo = migrated_repo(&base_url, &schema).await;
    for i in 0..5 {
        let row = sample_search_row("orders", &format!("dup-{i}"), i, None, "application/json");
        repo.upsert_message(&row).await.unwrap();
    }

    let q = SearchQuery::new("application/json", None, 1).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 1);

    let q = SearchQuery::new("application/json", None, 3).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 3);

    let q = SearchQuery::new("application/json", None, 100).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 5);

    assert!(matches!(
        SearchQuery::new("order", None, 0),
        Err(PostgresError::InvalidSearchLimit)
    ));
    assert!(matches!(
        SearchQuery::new("order", None, 101),
        Err(PostgresError::InvalidSearchLimit)
    ));

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn search_ordering_is_deterministic_for_ties() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_order");
    let repo = migrated_repo(&base_url, &schema).await;
    for (topic, pid, mid, offset, t) in [
        ("zeta", 0, "tie-a", 0, 1_700_000_000_000),
        ("alpha", 1, "tie-b", 0, 1_700_000_000_000),
        ("alpha", 0, "tie-c", 0, 1_700_000_000_000),
        ("alpha", 0, "tie-d", 1, 1_700_000_000_001),
    ] {
        let mut row = sample_search_row(topic, mid, offset, None, "application/json");
        row.partition_id = pid;
        row.time_unix_ms = t;
        repo.upsert_message(&row).await.unwrap();
    }

    let q = SearchQuery::new("application/json", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    let order: Vec<&str> = results.iter().map(|r| r.message_id.as_str()).collect();
    assert_eq!(order, vec!["tie-d", "tie-c", "tie-b", "tie-a"]);

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn search_does_not_find_payload_only_content() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_payload");
    let repo = migrated_repo(&base_url, &schema).await;
    let row = sample_search_row(
        "orders",
        "msg-payload",
        0,
        Some("order/1"),
        "application/json",
    );
    repo.upsert_message(&row).await.unwrap();

    let q = SearchQuery::new("zqzqzqzqzq-payload-secret-word", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert!(results.is_empty());

    let q = SearchQuery::new("order/1", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "msg-payload");

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn search_results_do_not_expose_idempotency_keys_or_payload() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_no_leak");
    let repo = migrated_repo(&base_url, &schema).await;
    let row = sample_search_row(
        "orders",
        "msg-leak-test",
        0,
        Some("order/1"),
        "application/json",
    );
    repo.upsert_message(&row).await.unwrap();

    let q = SearchQuery::new("msg-leak-test", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 1);
    let json = serde_json::to_string(&results).unwrap();
    assert!(!json.contains("idem-msg-leak-test"));
    assert!(!json.contains("key-1"));
    assert!(!json.contains("idempotency_key"));
    assert!(!json.contains("partition_key"));
    assert!(!json.contains("headers"));
    assert!(!json.contains("payload_secret"));
    assert!(json.contains("payload_sha256"));
    assert!(json.contains("payload_len"));

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn rebuild_twice_does_not_duplicate_and_search_still_works() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_rebuild");
    let repo = migrated_repo(&base_url, &schema).await;
    let data = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&data, 64 * 1024 * 1024);
        create_topic(&mut broker, "orders", 1);
        publish(
            &mut broker,
            "orders",
            envelope("searchable-msg-1", 1_000, b"first", None, None),
        );
        publish(
            &mut broker,
            "orders",
            envelope("searchable-msg-2", 2_000, b"second", None, None),
        );
    }

    let first = rebuild_projection(&repo, data.path()).await.unwrap();
    let second = rebuild_projection(&repo, data.path()).await.unwrap();
    assert_eq!((first.topics_count, first.messages_count), (1, 2));
    assert_eq!((second.topics_count, second.messages_count), (1, 2));

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ferrumq_messages")
        .fetch_one(repo.pool())
        .await
        .unwrap();
    assert_eq!(count, 2);

    let q = SearchQuery::new("searchable-msg-1", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "searchable-msg-1");

    let q = SearchQuery::new("orders", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 2);

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn empty_topics_do_not_break_search() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_empty_topic");
    let repo = migrated_repo(&base_url, &schema).await;
    let data = TempDir::new().unwrap();
    {
        let mut broker = open_broker(&data, 64 * 1024 * 1024);
        create_topic(&mut broker, "empty-topic", 3);
        create_topic(&mut broker, "orders", 1);
        publish(
            &mut broker,
            "orders",
            envelope("msg-1", 1_000, b"data", None, None),
        );
    }
    rebuild_projection(&repo, data.path()).await.unwrap();

    let q = SearchQuery::new("application/json", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "msg-1");

    let topic_count: i64 = sqlx::query_scalar("SELECT count(*) FROM ferrumq_topics")
        .fetch_one(repo.pool())
        .await
        .unwrap();
    assert_eq!(topic_count, 2);

    let q = SearchQuery::new("empty", Some("empty-topic".to_owned()), 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert!(results.is_empty());

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn pre_milestone_16_database_upgraded_gets_searchable_rows() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_upgrade");
    create_schema(&base_url, &schema).await;
    let repo = scoped_repo(&base_url, &schema).await;

    sqlx::raw_sql(include_str!("../migrations/001_initial_schema.sql"))
        .execute(repo.pool())
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _ferrumq_migrations (
            version     INTEGER PRIMARY KEY,
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            name        TEXT NOT NULL
        )",
    )
    .execute(repo.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO _ferrumq_migrations (version, name) VALUES (1, '001_initial_schema')")
        .execute(repo.pool())
        .await
        .unwrap();

    let pre_msg = sample_search_row(
        "orders",
        "legacy-msg-1",
        0,
        Some("order/1"),
        "application/json",
    );
    sqlx::query(
        "INSERT INTO ferrumq_messages
         (topic, partition_id, message_offset, message_id, idempotency_key,
          partition_key, payload_len, payload_sha256, content_type,
          event_type, source, subject, headers, time_unix_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
    )
    .bind(&pre_msg.topic)
    .bind(pre_msg.partition_id)
    .bind(pre_msg.offset)
    .bind(&pre_msg.message_id)
    .bind(&pre_msg.idempotency_key)
    .bind(&pre_msg.partition_key)
    .bind(pre_msg.payload_len)
    .bind(&pre_msg.payload_sha256)
    .bind(&pre_msg.content_type)
    .bind(&pre_msg.event_type)
    .bind(&pre_msg.source)
    .bind(&pre_msg.subject)
    .bind(&pre_msg.headers)
    .bind(pre_msg.time_unix_ms)
    .execute(repo.pool())
    .await
    .unwrap();

    run_migrations(repo.pool()).await.unwrap();

    let (text, vector): (String, String) = sqlx::query_as(
        "SELECT search_text, search_vector::text
         FROM ferrumq_messages WHERE message_id = 'legacy-msg-1'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert!(text.contains("legacy-msg-1"));
    assert!(text.contains("orders"));
    assert!(text.contains("order/1"));
    assert!(!vector.is_empty());

    let q = SearchQuery::new("legacy-msg-1", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "legacy-msg-1");

    let q = SearchQuery::new("orders", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 1);

    let q = SearchQuery::new("application/json", None, 20).unwrap();
    let results = repo.search_messages(&q).await.unwrap();
    assert_eq!(results.len(), 1);

    let migrations: Vec<(i32, String)> =
        sqlx::query_as("SELECT version, name FROM _ferrumq_migrations ORDER BY version")
            .fetch_all(repo.pool())
            .await
            .unwrap();
    assert_eq!(
        migrations,
        vec![
            (1, "001_initial_schema".to_owned()),
            (2, "002_full_text_search".to_owned()),
        ]
    );

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn search_text_matches_compute_search_text_for_subject_and_no_subject() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("search_parity");
    let repo = migrated_repo(&base_url, &schema).await;
    let with_subject = sample_search_row(
        "orders",
        "parity-with",
        0,
        Some("order/9"),
        "application/json",
    );
    let without_subject =
        sample_search_row("orders", "parity-without", 1, None, "application/json");
    repo.upsert_message(&with_subject).await.unwrap();
    repo.upsert_message(&without_subject).await.unwrap();

    let stored_with: String = sqlx::query_scalar(
        "SELECT search_text FROM ferrumq_messages WHERE message_id = 'parity-with'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();
    let stored_without: String = sqlx::query_scalar(
        "SELECT search_text FROM ferrumq_messages WHERE message_id = 'parity-without'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();

    assert_eq!(
        stored_with,
        msg_postgres::models::compute_search_text(&with_subject)
    );
    assert_eq!(
        stored_without,
        msg_postgres::models::compute_search_text(&without_subject)
    );

    drop(repo);
    drop_schema(&base_url, &schema).await;
}

#[tokio::test]
async fn connect_with_pool_size_supports_serving_workload() {
    let Some(base_url) = test_database_url() else {
        eprintln!("Skipping: FERRUMQ_POSTGRES_TEST_URL not set");
        return;
    };
    let schema = unique_schema("poolsize");
    create_schema(&base_url, &schema).await;

    let config = PostgresConfig::from_url(Some(schema_url(&base_url, &schema))).unwrap();
    let repo = PostgresRepository::connect_with_pool_size(config, 4)
        .await
        .unwrap();
    run_migrations(repo.pool()).await.unwrap();

    let row = sample_message_row("orders", "msg-pool-1");
    repo.upsert_message(&row).await.unwrap();

    let query = SearchQuery::new("msg-pool-1", None, 5).unwrap();
    let results = repo.search_messages(&query).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "msg-pool-1");

    let _concurrent = tokio::join!(
        async {
            let q = SearchQuery::new("msg-pool-1", None, 5).unwrap();
            repo.search_messages(&q).await
        },
        async {
            let q = SearchQuery::new("msg-pool-1", None, 5).unwrap();
            repo.search_messages(&q).await
        },
    );

    drop(repo);
    drop_schema(&base_url, &schema).await;
}
