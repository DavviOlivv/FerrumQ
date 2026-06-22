use std::time::Duration;

use sqlx::{PgPool, postgres::PgPoolOptions};
use tracing::info;

use crate::{
    PostgresError,
    config::{PostgresConfig, log_database_target},
    models::{
        MessageRow, ProjectionResult, SearchQuery, SearchResult, TopicRow, compute_search_text,
    },
};

/// Async PostgreSQL repository for the FerrumQ metadata projection store.
#[derive(Debug, Clone)]
pub struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    /// Connects to PostgreSQL and returns a repository handle.
    ///
    /// The pool uses a single connection for the offline rebuild command.
    pub async fn connect(config: &PostgresConfig) -> Result<Self, PostgresError> {
        log_database_target(&config.sanitized_url());
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(config.database_url())
            .await
            .map_err(PostgresError::ConnectionFailed)?;
        Ok(Self { pool })
    }

    /// Returns a reference to the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Upserts a message-bearing topic row with deterministic timestamps.
    pub async fn upsert_topic(&self, row: &TopicRow) -> Result<(), PostgresError> {
        sqlx::query(
            "INSERT INTO ferrumq_topics (name, partitions, first_seen_at, last_seen_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (name) DO UPDATE SET
               partitions = EXCLUDED.partitions,
               first_seen_at = EXCLUDED.first_seen_at,
               last_seen_at = EXCLUDED.last_seen_at",
        )
        .bind(&row.name)
        .bind(row.partitions)
        .bind(row.first_seen_at)
        .bind(row.last_seen_at)
        .execute(self.pool())
        .await
        .map_err(|source| PostgresError::query("topic upsert", source))?;
        Ok(())
    }

    /// Records an empty topic. Existing timestamps remain unchanged so
    /// repeated rebuilds do not make metadata appear newer.
    pub async fn upsert_empty_topic(
        &self,
        topic_name: &str,
        partitions: i32,
    ) -> Result<(), PostgresError> {
        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO ferrumq_topics (name, partitions, first_seen_at, last_seen_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (name) DO UPDATE SET
               partitions = EXCLUDED.partitions",
        )
        .bind(topic_name)
        .bind(partitions)
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(|source| PostgresError::query("empty topic upsert", source))?;
        Ok(())
    }

    /// Upserts a message row.
    ///
    /// - `ON CONFLICT (topic, partition_id, message_offset)` → `DO NOTHING`
    ///   (idempotent rebuild).
    /// - `ON CONFLICT (topic, message_id)` → returns
    ///   `PostgresError::MessageIdConflict` because the same message_id at a
    ///   different message offset indicates a data integrity issue.
    ///
    /// Postgres fires at most one `ON CONFLICT` action per row. Since the
    /// primary key includes `message_offset`, a repeated location
    /// row hits the PK conflict and is silently ignored (rebuild idempotency).
    /// A different location with the same `message_id` hits the
    /// unique constraint on (topic, message_id), which returns a constraint
    /// violation error that we translate.
    ///
    /// `search_text` is derived from safe metadata fields via
    /// `compute_search_text`, and `search_vector` is computed in SQL via
    /// `to_tsvector('simple', search_text)`. Raw payload bytes,
    /// `payload_sha256`, `idempotency_key`, and `partition_key` are not
    /// included in the search text.
    pub async fn upsert_message(&self, row: &MessageRow) -> Result<(), PostgresError> {
        let search_text = compute_search_text(row);
        let result = sqlx::query(
            "INSERT INTO ferrumq_messages
             (topic, partition_id, message_offset, message_id, idempotency_key,
              partition_key, payload_len, payload_sha256, content_type,
              event_type, source, subject, headers, time_unix_ms,
              search_text, search_vector)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                     $15, to_tsvector('simple', $15))
             ON CONFLICT (topic, partition_id, message_offset) DO NOTHING",
        )
        .bind(&row.topic)
        .bind(row.partition_id)
        .bind(row.offset)
        .bind(&row.message_id)
        .bind(&row.idempotency_key)
        .bind(&row.partition_key)
        .bind(row.payload_len)
        .bind(&row.payload_sha256)
        .bind(&row.content_type)
        .bind(&row.event_type)
        .bind(&row.source)
        .bind(&row.subject)
        .bind(&row.headers)
        .bind(row.time_unix_ms)
        .bind(&search_text)
        .execute(self.pool())
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(db_err))
                if db_err.constraint() == Some("ferrumq_messages_topic_message_id_key") =>
            {
                Err(PostgresError::MessageIdConflict {
                    topic: row.topic.clone(),
                })
            }
            Err(source) => Err(PostgresError::query("message upsert", source)),
        }
    }

    /// Searches projected message metadata using PostgreSQL full-text search.
    ///
    /// The query is validated before reaching the database: empty, blank, or
    /// punctuation-only inputs return `EmptySearchQuery`; limits outside
    /// `1..=100` return `InvalidSearchLimit`. All user input is bound as
    /// parameters; no string concatenation is used.
    ///
    /// Ordering is deterministic:
    /// `rank DESC, time_unix_ms DESC, topic ASC, partition_id ASC, message_offset ASC`.
    pub async fn search_messages(
        &self,
        query: &SearchQuery,
    ) -> Result<Vec<SearchResult>, PostgresError> {
        let limit = i64::from(query.limit());
        let topic_filter: Option<String> = query.topic().map(str::to_owned);
        let rows: Vec<SearchResult> = sqlx::query_as(
            "WITH q AS (
                 SELECT websearch_to_tsquery('simple', $1) AS tsq
             )
             SELECT m.topic, m.partition_id, m.message_offset, m.message_id, m.event_type,
                    m.source, m.subject, m.content_type, m.time_unix_ms, m.payload_len,
                    m.payload_sha256,
                    ts_rank(m.search_vector, q.tsq) AS rank
             FROM ferrumq_messages m
             CROSS JOIN q
             WHERE m.search_vector @@ q.tsq
               AND ($2::text IS NULL OR m.topic = $2)
             ORDER BY rank DESC, m.time_unix_ms DESC, m.topic ASC,
                      m.partition_id ASC, m.message_offset ASC
             LIMIT $3",
        )
        .bind(query.query())
        .bind(topic_filter)
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .map_err(|source| PostgresError::query("message search", source))?;
        Ok(rows)
    }

    /// Records the start of a projection run.
    pub async fn start_projection_run(&self) -> Result<i64, PostgresError> {
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO ferrumq_projection_runs (status) VALUES ('in_progress') RETURNING id",
        )
        .fetch_one(self.pool())
        .await
        .map_err(|source| PostgresError::query("projection run start", source))?;
        Ok(row.0)
    }

    /// Marks a projection run as completed with the given result.
    pub async fn complete_projection_run(
        &self,
        run_id: i64,
        result: &ProjectionResult,
    ) -> Result<(), PostgresError> {
        let topics_count = i32::try_from(result.topics_count).map_err(|_| {
            PostgresError::ProjectionValueOutOfRange {
                field: "topics_count",
            }
        })?;
        let messages_count = i32::try_from(result.messages_count).map_err(|_| {
            PostgresError::ProjectionValueOutOfRange {
                field: "messages_count",
            }
        })?;
        let update = sqlx::query(
            "UPDATE ferrumq_projection_runs
             SET completed_at = now(),
                 topics_count = $1,
                 messages_count = $2,
                 status = 'success',
                 error_message = NULL
             WHERE id = $3",
        )
        .bind(topics_count)
        .bind(messages_count)
        .bind(run_id)
        .execute(self.pool())
        .await
        .map_err(|source| PostgresError::query("projection run completion", source))?;
        ensure_one_run_updated(run_id, update.rows_affected())?;
        info!(
            run_id = run_id,
            topics = result.topics_count,
            messages = result.messages_count,
            "projection run completed successfully"
        );
        Ok(())
    }

    /// Marks a projection run as failed with the given error message.
    pub async fn fail_projection_run(
        &self,
        run_id: i64,
        error_message: &str,
    ) -> Result<(), PostgresError> {
        let result = sqlx::query(
            "UPDATE ferrumq_projection_runs
             SET completed_at = now(),
                 status = 'error',
                 error_message = $1
             WHERE id = $2",
        )
        .bind(error_message)
        .bind(run_id)
        .execute(self.pool())
        .await
        .map_err(|source| PostgresError::query("projection run failure", source))?;
        ensure_one_run_updated(run_id, result.rows_affected())?;
        Ok(())
    }
}

fn ensure_one_run_updated(run_id: i64, rows_affected: u64) -> Result<(), PostgresError> {
    if rows_affected == 1 {
        Ok(())
    } else {
        Err(PostgresError::ProjectionRunNotFound { run_id })
    }
}
