pub mod config;
pub mod migrations;
pub mod models;
pub mod projection;
pub mod repository;

pub use config::PostgresConfig;

use thiserror::Error;

/// Errors raised by the PostgreSQL metadata store.
#[derive(Debug, Error)]
pub enum PostgresError {
    #[error("missing database URL; set --database-url or FERRUMQ_DATABASE_URL")]
    MissingDatabaseUrl,

    #[error("invalid database URL")]
    InvalidDatabaseUrl,

    #[error("database connection failed")]
    ConnectionFailed(#[source] sqlx::Error),

    #[error("database migration failed during {operation}")]
    MigrationFailed {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },

    #[error("database migration metadata is inconsistent")]
    InconsistentMigrationMetadata,

    #[error("database query failed during {operation}")]
    QueryFailed {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },

    #[error("projection run {run_id} does not exist")]
    ProjectionRunNotFound { run_id: i64 },

    #[error("projection failed: {0}")]
    ProjectionFailed(String),

    #[error("message_id conflict for topic '{topic}'")]
    MessageIdConflict { topic: String },

    #[error("projection data is outside the PostgreSQL range for {field}")]
    ProjectionValueOutOfRange { field: &'static str },

    #[error("projection source layout is invalid")]
    InvalidProjectionSource,

    #[error("projection source I/O failed")]
    Io(#[source] std::io::Error),

    #[error("broker recovery failed while reading projection source")]
    BrokerRecovery(#[source] msg_broker::DurableBrokerError),

    #[error("storage recovery failed while reading projection source")]
    Storage(#[source] msg_storage::StorageError),
}

impl PostgresError {
    #[must_use]
    pub fn query(operation: &'static str, source: sqlx::Error) -> Self {
        Self::QueryFailed { operation, source }
    }

    #[must_use]
    pub fn migration(operation: &'static str, source: sqlx::Error) -> Self {
        Self::MigrationFailed { operation, source }
    }
}

impl From<std::io::Error> for PostgresError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<msg_broker::DurableBrokerError> for PostgresError {
    fn from(error: msg_broker::DurableBrokerError) -> Self {
        Self::BrokerRecovery(error)
    }
}

impl From<msg_storage::StorageError> for PostgresError {
    fn from(error: msg_storage::StorageError) -> Self {
        Self::Storage(error)
    }
}
