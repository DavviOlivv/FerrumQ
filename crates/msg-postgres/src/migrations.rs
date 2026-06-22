use sqlx::{PgPool, Row};
use tracing::info;

use crate::PostgresError;

const MIGRATION_LOCK_ID: i64 = 0x4665_7272_756d_510f;
const MIGRATIONS: &[(&str, &str)] = &[(
    "001_initial_schema",
    include_str!("../migrations/001_initial_schema.sql"),
)];

/// Runs all pending migrations against the given pool.
///
/// Migrations are tracked in `_ferrumq_migrations` and applied in order.
/// Each migration runs inside a transaction. Already-applied migrations are
/// skipped.
pub async fn run_migrations(pool: &PgPool) -> Result<(), PostgresError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|source| PostgresError::migration("transaction start", source))?;

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut *tx)
        .await
        .map_err(|source| PostgresError::migration("migration serialization", source))?;

    ensure_migrations_table(&mut tx).await?;

    let applied =
        sqlx::query("SELECT version, name FROM _ferrumq_migrations ORDER BY version FOR UPDATE")
            .fetch_all(&mut *tx)
            .await
            .map_err(|source| PostgresError::migration("migration metadata read", source))?;

    validate_applied_migrations(&applied)?;

    for (index, (name, sql)) in MIGRATIONS.iter().enumerate().skip(applied.len()) {
        let version =
            i32::try_from(index + 1).map_err(|_| PostgresError::InconsistentMigrationMetadata)?;
        info!(version, migration = %name, "applying migration");

        sqlx::raw_sql(sql)
            .execute(&mut *tx)
            .await
            .map_err(|source| PostgresError::migration("migration SQL", source))?;

        sqlx::query("INSERT INTO _ferrumq_migrations (version, name) VALUES ($1, $2)")
            .bind(version)
            .bind(name)
            .execute(&mut *tx)
            .await
            .map_err(|source| PostgresError::migration("migration tracking", source))?;

        info!(version, migration = %name, "migration applied");
    }

    let pending = MIGRATIONS.len().saturating_sub(applied.len());
    tx.commit()
        .await
        .map_err(|source| PostgresError::migration("transaction commit", source))?;

    info!(total = MIGRATIONS.len(), pending, "migrations complete");

    Ok(())
}

fn validate_applied_migrations(rows: &[sqlx::postgres::PgRow]) -> Result<(), PostgresError> {
    if rows.len() > MIGRATIONS.len() {
        return Err(PostgresError::InconsistentMigrationMetadata);
    }

    for (index, row) in rows.iter().enumerate() {
        let expected_version =
            i32::try_from(index + 1).map_err(|_| PostgresError::InconsistentMigrationMetadata)?;
        let version: i32 = row
            .try_get("version")
            .map_err(|_| PostgresError::InconsistentMigrationMetadata)?;
        let name: String = row
            .try_get("name")
            .map_err(|_| PostgresError::InconsistentMigrationMetadata)?;
        if version != expected_version || name != MIGRATIONS[index].0 {
            return Err(PostgresError::InconsistentMigrationMetadata);
        }
    }

    Ok(())
}

async fn ensure_migrations_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), PostgresError> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS _ferrumq_migrations (
            version     INTEGER PRIMARY KEY,
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            name        TEXT NOT NULL
        )",
    )
    .execute(&mut **tx)
    .await
    .map_err(|source| PostgresError::migration("migration table creation", source))?;

    Ok(())
}
