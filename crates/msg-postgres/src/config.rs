use std::env;

use tracing::info;
use url::Url;

use crate::PostgresError;

/// Configuration for the PostgreSQL metadata store.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    database_url: String,
}

impl PostgresConfig {
    /// Creates a config from an explicit URL. Returns an error if the URL is
    /// absent or empty.
    pub fn from_url(url: Option<String>) -> Result<Self, PostgresError> {
        let url = url
            .filter(|value| !value.trim().is_empty())
            .ok_or(PostgresError::MissingDatabaseUrl)?;
        let url = url.trim();
        let parsed = Url::parse(url).map_err(|_| PostgresError::InvalidDatabaseUrl)?;
        if !matches!(parsed.scheme(), "postgres" | "postgresql") {
            return Err(PostgresError::InvalidDatabaseUrl);
        }

        Ok(Self {
            database_url: url.to_owned(),
        })
    }

    /// Resolves the database URL from an explicit CLI flag, then
    /// `FERRUMQ_DATABASE_URL`, then fails with a clear error.
    pub fn from_env_or_flag(cli_url: Option<String>) -> Result<Self, PostgresError> {
        let url = cli_url.filter(|u| !u.trim().is_empty()).or_else(|| {
            env::var("FERRUMQ_DATABASE_URL")
                .ok()
                .filter(|u| !u.trim().is_empty())
        });
        Self::from_url(url)
    }

    #[must_use]
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Returns a sanitized version of the database URL with the password
    /// masked and query parameters omitted for logs and error reports.
    #[must_use]
    pub fn sanitized_url(&self) -> String {
        sanitize_database_url(&self.database_url)
    }
}

/// Masks the password component of a PostgreSQL connection URI.
///
/// Examples:
/// - `postgres://user:password@host:5432/db` → `postgres://user:***@host:5432/db`
/// - `postgres://user@host/db` → `postgres://user@host/db` (no password)
/// - `postgres://host/db` → `postgres://host/db` (no credentials)
#[must_use]
pub fn sanitize_database_url(url: &str) -> String {
    let Ok(parsed) = Url::parse(url) else {
        return "<invalid PostgreSQL target>".to_owned();
    };

    let mut sanitized = format!("{}://", parsed.scheme());
    if !parsed.username().is_empty() {
        sanitized.push_str(parsed.username());
        if parsed.password().is_some() {
            sanitized.push_str(":***");
        }
        sanitized.push('@');
    }
    if let Some(host) = parsed.host_str() {
        if host.contains(':') {
            sanitized.push('[');
            sanitized.push_str(host);
            sanitized.push(']');
        } else {
            sanitized.push_str(host);
        }
    }
    if let Some(port) = parsed.port() {
        sanitized.push(':');
        sanitized.push_str(&port.to_string());
    }
    sanitized.push_str(parsed.path());
    if parsed.path().is_empty() {
        sanitized.push('/');
    }
    sanitized
}

/// Logs the sanitized database target. Called at the start of
/// Postgres commands to make the connection target visible without leaking
/// credentials.
pub fn log_database_target(sanitized_url: &str) {
    info!(database_url = %sanitized_url, "connecting to PostgreSQL metadata store");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_masks_password() {
        assert_eq!(
            sanitize_database_url("postgres://user:secret@host:5432/db"),
            "postgres://user:***@host:5432/db"
        );
    }

    #[test]
    fn sanitize_no_password() {
        assert_eq!(
            sanitize_database_url("postgres://user@host:5432/db"),
            "postgres://user@host:5432/db"
        );
    }

    #[test]
    fn sanitize_no_credentials() {
        assert_eq!(
            sanitize_database_url("postgres://host:5432/db"),
            "postgres://host:5432/db"
        );
    }

    #[test]
    fn sanitize_empty_string() {
        assert_eq!(sanitize_database_url(""), "<invalid PostgreSQL target>");
    }

    #[test]
    fn sanitize_omits_query_parameters() {
        assert_eq!(
            sanitize_database_url(
                "postgres://user:secret@host:5432/db?sslmode=require&password=other"
            ),
            "postgres://user:***@host:5432/db"
        );
    }

    #[test]
    fn from_env_or_flag_uses_cli_flag_first() {
        let config = PostgresConfig::from_env_or_flag(Some("postgres://flag".to_owned())).unwrap();
        assert_eq!(config.database_url, "postgres://flag");
    }

    #[test]
    fn from_url_rejects_empty() {
        assert!(matches!(
            PostgresConfig::from_url(None),
            Err(PostgresError::MissingDatabaseUrl)
        ));
        assert!(matches!(
            PostgresConfig::from_url(Some("".to_owned())),
            Err(PostgresError::MissingDatabaseUrl)
        ));
        assert!(matches!(
            PostgresConfig::from_url(Some("  ".to_owned())),
            Err(PostgresError::MissingDatabaseUrl)
        ));
    }

    #[test]
    fn from_url_rejects_invalid_or_non_postgres_urls() {
        assert!(matches!(
            PostgresConfig::from_url(Some("not-a-url".to_owned())),
            Err(PostgresError::InvalidDatabaseUrl)
        ));
        assert!(matches!(
            PostgresConfig::from_url(Some("https://example.com/db".to_owned())),
            Err(PostgresError::InvalidDatabaseUrl)
        ));
    }
}
