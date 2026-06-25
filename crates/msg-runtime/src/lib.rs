use std::{future::Future, net::SocketAddr, path::PathBuf};

use msg_control_api::{ControlApiConfig, ControlApiError, build_router, open_state_with_search};
use msg_data_plane::{DataPlaneService, FerrumQDataPlaneServer};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::watch,
    task::{self, JoinHandle},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
#[cfg(feature = "postgres")]
use {
    msg_control_api::MessageSearch, msg_postgres::PostgresError, std::sync::Arc as StdArc,
    tracing::info,
};

/// Pool size for the HTTP control plane search endpoint.
#[cfg(feature = "postgres")]
pub const SEARCH_POOL_SIZE: u32 = 4;

#[cfg(not(feature = "postgres"))]
const POSTGRES_DISABLED_MESSAGE: &str = "PostgreSQL search support is disabled in this build";

/// Configuration for the unified local HTTP and gRPC runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServeAllConfig {
    pub data_dir: PathBuf,
    pub http_listen: SocketAddr,
    pub grpc_listen: SocketAddr,
    /// Optional PostgreSQL connection URL for the search endpoint.
    ///
    /// When set, the runtime connects at startup, runs migrations, and wires
    /// the search dependency into the HTTP control API. When absent, the
    /// server starts with search disabled and the `/v1/search/messages`
    /// endpoint returns 503.
    pub postgres_database_url: Option<String>,
}

impl ServeAllConfig {
    #[must_use]
    pub fn new(
        data_dir: impl Into<PathBuf>,
        http_listen: SocketAddr,
        grpc_listen: SocketAddr,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            http_listen,
            grpc_listen,
            postgres_database_url: None,
        }
    }

    /// Sets the optional PostgreSQL connection URL.
    #[must_use]
    pub fn with_postgres_database_url(mut self, url: Option<String>) -> Self {
        self.postgres_database_url = url;
        self
    }
}

/// Errors raised by the unified local runtime.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to bind HTTP listener at {addr}")]
    BindHttp {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind gRPC listener at {addr}")]
    BindGrpc {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open shared durable broker state")]
    OpenState(#[from] ControlApiError),
    #[error("HTTP server failed")]
    Http(#[source] std::io::Error),
    #[error("gRPC server failed")]
    Grpc(#[source] tonic::transport::Error),
    #[error("PostgreSQL setup failed: {0}")]
    PostgresSetup(String),
}

/// Serves the unified local runtime until the process exits or a server fails.
///
/// Both sockets are bound before the shared durable broker state is opened, so
/// bind errors are reported before any long-running serving task starts.
pub async fn serve_all(config: ServeAllConfig) -> Result<(), RuntimeError> {
    let http_listener = TcpListener::bind(config.http_listen)
        .await
        .map_err(|source| RuntimeError::BindHttp {
            addr: config.http_listen,
            source,
        })?;
    let grpc_listener = TcpListener::bind(config.grpc_listen)
        .await
        .map_err(|source| RuntimeError::BindGrpc {
            addr: config.grpc_listen,
            source,
        })?;

    serve_all_with_listeners(
        config.data_dir.clone(),
        http_listener,
        grpc_listener,
        std::future::pending::<()>(),
        config.postgres_database_url,
    )
    .await
}

/// Serves the unified local runtime on pre-bound listeners.
///
/// This is primarily useful for tests and harnesses that need ephemeral ports.
/// HTTP and gRPC share one `DurableBroker` through the existing
/// `Arc<Mutex<DurableBroker>>` adapter boundary.
///
/// When `postgres_database_url` is `Some`, the runtime attempts to connect and
/// run migrations at startup. Connection or migration failures fail startup
/// with a sanitized error and the URL/credentials are never logged. When
/// `None`, the server starts normally with search disabled.
pub async fn serve_all_with_listeners(
    data_dir: PathBuf,
    http_listener: TcpListener,
    grpc_listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
    postgres_database_url: Option<String>,
) -> Result<(), RuntimeError> {
    #[cfg(feature = "postgres")]
    let search: Option<StdArc<dyn MessageSearch>> = build_search(postgres_database_url).await?;

    #[cfg(not(feature = "postgres"))]
    ensure_postgres_disabled(postgres_database_url)?;

    let state = {
        #[cfg(feature = "postgres")]
        {
            open_state_with_search(ControlApiConfig::new(data_dir), search)?
        }
        #[cfg(not(feature = "postgres"))]
        {
            open_state_with_search(
                ControlApiConfig::new(data_dir),
                msg_control_api::NoopSearchHandle::new(),
            )?
        }
    };
    let service = DataPlaneService::from_shared(state.broker());
    let router = build_router(state);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shutdown_task = spawn_shutdown_signal(shutdown_tx, shutdown);

    let http_shutdown = wait_for_shutdown(shutdown_rx.clone());
    let grpc_shutdown = wait_for_shutdown(shutdown_rx);
    let http = async move {
        axum::serve(http_listener, router)
            .with_graceful_shutdown(http_shutdown)
            .await
            .map_err(RuntimeError::Http)
    };
    let grpc = async move {
        Server::builder()
            .add_service(FerrumQDataPlaneServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(grpc_listener), grpc_shutdown)
            .await
            .map_err(RuntimeError::Grpc)
    };

    let result = tokio::try_join!(http, grpc).map(|(_http, _grpc)| ());

    shutdown_task.abort();
    result
}

#[cfg(not(feature = "postgres"))]
fn ensure_postgres_disabled(postgres_database_url: Option<String>) -> Result<(), RuntimeError> {
    if postgres_database_url.is_some() || std::env::var_os("FERRUMQ_DATABASE_URL").is_some() {
        return Err(RuntimeError::PostgresSetup(
            POSTGRES_DISABLED_MESSAGE.to_owned(),
        ));
    }

    Ok(())
}

#[cfg(feature = "postgres")]
async fn build_search(
    postgres_database_url: Option<String>,
) -> Result<Option<StdArc<dyn MessageSearch>>, RuntimeError> {
    let config = match msg_postgres::PostgresConfig::from_env_or_flag(postgres_database_url) {
        Ok(config) => config,
        Err(msg_postgres::PostgresError::MissingDatabaseUrl) => {
            info!("search disabled; database URL not configured");
            return Ok(None);
        }
        Err(error) => {
            return Err(RuntimeError::PostgresSetup(sanitize_pg_error(&error)));
        }
    };
    let sanitized = config.sanitized_url();
    let repo =
        match msg_postgres::PostgresRepository::connect_with_pool_size(config, SEARCH_POOL_SIZE)
            .await
        {
            Ok(repo) => repo,
            Err(error) => {
                return Err(RuntimeError::PostgresSetup(sanitize_pg_error(&error)));
            }
        };
    if let Err(error) = msg_postgres::migrations::run_migrations(repo.pool()).await {
        return Err(RuntimeError::PostgresSetup(sanitize_pg_error(&error)));
    }
    info!(
        database_url = %sanitized,
        pool_size = SEARCH_POOL_SIZE,
        "search enabled",
    );
    Ok(Some(StdArc::new(repo) as StdArc<dyn MessageSearch>))
}

#[cfg(feature = "postgres")]
fn sanitize_pg_error(error: &PostgresError) -> String {
    let message = error.to_string();
    if message.is_empty() {
        "search backend is unavailable".to_owned()
    } else {
        message
    }
}

fn spawn_shutdown_signal(
    shutdown_tx: watch::Sender<bool>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<()> {
    task::spawn(async move {
        shutdown.await;
        let _ = shutdown_tx.send(true);
    })
}

async fn wait_for_shutdown(mut shutdown_rx: watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow_and_update() {
            break;
        }
        if shutdown_rx.changed().await.is_err() {
            break;
        }
    }
}

/// Returns this crate's package name.
#[must_use]
pub fn crate_name() -> &'static str {
    "msg-runtime"
}

#[cfg(test)]
mod tests {
    use super::crate_name;

    #[test]
    fn reports_crate_name() {
        assert_eq!(crate_name(), "msg-runtime");
    }
}
