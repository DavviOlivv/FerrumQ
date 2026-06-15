use std::{future::Future, net::SocketAddr, path::PathBuf};

use msg_control_api::{ControlApiConfig, ControlApiError, build_router, open_state};
use msg_data_plane::{DataPlaneService, FerrumQDataPlaneServer};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::watch,
    task::{self, JoinHandle},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Configuration for the unified local HTTP and gRPC runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServeAllConfig {
    pub data_dir: PathBuf,
    pub http_listen: SocketAddr,
    pub grpc_listen: SocketAddr,
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
        }
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
        config.data_dir,
        http_listener,
        grpc_listener,
        std::future::pending::<()>(),
    )
    .await
}

/// Serves the unified local runtime on pre-bound listeners.
///
/// This is primarily useful for tests and harnesses that need ephemeral ports.
/// HTTP and gRPC share one `DurableBroker` through the existing
/// `Arc<Mutex<DurableBroker>>` adapter boundary.
pub async fn serve_all_with_listeners(
    data_dir: impl Into<PathBuf>,
    http_listener: TcpListener,
    grpc_listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), RuntimeError> {
    let state = open_state(ControlApiConfig::new(data_dir.into()))?;
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
