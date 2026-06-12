use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use msg_control_api::{ControlApiConfig, build_router, open_state};
use msg_data_plane::{DataPlaneConfig, FerrumQDataPlaneServer, open_service};
use msg_observability::init_tracing_from_env;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{info, info_span};

#[derive(Debug, Parser)]
#[command(
    name = "brokerd",
    version,
    about = "FerrumQ local broker control-plane runtime"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the local durable broker control-plane HTTP API.
    Serve {
        /// Root directory for local durable broker state.
        #[arg(long, default_value = "./.ferrumq")]
        data_dir: PathBuf,

        /// Socket address for the HTTP listener.
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: SocketAddr,
    },

    /// Serve the local durable broker data-plane gRPC API.
    ServeGrpc {
        /// Root directory for local durable broker state.
        #[arg(long, default_value = "./.ferrumq")]
        data_dir: PathBuf,

        /// Socket address for the gRPC listener.
        #[arg(long, default_value = "127.0.0.1:9090")]
        listen: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Serve { data_dir, listen }) => {
            init_tracing_from_env()?;
            let span = info_span!("brokerd.serve", operation = "serve", listen = %listen);
            let _guard = span.enter();
            info!(operation = "serve", listen = %listen);
            let state = open_state(ControlApiConfig::new(data_dir))?;
            let listener = TcpListener::bind(listen).await?;
            axum::serve(listener, build_router(state)).await?;
        }
        Some(Command::ServeGrpc { data_dir, listen }) => {
            init_tracing_from_env()?;
            let span = info_span!("brokerd.serve_grpc", operation = "serve_grpc", listen = %listen);
            let _guard = span.enter();
            info!(operation = "serve_grpc", listen = %listen);
            let service = open_service(DataPlaneConfig::new(data_dir))?;
            Server::builder()
                .add_service(FerrumQDataPlaneServer::new(service))
                .serve(listen)
                .await?;
        }
        None => {}
    }

    Ok(())
}
