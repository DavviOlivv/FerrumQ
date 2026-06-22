use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use msg_control_api::{ControlApiConfig, build_router, open_state};
use msg_data_plane::{DataPlaneConfig, FerrumQDataPlaneServer, open_service};
use msg_observability::init_tracing_from_env;
use msg_runtime::{ServeAllConfig, serve_all};
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{info, info_span};

#[cfg(feature = "postgres")]
use msg_postgres::{
    PostgresConfig, migrations::run_migrations, projection::rebuild_projection,
    repository::PostgresRepository,
};

#[derive(Debug, Parser)]
#[command(name = "brokerd", version, about = "FerrumQ local broker runtime")]
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

    /// Serve the local durable broker HTTP and gRPC APIs in one process.
    ServeAll {
        /// Root directory for local durable broker state.
        #[arg(long, default_value = "./.ferrumq")]
        data_dir: PathBuf,

        /// Socket address for the HTTP listener.
        #[arg(long, default_value = "127.0.0.1:8080")]
        http_listen: SocketAddr,

        /// Socket address for the gRPC listener.
        #[arg(long, default_value = "127.0.0.1:9090")]
        grpc_listen: SocketAddr,
    },

    /// Manage the PostgreSQL metadata projection store.
    #[cfg(feature = "postgres")]
    #[command(subcommand)]
    Postgres(PostgresSubcommand),
}

#[cfg(feature = "postgres")]
#[derive(Debug, Subcommand)]
enum PostgresSubcommand {
    /// Run database migrations.
    Migrate {
        /// PostgreSQL connection URL.
        #[arg(long)]
        database_url: Option<String>,
    },
    /// Rebuild the PostgreSQL metadata projection from the durable message log.
    Rebuild {
        /// Root directory for local durable broker state.
        #[arg(long, default_value = "./.ferrumq")]
        data_dir: PathBuf,

        /// PostgreSQL connection URL.
        #[arg(long)]
        database_url: Option<String>,
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
        Some(Command::ServeAll {
            data_dir,
            http_listen,
            grpc_listen,
        }) => {
            init_tracing_from_env()?;
            let span = info_span!(
                "brokerd.serve_all",
                operation = "serve_all",
                http_listen = %http_listen,
                grpc_listen = %grpc_listen
            );
            let _guard = span.enter();
            info!(
                operation = "serve_all",
                http_listen = %http_listen,
                grpc_listen = %grpc_listen
            );
            serve_all(ServeAllConfig::new(data_dir, http_listen, grpc_listen)).await?;
        }
        #[cfg(feature = "postgres")]
        Some(Command::Postgres(sub)) => {
            init_tracing_from_env()?;
            if let Err(error) = handle_postgres(sub).await {
                return Err(std::io::Error::other(error.to_string()).into());
            }
        }
        None => {}
    }

    Ok(())
}

#[cfg(feature = "postgres")]
async fn handle_postgres(sub: PostgresSubcommand) -> Result<(), msg_postgres::PostgresError> {
    match sub {
        PostgresSubcommand::Migrate { database_url } => {
            let config = PostgresConfig::from_env_or_flag(database_url)?;
            let repo = PostgresRepository::connect(&config).await?;
            run_migrations(repo.pool()).await?;
            info!("migrations complete");
            println!("PostgreSQL migrations complete");
        }
        PostgresSubcommand::Rebuild {
            data_dir,
            database_url,
        } => {
            let config = PostgresConfig::from_env_or_flag(database_url)?;
            let repo = PostgresRepository::connect(&config).await?;
            run_migrations(repo.pool()).await?;
            let result = rebuild_projection(&repo, &data_dir).await?;
            info!(
                topics = result.topics_count,
                messages = result.messages_count,
                "projection rebuild complete"
            );
            println!(
                "Projection rebuild complete: {} topics, {} messages",
                result.topics_count, result.messages_count
            );
        }
    }
    Ok(())
}
