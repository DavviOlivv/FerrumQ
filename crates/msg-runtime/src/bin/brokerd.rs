use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use msg_control_api::{ControlApiConfig, build_router, open_state};
use tokio::net::TcpListener;

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Serve { data_dir, listen }) => {
            let state = open_state(ControlApiConfig::new(data_dir))?;
            let listener = TcpListener::bind(listen).await?;
            axum::serve(listener, build_router(state)).await?;
        }
        None => {}
    }

    Ok(())
}
