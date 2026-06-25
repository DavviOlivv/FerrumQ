use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use msg_control_api::{ControlApiConfig, build_router, open_state};
use msg_data_plane::{DataPlaneConfig, FerrumQDataPlaneServer, open_service};
use msg_observability::init_tracing_from_env;
#[cfg(feature = "postgres")]
use msg_postgres::models::SearchResult;
use msg_runtime::{ServeAllConfig, serve_all};
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{info, info_span};

#[cfg(feature = "postgres")]
use msg_postgres::{
    PostgresConfig, migrations::run_migrations, models::SearchQuery,
    models::serialize_search_results_json, projection::rebuild_projection,
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

        /// Optional PostgreSQL connection URL enabling the
        /// `POST /v1/search/messages` HTTP endpoint. Falls back to
        /// `FERRUMQ_DATABASE_URL` when unset. Credentials are never logged.
        #[arg(long)]
        postgres_database_url: Option<String>,
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
    /// Search projected message metadata using full-text search.
    Search {
        /// PostgreSQL connection URL.
        #[arg(long)]
        database_url: Option<String>,

        /// Search query string. Must contain at least one alphanumeric
        /// character. Punctuation-only and blank queries are rejected.
        #[arg(long)]
        query: String,

        /// Optional exact topic filter.
        #[arg(long)]
        topic: Option<String>,

        /// Maximum number of results (1..=100).
        #[arg(long, default_value = "20")]
        limit: u32,

        /// Output results as JSON instead of human-readable text.
        #[arg(long)]
        json: bool,
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
            postgres_database_url,
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
            serve_all(
                ServeAllConfig::new(data_dir, http_listen, grpc_listen)
                    .with_postgres_database_url(postgres_database_url),
            )
            .await?;
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
            let repo = PostgresRepository::connect(config).await?;
            run_migrations(repo.pool()).await?;
            info!("migrations complete");
            println!("PostgreSQL migrations complete");
        }
        PostgresSubcommand::Rebuild {
            data_dir,
            database_url,
        } => {
            let config = PostgresConfig::from_env_or_flag(database_url)?;
            let repo = PostgresRepository::connect(config).await?;
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
        PostgresSubcommand::Search {
            database_url,
            query,
            topic,
            limit,
            json,
        } => {
            let search_query = SearchQuery::new(query, topic, limit)?;
            let config = PostgresConfig::from_env_or_flag(database_url)?;
            let repo = PostgresRepository::connect(config).await?;
            run_migrations(repo.pool()).await?;
            let results = repo.search_messages(&search_query).await?;
            log_search_complete(&search_query, results.len(), json);
            print_search_results(&results, json)?;
        }
    }
    Ok(())
}

#[cfg(feature = "postgres")]
fn log_search_complete(search_query: &SearchQuery, result_count: usize, json_output: bool) {
    info!(
        result_count,
        limit = search_query.limit(),
        json_output,
        topic_filter_present = search_query.topic().is_some(),
        "search complete"
    );
}

#[cfg(feature = "postgres")]
fn print_search_results(
    results: &[SearchResult],
    json: bool,
) -> Result<(), msg_postgres::PostgresError> {
    if json {
        let text = serialize_search_results_json(results)?;
        println!("{text}");
        return Ok(());
    }
    if results.is_empty() {
        println!("No results.");
        return Ok(());
    }
    println!("Found {} result(s):", results.len());
    for (index, row) in results.iter().enumerate() {
        println!("\n{}. topic={}", index + 1, row.topic);
        println!("   partition_id={}", row.partition_id);
        println!("   message_offset={}", row.offset);
        println!("   message_id={}", row.message_id);
        println!("   event_type={}", row.event_type);
        println!("   source={}", row.source);
        match &row.subject {
            Some(subject) => println!("   subject={subject}"),
            None => println!("   subject=<none>"),
        }
        println!("   content_type={}", row.content_type);
        println!("   time_unix_ms={}", row.time_unix_ms);
        println!("   payload_len={}", row.payload_len);
        println!("   payload_sha256={}", row.payload_sha256);
        println!("   rank={:.6}", row.rank);
    }
    Ok(())
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    const SENTINEL_QUERY: &str = "super-secret-token-should-not-log";
    const SENTINEL_TOPIC: &str = "sensitive-customer-topic";

    #[derive(Clone, Debug)]
    struct TestWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl TestWriter {
        fn new() -> Self {
            Self {
                buffer: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn output(&self) -> String {
            String::from_utf8(self.buffer.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for TestWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.buffer.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for TestWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn search_query() -> SearchQuery {
        SearchQuery::new(SENTINEL_QUERY, Some(SENTINEL_TOPIC.to_owned()), 25).unwrap()
    }

    fn assert_search_log_is_sanitized(output: &str) {
        assert!(output.contains("search complete"));
        assert!(output.contains("result_count"));
        assert!(output.contains("limit"));
        assert!(output.contains("json_output"));
        assert!(output.contains("topic_filter_present"));
        assert!(!output.contains(SENTINEL_QUERY));
        assert!(!output.contains(SENTINEL_TOPIC));
    }

    #[test]
    fn compact_search_log_does_not_include_query_or_topic() {
        let writer = TestWriter::new();
        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .compact()
            .with_writer(writer.clone())
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_level(false)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log_search_complete(&search_query(), 3, false);
        });

        assert_search_log_is_sanitized(&writer.output());
    }

    #[test]
    fn json_search_log_does_not_include_query_or_topic() {
        let writer = TestWriter::new();
        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .json()
            .with_writer(writer.clone())
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_level(false)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log_search_complete(&search_query(), 3, true);
        });

        assert_search_log_is_sanitized(&writer.output());
    }
}
