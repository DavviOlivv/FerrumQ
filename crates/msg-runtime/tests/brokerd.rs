use std::{
    io::ErrorKind,
    net::SocketAddr,
    process::{Command, Output, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

use msg_observability::{metric_names, metrics};
use msg_protocol::ferrumq::dataplane::v1::{
    AckRequest, ConsumeRequest, NackRequest, PublishRequest,
    ferrum_q_data_plane_client::FerrumQDataPlaneClient,
};
use serde_json::{Value, json};
use tempfile::{NamedTempFile, TempDir};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, oneshot},
};

fn brokerd() -> Command {
    Command::new(env!("CARGO_BIN_EXE_brokerd"))
}

async fn metrics_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

fn publish_request(message_id: &str) -> PublishRequest {
    PublishRequest {
        topic: "orders".to_owned(),
        message_id: message_id.to_owned(),
        key: "account-1".to_owned(),
        payload: br#"{"ok":true}"#.to_vec(),
        content_type: "application/json".to_owned(),
        r#type: "order.created".to_owned(),
        source: "/runtime-test".to_owned(),
        subject: "subject-1".to_owned(),
        idempotency_key: String::new(),
        time_unix_ms: 1_700_000_000_000,
    }
}

fn consume_request(now_unix_ms: u64) -> ConsumeRequest {
    ConsumeRequest {
        topic: "orders".to_owned(),
        consumer_group: "group.1".to_owned(),
        consumer_id: "consumer-1".to_owned(),
        max_messages: 10,
        lease_ms: 1_000,
        now_unix_ms,
    }
}

async fn http_json(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (u16, String) {
    let body = body.map_or_else(String::new, |body| body.to_string());
    let content_headers = if body.is_empty() {
        String::new()
    } else {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        )
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n{content_headers}\r\n{body}"
    );
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    parse_http_response(&response)
}

fn parse_http_response(response: &[u8]) -> (u16, String) {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP response should include headers");
    let headers = std::str::from_utf8(&response[..header_end]).unwrap();
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .expect("HTTP response should include numeric status");
    let body = String::from_utf8(response[header_end + 4..].to_vec()).unwrap();
    (status, body)
}

fn reserve_port() -> std::net::TcpListener {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap()
}

fn loopback_bind_available() -> bool {
    match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => {
            drop(listener);
            true
        }
        Err(error) if error.kind() == ErrorKind::PermissionDenied => false,
        Err(error) => panic!("failed to check loopback bind availability: {error}"),
    }
}

fn output_with_timeout(mut command: Command, timeout: Duration) -> Output {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + timeout;

    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }

        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("command did not exit within {timeout:?}");
        }

        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn version_command_succeeds() {
    let output = brokerd().arg("--version").output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("brokerd"));
}

#[test]
fn version_command_ignores_invalid_log_format() {
    let output = brokerd()
        .arg("--version")
        .env("FERRUMQ_LOG_FORMAT", "xml")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("brokerd"));
}

#[test]
fn serve_help_documents_defaults() {
    let output = brokerd().args(["serve", "--help"]).output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--data-dir"));
    assert!(stdout.contains("./.ferrumq"));
    assert!(stdout.contains("--listen"));
    assert!(stdout.contains("127.0.0.1:8080"));
}

#[test]
fn serve_grpc_help_documents_defaults() {
    let output = brokerd().args(["serve-grpc", "--help"]).output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--data-dir"));
    assert!(stdout.contains("./.ferrumq"));
    assert!(stdout.contains("--listen"));
    assert!(stdout.contains("127.0.0.1:9090"));
}

#[test]
fn serve_all_help_documents_defaults() {
    let output = brokerd().args(["serve-all", "--help"]).output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--data-dir"));
    assert!(stdout.contains("./.ferrumq"));
    assert!(stdout.contains("--http-listen"));
    assert!(stdout.contains("127.0.0.1:8080"));
    assert!(stdout.contains("--grpc-listen"));
    assert!(stdout.contains("127.0.0.1:9090"));
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_help_documents_migrate_and_rebuild() {
    let output = brokerd().args(["postgres", "--help"]).output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("migrate"));
    assert!(stdout.contains("rebuild"));
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_subcommand_help_documents_database_and_data_options() {
    let migrate = brokerd()
        .args(["postgres", "migrate", "--help"])
        .output()
        .unwrap();
    assert!(migrate.status.success());
    let migrate_stdout = String::from_utf8(migrate.stdout).unwrap();
    assert!(migrate_stdout.contains("--database-url"));

    let rebuild = brokerd()
        .args(["postgres", "rebuild", "--help"])
        .output()
        .unwrap();
    assert!(rebuild.status.success());
    let rebuild_stdout = String::from_utf8(rebuild.stdout).unwrap();
    assert!(rebuild_stdout.contains("--database-url"));
    assert!(rebuild_stdout.contains("--data-dir"));
    assert!(rebuild_stdout.contains("./.ferrumq"));
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_commands_require_a_database_url() {
    let output = brokerd()
        .args(["postgres", "migrate"])
        .env_remove("FERRUMQ_DATABASE_URL")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("missing database URL"));
    assert!(stderr.contains("FERRUMQ_DATABASE_URL"));
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_commands_reject_invalid_urls_and_flag_precedes_environment() {
    let output = brokerd()
        .args([
            "postgres",
            "migrate",
            "--database-url",
            "not-a-postgres-url",
        ])
        .env(
            "FERRUMQ_DATABASE_URL",
            "postgres://user:environment-secret@127.0.0.1:1/postgres",
        )
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid database URL"));
    assert!(!stderr.contains("environment-secret"));
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_connection_errors_do_not_expose_credentials_or_query_parameters() {
    let database_url = "postgres://user:database-secret@127.0.0.1:1/postgres?connect_timeout=1&password=query-secret";
    let output = brokerd()
        .args(["postgres", "migrate", "--database-url", database_url])
        .env("RUST_LOG", "info")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("database connection failed"));
    assert!(!stderr.contains("database-secret"));
    assert!(!stderr.contains("query-secret"));
    assert!(!stderr.contains(database_url));
}

#[test]
fn invalid_listen_address_fails_cleanly() {
    let output = brokerd()
        .args(["serve", "--listen", "not-a-socket-address"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid value"));
    assert!(stderr.contains("--listen"));
}

#[test]
fn invalid_serve_all_http_listen_address_fails_cleanly() {
    let output = brokerd()
        .args(["serve-all", "--http-listen", "not-a-socket-address"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid value"));
    assert!(stderr.contains("--http-listen"));
}

#[test]
fn invalid_serve_all_grpc_listen_address_fails_cleanly() {
    let output = brokerd()
        .args(["serve-all", "--grpc-listen", "not-a-socket-address"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid value"));
    assert!(stderr.contains("--grpc-listen"));
}

#[test]
fn serve_rejects_invalid_log_format() {
    let output = brokerd()
        .args(["serve", "--listen", "127.0.0.1:0"])
        .env("FERRUMQ_LOG_FORMAT", "xml")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid FERRUMQ_LOG_FORMAT value"));
    assert!(stderr.contains("compact"));
    assert!(stderr.contains("json"));
}

#[test]
fn serve_all_rejects_invalid_log_format() {
    let output = brokerd()
        .args([
            "serve-all",
            "--http-listen",
            "127.0.0.1:0",
            "--grpc-listen",
            "127.0.0.1:0",
        ])
        .env("FERRUMQ_LOG_FORMAT", "xml")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid FERRUMQ_LOG_FORMAT value"));
    assert!(stderr.contains("compact"));
    assert!(stderr.contains("json"));
}

#[test]
fn serve_accepts_compact_and_json_log_formats() {
    for format in ["compact", "json"] {
        let data_dir = NamedTempFile::new().unwrap();
        let output = brokerd()
            .args(["serve", "--data-dir"])
            .arg(data_dir.path())
            .args(["--listen", "127.0.0.1:0"])
            .env("FERRUMQ_LOG_FORMAT", format)
            .output()
            .unwrap();

        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains("OpenState"),
            "expected accepted format {format:?} to reach state opening, got {stderr:?}"
        );
        assert!(!stderr.contains("invalid FERRUMQ_LOG_FORMAT value"));
    }
}

#[test]
fn invalid_grpc_listen_address_fails_cleanly() {
    let output = brokerd()
        .args(["serve-grpc", "--listen", "not-a-socket-address"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid value"));
    assert!(stderr.contains("--listen"));
}

#[test]
fn serve_grpc_rejects_invalid_log_format() {
    let output = brokerd()
        .args(["serve-grpc", "--listen", "127.0.0.1:0"])
        .env("FERRUMQ_LOG_FORMAT", "xml")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid FERRUMQ_LOG_FORMAT value"));
    assert!(stderr.contains("compact"));
    assert!(stderr.contains("json"));
}

#[test]
fn serve_grpc_accepts_compact_and_json_log_formats() {
    for format in ["compact", "json"] {
        let data_dir = NamedTempFile::new().unwrap();
        let output = brokerd()
            .args(["serve-grpc", "--data-dir"])
            .arg(data_dir.path())
            .args(["--listen", "127.0.0.1:0"])
            .env("FERRUMQ_LOG_FORMAT", format)
            .output()
            .unwrap();

        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains("OpenState"),
            "expected accepted format {format:?} to reach state opening, got {stderr:?}"
        );
        assert!(!stderr.contains("invalid FERRUMQ_LOG_FORMAT value"));
    }
}

#[test]
fn serve_grpc_invalid_data_dir_file_fails_cleanly() {
    let data_dir = NamedTempFile::new().unwrap();
    let output = brokerd()
        .args(["serve-grpc", "--data-dir"])
        .arg(data_dir.path())
        .args(["--listen", "127.0.0.1:0"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("OpenState"));
    assert!(stderr.contains("AlreadyExists"));
}

#[test]
fn serve_all_invalid_data_dir_file_fails_cleanly() {
    if !loopback_bind_available() {
        return;
    }

    let data_dir = NamedTempFile::new().unwrap();
    let output = brokerd()
        .args(["serve-all", "--data-dir"])
        .arg(data_dir.path())
        .args([
            "--http-listen",
            "127.0.0.1:0",
            "--grpc-listen",
            "127.0.0.1:0",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("OpenState"));
    assert!(stderr.contains("AlreadyExists"));
}

#[test]
fn serve_all_http_bind_failure_fails_cleanly() {
    if !loopback_bind_available() {
        return;
    }

    let data_dir = TempDir::new().unwrap();
    let reserved = reserve_port();
    let addr = reserved.local_addr().unwrap().to_string();
    let mut command = brokerd();
    command
        .args(["serve-all", "--data-dir"])
        .arg(data_dir.path())
        .args(["--http-listen", &addr, "--grpc-listen", "127.0.0.1:0"]);
    let output = output_with_timeout(command, Duration::from_secs(5));

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("BindHttp"));
    assert!(stderr.contains(&addr));
}

#[test]
fn serve_all_grpc_bind_failure_fails_cleanly() {
    if !loopback_bind_available() {
        return;
    }

    let data_dir = TempDir::new().unwrap();
    let reserved = reserve_port();
    let addr = reserved.local_addr().unwrap().to_string();
    let mut command = brokerd();
    command
        .args(["serve-all", "--data-dir"])
        .arg(data_dir.path())
        .args(["--http-listen", "127.0.0.1:0", "--grpc-listen", &addr]);
    let output = output_with_timeout(command, Duration::from_secs(5));

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("BindGrpc"));
    assert!(stderr.contains(&addr));
}

#[tokio::test]
async fn serve_all_shares_http_grpc_state_and_metrics() {
    if !loopback_bind_available() {
        return;
    }

    let _guard = metrics_test_guard().await;
    metrics::reset_for_tests();
    let data_dir = TempDir::new().unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_data_dir = data_dir.path().to_path_buf();

    let server = tokio::spawn(async move {
        msg_runtime::serve_all_with_listeners(
            server_data_dir,
            http_listener,
            grpc_listener,
            async {
                let _ = shutdown_rx.await;
            },
        )
        .await
    });

    let (status, body) = http_json(
        http_addr,
        "POST",
        "/v1/topics",
        Some(json!({ "name": "orders", "partitions": 1 })),
    )
    .await;
    assert_eq!(status, 201);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({ "name": "orders", "partitions": 1 })
    );

    let mut client = FerrumQDataPlaneClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    let published = client
        .publish(publish_request("message-1"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(published.topic, "orders");
    assert_eq!(published.offset, 0);

    let consumed = client
        .consume(consume_request(10))
        .await
        .unwrap()
        .into_inner()
        .messages;
    assert_eq!(consumed.len(), 1);
    assert_eq!(consumed[0].message_id, "message-1");

    client
        .ack(AckRequest {
            delivery_id: consumed[0].delivery_id.clone(),
            consumer_id: "consumer-1".to_owned(),
        })
        .await
        .unwrap();

    let (status, body) = http_json(http_addr, "GET", "/v1/status", None).await;
    assert_eq!(status, 200);
    let status_body = serde_json::from_str::<Value>(&body).unwrap();
    assert_eq!(status_body["topics"], 1);
    assert_eq!(status_body["dlqEntries"], 0);

    client.publish(publish_request("message-2")).await.unwrap();
    let first_attempt = client
        .consume(consume_request(20))
        .await
        .unwrap()
        .into_inner()
        .messages;
    assert_eq!(first_attempt.len(), 1);
    assert_eq!(first_attempt[0].message_id, "message-2");
    client
        .nack(NackRequest {
            delivery_id: first_attempt[0].delivery_id.clone(),
            consumer_id: "consumer-1".to_owned(),
            reason: "transient".to_owned(),
        })
        .await
        .unwrap();

    let second_attempt = client
        .consume(consume_request(4_000_000_000_000))
        .await
        .unwrap()
        .into_inner()
        .messages;
    assert_eq!(second_attempt.len(), 1);
    assert_eq!(second_attempt[0].message_id, "message-2");
    assert_eq!(second_attempt[0].attempt_number, 2);
    client
        .nack(NackRequest {
            delivery_id: second_attempt[0].delivery_id.clone(),
            consumer_id: "consumer-1".to_owned(),
            reason: "still failing".to_owned(),
        })
        .await
        .unwrap();

    let third_attempt = client
        .consume(consume_request(4_000_000_010_000))
        .await
        .unwrap()
        .into_inner()
        .messages;
    assert_eq!(third_attempt.len(), 1);
    assert_eq!(third_attempt[0].message_id, "message-2");
    assert_eq!(third_attempt[0].attempt_number, 3);
    client
        .nack(NackRequest {
            delivery_id: third_attempt[0].delivery_id.clone(),
            consumer_id: "consumer-1".to_owned(),
            reason: "poison".to_owned(),
        })
        .await
        .unwrap();

    let (status, body) = http_json(http_addr, "GET", "/v1/dlq", None).await;
    assert_eq!(status, 200);
    let dlq_body = serde_json::from_str::<Value>(&body).unwrap();
    assert_eq!(dlq_body["items"].as_array().unwrap().len(), 1);
    assert_eq!(dlq_body["items"][0]["messageId"], "message-2");
    assert_eq!(dlq_body["items"][0]["reason"], "poison");
    assert_eq!(dlq_body["items"][0]["attemptCount"], 3);

    let (status, body) = http_json(http_addr, "GET", "/v1/status", None).await;
    assert_eq!(status, 200);
    let status_body = serde_json::from_str::<Value>(&body).unwrap();
    assert_eq!(status_body["topics"], 1);
    assert_eq!(status_body["dlqEntries"], 1);

    let (status, metrics_body) = http_json(http_addr, "GET", "/metrics", None).await;
    assert_eq!(status, 200);
    assert!(metrics_body.contains("ferrumq_control_topics_created_total{status=\"success\"} 1"));
    assert!(metrics_body.contains("ferrumq_data_publishes_total{status=\"success\"} 2"));
    assert!(metrics_body.contains("ferrumq_data_consumes_total{status=\"success\"} 4"));
    assert!(metrics_body.contains("ferrumq_data_acks_total{status=\"success\"} 1"));
    assert!(metrics_body.contains("ferrumq_data_nacks_total{status=\"success\"} 3"));
    assert!(metrics_body.contains("ferrumq_broker_dlq_transitions_total{kind=\"nack\"} 1"));
    assert_eq!(
        metrics::counter_value(
            metric_names::CONTROL_TOPICS_CREATED_TOTAL,
            &[("status", "success")]
        ),
        1
    );

    drop(client);
    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}
