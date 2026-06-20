use std::env;

use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Prometheus text exposition content type used by the HTTP control plane.
pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Stable metric names exported by FerrumQ processes.
pub mod metric_names {
    pub const CONTROL_HTTP_REQUESTS_TOTAL: &str = "ferrumq_control_http_requests_total";
    pub const CONTROL_HTTP_ERRORS_TOTAL: &str = "ferrumq_control_http_errors_total";
    pub const CONTROL_TOPICS_CREATED_TOTAL: &str = "ferrumq_control_topics_created_total";

    pub const DATA_RPC_REQUESTS_TOTAL: &str = "ferrumq_data_rpc_requests_total";
    pub const DATA_RPC_ERRORS_TOTAL: &str = "ferrumq_data_rpc_errors_total";
    pub const DATA_PUBLISHES_TOTAL: &str = "ferrumq_data_publishes_total";
    pub const DATA_CONSUMES_TOTAL: &str = "ferrumq_data_consumes_total";
    pub const DATA_MESSAGES_DELIVERED_TOTAL: &str = "ferrumq_data_messages_delivered_total";
    pub const DATA_ACKS_TOTAL: &str = "ferrumq_data_acks_total";
    pub const DATA_NACKS_TOTAL: &str = "ferrumq_data_nacks_total";

    pub const BROKER_OPENS_TOTAL: &str = "ferrumq_broker_opens_total";
    pub const BROKER_RECOVERIES_TOTAL: &str = "ferrumq_broker_recoveries_total";
    pub const BROKER_TOPICS_CREATED_TOTAL: &str = "ferrumq_broker_topics_created_total";
    pub const BROKER_MESSAGES_PUBLISHED_TOTAL: &str = "ferrumq_broker_messages_published_total";
    pub const BROKER_PUBLISH_DEDUPLICATED_TOTAL: &str = "ferrumq_broker_publish_deduplicated_total";
    pub const BROKER_PUBLISH_IDEMPOTENCY_CONFLICTS_TOTAL: &str =
        "ferrumq_broker_publish_idempotency_conflicts_total";
    pub const BROKER_CONSUMES_TOTAL: &str = "ferrumq_broker_consumes_total";
    pub const BROKER_DELIVERIES_CREATED_TOTAL: &str = "ferrumq_broker_deliveries_created_total";
    pub const BROKER_ACKS_TOTAL: &str = "ferrumq_broker_acks_total";
    pub const BROKER_NACKS_TOTAL: &str = "ferrumq_broker_nacks_total";
    pub const BROKER_RETRY_MAINTENANCE_TOTAL: &str = "ferrumq_broker_retry_maintenance_total";
    pub const BROKER_DLQ_TRANSITIONS_TOTAL: &str = "ferrumq_broker_dlq_transitions_total";

    pub const STORAGE_PARTITION_LOG_OPENS_TOTAL: &str = "ferrumq_storage_partition_log_opens_total";
    pub const STORAGE_PARTITION_LOG_RECOVERIES_TOTAL: &str =
        "ferrumq_storage_partition_log_recoveries_total";
    pub const STORAGE_APPENDS_TOTAL: &str = "ferrumq_storage_appends_total";
    pub const STORAGE_TRAILING_REPAIRS_TOTAL: &str = "ferrumq_storage_trailing_repairs_total";
    pub const STORAGE_ERRORS_TOTAL: &str = "ferrumq_storage_errors_total";
}

#[derive(Debug, Clone, Copy)]
struct CounterDescriptor {
    name: &'static str,
    help: &'static str,
}

const COUNTERS: &[CounterDescriptor] = &[
    CounterDescriptor {
        name: metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
        help: "HTTP control-plane requests observed by this process.",
    },
    CounterDescriptor {
        name: metric_names::CONTROL_HTTP_ERRORS_TOTAL,
        help: "HTTP control-plane error responses observed by this process.",
    },
    CounterDescriptor {
        name: metric_names::CONTROL_TOPICS_CREATED_TOTAL,
        help: "Control-plane topic creation attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::DATA_RPC_REQUESTS_TOTAL,
        help: "Data-plane RPC requests observed by this process.",
    },
    CounterDescriptor {
        name: metric_names::DATA_RPC_ERRORS_TOTAL,
        help: "Data-plane RPC error responses observed by this process.",
    },
    CounterDescriptor {
        name: metric_names::DATA_PUBLISHES_TOTAL,
        help: "Data-plane publish attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::DATA_CONSUMES_TOTAL,
        help: "Data-plane consume attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::DATA_MESSAGES_DELIVERED_TOTAL,
        help: "Messages delivered by data-plane consume responses.",
    },
    CounterDescriptor {
        name: metric_names::DATA_ACKS_TOTAL,
        help: "Data-plane ACK attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::DATA_NACKS_TOTAL,
        help: "Data-plane NACK attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_OPENS_TOTAL,
        help: "Durable broker open attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_RECOVERIES_TOTAL,
        help: "Durable broker recovery passes by outcome.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_TOPICS_CREATED_TOTAL,
        help: "Durable broker topic creation attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_MESSAGES_PUBLISHED_TOTAL,
        help: "Durable broker messages actually appended to the log by outcome. \
               Deduplicated retries are not counted here; see \
               ferrumq_broker_publish_deduplicated_total.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_PUBLISH_DEDUPLICATED_TOTAL,
        help: "Durable broker publish requests that were deduplicated as \
               equivalent retries of a prior successful publish.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_PUBLISH_IDEMPOTENCY_CONFLICTS_TOTAL,
        help: "Durable broker publish requests rejected because the \
               idempotency key was reused with conflicting semantic intent.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_CONSUMES_TOTAL,
        help: "Durable broker consume attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_DELIVERIES_CREATED_TOTAL,
        help: "Durable broker delivery records created.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_ACKS_TOTAL,
        help: "Durable broker ACK attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_NACKS_TOTAL,
        help: "Durable broker NACK attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_RETRY_MAINTENANCE_TOTAL,
        help: "Durable broker retry maintenance passes by outcome.",
    },
    CounterDescriptor {
        name: metric_names::BROKER_DLQ_TRANSITIONS_TOTAL,
        help: "Durable broker dead-letter transitions by kind.",
    },
    CounterDescriptor {
        name: metric_names::STORAGE_PARTITION_LOG_OPENS_TOTAL,
        help: "Partition log open attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::STORAGE_PARTITION_LOG_RECOVERIES_TOTAL,
        help: "Partition log recovery passes by outcome.",
    },
    CounterDescriptor {
        name: metric_names::STORAGE_APPENDS_TOTAL,
        help: "Partition log append attempts by outcome.",
    },
    CounterDescriptor {
        name: metric_names::STORAGE_TRAILING_REPAIRS_TOTAL,
        help: "Final trailing record repairs by kind.",
    },
    CounterDescriptor {
        name: metric_names::STORAGE_ERRORS_TOTAL,
        help: "Storage errors by sanitized kind.",
    },
];

/// Initializes structured tracing from environment variables.
///
/// `RUST_LOG` controls filtering. `FERRUMQ_LOG_FORMAT=json` selects JSON logs;
/// an unset value or `compact` uses compact text. Any other value is rejected.
pub fn init_tracing_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let filter = EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| EnvFilter::new("info"));

    match env::var("FERRUMQ_LOG_FORMAT") {
        Ok(value) if value == "compact" => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().compact())
                .try_init()?;
        }
        Ok(value) if value == "json" => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().json())
                .try_init()?;
        }
        Err(env::VarError::NotPresent) => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().compact())
                .try_init()?;
        }
        Ok(value) => return Err(invalid_log_format(&value).into()),
        Err(error) => return Err(error.into()),
    }

    Ok(())
}

fn invalid_log_format(value: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "invalid FERRUMQ_LOG_FORMAT value {value:?}; expected unset, \"compact\", or \"json\""
        ),
    )
}

/// Process-local metrics registry and recording helpers.
pub mod metrics {
    use super::{COUNTERS, CounterDescriptor, metric_names};
    use std::{
        collections::BTreeMap,
        sync::{Mutex, OnceLock},
    };

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct CounterKey {
        name: &'static str,
        labels: Vec<(&'static str, String)>,
    }

    #[derive(Debug, Default)]
    struct Registry {
        counters: Mutex<BTreeMap<CounterKey, u64>>,
    }

    static REGISTRY: OnceLock<Registry> = OnceLock::new();

    /// Renders the current process-local counters in Prometheus text format.
    #[must_use]
    pub fn render_prometheus() -> String {
        let counters = registry()
            .counters
            .lock()
            .expect("metrics registry poisoned");
        let mut output = String::new();

        for descriptor in COUNTERS {
            render_descriptor(&mut output, *descriptor);
            for (key, value) in counters
                .iter()
                .filter(|(key, _value)| key.name == descriptor.name)
            {
                render_sample(&mut output, key, *value);
            }
        }

        output
    }

    /// Returns a counter value for tests and in-process health checks.
    #[must_use]
    pub fn counter_value(name: &str, labels: &[(&str, &str)]) -> u64 {
        let Some(name) = known_counter_name(name) else {
            return 0;
        };
        let Some(labels) = known_labels(labels) else {
            return 0;
        };
        let key = CounterKey { name, labels };
        registry()
            .counters
            .lock()
            .expect("metrics registry poisoned")
            .get(&key)
            .copied()
            .unwrap_or_default()
    }

    #[doc(hidden)]
    pub fn reset_for_tests() {
        registry()
            .counters
            .lock()
            .expect("metrics registry poisoned")
            .clear();
    }

    pub fn record_control_http_request(method: &'static str, route: &'static str, status: u16) {
        increment_counter(
            metric_names::CONTROL_HTTP_REQUESTS_TOTAL,
            vec![
                ("method", method.to_owned()),
                ("route", route.to_owned()),
                ("status", status.to_string()),
            ],
            1,
        );
    }

    pub fn record_control_http_error(
        method: &'static str,
        route: &'static str,
        status: u16,
        code: &'static str,
    ) {
        increment_counter(
            metric_names::CONTROL_HTTP_ERRORS_TOTAL,
            vec![
                ("method", method.to_owned()),
                ("route", route.to_owned()),
                ("status", status.to_string()),
                ("code", code.to_owned()),
            ],
            1,
        );
    }

    pub fn record_control_topic_create(status: &'static str) {
        increment_status_counter(metric_names::CONTROL_TOPICS_CREATED_TOTAL, status);
    }

    pub fn record_data_rpc_request(method: &'static str, status: &'static str) {
        increment_counter(
            metric_names::DATA_RPC_REQUESTS_TOTAL,
            vec![("method", method.to_owned()), ("status", status.to_owned())],
            1,
        );
    }

    pub fn record_data_rpc_error(method: &'static str, code: &'static str) {
        increment_counter(
            metric_names::DATA_RPC_ERRORS_TOTAL,
            vec![("method", method.to_owned()), ("code", code.to_owned())],
            1,
        );
    }

    pub fn record_data_publish(status: &'static str) {
        increment_status_counter(metric_names::DATA_PUBLISHES_TOTAL, status);
    }

    pub fn record_data_consume(status: &'static str) {
        increment_status_counter(metric_names::DATA_CONSUMES_TOTAL, status);
    }

    pub fn record_data_messages_delivered(count: usize) {
        increment_counter(
            metric_names::DATA_MESSAGES_DELIVERED_TOTAL,
            Vec::new(),
            usize_to_u64(count),
        );
    }

    pub fn record_data_ack(status: &'static str) {
        increment_status_counter(metric_names::DATA_ACKS_TOTAL, status);
    }

    pub fn record_data_nack(status: &'static str) {
        increment_status_counter(metric_names::DATA_NACKS_TOTAL, status);
    }

    pub fn record_broker_open(status: &'static str) {
        increment_status_counter(metric_names::BROKER_OPENS_TOTAL, status);
    }

    pub fn record_broker_recovery(status: &'static str) {
        increment_status_counter(metric_names::BROKER_RECOVERIES_TOTAL, status);
    }

    pub fn record_broker_topic_create(status: &'static str) {
        increment_status_counter(metric_names::BROKER_TOPICS_CREATED_TOTAL, status);
    }

    pub fn record_broker_publish(status: &'static str) {
        increment_status_counter(metric_names::BROKER_MESSAGES_PUBLISHED_TOTAL, status);
    }

    /// Records a deduplicated publish retry. This counter is labelless and
    /// increments only when an equivalent retry returns the original publish
    /// identity without appending a new message. It does NOT increment
    /// `ferrumq_broker_messages_published_total`.
    pub fn record_broker_publish_deduplicated() {
        increment_counter(
            metric_names::BROKER_PUBLISH_DEDUPLICATED_TOTAL,
            Vec::new(),
            1,
        );
    }

    /// Records an idempotency key conflict. This counter is labelless and
    /// increments only when a publish is rejected because the idempotency key
    /// was reused with conflicting semantic intent.
    pub fn record_broker_publish_idempotency_conflict() {
        increment_counter(
            metric_names::BROKER_PUBLISH_IDEMPOTENCY_CONFLICTS_TOTAL,
            Vec::new(),
            1,
        );
    }

    pub fn record_broker_consume(status: &'static str) {
        increment_status_counter(metric_names::BROKER_CONSUMES_TOTAL, status);
    }

    pub fn record_broker_deliveries_created(count: usize) {
        increment_counter(
            metric_names::BROKER_DELIVERIES_CREATED_TOTAL,
            Vec::new(),
            usize_to_u64(count),
        );
    }

    pub fn record_broker_ack(status: &'static str) {
        increment_status_counter(metric_names::BROKER_ACKS_TOTAL, status);
    }

    pub fn record_broker_nack(status: &'static str) {
        increment_status_counter(metric_names::BROKER_NACKS_TOTAL, status);
    }

    pub fn record_broker_retry_maintenance(status: &'static str) {
        increment_status_counter(metric_names::BROKER_RETRY_MAINTENANCE_TOTAL, status);
    }

    pub fn record_broker_dlq_transition(kind: &'static str, count: usize) {
        increment_counter(
            metric_names::BROKER_DLQ_TRANSITIONS_TOTAL,
            vec![("kind", kind.to_owned())],
            usize_to_u64(count),
        );
    }

    pub fn record_storage_partition_log_open(status: &'static str) {
        increment_status_counter(metric_names::STORAGE_PARTITION_LOG_OPENS_TOTAL, status);
    }

    pub fn record_storage_partition_log_recovery(status: &'static str) {
        increment_status_counter(metric_names::STORAGE_PARTITION_LOG_RECOVERIES_TOTAL, status);
    }

    pub fn record_storage_append(status: &'static str) {
        increment_status_counter(metric_names::STORAGE_APPENDS_TOTAL, status);
    }

    pub fn record_storage_trailing_repair(kind: &'static str) {
        increment_counter(
            metric_names::STORAGE_TRAILING_REPAIRS_TOTAL,
            vec![("kind", kind.to_owned())],
            1,
        );
    }

    pub fn record_storage_error(kind: &'static str) {
        increment_counter(
            metric_names::STORAGE_ERRORS_TOTAL,
            vec![("kind", kind.to_owned())],
            1,
        );
    }

    fn registry() -> &'static Registry {
        REGISTRY.get_or_init(Registry::default)
    }

    fn increment_status_counter(name: &'static str, status: &'static str) {
        increment_counter(name, vec![("status", status.to_owned())], 1);
    }

    fn increment_counter(name: &'static str, labels: Vec<(&'static str, String)>, value: u64) {
        if value == 0 {
            return;
        }

        debug_assert!(COUNTERS.iter().any(|counter| counter.name == name));
        debug_assert!(labels.iter().all(|(name, _value)| matches!(
            *name,
            "method" | "route" | "status" | "code" | "kind"
        )));

        let key = CounterKey { name, labels };
        let mut counters = registry()
            .counters
            .lock()
            .expect("metrics registry poisoned");
        let counter = counters.entry(key).or_default();
        *counter = counter.saturating_add(value);
    }

    fn render_descriptor(output: &mut String, descriptor: CounterDescriptor) {
        output.push_str("# HELP ");
        output.push_str(descriptor.name);
        output.push(' ');
        output.push_str(descriptor.help);
        output.push('\n');
        output.push_str("# TYPE ");
        output.push_str(descriptor.name);
        output.push_str(" counter\n");
    }

    fn render_sample(output: &mut String, key: &CounterKey, value: u64) {
        output.push_str(key.name);
        if !key.labels.is_empty() {
            output.push('{');
            for (index, (label_name, label_value)) in key.labels.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(label_name);
                output.push_str("=\"");
                escape_label_value(output, label_value);
                output.push('"');
            }
            output.push('}');
        }
        output.push(' ');
        output.push_str(&value.to_string());
        output.push('\n');
    }

    fn escape_label_value(output: &mut String, value: &str) {
        for character in value.chars() {
            match character {
                '\\' => output.push_str("\\\\"),
                '"' => output.push_str("\\\""),
                '\n' => output.push_str("\\n"),
                _ => output.push(character),
            }
        }
    }

    fn known_counter_name(name: &str) -> Option<&'static str> {
        COUNTERS
            .iter()
            .find(|descriptor| descriptor.name == name)
            .map(|descriptor| descriptor.name)
    }

    fn known_labels(labels: &[(&str, &str)]) -> Option<Vec<(&'static str, String)>> {
        labels
            .iter()
            .map(|(name, value)| {
                let label_name = match *name {
                    "method" => "method",
                    "route" => "route",
                    "status" => "status",
                    "code" => "code",
                    "kind" => "kind",
                    _ => return None,
                };
                Some((label_name, (*value).to_owned()))
            })
            .collect()
    }

    fn usize_to_u64(value: usize) -> u64 {
        u64::try_from(value).unwrap_or(u64::MAX)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::{Mutex, OnceLock};

        const ALLOWED_LABELS: &[&str] = &["method", "route", "status", "code", "kind"];
        const PRIVATE_STRINGS: &[&str] = &[
            r#"{"ok":true}"#,
            "payload",
            "idem-1",
            "message-1",
            "delivery-1",
            "consumer-1",
            "/tmp/ferrumq",
            "/home/user/ferrumq",
            "secret",
            "token",
            "password",
            "topic=",
        ];

        fn test_guard() -> std::sync::MutexGuard<'static, ()> {
            static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
            LOCK.get_or_init(|| Mutex::new(()))
                .lock()
                .expect("observability test lock poisoned")
        }

        #[test]
        fn renders_counter_descriptors_and_samples() {
            let _guard = test_guard();
            reset_for_tests();
            record_control_http_request("GET", "/health", 200);

            let output = render_prometheus();

            assert!(output.contains("# HELP ferrumq_control_http_requests_total"));
            assert!(output.contains("# TYPE ferrumq_control_http_requests_total counter"));
            assert!(output.contains(
                "ferrumq_control_http_requests_total{method=\"GET\",route=\"/health\",status=\"200\"} 1"
            ));
        }

        #[test]
        fn renders_descriptors_for_every_documented_counter() {
            let _guard = test_guard();
            reset_for_tests();

            let output = render_prometheus();

            for descriptor in COUNTERS {
                assert!(
                    output.contains(&format!("# HELP {} ", descriptor.name)),
                    "missing HELP for {}",
                    descriptor.name
                );
                assert!(
                    output.contains(&format!("# TYPE {} counter", descriptor.name)),
                    "missing TYPE for {}",
                    descriptor.name
                );
            }
        }

        #[test]
        fn helper_samples_use_only_allowed_label_names() {
            let _guard = test_guard();
            reset_for_tests();
            record_control_http_request("GET", "/metrics", 200);
            record_control_http_error("POST", "/v1/topics", 409, "TOPIC_ALREADY_EXISTS");
            record_control_topic_create("success");
            record_data_rpc_request("Publish", "ok");
            record_data_rpc_error("Publish", "invalid_argument");
            record_data_publish("success");
            record_data_consume("success");
            record_data_messages_delivered(1);
            record_data_ack("success");
            record_data_nack("success");
            record_broker_open("success");
            record_broker_recovery("success");
            record_broker_topic_create("success");
            record_broker_publish("success");
            record_broker_publish_deduplicated();
            record_broker_publish_idempotency_conflict();
            record_broker_consume("success");
            record_broker_deliveries_created(1);
            record_broker_ack("success");
            record_broker_nack("success");
            record_broker_retry_maintenance("success");
            record_broker_dlq_transition("nack", 1);
            record_storage_partition_log_open("success");
            record_storage_partition_log_recovery("success");
            record_storage_append("success");
            record_storage_trailing_repair("checksum");
            record_storage_error("io");

            for line in render_prometheus()
                .lines()
                .filter(|line| !line.starts_with('#'))
            {
                let Some(labels) = line
                    .split_once('{')
                    .and_then(|(_, rest)| rest.split_once('}'))
                else {
                    continue;
                };

                for label in labels.0.split(',') {
                    let name = label.split_once('=').unwrap().0;
                    assert!(
                        ALLOWED_LABELS.contains(&name),
                        "unexpected label {name:?} in {line:?}"
                    );
                }
            }
        }

        #[test]
        fn render_order_is_deterministic() {
            let _guard = test_guard();
            reset_for_tests();
            record_control_http_request("GET", "/ready", 200);
            record_control_http_request("GET", "/health", 200);
            record_data_rpc_request("Publish", "ok");

            let first = render_prometheus();
            let second = render_prometheus();

            assert_eq!(first, second);
        }

        #[test]
        fn escapes_label_values_for_prometheus_text() {
            let _guard = test_guard();
            reset_for_tests();
            increment_counter(
                metric_names::CONTROL_HTTP_ERRORS_TOTAL,
                vec![
                    ("method", "GET".to_owned()),
                    ("route", "/bad\"route\\name\nnext".to_owned()),
                    ("status", "404".to_owned()),
                    ("code", "NOT_FOUND".to_owned()),
                ],
                1,
            );

            let output = render_prometheus();

            assert!(output.contains("route=\"/bad\\\"route\\\\name\\nnext\""));
        }

        #[test]
        fn counter_values_match_labels() {
            let _guard = test_guard();
            reset_for_tests();
            record_data_rpc_request("Publish", "ok");
            record_data_rpc_request("Publish", "ok");
            record_data_rpc_error("Publish", "not_found");

            assert_eq!(
                counter_value(
                    metric_names::DATA_RPC_REQUESTS_TOTAL,
                    &[("method", "Publish"), ("status", "ok")]
                ),
                2
            );
            assert_eq!(
                counter_value(
                    metric_names::DATA_RPC_ERRORS_TOTAL,
                    &[("method", "Publish"), ("code", "not_found")]
                ),
                1
            );
        }

        #[test]
        fn counters_are_monotonic_and_zero_increments_are_noops() {
            let _guard = test_guard();
            reset_for_tests();
            increment_counter(metric_names::DATA_MESSAGES_DELIVERED_TOTAL, Vec::new(), 0);
            assert_eq!(
                counter_value(metric_names::DATA_MESSAGES_DELIVERED_TOTAL, &[]),
                0
            );

            record_data_messages_delivered(2);
            record_data_messages_delivered(3);

            assert_eq!(
                counter_value(metric_names::DATA_MESSAGES_DELIVERED_TOTAL, &[]),
                5
            );
        }

        #[test]
        fn unknown_counter_or_label_lookup_returns_zero() {
            let _guard = test_guard();
            reset_for_tests();
            record_data_publish("success");

            assert_eq!(counter_value("ferrumq_unknown_total", &[]), 0);
            assert_eq!(
                counter_value(metric_names::DATA_PUBLISHES_TOTAL, &[("topic", "orders")]),
                0
            );
        }

        #[test]
        fn reset_for_tests_clears_registry_for_exact_assertions() {
            let _guard = test_guard();
            reset_for_tests();
            record_control_topic_create("success");
            assert_eq!(
                counter_value(
                    metric_names::CONTROL_TOPICS_CREATED_TOTAL,
                    &[("status", "success")]
                ),
                1
            );

            reset_for_tests();

            assert_eq!(
                counter_value(
                    metric_names::CONTROL_TOPICS_CREATED_TOTAL,
                    &[("status", "success")]
                ),
                0
            );
        }

        #[test]
        fn helpers_do_not_emit_topic_or_payload_labels() {
            let _guard = test_guard();
            reset_for_tests();
            record_data_publish("success");
            record_data_messages_delivered(2);
            record_broker_dlq_transition("nack", 1);
            record_broker_publish_deduplicated();
            record_broker_publish_idempotency_conflict();
            record_storage_error("io");

            let output = render_prometheus();

            assert!(!output.contains("topic="));
            assert!(!output.contains("message="));
            assert!(!output.contains("delivery="));
            assert!(!output.contains(r#"{"ok":true}"#));
        }

        #[test]
        fn idempotency_counters_are_label_free() {
            let _guard = test_guard();
            reset_for_tests();
            record_broker_publish_deduplicated();
            record_broker_publish_idempotency_conflict();

            let output = render_prometheus();
            assert!(output.contains("ferrumq_broker_publish_deduplicated_total 1"));
            assert!(output.contains("ferrumq_broker_publish_idempotency_conflicts_total 1"));
            assert!(!output.contains("ferrumq_broker_publish_deduplicated_total{"));
            assert!(!output.contains("ferrumq_broker_publish_idempotency_conflicts_total{"));
        }

        #[test]
        fn metrics_do_not_render_private_payload_or_identifier_strings() {
            let _guard = test_guard();
            reset_for_tests();
            record_control_http_request("GET", "/metrics", 200);
            record_data_rpc_request("Publish", "ok");
            record_broker_dlq_transition("nack", 1);
            record_broker_publish_deduplicated();
            record_broker_publish_idempotency_conflict();
            record_storage_error("io");

            let output = render_prometheus();

            for private in PRIVATE_STRINGS {
                assert!(
                    !output.contains(private),
                    "metrics output leaked private string {private:?}"
                );
            }
        }
    }
}

/// Returns this crate's package name.
#[must_use]
pub fn crate_name() -> &'static str {
    "msg-observability"
}
