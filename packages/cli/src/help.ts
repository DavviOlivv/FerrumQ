import { cliVersion } from "./config.js";

export function rootHelpText(): string {
  return [
    "FerrumQ message broker CLI",
    "",
    "Usage:",
    "  ferrumq [--control-url <url>] [--grpc-url <url>] [--json] <command>",
    "  ferrumq --version",
    "  ferrumq --help",
    "",
    "Control-plane commands:",
    "  ferrumq health",
    "  ferrumq ready",
    "  ferrumq status",
    "  ferrumq topic create <topic> [--partitions <count>]",
    "  ferrumq topic get <topic>",
    "  ferrumq topic list",
    "  ferrumq dlq list [--topic <topic>]",
    "  ferrumq search <query> [--topic <topic>] [--limit <count>]",
    "",
    "Data-plane commands:",
    "  ferrumq publish <topic> --data <string> [--key <key>]",
    "  ferrumq consume <topic> --group <group> [--consumer-id ferrumq-cli] [--max 1] [--lease-ms 30000]",
    "  ferrumq ack <delivery-id> [--consumer-id ferrumq-cli]",
    "  ferrumq nack <delivery-id> [--consumer-id ferrumq-cli] [--reason <text>]",
    "",
    "Broker commands:",
    "  ferrumq broker version",
    "  ferrumq broker --help",
    "",
    "Compatibility:",
    "  msg is an alias for ferrumq.",
  ].join("\n");
}

export function brokerHelpText(): string {
  return [
    "FerrumQ broker commands",
    "",
    "Usage:",
    "  ferrumq broker version",
    "  ferrumq broker --help",
    "",
    "Process management is deferred. Start brokerd directly for control-plane or data-plane serving.",
  ].join("\n");
}

export function topicHelpText(): string {
  return [
    "FerrumQ topic commands",
    "",
    "Usage:",
    "  ferrumq topic create <topic> [--partitions <count>]",
    "  ferrumq topic get <topic>",
    "  ferrumq topic list",
    "  ferrumq topic --help",
    "",
    "Topic commands use the HTTP control plane.",
  ].join("\n");
}

export function dlqHelpText(): string {
  return [
    "FerrumQ DLQ commands",
    "",
    "Usage:",
    "  ferrumq dlq list [--topic <topic>]",
    "  ferrumq dlq --help",
    "",
    "DLQ inspection uses the HTTP control plane.",
  ].join("\n");
}

export function publishHelpText(): string {
  return [
    "FerrumQ publish command",
    "",
    "Usage:",
    "  ferrumq publish <topic> --data <string> [--key <key>]",
    "  ferrumq publish <topic> --data <string> [--message-id <id>]",
    "  ferrumq publish <topic> --data <string> [--content-type <type>] [--type <type>] [--source <source>] [--subject <subject>] [--idempotency-key <key>]",
    "  ferrumq publish --help",
    "",
    "--idempotency-key is scoped per topic. A retry with the same key and",
    "equivalent content returns the original publish result without appending",
    "another message. Conflicting reuse (same key, different content) fails.",
    "",
    "Publish uses the unary gRPC data plane.",
  ].join("\n");
}

export function consumeHelpText(): string {
  return [
    "FerrumQ consume command",
    "",
    "Usage:",
    "  ferrumq consume <topic> --group <group> [--consumer-id ferrumq-cli] [--max 1] [--lease-ms 30000]",
    "  ferrumq consume --help",
    "",
    "Consume uses the unary gRPC data plane.",
  ].join("\n");
}

export function ackHelpText(): string {
  return [
    "FerrumQ ACK command",
    "",
    "Usage:",
    "  ferrumq ack <delivery-id> [--consumer-id ferrumq-cli]",
    "  ferrumq ack --help",
    "",
    "ACK uses the unary gRPC data plane.",
  ].join("\n");
}

export function nackHelpText(): string {
  return [
    "FerrumQ NACK command",
    "",
    "Usage:",
    "  ferrumq nack <delivery-id> [--consumer-id ferrumq-cli] [--reason <text>]",
    "  ferrumq nack --help",
    "",
    "NACK uses the unary gRPC data plane.",
  ].join("\n");
}

export function searchHelpText(): string {
  return [
    "FerrumQ search command",
    "",
    "Usage:",
    "  ferrumq search <query> [--topic <topic>] [--limit <count>]",
    "  ferrumq search --help",
    "",
    "Search returns up to <limit> (default 20, max 100) projected message rows",
    "matching <query>. HTTP search sends the query in a POST JSON body, so raw",
    "query text is not placed in HTTP URLs, access logs, proxies, or HTTP",
    "client logs. FerrumQ logs and traces do not persist the raw query.",
    "`ferrumq search <query>` still receives the query as a CLI argument, so",
    "it may appear in shell history and process argv. Avoid secrets as CLI",
    "search queries when local shell history or process visibility matters.",
    "Search requires the local broker to be started with --postgres-database-url",
    "or FERRUMQ_DATABASE_URL; otherwise the endpoint returns SEARCH_UNAVAILABLE.",
    "",
    "Search uses the HTTP control plane.",
  ].join("\n");
}

export function versionText(): string {
  return cliVersion;
}
