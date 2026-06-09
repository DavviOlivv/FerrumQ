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

export function versionText(): string {
  return cliVersion;
}
