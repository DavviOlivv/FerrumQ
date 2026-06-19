export interface ChatConfig {
  name: string;
  room: string;
  httpUrl: string;
  grpcUrl: string;
  timeoutMs: number;
  pollIntervalMs: number;
}

export interface ChatEnvironment {
  FERRUMQ_HTTP_URL?: string;
  FERRUMQ_GRPC_URL?: string;
}

const DEFAULT_HTTP_URL = "http://127.0.0.1:8080";
const DEFAULT_GRPC_URL = "http://127.0.0.1:9090";
export const DEFAULT_TIMEOUT_MS = 10_000;
export const DEFAULT_POLL_INTERVAL_MS = 500;
const MAX_SAFE_TIMER_MS = 2_147_483_647;

export function parseChatArgs(
  args: string[],
  env: ChatEnvironment = {},
):
  | { config: ChatConfig }
  | { help: string }
  | { version: string }
  | { error: string; exitCode: number } {
  let name: string | undefined;
  let room: string | undefined;
  let httpUrl: string | undefined;
  let grpcUrl: string | undefined;
  let timeoutMs: number | undefined;
  let pollIntervalMs: number | undefined;
  let wantHelp = false;
  let wantVersion = false;
  const errors: string[] = [];
  const seen = new Set<string>();

  for (let i = 0; i < args.length; i++) {
    const arg = args[i] ?? "";
    const canonical =
      arg === "-h" ? "--help" : arg === "-V" ? "--version" : undefined;
    if (canonical !== undefined) {
      if (seen.has(canonical)) {
        errors.push(`duplicate option: ${arg}`);
      } else {
        seen.add(canonical);
        if (canonical === "--help") wantHelp = true;
        else wantVersion = true;
      }
      continue;
    }

    if (!arg.startsWith("--")) {
      errors.push(`unknown option: ${arg}`);
      continue;
    }

    const equalsIndex = arg.indexOf("=");
    const flag = equalsIndex === -1 ? arg : arg.slice(0, equalsIndex);
    const isValueFlag = VALUE_FLAGS.has(flag);

    if (flag === "--help" || flag === "--version") {
      if (equalsIndex !== -1) {
        errors.push(`unknown option: ${arg}`);
        continue;
      }
      if (seen.has(flag)) {
        errors.push(`duplicate option: ${arg}`);
      } else {
        seen.add(flag);
        if (flag === "--help") wantHelp = true;
        else wantVersion = true;
      }
      continue;
    }

    if (!isValueFlag) {
      errors.push(`unknown option: ${arg}`);
      continue;
    }

    let raw: string | undefined;
    if (equalsIndex === -1) {
      const candidate = args[i + 1];
      if (candidate !== undefined && !candidate.startsWith("-")) {
        raw = candidate;
        i++;
      }
    } else {
      raw = arg.slice(equalsIndex + 1);
      if (raw.startsWith("=")) {
        errors.push(`invalid option syntax: ${arg}`);
        continue;
      }
    }

    if (seen.has(flag)) {
      errors.push(`duplicate option: ${arg}`);
      continue;
    }
    seen.add(flag);

    if (raw === undefined || raw.length === 0) {
      errors.push(`${flag} requires a value`);
      continue;
    }

    if (flag === "--name") {
      name = raw;
    } else if (flag === "--room") {
      room = raw;
    } else if (flag === "--http-url") {
      httpUrl = raw;
    } else if (flag === "--grpc-url") {
      grpcUrl = raw;
    } else if (flag === "--timeout-ms") {
      const parsed = parseUnsignedInteger(raw);
      if (parsed === null || parsed > MAX_SAFE_TIMER_MS) {
        errors.push("--timeout-ms must be a non-negative integer");
      } else {
        timeoutMs = parsed;
      }
    } else {
      const parsed = parseUnsignedInteger(raw);
      if (parsed === null || parsed <= 0 || parsed > MAX_SAFE_TIMER_MS) {
        errors.push("--poll-interval-ms must be a positive integer");
      } else {
        pollIntervalMs = parsed;
      }
    }
  }

  if (wantHelp && wantVersion) {
    return { error: "--help and --version cannot be combined", exitCode: 1 };
  }

  if (errors.length > 0) {
    return {
      error: `${errors.join("\n")}\n\n${buildHelp()}`,
      exitCode: 1,
    };
  }

  if (wantHelp) {
    return { help: buildHelp() };
  }

  if (wantVersion) {
    return { version: "ferrumq-chat 0.1.0" };
  }

  if (name === undefined || name.length === 0) {
    errors.push("--name is required");
  }

  if (room === undefined || room.length === 0) {
    errors.push("--room is required");
  }

  if (errors.length > 0) {
    return {
      error: `${errors.join("\n")}\n\n${buildHelp()}`,
      exitCode: 1,
    };
  }

  return {
    config: {
      name: name ?? "",
      room: room ?? "",
      httpUrl: httpUrl ?? env.FERRUMQ_HTTP_URL ?? DEFAULT_HTTP_URL,
      grpcUrl: grpcUrl ?? env.FERRUMQ_GRPC_URL ?? DEFAULT_GRPC_URL,
      timeoutMs: timeoutMs ?? DEFAULT_TIMEOUT_MS,
      pollIntervalMs: pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
    },
  };
}

const VALUE_FLAGS = new Set([
  "--name",
  "--room",
  "--http-url",
  "--grpc-url",
  "--timeout-ms",
  "--poll-interval-ms",
]);

function parseUnsignedInteger(raw: string): number | null {
  if (!/^[0-9]+$/.test(raw)) {
    return null;
  }

  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function buildHelp(): string {
  return [
    "ferrumq-chat - Terminal chat over FerrumQ",
    "",
    "Usage:",
    "  ferrumq-chat --name <name> --room <room> [options]",
    "",
    "Required:",
    "  --name <name>          Display name (alphanumeric, dots, hyphens, underscores)",
    "  --room <room>          Room name (alphanumeric, dots, hyphens, underscores)",
    "",
    "Options:",
    "  --http-url <url>       HTTP control plane URL",
    "  --grpc-url <url>       gRPC data plane URL",
    "  --timeout-ms <ms>      Request timeout in milliseconds (default: 10000)",
    "  --poll-interval-ms <ms> Poll interval in milliseconds (default: 500)",
    "",
    "Other:",
    "  --help, -h             Show this help",
    "  --version, -V          Show version",
    "",
    "Environment:",
    "  FERRUMQ_HTTP_URL       HTTP URL fallback (default: http://127.0.0.1:8080)",
    "  FERRUMQ_GRPC_URL       gRPC URL fallback (default: http://127.0.0.1:9090)",
    "  URL precedence         CLI flag, then environment variable, then default",
    "",
    "Controls in chat:",
    "  Enter                  Send message",
    "  Esc or Ctrl+C          Quit",
    "",
    "Example:",
    "  ferrumq-chat --name davi --room general",
  ].join("\n");
}
