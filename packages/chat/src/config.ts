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

    if (arg.startsWith("--")) {
      const flag = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
      if (seen.has(flag)) {
        errors.push(`duplicate option: ${arg}`);
        if (
          !arg.includes("=") &&
          i + 1 < args.length &&
          !(args[i + 1] ?? "").startsWith("-")
        ) {
          i++;
        }
        continue;
      }
      seen.add(flag);
    }

    switch (arg) {
      case "--help":
      case "-h":
        wantHelp = true;
        break;
      case "--version":
      case "-V":
        wantVersion = true;
        break;
      case "--name": {
        const raw = args[++i];
        if (raw === undefined || raw.startsWith("-")) {
          errors.push("--name requires a value");
        } else {
          name = raw;
        }
        break;
      }
      case "--room": {
        const raw = args[++i];
        if (raw === undefined || raw.startsWith("-")) {
          errors.push("--room requires a value");
        } else {
          room = raw;
        }
        break;
      }
      case "--http-url": {
        const raw = args[++i];
        if (raw === undefined || raw.startsWith("-")) {
          errors.push("--http-url requires a value");
        } else {
          httpUrl = raw;
        }
        break;
      }
      case "--grpc-url": {
        const raw = args[++i];
        if (raw === undefined || raw.startsWith("-")) {
          errors.push("--grpc-url requires a value");
        } else {
          grpcUrl = raw;
        }
        break;
      }
      case "--timeout-ms": {
        const raw = args[++i];
        if (raw === undefined || raw.startsWith("-")) {
          errors.push("--timeout-ms requires a value");
        } else {
          const parsed = parseUnsignedInteger(raw);
          if (parsed === null || parsed > MAX_SAFE_TIMER_MS) {
            errors.push("--timeout-ms must be a non-negative integer");
          } else {
            timeoutMs = parsed;
          }
        }
        break;
      }
      case "--poll-interval-ms": {
        const raw = args[++i];
        if (raw === undefined || raw.startsWith("-")) {
          errors.push("--poll-interval-ms requires a value");
        } else {
          const parsed = parseUnsignedInteger(raw);
          if (parsed === null || parsed <= 0 || parsed > MAX_SAFE_TIMER_MS) {
            errors.push("--poll-interval-ms must be a positive integer");
          } else {
            pollIntervalMs = parsed;
          }
        }
        break;
      }
      default: {
        if (arg.startsWith("--name=")) {
          const raw = arg.slice("--name=".length);
          name = raw.startsWith("=") ? raw.slice(1) : raw;
        } else if (arg.startsWith("--room=")) {
          const raw = arg.slice("--room=".length);
          room = raw.startsWith("=") ? raw.slice(1) : raw;
        } else if (arg.startsWith("--http-url=")) {
          const raw = arg.slice("--http-url=".length);
          httpUrl = raw.startsWith("=") ? raw.slice(1) : raw;
        } else if (arg.startsWith("--grpc-url=")) {
          const raw = arg.slice("--grpc-url=".length);
          grpcUrl = raw.startsWith("=") ? raw.slice(1) : raw;
        } else if (arg.startsWith("--timeout-ms=")) {
          const parsed = parseUnsignedInteger(
            arg.slice("--timeout-ms=".length),
          );
          if (parsed === null || parsed > MAX_SAFE_TIMER_MS) {
            errors.push("--timeout-ms must be a non-negative integer");
          } else {
            timeoutMs = parsed;
          }
        } else if (arg.startsWith("--poll-interval-ms=")) {
          const parsed = parseUnsignedInteger(
            arg.slice("--poll-interval-ms=".length),
          );
          if (parsed === null || parsed <= 0 || parsed > MAX_SAFE_TIMER_MS) {
            errors.push("--poll-interval-ms must be a positive integer");
          } else {
            pollIntervalMs = parsed;
          }
        } else {
          errors.push(`unknown option: ${arg}`);
        }
      }
    }
  }

  if (wantHelp && wantVersion) {
    return { error: "--help and --version cannot be combined", exitCode: 1 };
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
