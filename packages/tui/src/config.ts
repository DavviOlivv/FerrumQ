import { z } from "zod";

export const tuiVersion = "0.1.0";
export const defaultControlUrl = "http://127.0.0.1:8080";
export const defaultGrpcUrl = "http://127.0.0.1:9090";

export interface TuiCliOptions {
  help: boolean;
  version: boolean;
  controlUrl?: string;
  grpcUrl?: string;
}

export interface TuiConfig {
  controlUrl: string;
  grpcUrl: string;
}

export interface TuiEnvironment {
  FERRUMQ_CONTROL_URL?: string;
  FERRUMQ_GRPC_URL?: string;
}

export class ExpectedTuiError extends Error {
  readonly exitCode: number;

  constructor(message: string, exitCode = 1) {
    super(message);
    this.name = "ExpectedTuiError";
    this.exitCode = exitCode;
  }
}

export function parseTuiArgs(args: readonly string[]): TuiCliOptions {
  const options: TuiCliOptions = { help: false, version: false };

  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    if (token === undefined) {
      continue;
    }

    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }

    if (token === "--version" || token === "-V") {
      options.version = true;
      continue;
    }

    const controlUrl = readOptionValue(args, index, token, "--control-url");
    if (controlUrl.matched) {
      options.controlUrl = controlUrl.value;
      index = controlUrl.nextIndex;
      continue;
    }

    const grpcUrl = readOptionValue(args, index, token, "--grpc-url");
    if (grpcUrl.matched) {
      options.grpcUrl = grpcUrl.value;
      index = grpcUrl.nextIndex;
      continue;
    }

    if (token.startsWith("-")) {
      throw new ExpectedTuiError(`Unknown option: ${token}`);
    }

    throw new ExpectedTuiError(`Unknown argument: ${token}`);
  }

  if (options.help && options.version) {
    throw new ExpectedTuiError("--help and --version cannot be combined");
  }

  return options;
}

export function resolveTuiConfig(
  options: Pick<TuiCliOptions, "controlUrl" | "grpcUrl"> = {},
  env: TuiEnvironment = {},
): TuiConfig {
  const controlUrl =
    options.controlUrl ?? env.FERRUMQ_CONTROL_URL ?? defaultControlUrl;
  const grpcUrl = options.grpcUrl ?? env.FERRUMQ_GRPC_URL ?? defaultGrpcUrl;

  return {
    controlUrl: validateHttpUrl(controlUrl, "control URL"),
    grpcUrl: validateGrpcUrl(grpcUrl, "gRPC URL"),
  };
}

export function tuiHelpText(): string {
  return [
    "FerrumQ TUI",
    "",
    "Usage:",
    "  ferrumq-tui [--control-url <url>] [--grpc-url <url>]",
    "  ferrumq-tui --version",
    "  ferrumq-tui --help",
    "",
    "Configuration:",
    "  --control-url <url>    HTTP control plane URL",
    "  --grpc-url <url>       gRPC data plane URL shown as configured state",
    "  FERRUMQ_CONTROL_URL    environment fallback for --control-url",
    "  FERRUMQ_GRPC_URL       environment fallback for --grpc-url",
    "",
    "Keys:",
    "  1 dashboard   2 topics   3 DLQ   4 search   ? help   r refresh   q quit",
    "",
    "Search:",
    "  Type a query in the search view (4). Press Enter to send the request,",
    "  Backspace to edit, Esc to return, and Ctrl+C to quit. Search requires",
    "  the local broker to be started with --postgres-database-url or",
    "  FERRUMQ_DATABASE_URL.",
  ].join("\n");
}

function readOptionValue(
  args: readonly string[],
  index: number,
  token: string,
  flag: string,
):
  | { matched: true; value: string; nextIndex: number }
  | { matched: false; nextIndex: number; value?: never } {
  if (token === flag) {
    const value = args[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new ExpectedTuiError(`${flag} requires a value`);
    }
    return { matched: true, value, nextIndex: index + 1 };
  }

  const prefix = `${flag}=`;
  if (token.startsWith(prefix)) {
    const value = token.slice(prefix.length);
    if (value.length === 0) {
      throw new ExpectedTuiError(`${flag} requires a value`);
    }
    return { matched: true, value, nextIndex: index };
  }

  return { matched: false, nextIndex: index };
}

function validateHttpUrl(value: string, field: string): string {
  const parsed = parseUrl(value, field);
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new ExpectedTuiError(`${field} must use http:// or https://`);
  }
  validateUrlParts(parsed, field);
  if (parsed.pathname !== "/" || parsed.search !== "" || parsed.hash !== "") {
    throw new ExpectedTuiError(
      `${field} must not include a path, query, or fragment`,
    );
  }
  return stripTrailingSlash(parsed.toString());
}

function validateGrpcUrl(value: string, field: string): string {
  const parsed = parseUrl(value, field);
  if (parsed.protocol === "https:") {
    throw new ExpectedTuiError(
      `${field} TLS/HTTPS is deferred; use http://host:port`,
    );
  }
  if (parsed.protocol !== "http:") {
    throw new ExpectedTuiError(`${field} must use http://host:port`);
  }
  validateUrlParts(parsed, field);
  if (parsed.port === "") {
    throw new ExpectedTuiError(`${field} must include a port`);
  }
  if (parsed.pathname !== "/" || parsed.search !== "" || parsed.hash !== "") {
    throw new ExpectedTuiError(
      `${field} must not include a path, query, or fragment`,
    );
  }
  return stripTrailingSlash(parsed.toString());
}

function parseUrl(value: string, field: string): URL {
  const parsedValue = z.string().trim().min(1, `${field} must be a valid URL`);
  const parsedText = parsedValue.safeParse(value);
  if (!parsedText.success) {
    throw new ExpectedTuiError(parsedText.error.issues[0]?.message ?? "");
  }

  try {
    return new URL(parsedText.data);
  } catch {
    throw new ExpectedTuiError(`${field} must be a valid URL`);
  }
}

function validateUrlParts(parsed: URL, field: string): void {
  if (parsed.hostname === "") {
    throw new ExpectedTuiError(`${field} must include a host`);
  }
  if (parsed.username !== "" || parsed.password !== "") {
    throw new ExpectedTuiError(`${field} must not include credentials`);
  }
}

function stripTrailingSlash(value: string): string {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}
