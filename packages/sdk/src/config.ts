import { normalizeGrpcTarget } from "@ferrumq/protocol";

import { FerrumQError } from "./errors.js";

export interface FerrumQClientOptions {
  httpUrl: string;
  grpcUrl: string;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
}

export const MAX_TIMEOUT_MS = 2_147_483_647;

export interface ValidatedOptions {
  httpUrl: string;
  grpcUrl: string;
  timeoutMs: number;
  fetchImpl: typeof fetch;
}

export function validateOptions(
  options: FerrumQClientOptions,
): ValidatedOptions {
  if (typeof options !== "object" || options === null) {
    throw configurationError("Client options are required");
  }
  if (
    options.fetchImpl !== undefined &&
    typeof options.fetchImpl !== "function"
  ) {
    throw configurationError("fetchImpl must be a function when provided");
  }
  return {
    httpUrl: validateHttpUrl(options.httpUrl),
    grpcUrl: validateGrpcUrl(options.grpcUrl),
    timeoutMs: validateTimeoutMs(options.timeoutMs),
    fetchImpl: options.fetchImpl ?? (fetch as typeof fetch),
  };
}

function validateHttpUrl(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw configurationError("httpUrl is required");
  }

  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw configurationError(`Invalid httpUrl: ${value}`);
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw configurationError("httpUrl must use http:// or https:// protocol");
  }

  if (parsed.username !== "" || parsed.password !== "") {
    throw configurationError("httpUrl must not include credentials");
  }

  if (parsed.hostname === "") {
    throw configurationError("httpUrl must include a host");
  }

  if (parsed.pathname !== "/" || parsed.search !== "" || parsed.hash !== "") {
    throw configurationError(
      "httpUrl must not include a path, query, or fragment",
    );
  }

  return value;
}

function validateGrpcUrl(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw configurationError("grpcUrl is required");
  }

  try {
    normalizeGrpcTarget(value);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "invalid gRPC target";
    throw new FerrumQError(`Invalid grpcUrl: ${message}`, {
      transport: "sdk",
      code: "SDK_CONFIGURATION",
      cause: error,
    });
  }

  return value;
}

function validateTimeoutMs(value: number | undefined): number {
  if (value === undefined) {
    return 0;
  }

  if (
    !Number.isFinite(value) ||
    !Number.isInteger(value) ||
    value < 0 ||
    value > MAX_TIMEOUT_MS
  ) {
    throw configurationError(
      `timeoutMs must be a non-negative finite integer no greater than ${MAX_TIMEOUT_MS}`,
    );
  }

  return value;
}

function configurationError(message: string): FerrumQError {
  return new FerrumQError(message, {
    transport: "sdk",
    code: "SDK_CONFIGURATION",
  });
}
