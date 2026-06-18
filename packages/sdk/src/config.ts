import { normalizeGrpcTarget } from "@ferrumq/protocol";

import { FerrumQError } from "./errors.js";

export interface FerrumQClientOptions {
  httpUrl: string;
  grpcUrl: string;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
}

export interface ValidatedOptions {
  httpUrl: string;
  grpcUrl: string;
  timeoutMs: number;
  fetchImpl: typeof fetch;
}

export function validateOptions(
  options: FerrumQClientOptions,
): ValidatedOptions {
  return {
    httpUrl: validateHttpUrl(options.httpUrl),
    grpcUrl: validateGrpcUrl(options.grpcUrl),
    timeoutMs: validateTimeoutMs(options.timeoutMs),
    fetchImpl: options.fetchImpl ?? (fetch as typeof fetch),
  };
}

function validateHttpUrl(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new FerrumQError("httpUrl is required", { transport: "sdk" });
  }

  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new FerrumQError(`Invalid httpUrl: ${value}`, { transport: "sdk" });
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new FerrumQError("httpUrl must use http:// or https:// protocol", {
      transport: "sdk",
    });
  }

  if (parsed.username !== "" || parsed.password !== "") {
    throw new FerrumQError("httpUrl must not include credentials", {
      transport: "sdk",
    });
  }

  return value;
}

function validateGrpcUrl(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new FerrumQError("grpcUrl is required", { transport: "sdk" });
  }

  try {
    normalizeGrpcTarget(value);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "invalid gRPC target";
    throw new FerrumQError(`Invalid grpcUrl: ${message}`, {
      transport: "sdk",
      cause: error,
    });
  }

  return value;
}

function validateTimeoutMs(value: number | undefined): number {
  if (value === undefined) {
    return 0;
  }

  if (!Number.isInteger(value) || value <= 0) {
    throw new FerrumQError("timeoutMs must be a positive integer", {
      transport: "sdk",
    });
  }

  return value;
}
