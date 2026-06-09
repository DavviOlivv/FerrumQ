import { z } from "zod";

import { ExpectedCliError } from "./errors.js";

const identifierMaxLength = 255;
const topicNameSchema = z
  .string()
  .trim()
  .min(1, "topic must not be empty")
  .max(identifierMaxLength, "topic must be at most 255 characters")
  .regex(/^[A-Za-z0-9._-]+$/, "topic contains invalid characters")
  .refine((value) => !value.startsWith("."), "topic must not start with '.'")
  .refine((value) => !value.endsWith("."), "topic must not end with '.'")
  .refine((value) => !value.includes(".."), "topic must not contain '..'");

const consumerGroupSchema = z
  .string()
  .trim()
  .min(1, "consumer group must not be empty")
  .max(identifierMaxLength, "consumer group must be at most 255 characters")
  .regex(/^[A-Za-z0-9._-]+$/, "consumer group contains invalid characters");

const boundedTextSchema = (field: string) =>
  z
    .string()
    .trim()
    .min(1, `${field} must not be empty`)
    .max(identifierMaxLength, `${field} must be at most 255 characters`);

export function validateTopic(value: string): string {
  return parseOrThrow(topicNameSchema, value);
}

export function validateConsumerGroup(value: string): string {
  return parseOrThrow(consumerGroupSchema, value);
}

export function validateBoundedText(value: string, field: string): string {
  return parseOrThrow(boundedTextSchema(field), value);
}

export function validateNonEmptyPayload(value: string): string {
  if (value.length === 0) {
    throw new ExpectedCliError("--data must not be empty");
  }
  return value;
}

export function parsePositiveInteger(
  value: string | undefined,
  field: string,
): number {
  if (value === undefined) {
    throw new ExpectedCliError(`${field} is required`);
  }

  if (!/^\d+$/.test(value)) {
    throw new ExpectedCliError(`${field} must be a positive integer`);
  }

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new ExpectedCliError(`${field} must be a positive integer`);
  }

  return parsed;
}

export function validateHttpUrl(value: string, field: string): string {
  const parsed = parseUrl(value, field);
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new ExpectedCliError(`${field} must use http:// or https://`);
  }
  validateUrlParts(parsed, field);
  return stripTrailingSlash(parsed.toString());
}

export function validateGrpcUrl(value: string, field: string): string {
  const parsed = parseUrl(value, field);
  if (parsed.protocol === "https:") {
    throw new ExpectedCliError(
      `${field} TLS/HTTPS is deferred; use http://host:port`,
    );
  }
  if (parsed.protocol !== "http:") {
    throw new ExpectedCliError(`${field} must use http://host:port`);
  }
  validateUrlParts(parsed, field);
  if (parsed.port === "") {
    throw new ExpectedCliError(`${field} must include a port`);
  }
  if (parsed.pathname !== "/" || parsed.search !== "" || parsed.hash !== "") {
    throw new ExpectedCliError(
      `${field} must not include a path, query, or fragment`,
    );
  }
  return stripTrailingSlash(parsed.toString());
}

function parseUrl(value: string, field: string): URL {
  try {
    return new URL(value);
  } catch {
    throw new ExpectedCliError(`${field} must be a valid URL`);
  }
}

function validateUrlParts(parsed: URL, field: string): void {
  if (parsed.hostname === "") {
    throw new ExpectedCliError(`${field} must include a host`);
  }
  if (parsed.username !== "" || parsed.password !== "") {
    throw new ExpectedCliError(`${field} must not include credentials`);
  }
}

function stripTrailingSlash(value: string): string {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

function parseOrThrow<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value);
  if (result.success) {
    return result.data;
  }

  throw new ExpectedCliError(
    result.error.issues[0]?.message ?? "invalid value",
  );
}
