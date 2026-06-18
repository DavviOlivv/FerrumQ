import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { z, ZodError, type ZodType } from "zod";

export const httpStatusResponseSchema = z.object({
  status: z.string().min(1),
});

export type HttpStatusResponse = z.infer<typeof httpStatusResponseSchema>;

export const brokerStatusResponseSchema = z.object({
  mode: z.string().min(1),
  dataDir: z.string(),
  topics: z.number().int().nonnegative(),
  dlqEntries: z.number().int().nonnegative(),
});

export type BrokerStatusResponse = z.infer<typeof brokerStatusResponseSchema>;

export const topicResponseSchema = z.object({
  name: z.string().min(1),
  partitions: z.number().int().positive(),
});

export type TopicResponse = z.infer<typeof topicResponseSchema>;

export const topicListResponseSchema = z.object({
  items: z.array(topicResponseSchema),
});

export type TopicListResponse = z.infer<typeof topicListResponseSchema>;

export const dlqEntryResponseSchema = z.object({
  topic: z.string().min(1),
  partition: z.number().int().nonnegative(),
  offset: z.number().int().nonnegative(),
  messageId: z.string().min(1),
  consumerGroupId: z.string().min(1),
  reason: z.string(),
  attemptCount: z.number().int().nonnegative(),
  timestamp: z.number().int().nonnegative(),
});

export type DlqEntryResponse = z.infer<typeof dlqEntryResponseSchema>;

export const dlqListResponseSchema = z.object({
  items: z.array(dlqEntryResponseSchema),
});

export type DlqListResponse = z.infer<typeof dlqListResponseSchema>;

export const ferrumQErrorEnvelopeSchema = z.object({
  error: z.object({
    code: z.string().min(1),
    message: z.string(),
    details: z.unknown(),
    statusCode: z.number().int().min(400).max(599),
  }),
});

export type FerrumQErrorEnvelope = z.infer<typeof ferrumQErrorEnvelopeSchema>;

export type FetchLike = (
  input: string,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
    signal?: AbortSignal;
  },
) => Promise<ResponseLike>;

export interface ResponseLike {
  ok: boolean;
  status: number;
  statusText: string;
  json(): Promise<unknown>;
}

export interface ControlPlaneClient {
  health(): Promise<HttpStatusResponse>;
  ready(): Promise<HttpStatusResponse>;
  status(): Promise<BrokerStatusResponse>;
  createTopic(name: string, partitions: number): Promise<TopicResponse>;
  getTopic(name: string): Promise<TopicResponse>;
  listTopics(): Promise<TopicListResponse>;
  listDlq(topic?: string): Promise<DlqListResponse>;
}

export type ControlPlaneRequestErrorKind =
  | "network"
  | "ferrumq-error"
  | "malformed-error"
  | "invalid-json"
  | "schema";

export interface ControlPlaneRequestErrorOptions {
  kind: ControlPlaneRequestErrorKind;
  method: string;
  url: string;
  status?: number;
  statusText?: string;
  ferrumqError?: FerrumQErrorEnvelope["error"];
  validationIssues?: unknown[];
  cause?: unknown;
}

export class ControlPlaneRequestError extends Error {
  readonly kind: ControlPlaneRequestErrorKind;
  readonly method: string;
  readonly url: string;
  readonly status: number | undefined;
  readonly statusText: string | undefined;
  readonly ferrumqError: FerrumQErrorEnvelope["error"] | undefined;
  readonly validationIssues: unknown[] | undefined;

  constructor(message: string, options: ControlPlaneRequestErrorOptions) {
    super(message, { cause: options.cause });
    this.name = "ControlPlaneRequestError";
    this.kind = options.kind;
    this.method = options.method;
    this.url = options.url;
    this.status = options.status;
    this.statusText = options.statusText;
    this.ferrumqError = options.ferrumqError;
    this.validationIssues = options.validationIssues;
  }
}

export function createControlPlaneClient(
  controlUrl: string,
  fetchImpl: FetchLike = fetch as FetchLike,
): ControlPlaneClient {
  return {
    health: () =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        "/health",
        httpStatusResponseSchema,
      ),
    ready: () =>
      request(controlUrl, fetchImpl, "GET", "/ready", httpStatusResponseSchema),
    status: () =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        "/v1/status",
        brokerStatusResponseSchema,
      ),
    createTopic: (name, partitions) =>
      request(
        controlUrl,
        fetchImpl,
        "POST",
        "/v1/topics",
        topicResponseSchema,
        {
          name,
          partitions,
        },
      ),
    getTopic: (name) =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        `/v1/topics/${encodeURIComponent(name)}`,
        topicResponseSchema,
      ),
    listTopics: () =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        "/v1/topics",
        topicListResponseSchema,
      ),
    listDlq: (topic) =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        topic === undefined
          ? "/v1/dlq"
          : `/v1/dlq?topic=${encodeURIComponent(topic)}`,
        dlqListResponseSchema,
      ),
  };
}

async function request<T>(
  controlUrl: string,
  fetchImpl: FetchLike,
  method: string,
  requestPath: string,
  schema: ZodType<T>,
  body?: unknown,
): Promise<T> {
  const url = buildUrl(controlUrl, requestPath);
  let response: ResponseLike;
  try {
    const init: {
      method: string;
      headers?: Record<string, string>;
      body?: string;
    } = { method };
    if (body !== undefined) {
      init.headers = { "content-type": "application/json" };
      init.body = JSON.stringify(body);
    }
    response = await fetchImpl(url, init);
  } catch (error) {
    throw new ControlPlaneRequestError(
      `Network request failed for ${method} ${url}: ${errorMessage(error)}`,
      { kind: "network", method, url, cause: error },
    );
  }

  const json = await readJson(response);
  if (!json.ok) {
    if (!response.ok) {
      throw malformedHttpError(method, url, response);
    }

    throw new ControlPlaneRequestError(
      "Unexpected response from control API: invalid JSON",
      { kind: "invalid-json", method, url, cause: json.error },
    );
  }

  if (!response.ok) {
    const envelope = ferrumQErrorEnvelopeSchema.safeParse(json.payload);
    if (envelope.success) {
      throw new ControlPlaneRequestError(
        `HTTP ${response.status} ${envelope.data.error.code}: ${envelope.data.error.message}`,
        {
          kind: "ferrumq-error",
          method,
          url,
          status: response.status,
          statusText: response.statusText,
          ferrumqError: envelope.data.error,
        },
      );
    }

    throw malformedHttpError(method, url, response);
  }

  try {
    return schema.parse(json.payload);
  } catch (error) {
    if (error instanceof ZodError) {
      throw new ControlPlaneRequestError(
        `Unexpected response from control API: ${error.issues[0]?.message}`,
        {
          kind: "schema",
          method,
          url,
          validationIssues: error.issues,
          cause: error,
        },
      );
    }
    throw error;
  }
}

function malformedHttpError(
  method: string,
  url: string,
  response: ResponseLike,
): ControlPlaneRequestError {
  return new ControlPlaneRequestError(
    `HTTP ${response.status}: ${response.statusText || "request failed"}`,
    {
      kind: "malformed-error",
      method,
      url,
      status: response.status,
      statusText: response.statusText,
    },
  );
}

function buildUrl(controlUrl: string, requestPath: string): string {
  return new URL(requestPath, `${controlUrl}/`).toString();
}

async function readJson(
  response: ResponseLike,
): Promise<{ ok: true; payload: unknown } | { ok: false; error: unknown }> {
  try {
    return { ok: true, payload: await response.json() };
  } catch (error) {
    return { ok: false, error };
  }
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  return "unexpected error";
}

export type DecimalString = `${number}` | string;

export interface DataPlanePublishRequest {
  topic: string;
  messageId: string;
  key?: string;
  payload: Uint8Array;
  contentType: string;
  type: string;
  source: string;
  subject?: string;
  idempotencyKey?: string;
  timeUnixMs: DecimalString;
}

export interface DataPlanePublishResponse {
  topic: string;
  partition: number;
  offset: string;
  messageId: string;
}

export interface DataPlaneConsumeRequest {
  topic: string;
  consumerGroup: string;
  consumerId: string;
  maxMessages: number;
  leaseMs: DecimalString;
  nowUnixMs: DecimalString;
}

export interface DataPlaneConsumedMessage {
  deliveryId: string;
  topic: string;
  partition: number;
  offset: string;
  messageId: string;
  key: string;
  payload: Buffer;
  contentType: string;
  type: string;
  source: string;
  subject: string;
  idempotencyKey: string;
  timeUnixMs: string;
  consumerGroup: string;
  consumerId: string;
  attemptNumber: number;
  deliveredAtUnixMs: string;
  leaseExpiresAtUnixMs: string;
}

export interface DataPlaneConsumeResponse {
  messages: DataPlaneConsumedMessage[];
}

export interface DataPlaneAckRequest {
  deliveryId: string;
  consumerId: string;
}

export interface DataPlaneNackRequest {
  deliveryId: string;
  consumerId: string;
  reason?: string;
}

export interface DataPlaneClient {
  publish(request: DataPlanePublishRequest): Promise<DataPlanePublishResponse>;
  consume(request: DataPlaneConsumeRequest): Promise<DataPlaneConsumeResponse>;
  ack(request: DataPlaneAckRequest): Promise<void>;
  nack(request: DataPlaneNackRequest): Promise<void>;
  close(): void;
}

type RawGrpcCallback = (
  error: grpc.ServiceError | null,
  response: unknown,
) => void;
type RawGrpcMethod = (
  request: Record<string, unknown>,
  callback: RawGrpcCallback,
) => void;
type RawGrpcClient = Record<string, unknown>;

export interface GrpcDataPlaneClientOptions {
  protoPath?: string;
  createRawClient?: (target: string, protoPath: string) => RawGrpcClient;
}

export function normalizeGrpcTarget(grpcUrl: string): string {
  let parsed: URL;
  try {
    parsed = new URL(grpcUrl);
  } catch {
    throw new Error("gRPC URL must be a valid URL like http://127.0.0.1:9090");
  }

  if (parsed.protocol === "https:") {
    throw new Error("gRPC TLS/HTTPS URLs are deferred; use http://host:port");
  }

  if (parsed.protocol !== "http:") {
    throw new Error("gRPC URL must use http://host:port");
  }

  if (parsed.username !== "" || parsed.password !== "") {
    throw new Error("gRPC URL must not include credentials");
  }

  if (parsed.hostname === "" || parsed.port === "") {
    throw new Error("gRPC URL must include host and port");
  }

  if (parsed.pathname !== "/" || parsed.search !== "" || parsed.hash !== "") {
    throw new Error("gRPC URL must not include a path, query, or fragment");
  }

  return parsed.host;
}

export function defaultDataPlaneProtoPath(): string {
  const moduleDir = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(moduleDir, "../../..");
  return path.resolve(
    repoRoot,
    "crates/msg-protocol/proto/ferrumq/dataplane/v1/dataplane.proto",
  );
}

export function createGrpcDataPlaneClient(
  grpcUrl: string,
  options: GrpcDataPlaneClientOptions = {},
): DataPlaneClient {
  const target = normalizeGrpcTarget(grpcUrl);
  const protoPath = options.protoPath ?? defaultDataPlaneProtoPath();
  const rawClient =
    options.createRawClient?.(target, protoPath) ??
    createDefaultRawClient(target, protoPath);

  return {
    async publish(request) {
      const response = await callUnary(rawClient, "publish", "Publish", {
        topic: request.topic,
        messageId: request.messageId,
        key: request.key ?? "",
        payload: Buffer.from(request.payload),
        contentType: request.contentType,
        type: request.type,
        source: request.source,
        subject: request.subject ?? "",
        idempotencyKey: request.idempotencyKey ?? "",
        timeUnixMs: request.timeUnixMs,
      });

      return {
        topic: stringField(response, "topic"),
        partition: numberField(response, "partition"),
        offset: decimalStringField(response, "offset"),
        messageId: stringField(response, "messageId"),
      };
    },

    async consume(request) {
      const response = await callUnary(rawClient, "consume", "Consume", {
        topic: request.topic,
        consumerGroup: request.consumerGroup,
        consumerId: request.consumerId,
        maxMessages: request.maxMessages,
        leaseMs: request.leaseMs,
        nowUnixMs: request.nowUnixMs,
      });

      return {
        messages: arrayField(response, "messages").map((message) => ({
          deliveryId: stringField(message, "deliveryId"),
          topic: stringField(message, "topic"),
          partition: numberField(message, "partition"),
          offset: decimalStringField(message, "offset"),
          messageId: stringField(message, "messageId"),
          key: stringField(message, "key"),
          payload: bytesField(message, "payload"),
          contentType: stringField(message, "contentType"),
          type: stringField(message, "type"),
          source: stringField(message, "source"),
          subject: stringField(message, "subject"),
          idempotencyKey: stringField(message, "idempotencyKey"),
          timeUnixMs: decimalStringField(message, "timeUnixMs"),
          consumerGroup: stringField(message, "consumerGroup"),
          consumerId: stringField(message, "consumerId"),
          attemptNumber: numberField(message, "attemptNumber"),
          deliveredAtUnixMs: decimalStringField(message, "deliveredAtUnixMs"),
          leaseExpiresAtUnixMs: decimalStringField(
            message,
            "leaseExpiresAtUnixMs",
          ),
        })),
      };
    },

    async ack(request) {
      await callUnary(rawClient, "ack", "Ack", {
        deliveryId: request.deliveryId,
        consumerId: request.consumerId,
      });
    },

    async nack(request) {
      await callUnary(rawClient, "nack", "Nack", {
        deliveryId: request.deliveryId,
        consumerId: request.consumerId,
        reason: request.reason ?? "",
      });
    },

    close() {
      const closeable = rawClient as unknown as { close(): void };
      closeable.close();
    },
  };
}

export function grpcStatusName(code: number): string {
  const entry = Object.entries(grpc.status).find(([, value]) => value === code);
  return entry?.[0] ?? "UNKNOWN";
}

export function formatGrpcError(error: unknown): string {
  const candidate = error as {
    code?: unknown;
    details?: unknown;
    message?: unknown;
  };
  if (typeof candidate.code === "number") {
    const details =
      typeof candidate.details === "string" && candidate.details.length > 0
        ? candidate.details
        : typeof candidate.message === "string"
          ? candidate.message
          : "request failed";
    return `gRPC ${grpcStatusName(candidate.code)} (${candidate.code}): ${details}`;
  }

  if (error instanceof Error) {
    return `gRPC request failed: ${error.message}`;
  }

  return "gRPC request failed";
}

function createDefaultRawClient(
  target: string,
  protoPath: string,
): RawGrpcClient {
  if (!existsSync(protoPath)) {
    throw new Error(`data-plane proto file not found at ${protoPath}`);
  }

  const definition = protoLoader.loadSync(protoPath, {
    arrays: true,
    bytes: Buffer,
    defaults: true,
    enums: String,
    longs: String,
    oneofs: true,
  });
  const grpcObject = grpc.loadPackageDefinition(definition) as Record<
    string,
    unknown
  >;
  const service = resolveFerrumQDataPlaneService(grpcObject);
  return new service(
    target,
    grpc.credentials.createInsecure(),
  ) as RawGrpcClient;
}

function resolveFerrumQDataPlaneService(
  grpcObject: Record<string, unknown>,
): new (target: string, credentials: grpc.ChannelCredentials) => unknown {
  const ferrumq = recordField(grpcObject, "ferrumq");
  const dataplane = recordField(recordField(ferrumq, "dataplane"), "v1");
  const service = dataplane.FerrumQDataPlane;
  if (typeof service !== "function") {
    throw new Error(
      "data-plane proto does not expose ferrumq.dataplane.v1.FerrumQDataPlane",
    );
  }
  return service as new (
    target: string,
    credentials: grpc.ChannelCredentials,
  ) => unknown;
}

function callUnary(
  client: RawGrpcClient,
  lowerName: string,
  upperName: string,
  request: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const method = client[lowerName] ?? client[upperName];
  if (typeof method !== "function") {
    throw new Error(`gRPC client does not expose ${upperName}`);
  }

  return new Promise((resolve, reject) => {
    (method as RawGrpcMethod).call(client, request, (error, response) => {
      if (error !== null) {
        reject(error);
        return;
      }

      resolve(recordValue(response));
    });
  });
}

function recordField(
  value: Record<string, unknown>,
  field: string,
): Record<string, unknown> {
  return recordValue(value[field]);
}

function recordValue(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("gRPC response had an unexpected shape");
  }
  return value as Record<string, unknown>;
}

function stringField(value: Record<string, unknown>, field: string): string {
  const fieldValue = value[field];
  if (typeof fieldValue === "string") {
    return fieldValue;
  }
  throw new Error(`gRPC response field ${field} was not a string`);
}

function numberField(value: Record<string, unknown>, field: string): number {
  const fieldValue = value[field];
  if (typeof fieldValue === "number" && Number.isInteger(fieldValue)) {
    return fieldValue;
  }
  throw new Error(`gRPC response field ${field} was not an integer`);
}

function decimalStringField(
  value: Record<string, unknown>,
  field: string,
): string {
  const fieldValue = value[field];
  if (typeof fieldValue === "string" && /^\d+$/.test(fieldValue)) {
    return fieldValue;
  }
  if (
    typeof fieldValue === "number" &&
    Number.isInteger(fieldValue) &&
    fieldValue >= 0
  ) {
    return String(fieldValue);
  }
  if (typeof fieldValue === "bigint" && fieldValue >= 0n) {
    return fieldValue.toString();
  }
  if (
    typeof fieldValue === "object" &&
    fieldValue !== null &&
    "toString" in fieldValue &&
    typeof fieldValue.toString === "function"
  ) {
    const rendered = fieldValue.toString();
    if (/^\d+$/.test(rendered)) {
      return rendered;
    }
  }
  throw new Error(
    `gRPC response field ${field} was not a decimal uint64 string`,
  );
}

function bytesField(value: Record<string, unknown>, field: string): Buffer {
  const fieldValue = value[field];
  if (Buffer.isBuffer(fieldValue)) {
    return fieldValue;
  }
  if (fieldValue instanceof Uint8Array) {
    return Buffer.from(fieldValue);
  }
  if (typeof fieldValue === "string") {
    return Buffer.from(fieldValue, "base64");
  }
  throw new Error(`gRPC response field ${field} was not bytes`);
}

function arrayField(
  value: Record<string, unknown>,
  field: string,
): Record<string, unknown>[] {
  const fieldValue = value[field];
  if (!Array.isArray(fieldValue)) {
    throw new Error(`gRPC response field ${field} was not an array`);
  }
  return fieldValue.map(recordValue);
}
