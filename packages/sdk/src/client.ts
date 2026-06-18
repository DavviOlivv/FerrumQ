import { randomUUID } from "node:crypto";
import {
  ControlPlaneRequestError,
  createControlPlaneClient,
  createGrpcDataPlaneClient,
  type BrokerStatusResponse,
  type ControlPlaneClient,
  type DataPlaneClient,
  type DlqEntryResponse,
  type HttpStatusResponse,
  type TopicResponse,
} from "@ferrumq/protocol";
import { validateOptions, type FerrumQClientOptions } from "./config.js";
import { encodePayload } from "./encoding.js";
import { FerrumQError } from "./errors.js";

export type {
  BrokerStatusResponse as BrokerStatus,
  DlqEntryResponse as DlqEntry,
  HttpStatusResponse as HealthStatus,
  TopicResponse as Topic,
} from "@ferrumq/protocol";

export type { FerrumQClientOptions } from "./config.js";
export { FerrumQError } from "./errors.js";
export type { FerrumQTransport } from "./errors.js";

export interface CreateTopicRequest {
  name: string;
  partitions: number;
}

export interface PublishRequest {
  topic: string;
  key?: string;
  payload: unknown;
  contentType?: string;
  type?: string;
  source?: string;
  subject?: string;
  idempotencyKey?: string;
  messageId?: string;
  timeUnixMs?: number;
}

export interface PublishResult {
  topic: string;
  partition: number;
  offset: string;
  messageId: string;
}

export interface ConsumeRequest {
  topic: string;
  group: string;
  consumerId?: string;
  maxMessages?: number;
  leaseMs?: number;
}

export interface ConsumedMessage {
  deliveryId: string;
  topic: string;
  partition: number;
  offset: string;
  messageId: string;
  key: string | null;
  payload: Uint8Array;
  contentType: string;
  type: string;
  source: string;
  subject: string | null;
  idempotencyKey: string | null;
  timeUnixMs: string;
  consumerGroup: string;
  consumerId: string;
  attemptNumber: number;
  deliveredAtUnixMs: string;
  leaseExpiresAtUnixMs: string;
}

export interface AckRequest {
  deliveryId: string;
  consumerId?: string;
}

export interface NackRequest {
  deliveryId: string;
  consumerId?: string;
  reason?: string;
}

const DEFAULT_CONSUMER_ID = "ferrumq-sdk";
const DEFAULT_MAX_MESSAGES = 1;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_PUBLISH_TYPE = "ferrumq.sdk.message";
const DEFAULT_PUBLISH_SOURCE = "ferrumq-sdk";

export class FerrumQClient {
  private readonly httpUrl: string;
  private readonly grpcUrl: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly controlPlane: ControlPlaneClient;
  private dataPlane: DataPlaneClient | null = null;
  private closed = false;

  constructor(options: FerrumQClientOptions) {
    const validated = validateOptions(options);
    this.httpUrl = validated.httpUrl;
    this.grpcUrl = validated.grpcUrl;
    this.timeoutMs = validated.timeoutMs;
    this.fetchImpl = validated.fetchImpl;
    this.controlPlane = createControlPlaneClient(
      this.httpUrl,
      this.fetchImpl as import("@ferrumq/protocol").FetchLike,
    );
  }

  async health(): Promise<HttpStatusResponse> {
    this.checkClosed();
    return this.executeControl(() => this.controlPlane.health());
  }

  async readiness(): Promise<HttpStatusResponse> {
    this.checkClosed();
    return this.executeControl(() => this.controlPlane.ready());
  }

  async status(): Promise<BrokerStatusResponse> {
    this.checkClosed();
    return this.executeControl(() => this.controlPlane.status());
  }

  async createTopic(request: CreateTopicRequest): Promise<TopicResponse> {
    this.checkClosed();
    return this.executeControl(() =>
      this.controlPlane.createTopic(request.name, request.partitions),
    );
  }

  async listTopics(): Promise<TopicResponse[]> {
    this.checkClosed();
    const response = await this.executeControl(() =>
      this.controlPlane.listTopics(),
    );
    return response.items;
  }

  async getTopic(name: string): Promise<TopicResponse> {
    this.checkClosed();
    return this.executeControl(() => this.controlPlane.getTopic(name));
  }

  async listDlq(topic?: string): Promise<DlqEntryResponse[]> {
    this.checkClosed();
    const response = await this.executeControl(() =>
      this.controlPlane.listDlq(topic),
    );
    return response.items;
  }

  async metrics(): Promise<string> {
    this.checkClosed();
    const url = new URL("/metrics", `${this.httpUrl}/`).toString();

    try {
      const response = await this.withTimeout(
        this.fetchImpl(url).then((res) => {
          if (!res.ok) {
            throw new Error(
              `HTTP ${res.status}: ${res.statusText || "request failed"}`,
            );
          }
          return res.text();
        }),
      );
      return response;
    } catch (error) {
      if (error instanceof FerrumQError) throw error;
      throw new FerrumQError(
        `HTTP request failed for metrics: ${errorMessage(error)}`,
        { transport: "http", cause: error },
      );
    }
  }

  async publish(request: PublishRequest): Promise<PublishResult> {
    this.checkClosed();
    const encoded = encodePayload(request.payload);
    const messageId = nonEmpty(request.messageId) ?? randomUUID();
    const timeUnixMs = request.timeUnixMs ?? Date.now();
    const contentType = nonEmpty(request.contentType) ?? encoded.contentType;
    const type = nonEmpty(request.type) ?? DEFAULT_PUBLISH_TYPE;
    const source = nonEmpty(request.source) ?? DEFAULT_PUBLISH_SOURCE;

    try {
      const response = await this.withTimeout(
        this.getDataPlane().publish({
          topic: request.topic,
          messageId,
          key: request.key ?? "",
          payload: encoded.data,
          contentType,
          type,
          source,
          subject: request.subject ?? "",
          idempotencyKey: request.idempotencyKey ?? "",
          timeUnixMs: String(timeUnixMs),
        }),
      );

      return {
        topic: response.topic,
        partition: response.partition,
        offset: response.offset,
        messageId: response.messageId,
      };
    } catch (error) {
      throw this.wrapGrpcError(error);
    }
  }

  async consume(request: ConsumeRequest): Promise<ConsumedMessage[]> {
    this.checkClosed();
    const consumerId = nonEmpty(request.consumerId) ?? DEFAULT_CONSUMER_ID;
    const maxMessages = request.maxMessages ?? DEFAULT_MAX_MESSAGES;
    const leaseMs = request.leaseMs ?? DEFAULT_LEASE_MS;
    const nowUnixMs = Date.now();

    try {
      const response = await this.withTimeout(
        this.getDataPlane().consume({
          topic: request.topic,
          consumerGroup: request.group,
          consumerId,
          maxMessages,
          leaseMs: String(leaseMs),
          nowUnixMs: String(nowUnixMs),
        }),
      );

      return response.messages.map(
        (message): ConsumedMessage => ({
          deliveryId: message.deliveryId,
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          messageId: message.messageId,
          key: message.key.length > 0 ? message.key : null,
          payload: message.payload,
          contentType: message.contentType,
          type: message.type,
          source: message.source,
          subject: message.subject.length > 0 ? message.subject : null,
          idempotencyKey:
            message.idempotencyKey.length > 0 ? message.idempotencyKey : null,
          timeUnixMs: message.timeUnixMs,
          consumerGroup: message.consumerGroup,
          consumerId: message.consumerId,
          attemptNumber: message.attemptNumber,
          deliveredAtUnixMs: message.deliveredAtUnixMs,
          leaseExpiresAtUnixMs: message.leaseExpiresAtUnixMs,
        }),
      );
    } catch (error) {
      throw this.wrapGrpcError(error);
    }
  }

  async ack(request: AckRequest): Promise<void> {
    this.checkClosed();
    const consumerId = nonEmpty(request.consumerId) ?? DEFAULT_CONSUMER_ID;

    try {
      await this.withTimeout(
        this.getDataPlane().ack({
          deliveryId: request.deliveryId,
          consumerId,
        }),
      );
    } catch (error) {
      throw this.wrapGrpcError(error);
    }
  }

  async nack(request: NackRequest): Promise<void> {
    this.checkClosed();
    const consumerId = nonEmpty(request.consumerId) ?? DEFAULT_CONSUMER_ID;

    try {
      await this.withTimeout(
        this.getDataPlane().nack({
          deliveryId: request.deliveryId,
          consumerId,
          reason: request.reason ?? "",
        }),
      );
    } catch (error) {
      throw this.wrapGrpcError(error);
    }
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    if (this.dataPlane !== null) {
      this.dataPlane.close();
      this.dataPlane = null;
    }
  }

  private getDataPlane(): DataPlaneClient {
    if (this.dataPlane === null) {
      this.dataPlane = createGrpcDataPlaneClient(this.grpcUrl);
    }
    return this.dataPlane;
  }

  private executeControl<T>(fn: () => Promise<T>): Promise<T> {
    return this.withTimeout(fn()).catch((error) => {
      if (error instanceof ControlPlaneRequestError) {
        const options: {
          code?: string;
          status?: number;
          transport: "http";
          cause: unknown;
        } = {
          transport: "http",
          cause: error,
        };
        if (error.ferrumqError?.code !== undefined) {
          options.code = error.ferrumqError.code;
        }
        if (error.status !== undefined) {
          options.status = error.status;
        }
        throw new FerrumQError(error.message, options);
      }
      throw error;
    });
  }

  private checkClosed(): void {
    if (this.closed) {
      throw new FerrumQError("Client is closed", { transport: "sdk" });
    }
  }

  private withTimeout<T>(promise: Promise<T>): Promise<T> {
    if (this.timeoutMs <= 0) {
      return promise;
    }

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(
          new FerrumQError(`Request timed out after ${this.timeoutMs}ms`, {
            transport: "sdk",
          }),
        );
      }, this.timeoutMs);

      promise
        .then((result) => {
          clearTimeout(timer);
          resolve(result);
        })
        .catch((error) => {
          clearTimeout(timer);
          reject(error);
        });
    });
  }

  private wrapGrpcError(error: unknown): never {
    if (error instanceof FerrumQError) {
      throw error;
    }

    const candidate = error as {
      code?: unknown;
      details?: unknown;
      message?: unknown;
    };

    const code =
      typeof candidate.code === "number"
        ? gRPCCodeToString(candidate.code)
        : undefined;

    const message =
      typeof candidate.details === "string" && candidate.details.length > 0
        ? candidate.details
        : typeof candidate.message === "string"
          ? candidate.message
          : "gRPC request failed";

    const errorOptions: {
      code?: string;
      transport: "grpc";
      cause: unknown;
    } = {
      transport: "grpc",
      cause: error,
    };
    if (code !== undefined) {
      errorOptions.code = code;
    }

    throw new FerrumQError(`gRPC request failed: ${message}`, errorOptions);
  }
}

function nonEmpty(value: string | undefined): string | undefined {
  if (value === undefined || value.length === 0) {
    return undefined;
  }
  return value;
}

function gRPCCodeToString(code: number): string {
  const names: Record<number, string> = {
    0: "OK",
    1: "CANCELLED",
    2: "UNKNOWN",
    3: "INVALID_ARGUMENT",
    4: "DEADLINE_EXCEEDED",
    5: "NOT_FOUND",
    6: "ALREADY_EXISTS",
    7: "PERMISSION_DENIED",
    8: "RESOURCE_EXHAUSTED",
    9: "FAILED_PRECONDITION",
    10: "ABORTED",
    11: "OUT_OF_RANGE",
    12: "UNIMPLEMENTED",
    13: "INTERNAL",
    14: "UNAVAILABLE",
    15: "DATA_LOSS",
    16: "UNAUTHENTICATED",
  };
  return names[code] ?? "UNKNOWN";
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "string") return error;
  return "unexpected error";
}
