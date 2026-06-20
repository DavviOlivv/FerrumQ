import { randomUUID } from "node:crypto";
import {
  type BrokerStatusResponse,
  type ControlPlaneClient,
  ControlPlaneRequestError,
  createControlPlaneClient,
  createGrpcDataPlaneClient,
  type DataPlaneClient,
  type DlqEntryResponse,
  type GrpcDataPlaneClientOptions,
  grpcStatusName,
  type HttpStatusResponse,
  type TopicResponse,
} from "@ferrumq/protocol";
import { type FerrumQClientOptions, validateOptions } from "./config.js";
import { encodePayload } from "./encoding.js";
import { FerrumQError } from "./errors.js";

export type {
  BrokerStatusResponse as BrokerStatus,
  DlqEntryResponse as DlqEntry,
  GrpcDataPlaneClientOptions,
  HttpStatusResponse as HealthStatus,
  TopicResponse as Topic,
} from "@ferrumq/protocol";
export type { FerrumQClientOptions } from "./config.js";
export type { FerrumQTransport } from "./errors.js";
export { FerrumQError } from "./errors.js";

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
  deduplicated: boolean;
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
const IDEMPOTENCY_CONFLICT_DETAIL = "idempotency key conflict";

export class FerrumQClient {
  private readonly httpUrl: string;
  private readonly grpcUrl: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly grpcClientOptions: GrpcDataPlaneClientOptions;
  private readonly controlPlane: ControlPlaneClient;
  private dataPlane: DataPlaneClient | null = null;
  private readonly activeHttpControllers = new Set<AbortController>();
  private closed = false;

  constructor(
    options: FerrumQClientOptions & {
      grpcClientOptions?: GrpcDataPlaneClientOptions;
    },
  ) {
    const validated = validateOptions(options);
    this.httpUrl = validated.httpUrl;
    this.grpcUrl = validated.grpcUrl;
    this.timeoutMs = validated.timeoutMs;
    this.fetchImpl = validated.fetchImpl;
    this.grpcClientOptions = options.grpcClientOptions ?? {};
    this.controlPlane = createControlPlaneClient(
      this.httpUrl,
      this.fetchImpl as import("@ferrumq/protocol").FetchLike,
    );
  }

  async health(): Promise<HttpStatusResponse> {
    this.checkClosed();
    return this.executeControl("health", (signal) =>
      this.controlPlane.health({ signal }),
    );
  }

  async readiness(): Promise<HttpStatusResponse> {
    this.checkClosed();
    return this.executeControl("readiness", (signal) =>
      this.controlPlane.ready({ signal }),
    );
  }

  async status(): Promise<BrokerStatusResponse> {
    this.checkClosed();
    return this.executeControl("status", (signal) =>
      this.controlPlane.status({ signal }),
    );
  }

  async createTopic(request: CreateTopicRequest): Promise<TopicResponse> {
    this.checkClosed();
    return this.executeControl(
      "createTopic",
      (signal) =>
        this.controlPlane.createTopic(request.name, request.partitions, {
          signal,
        }),
      { topic: request.name },
    );
  }

  async listTopics(): Promise<TopicResponse[]> {
    this.checkClosed();
    const response = await this.executeControl("listTopics", (signal) =>
      this.controlPlane.listTopics({ signal }),
    );
    return response.items;
  }

  async getTopic(name: string): Promise<TopicResponse> {
    this.checkClosed();
    return this.executeControl(
      "getTopic",
      (signal) => this.controlPlane.getTopic(name, { signal }),
      { topic: name },
    );
  }

  async listDlq(topic?: string): Promise<DlqEntryResponse[]> {
    this.checkClosed();
    const response = await this.executeControl(
      "listDlq",
      (signal) => this.controlPlane.listDlq(topic, { signal }),
      topic === undefined ? undefined : { topic },
    );
    return response.items;
  }

  async metrics(): Promise<string> {
    this.checkClosed();
    const url = new URL("/metrics", `${this.httpUrl}/`).toString();
    return this.executeHttp("metrics", async (signal) => {
      const response = await this.fetchImpl(url, { signal });
      if (!response.ok) {
        throw new FerrumQError(
          `HTTP ${response.status}: ${response.statusText || "request failed"}`,
          {
            transport: "http",
            status: response.status,
            operation: "metrics",
          },
        );
      }
      try {
        return await response.text();
      } catch (error) {
        throw new FerrumQError(
          `HTTP response failed for metrics: ${errorMessage(error)}`,
          {
            transport: "http",
            code: "SDK_INVALID_RESPONSE",
            operation: "metrics",
            cause: error,
          },
        );
      }
    }).catch((error) => {
      if (error instanceof FerrumQError) {
        throw error;
      }
      throw new FerrumQError(
        `HTTP request failed for metrics: ${errorMessage(error)}`,
        {
          transport: "http",
          operation: "metrics",
          cause: error,
        },
      );
    });
  }

  async publish(request: PublishRequest): Promise<PublishResult> {
    this.checkClosed();
    let encoded: ReturnType<typeof encodePayload>;
    try {
      encoded = encodePayload(request.payload);
    } catch (error) {
      if (error instanceof FerrumQError) {
        throw error;
      }
      throw new FerrumQError(
        `Failed to serialize payload for publish: ${errorMessage(error)}`,
        {
          transport: "sdk",
          code: "SDK_SERIALIZATION",
          operation: "publish",
          topic: request.topic,
          cause: error,
        },
      );
    }
    const messageId = nonEmpty(request.messageId) ?? randomUUID();
    const timeUnixMs = request.timeUnixMs ?? Date.now();
    const contentType = nonEmpty(request.contentType) ?? encoded.contentType;
    const type = nonEmpty(request.type) ?? DEFAULT_PUBLISH_TYPE;
    const source = nonEmpty(request.source) ?? DEFAULT_PUBLISH_SOURCE;

    try {
      const response = await this.getDataPlane().publish(
        {
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
        },
        this.grpcCallOptions(),
      );

      return {
        topic: response.topic,
        partition: response.partition,
        offset: response.offset,
        messageId: response.messageId,
        deduplicated: response.deduplicated,
      };
    } catch (error) {
      throw this.wrapGrpcError(error, "publish", { topic: request.topic });
    }
  }

  async consume(request: ConsumeRequest): Promise<ConsumedMessage[]> {
    this.checkClosed();
    const consumerId = nonEmpty(request.consumerId) ?? DEFAULT_CONSUMER_ID;
    const maxMessages = request.maxMessages ?? DEFAULT_MAX_MESSAGES;
    const leaseMs = request.leaseMs ?? DEFAULT_LEASE_MS;
    const nowUnixMs = Date.now();

    try {
      const response = await this.getDataPlane().consume(
        {
          topic: request.topic,
          consumerGroup: request.group,
          consumerId,
          maxMessages,
          leaseMs: String(leaseMs),
          nowUnixMs: String(nowUnixMs),
        },
        this.grpcCallOptions(),
      );

      return response.messages.map(
        (message): ConsumedMessage => ({
          deliveryId: message.deliveryId,
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          messageId: message.messageId,
          key: message.key.length > 0 ? message.key : null,
          payload: new Uint8Array(message.payload),
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
      throw this.wrapGrpcError(error, "consume", { topic: request.topic });
    }
  }

  async ack(request: AckRequest): Promise<void> {
    this.checkClosed();
    const consumerId = nonEmpty(request.consumerId) ?? DEFAULT_CONSUMER_ID;

    try {
      await this.getDataPlane().ack(
        {
          deliveryId: request.deliveryId,
          consumerId,
        },
        this.grpcCallOptions(),
      );
    } catch (error) {
      throw this.wrapGrpcError(error, "ack", {
        deliveryId: request.deliveryId,
      });
    }
  }

  async nack(request: NackRequest): Promise<void> {
    this.checkClosed();
    const consumerId = nonEmpty(request.consumerId) ?? DEFAULT_CONSUMER_ID;

    try {
      await this.getDataPlane().nack(
        {
          deliveryId: request.deliveryId,
          consumerId,
          reason: request.reason ?? "",
        },
        this.grpcCallOptions(),
      );
    } catch (error) {
      throw this.wrapGrpcError(error, "nack", {
        deliveryId: request.deliveryId,
      });
    }
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    for (const controller of this.activeHttpControllers) {
      controller.abort();
    }
    this.activeHttpControllers.clear();
    if (this.dataPlane !== null) {
      this.dataPlane.close();
      this.dataPlane = null;
    }
  }

  private getDataPlane(): DataPlaneClient {
    if (this.dataPlane === null) {
      try {
        this.dataPlane = createGrpcDataPlaneClient(
          this.grpcUrl,
          this.grpcClientOptions,
        );
      } catch (error) {
        throw new FerrumQError(
          `Failed to initialize gRPC client: ${errorMessage(error)}`,
          {
            transport: "grpc",
            code: "SDK_CONFIGURATION",
            operation: "initializeGrpc",
            cause: error,
          },
        );
      }
    }
    return this.dataPlane;
  }

  private executeControl<T>(
    operation: string,
    fn: (signal: AbortSignal) => Promise<T>,
    context?: ErrorContext,
  ): Promise<T> {
    return this.executeHttp(operation, fn, context).catch((error) => {
      if (error instanceof FerrumQError) {
        throw error;
      }
      if (error instanceof ControlPlaneRequestError) {
        throw this.wrapControlError(error, operation, context);
      }
      throw new FerrumQError(
        `HTTP request failed for ${operation}: ${errorMessage(error)}`,
        {
          transport: "http",
          operation,
          ...context,
          cause: error,
        },
      );
    });
  }

  private checkClosed(): void {
    if (this.closed) {
      throw this.closedError();
    }
  }

  private executeHttp<T>(
    operation: string,
    fn: (signal: AbortSignal) => Promise<T>,
    context?: ErrorContext,
  ): Promise<T> {
    const controller = new AbortController();
    this.activeHttpControllers.add(controller);
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const finish = (callback: () => void) => {
        if (settled) {
          return;
        }
        settled = true;
        if (timer !== undefined) {
          clearTimeout(timer);
        }
        controller.signal.removeEventListener("abort", onAbort);
        this.activeHttpControllers.delete(controller);
        callback();
      };
      const onAbort = () =>
        finish(() => {
          reject(
            this.closed
              ? this.closedError(operation, context)
              : this.timeoutError(operation, context),
          );
        });
      controller.signal.addEventListener("abort", onAbort, { once: true });
      const timer =
        this.timeoutMs > 0
          ? setTimeout(() => controller.abort(), this.timeoutMs)
          : undefined;
      Promise.resolve()
        .then(() => fn(controller.signal))
        .then(
          (value) => finish(() => resolve(value)),
          (error) => finish(() => reject(error)),
        );
    });
  }

  private wrapGrpcError(
    error: unknown,
    operation: string,
    context?: ErrorContext,
  ): never {
    if (error instanceof FerrumQError) {
      throw error;
    }

    const candidate = error as {
      code?: unknown;
      details?: unknown;
      message?: unknown;
    };

    const grpcStatus =
      typeof candidate.code === "number"
        ? grpcStatusName(candidate.code)
        : undefined;

    const message =
      typeof candidate.details === "string" && candidate.details.length > 0
        ? candidate.details
        : typeof candidate.message === "string"
          ? candidate.message
          : "gRPC request failed";

    if (this.closed && grpcStatus === "CANCELLED") {
      throw this.closedError(operation, context, error);
    }
    if (grpcStatus === "DEADLINE_EXCEEDED") {
      throw this.timeoutError(operation, context, error, grpcStatus);
    }

    // Normalize idempotency key conflicts to a stable public application error
    // code. The gRPC transport status (ALREADY_EXISTS) is preserved as
    // grpcStatus, but the public code is IDEMPOTENCY_KEY_CONFLICT so callers
    // can detect conflicts consistently across SDK and CLI.
    const conflictCode =
      grpcStatus === "ALREADY_EXISTS" &&
      operation === "publish" &&
      candidate.details === IDEMPOTENCY_CONFLICT_DETAIL
        ? "IDEMPOTENCY_KEY_CONFLICT"
        : grpcStatus;

    const options: ConstructorParameters<typeof FerrumQError>[1] = {
      transport: "grpc",
      code: conflictCode ?? "SDK_INVALID_RESPONSE",
      operation,
      ...context,
      cause: error,
    };
    if (grpcStatus !== undefined) {
      options.grpcStatus = grpcStatus;
    }
    throw new FerrumQError(`gRPC request failed: ${message}`, options);
  }

  private grpcCallOptions(): { deadline?: number } {
    return this.timeoutMs > 0 ? { deadline: Date.now() + this.timeoutMs } : {};
  }

  private wrapControlError(
    error: ControlPlaneRequestError,
    operation: string,
    context?: ErrorContext,
  ): FerrumQError {
    const invalidResponse =
      error.kind === "invalid-json" ||
      error.kind === "schema" ||
      error.kind === "malformed-error";
    const options: ConstructorParameters<typeof FerrumQError>[1] = {
      transport: "http",
      operation,
      ...context,
      cause: error,
    };
    const code =
      error.ferrumqError?.code ??
      (invalidResponse ? "SDK_INVALID_RESPONSE" : undefined);
    if (code !== undefined) {
      options.code = code;
    }
    if (error.status !== undefined) {
      options.status = error.status;
    }
    return new FerrumQError(error.message, options);
  }

  private timeoutError(
    operation: string,
    context?: ErrorContext,
    cause?: unknown,
    grpcStatus?: string,
  ): FerrumQError {
    const options: ConstructorParameters<typeof FerrumQError>[1] = {
      transport: "sdk",
      code: "SDK_TIMEOUT",
      operation,
      ...context,
      cause,
    };
    if (grpcStatus !== undefined) {
      options.grpcStatus = grpcStatus;
    }
    return new FerrumQError(
      `Operation ${operation} timed out after ${this.timeoutMs}ms`,
      options,
    );
  }

  private closedError(
    operation?: string,
    context?: ErrorContext,
    cause?: unknown,
  ): FerrumQError {
    const options: ConstructorParameters<typeof FerrumQError>[1] = {
      transport: "sdk",
      code: "SDK_CLIENT_CLOSED",
      ...context,
      cause,
    };
    if (operation !== undefined) {
      options.operation = operation;
    }
    return new FerrumQError("Client is closed", options);
  }
}

interface ErrorContext {
  topic?: string;
  deliveryId?: string;
}

function nonEmpty(value: string | undefined): string | undefined {
  if (value === undefined || value.length === 0) {
    return undefined;
  }
  return value;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "string") return error;
  return "unexpected error";
}
