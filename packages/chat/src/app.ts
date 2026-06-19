import { randomUUID } from "node:crypto";
import { FerrumQClient, FerrumQError } from "@ferrumq/sdk";
import { DEFAULT_POLL_INTERVAL_MS, DEFAULT_TIMEOUT_MS } from "./config.js";
import {
  buildChatMessage,
  DeduplicationCache,
  type DisplayMessage,
  fingerprintChatMessage,
  type MalformedMessage,
  makeConsumerGroup,
  makeConsumerId,
  makeTopicName,
  type ParticipantIdentity,
  parseChatPayload,
  toDisplayMessage,
  validateName,
  validateRoom,
} from "./domain.js";

export interface ChatAppOptions {
  httpUrl: string;
  grpcUrl: string;
  room: string;
  name: string;
  timeoutMs?: number;
  pollIntervalMs?: number;
}

export type ChatState =
  | { status: "disconnected" }
  | { status: "connecting" }
  | { status: "connected" }
  | { status: "error"; message: string };

export interface ChatAppDeps {
  onMessage: (message: DisplayMessage) => void;
  onStateChange: (state: ChatState) => void;
  onError: (message: string) => void;
  onWarning: (message: string | null) => void;
  onDebug?: (message: string) => void;
}

type ChatLifecycle = "idle" | "starting" | "connected" | "failed" | "stopped";

const TRANSIENT_GRPC_STATUSES = new Set([
  "CANCELLED",
  "UNKNOWN",
  "DEADLINE_EXCEEDED",
  "RESOURCE_EXHAUSTED",
  "ABORTED",
  "INTERNAL",
  "UNAVAILABLE",
]);

export class ChatApp {
  private readonly options: Required<ChatAppOptions>;
  private readonly identity: ParticipantIdentity;
  private readonly deps: ChatAppDeps;
  private readonly dedup: DeduplicationCache;

  private client: FerrumQClient | null = null;
  private controller: AbortController | null = null;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private lifecycle: ChatLifecycle = "idle";
  private startPromise: Promise<void> | null = null;
  private pollBackoffMs = 0;
  private lastWarningText: string | null = null;

  constructor(options: ChatAppOptions, deps: ChatAppDeps) {
    const room = validateRoom(options.room);
    const name = validateName(options.name);

    this.options = {
      httpUrl: options.httpUrl,
      grpcUrl: options.grpcUrl,
      room,
      name,
      timeoutMs: options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
      pollIntervalMs: options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
    };

    this.identity = {
      id: randomUUID(),
      name,
      sessionId: randomUUID(),
    };

    this.deps = deps;
    this.dedup = new DeduplicationCache();
  }

  get participant(): Readonly<ParticipantIdentity> {
    return this.identity;
  }

  get room(): string {
    return this.options.room;
  }

  get consumerGroup(): string {
    return makeConsumerGroup(this.options.room, this.identity.sessionId);
  }

  get consumerId(): string {
    return makeConsumerId(this.identity.sessionId);
  }

  get topicName(): string {
    return makeTopicName(this.options.room);
  }

  start(): Promise<void> {
    if (this.lifecycle === "starting") {
      return this.startPromise ?? Promise.resolve();
    }
    if (this.lifecycle !== "idle") {
      return Promise.resolve();
    }

    this.lifecycle = "starting";
    this.startPromise = this.performStart();
    return this.startPromise;
  }

  private async performStart(): Promise<void> {
    this.emitState({ status: "connecting" });

    try {
      const client = new FerrumQClient({
        httpUrl: this.options.httpUrl,
        grpcUrl: this.options.grpcUrl,
        timeoutMs: this.options.timeoutMs,
      });
      this.client = client;

      await client.health();
      if (!this.isStartingWith(client)) return;
      await client.readiness();
      if (!this.isStartingWith(client)) return;

      await this.ensureTopic(client);
      if (!this.isStartingWith(client)) return;

      this.lifecycle = "connected";
      this.emitState({ status: "connected" });
      this.controller = new AbortController();
      this.schedulePoll();
    } catch (error) {
      if (this.lifecycle === "stopped") {
        return;
      }
      const startupError = errorMessage(error);
      this.lifecycle = "failed";
      this.cleanupResources();
      this.emitState({
        status: "error",
        message: startupError,
      });
      this.emitError(`Failed to connect: ${startupError}`);
    } finally {
      this.startPromise = null;
    }
  }

  async stop(): Promise<void> {
    if (this.lifecycle === "stopped") {
      return;
    }
    this.lifecycle = "stopped";
    this.cleanupResources();
    this.emitState({ status: "disconnected" });
  }

  private cleanupResources(): void {
    if (this.pollTimer !== null) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }

    if (this.controller !== null) {
      this.controller.abort();
      this.controller = null;
    }

    if (this.client !== null) {
      const client = this.client;
      this.client = null;
      try {
        client.close();
      } catch (error) {
        this.emitWarning(`Client close failed: ${errorMessage(error)}`);
      }
    }
  }

  async sendMessage(text: string): Promise<boolean> {
    if (this.lifecycle !== "connected" || this.client === null) {
      this.emitError("Not connected");
      return false;
    }

    const client = this.client;
    try {
      const chatMsg = buildChatMessage(this.identity, this.options.room, text);
      const payload = JSON.stringify(chatMsg);

      await client.publish({
        topic: this.topicName,
        payload,
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      return true;
    } catch (error) {
      if (this.isStopped()) {
        return false;
      }
      this.emitError(`Failed to send message: ${errorMessage(error)}`);
      return false;
    }
  }

  private async ensureTopic(client: FerrumQClient): Promise<void> {
    if (!this.isStartingWith(client)) {
      return;
    }

    try {
      await client.createTopic({
        name: this.topicName,
        partitions: 1,
      });
      if (this.isStartingWith(client)) {
        this.deps.onDebug?.(`Created topic ${this.topicName}`);
      }
    } catch (error) {
      if (
        error instanceof FerrumQError &&
        (error.code === "TOPIC_ALREADY_EXISTS" ||
          error.status === 409 ||
          error.code === "ALREADY_EXISTS")
      ) {
        return;
      }
      throw error;
    }
  }

  private schedulePoll(): void {
    if (this.lifecycle !== "connected" || this.pollTimer !== null) {
      return;
    }

    const delay = this.nextPollDelay();

    this.pollTimer = setTimeout(() => {
      this.pollTimer = null;
      this.poll().catch((err) => {
        if (this.lifecycle === "connected") {
          this.failPolling(`Poll error: ${errorMessage(err)}`);
        }
      });
    }, delay);
  }

  private async poll(): Promise<void> {
    if (this.lifecycle !== "connected" || this.client === null) {
      return;
    }

    const client = this.client;
    const signal = this.controller?.signal;
    if (signal?.aborted) {
      return;
    }

    try {
      const deliveries = await client.consume({
        topic: this.topicName,
        group: this.consumerGroup,
        consumerId: this.consumerId,
        maxMessages: 5,
        leaseMs: 30_000,
      });

      if (!this.isConnectedWith(client) || signal?.aborted) {
        return;
      }

      this.pollBackoffMs = 0;

      if (this.lastWarningText !== null) {
        this.lastWarningText = null;
        this.deps.onWarning(null);
      }

      for (const delivery of deliveries) {
        if (!this.isConnectedWith(client) || signal?.aborted) {
          break;
        }

        const parsed = parseChatPayload(delivery.payload);

        if ("kind" in parsed) {
          await this.handleMalformedMessage(delivery.deliveryId, parsed);
          continue;
        }

        if (parsed.room !== this.options.room) {
          await this.handleMalformedMessage(delivery.deliveryId, {
            kind: "room-mismatch",
          });
          continue;
        }

        const fingerprint = fingerprintChatMessage(parsed);
        const deduplication = this.dedup.observe(parsed.id, fingerprint);
        if (deduplication === "duplicate") {
          await this.ackDelivery(delivery.deliveryId);
          continue;
        }
        if (deduplication === "conflict") {
          this.emitWarning(
            `Skipping conflicting chat message ID ${parsed.id}: delivery ${delivery.deliveryId}`,
          );
          await this.ackDelivery(delivery.deliveryId);
          continue;
        }

        const display = toDisplayMessage(parsed, this.identity.sessionId);
        this.deps.onMessage(display);
        this.dedup.add(parsed.id, fingerprint);

        await this.ackDelivery(delivery.deliveryId);
      }
    } catch (error) {
      if (
        !this.isConnectedWith(client) ||
        signal?.aborted ||
        this.client !== client
      ) {
        return;
      }

      const classification = classifyConsumeError(error);
      if (classification === "timeout") {
        this.applyBackoff();
        this.emitWarning(`Consume timed out: ${errorMessage(error)}`);
      } else if (classification === "transient") {
        this.applyBackoff();
        this.emitWarning(`Broker unavailable: ${errorMessage(error)}`);
      } else {
        this.failPolling(
          error instanceof FerrumQError
            ? `Chat error: ${error.message}`
            : `Unexpected error: ${errorMessage(error)}`,
        );
        return;
      }
    }

    if (this.lifecycle === "connected" && !signal?.aborted) {
      this.schedulePoll();
    }
  }

  private async handleMalformedMessage(
    deliveryId: string,
    malformed: MalformedMessage,
  ): Promise<void> {
    const kind = malformed.kind;
    this.emitWarning(
      `Skipping malformed chat message (${kind}): delivery ${deliveryId}`,
    );

    try {
      const client = this.client;
      if (this.lifecycle === "connected" && client !== null) {
        await client.ack({
          deliveryId,
          consumerId: this.consumerId,
        });
      }
    } catch {
      if (this.lifecycle === "connected") {
        this.emitWarning(
          `Could not ACK malformed message delivery ${deliveryId}`,
        );
      }
    }
  }

  private async ackDelivery(deliveryId: string): Promise<void> {
    try {
      const client = this.client;
      if (this.lifecycle === "connected" && client !== null) {
        await client.ack({
          deliveryId,
          consumerId: this.consumerId,
        });
      }
    } catch (error) {
      if (this.lifecycle === "connected") {
        this.emitWarning(
          `ACK failed for ${deliveryId}: ${errorMessage(error)}`,
        );
      }
    }
  }

  private applyBackoff(): void {
    const cap = Math.max(30_000, this.options.pollIntervalMs);

    if (this.pollBackoffMs === 0) {
      this.pollBackoffMs = this.options.pollIntervalMs;
    } else {
      this.pollBackoffMs =
        this.pollBackoffMs >= cap / 2
          ? cap
          : Math.min(this.pollBackoffMs * 2, cap);
    }
  }

  private nextPollDelay(): number {
    return this.pollBackoffMs > 0
      ? this.pollBackoffMs
      : this.options.pollIntervalMs;
  }

  private emitState(state: ChatState): void {
    this.deps.onStateChange(state);
  }

  private emitError(message: string): void {
    this.deps.onError(message);
  }

  private emitWarning(message: string): void {
    if (message === this.lastWarningText) {
      return;
    }
    this.lastWarningText = message;
    this.deps.onWarning(message);
  }

  private isStartingWith(client: FerrumQClient): boolean {
    return this.lifecycle === "starting" && this.client === client;
  }

  private isConnectedWith(client: FerrumQClient): boolean {
    return this.lifecycle === "connected" && this.client === client;
  }

  private isStopped(): boolean {
    return this.lifecycle === "stopped";
  }

  private failPolling(message: string): void {
    if (this.lifecycle !== "connected") {
      return;
    }
    this.lifecycle = "failed";
    this.cleanupResources();
    this.emitState({ status: "error", message });
    this.emitError(message);
  }
}

type ConsumeErrorClassification = "timeout" | "transient" | "permanent";

function classifyConsumeError(error: unknown): ConsumeErrorClassification {
  if (!(error instanceof FerrumQError)) {
    return "permanent";
  }
  if (error.code === "SDK_TIMEOUT") {
    return "timeout";
  }
  if (error.transport !== "grpc") {
    return "permanent";
  }

  const status = error.grpcStatus ?? error.code;
  if (status === undefined || TRANSIENT_GRPC_STATUSES.has(status)) {
    return "transient";
  }
  return "permanent";
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
