import { randomUUID } from "node:crypto";
import { FerrumQClient, FerrumQError } from "@ferrumq/sdk";
import { DEFAULT_POLL_INTERVAL_MS, DEFAULT_TIMEOUT_MS } from "./config.js";
import {
  buildChatMessage,
  DeduplicationCache,
  type DisplayMessage,
  type MalformedMessage,
  makeConsumerGroup,
  makeConsumerId,
  makeTopicName,
  type ParticipantIdentity,
  parseChatMessage,
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

const decoder = new TextDecoder();

export class ChatApp {
  private readonly options: Required<ChatAppOptions>;
  private readonly identity: ParticipantIdentity;
  private readonly deps: ChatAppDeps;
  private readonly dedup: DeduplicationCache;

  private client: FerrumQClient | null = null;
  private controller: AbortController | null = null;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;
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

  async start(): Promise<void> {
    if (this.stopped) {
      return;
    }

    this.emitState({ status: "connecting" });

    try {
      this.client = new FerrumQClient({
        httpUrl: this.options.httpUrl,
        grpcUrl: this.options.grpcUrl,
        timeoutMs: this.options.timeoutMs,
      });

      await this.client.health();
      await this.client.readiness();

      await this.ensureTopic();

      this.emitState({ status: "connected" });
      this.controller = new AbortController();
      this.schedulePoll();
    } catch (error) {
      const startupError = errorMessage(error);
      this.cleanup();
      this.emitState({
        status: "error",
        message: startupError,
      });
      this.emitError(`Failed to connect: ${startupError}`);
    }
  }

  async stop(): Promise<void> {
    this.cleanup();
    this.emitState({ status: "disconnected" });
  }

  private cleanup(): void {
    this.stopped = true;

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
    if (this.stopped || this.client === null) {
      this.emitError("Not connected");
      return false;
    }

    try {
      const chatMsg = buildChatMessage(this.identity, this.options.room, text);
      const payload = JSON.stringify(chatMsg);

      await this.client.publish({
        topic: this.topicName,
        payload,
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      return true;
    } catch (error) {
      this.emitError(`Failed to send message: ${errorMessage(error)}`);
      return false;
    }
  }

  private async ensureTopic(): Promise<void> {
    if (this.client === null) {
      return;
    }

    try {
      await this.client.createTopic({
        name: this.topicName,
        partitions: 1,
      });
      this.deps.onDebug?.(`Created topic ${this.topicName}`);
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
    if (this.stopped) {
      return;
    }

    const delay = this.nextPollDelay();

    this.pollTimer = setTimeout(() => {
      this.poll().catch((err) => {
        this.emitError(`Poll error: ${errorMessage(err)}`);
      });
    }, delay);
  }

  private async poll(): Promise<void> {
    if (this.stopped || this.client === null) {
      return;
    }

    const signal = this.controller?.signal;
    if (signal?.aborted) {
      return;
    }

    try {
      const deliveries = await this.client.consume({
        topic: this.topicName,
        group: this.consumerGroup,
        consumerId: this.consumerId,
        maxMessages: 5,
        leaseMs: 30_000,
      });

      this.pollBackoffMs = 0;

      if (this.lastWarningText !== null) {
        this.lastWarningText = null;
        this.deps.onWarning(null);
      }

      for (const delivery of deliveries) {
        if (this.stopped || signal?.aborted) {
          break;
        }

        const rawText = decoder.decode(delivery.payload);
        const parsed = parseChatMessage(rawText);

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

        if (this.dedup.has(parsed.id)) {
          await this.ackDelivery(delivery.deliveryId);
          continue;
        }

        this.dedup.add(parsed.id);
        const display = toDisplayMessage(parsed, this.identity.sessionId);
        this.deps.onMessage(display);

        await this.ackDelivery(delivery.deliveryId);
      }
    } catch (error) {
      if (this.stopped || signal?.aborted) {
        return;
      }

      if (error instanceof FerrumQError) {
        if (error.code === "SDK_CLIENT_CLOSED") {
          return;
        }
        if (error.transport === "http" || error.transport === "grpc") {
          this.applyBackoff();
          this.emitWarning(`Broker unavailable: ${error.message}`);
        } else if (error.code === "SDK_TIMEOUT") {
          this.applyBackoff();
          this.emitWarning(`Consume timed out: ${error.message}`);
        } else {
          this.emitError(`Chat error: ${error.message}`);
          return;
        }
      } else {
        this.emitError(`Unexpected error: ${errorMessage(error)}`);
        return;
      }
    }

    if (!this.stopped && !signal?.aborted) {
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
      if (this.client !== null) {
        await this.client.ack({
          deliveryId,
          consumerId: this.consumerId,
        });
      }
    } catch {
      this.emitWarning(
        `Could not ACK malformed message delivery ${deliveryId}`,
      );
    }
  }

  private async ackDelivery(deliveryId: string): Promise<void> {
    try {
      if (this.client !== null) {
        await this.client.ack({
          deliveryId,
          consumerId: this.consumerId,
        });
      }
    } catch (error) {
      this.emitWarning(`ACK failed for ${deliveryId}: ${errorMessage(error)}`);
    }
  }

  private applyBackoff(): void {
    const cap = Math.max(30_000, this.options.pollIntervalMs);

    if (this.pollBackoffMs === 0) {
      this.pollBackoffMs = Math.max(this.options.pollIntervalMs, 100);
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
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
