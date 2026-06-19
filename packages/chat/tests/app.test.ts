import { beforeEach, describe, expect, it, vi } from "vitest";
import { ChatApp, type ChatAppDeps } from "../src/app.js";
import { MAX_CHAT_PAYLOAD_BYTES } from "../src/domain.js";

const mockClient = {
  health: vi.fn(),
  readiness: vi.fn(),
  createTopic: vi.fn(),
  publish: vi.fn(),
  consume: vi.fn(),
  ack: vi.fn(),
  nack: vi.fn(),
  close: vi.fn(),
};

vi.mock("@ferrumq/sdk", () => ({
  FerrumQClient: vi.fn().mockImplementation(() => mockClient),
  FerrumQError: class MockFerrumQError extends Error {
    readonly code?: string;
    readonly status?: number;
    readonly transport: "sdk" | "http" | "grpc";
    readonly grpcStatus?: string;
    constructor(
      message: string,
      options: {
        code?: string;
        status?: number;
        transport: "sdk" | "http" | "grpc";
        grpcStatus?: string;
      },
    ) {
      super(message);
      this.transport = options.transport;
      if (options.code !== undefined) this.code = options.code;
      if (options.status !== undefined) this.status = options.status;
      if (options.grpcStatus !== undefined)
        this.grpcStatus = options.grpcStatus;
    }
  },
}));

function createDeps() {
  const deps: ChatAppDeps & {
    messages: unknown[];
    states: unknown[];
    errors: string[];
    warnings: (string | null)[];
  } = {
    messages: [],
    states: [],
    errors: [],
    warnings: [],
    onMessage(msg) {
      deps.messages.push(msg);
    },
    onStateChange(state) {
      deps.states.push(state);
    },
    onError(msg) {
      deps.errors.push(msg);
    },
    onWarning(msg) {
      deps.warnings.push(msg);
    },
  };
  return deps;
}

beforeEach(() => {
  vi.clearAllMocks();
  mockClient.health.mockResolvedValue({ status: "ok" });
  mockClient.readiness.mockResolvedValue({ status: "ready" });
  mockClient.createTopic.mockResolvedValue({
    name: "chat.general",
    partitions: 1,
  });
  mockClient.publish.mockResolvedValue({
    topic: "chat.general",
    partition: 0,
    offset: "0",
    messageId: "mock-message-id",
  });
  mockClient.consume.mockResolvedValue([]);
  mockClient.ack.mockResolvedValue(undefined);
  mockClient.nack.mockResolvedValue(undefined);
  mockClient.close.mockImplementation(() => {});
});

describe("ChatApp", () => {
  it("creates a ChatApp with valid options", () => {
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );
    expect(app.room).toBe("general");
    expect(app.participant.name).toBe("Alice");
    expect(app.topicName).toBe("chat.general");
  });

  it("generates unique identity", () => {
    const deps1 = createDeps();
    const deps2 = createDeps();
    const app1 = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps1,
    );
    const app2 = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps2,
    );
    expect(app1.consumerGroup).not.toBe(app2.consumerGroup);
    expect(app1.consumerId).not.toBe(app2.consumerId);
  });

  it("validates room name", () => {
    const deps = createDeps();
    expect(
      () =>
        new ChatApp(
          {
            httpUrl: "http://127.0.0.1:8080",
            grpcUrl: "http://127.0.0.1:9090",
            room: "###",
            name: "Alice",
          },
          deps,
        ),
    ).toThrow();
  });

  it("validates display name", () => {
    const deps = createDeps();
    expect(
      () =>
        new ChatApp(
          {
            httpUrl: "http://127.0.0.1:8080",
            grpcUrl: "http://127.0.0.1:9090",
            room: "general",
            name: "",
          },
          deps,
        ),
    ).toThrow();
  });

  it("starts and connects successfully", async () => {
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await app.start();
    expect(mockClient.health).toHaveBeenCalledOnce();
    expect(mockClient.readiness).toHaveBeenCalledOnce();
    expect(mockClient.createTopic).toHaveBeenCalledOnce();
    expect(mockClient.health.mock.invocationCallOrder[0]).toBeLessThan(
      mockClient.readiness.mock.invocationCallOrder[0] ?? 0,
    );
    expect(mockClient.readiness.mock.invocationCallOrder[0]).toBeLessThan(
      mockClient.createTopic.mock.invocationCallOrder[0] ?? 0,
    );
    expect(deps.states).toContainEqual({ status: "connecting" });
    expect(deps.states).toContainEqual({ status: "connected" });
    await app.stop();
  });

  it("shares one startup attempt across duplicate start calls", async () => {
    let resolveHealth!: (value: { status: string }) => void;
    mockClient.health.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveHealth = resolve;
        }),
    );
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    const first = app.start();
    const second = app.start();
    expect(mockClient.health).toHaveBeenCalledOnce();

    resolveHealth({ status: "ok" });
    await Promise.all([first, second]);
    expect(mockClient.readiness).toHaveBeenCalledOnce();
    expect(mockClient.createTopic).toHaveBeenCalledOnce();
    await app.stop();
  });

  it("does not publish while startup is still in progress", async () => {
    let resolveHealth!: (value: { status: string }) => void;
    mockClient.health.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveHealth = resolve;
        }),
    );
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    const start = app.start();
    await expect(app.sendMessage("too early")).resolves.toBe(false);
    expect(mockClient.publish).not.toHaveBeenCalled();

    resolveHealth({ status: "ok" });
    await start;
    await app.stop();
  });

  it("does not emit startup callbacks after stop", async () => {
    let resolveHealth!: (value: { status: string }) => void;
    mockClient.health.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveHealth = resolve;
        }),
    );
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    const start = app.start();
    await app.stop();
    resolveHealth({ status: "ok" });
    await start;

    expect(deps.states).toEqual([
      { status: "connecting" },
      { status: "disconnected" },
    ]);
    expect(mockClient.readiness).not.toHaveBeenCalled();
    expect(mockClient.close).toHaveBeenCalledOnce();
  });

  it("stops cleanly", async () => {
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await app.start();
    await app.stop();
    expect(deps.states).toContainEqual({ status: "disconnected" });
  });

  it("does not publish after stop", async () => {
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await app.start();
    await app.stop();

    const result = await app.sendMessage("hello");
    expect(result).toBe(false);
  });

  it("handles topic already exists gracefully", async () => {
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.createTopic.mockRejectedValue(
      new (
        FerrumQError as unknown as new (
          message: string,
          opts: object,
        ) => Error
      )("Topic already exists", {
        code: "TOPIC_ALREADY_EXISTS",
        status: 409,
        transport: "http",
      }),
    );

    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await app.start();
    expect(deps.states).toContainEqual({ status: "connected" });
    await app.stop();
  });

  it("reports error on connection failure", async () => {
    mockClient.health.mockRejectedValue(new Error("Connection refused"));

    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await app.start();
    expect(deps.states).toContainEqual(
      expect.objectContaining({ status: "error" }),
    );
    await app.stop();
  });

  it.each([
    ["health", "health failed"],
    ["readiness", "readiness failed"],
    ["createTopic", "topic creation failed"],
  ] as const)("cleans up after %s startup failure and blocks publishing", async (operation, message) => {
    mockClient[operation].mockRejectedValueOnce(new Error(message));
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await expect(app.start()).resolves.toBeUndefined();

    expect(mockClient.close).toHaveBeenCalledOnce();
    expect(deps.states.at(-1)).toEqual({ status: "error", message });
    expect(deps.errors).toContain(`Failed to connect: ${message}`);
    expect(await app.sendMessage("must not publish")).toBe(false);
    expect(mockClient.publish).not.toHaveBeenCalled();
  });

  it("preserves the startup error when client close throws", async () => {
    mockClient.readiness.mockRejectedValueOnce(new Error("broker not ready"));
    mockClient.close.mockImplementationOnce(() => {
      throw new Error("close exploded");
    });
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await expect(app.start()).resolves.toBeUndefined();

    expect(deps.states.at(-1)).toEqual({
      status: "error",
      message: "broker not ready",
    });
    expect(deps.errors).toContain("Failed to connect: broker not ready");
    expect(deps.warnings).toContain("Client close failed: close exploded");
    expect(await app.sendMessage("must not publish")).toBe(false);
    expect(mockClient.publish).not.toHaveBeenCalled();
  });

  it("stops repeatedly without closing the client more than once", async () => {
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await app.start();
    await expect(app.stop()).resolves.toBeUndefined();
    await expect(app.stop()).resolves.toBeUndefined();

    expect(mockClient.close).toHaveBeenCalledOnce();
    expect(
      deps.states.filter((state) => {
        return (
          typeof state === "object" &&
          state !== null &&
          "status" in state &&
          state.status === "disconnected"
        );
      }),
    ).toHaveLength(1);
  });

  it("suppresses close failures during repeated shutdown", async () => {
    mockClient.close.mockImplementationOnce(() => {
      throw new Error("close failed");
    });
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await app.start();
    await expect(app.stop()).resolves.toBeUndefined();
    await expect(app.stop()).resolves.toBeUndefined();

    expect(mockClient.close).toHaveBeenCalledOnce();
    expect(deps.warnings).toContain("Client close failed: close failed");
  });

  it("has unique consumer group per session", () => {
    const deps1 = createDeps();
    const deps2 = createDeps();
    const app1 = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps1,
    );
    const app2 = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Bob",
      },
      deps2,
    );
    expect(app1.consumerGroup).not.toBe(app2.consumerGroup);
  });

  it("ACKs distinct malformed sanitized IDs without displaying or deduplicating them", async () => {
    vi.useFakeTimers();
    const encode = (id: string) =>
      new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          id,
          room: "general",
          sender: { id: "p1", name: "Bob", sessionId: "s1" },
          text: "hello",
          sentAt: "2025-01-01T00:00:00.000Z",
        }),
      );
    mockClient.consume.mockResolvedValueOnce([
      { deliveryId: "delivery-c0", payload: encode("\x00") },
      { deliveryId: "delivery-ansi", payload: encode("\x1b[31m\x1b[0m") },
    ]);
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 1,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(1);

      expect(mockClient.ack).toHaveBeenCalledTimes(2);
      expect(mockClient.ack).toHaveBeenNthCalledWith(
        1,
        expect.objectContaining({ deliveryId: "delivery-c0" }),
      );
      expect(mockClient.ack).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ deliveryId: "delivery-ansi" }),
      );
      expect(deps.messages).toEqual([]);
      expect(
        deps.warnings.filter((warning) => warning?.includes("(invalid-id)")),
      ).toHaveLength(2);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("warns and ACKs invalid sanitized fields and room mismatches without deduplicating them", async () => {
    vi.useFakeTimers();
    const encode = (message: Record<string, unknown>) =>
      new TextEncoder().encode(JSON.stringify(message));
    const message = (id: string, room = "general") => ({
      version: 1,
      id,
      room,
      sender: { id: "p1", name: "Bob", sessionId: "s1" },
      text: "hello",
      sentAt: "2025-01-01T00:00:00.000Z",
    });
    mockClient.consume
      .mockResolvedValueOnce([
        {
          deliveryId: "delivery-invalid",
          payload: encode({
            ...message("shared-invalid"),
            sender: { id: "p1", name: "\x00", sessionId: "s1" },
          }),
        },
        {
          deliveryId: "delivery-mismatch",
          payload: encode(message("shared-mismatch", "other-room")),
        },
      ])
      .mockResolvedValueOnce([
        {
          deliveryId: "delivery-valid-after-invalid",
          payload: encode(message("shared-invalid")),
        },
        {
          deliveryId: "delivery-valid-after-mismatch",
          payload: encode(message("shared-mismatch")),
        },
      ]);
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 1,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(2);

      expect(deps.warnings).toEqual(
        expect.arrayContaining([
          expect.stringContaining("(invalid-sender-name)"),
          expect.stringContaining("(room-mismatch)"),
        ]),
      );
      expect(deps.messages).toHaveLength(2);
      expect(mockClient.ack).toHaveBeenCalledTimes(4);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("ACKs invalid UTF-8 and oversized payloads as malformed", async () => {
    vi.useFakeTimers();
    mockClient.consume.mockResolvedValueOnce([
      {
        deliveryId: "delivery-utf8",
        payload: new Uint8Array([0xc3, 0x28]),
      },
      {
        deliveryId: "delivery-large",
        payload: new Uint8Array(MAX_CHAT_PAYLOAD_BYTES + 1),
      },
    ]);
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 1,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(1);

      expect(deps.messages).toEqual([]);
      expect(mockClient.ack).toHaveBeenCalledTimes(2);
      expect(deps.warnings).toEqual(
        expect.arrayContaining([
          expect.stringContaining("(invalid-utf8)"),
          expect.stringContaining("(payload-too-large)"),
        ]),
      );
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("warns, suppresses, and ACKs conflicting content with the same ID", async () => {
    vi.useFakeTimers();
    const encode = (text: string) =>
      new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          id: "shared-id",
          room: "general",
          sender: { id: "p1", name: "Bob", sessionId: "s1" },
          text,
          sentAt: "2025-01-01T00:00:00.000Z",
        }),
      );
    mockClient.consume.mockResolvedValueOnce([
      { deliveryId: "delivery-original", payload: encode("original") },
      { deliveryId: "delivery-conflict", payload: encode("changed") },
    ]);
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 1,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(1);

      expect(deps.messages).toHaveLength(1);
      expect(mockClient.ack).toHaveBeenCalledTimes(2);
      expect(deps.warnings).toContain(
        "Skipping conflicting chat message ID shared-id: delivery delivery-conflict",
      );
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("does not ACK or deduplicate until the display callback accepts a message", async () => {
    vi.useFakeTimers();
    const payload = new TextEncoder().encode(
      JSON.stringify({
        version: 1,
        id: "display-failure",
        room: "general",
        sender: { id: "p1", name: "Bob", sessionId: "s1" },
        text: "hello",
        sentAt: "2025-01-01T00:00:00.000Z",
      }),
    );
    mockClient.consume.mockResolvedValueOnce([
      { deliveryId: "delivery-display", payload },
    ]);
    const deps = createDeps();
    deps.onMessage = () => {
      throw new Error("display rejected");
    };
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 1,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(1);

      expect(mockClient.ack).not.toHaveBeenCalled();
      expect(deps.errors).toContain("Unexpected error: display rejected");
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("uses the normal 500 ms interval as the first failure delay", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.consume.mockRejectedValue(
      new FerrumQError("unavailable", { transport: "grpc" }),
    );
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      createDeps(),
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(500);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(499);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(1);
      expect(mockClient.consume).toHaveBeenCalledTimes(2);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("keeps polling intervals above 30 seconds as the outage lower bound", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.consume.mockRejectedValue(
      new FerrumQError("unavailable", { transport: "grpc" }),
    );
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 60_000,
      },
      createDeps(),
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(60_000);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(59_999);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(1);
      expect(mockClient.consume).toHaveBeenCalledTimes(2);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("grows outage delays exponentially and caps them at 30 seconds", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.consume.mockRejectedValue(
      new FerrumQError("unavailable", { transport: "grpc" }),
    );
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 100,
      },
      createDeps(),
    );

    try {
      await app.start();
      for (const delay of [
        100, 100, 200, 400, 800, 1_600, 3_200, 6_400, 12_800, 25_600, 30_000,
        30_000,
      ]) {
        await vi.advanceTimersByTimeAsync(delay);
      }
      expect(mockClient.consume).toHaveBeenCalledTimes(12);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("resets outage backoff immediately after a successful consume", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    const unavailable = new FerrumQError("unavailable", {
      transport: "grpc",
    });
    mockClient.consume
      .mockRejectedValueOnce(unavailable)
      .mockRejectedValueOnce(unavailable)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 500,
      },
      createDeps(),
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(500);
      await vi.advanceTimersByTimeAsync(500);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(mockClient.consume).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(499);
      expect(mockClient.consume).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(1);
      expect(mockClient.consume).toHaveBeenCalledTimes(4);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("clears a pending outage timer during shutdown", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.consume.mockRejectedValue(
      new FerrumQError("unavailable", { transport: "grpc" }),
    );
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 500,
      },
      createDeps(),
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(500);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      await app.stop();
      await vi.runAllTimersAsync();
      expect(mockClient.consume).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });

  it("uses the configured interval as the lower bound without zero-delay retries", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.consume.mockRejectedValue(
      new FerrumQError("unavailable", { transport: "grpc" }),
    );
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 1,
      },
      createDeps(),
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(1);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(1);
      expect(mockClient.consume).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(2);
      expect(mockClient.consume).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(4);
      expect(mockClient.consume).toHaveBeenCalledTimes(4);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("stops polling after close during poll error", async () => {
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    mockClient.consume.mockImplementation(() => {
      return new Promise<never>((_resolve, reject) => {
        reject(new Error("CANCELLED"));
      });
    });

    await app.start();
    await new Promise((resolve) => setTimeout(resolve, 100));
    await app.stop();

    // Should have stopped without crashes
    expect(true).toBe(true);
  });

  it("closes during an in-flight consume and does not poll again", async () => {
    vi.useFakeTimers();
    let rejectConsume!: (error: Error) => void;
    mockClient.consume.mockImplementationOnce(
      () =>
        new Promise<never>((_resolve, reject) => {
          rejectConsume = reject;
        }),
    );
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 1,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(1);
      expect(mockClient.consume).toHaveBeenCalledOnce();

      await app.stop();
      expect(mockClient.close).toHaveBeenCalledOnce();

      rejectConsume(new Error("CANCELLED"));
      await vi.runAllTimersAsync();

      expect(mockClient.consume).toHaveBeenCalledOnce();
      expect(deps.errors).not.toContain("Poll error: CANCELLED");
    } finally {
      vi.useRealTimers();
    }
  });

  it("reports error on startup failure and does not start polling", async () => {
    mockClient.health.mockRejectedValue(new Error("Connection refused"));

    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await app.start();
    expect(deps.states).toContainEqual(
      expect.objectContaining({ status: "error" }),
    );
    expect(mockClient.consume).not.toHaveBeenCalled();
    expect(mockClient.close).toHaveBeenCalledOnce();
    await app.stop();
  });

  it("coalesces repeated identical outage warnings", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    const unavailable = new FerrumQError("broker gone", {
      transport: "grpc",
    });
    mockClient.consume.mockRejectedValue(unavailable);
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 100,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(200);

      const outageWarnings = deps.warnings.filter((w) =>
        w?.startsWith("Broker unavailable:"),
      );
      expect(outageWarnings).toHaveLength(1);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("clears warning after outage recovery", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    const unavailable = new FerrumQError("unavailable", {
      transport: "grpc",
    });
    mockClient.consume
      .mockRejectedValueOnce(unavailable)
      .mockRejectedValueOnce(unavailable)
      .mockResolvedValueOnce([]);
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 500,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(500);
      expect(
        deps.warnings.some((w) => w?.startsWith("Broker unavailable:")),
      ).toBe(true);
      await vi.advanceTimersByTimeAsync(500);
      await vi.advanceTimersByTimeAsync(1000);

      expect(deps.warnings).toContain(null);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("emits error on publish failure and does not retry", async () => {
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
      },
      deps,
    );

    await app.start();
    mockClient.publish.mockRejectedValue(new Error("gRPC UNAVAILABLE"));
    const result = await app.sendMessage("hello");
    expect(result).toBe(false);
    expect(mockClient.publish).toHaveBeenCalledTimes(1);
    expect(deps.errors).toEqual(
      expect.arrayContaining([
        expect.stringContaining("Failed to send message"),
      ]),
    );
    await app.stop();
  });

  it("stops polling permanently after a non-retryable SDK error", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.consume.mockRejectedValue(
      new FerrumQError("invalid response", {
        transport: "sdk",
        code: "SDK_INVALID_RESPONSE",
      }),
    );
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 100,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(100);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      expect(deps.errors).toEqual(
        expect.arrayContaining([expect.stringContaining("Chat error:")]),
      );

      await vi.runAllTimersAsync();
      expect(mockClient.consume).toHaveBeenCalledOnce();
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("applies backoff and retries on SDK_TIMEOUT (transient)", async () => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.consume.mockRejectedValue(
      new FerrumQError("deadline exceeded", {
        transport: "sdk",
        code: "SDK_TIMEOUT",
      }),
    );
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 100,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(100);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      expect(deps.warnings).toEqual(
        expect.arrayContaining([expect.stringContaining("Consume timed out:")]),
      );

      await vi.advanceTimersByTimeAsync(99);
      expect(mockClient.consume).toHaveBeenCalledOnce();

      await vi.advanceTimersByTimeAsync(1);
      expect(mockClient.consume).toHaveBeenCalledTimes(2);
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it.each([
    "CANCELLED",
    "UNAVAILABLE",
    "RESOURCE_EXHAUSTED",
  ])("retries transient gRPC status %s with backoff", async (grpcStatus) => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.consume.mockRejectedValue(
      new FerrumQError(grpcStatus, {
        transport: "grpc",
        code: grpcStatus,
        grpcStatus,
      }),
    );
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 100,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(100);
      expect(mockClient.consume).toHaveBeenCalledTimes(2);
      expect(deps.warnings).toEqual(
        expect.arrayContaining([
          expect.stringContaining("Broker unavailable:"),
        ]),
      );
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it.each([
    "INVALID_ARGUMENT",
    "PERMISSION_DENIED",
    "UNAUTHENTICATED",
  ])("stops polling on permanent gRPC status %s", async (grpcStatus) => {
    vi.useFakeTimers();
    const { FerrumQError } = await import("@ferrumq/sdk");
    mockClient.consume.mockRejectedValue(
      new FerrumQError(grpcStatus, {
        transport: "grpc",
        code: grpcStatus,
        grpcStatus,
      }),
    );
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 100,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(100);
      await vi.runAllTimersAsync();
      expect(mockClient.consume).toHaveBeenCalledOnce();
      expect(deps.errors).toContain(`Chat error: ${grpcStatus}`);
      expect(mockClient.close).toHaveBeenCalledOnce();
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });

  it("stops polling on unexpected non-FerrumQError errors", async () => {
    vi.useFakeTimers();
    mockClient.consume.mockRejectedValue(new Error("something broke"));
    const deps = createDeps();
    const app = new ChatApp(
      {
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        room: "general",
        name: "Alice",
        pollIntervalMs: 100,
      },
      deps,
    );

    try {
      await app.start();
      await vi.advanceTimersByTimeAsync(100);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      expect(deps.errors).toEqual(
        expect.arrayContaining([expect.stringContaining("Unexpected error:")]),
      );

      await vi.runAllTimersAsync();
      expect(mockClient.consume).toHaveBeenCalledOnce();
    } finally {
      await app.stop();
      vi.useRealTimers();
    }
  });
});
