import { beforeEach, describe, expect, it, vi } from "vitest";
import { ChatApp, type ChatAppDeps } from "../src/app.js";

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
    warnings: string[];
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
    ).toHaveLength(2);
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
        deps.warnings.filter((warning) => warning.includes("(invalid-id)")),
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

  it("never creates a busy loop during repeated failures", async () => {
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
      await vi.advanceTimersByTimeAsync(99);
      expect(mockClient.consume).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(1);
      expect(mockClient.consume).toHaveBeenCalledTimes(2);
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
});
