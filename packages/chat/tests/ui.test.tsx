import { FerrumQClient } from "@ferrumq/sdk";
import { render } from "ink-testing-library";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  appendMessageHistory,
  ChatUi,
  type ChatUiConfig,
  MAX_MESSAGE_HISTORY,
} from "../src/ui.js";

interface MockClient {
  health: ReturnType<typeof vi.fn>;
  readiness: ReturnType<typeof vi.fn>;
  createTopic: ReturnType<typeof vi.fn>;
  publish: ReturnType<typeof vi.fn>;
  consume: ReturnType<typeof vi.fn>;
  ack: ReturnType<typeof vi.fn>;
  nack: ReturnType<typeof vi.fn>;
  close: ReturnType<typeof vi.fn>;
}

vi.mock("@ferrumq/sdk", () => ({
  FerrumQClient: vi.fn().mockImplementation(
    (): MockClient => ({
      health: vi.fn().mockResolvedValue({ status: "ok" }),
      readiness: vi.fn().mockResolvedValue({ status: "ready" }),
      createTopic: vi
        .fn()
        .mockResolvedValue({ name: "chat.general", partitions: 1 }),
      publish: vi.fn().mockResolvedValue(undefined),
      consume: vi.fn().mockResolvedValue([]),
      ack: vi.fn().mockResolvedValue(undefined),
      nack: vi.fn().mockResolvedValue(undefined),
      close: vi.fn(),
    }),
  ),
  FerrumQError: class MockFerrumQError extends Error {
    readonly code?: string;
    readonly status?: number;
    readonly transport: "sdk" | "http" | "grpc";
    constructor(
      message: string,
      options: {
        code?: string;
        status?: number;
        transport: "sdk" | "http" | "grpc";
      },
    ) {
      super(message);
      this.transport = options.transport;
      if (options.code !== undefined) this.code = options.code;
      if (options.status !== undefined) this.status = options.status;
    }
  },
}));

const baseConfig: ChatUiConfig = {
  httpUrl: "http://127.0.0.1:8080",
  grpcUrl: "http://127.0.0.1:9090",
  room: "general",
  name: "Alice",
  timeoutMs: 10_000,
  pollIntervalMs: 500,
};

describe("ChatUi", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("renders the header with room and name", () => {
    const { lastFrame } = render(<ChatUi config={baseConfig} />);

    const output = lastFrame() ?? "";
    expect(output).toContain("FerrumQ Chat");
    expect(output).toContain("general");
    expect(output).toContain("Alice");
  });

  it("renders the input prompt", () => {
    const { lastFrame } = render(<ChatUi config={baseConfig} />);

    const output = lastFrame() ?? "";
    expect(output).toContain("Enter");
    expect(output).toContain("Esc");
    expect(output).toContain("quit");
  });

  it("shows disconnected state initially", () => {
    const { lastFrame } = render(<ChatUi config={baseConfig} />);

    expect(lastFrame() ?? "").toContain("disconnected");
  });

  it("shows empty state message", () => {
    const { lastFrame } = render(<ChatUi config={baseConfig} />);

    expect(lastFrame() ?? "").toContain("No messages yet");
  });

  it("creates one application lifecycle on initial mount", async () => {
    const view = render(<ChatUi config={baseConfig} />);

    await flushEffects();

    expect(FerrumQClient).toHaveBeenCalledOnce();
    expect(clientAt(0).close).not.toHaveBeenCalled();

    view.unmount();
    await flushEffects();
  });

  it("preserves the client for a distinct scalar-equivalent config", async () => {
    const view = render(<ChatUi config={baseConfig} />);
    await flushEffects();

    view.rerender(<ChatUi config={{ ...baseConfig }} />);
    await flushEffects();

    expect(FerrumQClient).toHaveBeenCalledOnce();
    expect(clientAt(0).close).not.toHaveBeenCalled();

    view.unmount();
    await flushEffects();
    expect(clientAt(0).close).toHaveBeenCalledOnce();
  });

  it("keeps one polling loop across equivalent and unrelated parent rerenders", async () => {
    vi.useFakeTimers();
    const Parent = ({
      label,
      config,
    }: {
      label: string;
      config: ChatUiConfig;
    }) => <ChatUi config={config} onExit={() => label.length} />;
    const view = render(<Parent label="first" config={baseConfig} />);
    await vi.advanceTimersByTimeAsync(0);

    view.rerender(<Parent label="second" config={{ ...baseConfig }} />);
    view.rerender(<Parent label="third" config={{ ...baseConfig }} />);
    await vi.advanceTimersByTimeAsync(500);

    expect(FerrumQClient).toHaveBeenCalledOnce();
    expect(clientAt(0).consume).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(1);

    view.unmount();
    await vi.runAllTimersAsync();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([
    {
      field: "httpUrl",
      value: "http://broker.example:8080",
      assertUpdated(client: MockClient, frame: string) {
        expect(FerrumQClient).toHaveBeenLastCalledWith(
          expect.objectContaining({ httpUrl: "http://broker.example:8080" }),
        );
        expect(client.createTopic).toHaveBeenCalledWith({
          name: "chat.general",
          partitions: 1,
        });
        expect(frame).toContain("general");
      },
    },
    {
      field: "grpcUrl",
      value: "http://broker.example:9090",
      assertUpdated() {
        expect(FerrumQClient).toHaveBeenLastCalledWith(
          expect.objectContaining({ grpcUrl: "http://broker.example:9090" }),
        );
      },
    },
    {
      field: "room",
      value: "engineering",
      assertUpdated(client: MockClient, frame: string) {
        expect(client.createTopic).toHaveBeenCalledWith({
          name: "chat.engineering",
          partitions: 1,
        });
        expect(frame).toContain("engineering");
      },
    },
    {
      field: "name",
      value: "Bob",
      assertUpdated(_client: MockClient, frame: string) {
        expect(frame).toContain("Bob");
      },
    },
    {
      field: "timeoutMs",
      value: 25_000,
      assertUpdated() {
        expect(FerrumQClient).toHaveBeenLastCalledWith(
          expect.objectContaining({ timeoutMs: 25_000 }),
        );
      },
    },
    {
      field: "pollIntervalMs",
      value: 750,
      assertUpdated(client: MockClient) {
        expect(client.consume).not.toHaveBeenCalled();
      },
    },
  ] as const)("restarts with updated $field when that session scalar changes", async ({
    field,
    value,
    assertUpdated,
  }) => {
    vi.useFakeTimers();
    const view = render(<ChatUi config={baseConfig} />);
    await vi.advanceTimersByTimeAsync(0);

    view.rerender(<ChatUi config={{ ...baseConfig, [field]: value }} />);
    await vi.advanceTimersByTimeAsync(0);

    expect(FerrumQClient).toHaveBeenCalledTimes(2);
    expect(clientAt(0).close).toHaveBeenCalledOnce();
    expect(clientAt(1).close).not.toHaveBeenCalled();
    assertUpdated(clientAt(1), view.lastFrame() ?? "");
    expect(vi.getTimerCount()).toBeGreaterThanOrEqual(1);

    view.unmount();
    await vi.advanceTimersByTimeAsync(0);
    expect(clientAt(1).close).toHaveBeenCalledOnce();
  });

  it("stops every generation once without overlapping polls or stale callbacks", async () => {
    vi.useFakeTimers();
    const view = render(<ChatUi config={baseConfig} />);
    await vi.advanceTimersByTimeAsync(0);

    view.rerender(<ChatUi config={{ ...baseConfig }} />);
    view.rerender(<ChatUi config={{ ...baseConfig, room: "engineering" }} />);
    await vi.advanceTimersByTimeAsync(0);
    view.rerender(
      <ChatUi config={{ ...baseConfig, room: "engineering", name: "Bob" }} />,
    );
    view.rerender(
      <ChatUi config={{ ...baseConfig, room: "engineering", name: "Bob" }} />,
    );
    await vi.advanceTimersByTimeAsync(0);

    expect(FerrumQClient).toHaveBeenCalledTimes(3);
    expect(clientAt(0).close).toHaveBeenCalledOnce();
    expect(clientAt(1).close).toHaveBeenCalledOnce();
    expect(clientAt(2).close).not.toHaveBeenCalled();
    expect(view.lastFrame() ?? "").toContain("Bob");
    expect(view.lastFrame() ?? "").not.toContain("Alice");

    await vi.advanceTimersByTimeAsync(500);
    expect(clientAt(0).consume).not.toHaveBeenCalled();
    expect(clientAt(1).consume).not.toHaveBeenCalled();
    expect(clientAt(2).consume).toHaveBeenCalledOnce();

    view.unmount();
    await vi.advanceTimersByTimeAsync(0);

    for (const client of clients()) {
      expect(client.close).toHaveBeenCalledOnce();
    }
  });

  it("runs shared shutdown once across explicit exit and unmount", async () => {
    let shutdown: (() => Promise<void>) | undefined;
    const view = render(
      <ChatUi
        config={baseConfig}
        onShutdownReady={(value) => {
          shutdown = value;
        }}
      />,
    );

    await flushEffects();
    await shutdown?.();
    view.unmount();
    await flushEffects();

    expect(clientAt(0).close).toHaveBeenCalledOnce();
  });

  it("calls client close on unmount without unhandled rejections", async () => {
    const view = render(<ChatUi config={baseConfig} />);
    await flushEffects();

    const client = clientAt(0);
    expect(client.close).not.toHaveBeenCalled();

    view.unmount();
    await flushEffects();
    expect(client.close).toHaveBeenCalledOnce();
  });

  it("blocks repeated Enter while a publish is pending", async () => {
    const view = render(<ChatUi config={baseConfig} />);
    await flushEffects();
    const publish = deferred<void>();
    clientAt(0).publish.mockImplementationOnce(() => publish.promise);

    view.stdin.write("hello\r");
    await flushEffects();
    view.stdin.write("\r");
    await flushEffects();

    expect(clientAt(0).publish).toHaveBeenCalledOnce();
    expect(view.lastFrame() ?? "").toContain("hello");
    expect(view.lastFrame() ?? "").toContain("sending");

    publish.resolve();
    await flushEffects();
    expect(view.lastFrame() ?? "").not.toContain("> hello");
    view.unmount();
  });

  it("removes only the submitted snapshot after a slow publish succeeds", async () => {
    const view = render(<ChatUi config={baseConfig} />);
    await flushEffects();
    const publish = deferred<void>();
    clientAt(0).publish.mockImplementationOnce(() => publish.promise);

    view.stdin.write("hello");
    await flushEffects();
    view.stdin.write("\r");
    await flushEffects();
    view.stdin.write(" world");
    await flushEffects();
    expect(view.lastFrame() ?? "").toContain("hello world");

    publish.resolve();
    await flushEffects();
    expect(view.lastFrame() ?? "").toContain(">  world|");
    view.unmount();
  });

  it("preserves the complete input when publish fails", async () => {
    const view = render(<ChatUi config={baseConfig} />);
    await flushEffects();
    clientAt(0).publish.mockRejectedValueOnce(new Error("offline"));

    view.stdin.write("retry me");
    await flushEffects();
    view.stdin.write("\r");
    await flushEffects();

    expect(clientAt(0).publish).toHaveBeenCalledOnce();
    expect(view.lastFrame() ?? "").toContain("> retry me|");
    expect(view.lastFrame() ?? "").toContain("Failed to send message: offline");
    view.unmount();
  });

  it("does not let an old publish mutate a replacement generation", async () => {
    const view = render(<ChatUi config={baseConfig} />);
    await flushEffects();
    const publish = deferred<void>();
    clientAt(0).publish.mockImplementationOnce(() => publish.promise);

    view.stdin.write("old");
    await flushEffects();
    view.stdin.write("\r");
    await flushEffects();

    view.rerender(<ChatUi config={{ ...baseConfig, room: "engineering" }} />);
    await flushEffects();
    view.stdin.write("new");
    await flushEffects();

    publish.resolve();
    await flushEffects();

    expect(view.lastFrame() ?? "").toContain("engineering");
    expect(view.lastFrame() ?? "").toContain("> new|");
    view.unmount();
  });

  it("renders only the 200 most recent messages", async () => {
    vi.useFakeTimers();
    const view = render(<ChatUi config={baseConfig} />);
    await vi.advanceTimersByTimeAsync(0);
    clientAt(0).consume.mockResolvedValueOnce(
      Array.from({ length: 205 }, (_, index) => ({
        deliveryId: `delivery-${index}`,
        payload: new TextEncoder().encode(
          JSON.stringify({
            version: 1,
            id: `id-${index}`,
            room: "general",
            sender: { id: "bob", name: "Bob", sessionId: "bob-session" },
            text: `msg-${String(index).padStart(4, "0")}`,
            sentAt: "2025-01-01T00:00:00.000Z",
          }),
        ),
      })),
    );

    await vi.advanceTimersByTimeAsync(500);
    await flushMicrotasks(220);
    await vi.advanceTimersByTimeAsync(0);
    expect(clientAt(0).consume).toHaveBeenCalledOnce();
    expect(clientAt(0).ack).toHaveBeenCalledTimes(205);
    const frame = view.lastFrame() ?? "";
    expect(frame).not.toContain("msg-0004");
    expect(frame).toContain("msg-0005");
    expect(frame).toContain("msg-0204");
    view.unmount();
    await vi.advanceTimersByTimeAsync(0);
  });
});

describe("appendMessageHistory", () => {
  it("keeps a strict 500-message in-memory limit", () => {
    const messages = Array.from({ length: MAX_MESSAGE_HISTORY + 1 }).reduce<
      Array<{
        id: string;
        senderName: string;
        text: string;
        timestamp: Date;
        isSelf: boolean;
      }>
    >(
      (history, _, index) =>
        appendMessageHistory(history, {
          id: `id-${index}`,
          senderName: "Alice",
          text: `message-${index}`,
          timestamp: new Date(0),
          isSelf: false,
        }),
      [],
    );

    expect(messages).toHaveLength(MAX_MESSAGE_HISTORY);
    expect(messages[0]?.id).toBe("id-1");
    expect(messages.at(-1)?.id).toBe(`id-${MAX_MESSAGE_HISTORY}`);
  });
});

function clients(): MockClient[] {
  return vi
    .mocked(FerrumQClient)
    .mock.results.map((result) => result.value as MockClient);
}

function clientAt(index: number): MockClient {
  const client = clients()[index];
  if (client === undefined) {
    throw new Error(`Expected SDK client at index ${index}`);
  }
  return client;
}

async function flushEffects(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((value) => {
    resolve = value;
  });
  return { promise, resolve };
}

async function flushMicrotasks(count: number): Promise<void> {
  for (let index = 0; index < count; index++) {
    await Promise.resolve();
  }
}
