import type { ReactElement } from "react";
import { describe, expect, it, vi } from "vitest";
import {
  type ChatRenderer,
  type RunnerRuntime,
  runChatCli,
} from "../src/runner.js";
import type { ChatUiProps } from "../src/ui.js";

function deferred(): {
  promise: Promise<void>;
  resolve: () => void;
} {
  let resolve!: () => void;
  const promise = new Promise<void>((value) => {
    resolve = value;
  });
  return { promise, resolve };
}

function createRuntime() {
  const inkExit = deferred();
  let signalListener: (() => void) | undefined;
  let element: ReactElement<ChatUiProps> | undefined;
  const renderer: ChatRenderer = {
    waitUntilExit: vi.fn(() => inkExit.promise),
    unmount: vi.fn(() => inkExit.resolve()),
  };
  const runtime: RunnerRuntime = {
    addSignalListener: vi.fn((_signal, listener) => {
      signalListener = listener;
    }),
    removeSignalListener: vi.fn(),
    render: vi.fn((value) => {
      element = value as ReactElement<ChatUiProps>;
      return renderer;
    }),
  };

  return {
    runtime,
    renderer,
    inkExit,
    getProps: () => {
      if (element === undefined) {
        throw new Error("UI was not rendered");
      }
      return element.props;
    },
    signal: () => signalListener?.(),
  };
}

const output = {
  writeLine: vi.fn(),
  writeError: vi.fn(),
};

describe("runChatCli shutdown lifecycle", () => {
  it("shuts down and unmounts Ink on SIGTERM", async () => {
    const fixture = createRuntime();
    const shutdown = vi.fn(async () => {});
    const result = runChatCli(
      ["--name", "Alice", "--room", "general"],
      output,
      {},
      fixture.runtime,
    );

    fixture.getProps().onShutdownReady?.(shutdown);
    fixture.signal();

    await expect(result).resolves.toBe(0);
    expect(shutdown).toHaveBeenCalledOnce();
    expect(fixture.renderer.unmount).toHaveBeenCalledOnce();
    expect(fixture.runtime.removeSignalListener).toHaveBeenCalledOnce();
  });

  it("passes normalized room and display name values to the UI", async () => {
    const fixture = createRuntime();
    const shutdown = vi.fn(async () => {});
    const result = runChatCli(
      ["--name", "  Alice  ", "--room", "  Room_Name.V1  "],
      output,
      {},
      fixture.runtime,
    );

    const props = fixture.getProps();
    expect(props.config.name).toBe("Alice");
    expect(props.config.room).toBe("room_name.v1");
    props.onShutdownReady?.(shutdown);
    fixture.inkExit.resolve();

    await expect(result).resolves.toBe(0);
  });

  it("ignores repeated SIGTERM during cleanup", async () => {
    const fixture = createRuntime();
    const shutdownDone = deferred();
    const shutdown = vi.fn(() => shutdownDone.promise);
    const result = runChatCli(
      ["--name", "Alice", "--room", "general"],
      output,
      {},
      fixture.runtime,
    );

    fixture.getProps().onShutdownReady?.(shutdown);
    fixture.signal();
    fixture.signal();
    shutdownDone.resolve();

    await expect(result).resolves.toBe(0);
    expect(shutdown).toHaveBeenCalledOnce();
    expect(fixture.renderer.unmount).toHaveBeenCalledOnce();
  });

  it("removes the listener and shares shutdown when Ink exits normally", async () => {
    const fixture = createRuntime();
    const shutdown = vi.fn(async () => {});
    const result = runChatCli(
      ["--name", "Alice", "--room", "general"],
      output,
      {},
      fixture.runtime,
    );

    fixture.getProps().onShutdownReady?.(shutdown);
    fixture.inkExit.resolve();
    fixture.signal();

    await expect(result).resolves.toBe(0);
    expect(shutdown).toHaveBeenCalledOnce();
    expect(fixture.renderer.unmount).not.toHaveBeenCalled();
    expect(fixture.runtime.removeSignalListener).toHaveBeenCalledWith(
      "SIGTERM",
      expect.any(Function),
    );
  });

  it("prints help text and returns exit code 0 for --help", async () => {
    const result = await runChatCli(
      ["--help"],
      output,
      {},
      createRuntime().runtime,
    );
    expect(result).toBe(0);
    expect(output.writeLine).toHaveBeenCalledWith(
      expect.stringContaining("ferrumq-chat - Terminal chat over FerrumQ"),
    );
  });

  it("prints version and returns exit code 0 for --version", async () => {
    const result = await runChatCli(
      ["--version"],
      output,
      {},
      createRuntime().runtime,
    );
    expect(result).toBe(0);
    expect(output.writeLine).toHaveBeenCalledWith("ferrumq-chat 0.1.0");
  });

  it("returns exit code 1 when --help and --version are combined", async () => {
    const result = await runChatCli(
      ["--help", "--version"],
      output,
      {},
      createRuntime().runtime,
    );
    expect(result).toBe(1);
    expect(output.writeError).toHaveBeenCalledWith(
      expect.stringContaining("--help and --version cannot be combined"),
    );
  });

  it("returns exit code 1 for missing required options", async () => {
    const result = await runChatCli(
      ["--name", "Alice"],
      output,
      {},
      createRuntime().runtime,
    );
    expect(result).toBe(1);
    expect(output.writeError).toHaveBeenCalledWith(
      expect.stringContaining("--room is required"),
    );
  });

  it("returns exit code 1 for an invalid display name", async () => {
    const result = await runChatCli(
      ["--name", "!!!", "--room", "general"],
      output,
      {},
      createRuntime().runtime,
    );
    expect(result).toBe(1);
    expect(output.writeError).toHaveBeenCalledWith(
      expect.stringContaining("Invalid display name"),
    );
  });

  it("returns exit code 1 for an invalid room name", async () => {
    const result = await runChatCli(
      ["--name", "Alice", "--room", "###"],
      output,
      {},
      createRuntime().runtime,
    );
    expect(result).toBe(1);
    expect(output.writeError).toHaveBeenCalledWith(
      expect.stringContaining("Invalid room name"),
    );
  });

  it("returns exit code 1 for unknown options", async () => {
    const result = await runChatCli(
      ["--name", "Alice", "--room", "general", "--bogus"],
      output,
      {},
      createRuntime().runtime,
    );
    expect(result).toBe(1);
    expect(output.writeError).toHaveBeenCalledWith(
      expect.stringContaining("unknown option"),
    );
  });

  it.each([
    ["--http-url", "http://127.0.0.1:8080/api", "Invalid broker configuration"],
    ["--grpc-url", "http://127.0.0.1", "Invalid broker configuration"],
  ])("validates %s before rendering the UI", async (flag, value, message) => {
    const fixture = createRuntime();
    const result = await runChatCli(
      ["--name", "Alice", "--room", "general", flag, value],
      output,
      {},
      fixture.runtime,
    );

    expect(result).toBe(1);
    expect(output.writeError).toHaveBeenCalledWith(
      expect.stringContaining(message),
    );
    expect(fixture.runtime.render).not.toHaveBeenCalled();
  });
});
