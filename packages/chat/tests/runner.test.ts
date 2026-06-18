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
});
