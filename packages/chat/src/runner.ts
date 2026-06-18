import { render } from "ink";
import { createElement, type ReactElement } from "react";
import { type ChatEnvironment, parseChatArgs } from "./config.js";
import { validateName, validateRoom } from "./domain.js";
import { ChatUi, type ChatUiConfig } from "./ui.js";

export interface RunnerOutput {
  writeLine(message: string): void;
  writeError(message: string): void;
}

export type RunnerEnvironment = ChatEnvironment;

export interface ChatRenderer {
  waitUntilExit(): Promise<unknown>;
  unmount(): void;
}

export interface RunnerRuntime {
  addSignalListener(signal: "SIGTERM", listener: () => void): void;
  removeSignalListener(signal: "SIGTERM", listener: () => void): void;
  render(element: ReactElement): ChatRenderer;
}

const defaultRuntime: RunnerRuntime = {
  addSignalListener(signal, listener) {
    process.on(signal, listener);
  },
  removeSignalListener(signal, listener) {
    process.off(signal, listener);
  },
  render(element) {
    return render(element, {
      exitOnCtrlC: true,
      patchConsole: false,
    });
  },
};

export async function runChatCli(
  argv: string[],
  output: RunnerOutput,
  env: RunnerEnvironment,
  runtime: RunnerRuntime = defaultRuntime,
): Promise<number> {
  const result = parseChatArgs(argv, env);

  if ("help" in result) {
    output.writeLine(result.help);
    return 0;
  }

  if ("version" in result) {
    output.writeLine(result.version);
    return 0;
  }

  if ("error" in result) {
    output.writeError(result.error);
    return result.exitCode;
  }

  const raw = result.config;

  let name: string;
  try {
    name = validateName(raw.name);
  } catch (err) {
    output.writeError(
      `Invalid display name: ${errorMessage(err)}\n\n${buildUsageHelp()}`,
    );
    return 1;
  }

  let room: string;
  try {
    room = validateRoom(raw.room);
  } catch (err) {
    output.writeError(
      `Invalid room name: ${errorMessage(err)}\n\n${buildUsageHelp()}`,
    );
    return 1;
  }

  const config: ChatUiConfig = {
    httpUrl: raw.httpUrl,
    grpcUrl: raw.grpcUrl,
    room,
    name,
    timeoutMs: raw.timeoutMs,
    pollIntervalMs: raw.pollIntervalMs,
  };

  let shutdown: (() => Promise<void>) | undefined;
  let sigtermReceived = false;
  let resolveSigterm: (() => void) | undefined;
  const sigterm = new Promise<void>((resolve) => {
    resolveSigterm = resolve;
  });
  const handleSigterm = () => {
    if (sigtermReceived) {
      return;
    }
    sigtermReceived = true;
    resolveSigterm?.();
  };

  runtime.addSignalListener("SIGTERM", handleSigterm);
  try {
    const app = runtime.render(
      createElement(ChatUi, {
        config,
        onShutdownReady(value) {
          shutdown = value;
        },
      }),
    );
    const inkExit = app.waitUntilExit();
    const completedBy = await Promise.race([
      inkExit.then(() => "ink" as const),
      sigterm.then(() => "sigterm" as const),
    ]);

    if (completedBy === "sigterm") {
      await shutdown?.();
      app.unmount();
      await inkExit;
    } else {
      await shutdown?.();
    }

    return 0;
  } finally {
    runtime.removeSignalListener("SIGTERM", handleSigterm);
  }
}

function buildUsageHelp(): string {
  return "Usage: ferrumq-chat --name <name> --room <room> [options]";
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
