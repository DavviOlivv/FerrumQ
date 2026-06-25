import { createControlPlaneClient } from "@ferrumq/protocol";
import { render as inkRender } from "ink";
import { createElement, type ReactElement } from "react";

import { FerrumQTui, type SearchDependencies } from "./components.js";
import {
  ExpectedTuiError,
  parseTuiArgs,
  resolveTuiConfig,
  type TuiEnvironment,
  tuiHelpText,
  tuiVersion,
} from "./config.js";

export interface TuiCliOutput {
  writeLine(message: string): void;
  writeError?(message: string): void;
}

export type TuiRenderer = (element: ReactElement) => {
  waitUntilExit(): Promise<unknown>;
};

export interface RunTuiCliOptions {
  env?: TuiEnvironment;
  renderTui?: TuiRenderer;
  searchDependencies?: SearchDependencies;
}

export async function runTuiCli(
  args: readonly string[],
  output: TuiCliOutput,
  options: RunTuiCliOptions = {},
): Promise<number> {
  try {
    const parsed = parseTuiArgs(args);
    if (parsed.help) {
      output.writeLine(tuiHelpText());
      return 0;
    }

    if (parsed.version) {
      output.writeLine(tuiVersion);
      return 0;
    }

    const config = resolveTuiConfig(parsed, options.env);
    const searchDependencies =
      options.searchDependencies ??
      defaultSearchDependencies(config.controlUrl);
    const renderTui: TuiRenderer = options.renderTui ?? defaultTuiRenderer;
    const instance = renderTui(
      createElement(FerrumQTui, { config, searchDependencies }),
    );
    await instance.waitUntilExit();
    return 0;
  } catch (error) {
    if (error instanceof ExpectedTuiError) {
      writeError(output, error.message);
      return error.exitCode;
    }

    writeError(output, errorMessage(error));
    return 1;
  }
}

function defaultTuiRenderer(element: ReactElement): {
  waitUntilExit(): Promise<unknown>;
} {
  return inkRender(element, { exitOnCtrlC: false });
}

function defaultSearchDependencies(controlUrl: string): SearchDependencies {
  const client = createControlPlaneClient(controlUrl);
  return {
    searchMessages: (request) => client.searchMessages(request),
  };
}

function writeError(output: TuiCliOutput, message: string): void {
  if (output.writeError !== undefined) {
    output.writeError(message);
    return;
  }

  output.writeLine(message);
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  return "unexpected error";
}
