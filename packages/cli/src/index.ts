export { executeCommand } from "./commands.js";
export { cliVersion, resolveConfig } from "./config.js";
export { ExpectedCliError } from "./errors.js";
export { createControlPlaneClient } from "./http-client.js";
export { parseCliArgs } from "./parser.js";

import { type CommandDependencies, executeCommand } from "./commands.js";
import {
  type CliEnvironment,
  defaultControlUrl,
  defaultGrpcUrl,
  type ResolvedConfig,
  resolveConfig,
} from "./config.js";
import { ExpectedCliError, errorMessage } from "./errors.js";
import { type ParsedCommand, parseCliArgs } from "./parser.js";

export interface CliOutput {
  writeLine(message: string): void;
  writeError?(message: string): void;
}

export interface RunCliOptions extends CommandDependencies {
  env?: CliEnvironment;
}

export async function runCli(
  args: readonly string[],
  output: CliOutput,
  options: RunCliOptions = {},
): Promise<number> {
  try {
    const parsed = parseCliArgs(args);
    const config = commandRequiresResolvedConfig(parsed.command)
      ? resolveConfig(parsed.globals, options.env)
      : localCommandConfig(parsed.globals.json);
    const result = await executeCommand(parsed.command, config, options);
    output.writeLine(result.stdout);
    return 0;
  } catch (error) {
    if (error instanceof ExpectedCliError) {
      writeError(output, error.message);
      return error.exitCode;
    }

    writeError(output, errorMessage(error));
    return 1;
  }
}

function writeError(output: CliOutput, message: string): void {
  if (output.writeError !== undefined) {
    output.writeError(message);
    return;
  }

  output.writeLine(message);
}

function commandRequiresResolvedConfig(command: ParsedCommand): boolean {
  switch (command.kind) {
    case "root-help":
    case "version":
    case "broker-help":
    case "topic-help":
    case "dlq-help":
    case "publish-help":
    case "consume-help":
    case "ack-help":
    case "nack-help":
    case "search-help":
    case "broker-version":
      return false;
    default:
      return true;
  }
}

function localCommandConfig(json: boolean): ResolvedConfig {
  return {
    controlUrl: defaultControlUrl,
    grpcUrl: defaultGrpcUrl,
    json,
  };
}
