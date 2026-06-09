export { cliVersion, resolveConfig } from "./config.js";
export { parseCliArgs } from "./parser.js";
export { createControlPlaneClient } from "./http-client.js";
export { executeCommand } from "./commands.js";
export { ExpectedCliError } from "./errors.js";

import { resolveConfig, type CliEnvironment } from "./config.js";
import { executeCommand, type CommandDependencies } from "./commands.js";
import { errorMessage, ExpectedCliError } from "./errors.js";
import { parseCliArgs } from "./parser.js";

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
    const config = resolveConfig(parsed.globals, options.env);
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
