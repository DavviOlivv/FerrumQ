export const cliVersion = "0.1.0";

export interface CliOutput {
  writeLine(message: string): void;
}

export function helpText(): string {
  return [
    "FerrumQ message broker CLI",
    "",
    "Usage:",
    "  msg --version",
    "  msg --help",
    "",
    "Milestone 0 exposes version and help only.",
  ].join("\n");
}

export function runCli(args: readonly string[], output: CliOutput): number {
  const firstArg = args[0];

  if (firstArg === "--version" || firstArg === "-V") {
    output.writeLine(cliVersion);
    return 0;
  }

  if (firstArg === undefined || firstArg === "--help" || firstArg === "-h") {
    output.writeLine(helpText());
    return 0;
  }

  output.writeLine(`Unknown command: ${firstArg}`);
  output.writeLine(helpText());
  return 1;
}
