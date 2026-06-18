#!/usr/bin/env node
import { runChatCli } from "./runner.js";

const exitCode = await runChatCli(
  process.argv.slice(2),
  {
    writeLine(message) {
      process.stdout.write(`${message}\n`);
    },
    writeError(message) {
      process.stderr.write(`${message}\n`);
    },
  },
  process.env as Record<string, string | undefined>,
);

process.exitCode = exitCode;
