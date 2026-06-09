#!/usr/bin/env node
import { runCli } from "./index.js";

const exitCode = await runCli(
  process.argv.slice(2),
  {
    writeLine(message) {
      process.stdout.write(`${message}\n`);
    },
    writeError(message) {
      process.stderr.write(`${message}\n`);
    },
  },
  {
    env: process.env,
  },
);

process.exitCode = exitCode;
