#!/usr/bin/env node
import { runTuiCli } from "./runner.js";

const exitCode = await runTuiCli(
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
