#!/usr/bin/env node
import { runCli } from "./index.js";

const exitCode = runCli(process.argv.slice(2), {
  writeLine(message) {
    process.stdout.write(`${message}\n`);
  },
});

process.exitCode = exitCode;
