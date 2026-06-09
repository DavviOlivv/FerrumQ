import { execFile } from "node:child_process";

import { ExpectedCliError } from "./errors.js";

export type BrokerdVersionRunner = () => Promise<string>;

export const runBrokerdVersion: BrokerdVersionRunner = () =>
  new Promise((resolve, reject) => {
    execFile(
      "brokerd",
      ["--version"],
      { windowsHide: true },
      (error, stdout, stderr) => {
        if (error !== null) {
          const code = (error as NodeJS.ErrnoException).code;
          if (code === "ENOENT") {
            reject(
              new ExpectedCliError(
                "brokerd is not on PATH; build or install the Rust broker runtime before using broker version",
              ),
            );
            return;
          }

          reject(
            new ExpectedCliError(
              `brokerd --version failed: ${stderr.trim() || error.message}`,
            ),
          );
          return;
        }

        resolve(stdout.trim());
      },
    );
  });
