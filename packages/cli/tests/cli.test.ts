import { describe, expect, it } from "vitest";

import { cliVersion, runCli } from "../src/index.js";

function captureRun(args: readonly string[]): {
  code: number;
  lines: string[];
} {
  const lines: string[] = [];
  const code = runCli(args, {
    writeLine(message) {
      lines.push(message);
    },
  });

  return { code, lines };
}

describe("CLI placeholder", () => {
  it("prints the version", () => {
    const result = captureRun(["--version"]);

    expect(result).toEqual({ code: 0, lines: [cliVersion] });
  });

  it("prints help by default", () => {
    const result = captureRun([]);

    expect(result.code).toBe(0);
    expect(result.lines.join("\n")).toContain("Milestone 0");
  });
});
