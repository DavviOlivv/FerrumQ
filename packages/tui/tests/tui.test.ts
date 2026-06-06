import { describe, expect, it } from "vitest";

import { tuiStatus } from "../src/index.js";

describe("TUI placeholder", () => {
  it("reports Milestone 0 status", () => {
    expect(tuiStatus()).toEqual({
      packageName: "@ferrumq/tui",
      status: "milestone-0",
    });
  });
});
