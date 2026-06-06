import { describe, expect, it } from "vitest";

import { sdkStatus } from "../src/index.js";

describe("SDK placeholder", () => {
  it("reports Milestone 0 status", () => {
    expect(sdkStatus()).toEqual({
      packageName: "@ferrumq/sdk",
      status: "milestone-0",
    });
  });
});
