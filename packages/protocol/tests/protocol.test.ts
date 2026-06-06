import { describe, expect, it } from "vitest";

import { parseHealthStatus } from "../src/index.js";

describe("protocol placeholder schema", () => {
  it("parses a Milestone 0 health status", () => {
    expect(
      parseHealthStatus({
        service: "brokerd",
        status: "milestone-0",
        version: "0.1.0",
      }),
    ).toEqual({
      service: "brokerd",
      status: "milestone-0",
      version: "0.1.0",
    });
  });
});
