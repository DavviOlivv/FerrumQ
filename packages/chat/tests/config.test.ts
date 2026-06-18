import { describe, expect, it } from "vitest";
import { parseChatArgs } from "../src/config.js";

const requiredArgs = ["--name", "a", "--room", "general"];

function parseOption(option: string, value: string, equals: boolean) {
  const args = equals
    ? [...requiredArgs, `${option}=${value}`]
    : [...requiredArgs, option, value];
  return parseChatArgs(args);
}

describe("numeric CLI options", () => {
  it.each([
    ["--timeout-ms", "0", 0],
    ["--timeout-ms", "2147483647", 2_147_483_647],
    ["--poll-interval-ms", "1", 1],
    ["--poll-interval-ms", "500", 500],
    ["--poll-interval-ms", "2147483647", 2_147_483_647],
  ])("accepts %s %s in spaced and equals forms", (option, raw, expected) => {
    for (const equals of [false, true]) {
      const result = parseOption(option, raw, equals);
      expect("config" in result).toBe(true);
      if ("config" in result) {
        const key = option === "--timeout-ms" ? "timeoutMs" : "pollIntervalMs";
        expect(result.config[key]).toBe(expected);
      }
    }
  });

  it.each([
    "",
    " ",
    " 1",
    "1 ",
    "+1",
    "-1",
    "1.0",
    "1e3",
    "1ms",
    "0x10",
    "１２",
  ])("rejects malformed integer %j in both forms", (raw) => {
    for (const option of ["--timeout-ms", "--poll-interval-ms"]) {
      for (const equals of [false, true]) {
        const result = parseOption(option, raw, equals);
        expect("error" in result).toBe(true);
      }
    }
  });

  it.each([
    "--timeout-ms",
    "--poll-interval-ms",
  ])("rejects %s values above the safe timer maximum", (option) => {
    for (const value of ["2147483648", "9007199254740992"]) {
      for (const equals of [false, true]) {
        const result = parseOption(option, value, equals);
        expect("error" in result).toBe(true);
      }
    }
  });

  it.each([
    false,
    true,
  ])("rejects a zero polling interval (equals=%s)", (equals) => {
    const result = parseOption("--poll-interval-ms", "0", equals);
    expect("error" in result).toBe(true);
  });

  it.each([
    "--timeout-ms=",
    "--poll-interval-ms=",
  ])("rejects an empty equals value for %s", (arg) => {
    const result = parseChatArgs([...requiredArgs, arg]);
    expect("error" in result).toBe(true);
  });
});

describe("duplicate flag rejection", () => {
  it("rejects duplicate --name flags", () => {
    const result = parseChatArgs([
      "--name",
      "Alice",
      "--name",
      "Bob",
      "--room",
      "general",
    ]);
    expect("error" in result).toBe(true);
    if ("error" in result) {
      expect(result.error).toContain("duplicate");
      expect(result.error).toContain("--name");
    }
  });

  it("rejects duplicate --room flags", () => {
    const result = parseChatArgs([
      "--name",
      "Alice",
      "--room",
      "a",
      "--room",
      "b",
    ]);
    expect("error" in result).toBe(true);
    if ("error" in result) {
      expect(result.error).toContain("duplicate");
    }
  });

  it("rejects duplicate equals-form flags", () => {
    const result = parseChatArgs([
      "--name",
      "Alice",
      "--room",
      "a",
      "--timeout-ms=5000",
      "--timeout-ms=10000",
    ]);
    expect("error" in result).toBe(true);
    if ("error" in result) {
      expect(result.error).toContain("duplicate");
    }
  });

  it("rejects duplicate flag when mixing spaced and equals forms", () => {
    const result = parseChatArgs([
      "--name=Alice",
      "--name",
      "Bob",
      "--room",
      "general",
    ]);
    expect("error" in result).toBe(true);
    if ("error" in result) {
      expect(result.error).toContain("duplicate");
    }
  });

  it("rejects duplicate numeric options", () => {
    const result = parseChatArgs([
      "--name",
      "Alice",
      "--room",
      "general",
      "--poll-interval-ms",
      "100",
      "--poll-interval-ms=200",
    ]);
    expect("error" in result).toBe(true);
    if ("error" in result) {
      expect(result.error).toContain("duplicate");
    }
  });
});

describe("equals-form value edge cases", () => {
  it("strips a leading equals from equals-form values", () => {
    const result = parseChatArgs(["--name==foo", "--room", "general"]);
    expect("config" in result).toBe(true);
    if ("config" in result) {
      expect(result.config.name).toBe("foo");
    }
  });

  it("handles equals-form with empty value as empty string", () => {
    const result = parseChatArgs(["--name=", "--room=general"]);
    expect("error" in result).toBe(true);
    if ("error" in result) {
      expect(result.error).toContain("--name is required");
    }
  });

  it("handles equals-form URL with leading equals after ==", () => {
    const result = parseChatArgs([
      "--name",
      "Alice",
      "--room",
      "general",
      "--http-url==http://example.com",
    ]);
    expect("config" in result).toBe(true);
    if ("config" in result) {
      expect(result.config.httpUrl).toBe("http://example.com");
    }
  });
});

describe("URL configuration precedence", () => {
  it.each([
    {
      option: "--http-url",
      envKey: "FERRUMQ_HTTP_URL" as const,
      configKey: "httpUrl" as const,
      defaultValue: "http://127.0.0.1:8080",
    },
    {
      option: "--grpc-url",
      envKey: "FERRUMQ_GRPC_URL" as const,
      configKey: "grpcUrl" as const,
      defaultValue: "http://127.0.0.1:9090",
    },
  ])("resolves $configKey as CLI, environment, then default", ({
    option,
    envKey,
    configKey,
    defaultValue,
  }) => {
    const cliOnly = parseChatArgs([
      ...requiredArgs,
      option,
      "http://cli.example",
    ]);
    expect("config" in cliOnly && cliOnly.config[configKey]).toBe(
      "http://cli.example",
    );

    const envOnly = parseChatArgs(requiredArgs, {
      [envKey]: "http://env.example",
    });
    expect("config" in envOnly && envOnly.config[configKey]).toBe(
      "http://env.example",
    );

    const both = parseChatArgs(
      [...requiredArgs, option, "http://cli.example"],
      { [envKey]: "http://env.example" },
    );
    expect("config" in both && both.config[configKey]).toBe(
      "http://cli.example",
    );

    const neither = parseChatArgs(requiredArgs);
    expect("config" in neither && neither.config[configKey]).toBe(defaultValue);
  });
});
