import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { DataPlaneClient } from "@ferrumq/protocol";
import { describe, expect, it, vi } from "vitest";
import type {
  ControlPlaneClient,
  FetchLike,
  ResponseLike,
} from "../src/http-client.js";
import type { RunCliOptions } from "../src/index.js";
import {
  cliVersion,
  createControlPlaneClient,
  parseCliArgs,
  resolveConfig,
  runCli,
} from "../src/index.js";

const packageRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const distCliPath = path.join(packageRoot, "dist/cli.js");
const describeBuiltCli = existsSync(distCliPath) ? describe : describe.skip;
let builtCliImportCounter = 0;

async function captureRun(
  args: readonly string[],
  options: RunCliOptions = {},
): Promise<{
  code: number;
  stdout: string[];
  stderr: string[];
}> {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const code = await runCli(
    args,
    {
      writeLine(message) {
        stdout.push(message);
      },
      writeError(message) {
        stderr.push(message);
      },
    },
    {
      env: {},
      now: () => 1_700_000_000_000,
      randomUUID: () => "00000000-0000-4000-8000-000000000001",
      ...options,
    },
  );

  return { code, stdout, stderr };
}

function expectSuccessfulJson(
  result: { code: number; stdout: string[]; stderr: string[] },
  expected: unknown,
): void {
  expect(result.code).toBe(0);
  expect(result.stderr).toEqual([]);
  expect(result.stdout).toHaveLength(1);
  expect(JSON.parse(result.stdout[0] as string)).toEqual(expected);
}

function response(
  status: number,
  payload: unknown,
  statusText = status >= 200 && status < 300 ? "OK" : "Bad Request",
): ResponseLike {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText,
    async json() {
      return payload;
    },
  };
}

function invalidJsonResponse(status: number, statusText = "OK"): ResponseLike {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText,
    async json() {
      throw new SyntaxError("Unexpected token");
    },
  };
}

describe("command parsing", () => {
  it("parses every command family", () => {
    expect(parseCliArgs(["--version"]).command).toEqual({ kind: "version" });
    expect(parseCliArgs(["--help"]).command).toEqual({ kind: "root-help" });
    expect(parseCliArgs(["broker", "--help"]).command).toEqual({
      kind: "broker-help",
    });
    expect(parseCliArgs(["topic", "--help"]).command).toEqual({
      kind: "topic-help",
    });
    expect(parseCliArgs(["publish", "--help"]).command).toEqual({
      kind: "publish-help",
    });
    expect(parseCliArgs(["consume", "-h"]).command).toEqual({
      kind: "consume-help",
    });
    expect(parseCliArgs(["broker", "version"]).command).toEqual({
      kind: "broker-version",
    });
    expect(parseCliArgs(["health"]).command).toEqual({ kind: "health" });
    expect(parseCliArgs(["ready"]).command).toEqual({ kind: "ready" });
    expect(parseCliArgs(["status"]).command).toEqual({ kind: "status" });
    expect(
      parseCliArgs(["topic", "create", "orders", "--partitions", "3"]).command,
    ).toEqual({
      kind: "topic-create",
      topic: "orders",
      partitions: "3",
    });
    expect(parseCliArgs(["topic", "get", "orders"]).command).toEqual({
      kind: "topic-get",
      topic: "orders",
    });
    expect(parseCliArgs(["topic", "list"]).command).toEqual({
      kind: "topic-list",
    });
    expect(parseCliArgs(["dlq", "list", "--topic", "orders"]).command).toEqual({
      kind: "dlq-list",
      topic: "orders",
    });
    expect(
      parseCliArgs(["publish", "orders", "--data", "hello"]).command,
    ).toEqual({
      kind: "publish",
      topic: "orders",
      data: "hello",
    });
    expect(
      parseCliArgs(["consume", "orders", "--group", "group.1"]).command,
    ).toEqual({
      kind: "consume",
      topic: "orders",
      group: "group.1",
    });
    expect(parseCliArgs(["ack", "delivery-1"]).command).toEqual({
      kind: "ack",
      deliveryId: "delivery-1",
    });
    expect(
      parseCliArgs(["nack", "delivery-1", "--reason", "poison"]).command,
    ).toEqual({
      kind: "nack",
      deliveryId: "delivery-1",
      reason: "poison",
    });
  });

  it("parses global flags anywhere", () => {
    expect(
      parseCliArgs([
        "topic",
        "list",
        "--json",
        "--control-url",
        "http://broker.local:8080",
        "--grpc-url=http://broker.local:9090",
      ]),
    ).toEqual({
      globals: {
        json: true,
        controlUrl: "http://broker.local:8080",
        grpcUrl: "http://broker.local:9090",
      },
      command: { kind: "topic-list" },
    });
  });

  it("rejects parser errors before clients are needed", () => {
    expect(() => parseCliArgs(["unknown"])).toThrow("Unknown command: unknown");
    expect(() => parseCliArgs(["topic", "list", "--unknown"])).toThrow(
      "Unknown option: --unknown",
    );
    expect(() =>
      parseCliArgs(["publish", "orders", "--data", "one", "--data", "two"]),
    ).toThrow("Duplicate option: --data");
    expect(() =>
      parseCliArgs(["publish", "--data", "hello", "orders"]),
    ).toThrow("Positional arguments must appear before options");
    expect(() => parseCliArgs(["topic", "create"])).toThrow(
      "Usage: ferrumq topic create <topic>",
    );
    expect(() => parseCliArgs(["ack"])).toThrow(
      "Usage: ferrumq ack <delivery-id>",
    );
  });
});

describe("config resolution", () => {
  it("uses defaults", () => {
    expect(resolveConfig({ json: false }, {})).toEqual({
      controlUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      json: false,
    });
  });

  it("uses environment overrides", () => {
    expect(
      resolveConfig(
        { json: true },
        {
          FERRUMQ_CONTROL_URL: "https://env.local:8443",
          FERRUMQ_GRPC_URL: "http://env.local:9090",
        },
      ),
    ).toEqual({
      controlUrl: "https://env.local:8443",
      grpcUrl: "http://env.local:9090",
      json: true,
    });
  });

  it("uses flag overrides before environment variables", () => {
    expect(
      resolveConfig(
        {
          json: false,
          controlUrl: "http://flag.local:8080",
          grpcUrl: "http://flag.local:9090",
        },
        {
          FERRUMQ_CONTROL_URL: "http://env.local:8080",
          FERRUMQ_GRPC_URL: "http://env.local:9090",
        },
      ),
    ).toEqual({
      controlUrl: "http://flag.local:8080",
      grpcUrl: "http://flag.local:9090",
      json: false,
    });
  });

  it("rejects invalid control URLs", () => {
    expect(() =>
      resolveConfig({ json: false, controlUrl: "not-a-url" }, {}),
    ).toThrow("control URL must be a valid URL");
    expect(() =>
      resolveConfig({ json: false, controlUrl: "ftp://broker.local" }, {}),
    ).toThrow("control URL must use http:// or https://");
    expect(() =>
      resolveConfig(
        { json: false, controlUrl: "http://user@broker.local" },
        {},
      ),
    ).toThrow("control URL must not include credentials");
    expect(() =>
      resolveConfig({ json: false, controlUrl: "http://broker.local/api" }, {}),
    ).toThrow("control URL must not include a path, query, or fragment");
  });

  it("rejects invalid gRPC URLs", () => {
    expect(() =>
      resolveConfig({ json: false, grpcUrl: "not-a-url" }, {}),
    ).toThrow("gRPC URL must be a valid URL");
    expect(() =>
      resolveConfig({ json: false, grpcUrl: "https://broker.local:9090" }, {}),
    ).toThrow("gRPC URL TLS/HTTPS is deferred; use http://host:port");
    expect(() =>
      resolveConfig({ json: false, grpcUrl: "http://broker.local" }, {}),
    ).toThrow("gRPC URL must include a port");
    expect(() =>
      resolveConfig(
        { json: false, grpcUrl: "http://broker.local:9090/api" },
        {},
      ),
    ).toThrow("gRPC URL must not include a path, query, or fragment");
  });
});

describe("CLI local commands", () => {
  it("prints version and root help", async () => {
    await expect(captureRun(["--version"])).resolves.toEqual({
      code: 0,
      stdout: [cliVersion],
      stderr: [],
    });

    const help = await captureRun([]);
    expect(help.code).toBe(0);
    expect(help.stderr).toEqual([]);
    expect(help.stdout.join("\n")).toContain("ferrumq publish <topic>");
  });

  it("prints command-specific help without validating URLs or calling clients", async () => {
    const invalidEnv = {
      FERRUMQ_CONTROL_URL: "not-a-url",
      FERRUMQ_GRPC_URL: "https://broker.local:9090",
    };
    const dependencies = {
      env: invalidEnv,
      controlClient: unreachableControlClient(),
      dataPlaneClient: unreachableDataPlaneClient(),
    };

    const topicHelp = await captureRun(["topic", "--help"], dependencies);
    expect(topicHelp.code).toBe(0);
    expect(topicHelp.stderr).toEqual([]);
    expect(topicHelp.stdout.join("\n")).toContain("FerrumQ topic commands");

    const publishHelp = await captureRun(["publish", "--help"], dependencies);
    expect(publishHelp.code).toBe(0);
    expect(publishHelp.stderr).toEqual([]);
    expect(publishHelp.stdout.join("\n")).toContain("FerrumQ publish command");
  });
});

describe("human output contracts", () => {
  it("formats control-plane commands", async () => {
    const controlClient = stubControlClient();

    await expect(captureRun(["health"], { controlClient })).resolves.toEqual({
      code: 0,
      stdout: ["health: ok"],
      stderr: [],
    });
    await expect(captureRun(["ready"], { controlClient })).resolves.toEqual({
      code: 0,
      stdout: ["ready: ready"],
      stderr: [],
    });
    await expect(captureRun(["status"], { controlClient })).resolves.toEqual({
      code: 0,
      stdout: [
        "mode: durable\ndata dir: ./.ferrumq\ntopics: 2\ndlq entries: 1",
      ],
      stderr: [],
    });
    await expect(
      captureRun(["topic", "create", "orders", "--partitions", "3"], {
        controlClient,
      }),
    ).resolves.toEqual({
      code: 0,
      stdout: ["topic created: orders (partitions: 3)"],
      stderr: [],
    });
    await expect(
      captureRun(["topic", "get", "orders"], { controlClient }),
    ).resolves.toEqual({
      code: 0,
      stdout: ["topic: orders (partitions: 3)"],
      stderr: [],
    });
    await expect(
      captureRun(["topic", "list"], { controlClient }),
    ).resolves.toEqual({
      code: 0,
      stdout: ["orders\tpartitions=3\npayments\tpartitions=1"],
      stderr: [],
    });
    await expect(
      captureRun(["dlq", "list"], { controlClient }),
    ).resolves.toEqual({
      code: 0,
      stdout: ["orders[0]@42\tmessage=message-1\tgroup=workers\treason=poison"],
      stderr: [],
    });
  });

  it("formats data-plane commands", async () => {
    const dataPlaneClient = stubDataPlaneClient();

    await expect(
      captureRun(["publish", "orders", "--data", "hello"], {
        dataPlaneClient,
      }),
    ).resolves.toEqual({
      code: 0,
      stdout: [
        "published: msg_00000000-0000-4000-8000-000000000001 orders[0]@9007199254740993",
      ],
      stderr: [],
    });
    await expect(
      captureRun(["consume", "orders", "--group", "workers"], {
        dataPlaneClient,
      }),
    ).resolves.toEqual({
      code: 0,
      stdout: [
        "delivery=delivery-1\tmessage=message-1\ttopic=orders\tpartition=0\toffset=9007199254740993\tattempt=2\tpayload=hello",
      ],
      stderr: [],
    });
    await expect(
      captureRun(["ack", "delivery-1"], { dataPlaneClient }),
    ).resolves.toEqual({
      code: 0,
      stdout: ["acked: delivery-1 consumer=ferrumq-cli"],
      stderr: [],
    });
    await expect(
      captureRun(["nack", "delivery-1", "--reason", "poison"], {
        dataPlaneClient,
      }),
    ).resolves.toEqual({
      code: 0,
      stdout: ["nacked: delivery-1 consumer=ferrumq-cli reason=poison"],
      stderr: [],
    });
  });
});

describe("JSON output contracts", () => {
  it("writes one JSON object and no stderr for control-plane successes", async () => {
    const controlClient = stubControlClient();

    expectSuccessfulJson(
      await captureRun(["--json", "health"], { controlClient }),
      {
        health: { status: "ok" },
      },
    );
    expectSuccessfulJson(
      await captureRun(["--json", "ready"], { controlClient }),
      {
        ready: { status: "ready" },
      },
    );
    expectSuccessfulJson(
      await captureRun(["--json", "status"], { controlClient }),
      {
        status: {
          mode: "durable",
          dataDir: "./.ferrumq",
          topics: 2,
          dlqEntries: 1,
        },
      },
    );
    expectSuccessfulJson(
      await captureRun(["--json", "topic", "create", "orders"], {
        controlClient,
      }),
      { topic: { name: "orders", partitions: 1 } },
    );
    expectSuccessfulJson(
      await captureRun(["--json", "topic", "get", "orders"], {
        controlClient,
      }),
      { topic: { name: "orders", partitions: 3 } },
    );
    expectSuccessfulJson(
      await captureRun(["--json", "topic", "list"], { controlClient }),
      {
        topics: [
          { name: "orders", partitions: 3 },
          { name: "payments", partitions: 1 },
        ],
      },
    );
    expectSuccessfulJson(
      await captureRun(["--json", "dlq", "list"], { controlClient }),
      {
        dlq: {
          items: [
            {
              topic: "orders",
              partition: 0,
              offset: 42,
              messageId: "message-1",
              consumerGroupId: "workers",
              reason: "poison",
              attemptCount: 3,
              timestamp: 1_700_000_000_000,
            },
          ],
        },
      },
    );
  });

  it("writes one JSON object and no stderr for data-plane successes", async () => {
    const dataPlaneClient = stubDataPlaneClient();

    expectSuccessfulJson(
      await captureRun(["--json", "publish", "orders", "--data", "hello"], {
        dataPlaneClient,
      }),
      {
        message: {
          id: "msg_00000000-0000-4000-8000-000000000001",
          topic: "orders",
          partition: 0,
          offset: "9007199254740993",
        },
      },
    );
    expectSuccessfulJson(
      await captureRun(["--json", "consume", "orders", "--group", "workers"], {
        dataPlaneClient,
      }),
      {
        messages: [
          {
            deliveryId: "delivery-1",
            topic: "orders",
            partition: 0,
            offset: "9007199254740993",
            messageId: "message-1",
            key: null,
            data: "hello",
            contentType: "text/plain",
            type: "example",
            source: "test",
            subject: null,
            idempotencyKey: null,
            timeUnixMs: "1700000000000",
            consumerGroup: "workers",
            consumerId: "ferrumq-cli",
            attemptNumber: 2,
            deliveredAtUnixMs: "1700000000000",
            leaseExpiresAtUnixMs: "1700000030000",
          },
        ],
      },
    );
    expectSuccessfulJson(
      await captureRun(["--json", "ack", "delivery-1"], { dataPlaneClient }),
      { ack: { deliveryId: "delivery-1", consumerId: "ferrumq-cli" } },
    );
    expectSuccessfulJson(
      await captureRun(["--json", "nack", "delivery-1"], { dataPlaneClient }),
      {
        nack: {
          deliveryId: "delivery-1",
          consumerId: "ferrumq-cli",
          reason: null,
        },
      },
    );
  });

  it("keeps expected errors as human text when --json is set", async () => {
    const result = await captureRun(["--json", "publish", "orders"], {
      dataPlaneClient: unreachableDataPlaneClient(),
    });

    expect(result).toEqual({
      code: 1,
      stdout: [],
      stderr: ["--data is required"],
    });
  });
});

describe("validation and expected failures", () => {
  it("validates topics and positive numbers before calling clients", async () => {
    const invalidTopic = await captureRun(["topic", "create", "bad topic"], {
      controlClient: unreachableControlClient(),
    });
    expect(invalidTopic.code).toBe(1);
    expect(invalidTopic.stderr.join("\n")).toContain(
      "topic contains invalid characters",
    );

    const invalidMax = await captureRun(
      ["consume", "orders", "--group", "group.1", "--max", "0"],
      {
        dataPlaneClient: unreachableDataPlaneClient(),
      },
    );
    expect(invalidMax.code).toBe(1);
    expect(invalidMax.stderr.join("\n")).toContain(
      "--max must be a positive integer",
    );
  });

  it("formats gRPC status errors without stack traces", async () => {
    const dataPlaneClient: DataPlaneClient = {
      async publish() {
        throw { code: 3, details: "topic_name must not be empty" };
      },
      consume: unreachableDataPlaneClient().consume,
      ack: unreachableDataPlaneClient().ack,
      nack: unreachableDataPlaneClient().nack,
      close: unreachableDataPlaneClient().close,
    };

    const result = await captureRun(["publish", "orders", "--data", "hello"], {
      dataPlaneClient,
    });

    expect(result).toEqual({
      code: 1,
      stdout: [],
      stderr: ["gRPC INVALID_ARGUMENT (3): topic_name must not be empty"],
    });
  });
});

describe("HTTP control client", () => {
  it("maps successful control-plane requests and DTOs", async () => {
    const calls: Array<{
      input: string;
      init: Parameters<FetchLike>[1];
    }> = [];
    const fetchImpl = vi.fn<FetchLike>(async (input, init) => {
      calls.push({ input, init });
      switch (`${init?.method ?? "GET"} ${input}`) {
        case "GET http://control.local:8080/health":
          return response(200, { status: "ok" });
        case "GET http://control.local:8080/ready":
          return response(200, { status: "ready" });
        case "GET http://control.local:8080/v1/status":
          return response(200, {
            mode: "durable",
            dataDir: "./.ferrumq",
            topics: 2,
            dlqEntries: 1,
          });
        case "POST http://control.local:8080/v1/topics":
          expect(init?.body).toBe('{"name":"orders","partitions":3}');
          return response(200, { name: "orders", partitions: 3 });
        case "GET http://control.local:8080/v1/topics/orders":
          return response(200, { name: "orders", partitions: 3 });
        case "GET http://control.local:8080/v1/topics":
          return response(200, { items: [{ name: "orders", partitions: 3 }] });
        case "GET http://control.local:8080/v1/dlq?topic=orders":
          return response(200, { items: [] });
        default:
          throw new Error(`unexpected request ${init?.method} ${input}`);
      }
    });
    const client = createControlPlaneClient(
      "http://control.local:8080",
      fetchImpl,
    );

    await expect(client.health()).resolves.toEqual({ status: "ok" });
    await expect(client.ready()).resolves.toEqual({ status: "ready" });
    await expect(client.status()).resolves.toEqual({
      mode: "durable",
      dataDir: "./.ferrumq",
      topics: 2,
      dlqEntries: 1,
    });
    await expect(client.createTopic("orders", 3)).resolves.toEqual({
      name: "orders",
      partitions: 3,
    });
    await expect(client.getTopic("orders")).resolves.toEqual({
      name: "orders",
      partitions: 3,
    });
    await expect(client.listTopics()).resolves.toEqual({
      items: [{ name: "orders", partitions: 3 }],
    });
    await expect(client.listDlq("orders")).resolves.toEqual({ items: [] });
    expect(calls.map((call) => `${call.init?.method} ${call.input}`)).toEqual([
      "GET http://control.local:8080/health",
      "GET http://control.local:8080/ready",
      "GET http://control.local:8080/v1/status",
      "POST http://control.local:8080/v1/topics",
      "GET http://control.local:8080/v1/topics/orders",
      "GET http://control.local:8080/v1/topics",
      "GET http://control.local:8080/v1/dlq?topic=orders",
    ]);
  });

  it("surfaces FerrumQ error envelopes", async () => {
    for (const [status, code] of [
      [400, "VALIDATION_ERROR"],
      [404, "NOT_FOUND"],
      [409, "CONFLICT"],
      [500, "INTERNAL_ERROR"],
    ] as const) {
      const client = createControlPlaneClient(
        "http://control.local:8080",
        vi.fn<FetchLike>(async () =>
          response(status, {
            error: {
              code,
              message: "request failed",
              details: {},
              statusCode: status,
            },
          }),
        ),
      );

      await expect(client.status()).rejects.toThrow(
        `HTTP ${status} ${code}: request failed`,
      );
    }
  });

  it("surfaces malformed error bodies, network failures, invalid JSON, and schema mismatches", async () => {
    const malformedErrorClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => response(418, { nope: true }, "Teapot")),
    );
    await expect(malformedErrorClient.status()).rejects.toThrow(
      "HTTP 418: Teapot",
    );

    const invalidErrorJsonClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => invalidJsonResponse(500, "Server Error")),
    );
    await expect(invalidErrorJsonClient.status()).rejects.toThrow(
      "HTTP 500: Server Error",
    );

    const networkClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => {
        throw new TypeError("connection refused");
      }),
    );
    await expect(networkClient.ready()).rejects.toThrow(
      "Network request failed for GET http://control.local:8080/ready: connection refused",
    );

    const invalidJsonClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => invalidJsonResponse(200)),
    );
    await expect(invalidJsonClient.health()).rejects.toThrow(
      "Unexpected response from control API",
    );

    const schemaMismatchClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => response(200, { status: "" })),
    );
    await expect(schemaMismatchClient.health()).rejects.toThrow(
      "Unexpected response from control API",
    );
  });
});

describeBuiltCli("built CLI smoke and parser failures", () => {
  it("prints root, topic, publish help, and version through dist/cli.js", async () => {
    await expect(runBuiltCli(["--version"])).resolves.toMatchObject({
      code: 0,
      stdout: cliVersion,
      stderr: "",
    });
    await expect(runBuiltCli(["--help"])).resolves.toMatchObject({
      code: 0,
      stderr: "",
    });

    const topicHelp = await runBuiltCli(["topic", "--help"]);
    expect(topicHelp).toMatchObject({ code: 0, stderr: "" });
    expect(topicHelp.stdout).toContain("FerrumQ topic commands");

    const publishHelp = await runBuiltCli(["publish", "--help"]);
    expect(publishHelp).toMatchObject({ code: 0, stderr: "" });
    expect(publishHelp.stdout).toContain("FerrumQ publish command");
  });

  it("returns stackless expected parser and validation errors through dist/cli.js", async () => {
    for (const [args, expected] of [
      [["topic", "create"], "Usage: ferrumq topic create <topic>"],
      [["ack"], "Usage: ferrumq ack <delivery-id>"],
      [["publish", "orders"], "--data is required"],
      [
        ["topic", "create", "orders", "--partitions", "0"],
        "--partitions must be a positive integer",
      ],
      [["unknown"], "Unknown command: unknown"],
      [["topic", "list", "--unknown"], "Unknown option: --unknown"],
      [
        ["publish", "orders", "--data", "one", "--data", "two"],
        "Duplicate option: --data",
      ],
      [
        ["publish", "--data", "hello", "orders"],
        "Positional arguments must appear before options",
      ],
    ] as const) {
      const result = await runBuiltCli(args);
      expect(result.code).toBe(1);
      expect(result.stdout).toBe("");
      expect(result.stderr).toBe(expected);
      expect(result.stderr).not.toContain("Error:");
    }
  });

  it("supports the msg compatibility alias entrypoint", async () => {
    const packageJson = JSON.parse(
      readFileSync(path.join(packageRoot, "package.json"), "utf8"),
    ) as { bin?: Record<string, string> };
    expect(packageJson.bin?.msg).toBe("./dist/cli.js");

    await expect(runBuiltCli(["--version"], "msg")).resolves.toMatchObject({
      code: 0,
      stdout: cliVersion,
      stderr: "",
    });
  });
});

async function runBuiltCli(
  args: readonly string[],
  argv1 = distCliPath,
): Promise<{ code: number; stdout: string; stderr: string }> {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const previousArgv = process.argv;
  const previousExitCode = process.exitCode;
  const previousControlUrl = process.env.FERRUMQ_CONTROL_URL;
  const previousGrpcUrl = process.env.FERRUMQ_GRPC_URL;
  const stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
    chunk: unknown,
    ...writeArgs: unknown[]
  ) => {
    stdout.push(renderChunk(chunk));
    callWriteCallback(writeArgs);
    return true;
  }) as typeof process.stdout.write);
  const stderrSpy = vi.spyOn(process.stderr, "write").mockImplementation(((
    chunk: unknown,
    ...writeArgs: unknown[]
  ) => {
    stderr.push(renderChunk(chunk));
    callWriteCallback(writeArgs);
    return true;
  }) as typeof process.stderr.write);

  try {
    process.argv = [process.execPath, argv1, ...args];
    process.exitCode = undefined;
    delete process.env.FERRUMQ_CONTROL_URL;
    delete process.env.FERRUMQ_GRPC_URL;
    await import(
      `${pathToFileURL(distCliPath).href}?vitest=${builtCliImportCounter++}`
    );
    return {
      code: typeof process.exitCode === "number" ? process.exitCode : 0,
      stdout: stdout.join("").trimEnd(),
      stderr: stderr.join("").trimEnd(),
    };
  } finally {
    process.argv = previousArgv;
    process.exitCode = previousExitCode;
    restoreOptionalEnv("FERRUMQ_CONTROL_URL", previousControlUrl);
    restoreOptionalEnv("FERRUMQ_GRPC_URL", previousGrpcUrl);
    stdoutSpy.mockRestore();
    stderrSpy.mockRestore();
  }
}

function renderChunk(chunk: unknown): string {
  if (Buffer.isBuffer(chunk)) {
    return chunk.toString("utf8");
  }
  if (chunk instanceof Uint8Array) {
    return Buffer.from(chunk).toString("utf8");
  }
  return String(chunk);
}

function callWriteCallback(writeArgs: unknown[]): void {
  for (const arg of writeArgs) {
    if (typeof arg === "function") {
      (arg as () => void)();
    }
  }
}

function restoreOptionalEnv(key: string, value: string | undefined): void {
  if (value === undefined) {
    delete process.env[key];
    return;
  }
  process.env[key] = value;
}

function stubControlClient(): ControlPlaneClient {
  return {
    async health() {
      return { status: "ok" };
    },
    async ready() {
      return { status: "ready" };
    },
    async status() {
      return {
        mode: "durable",
        dataDir: "./.ferrumq",
        topics: 2,
        dlqEntries: 1,
      };
    },
    async createTopic(name, partitions) {
      return { name, partitions };
    },
    async getTopic(name) {
      return { name, partitions: 3 };
    },
    async listTopics() {
      return {
        items: [
          { name: "orders", partitions: 3 },
          { name: "payments", partitions: 1 },
        ],
      };
    },
    async listDlq() {
      return {
        items: [
          {
            topic: "orders",
            partition: 0,
            offset: 42,
            messageId: "message-1",
            consumerGroupId: "workers",
            reason: "poison",
            attemptCount: 3,
            timestamp: 1_700_000_000_000,
          },
        ],
      };
    },
  };
}

function stubDataPlaneClient(): DataPlaneClient {
  return {
    async publish(request) {
      return {
        topic: request.topic,
        partition: 0,
        offset: "9007199254740993",
        messageId: request.messageId,
      };
    },
    async consume(request) {
      return {
        messages: [
          {
            deliveryId: "delivery-1",
            topic: request.topic,
            partition: 0,
            offset: "9007199254740993",
            messageId: "message-1",
            key: "",
            payload: Buffer.from("hello"),
            contentType: "text/plain",
            type: "example",
            source: "test",
            subject: "",
            idempotencyKey: "",
            timeUnixMs: "1700000000000",
            consumerGroup: request.consumerGroup,
            consumerId: request.consumerId,
            attemptNumber: 2,
            deliveredAtUnixMs: "1700000000000",
            leaseExpiresAtUnixMs: "1700000030000",
          },
        ],
      };
    },
    async ack() {},
    async nack() {},
    close() {},
  };
}

function unreachableControlClient(): ControlPlaneClient {
  return {
    async health() {
      throw new Error("unexpected health call");
    },
    async ready() {
      throw new Error("unexpected ready call");
    },
    async status() {
      throw new Error("unexpected status call");
    },
    async createTopic() {
      throw new Error("unexpected createTopic call");
    },
    async getTopic() {
      throw new Error("unexpected getTopic call");
    },
    async listTopics() {
      throw new Error("unexpected listTopics call");
    },
    async listDlq() {
      throw new Error("unexpected listDlq call");
    },
  };
}

function unreachableDataPlaneClient(): DataPlaneClient {
  return {
    async publish() {
      throw new Error("unexpected publish call");
    },
    async consume() {
      throw new Error("unexpected consume call");
    },
    async ack() {
      throw new Error("unexpected ack call");
    },
    async nack() {
      throw new Error("unexpected nack call");
    },
    close() {
      throw new Error("unexpected close call");
    },
  };
}
