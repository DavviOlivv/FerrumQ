import { describe, expect, it, vi } from "vitest";

import {
  cliVersion,
  parseCliArgs,
  resolveConfig,
  runCli,
} from "../src/index.js";

import type { DataPlaneClient } from "@ferrumq/protocol";
import type {
  ControlPlaneClient,
  FetchLike,
  ResponseLike,
} from "../src/http-client.js";
import type { RunCliOptions } from "../src/index.js";

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

function jsonOutput(result: { stdout: string[] }): unknown {
  return JSON.parse(result.stdout.join("\n"));
}

function response(status: number, payload: unknown): ResponseLike {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? "OK" : "Bad Request",
    async json() {
      return payload;
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
});

describe("config resolution", () => {
  it("uses defaults, environment, then flags in precedence order", () => {
    expect(resolveConfig({ json: false })).toEqual({
      controlUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      json: false,
    });

    expect(
      resolveConfig(
        { json: true },
        {
          FERRUMQ_CONTROL_URL: "http://env.local:8080",
          FERRUMQ_GRPC_URL: "http://env.local:9090",
        },
      ),
    ).toEqual({
      controlUrl: "http://env.local:8080",
      grpcUrl: "http://env.local:9090",
      json: true,
    });

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
});

describe("CLI output and expected failures", () => {
  it("prints version and help", async () => {
    await expect(captureRun(["--version"])).resolves.toEqual({
      code: 0,
      stdout: [cliVersion],
      stderr: [],
    });

    const help = await captureRun([]);
    expect(help.code).toBe(0);
    expect(help.stdout.join("\n")).toContain("ferrumq publish <topic>");
  });

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

  it("formats HTTP success JSON wrappers", async () => {
    const fetchImpl = vi.fn<FetchLike>(async (input, init) => {
      expect(input).toBe("http://127.0.0.1:8080/health");
      expect(init?.method).toBe("GET");
      return response(200, { status: "ok" });
    });

    const result = await captureRun(["--json", "health"], { fetch: fetchImpl });

    expect(result.code).toBe(0);
    expect(jsonOutput(result)).toEqual({ health: { status: "ok" } });
  });

  it("surfaces HTTP error envelopes and network failures without stack traces", async () => {
    const errorFetch = vi.fn<FetchLike>(async () =>
      response(400, {
        error: {
          code: "VALIDATION_ERROR",
          message: "topic_name must not be empty",
          details: {},
          statusCode: 400,
        },
      }),
    );

    const httpError = await captureRun(["topic", "get", "orders"], {
      fetch: errorFetch,
    });
    expect(httpError).toEqual({
      code: 1,
      stdout: [],
      stderr: ["HTTP 400 VALIDATION_ERROR: topic_name must not be empty"],
    });

    const networkFetch = vi.fn<FetchLike>(async () => {
      throw new TypeError("connection refused");
    });
    const networkError = await captureRun(["ready"], { fetch: networkFetch });
    expect(networkError.code).toBe(1);
    expect(networkError.stderr.join("\n")).toContain("Network request failed");
    expect(networkError.stderr.join("\n")).not.toContain("TypeError:");
  });

  it("formats gRPC publish, consume, ack, and nack JSON wrappers", async () => {
    const requests: unknown[] = [];
    const dataPlaneClient: DataPlaneClient = {
      async publish(request) {
        requests.push(request);
        return {
          topic: request.topic,
          partition: 0,
          offset: "9007199254740993",
          messageId: request.messageId,
        };
      },
      async consume(request) {
        requests.push(request);
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
              attemptNumber: 1,
              deliveredAtUnixMs: "1700000000000",
              leaseExpiresAtUnixMs: "1700000030000",
            },
          ],
        };
      },
      async ack(request) {
        requests.push(request);
      },
      async nack(request) {
        requests.push(request);
      },
    };

    const publish = await captureRun(
      ["--json", "publish", "orders", "--data", "hello"],
      {
        dataPlaneClient,
      },
    );
    expect(jsonOutput(publish)).toEqual({
      message: {
        id: "msg_00000000-0000-4000-8000-000000000001",
        topic: "orders",
        partition: 0,
        offset: "9007199254740993",
      },
    });

    const consume = await captureRun(
      ["--json", "consume", "orders", "--group", "group.1"],
      {
        dataPlaneClient,
      },
    );
    expect(jsonOutput(consume)).toEqual({
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
          consumerGroup: "group.1",
          consumerId: "ferrumq-cli",
          attemptNumber: 1,
          deliveredAtUnixMs: "1700000000000",
          leaseExpiresAtUnixMs: "1700000030000",
        },
      ],
    });

    expect(
      jsonOutput(
        await captureRun(["--json", "ack", "delivery-1"], {
          dataPlaneClient,
        }),
      ),
    ).toEqual({ ack: { deliveryId: "delivery-1", consumerId: "ferrumq-cli" } });
    expect(
      jsonOutput(
        await captureRun(
          ["--json", "nack", "delivery-1", "--reason", "poison"],
          {
            dataPlaneClient,
          },
        ),
      ),
    ).toEqual({
      nack: {
        deliveryId: "delivery-1",
        consumerId: "ferrumq-cli",
        reason: "poison",
      },
    });

    expect(requests[0]).toMatchObject({
      topic: "orders",
      messageId: "msg_00000000-0000-4000-8000-000000000001",
      contentType: "application/json",
      type: "ferrumq.cli.message",
      source: "ferrumq-cli",
      timeUnixMs: "1700000000000",
    });
  });

  it("formats gRPC status errors", async () => {
    const dataPlaneClient: DataPlaneClient = {
      async publish() {
        throw { code: 3, details: "topic_name must not be empty" };
      },
      consume: unreachableDataPlaneClient().consume,
      ack: unreachableDataPlaneClient().ack,
      nack: unreachableDataPlaneClient().nack,
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
  };
}
