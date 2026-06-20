import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it, vi } from "vitest";
import type { FetchLike, ResponseLike } from "../src/index.js";
import {
  brokerStatusResponseSchema,
  ControlPlaneRequestError,
  createControlPlaneClient,
  createGrpcDataPlaneClient,
  defaultDataPlaneProtoPath,
  dlqListResponseSchema,
  ferrumQErrorEnvelopeSchema,
  formatGrpcError,
  grpcStatusName,
  httpStatusResponseSchema,
  normalizeGrpcTarget,
  topicListResponseSchema,
  topicResponseSchema,
} from "../src/index.js";

describe("HTTP schemas", () => {
  it("parses success DTOs", () => {
    expect(httpStatusResponseSchema.parse({ status: "ok" })).toEqual({
      status: "ok",
    });
    expect(
      brokerStatusResponseSchema.parse({
        mode: "durable",
        dataDir: "./.ferrumq",
        topics: 2,
        dlqEntries: 1,
      }),
    ).toEqual({
      mode: "durable",
      dataDir: "./.ferrumq",
      topics: 2,
      dlqEntries: 1,
    });
    expect(
      topicResponseSchema.parse({ name: "orders", partitions: 3 }),
    ).toEqual({
      name: "orders",
      partitions: 3,
    });
    expect(
      topicListResponseSchema.parse({
        items: [
          { name: "orders", partitions: 3 },
          { name: "payments", partitions: 1 },
        ],
      }),
    ).toEqual({
      items: [
        { name: "orders", partitions: 3 },
        { name: "payments", partitions: 1 },
      ],
    });
    expect(
      dlqListResponseSchema.parse({
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
      }),
    ).toEqual({
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
    });
  });

  it("parses FerrumQ error envelopes", () => {
    expect(
      ferrumQErrorEnvelopeSchema.parse({
        error: {
          code: "VALIDATION_ERROR",
          message: "topic_name must not be empty",
          details: {},
          statusCode: 400,
        },
      }),
    ).toEqual({
      error: {
        code: "VALIDATION_ERROR",
        message: "topic_name must not be empty",
        details: {},
        statusCode: 400,
      },
    });
  });
});

describe("HTTP control-plane client", () => {
  it.each([
    {
      name: "GET /health",
      requestPath: "/health",
      payload: { status: "ok" },
      request: (client: ReturnType<typeof createControlPlaneClient>) =>
        client.health(),
      expected: { status: "ok" },
    },
    {
      name: "GET /ready",
      requestPath: "/ready",
      payload: { status: "ready" },
      request: (client: ReturnType<typeof createControlPlaneClient>) =>
        client.ready(),
      expected: { status: "ready" },
    },
    {
      name: "GET /v1/status",
      requestPath: "/v1/status",
      payload: {
        mode: "durable",
        dataDir: "./.ferrumq",
        topics: 2,
        dlqEntries: 1,
      },
      request: (client: ReturnType<typeof createControlPlaneClient>) =>
        client.status(),
      expected: {
        mode: "durable",
        dataDir: "./.ferrumq",
        topics: 2,
        dlqEntries: 1,
      },
    },
    {
      name: "GET /v1/topics",
      requestPath: "/v1/topics",
      payload: { items: [{ name: "orders", partitions: 3 }] },
      request: (client: ReturnType<typeof createControlPlaneClient>) =>
        client.listTopics(),
      expected: { items: [{ name: "orders", partitions: 3 }] },
    },
    {
      name: "GET /v1/dlq",
      requestPath: "/v1/dlq",
      payload: {
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
      request: (client: ReturnType<typeof createControlPlaneClient>) =>
        client.listDlq(),
      expected: {
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
  ])("maps $name success responses", async (testCase) => {
    const fetchImpl = vi.fn<FetchLike>(async (input, init) => {
      expect(init?.method).toBe("GET");
      expect(input).toBe(`http://control.local:8080${testCase.requestPath}`);
      return response(200, testCase.payload);
    });
    const client = createControlPlaneClient(
      "http://control.local:8080/",
      fetchImpl,
    );

    await expect(testCase.request(client)).resolves.toEqual(testCase.expected);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

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

  it("distinguishes expected HTTP failure modes", async () => {
    const ferrumQClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () =>
        response(409, {
          error: {
            code: "TOPIC_ALREADY_EXISTS",
            message: "topic exists",
            details: {},
            statusCode: 409,
          },
        }),
      ),
    );
    await expect(ferrumQClient.status()).rejects.toMatchObject({
      kind: "ferrumq-error",
      method: "GET",
      message: "HTTP 409 TOPIC_ALREADY_EXISTS: topic exists",
      status: 409,
      url: "http://control.local:8080/v1/status",
      ferrumqError: {
        code: "TOPIC_ALREADY_EXISTS",
        message: "topic exists",
        details: {},
        statusCode: 409,
      },
    });

    const malformedErrorClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => response(418, { nope: true }, "Teapot")),
    );
    await expect(malformedErrorClient.status()).rejects.toMatchObject({
      kind: "malformed-error",
      message: "HTTP 418: Teapot",
      status: 418,
      statusText: "Teapot",
    });

    const networkClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => {
        throw new TypeError("connection refused");
      }),
    );
    await expect(networkClient.ready()).rejects.toMatchObject({
      kind: "network",
      method: "GET",
      url: "http://control.local:8080/ready",
      message:
        "Network request failed for GET http://control.local:8080/ready: connection refused",
    });

    const invalidJsonClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => invalidJsonResponse(200)),
    );
    await expect(invalidJsonClient.health()).rejects.toMatchObject({
      kind: "invalid-json",
      method: "GET",
      url: "http://control.local:8080/health",
      message: "Unexpected response from control API: invalid JSON",
    });

    const invalidJsonHttpErrorClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => invalidJsonResponse(500, "Server Error")),
    );
    await expect(invalidJsonHttpErrorClient.status()).rejects.toMatchObject({
      kind: "malformed-error",
      method: "GET",
      url: "http://control.local:8080/v1/status",
      status: 500,
      statusText: "Server Error",
      message: "HTTP 500: Server Error",
    });

    const schemaMismatchClient = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => response(200, { status: "" })),
    );
    await expect(schemaMismatchClient.health()).rejects.toMatchObject({
      kind: "schema",
      method: "GET",
      url: "http://control.local:8080/health",
      validationIssues: expect.arrayContaining([
        expect.objectContaining({ path: ["status"] }),
      ]),
    });
  });

  it("uses a typed request error class for control-plane failures", async () => {
    const client = createControlPlaneClient(
      "http://control.local:8080",
      vi.fn<FetchLike>(async () => invalidJsonResponse(500, "Server Error")),
    );

    await expect(client.status()).rejects.toBeInstanceOf(
      ControlPlaneRequestError,
    );
  });

  it("passes AbortSignal to fetch", async () => {
    const controller = new AbortController();
    const fetchImpl = vi.fn<FetchLike>(async (_input, init) => {
      expect(init?.signal).toBe(controller.signal);
      return response(200, { status: "ok" });
    });
    const client = createControlPlaneClient(
      "http://control.local:8080",
      fetchImpl,
    );

    await expect(client.health({ signal: controller.signal })).resolves.toEqual(
      { status: "ok" },
    );
  });
});

describe("gRPC URL helpers", () => {
  it("normalizes http://host:port URLs to grpc-js targets", () => {
    expect(normalizeGrpcTarget("http://127.0.0.1:9090")).toBe("127.0.0.1:9090");
    expect(normalizeGrpcTarget("http://broker.local:19090")).toBe(
      "broker.local:19090",
    );
  });

  it("rejects unsupported gRPC target URL shapes", () => {
    expect(() => normalizeGrpcTarget("not-a-url")).toThrow(
      "gRPC URL must be a valid URL like http://127.0.0.1:9090",
    );
    expect(() => normalizeGrpcTarget("https://broker.local:9090")).toThrow(
      "TLS/HTTPS",
    );
    expect(() => normalizeGrpcTarget("grpc://broker.local:9090")).toThrow(
      "gRPC URL must use http://host:port",
    );
    expect(() => normalizeGrpcTarget("http://user@broker.local:9090")).toThrow(
      "gRPC URL must not include credentials",
    );
    expect(() => normalizeGrpcTarget("http://broker.local")).toThrow(
      "gRPC URL must include host and port",
    );
    expect(() => normalizeGrpcTarget("http://broker.local:9090/api")).toThrow(
      "gRPC URL must not include a path, query, or fragment",
    );
  });
});

describe("gRPC status formatting", () => {
  it("formats expected tonic status codes", () => {
    for (const [code, name] of [
      [3, "INVALID_ARGUMENT"],
      [5, "NOT_FOUND"],
      [9, "FAILED_PRECONDITION"],
      [14, "UNAVAILABLE"],
      [13, "INTERNAL"],
    ] as const) {
      expect(grpcStatusName(code)).toBe(name);
      expect(formatGrpcError({ code, details: "request failed" })).toBe(
        `gRPC ${name} (${code}): request failed`,
      );
    }
  });

  it("formats unreachable and non-standard errors", () => {
    expect(formatGrpcError(new Error("connect ECONNREFUSED"))).toBe(
      "gRPC request failed: connect ECONNREFUSED",
    );
    expect(formatGrpcError("failed")).toBe("gRPC request failed");
  });
});

describe("gRPC client helpers", () => {
  it("resolves the packaged or source-tree proto", () => {
    expect(existsSync(defaultDataPlaneProtoPath())).toBe(true);
  });

  it("keeps publish response field numbers and packaged proto declarations compatible", () => {
    const sourcePath = path.resolve(
      process.cwd(),
      "../../crates/msg-protocol/proto/ferrumq/dataplane/v1/dataplane.proto",
    );
    const source = readFileSync(sourcePath, "utf8");
    expect(source).toContain("package ferrumq.dataplane.v1;");
    expect(source).toMatch(/string topic = 1;/);
    expect(source).toMatch(/uint32 partition = 2;/);
    expect(source).toMatch(/uint64 offset = 3;/);
    expect(source).toMatch(/string message_id = 4;/);
    expect(source).toMatch(/bool deduplicated = 5;/);

    const packagedPath = path.resolve(
      process.cwd(),
      "dist/proto/ferrumq/dataplane/v1/dataplane.proto",
    );
    if (existsSync(packagedPath)) {
      expect(readFileSync(packagedPath, "utf8")).toBe(source);
    }
  });

  it("defaults an absent deduplicated response field to false", async () => {
    const client = createGrpcDataPlaneClient("http://broker.local:19090", {
      protoPath: "/tmp/dataplane.proto",
      createRawClient() {
        return {
          publish(
            _request: unknown,
            _options: unknown,
            callback: (error: null, response: unknown) => void,
          ) {
            callback(null, {
              topic: "orders",
              partition: 0,
              offset: "0",
              messageId: "message-1",
            });
          },
        };
      },
    });

    await expect(
      client.publish({
        topic: "orders",
        messageId: "message-1",
        payload: Buffer.from("hello"),
        contentType: "text/plain",
        type: "example",
        source: "test",
        timeUnixMs: "1",
      }),
    ).resolves.toMatchObject({ deduplicated: false });
  });

  it("passes deadlines, cancels active calls, and ignores late callbacks", async () => {
    let callback: ((error: null, response: unknown) => void) | undefined;
    const cancel = vi.fn();
    const close = vi.fn();
    const client = createGrpcDataPlaneClient("http://broker.local:19090", {
      protoPath: "/tmp/dataplane.proto",
      createRawClient() {
        return {
          close,
          publish(
            _request: unknown,
            options: { deadline?: number },
            rawCallback: (error: null, response: unknown) => void,
          ) {
            expect(options.deadline).toBe(1_700_000_000_000);
            callback = rawCallback;
            return { cancel };
          },
        };
      },
    });

    const pending = client.publish(
      {
        topic: "orders",
        messageId: "message-1",
        payload: Buffer.from("hello"),
        contentType: "text/plain",
        type: "example",
        source: "test",
        timeUnixMs: "1700000000000",
      },
      { deadline: 1_700_000_000_000 },
    );
    client.close();
    client.close();

    await expect(pending).rejects.toMatchObject({
      code: 1,
      details: "gRPC client is closed",
    });
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(close).toHaveBeenCalledTimes(1);
    expect(() =>
      callback?.(null, {
        topic: "orders",
        partition: 0,
        offset: "1",
        messageId: "message-1",
      }),
    ).not.toThrow();
  });

  it("maps publish requests and preserves decimal uint64 responses", async () => {
    const calls: unknown[] = [];
    const client = createGrpcDataPlaneClient("http://broker.local:19090", {
      protoPath: "/tmp/dataplane.proto",
      createRawClient(target, protoPath) {
        expect(target).toBe("broker.local:19090");
        expect(protoPath).toBe("/tmp/dataplane.proto");
        return {
          publish(
            request: unknown,
            _options: unknown,
            callback: (error: null, response: unknown) => void,
          ) {
            calls.push(request);
            callback(null, {
              topic: "orders",
              partition: 0,
              offset: "18446744073709551615",
              messageId: "message-1",
              deduplicated: false,
            });
          },
        };
      },
    });

    await expect(
      client.publish({
        topic: "orders",
        messageId: "message-1",
        payload: Buffer.from("hello"),
        contentType: "text/plain",
        type: "example",
        source: "test",
        timeUnixMs: "1700000000000",
      }),
    ).resolves.toEqual({
      topic: "orders",
      partition: 0,
      offset: "18446744073709551615",
      messageId: "message-1",
      deduplicated: false,
    });

    expect(calls).toEqual([
      {
        topic: "orders",
        messageId: "message-1",
        key: "",
        payload: Buffer.from("hello"),
        contentType: "text/plain",
        type: "example",
        source: "test",
        subject: "",
        idempotencyKey: "",
        timeUnixMs: "1700000000000",
      },
    ]);
  });

  it("maps consume, ack, and nack requests and responses", async () => {
    const calls: Array<{ method: string; request: unknown }> = [];
    const client = createGrpcDataPlaneClient("http://broker.local:19090", {
      protoPath: "/tmp/dataplane.proto",
      createRawClient() {
        return {
          consume(
            request: unknown,
            _options: unknown,
            callback: (error: null, response: unknown) => void,
          ) {
            calls.push({ method: "consume", request });
            callback(null, {
              messages: [
                {
                  deliveryId: "delivery-1",
                  topic: "orders",
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
                  consumerGroup: "workers",
                  consumerId: "consumer-1",
                  attemptNumber: 2,
                  deliveredAtUnixMs: "1700000000000",
                  leaseExpiresAtUnixMs: "1700000030000",
                },
              ],
            });
          },
          ack(
            request: unknown,
            _options: unknown,
            callback: (error: null, response: unknown) => void,
          ) {
            calls.push({ method: "ack", request });
            callback(null, {});
          },
          nack(
            request: unknown,
            _options: unknown,
            callback: (error: null, response: unknown) => void,
          ) {
            calls.push({ method: "nack", request });
            callback(null, {});
          },
        };
      },
    });

    await expect(
      client.consume({
        topic: "orders",
        consumerGroup: "workers",
        consumerId: "consumer-1",
        maxMessages: 10,
        leaseMs: "30000",
        nowUnixMs: "1700000000000",
      }),
    ).resolves.toEqual({
      messages: [
        {
          deliveryId: "delivery-1",
          topic: "orders",
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
          consumerGroup: "workers",
          consumerId: "consumer-1",
          attemptNumber: 2,
          deliveredAtUnixMs: "1700000000000",
          leaseExpiresAtUnixMs: "1700000030000",
        },
      ],
    });
    await expect(
      client.ack({ deliveryId: "delivery-1", consumerId: "consumer-1" }),
    ).resolves.toBeUndefined();
    await expect(
      client.nack({
        deliveryId: "delivery-1",
        consumerId: "consumer-1",
        reason: "poison",
      }),
    ).resolves.toBeUndefined();

    expect(calls).toEqual([
      {
        method: "consume",
        request: {
          topic: "orders",
          consumerGroup: "workers",
          consumerId: "consumer-1",
          maxMessages: 10,
          leaseMs: "30000",
          nowUnixMs: "1700000000000",
        },
      },
      {
        method: "ack",
        request: { deliveryId: "delivery-1", consumerId: "consumer-1" },
      },
      {
        method: "nack",
        request: {
          deliveryId: "delivery-1",
          consumerId: "consumer-1",
          reason: "poison",
        },
      },
    ]);
  });

  it("surfaces missing proto path and missing service failures", () => {
    expect(() =>
      createGrpcDataPlaneClient("http://broker.local:19090", {
        protoPath: path.join(tmpdir(), "missing-dataplane.proto"),
      }),
    ).toThrow("data-plane proto file not found");

    const protoDir = mkdtempSync(path.join(tmpdir(), "ferrumq-proto-"));
    const protoPath = path.join(protoDir, "dataplane.proto");
    writeFileSync(
      protoPath,
      [
        'syntax = "proto3";',
        "package ferrumq.dataplane.v1;",
        "message Placeholder {}",
      ].join("\n"),
    );

    expect(() =>
      createGrpcDataPlaneClient("http://broker.local:19090", { protoPath }),
    ).toThrow(
      "data-plane proto does not expose ferrumq.dataplane.v1.FerrumQDataPlane",
    );
  });

  it("surfaces missing raw methods and malformed response fields", async () => {
    const missingMethodClient = createGrpcDataPlaneClient(
      "http://broker.local:19090",
      {
        protoPath: "/tmp/dataplane.proto",
        createRawClient() {
          return {};
        },
      },
    );
    await expect(
      missingMethodClient.publish({
        topic: "orders",
        messageId: "message-1",
        payload: Buffer.from("hello"),
        contentType: "text/plain",
        type: "example",
        source: "test",
        timeUnixMs: "1700000000000",
      }),
    ).rejects.toThrow("gRPC client does not expose Publish");

    const malformedPublishClient = createGrpcDataPlaneClient(
      "http://broker.local:19090",
      {
        protoPath: "/tmp/dataplane.proto",
        createRawClient() {
          return {
            publish(
              _request: unknown,
              _options: unknown,
              callback: (error: null, response: unknown) => void,
            ) {
              callback(null, {
                topic: "orders",
                partition: "0",
                offset: "42",
                messageId: "message-1",
              });
            },
          };
        },
      },
    );
    await expect(
      malformedPublishClient.publish({
        topic: "orders",
        messageId: "message-1",
        payload: Buffer.from("hello"),
        contentType: "text/plain",
        type: "example",
        source: "test",
        timeUnixMs: "1700000000000",
      }),
    ).rejects.toThrow("gRPC response field partition was not an integer");

    const malformedConsumeClient = createGrpcDataPlaneClient(
      "http://broker.local:19090",
      {
        protoPath: "/tmp/dataplane.proto",
        createRawClient() {
          return {
            consume(
              _request: unknown,
              _options: unknown,
              callback: (error: null, response: unknown) => void,
            ) {
              callback(null, { messages: {} });
            },
          };
        },
      },
    );
    await expect(
      malformedConsumeClient.consume({
        topic: "orders",
        consumerGroup: "workers",
        consumerId: "consumer-1",
        maxMessages: 1,
        leaseMs: "30000",
        nowUnixMs: "1700000000000",
      }),
    ).rejects.toThrow("gRPC response field messages was not an array");
  });
});

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
