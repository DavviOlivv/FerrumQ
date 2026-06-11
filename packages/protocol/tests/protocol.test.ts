import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it } from "vitest";

import {
  brokerStatusResponseSchema,
  createGrpcDataPlaneClient,
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
            callback: (error: null, response: unknown) => void,
          ) {
            calls.push(request);
            callback(null, {
              topic: "orders",
              partition: 0,
              offset: "18446744073709551615",
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
        timeUnixMs: "1700000000000",
      }),
    ).resolves.toEqual({
      topic: "orders",
      partition: 0,
      offset: "18446744073709551615",
      messageId: "message-1",
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
            callback: (error: null, response: unknown) => void,
          ) {
            calls.push({ method: "ack", request });
            callback(null, {});
          },
          nack(
            request: unknown,
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
