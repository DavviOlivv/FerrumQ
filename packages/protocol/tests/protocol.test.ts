import { describe, expect, it } from "vitest";

import {
  createGrpcDataPlaneClient,
  ferrumQErrorEnvelopeSchema,
  httpStatusResponseSchema,
  normalizeGrpcTarget,
  topicListResponseSchema,
} from "../src/index.js";

describe("HTTP schemas", () => {
  it("parses success DTOs", () => {
    expect(httpStatusResponseSchema.parse({ status: "ok" })).toEqual({
      status: "ok",
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

describe("gRPC client helpers", () => {
  it("normalizes http://host:port URLs to grpc-js targets", () => {
    expect(normalizeGrpcTarget("http://127.0.0.1:9090")).toBe("127.0.0.1:9090");
    expect(normalizeGrpcTarget("http://broker.local:19090")).toBe(
      "broker.local:19090",
    );
  });

  it("rejects HTTPS because TLS is deferred", () => {
    expect(() => normalizeGrpcTarget("https://broker.local:9090")).toThrow(
      "TLS/HTTPS",
    );
  });

  it("uses injected proto-path and raw client factories", async () => {
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
});
