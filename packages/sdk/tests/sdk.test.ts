import { describe, expect, it, vi } from "vitest";

import {
  encodePayload,
  FerrumQClient,
  FerrumQError,
  validateOptions,
} from "../src/index.js";

describe("validateOptions", () => {
  it("normalizes missing options", () => {
    expect(() =>
      validateOptions(
        undefined as unknown as Parameters<typeof validateOptions>[0],
      ),
    ).toThrow(
      expect.objectContaining({
        code: "SDK_CONFIGURATION",
      }),
    );
  });

  it("accepts valid options", () => {
    const result = validateOptions({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
    });
    expect(result.httpUrl).toBe("http://127.0.0.1:8080");
    expect(result.grpcUrl).toBe("http://127.0.0.1:9090");
    expect(result.timeoutMs).toBe(0);
  });

  it("accepts valid options with timeout", () => {
    const result = validateOptions({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      timeoutMs: 5000,
    });
    expect(result.timeoutMs).toBe(5000);
  });

  it("accepts https httpUrl", () => {
    const result = validateOptions({
      httpUrl: "https://example.com:8443",
      grpcUrl: "http://127.0.0.1:9090",
    });
    expect(result.httpUrl).toBe("https://example.com:8443");
  });

  it("rejects missing httpUrl", () => {
    expect(() =>
      validateOptions({
        httpUrl: "",
        grpcUrl: "http://127.0.0.1:9090",
      }),
    ).toThrow(FerrumQError);
  });

  it("rejects invalid httpUrl", () => {
    expect(() =>
      validateOptions({
        httpUrl: "not-a-url",
        grpcUrl: "http://127.0.0.1:9090",
      }),
    ).toThrow(FerrumQError);
  });

  it("rejects httpUrl with credentials", () => {
    expect(() =>
      validateOptions({
        httpUrl: "http://user:pass@127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
      }),
    ).toThrow(FerrumQError);
  });

  it("rejects ftp httpUrl protocol", () => {
    expect(() =>
      validateOptions({
        httpUrl: "ftp://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
      }),
    ).toThrow(FerrumQError);
  });

  it("rejects missing grpcUrl", () => {
    expect(() =>
      validateOptions({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "",
      }),
    ).toThrow(FerrumQError);
  });

  it("rejects invalid grpcUrl", () => {
    expect(() =>
      validateOptions({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "not-a-url",
      }),
    ).toThrow(FerrumQError);
  });

  it("rejects grpcUrl without port", () => {
    expect(() =>
      validateOptions({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1",
      }),
    ).toThrow(FerrumQError);
  });

  it("rejects https grpcUrl", () => {
    expect(() =>
      validateOptions({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "https://127.0.0.1:9090",
      }),
    ).toThrow(FerrumQError);
  });

  it("rejects grpcUrl with credentials", () => {
    expect(() =>
      validateOptions({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://user:pass@127.0.0.1:9090",
      }),
    ).toThrow(FerrumQError);
  });

  it("accepts timeoutMs 0 as no timeout", () => {
    const result = validateOptions({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      timeoutMs: 0,
    });
    expect(result.timeoutMs).toBe(0);
  });

  it("rejects negative timeoutMs", () => {
    expect(() =>
      validateOptions({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        timeoutMs: -1,
      }),
    ).toThrow(FerrumQError);
  });

  it("rejects non-integer timeoutMs", () => {
    expect(() =>
      validateOptions({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        timeoutMs: 1.5,
      }),
    ).toThrow(FerrumQError);
  });

  it.each([
    Number.NaN,
    Number.POSITIVE_INFINITY,
    2_147_483_648,
  ])("rejects unsafe timeoutMs %s", (timeoutMs) => {
    expect(() =>
      validateOptions({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        timeoutMs,
      }),
    ).toThrow(
      expect.objectContaining({
        code: "SDK_CONFIGURATION",
      }),
    );
  });
});

describe("encodePayload", () => {
  it("encodes strings as UTF-8 text", () => {
    const result = encodePayload("hello");
    expect(result.contentType).toBe("text/plain");
    expect(new TextDecoder().decode(result.data)).toBe("hello");
  });

  it("encodes empty strings", () => {
    const result = encodePayload("");
    expect(result.contentType).toBe("text/plain");
    expect(result.data.length).toBe(0);
  });

  it("encodes unicode strings", () => {
    const result = encodePayload("héllo 🌍");
    expect(result.contentType).toBe("text/plain");
    expect(new TextDecoder().decode(result.data)).toBe("héllo 🌍");
  });

  it("copies Uint8Array payloads", () => {
    const data = new Uint8Array([0x01, 0x02, 0x03]);
    const result = encodePayload(data);
    expect(result.contentType).toBe("application/octet-stream");
    expect(result.data).toStrictEqual(data);
    expect(result.data).not.toBe(data);
    data[0] = 0xff;
    expect(result.data[0]).toBe(0x01);
  });

  it("copies Buffer payloads as Uint8Array", () => {
    const data = Buffer.from([0x01, 0x02, 0x03]);
    const result = encodePayload(data);
    expect(result.contentType).toBe("application/octet-stream");
    expect(result.data).toStrictEqual(new Uint8Array([0x01, 0x02, 0x03]));
    expect(result.data).not.toBe(data);
  });

  it("encodes plain objects as JSON", () => {
    const result = encodePayload({ orderId: 1, status: "created" });
    expect(result.contentType).toBe("application/json");
    expect(new TextDecoder().decode(result.data)).toBe(
      '{"orderId":1,"status":"created"}',
    );
  });

  it("rejects objects whose top-level toJSON returns undefined", () => {
    expect.assertions(3);

    try {
      encodePayload({ toJSON: () => undefined });
    } catch (error) {
      expect(error).toBeInstanceOf(FerrumQError);
      expect((error as FerrumQError).transport).toBe("sdk");
      expect((error as FerrumQError).message).toContain("returned undefined");
    }
  });

  it("encodes arrays as JSON", () => {
    const result = encodePayload([1, 2, 3]);
    expect(result.contentType).toBe("application/json");
    expect(new TextDecoder().decode(result.data)).toBe("[1,2,3]");
  });

  it("encodes null as JSON", () => {
    const result = encodePayload(null);
    expect(result.contentType).toBe("application/json");
    expect(new TextDecoder().decode(result.data)).toBe("null");
  });

  it("encodes booleans as JSON", () => {
    const result = encodePayload(true);
    expect(result.contentType).toBe("application/json");
    expect(new TextDecoder().decode(result.data)).toBe("true");
  });

  it("encodes numbers as JSON", () => {
    const result = encodePayload(42);
    expect(result.contentType).toBe("application/json");
    expect(new TextDecoder().decode(result.data)).toBe("42");
  });

  it("rejects functions", () => {
    expect(() => encodePayload(() => {})).toThrow(FerrumQError);
  });

  it("rejects symbols", () => {
    expect(() => encodePayload(Symbol("test"))).toThrow(FerrumQError);
  });

  it("rejects undefined", () => {
    expect(() => encodePayload(undefined)).toThrow(FerrumQError);
  });

  it("wraps nested serialization failures as SDK errors", () => {
    expect.assertions(3);

    try {
      encodePayload({ nested: { value: 1n } });
    } catch (error) {
      expect(error).toBeInstanceOf(FerrumQError);
      expect((error as FerrumQError).transport).toBe("sdk");
      expect((error as FerrumQError).cause).toBeInstanceOf(TypeError);
    }
  });
});

describe("FerrumQError", () => {
  it("creates SDK transport errors", () => {
    const error = new FerrumQError("test error", { transport: "sdk" });
    expect(error.name).toBe("FerrumQError");
    expect(error.message).toBe("test error");
    expect(error.transport).toBe("sdk");
    expect(error.code).toBeUndefined();
    expect(error.status).toBeUndefined();
  });

  it("creates HTTP transport errors with status and code", () => {
    const error = new FerrumQError("not found", {
      transport: "http",
      status: 404,
      code: "TOPIC_NOT_FOUND",
    });
    expect(error.transport).toBe("http");
    expect(error.status).toBe(404);
    expect(error.code).toBe("TOPIC_NOT_FOUND");
  });

  it("creates gRPC transport errors with code", () => {
    const error = new FerrumQError("not found", {
      transport: "grpc",
      code: "NOT_FOUND",
    });
    expect(error.transport).toBe("grpc");
    expect(error.code).toBe("NOT_FOUND");
  });

  it("preserves cause", () => {
    const cause = new Error("original");
    const error = new FerrumQError("wrapped", { transport: "sdk", cause });
    expect(error.cause).toBe(cause);
  });
});

describe("FerrumQClient configuration", () => {
  it.each([
    "http://127.0.0.1:9090",
    "http://localhost:9090",
    "http://broker.local:19090",
  ])("creates client with valid gRPC URL %s", (grpcUrl) => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl,
    });
    expect(client).toBeInstanceOf(FerrumQClient);
    client.close();
  });

  it("throws on invalid httpUrl", () => {
    expect(
      () =>
        new FerrumQClient({
          httpUrl: "",
          grpcUrl: "http://127.0.0.1:9090",
        }),
    ).toThrow(FerrumQError);
  });

  it.each([
    ["missing", ""],
    ["bare target", "127.0.0.1:9090"],
    ["HTTPS", "https://127.0.0.1:9090"],
    ["path", "http://127.0.0.1:9090/api"],
    ["query", "http://127.0.0.1:9090?key=value"],
    ["fragment", "http://127.0.0.1:9090#section"],
    ["credentials", "http://user:pass@127.0.0.1:9090"],
    ["missing port", "http://127.0.0.1"],
    ["malformed URL", "http://:9090"],
    ["unsupported scheme", "grpc://127.0.0.1:9090"],
  ])("throws on %s grpcUrl during construction", (_case, grpcUrl) => {
    expect.assertions(2);

    try {
      new FerrumQClient({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl,
      });
    } catch (error) {
      expect(error).toBeInstanceOf(FerrumQError);
      expect((error as FerrumQError).transport).toBe("sdk");
    }
  });
});

describe("FerrumQClient close", () => {
  it("is idempotent", () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
    });
    client.close();
    client.close();
    client.close();
    expect(() => client.close()).not.toThrow();
  });

  it("prevents operations after close", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
    });
    client.close();

    await expect(client.health()).rejects.toThrow(FerrumQError);
  });
});

describe("FerrumQClient HTTP methods", () => {
  function mockFetchResponse(body: unknown, status = 200) {
    return Promise.resolve({
      ok: status >= 200 && status < 300,
      status,
      statusText: status === 200 ? "OK" : "Error",
      json: () => Promise.resolve(body),
      text: () => Promise.resolve(JSON.stringify(body)),
    });
  }

  it("health returns ok", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      fetchImpl: vi
        .fn()
        .mockReturnValue(
          mockFetchResponse({ status: "ok" }),
        ) as unknown as typeof fetch,
    });

    const result = await client.health();
    expect(result).toEqual({ status: "ok" });
    client.close();
  });

  it("status returns broker status", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      fetchImpl: vi.fn().mockReturnValue(
        mockFetchResponse({
          mode: "local-durable",
          dataDir: "./.ferrumq",
          topics: 2,
          dlqEntries: 1,
        }),
      ) as unknown as typeof fetch,
    });

    const result = await client.status();
    expect(result.mode).toBe("local-durable");
    expect(result.topics).toBe(2);
    client.close();
  });

  it("createTopic returns topic", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      fetchImpl: vi
        .fn()
        .mockReturnValue(
          mockFetchResponse({ name: "orders", partitions: 3 }),
        ) as unknown as typeof fetch,
    });

    const result = await client.createTopic({ name: "orders", partitions: 3 });
    expect(result.name).toBe("orders");
    expect(result.partitions).toBe(3);
    client.close();
  });

  it("listTopics returns items", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      fetchImpl: vi.fn().mockReturnValue(
        mockFetchResponse({
          items: [
            { name: "orders", partitions: 3 },
            { name: "payments", partitions: 1 },
          ],
        }),
      ) as unknown as typeof fetch,
    });

    const result = await client.listTopics();
    expect(result).toHaveLength(2);
    expect(result[0]?.name).toBe("orders");
    client.close();
  });

  it("getTopic returns topic", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      fetchImpl: vi
        .fn()
        .mockReturnValue(
          mockFetchResponse({ name: "orders", partitions: 3 }),
        ) as unknown as typeof fetch,
    });

    const result = await client.getTopic("orders");
    expect(result.name).toBe("orders");
    client.close();
  });

  it("handles HTTP error envelope", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      fetchImpl: vi.fn().mockReturnValue(
        mockFetchResponse(
          {
            error: {
              code: "TOPIC_NOT_FOUND",
              message: "topic not found",
              details: {},
              statusCode: 404,
            },
          },
          404,
        ),
      ) as unknown as typeof fetch,
    });

    await expect(client.getTopic("missing")).rejects.toThrow(FerrumQError);

    try {
      await client.getTopic("missing");
    } catch (error) {
      expect(error).toBeInstanceOf(FerrumQError);
      const fe = error as FerrumQError;
      expect(fe.transport).toBe("http");
    }

    client.close();
  });

  it("handles network error", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      fetchImpl: vi
        .fn()
        .mockRejectedValue(
          new Error("ECONNREFUSED"),
        ) as unknown as typeof fetch,
    });

    await expect(client.health()).rejects.toThrow(FerrumQError);

    try {
      await client.health();
    } catch (error) {
      expect(error).toBeInstanceOf(FerrumQError);
      const fe = error as FerrumQError;
      expect(fe.transport).toBe("http");
    }

    client.close();
  });

  it("normalizes metrics network failures", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      fetchImpl: vi
        .fn()
        .mockRejectedValue(
          new TypeError("connection refused"),
        ) as unknown as typeof fetch,
    });

    await expect(client.metrics()).rejects.toMatchObject({
      operation: "metrics",
      transport: "http",
    });
    await expect(client.metrics()).rejects.toBeInstanceOf(FerrumQError);
    client.close();
  });

  it("timeout rejects with SDK error", async () => {
    let observedSignal: AbortSignal | undefined;
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      timeoutMs: 50,
      fetchImpl: vi.fn().mockImplementation((_input, init) => {
        observedSignal = init?.signal;
        return new Promise((resolve) =>
          setTimeout(
            () =>
              resolve({
                ok: true,
                status: 200,
                statusText: "OK",
                json: () => Promise.resolve({ status: "ok" }),
                text: () => Promise.resolve(""),
              }),
            200,
          ),
        );
      }) as unknown as typeof fetch,
    });

    await expect(client.health()).rejects.toThrow(FerrumQError);

    try {
      await client.health();
    } catch (error) {
      expect(error).toBeInstanceOf(FerrumQError);
      const fe = error as FerrumQError;
      expect(fe.transport).toBe("sdk");
      expect(fe.code).toBe("SDK_TIMEOUT");
      expect(fe.operation).toBe("health");
      expect(fe.message).toContain("timed out");
    }
    expect(observedSignal?.aborted).toBe(true);

    client.close();
  });

  it("close aborts in-flight HTTP work with a stable closed error", async () => {
    let observedSignal: AbortSignal | undefined;
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      fetchImpl: vi.fn().mockImplementation((_input, init) => {
        observedSignal = init?.signal;
        return new Promise(() => {});
      }) as unknown as typeof fetch,
    });

    const pending = client.health();
    await Promise.resolve();
    client.close();

    await expect(pending).rejects.toMatchObject({
      code: "SDK_CLIENT_CLOSED",
      operation: "health",
      transport: "sdk",
    });
    expect(observedSignal?.aborted).toBe(true);
  });
});

describe("FerrumQClient publish error wrapping", () => {
  it("preserves existing FerrumQError from serialization without double-wrapping", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
    });

    try {
      await client.publish({
        topic: "orders",
        payload: Symbol("test"),
      });
    } catch (error) {
      expect(error).toBeInstanceOf(FerrumQError);
      const fe = error as FerrumQError;
      expect(fe.transport).toBe("sdk");
      expect(fe.code).toBe("SDK_SERIALIZATION");
      expect(fe.cause).toBeUndefined();
    }

    client.close();
  });

  it("preserves serialization errors with their original cause chain", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
    });

    try {
      await client.publish({
        topic: "orders",
        payload: { nested: { value: 1n } },
      });
    } catch (error) {
      expect(error).toBeInstanceOf(FerrumQError);
      const fe = error as FerrumQError;
      expect(fe.transport).toBe("sdk");
      expect(fe.code).toBe("SDK_SERIALIZATION");
      expect(fe.cause).toBeInstanceOf(TypeError);
    }

    client.close();
  });
});

describe("FerrumQClient consumed payload", () => {
  it("returns Uint8Array payloads that are independent copies", async () => {
    const originalPayload = Buffer.from([0x01, 0x02, 0x03]);
    let capturedPayload: Buffer | undefined;
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      grpcClientOptions: {
        protoPath: "/tmp/dataplane.proto",
        createRawClient() {
          return {
            close: vi.fn(),
            consume(
              _request: unknown,
              _options: unknown,
              callback: (error: null, response: unknown) => void,
            ) {
              capturedPayload = Buffer.from(originalPayload);
              callback(null, {
                messages: [
                  {
                    deliveryId: "del-1",
                    topic: "orders",
                    partition: 0,
                    offset: "1",
                    messageId: "msg-1",
                    key: "key-1",
                    payload: capturedPayload,
                    contentType: "application/octet-stream",
                    type: "test",
                    source: "test",
                    subject: "",
                    idempotencyKey: "",
                    timeUnixMs: "1700000000000",
                    consumerGroup: "workers",
                    consumerId: "worker-1",
                    attemptNumber: 1,
                    deliveredAtUnixMs: "1700000000000",
                    leaseExpiresAtUnixMs: "1700000030000",
                  },
                ],
              });
            },
          };
        },
      },
    });

    const deliveries = await client.consume({
      topic: "orders",
      group: "workers",
    });
    expect(deliveries).toHaveLength(1);
    const [delivery] = deliveries;
    expect(delivery).toBeDefined();
    if (!delivery) throw new Error("unexpected empty delivery");
    const payload = delivery.payload;
    expect(payload).toBeInstanceOf(Uint8Array);
    expect(Buffer.isBuffer(payload)).toBe(false);
    expect(payload).toStrictEqual(new Uint8Array([0x01, 0x02, 0x03]));

    originalPayload[0] = 0xff;
    expect(payload[0]).toBe(0x01);

    if (capturedPayload) {
      capturedPayload[0] = 0xff;
    }
    expect(payload[0]).toBe(0x01);

    client.close();
  });
});

describe("FerrumQClient gRPC close", () => {
  it("close cancels in-flight publish", async () => {
    const cancel = vi.fn();
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      grpcClientOptions: {
        protoPath: "/tmp/dataplane.proto",
        createRawClient() {
          return {
            close: vi.fn(),
            publish(_request: unknown, _options: unknown, _callback: unknown) {
              return { cancel };
            },
          };
        },
      },
    });

    const pending = client.publish({
      topic: "orders",
      payload: "hello",
    });
    await Promise.resolve();
    client.close();

    await expect(pending).rejects.toMatchObject({
      code: "SDK_CLIENT_CLOSED",
      transport: "sdk",
    });
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it("close cancels in-flight consume", async () => {
    const cancel = vi.fn();
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      grpcClientOptions: {
        protoPath: "/tmp/dataplane.proto",
        createRawClient() {
          return {
            close: vi.fn(),
            consume(_request: unknown, _options: unknown, _callback: unknown) {
              return { cancel };
            },
          };
        },
      },
    });

    const pending = client.consume({ topic: "orders", group: "workers" });
    await Promise.resolve();
    client.close();

    await expect(pending).rejects.toMatchObject({
      code: "SDK_CLIENT_CLOSED",
      transport: "sdk",
    });
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it("close cancels in-flight ack", async () => {
    const cancel = vi.fn();
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      grpcClientOptions: {
        protoPath: "/tmp/dataplane.proto",
        createRawClient() {
          return {
            close: vi.fn(),
            ack(_request: unknown, _options: unknown, _callback: unknown) {
              return { cancel };
            },
          };
        },
      },
    });

    const pending = client.ack({ deliveryId: "del-1" });
    await Promise.resolve();
    client.close();

    await expect(pending).rejects.toMatchObject({
      code: "SDK_CLIENT_CLOSED",
      transport: "sdk",
    });
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it("close cancels in-flight nack", async () => {
    const cancel = vi.fn();
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      grpcClientOptions: {
        protoPath: "/tmp/dataplane.proto",
        createRawClient() {
          return {
            close: vi.fn(),
            nack(_request: unknown, _options: unknown, _callback: unknown) {
              return { cancel };
            },
          };
        },
      },
    });

    const pending = client.nack({ deliveryId: "del-1" });
    await Promise.resolve();
    client.close();

    await expect(pending).rejects.toMatchObject({
      code: "SDK_CLIENT_CLOSED",
      transport: "sdk",
    });
    expect(cancel).toHaveBeenCalledTimes(1);
  });
});

describe("public exports", () => {
  it("exports FerrumQClient", () => {
    expect(FerrumQClient).toBeDefined();
  });

  it("exports FerrumQError", () => {
    expect(FerrumQError).toBeDefined();
  });

  it("exports encodePayload", () => {
    expect(encodePayload).toBeDefined();
  });

  it("exports validateOptions", () => {
    expect(validateOptions).toBeDefined();
  });
});
