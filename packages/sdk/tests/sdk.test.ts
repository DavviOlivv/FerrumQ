import { describe, expect, it, vi } from "vitest";

import {
  encodePayload,
  FerrumQClient,
  FerrumQError,
  validateOptions,
} from "../src/index.js";

describe("validateOptions", () => {
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

  it("rejects non-positive timeoutMs", () => {
    expect(() =>
      validateOptions({
        httpUrl: "http://127.0.0.1:8080",
        grpcUrl: "http://127.0.0.1:9090",
        timeoutMs: 0,
      }),
    ).toThrow(FerrumQError);

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

  it("encodes Uint8Array as-is", () => {
    const data = new Uint8Array([0x01, 0x02, 0x03]);
    const result = encodePayload(data);
    expect(result.contentType).toBe("application/octet-stream");
    expect(result.data).toBe(data);
  });

  it("encodes Buffer as binary", () => {
    const data = Buffer.from([0x01, 0x02, 0x03]);
    const result = encodePayload(data);
    expect(result.contentType).toBe("application/octet-stream");
    expect(result.data).toBe(data);
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

  it("timeout rejects with SDK error", async () => {
    const client = new FerrumQClient({
      httpUrl: "http://127.0.0.1:8080",
      grpcUrl: "http://127.0.0.1:9090",
      timeoutMs: 50,
      fetchImpl: vi.fn().mockImplementation(
        () =>
          new Promise((resolve) =>
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
          ),
      ) as unknown as typeof fetch,
    });

    await expect(client.health()).rejects.toThrow(FerrumQError);

    try {
      await client.health();
    } catch (error) {
      expect(error).toBeInstanceOf(FerrumQError);
      const fe = error as FerrumQError;
      expect(fe.transport).toBe("sdk");
      expect(fe.message).toContain("timed out");
    }

    client.close();
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
