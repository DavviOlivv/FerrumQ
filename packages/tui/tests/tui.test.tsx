import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { createControlPlaneClient } from "@ferrumq/protocol";
import { render } from "ink-testing-library";
import { describe, expect, it, vi } from "vitest";

import {
  DashboardView,
  DlqView,
  FerrumQTui,
  HelpView,
  TopicsView,
  defaultControlUrl,
  defaultGrpcUrl,
  loadTuiSnapshot,
  parseTuiArgs,
  resolveTuiConfig,
  runTuiCli,
  tuiVersion,
  type TuiConfig,
  type TuiSnapshot,
} from "../src/index.js";

import type {
  ControlPlaneClient,
  FetchLike,
  ResponseLike,
} from "@ferrumq/protocol";

const packageRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const distCliPath = path.join(packageRoot, "dist/cli.js");
const describeBuiltCli = existsSync(distCliPath) ? describe : describe.skip;
let builtCliImportCounter = 0;

const config: TuiConfig = {
  controlUrl: "http://control.local:8080",
  grpcUrl: "http://grpc.local:9090",
};

describe("TUI config", () => {
  it("uses defaults, environment overrides, and flag overrides", () => {
    expect(resolveTuiConfig({}, {})).toEqual({
      controlUrl: defaultControlUrl,
      grpcUrl: defaultGrpcUrl,
    });
    expect(
      resolveTuiConfig(
        {},
        {
          FERRUMQ_CONTROL_URL: "https://env.local:8443",
          FERRUMQ_GRPC_URL: "http://env.local:9090",
        },
      ),
    ).toEqual({
      controlUrl: "https://env.local:8443",
      grpcUrl: "http://env.local:9090",
    });
    expect(
      resolveTuiConfig(
        {
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
    });
  });

  it("parses local flags without reading process env", () => {
    expect(
      parseTuiArgs([
        "--control-url",
        "http://control.local:8080",
        "--grpc-url=http://grpc.local:9090",
      ]),
    ).toEqual({
      help: false,
      version: false,
      controlUrl: "http://control.local:8080",
      grpcUrl: "http://grpc.local:9090",
    });
    expect(parseTuiArgs(["--help"])).toEqual({
      help: true,
      version: false,
    });
    expect(parseTuiArgs(["--version"])).toEqual({
      help: false,
      version: true,
    });
  });

  it("rejects invalid URLs", () => {
    expect(() => resolveTuiConfig({ controlUrl: "not-a-url" }, {})).toThrow(
      "control URL must be a valid URL",
    );
    expect(() =>
      resolveTuiConfig({ controlUrl: "ftp://broker.local" }, {}),
    ).toThrow("control URL must use http:// or https://");
    expect(() =>
      resolveTuiConfig({ controlUrl: "http://user@broker.local" }, {}),
    ).toThrow("control URL must not include credentials");
    expect(() =>
      resolveTuiConfig({ grpcUrl: "https://broker.local:9090" }, {}),
    ).toThrow("gRPC URL TLS/HTTPS is deferred; use http://host:port");
    expect(() =>
      resolveTuiConfig({ grpcUrl: "http://broker.local" }, {}),
    ).toThrow("gRPC URL must include a port");
  });
});

describe("TUI loader", () => {
  it("loads health, readiness, status, topics, and DLQ concurrently", async () => {
    const calls: string[] = [];
    const client: ControlPlaneClient = {
      async health() {
        calls.push("health");
        return { status: "ok" };
      },
      async ready() {
        calls.push("ready");
        return { status: "ready" };
      },
      async status() {
        calls.push("status");
        return {
          mode: "durable",
          dataDir: "./.ferrumq",
          topics: 2,
          dlqEntries: 1,
        };
      },
      async createTopic() {
        throw new Error("unexpected createTopic call");
      },
      async getTopic() {
        throw new Error("unexpected getTopic call");
      },
      async listTopics() {
        calls.push("topics");
        return {
          items: [
            { name: "orders", partitions: 3 },
            { name: "payments", partitions: 1 },
          ],
        };
      },
      async listDlq() {
        calls.push("dlq");
        return { items: [sampleDlqEntry()] };
      },
    };

    await expect(
      loadTuiSnapshot(config, {
        client,
        now: () => new Date("2026-06-11T12:00:00.000Z"),
      }),
    ).resolves.toEqual(sampleSnapshot());
    expect(calls.sort()).toEqual([
      "dlq",
      "health",
      "ready",
      "status",
      "topics",
    ]);
  });

  it("formats expected refresh failures without stack traces", async () => {
    for (const [pathSuffix, responseLike, expected] of [
      [
        "/v1/status",
        response(409, {
          error: {
            code: "CONFLICT",
            message: "request failed",
            details: {},
            statusCode: 409,
          },
        }),
        "HTTP 409 CONFLICT: request failed",
      ],
      ["/ready", response(418, { nope: true }, "Teapot"), "HTTP 418: Teapot"],
      [
        "/v1/topics",
        invalidJsonResponse(200),
        "Unexpected response from control API: invalid JSON",
      ],
      [
        "/health",
        response(200, { status: "" }),
        "Unexpected response from control API",
      ],
    ] as const) {
      const client = createControlPlaneClient(
        config.controlUrl,
        successFetchWith(pathSuffix, responseLike),
      );

      await expect(loadTuiSnapshot(config, { client })).rejects.toThrow(
        expected,
      );
      await expect(loadTuiSnapshot(config, { client })).rejects.not.toThrow(
        "Error:",
      );
    }

    const networkClient = createControlPlaneClient(
      config.controlUrl,
      vi.fn<FetchLike>(async (input) => {
        if (input.endsWith("/ready")) {
          throw new TypeError("connection refused");
        }
        return successResponseFor(input);
      }),
    );
    await expect(
      loadTuiSnapshot(config, { client: networkClient }),
    ).rejects.toThrow(
      "Network request failed for GET http://control.local:8080/ready: connection refused",
    );
  });
});

describe("TUI rendering", () => {
  it("renders dashboard status and counts", () => {
    const view = render(
      <DashboardView
        config={config}
        state={{
          activeView: "dashboard",
          snapshot: sampleSnapshot(),
          loading: false,
          error: undefined,
          refreshCount: 1,
          lastRefreshAt: sampleSnapshot().refreshedAt,
        }}
      />,
    );

    expect(view.lastFrame()).toContain(
      "control URL: http://control.local:8080",
    );
    expect(view.lastFrame()).toContain("gRPC URL: http://grpc.local:9090");
    expect(view.lastFrame()).toContain("health: ok");
    expect(view.lastFrame()).toContain("readiness: ready");
    expect(view.lastFrame()).toContain("broker mode: durable");
    expect(view.lastFrame()).toContain("topic count: 2");
    expect(view.lastFrame()).toContain("DLQ count: 1");
    expect(view.lastFrame()).toContain("last error: none");
    view.unmount();

    const errorView = render(
      <DashboardView
        config={config}
        state={{
          activeView: "dashboard",
          snapshot: undefined,
          loading: false,
          error: "control API refresh failed: status exploded",
          refreshCount: 1,
          lastRefreshAt: undefined,
        }}
      />,
    );
    expect(errorView.lastFrame()).toContain(
      "last error: control API refresh failed: status exploded",
    );
    errorView.unmount();
  });

  it("renders topics in API order and empty state", () => {
    const topics = render(
      <TopicsView
        topics={[
          { name: "orders", partitions: 3 },
          { name: "payments", partitions: 1 },
        ]}
      />,
    );
    expect(topics.lastFrame()).toContain("orders partitions=3");
    expect(topics.lastFrame()).toContain("payments partitions=1");
    topics.unmount();

    const empty = render(<TopicsView topics={[]} />);
    expect(empty.lastFrame()).toContain("no topics");
    empty.unmount();
  });

  it("renders DLQ rows and empty state", () => {
    const dlq = render(<DlqView entries={[sampleDlqEntry()]} />);
    expect(dlq.lastFrame()).toContain("orders[0]@42");
    expect(dlq.lastFrame()).toContain("message=message-1");
    expect(dlq.lastFrame()).toContain("group=workers");
    expect(dlq.lastFrame()).toContain("reason=poison");
    expect(dlq.lastFrame()).toContain("attempts=3");
    dlq.unmount();

    const empty = render(<DlqView entries={[]} />);
    expect(empty.lastFrame()).toContain("no DLQ entries");
    empty.unmount();
  });

  it("renders help key bindings", () => {
    const view = render(<HelpView />);
    expect(view.lastFrame()).toContain("1 dashboard");
    expect(view.lastFrame()).toContain("2 topics");
    expect(view.lastFrame()).toContain("3 DLQ");
    expect(view.lastFrame()).toContain("? help");
    expect(view.lastFrame()).toContain("r refresh");
    expect(view.lastFrame()).toContain("q quit");
    view.unmount();
  });
});

describe("TUI interactions", () => {
  it("keeps Topics loading state distinct before the first snapshot", async () => {
    const view = render(
      <FerrumQTui config={config} dependencies={{ client: pendingClient() }} />,
    );

    view.stdin.write("2");
    await waitForFrame(view, "Loading broker snapshot...");
    expect(view.lastFrame()).not.toContain("no topics");
    view.unmount();
  });

  it("keeps DLQ loading state distinct before the first snapshot", async () => {
    const view = render(
      <FerrumQTui config={config} dependencies={{ client: pendingClient() }} />,
    );

    view.stdin.write("3");
    await waitForFrame(view, "Loading broker snapshot...");
    expect(view.lastFrame()).not.toContain("no DLQ entries");
    view.unmount();
  });

  it("renders initial refresh errors on Topics without the empty state", async () => {
    const view = render(
      <FerrumQTui
        config={config}
        dependencies={{ client: failingStartupClient() }}
      />,
    );

    view.stdin.write("2");
    await waitForFrame(view, "control API refresh failed: status exploded");
    expect(view.lastFrame()).not.toContain("no topics");
    view.unmount();
  });

  it("renders initial refresh errors on DLQ without the empty state", async () => {
    const view = render(
      <FerrumQTui
        config={config}
        dependencies={{ client: failingStartupClient() }}
      />,
    );

    view.stdin.write("3");
    await waitForFrame(view, "control API refresh failed: status exploded");
    expect(view.lastFrame()).not.toContain("no DLQ entries");
    view.unmount();
  });

  it("keeps previous topic rows visible when refresh fails", async () => {
    const view = render(
      <FerrumQTui
        config={config}
        dependencies={{
          client: snapshotSequenceClient([
            sampleSnapshot(),
            new Error("refresh exploded"),
          ]),
          now: () => new Date("2026-06-11T12:00:00.000Z"),
        }}
      />,
    );

    await waitForFrame(view, "refreshes=1");
    view.stdin.write("2");
    await waitForFrame(view, "orders partitions=3");
    view.stdin.write("r");
    await waitForFrame(view, "control API refresh failed: refresh exploded");
    expect(view.lastFrame()).toContain("orders partitions=3");
    view.unmount();
  });

  it("keeps previous DLQ rows visible when refresh fails", async () => {
    const view = render(
      <FerrumQTui
        config={config}
        dependencies={{
          client: snapshotSequenceClient([
            sampleSnapshot(),
            new Error("refresh exploded"),
          ]),
          now: () => new Date("2026-06-11T12:00:00.000Z"),
        }}
      />,
    );

    await waitForFrame(view, "refreshes=1");
    view.stdin.write("3");
    await waitForFrame(view, "orders[0]@42");
    view.stdin.write("r");
    await waitForFrame(view, "control API refresh failed: refresh exploded");
    expect(view.lastFrame()).toContain("orders[0]@42");
    view.unmount();
  });

  it("clears refresh errors after a later successful refresh", async () => {
    const view = render(
      <FerrumQTui
        config={config}
        dependencies={{
          client: snapshotSequenceClient([
            sampleSnapshot(),
            new Error("refresh exploded"),
            updatedSnapshot(),
          ]),
          now: () => new Date("2026-06-11T12:00:00.000Z"),
        }}
      />,
    );

    await waitForFrame(view, "refreshes=1");
    view.stdin.write("2");
    await waitForFrame(view, "orders partitions=3");
    view.stdin.write("r");
    await waitForFrame(view, "control API refresh failed: refresh exploded");
    view.stdin.write("r");
    await waitForFrame(view, "invoices partitions=5");
    expect(view.lastFrame()).not.toContain("refresh exploded");
    view.unmount();
  });

  it("handles refresh, view keys, help, and quit", async () => {
    const onExit = vi.fn();
    let healthCalls = 0;
    const client = sampleClient({
      async health() {
        healthCalls += 1;
        return { status: "ok" };
      },
    });
    const view = render(
      <FerrumQTui
        config={config}
        dependencies={{
          client,
          now: () => new Date("2026-06-11T12:00:00.000Z"),
        }}
        onExit={onExit}
      />,
    );

    await waitForFrame(view, "refreshes=1");
    view.stdin.write("2");
    await waitForFrame(view, "orders partitions=3");
    view.stdin.write("3");
    await waitForFrame(view, "orders[0]@42");
    view.stdin.write("?");
    await waitForFrame(view, "q quit");
    view.stdin.write("1");
    await waitForFrame(view, "broker mode: durable");

    view.stdin.write("r");
    await vi.waitFor(() => expect(healthCalls).toBe(2));
    await waitForFrame(view, "refreshes=2");

    view.stdin.write("q");
    await vi.waitFor(() => expect(onExit).toHaveBeenCalledTimes(1));
    view.unmount();
  });
});

describe("TUI CLI runner", () => {
  it("prints version and help without resolving invalid env", async () => {
    await expect(captureRun(["--version"])).resolves.toEqual({
      code: 0,
      stdout: [tuiVersion],
      stderr: [],
    });

    const help = await captureRun(["--help"], {
      env: {
        FERRUMQ_CONTROL_URL: "not-a-url",
        FERRUMQ_GRPC_URL: "https://broker.local:9090",
      },
    });
    expect(help.code).toBe(0);
    expect(help.stderr).toEqual([]);
    expect(help.stdout.join("\n")).toContain("ferrumq-tui");
  });
});

describeBuiltCli("built TUI CLI smoke", () => {
  it("prints help and version through dist/cli.js", async () => {
    const packageJson = JSON.parse(
      readFileSync(path.join(packageRoot, "package.json"), "utf8"),
    ) as { bin?: Record<string, string> };
    expect(packageJson.bin?.["ferrumq-tui"]).toBe("./dist/cli.js");

    await expect(runBuiltCli(["--version"])).resolves.toMatchObject({
      code: 0,
      stdout: tuiVersion,
      stderr: "",
    });

    const help = await runBuiltCli(["--help"]);
    expect(help).toMatchObject({ code: 0, stderr: "" });
    expect(help.stdout).toContain("FerrumQ TUI");
  });
});

async function captureRun(
  args: readonly string[],
  options: { env?: Record<string, string> } = {},
): Promise<{ code: number; stdout: string[]; stderr: string[] }> {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const code = await runTuiCli(
    args,
    {
      writeLine(message) {
        stdout.push(message);
      },
      writeError(message) {
        stderr.push(message);
      },
    },
    options,
  );

  return { code, stdout, stderr };
}

async function runBuiltCli(
  args: readonly string[],
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
    process.argv = [process.execPath, distCliPath, ...args];
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

function sampleSnapshot(): TuiSnapshot {
  return {
    health: { status: "ok" },
    readiness: { status: "ready" },
    status: {
      mode: "durable",
      dataDir: "./.ferrumq",
      topics: 2,
      dlqEntries: 1,
    },
    topics: {
      items: [
        { name: "orders", partitions: 3 },
        { name: "payments", partitions: 1 },
      ],
    },
    dlq: { items: [sampleDlqEntry()] },
    refreshedAt: new Date("2026-06-11T12:00:00.000Z"),
  };
}

function updatedSnapshot(): TuiSnapshot {
  const snapshot = sampleSnapshot();

  return {
    ...snapshot,
    status: {
      ...snapshot.status,
      topics: 1,
    },
    topics: {
      items: [{ name: "invoices", partitions: 5 }],
    },
    refreshedAt: new Date("2026-06-11T12:01:00.000Z"),
  };
}

function sampleDlqEntry() {
  return {
    topic: "orders",
    partition: 0,
    offset: 42,
    messageId: "message-1",
    consumerGroupId: "workers",
    reason: "poison",
    attemptCount: 3,
    timestamp: 1_700_000_000_000,
  };
}

function pendingClient(): ControlPlaneClient {
  const pending = new Promise<never>(() => {});

  return sampleClient({
    async health() {
      return pending;
    },
  });
}

function failingStartupClient(): ControlPlaneClient {
  return sampleClient({
    async status() {
      throw new Error("status exploded");
    },
  });
}

function snapshotSequenceClient(
  results: readonly [TuiSnapshot | Error, ...(TuiSnapshot | Error)[]],
): ControlPlaneClient {
  let attempt = -1;

  const currentSnapshot = (): TuiSnapshot => {
    const index = Math.min(Math.max(attempt, 0), results.length - 1);
    const result = results[index];
    if (result === undefined) {
      throw new Error("missing snapshot sequence result");
    }

    if (result instanceof Error) {
      throw result;
    }

    return result;
  };

  return sampleClient({
    async health() {
      attempt += 1;
      return currentSnapshot().health;
    },
    async ready() {
      return currentSnapshot().readiness;
    },
    async status() {
      return currentSnapshot().status;
    },
    async listTopics() {
      return currentSnapshot().topics;
    },
    async listDlq() {
      return currentSnapshot().dlq;
    },
  });
}

function sampleClient(
  overrides: Partial<ControlPlaneClient> = {},
): ControlPlaneClient {
  return {
    async health() {
      return { status: "ok" };
    },
    async ready() {
      return { status: "ready" };
    },
    async status() {
      return sampleSnapshot().status;
    },
    async createTopic() {
      throw new Error("unexpected createTopic call");
    },
    async getTopic() {
      throw new Error("unexpected getTopic call");
    },
    async listTopics() {
      return sampleSnapshot().topics;
    },
    async listDlq() {
      return sampleSnapshot().dlq;
    },
    ...overrides,
  };
}

function successFetchWith(
  pathSuffix: string,
  failure: ResponseLike,
): FetchLike {
  return vi.fn<FetchLike>(async (input) => {
    if (input.endsWith(pathSuffix)) {
      return failure;
    }

    return successResponseFor(input);
  });
}

function successResponseFor(input: string): ResponseLike {
  if (input.endsWith("/health")) {
    return response(200, { status: "ok" });
  }
  if (input.endsWith("/ready")) {
    return response(200, { status: "ready" });
  }
  if (input.endsWith("/v1/status")) {
    return response(200, sampleSnapshot().status);
  }
  if (input.endsWith("/v1/topics")) {
    return response(200, sampleSnapshot().topics);
  }
  if (input.endsWith("/v1/dlq")) {
    return response(200, sampleSnapshot().dlq);
  }

  throw new Error(`unexpected request ${input}`);
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

async function waitForFrame(
  view: ReturnType<typeof render>,
  expected: string,
): Promise<void> {
  await vi.waitFor(() => expect(view.lastFrame()).toContain(expected));
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
