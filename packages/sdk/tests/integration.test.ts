import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";

import { expect, test } from "vitest";

const repoRoot = path.resolve(import.meta.dirname, "../../..");
const brokerPath = path.join(
  repoRoot,
  "target",
  "debug",
  process.platform === "win32" ? "brokerd.exe" : "brokerd",
);

test("package entry works against brokerd serve-all", async (context) => {
  let process: ChildProcess | undefined;
  const dataDir = mkdtempSync(path.join(tmpdir(), "ferrumq-sdk-integration-"));
  let output = "";

  try {
    if (!existsSync(brokerPath)) {
      throw new Error(
        `${brokerPath} is missing; the SDK test preparation step did not build it`,
      );
    }

    const [httpPort, grpcPort] = await reservePorts();
    process = spawn(
      brokerPath,
      [
        "serve-all",
        "--data-dir",
        dataDir,
        "--http-listen",
        `127.0.0.1:${httpPort}`,
        "--grpc-listen",
        `127.0.0.1:${grpcPort}`,
      ],
      { cwd: repoRoot, stdio: ["ignore", "pipe", "pipe"] },
    );
    process.stdout?.on("data", (chunk) => {
      output += chunk.toString();
    });
    process.stderr?.on("data", (chunk) => {
      output += chunk.toString();
    });

    const httpUrl = `http://127.0.0.1:${httpPort}`;
    await waitForReady(httpUrl, process, () => output);
    const sdk = await import("../dist/index.js");
    const client = new sdk.FerrumQClient({
      httpUrl,
      grpcUrl: `http://127.0.0.1:${grpcPort}`,
      timeoutMs: 5_000,
    });
    const runnerPid = process.pid;
    const topic = `sdk-integration-${runnerPid}-${Date.now()}`;

    try {
      await expect(client.health()).resolves.toEqual({ status: "ok" });
      await expect(client.readiness()).resolves.toEqual({ status: "ready" });
      await expect(
        client.createTopic({ name: topic, partitions: 1 }),
      ).resolves.toEqual({ name: topic, partitions: 1 });

      const published = await client.publish({
        topic,
        payload: { event: "created", sequence: 1 },
      });
      const deliveries = await client.consume({
        topic,
        group: "sdk-integration",
        consumerId: "sdk-integration-worker",
      });
      expect(deliveries).toHaveLength(1);
      expect(
        JSON.parse(new TextDecoder().decode(deliveries[0]?.payload)),
      ).toEqual({ event: "created", sequence: 1 });
      expect(deliveries[0]?.messageId).toBe(published.messageId);

      await client.ack({
        deliveryId: deliveries[0]?.deliveryId ?? "",
        consumerId: "sdk-integration-worker",
      });
      await expect(client.status()).resolves.toMatchObject({ topics: 1 });

      const metrics = await client.metrics();
      expect(metrics).toContain(
        'ferrumq_control_topics_created_total{status="success"} 1',
      );
      expect(metrics).toContain(
        'ferrumq_data_publishes_total{status="success"} 1',
      );
      expect(metrics).toContain("ferrumq_data_messages_delivered_total 1");
      expect(metrics).toContain('ferrumq_data_acks_total{status="success"} 1');
    } finally {
      client.close();
      client.close();
    }
  } catch (error) {
    if (isLoopbackPermissionError(error, output)) {
      context.skip();
      return;
    }
    throw new Error(
      `SDK integration failed: ${errorMessage(error)}\n${output}`,
      { cause: error },
    );
  } finally {
    await stopProcess(process);
    rmSync(dataDir, { recursive: true, force: true });
  }
});

async function reservePorts(): Promise<[number, number]> {
  return [await reservePort(), await reservePort()];
}

function reservePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close();
        reject(new Error("failed to reserve loopback port"));
        return;
      }
      server.close((error) => {
        if (error !== undefined) {
          reject(error);
          return;
        }
        resolve(address.port);
      });
    });
  });
}

async function waitForReady(
  httpUrl: string,
  process: ChildProcess,
  readOutput: () => string,
): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (process.exitCode !== null) {
      throw new Error(
        `brokerd exited with code ${process.exitCode}\n${readOutput()}`,
      );
    }
    try {
      const response = await fetch(`${httpUrl}/ready`);
      if (response.ok) {
        return;
      }
    } catch {
      // Startup polling is bounded by the deadline below.
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`brokerd readiness timed out\n${readOutput()}`);
}

async function stopProcess(process: ChildProcess | undefined): Promise<void> {
  if (process === undefined || process.exitCode !== null) {
    return;
  }
  process.kill("SIGTERM");
  if (await waitForExit(process, 2_000)) {
    return;
  }
  process.kill("SIGKILL");
  await waitForExit(process, 2_000);
}

function waitForExit(
  process: ChildProcess,
  timeoutMs: number,
): Promise<boolean> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), timeoutMs);
    process.once("exit", () => {
      clearTimeout(timer);
      resolve(true);
    });
  });
}

function isLoopbackPermissionError(error: unknown, output: string): boolean {
  const text = `${errorMessage(error)}\n${output}`.toLowerCase();
  return (
    text.includes("eacces") ||
    text.includes("operation not permitted") ||
    text.includes("permission denied")
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
