import { type ChildProcess, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
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

test("two chat clients send and receive messages in the same room", async (context) => {
  let process: ChildProcess | undefined;
  const dataDir = mkdtempSync(path.join(tmpdir(), "ferrumq-chat-integration-"));
  let output = "";

  try {
    if (!existsSync(brokerPath)) {
      throw new Error(`${brokerPath} is missing; build it first`);
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
    const grpcUrl = `http://127.0.0.1:${grpcPort}`;
    await waitForReady(httpUrl, process, () => output);

    const { FerrumQClient } = await import("@ferrumq/sdk");

    const room = `test-room-${randomUUID().slice(0, 8)}`;

    const aliceMessages: string[] = [];
    const bobMessages: string[] = [];

    const aliceClient = new FerrumQClient({
      httpUrl,
      grpcUrl,
      timeoutMs: 5_000,
    });

    const bobClient = new FerrumQClient({
      httpUrl,
      grpcUrl,
      timeoutMs: 5_000,
    });

    try {
      // Create the room topic (Alice creates, Bob may race)
      await aliceClient.createTopic({
        name: `chat.${room}`,
        partitions: 1,
      });

      // Both clients need unique consumer groups
      const aliceGroup = `chat.${room}.session.alice-${randomUUID().slice(0, 8)}`;
      const bobGroup = `chat.${room}.session.bob-${randomUUID().slice(0, 8)}`;

      // Alice publishes a message
      const chatPayload = JSON.stringify({
        version: 1,
        id: randomUUID(),
        room,
        sender: {
          id: "alice-id",
          name: "Alice",
          sessionId: aliceGroup,
        },
        text: "Hello from Alice",
        sentAt: new Date().toISOString(),
      });

      await aliceClient.publish({
        topic: `chat.${room}`,
        payload: chatPayload,
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      // Bob consumes - should see Alice's message
      const bobDeliveries1 = await bobClient.consume({
        topic: `chat.${room}`,
        group: bobGroup,
        consumerId: `consumer-${bobGroup}`,
        maxMessages: 5,
        leaseMs: 30_000,
      });

      expect(bobDeliveries1.length).toBeGreaterThanOrEqual(1);
      const bobSeen = new TextDecoder().decode(bobDeliveries1[0]?.payload);
      bobMessages.push(bobSeen);
      expect(bobSeen).toContain("Hello from Alice");

      // ACK Bob's delivery
      for (const delivery of bobDeliveries1) {
        await bobClient.ack({
          deliveryId: delivery.deliveryId,
          consumerId: `consumer-${bobGroup}`,
        });
      }

      // Alice also consumes - she should see her own message too (broker-confirmed)
      const aliceDeliveries1 = await aliceClient.consume({
        topic: `chat.${room}`,
        group: aliceGroup,
        consumerId: `consumer-${aliceGroup}`,
        maxMessages: 5,
        leaseMs: 30_000,
      });

      expect(aliceDeliveries1.length).toBeGreaterThanOrEqual(1);
      const aliceSeen = new TextDecoder().decode(aliceDeliveries1[0]?.payload);
      aliceMessages.push(aliceSeen);
      expect(aliceSeen).toContain("Hello from Alice");

      for (const delivery of aliceDeliveries1) {
        await aliceClient.ack({
          deliveryId: delivery.deliveryId,
          consumerId: `consumer-${aliceGroup}`,
        });
      }

      // Bob publishes a reply
      const bobPayload = JSON.stringify({
        version: 1,
        id: randomUUID(),
        room,
        sender: {
          id: "bob-id",
          name: "Bob",
          sessionId: bobGroup,
        },
        text: "Hello from Bob",
        sentAt: new Date().toISOString(),
      });

      await bobClient.publish({
        topic: `chat.${room}`,
        payload: bobPayload,
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      // Both should see Bob's message
      const aliceDeliveries2 = await aliceClient.consume({
        topic: `chat.${room}`,
        group: aliceGroup,
        consumerId: `consumer-${aliceGroup}`,
        maxMessages: 5,
        leaseMs: 30_000,
      });
      expect(aliceDeliveries2.length).toBeGreaterThanOrEqual(1);

      const bobDeliveries2 = await bobClient.consume({
        topic: `chat.${room}`,
        group: bobGroup,
        consumerId: `consumer-${bobGroup}`,
        maxMessages: 5,
        leaseMs: 30_000,
      });
      expect(bobDeliveries2.length).toBeGreaterThanOrEqual(1);

      const aliceSawBob = new TextDecoder().decode(
        aliceDeliveries2[0]?.payload,
      );
      expect(aliceSawBob).toContain("Hello from Bob");

      const bobSawOwn = new TextDecoder().decode(bobDeliveries2[0]?.payload);
      expect(bobSawOwn).toContain("Hello from Bob");

      // ACK all deliveries
      for (const delivery of aliceDeliveries2) {
        await aliceClient.ack({
          deliveryId: delivery.deliveryId,
          consumerId: `consumer-${aliceGroup}`,
        });
      }
      for (const delivery of bobDeliveries2) {
        await bobClient.ack({
          deliveryId: delivery.deliveryId,
          consumerId: `consumer-${bobGroup}`,
        });
      }

      // Verify that after ACK, no more messages are pending for either
      const aliceEmpty = await aliceClient.consume({
        topic: `chat.${room}`,
        group: aliceGroup,
        consumerId: `consumer-${aliceGroup}`,
        maxMessages: 5,
        leaseMs: 5_000,
      });
      expect(aliceEmpty).toHaveLength(0);

      const bobEmpty = await bobClient.consume({
        topic: `chat.${room}`,
        group: bobGroup,
        consumerId: `consumer-${bobGroup}`,
        maxMessages: 5,
        leaseMs: 5_000,
      });
      expect(bobEmpty).toHaveLength(0);

      // Verify that messages are not delivered again (duplicate handling via ACK)
      // Since we ACKed, the broker should not redeliver
      const aliceCheckDup = await aliceClient.consume({
        topic: `chat.${room}`,
        group: aliceGroup,
        consumerId: `consumer-${aliceGroup}`,
        maxMessages: 5,
        leaseMs: 5_000,
      });
      expect(aliceCheckDup).toHaveLength(0);
    } finally {
      aliceClient.close();
      bobClient.close();
    }
  } catch (error) {
    if (isLoopbackPermissionError(error, output)) {
      context.skip();
      return;
    }
    throw new Error(
      `Chat integration failed: ${errorMessage(error)}\n${output}`,
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
      // bounded polling continues
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
