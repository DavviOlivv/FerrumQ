import { type ChildProcess, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";

import { expect, test } from "vitest";

const repoRoot = path.resolve(import.meta.dirname, "../../..");
const brokerPath = path.join(repoRoot, "target/debug/brokerd");

interface BrokerFixture {
  httpUrl: string;
  grpcUrl: string;
  process: ChildProcess;
  dataDir: string;
  output: () => string;
}

async function startBroker(_context: {
  skip: () => void;
}): Promise<BrokerFixture> {
  if (!existsSync(brokerPath)) {
    throw new Error("target/debug/brokerd is missing; build it first");
  }

  const dataDir = mkdtempSync(path.join(tmpdir(), "ferrumq-chat-multi-"));
  let output = "";
  const [httpPort, grpcPort] = await reservePorts();

  const process = spawn(
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

  return {
    httpUrl,
    grpcUrl,
    process,
    dataDir,
    output: () => output,
  };
}

async function stopBroker(fixture: BrokerFixture): Promise<void> {
  await stopProcess(fixture.process);
  rmSync(fixture.dataDir, { recursive: true, force: true });
}

async function importClient() {
  const { FerrumQClient } = await import("@ferrumq/sdk");
  return FerrumQClient;
}

function chatPayload(
  room: string,
  senderId: string,
  senderName: string,
  sessionId: string,
  text: string,
): string {
  return JSON.stringify({
    version: 1,
    id: randomUUID(),
    room,
    sender: { id: senderId, name: senderName, sessionId },
    text,
    sentAt: new Date().toISOString(),
  });
}

const decoder = new TextDecoder();

test("three clients in the same room all see every message", async (context) => {
  let fixture: BrokerFixture | undefined;
  try {
    fixture = await startBroker(context);
    const FerrumQClient = await importClient();

    const room = `room-${randomUUID().slice(0, 8)}`;
    const topic = `chat.${room}`;

    const aClient = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });
    const aGroup = `chat.${room}.session.a-${randomUUID().slice(0, 8)}`;
    const bClient = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });
    const bGroup = `chat.${room}.session.b-${randomUUID().slice(0, 8)}`;
    const cClient = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });
    const cGroup = `chat.${room}.session.c-${randomUUID().slice(0, 8)}`;

    const clients = [
      { client: aClient, group: aGroup, name: "Alice" },
      { client: bClient, group: bGroup, name: "Bob" },
      { client: cClient, group: cGroup, name: "Carol" },
    ];

    try {
      await aClient.createTopic({ name: topic, partitions: 1 });

      await bClient.publish({
        topic,
        payload: chatPayload(room, "bob-id", "Bob", bGroup, "Message from Bob"),
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      const seen = await Promise.all(
        clients.map(async ({ client, group }) => {
          const deliveries = await client.consume({
            topic,
            group,
            consumerId: `consumer-${group}`,
            maxMessages: 5,
            leaseMs: 30_000,
          });
          for (const d of deliveries) {
            await client.ack({
              deliveryId: d.deliveryId,
              consumerId: `consumer-${group}`,
            });
          }
          return deliveries.map((d) => decoder.decode(d.payload));
        }),
      );

      for (const messages of seen) {
        expect(messages.some((m) => m.includes("Message from Bob"))).toBe(true);
      }
    } finally {
      for (const { client } of clients) {
        client.close();
      }
    }
  } catch (error) {
    if (isLoopbackPermissionError(error, fixture?.output() ?? "")) {
      context.skip();
      return;
    }
    throw new Error(
      `Three-client test failed: ${errorMessage(error)}\n${fixture?.output() ?? ""}`,
      { cause: error },
    );
  } finally {
    if (fixture) {
      await stopBroker(fixture);
    }
  }
});

test("clients with identical display names see all messages", async (context) => {
  let fixture: BrokerFixture | undefined;
  try {
    fixture = await startBroker(context);
    const FerrumQClient = await importClient();

    const room = `room-${randomUUID().slice(0, 8)}`;
    const topic = `chat.${room}`;

    const group1 = `chat.${room}.session.a-${randomUUID().slice(0, 8)}`;
    const group2 = `chat.${room}.session.b-${randomUUID().slice(0, 8)}`;

    const client1 = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });
    const client2 = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });

    try {
      await client1.createTopic({ name: topic, partitions: 1 });

      await client1.publish({
        topic,
        payload: chatPayload(room, "id-1", "User", group1, "Hello from first"),
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      const d1 = await client2.consume({
        topic,
        group: group2,
        consumerId: `consumer-${group2}`,
        maxMessages: 5,
        leaseMs: 30_000,
      });
      expect(d1.length).toBeGreaterThanOrEqual(1);
      const firstDelivery = d1[0];
      if (!firstDelivery) {
        throw new Error("expected at least one delivery");
      }
      expect(decoder.decode(firstDelivery.payload)).toContain(
        "Hello from first",
      );

      await client2.ack({
        deliveryId: firstDelivery.deliveryId,
        consumerId: `consumer-${group2}`,
      });
    } finally {
      client1.close();
      client2.close();
    }
  } catch (error) {
    if (isLoopbackPermissionError(error, fixture?.output() ?? "")) {
      context.skip();
      return;
    }
    throw new Error(
      `Same-name test failed: ${errorMessage(error)}\n${fixture?.output() ?? ""}`,
      { cause: error },
    );
  } finally {
    if (fixture) {
      await stopBroker(fixture);
    }
  }
});

test("messages are isolated across two rooms", async (context) => {
  let fixture: BrokerFixture | undefined;
  try {
    fixture = await startBroker(context);
    const FerrumQClient = await importClient();

    const roomA = `room-a-${randomUUID().slice(0, 8)}`;
    const roomB = `room-b-${randomUUID().slice(0, 8)}`;
    const topicA = `chat.${roomA}`;
    const topicB = `chat.${roomB}`;
    const groupA = `chat.${roomA}.session.a-${randomUUID().slice(0, 8)}`;
    const groupB = `chat.${roomB}.session.b-${randomUUID().slice(0, 8)}`;

    const client = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });

    try {
      await client.createTopic({ name: topicA, partitions: 1 });
      await client.createTopic({ name: topicB, partitions: 1 });

      await client.publish({
        topic: topicA,
        payload: chatPayload(roomA, "x", "X", groupA, "Only in room A"),
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      await client.publish({
        topic: topicB,
        payload: chatPayload(roomB, "y", "Y", groupB, "Only in room B"),
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      const da = await client.consume({
        topic: topicA,
        group: groupA,
        consumerId: `consumer-${groupA}`,
        maxMessages: 5,
        leaseMs: 30_000,
      });
      const db = await client.consume({
        topic: topicB,
        group: groupB,
        consumerId: `consumer-${groupB}`,
        maxMessages: 5,
        leaseMs: 30_000,
      });

      const textA = da.map((d) => decoder.decode(d.payload));
      const textB = db.map((d) => decoder.decode(d.payload));

      expect(textA.some((m) => m.includes("Only in room A"))).toBe(true);
      expect(textA.every((m) => !m.includes("Only in room B"))).toBe(true);
      expect(textB.some((m) => m.includes("Only in room B"))).toBe(true);
      expect(textB.every((m) => !m.includes("Only in room A"))).toBe(true);
    } finally {
      client.close();
    }
  } catch (error) {
    if (isLoopbackPermissionError(error, fixture?.output() ?? "")) {
      context.skip();
      return;
    }
    throw new Error(
      `Room isolation failed: ${errorMessage(error)}\n${fixture?.output() ?? ""}`,
      { cause: error },
    );
  } finally {
    if (fixture) {
      await stopBroker(fixture);
    }
  }
});

test("one client shutting down does not affect another", async (context) => {
  let fixture: BrokerFixture | undefined;
  try {
    fixture = await startBroker(context);
    const FerrumQClient = await importClient();

    const room = `room-${randomUUID().slice(0, 8)}`;
    const topic = `chat.${room}`;

    const group1 = `chat.${room}.session.a-${randomUUID().slice(0, 8)}`;
    const group2 = `chat.${room}.session.b-${randomUUID().slice(0, 8)}`;

    const client1 = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });
    const client2 = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });

    try {
      await client1.createTopic({ name: topic, partitions: 1 });

      await client1.publish({
        topic,
        payload: chatPayload(room, "id-1", "A", group1, "Pre-shutdown"),
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      client1.close();

      await client2.publish({
        topic,
        payload: chatPayload(room, "id-2", "B", group2, "Post-shutdown"),
        type: "ferrumq.chat.message.v1",
        source: "ferrumq-chat",
      });

      const deliveries = await client2.consume({
        topic,
        group: group2,
        consumerId: `consumer-${group2}`,
        maxMessages: 5,
        leaseMs: 30_000,
      });

      expect(deliveries.length).toBeGreaterThanOrEqual(1);
      const texts = deliveries.map((d) => decoder.decode(d.payload));
      expect(texts.some((t) => t.includes("Post-shutdown"))).toBe(true);
    } finally {
      client2.close();
    }
  } catch (error) {
    if (isLoopbackPermissionError(error, fixture?.output() ?? "")) {
      context.skip();
      return;
    }
    throw new Error(
      `Shutdown isolation failed: ${errorMessage(error)}\n${fixture?.output() ?? ""}`,
      { cause: error },
    );
  } finally {
    if (fixture) {
      await stopBroker(fixture);
    }
  }
});

test("topic creation race with two concurrent clients", async (context) => {
  let fixture: BrokerFixture | undefined;
  try {
    fixture = await startBroker(context);
    const FerrumQClient = await importClient();

    const room = `room-${randomUUID().slice(0, 8)}`;
    const topic = `chat.${room}`;

    const client1 = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });
    const client2 = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });

    try {
      const results = await Promise.allSettled([
        client1.createTopic({ name: topic, partitions: 1 }),
        client2.createTopic({ name: topic, partitions: 1 }),
      ]);

      const succeeded = results.filter((r) => r.status === "fulfilled");
      expect(succeeded.length).toBeGreaterThanOrEqual(1);

      const failed = results.filter((r) => r.status === "rejected");
      for (const r of failed) {
        expect(r.reason).toBeDefined();
      }
    } finally {
      client1.close();
      client2.close();
    }
  } catch (error) {
    if (isLoopbackPermissionError(error, fixture?.output() ?? "")) {
      context.skip();
      return;
    }
    throw new Error(
      `Topic race failed: ${errorMessage(error)}\n${fixture?.output() ?? ""}`,
      { cause: error },
    );
  } finally {
    if (fixture) {
      await stopBroker(fixture);
    }
  }
});

test("new session replays existing messages from the topic history", async (context) => {
  let fixture: BrokerFixture | undefined;
  try {
    fixture = await startBroker(context);
    const FerrumQClient = await importClient();

    const room = `room-${randomUUID().slice(0, 8)}`;
    const topic = `chat.${room}`;
    const group = `chat.${room}.session.a-${randomUUID().slice(0, 8)}`;

    const client = new FerrumQClient({
      httpUrl: fixture.httpUrl,
      grpcUrl: fixture.grpcUrl,
      timeoutMs: 5_000,
    });

    try {
      await client.createTopic({ name: topic, partitions: 1 });

      for (let i = 1; i <= 3; i++) {
        await client.publish({
          topic,
          payload: chatPayload(
            room,
            "sender",
            "User",
            group,
            `History message ${i}`,
          ),
          type: "ferrumq.chat.message.v1",
          source: "ferrumq-chat",
        });
      }

      const newGroup = `chat.${room}.session.new-${randomUUID().slice(0, 8)}`;
      const deliveries = await client.consume({
        topic,
        group: newGroup,
        consumerId: `consumer-${newGroup}`,
        maxMessages: 10,
        leaseMs: 30_000,
      });

      expect(deliveries.length).toBe(3);
      const texts = deliveries.map((d) => decoder.decode(d.payload));
      expect(texts[0]).toContain("History message 1");
      expect(texts[1]).toContain("History message 2");
      expect(texts[2]).toContain("History message 3");
    } finally {
      client.close();
    }
  } catch (error) {
    if (isLoopbackPermissionError(error, fixture?.output() ?? "")) {
      context.skip();
      return;
    }
    throw new Error(
      `History replay failed: ${errorMessage(error)}\n${fixture?.output() ?? ""}`,
      { cause: error },
    );
  } finally {
    if (fixture) {
      await stopBroker(fixture);
    }
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
