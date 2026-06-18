import { FerrumQClient, FerrumQError, type Topic } from "@ferrumq/sdk";

const HTTP_URL = process.env.FERRUMQ_HTTP_URL ?? "http://127.0.0.1:8080";
const GRPC_URL = process.env.FERRUMQ_GRPC_URL ?? "http://127.0.0.1:9090";
const TOPIC =
  process.env.FERRUMQ_EXAMPLE_TOPIC ??
  `orders-nack-${process.pid}-${Date.now()}`;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const client = new FerrumQClient({
    httpUrl: HTTP_URL,
    grpcUrl: GRPC_URL,
    timeoutMs: 10_000,
  });

  try {
    let topic: Topic | undefined;
    try {
      topic = await client.createTopic({ name: TOPIC, partitions: 1 });
    } catch (error) {
      if (
        error instanceof FerrumQError &&
        (error.code === "TOPIC_ALREADY_EXISTS" || error.status === 409)
      ) {
        console.log("topic already exists, continuing");
      } else {
        throw error;
      }
    }
    if (topic !== undefined) {
      console.log("created topic:", topic);
    }

    console.log("--- publish ---");
    const published = await client.publish({
      topic: TOPIC,
      payload: { orderId: 2, status: "reject" },
    });
    console.log("published:", published);

    const maxNacks = 3;
    for (let i = 0; i < maxNacks; i++) {
      console.log(`--- consume (attempt ${i + 1}) ---`);
      const deliveries = await pollForDelivery(client);

      const [delivery] = deliveries;
      if (delivery === undefined) {
        console.log("no deliveries (message may have moved to DLQ)");
        break;
      }

      console.log(`deliveryId: ${delivery.deliveryId}`);
      console.log(`  attempt: ${delivery.attemptNumber}`);

      console.log(`--- nack (attempt ${i + 1}) ---`);
      await client.nack({
        deliveryId: delivery.deliveryId,
        consumerId: "worker-1",
        reason: "poison",
      });
      console.log("nacked with reason: poison");

      const retryBackoffMs = 1;
      console.log(`waiting ${retryBackoffMs}s for retry backoff...`);
      await sleep(retryBackoffMs * 1000);
    }

    console.log("--- dlq inspection ---");
    const dlqEntries = await pollForDlq(client);
    if (dlqEntries.length === 0) {
      console.log("no DLQ entries yet (message may still be retrying)");
    } else {
      for (const entry of dlqEntries) {
        console.log(`topic:     ${entry.topic}`);
        console.log(`partition: ${entry.partition}`);
        console.log(`offset:    ${entry.offset}`);
        console.log(`messageId: ${entry.messageId}`);
        console.log(`group:     ${entry.consumerGroupId}`);
        console.log(`reason:    ${entry.reason}`);
        console.log(`attempts:  ${entry.attemptCount}`);
        console.log("---");
      }
    }

    console.log("done");
  } finally {
    client.close();
  }
}

async function pollForDelivery(client: FerrumQClient) {
  for (let attempt = 0; attempt < 20; attempt++) {
    const deliveries = await client.consume({
      topic: TOPIC,
      group: "workers",
      consumerId: "worker-1",
      maxMessages: 1,
    });
    if (deliveries.length > 0) {
      return deliveries;
    }
    await sleep(100);
  }
  return [];
}

async function pollForDlq(client: FerrumQClient) {
  for (let attempt = 0; attempt < 20; attempt++) {
    const entries = await client.listDlq(TOPIC);
    if (entries.length > 0) {
      return entries;
    }
    await sleep(100);
  }
  return [];
}

main().catch((error) => {
  console.error(
    "example failed:",
    error instanceof Error ? error.message : error,
  );
  process.exitCode = 1;
});
