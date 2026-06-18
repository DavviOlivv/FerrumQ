import { FerrumQClient } from "@ferrumq/sdk";

const HTTP_URL = process.env.FERRUMQ_HTTP_URL ?? "http://127.0.0.1:8080";
const GRPC_URL = process.env.FERRUMQ_GRPC_URL ?? "http://127.0.0.1:9090";

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
    let topic;
    try {
      topic = await client.createTopic({ name: "orders", partitions: 1 });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.includes("TOPIC_ALREADY_EXISTS") || message.includes("409")) {
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
      topic: "orders",
      payload: { orderId: 2, status: "reject" },
    });
    console.log("published:", published);

    const maxNacks = 3;
    for (let i = 0; i < maxNacks; i++) {
      console.log(`--- consume (attempt ${i + 1}) ---`);
      const deliveries = await client.consume({
        topic: "orders",
        group: "workers",
        consumerId: "worker-1",
        maxMessages: 1,
      });

      if (deliveries.length === 0) {
        console.log("no deliveries (message may have moved to DLQ)");
        break;
      }

      const delivery = deliveries[0]!;
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
    const dlqEntries = await client.listDlq("orders");
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

main().catch((error) => {
  console.error(
    "example failed:",
    error instanceof Error ? error.message : error,
  );
  process.exitCode = 1;
});
