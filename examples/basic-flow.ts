import { FerrumQClient, type Topic } from "@ferrumq/sdk";

const HTTP_URL = process.env.FERRUMQ_HTTP_URL ?? "http://127.0.0.1:8080";
const GRPC_URL = process.env.FERRUMQ_GRPC_URL ?? "http://127.0.0.1:9090";

async function main() {
  const client = new FerrumQClient({
    httpUrl: HTTP_URL,
    grpcUrl: GRPC_URL,
    timeoutMs: 10_000,
  });

  try {
    console.log("--- health ---");
    const health = await client.health();
    console.log(health);

    console.log("--- readiness ---");
    const ready = await client.readiness();
    console.log(ready);

    console.log("--- createTopic ---");
    let topic: Topic | undefined;
    try {
      topic = await client.createTopic({ name: "orders", partitions: 3 });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.includes("TOPIC_ALREADY_EXISTS") || message.includes("409")) {
        console.log("topic already exists, continuing");
      } else {
        throw error;
      }
    }
    if (topic !== undefined) {
      console.log(topic);
    }

    console.log("--- listTopics ---");
    const topics = await client.listTopics();
    console.log(topics.map((t) => t.name).join(", "));

    console.log("--- publish ---");
    const published = await client.publish({
      topic: "orders",
      key: "account-1",
      payload: { orderId: 1, status: "created" },
    });
    console.log(published);

    console.log("--- consume ---");
    const deliveries = await client.consume({
      topic: "orders",
      group: "workers",
      consumerId: "worker-1",
      maxMessages: 1,
    });

    for (const delivery of deliveries) {
      console.log(`deliveryId: ${delivery.deliveryId}`);
      console.log(`  partition: ${delivery.partition}`);
      console.log(`  offset:    ${delivery.offset}`);
      console.log(`  attempt:   ${delivery.attemptNumber}`);
      console.log(`  payload:   ${new TextDecoder().decode(delivery.payload)}`);

      console.log("--- ack ---");
      await client.ack({
        deliveryId: delivery.deliveryId,
        consumerId: "worker-1",
      });
      console.log("acked");
    }

    console.log("--- status ---");
    const status = await client.status();
    console.log(status);

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
