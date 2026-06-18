import { FerrumQClient } from "@ferrumq/sdk";

const HTTP_URL = process.env.FERRUMQ_HTTP_URL ?? "http://127.0.0.1:8080";
const GRPC_URL = process.env.FERRUMQ_GRPC_URL ?? "http://127.0.0.1:9090";

async function main() {
  const client = new FerrumQClient({
    httpUrl: HTTP_URL,
    grpcUrl: GRPC_URL,
    timeoutMs: 10_000,
  });

  try {
    console.log("--- broker status ---");
    const status = await client.status();
    console.log(`mode:       ${status.mode}`);
    console.log(`dataDir:    ${status.dataDir}`);
    console.log(`topics:     ${status.topics}`);
    console.log(`dlqEntries: ${status.dlqEntries}`);

    console.log("\n--- topics ---");
    const topics = await client.listTopics();
    for (const topic of topics) {
      console.log(`  ${topic.name}: ${topic.partitions} partitions`);
    }

    console.log("\n--- metrics (first 20 lines) ---");
    const metrics = await client.metrics();
    const lines = metrics.split("\n").slice(0, 20);
    for (const line of lines) {
      if (line.length > 120) {
        console.log(`${line.slice(0, 117)}...`);
      } else {
        console.log(line);
      }
    }
    console.log(`... (${metrics.split("\n").length} lines total)`);

    console.log("\n--- dlq entries ---");
    const dlqEntries = await client.listDlq();
    console.log(`${dlqEntries.length} DLQ entries`);
    for (const entry of dlqEntries.slice(0, 3)) {
      console.log(
        `  ${entry.topic}/${entry.partition}@${entry.offset} reason=${entry.reason} attempts=${entry.attemptCount}`,
      );
    }

    console.log("\ndone");
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
