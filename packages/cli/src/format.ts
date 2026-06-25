import type {
  BrokerStatusResponse,
  DataPlaneConsumedMessage,
  DataPlanePublishResponse,
  DlqEntryResponse,
  SearchResult,
  TopicResponse,
} from "@ferrumq/protocol";

export function jsonLine(value: unknown): string {
  return JSON.stringify(value);
}

export function formatStatus(status: BrokerStatusResponse): string {
  return [
    `mode: ${status.mode}`,
    `data dir: ${status.dataDir}`,
    `topics: ${status.topics}`,
    `dlq entries: ${status.dlqEntries}`,
  ].join("\n");
}

export function formatTopic(topic: TopicResponse, action = "topic"): string {
  return `${action}: ${topic.name} (partitions: ${topic.partitions})`;
}

export function formatTopicList(topics: TopicResponse[]): string {
  if (topics.length === 0) {
    return "no topics";
  }

  return topics
    .map((topic) => `${topic.name}\tpartitions=${topic.partitions}`)
    .join("\n");
}

export function formatDlq(entries: DlqEntryResponse[]): string {
  if (entries.length === 0) {
    return "no DLQ entries";
  }

  return entries
    .map(
      (entry) =>
        `${entry.topic}[${entry.partition}]@${entry.offset}\tmessage=${entry.messageId}\tgroup=${entry.consumerGroupId}\treason=${entry.reason}`,
    )
    .join("\n");
}

export function formatPublished(response: DataPlanePublishResponse): string {
  const base = `published: ${response.messageId} ${response.topic}[${response.partition}]@${response.offset}`;
  return response.deduplicated ? `${base} (deduplicated)` : base;
}

export function formatMessages(messages: DataPlaneConsumedMessage[]): string {
  if (messages.length === 0) {
    return "no messages";
  }

  return messages
    .map(
      (message) =>
        `delivery=${message.deliveryId}\tmessage=${message.messageId}\ttopic=${message.topic}\tpartition=${message.partition}\toffset=${message.offset}\tattempt=${message.attemptNumber}\tpayload=${message.payload.toString("utf8")}`,
    )
    .join("\n");
}

export function consumedMessageJson(
  message: DataPlaneConsumedMessage,
): Record<string, unknown> {
  return {
    deliveryId: message.deliveryId,
    topic: message.topic,
    partition: message.partition,
    offset: message.offset,
    messageId: message.messageId,
    key: emptyToNull(message.key),
    data: message.payload.toString("utf8"),
    contentType: message.contentType,
    type: message.type,
    source: message.source,
    subject: emptyToNull(message.subject),
    idempotencyKey: emptyToNull(message.idempotencyKey),
    timeUnixMs: message.timeUnixMs,
    consumerGroup: message.consumerGroup,
    consumerId: message.consumerId,
    attemptNumber: message.attemptNumber,
    deliveredAtUnixMs: message.deliveredAtUnixMs,
    leaseExpiresAtUnixMs: message.leaseExpiresAtUnixMs,
  };
}

function emptyToNull(value: string): string | null {
  return value.length === 0 ? null : value;
}

const SHA256_PREFIX_LEN = 12;

export function formatSearch(results: SearchResult[]): string {
  if (results.length === 0) {
    return "no results";
  }
  return results
    .map((result) => {
      const sha = shortenSha256(result.payloadSha256);
      return [
        `${result.topic}[${result.partitionId}]@${result.offset}`,
        `message=${result.messageId}`,
        `event=${result.eventType}`,
        `source=${result.source}`,
        `subject=${result.subject ?? ""}`,
        `content=${result.contentType}`,
        `time=${result.timeUnixMs}`,
        `payload_len=${result.payloadLen}`,
        `sha256=${sha}`,
        `rank=${result.rank}`,
      ].join("\t");
    })
    .join("\n");
}

/**
 * Emits the full search result set (including the full 64-char
 * `payloadSha256` hash) under the wrapper key `search`. Decimal-string
 * fields (`offset`, `timeUnixMs`) are preserved as strings.
 */
export function searchJson(results: SearchResult[]): Record<string, unknown> {
  return {
    search: { items: results },
  };
}

function shortenSha256(value: string): string {
  if (value.length <= SHA256_PREFIX_LEN) {
    return value;
  }
  return `${value.slice(0, SHA256_PREFIX_LEN)}…`;
}
