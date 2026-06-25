import { randomUUID as nodeRandomUUID } from "node:crypto";
import {
  createGrpcDataPlaneClient,
  type DataPlaneClient,
  formatGrpcError,
  grpcStatusName,
} from "@ferrumq/protocol";
import { type BrokerdVersionRunner, runBrokerdVersion } from "./brokerd.js";
import {
  defaultConsumerId,
  defaultPublishContentType,
  defaultPublishSource,
  defaultPublishType,
  type ResolvedConfig,
} from "./config.js";
import { ExpectedCliError } from "./errors.js";
import {
  consumedMessageJson,
  formatDlq,
  formatMessages,
  formatPublished,
  formatSearch,
  formatStatus,
  formatTopic,
  formatTopicList,
  jsonLine,
  searchJson,
} from "./format.js";
import {
  ackHelpText,
  brokerHelpText,
  consumeHelpText,
  dlqHelpText,
  nackHelpText,
  publishHelpText,
  rootHelpText,
  searchHelpText,
  topicHelpText,
  versionText,
} from "./help.js";
import {
  type ControlPlaneClient,
  createControlPlaneClient,
  type FetchLike,
} from "./http-client.js";
import type { ParsedCommand } from "./parser.js";
import {
  parsePositiveInteger,
  validateBoundedText,
  validateConsumerGroup,
  validateNonEmptyPayload,
  validateSearchLimit,
  validateSearchQuery,
  validateTopic,
} from "./validation.js";

const IDEMPOTENCY_CONFLICT_DETAIL = "idempotency key conflict";

export interface CommandDependencies {
  fetch?: FetchLike;
  controlClient?: ControlPlaneClient;
  dataPlaneClient?: DataPlaneClient;
  dataPlaneClientFactory?: (grpcUrl: string) => DataPlaneClient;
  brokerdVersionRunner?: BrokerdVersionRunner;
  now?: () => number;
  randomUUID?: () => string;
}

export interface CommandResult {
  stdout: string;
}

interface CommandContext {
  config: ResolvedConfig;
  dependencies: CommandDependencies;
}

export async function executeCommand(
  command: ParsedCommand,
  config: ResolvedConfig,
  dependencies: CommandDependencies = {},
): Promise<CommandResult> {
  const context: CommandContext = { config, dependencies };

  switch (command.kind) {
    case "root-help":
      return text(rootHelpText());
    case "version":
      return text(versionText());
    case "broker-help":
      return text(brokerHelpText());
    case "topic-help":
      return text(topicHelpText());
    case "dlq-help":
      return text(dlqHelpText());
    case "publish-help":
      return text(publishHelpText());
    case "consume-help":
      return text(consumeHelpText());
    case "ack-help":
      return text(ackHelpText());
    case "nack-help":
      return text(nackHelpText());
    case "search-help":
      return text(searchHelpText());
    case "broker-version":
      return text(await brokerdVersion(context)());
    case "health":
      return health(context);
    case "ready":
      return ready(context);
    case "status":
      return status(context);
    case "topic-create":
      return topicCreate(context, command);
    case "topic-get":
      return topicGet(context, command);
    case "topic-list":
      return topicList(context);
    case "dlq-list":
      return dlqList(context, command);
    case "publish":
      return publish(context, command);
    case "consume":
      return consume(context, command);
    case "ack":
      return ack(context, command);
    case "nack":
      return nack(context, command);
    case "search":
      return search(context, command);
  }
}

async function health(context: CommandContext): Promise<CommandResult> {
  const response = await controlClient(context).health();
  return context.config.json
    ? text(jsonLine({ health: response }))
    : text(`health: ${response.status}`);
}

async function ready(context: CommandContext): Promise<CommandResult> {
  const response = await controlClient(context).ready();
  return context.config.json
    ? text(jsonLine({ ready: response }))
    : text(`ready: ${response.status}`);
}

async function status(context: CommandContext): Promise<CommandResult> {
  const response = await controlClient(context).status();
  return context.config.json
    ? text(jsonLine({ status: response }))
    : text(formatStatus(response));
}

async function topicCreate(
  context: CommandContext,
  command: Extract<ParsedCommand, { kind: "topic-create" }>,
): Promise<CommandResult> {
  const topic = validateTopic(command.topic);
  const partitions =
    command.partitions === undefined
      ? 1
      : parsePositiveInteger(command.partitions, "--partitions");
  const response = await controlClient(context).createTopic(topic, partitions);
  return context.config.json
    ? text(jsonLine({ topic: response }))
    : text(formatTopic(response, "topic created"));
}

async function topicGet(
  context: CommandContext,
  command: Extract<ParsedCommand, { kind: "topic-get" }>,
): Promise<CommandResult> {
  const response = await controlClient(context).getTopic(
    validateTopic(command.topic),
  );
  return context.config.json
    ? text(jsonLine({ topic: response }))
    : text(formatTopic(response));
}

async function topicList(context: CommandContext): Promise<CommandResult> {
  const response = await controlClient(context).listTopics();
  return context.config.json
    ? text(jsonLine({ topics: response.items }))
    : text(formatTopicList(response.items));
}

async function dlqList(
  context: CommandContext,
  command: Extract<ParsedCommand, { kind: "dlq-list" }>,
): Promise<CommandResult> {
  const topic =
    command.topic === undefined ? undefined : validateTopic(command.topic);
  const response = await controlClient(context).listDlq(topic);
  return context.config.json
    ? text(jsonLine({ dlq: { items: response.items } }))
    : text(formatDlq(response.items));
}

async function publish(
  context: CommandContext,
  command: Extract<ParsedCommand, { kind: "publish" }>,
): Promise<CommandResult> {
  const topic = validateTopic(command.topic);
  const data = validateNonEmptyPayload(required(command.data, "--data"));
  const messageId =
    command.messageId === undefined
      ? `msg_${randomUUID(context)()}`
      : validateBoundedText(command.messageId, "message ID");
  const response = await dataPlaneClient(context).publish({
    topic,
    messageId,
    payload: Buffer.from(data, "utf8"),
    contentType: validateBoundedText(
      command.contentType ?? defaultPublishContentType,
      "content type",
    ),
    type: validateBoundedText(
      command.type ?? defaultPublishType,
      "message type",
    ),
    source: validateBoundedText(
      command.source ?? defaultPublishSource,
      "message source",
    ),
    timeUnixMs: String(now(context)()),
    ...(command.key === undefined
      ? {}
      : { key: validateBoundedText(command.key, "partition key") }),
    ...(command.subject === undefined
      ? {}
      : { subject: validateBoundedText(command.subject, "subject") }),
    ...(command.idempotencyKey === undefined
      ? {}
      : {
          idempotencyKey: validateBoundedText(
            command.idempotencyKey,
            "idempotency key",
          ),
        }),
  });

  return context.config.json
    ? text(
        jsonLine({
          message: {
            id: response.messageId,
            topic: response.topic,
            partition: response.partition,
            offset: response.offset,
            deduplicated: response.deduplicated,
          },
        }),
      )
    : text(formatPublished(response));
}

async function consume(
  context: CommandContext,
  command: Extract<ParsedCommand, { kind: "consume" }>,
): Promise<CommandResult> {
  const response = await dataPlaneClient(context).consume({
    topic: validateTopic(command.topic),
    consumerGroup: validateConsumerGroup(required(command.group, "--group")),
    consumerId: validateBoundedText(
      command.consumerId ?? defaultConsumerId,
      "consumer ID",
    ),
    maxMessages:
      command.max === undefined
        ? 1
        : parsePositiveInteger(command.max, "--max"),
    leaseMs: String(
      command.leaseMs === undefined
        ? 30_000
        : parsePositiveInteger(command.leaseMs, "--lease-ms"),
    ),
    nowUnixMs: String(now(context)()),
  });

  return context.config.json
    ? text(jsonLine({ messages: response.messages.map(consumedMessageJson) }))
    : text(formatMessages(response.messages));
}

async function ack(
  context: CommandContext,
  command: Extract<ParsedCommand, { kind: "ack" }>,
): Promise<CommandResult> {
  const deliveryId = validateBoundedText(command.deliveryId, "delivery ID");
  const consumerId = validateBoundedText(
    command.consumerId ?? defaultConsumerId,
    "consumer ID",
  );
  await dataPlaneClient(context).ack({ deliveryId, consumerId });
  return context.config.json
    ? text(jsonLine({ ack: { deliveryId, consumerId } }))
    : text(`acked: ${deliveryId} consumer=${consumerId}`);
}

async function nack(
  context: CommandContext,
  command: Extract<ParsedCommand, { kind: "nack" }>,
): Promise<CommandResult> {
  const deliveryId = validateBoundedText(command.deliveryId, "delivery ID");
  const consumerId = validateBoundedText(
    command.consumerId ?? defaultConsumerId,
    "consumer ID",
  );
  const reason =
    command.reason === undefined
      ? undefined
      : validateBoundedText(command.reason, "reason");
  await dataPlaneClient(context).nack({
    deliveryId,
    consumerId,
    ...(reason === undefined ? {} : { reason }),
  });
  return context.config.json
    ? text(
        jsonLine({ nack: { deliveryId, consumerId, reason: reason ?? null } }),
      )
    : text(
        `nacked: ${deliveryId} consumer=${consumerId}${reason === undefined ? "" : ` reason=${reason}`}`,
      );
}

function controlClient(context: CommandContext): ControlPlaneClient {
  return (
    context.dependencies.controlClient ??
    createControlPlaneClient(
      context.config.controlUrl,
      context.dependencies.fetch,
    )
  );
}

function dataPlaneClient(context: CommandContext): DataPlaneClient {
  const client =
    context.dependencies.dataPlaneClient ??
    context.dependencies.dataPlaneClientFactory?.(context.config.grpcUrl) ??
    createGrpcDataPlaneClient(context.config.grpcUrl);

  return wrapGrpcErrors(client);
}

function wrapGrpcErrors(client: DataPlaneClient): DataPlaneClient {
  return {
    async publish(request) {
      try {
        return await client.publish(request);
      } catch (error) {
        const statusName =
          typeof (error as { code?: unknown }).code === "number"
            ? grpcStatusName((error as { code?: unknown }).code as number)
            : undefined;
        const details = (error as { details?: unknown }).details;
        if (
          statusName === "ALREADY_EXISTS" &&
          details === IDEMPOTENCY_CONFLICT_DETAIL
        ) {
          throw new ExpectedCliError(
            `IDEMPOTENCY_KEY_CONFLICT: ${IDEMPOTENCY_CONFLICT_DETAIL}`,
            1,
          );
        }
        throw new ExpectedCliError(formatGrpcError(error));
      }
    },
    async consume(request) {
      try {
        return await client.consume(request);
      } catch (error) {
        throw new ExpectedCliError(formatGrpcError(error));
      }
    },
    async ack(request) {
      try {
        await client.ack(request);
      } catch (error) {
        throw new ExpectedCliError(formatGrpcError(error));
      }
    },
    async nack(request) {
      try {
        await client.nack(request);
      } catch (error) {
        throw new ExpectedCliError(formatGrpcError(error));
      }
    },
    close() {
      client.close();
    },
  };
}

function brokerdVersion(context: CommandContext): BrokerdVersionRunner {
  return context.dependencies.brokerdVersionRunner ?? runBrokerdVersion;
}

function now(context: CommandContext): () => number {
  return context.dependencies.now ?? Date.now;
}

function randomUUID(context: CommandContext): () => string {
  return context.dependencies.randomUUID ?? nodeRandomUUID;
}

function required(value: string | undefined, flag: string): string {
  if (value === undefined) {
    throw new ExpectedCliError(`${flag} is required`);
  }
  return value;
}

async function search(
  context: CommandContext,
  command: Extract<ParsedCommand, { kind: "search" }>,
): Promise<CommandResult> {
  const query = validateSearchQuery(command.query);
  const topic =
    command.topic === undefined ? undefined : validateTopic(command.topic);
  const limit = validateSearchLimit(command.limit);
  const response = await controlClient(context).searchMessages({
    query,
    ...(topic === undefined ? {} : { topic }),
    limit,
  });
  return context.config.json
    ? text(jsonLine(searchJson(response.items)))
    : text(formatSearch(response.items));
}

function text(stdout: string): CommandResult {
  return { stdout };
}
