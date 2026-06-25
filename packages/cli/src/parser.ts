import type { GlobalOptions } from "./config.js";
import { ExpectedCliError } from "./errors.js";

export type ParsedCommand =
  | { kind: "root-help" }
  | { kind: "version" }
  | { kind: "broker-help" }
  | { kind: "topic-help" }
  | { kind: "dlq-help" }
  | { kind: "publish-help" }
  | { kind: "consume-help" }
  | { kind: "ack-help" }
  | { kind: "nack-help" }
  | { kind: "search-help" }
  | { kind: "broker-version" }
  | { kind: "health" }
  | { kind: "ready" }
  | { kind: "status" }
  | { kind: "topic-create"; topic: string; partitions?: string }
  | { kind: "topic-get"; topic: string }
  | { kind: "topic-list" }
  | { kind: "dlq-list"; topic?: string }
  | { kind: "search"; query: string; topic?: string; limit?: string }
  | {
      kind: "publish";
      topic: string;
      data?: string;
      messageId?: string;
      key?: string;
      contentType?: string;
      type?: string;
      source?: string;
      subject?: string;
      idempotencyKey?: string;
    }
  | {
      kind: "consume";
      topic: string;
      group?: string;
      consumerId?: string;
      max?: string;
      leaseMs?: string;
    }
  | { kind: "ack"; deliveryId: string; consumerId?: string }
  | { kind: "nack"; deliveryId: string; consumerId?: string; reason?: string };

export interface ParsedCli {
  globals: GlobalOptions;
  command: ParsedCommand;
}

interface GlobalParseResult {
  globals: GlobalOptions;
  tokens: string[];
  helpRequested: boolean;
  versionRequested: boolean;
}

interface OptionParseResult {
  positionals: string[];
  options: Map<string, string>;
}

export function parseCliArgs(args: readonly string[]): ParsedCli {
  const parsed = parseGlobals(args);
  const command = parseCommand(
    parsed.tokens,
    parsed.helpRequested,
    parsed.versionRequested,
  );

  return {
    globals: parsed.globals,
    command,
  };
}

function parseGlobals(args: readonly string[]): GlobalParseResult {
  const tokens: string[] = [];
  const globals: GlobalOptions = { json: false };
  let helpRequested = false;
  let versionRequested = false;

  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    if (token === undefined) {
      continue;
    }

    if (token === "--json") {
      globals.json = true;
      continue;
    }

    if (token === "--help" || token === "-h") {
      helpRequested = true;
      continue;
    }

    if (token === "--version" || token === "-V") {
      versionRequested = true;
      continue;
    }

    const controlUrl = readGlobalValue(args, index, token, "--control-url");
    if (controlUrl.matched) {
      globals.controlUrl = controlUrl.value;
      index = controlUrl.nextIndex;
      continue;
    }

    const grpcUrl = readGlobalValue(args, index, token, "--grpc-url");
    if (grpcUrl.matched) {
      globals.grpcUrl = grpcUrl.value;
      index = grpcUrl.nextIndex;
      continue;
    }

    tokens.push(token);
  }

  return { globals, tokens, helpRequested, versionRequested };
}

function readGlobalValue(
  args: readonly string[],
  index: number,
  token: string,
  flag: string,
):
  | { matched: true; value: string; nextIndex: number }
  | { matched: false; nextIndex: number; value?: never } {
  if (token === flag) {
    const value = args[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new ExpectedCliError(`${flag} requires a value`);
    }
    return { matched: true, value, nextIndex: index + 1 };
  }

  const prefix = `${flag}=`;
  if (token.startsWith(prefix)) {
    const value = token.slice(prefix.length);
    if (value.length === 0) {
      throw new ExpectedCliError(`${flag} requires a value`);
    }
    return { matched: true, value, nextIndex: index };
  }

  return { matched: false, nextIndex: index };
}

function parseCommand(
  tokens: readonly string[],
  helpRequested: boolean,
  versionRequested: boolean,
): ParsedCommand {
  if (versionRequested) {
    if (tokens.length === 0) {
      return { kind: "version" };
    }
    throw new ExpectedCliError("--version must be used without a command");
  }

  if (helpRequested) {
    switch (tokens[0]) {
      case "broker":
        return { kind: "broker-help" };
      case "topic":
        return { kind: "topic-help" };
      case "dlq":
        return { kind: "dlq-help" };
      case "publish":
        return { kind: "publish-help" };
      case "consume":
        return { kind: "consume-help" };
      case "ack":
        return { kind: "ack-help" };
      case "nack":
        return { kind: "nack-help" };
      case "search":
        return { kind: "search-help" };
      default:
        return { kind: "root-help" };
    }
  }

  const command = tokens[0];
  if (command === undefined) {
    return { kind: "root-help" };
  }

  switch (command) {
    case "broker":
      return parseBroker(tokens.slice(1));
    case "health":
      assertNoExtra(tokens, "health");
      return { kind: "health" };
    case "ready":
      assertNoExtra(tokens, "ready");
      return { kind: "ready" };
    case "status":
      assertNoExtra(tokens, "status");
      return { kind: "status" };
    case "topic":
      return parseTopic(tokens.slice(1));
    case "dlq":
      return parseDlq(tokens.slice(1));
    case "publish":
      return parsePublish(tokens.slice(1));
    case "consume":
      return parseConsume(tokens.slice(1));
    case "ack":
      return parseAck(tokens.slice(1));
    case "nack":
      return parseNack(tokens.slice(1));
    case "search":
      return parseSearch(tokens.slice(1));
    default:
      throw new ExpectedCliError(`Unknown command: ${command}`);
  }
}

function parseBroker(tokens: readonly string[]): ParsedCommand {
  const subcommand = tokens[0];
  if (subcommand === undefined) {
    return { kind: "broker-help" };
  }

  if (subcommand !== "version") {
    throw new ExpectedCliError(`Unknown broker command: ${subcommand}`);
  }

  assertNoExtra(tokens, "broker version");
  return { kind: "broker-version" };
}

function parseTopic(tokens: readonly string[]): ParsedCommand {
  const subcommand = tokens[0];
  if (subcommand === undefined) {
    throw new ExpectedCliError("topic command requires create, get, or list");
  }

  const parsed = parseOptions(tokens.slice(1), ["partitions"]);
  switch (subcommand) {
    case "create":
      assertPositionals(parsed.positionals, 1, "topic create <topic>");
      return {
        kind: "topic-create",
        topic: parsed.positionals[0] as string,
        ...optionalProperty("partitions", parsed.options.get("partitions")),
      };
    case "get":
      rejectOptions(parsed.options);
      assertPositionals(parsed.positionals, 1, "topic get <topic>");
      return { kind: "topic-get", topic: parsed.positionals[0] as string };
    case "list":
      rejectOptions(parsed.options);
      assertPositionals(parsed.positionals, 0, "topic list");
      return { kind: "topic-list" };
    default:
      throw new ExpectedCliError(`Unknown topic command: ${subcommand}`);
  }
}

function parseDlq(tokens: readonly string[]): ParsedCommand {
  if (tokens[0] !== "list") {
    throw new ExpectedCliError("dlq command requires list");
  }

  const parsed = parseOptions(tokens.slice(1), ["topic"]);
  assertPositionals(parsed.positionals, 0, "dlq list");
  return {
    kind: "dlq-list",
    ...optionalProperty("topic", parsed.options.get("topic")),
  };
}

function parsePublish(tokens: readonly string[]): ParsedCommand {
  const parsed = parseOptions(tokens, [
    "data",
    "message-id",
    "key",
    "content-type",
    "type",
    "source",
    "subject",
    "idempotency-key",
  ]);
  assertPositionals(parsed.positionals, 1, "publish <topic> --data <string>");

  return {
    kind: "publish",
    topic: parsed.positionals[0] as string,
    ...optionalProperty("data", parsed.options.get("data")),
    ...optionalProperty("messageId", parsed.options.get("message-id")),
    ...optionalProperty("key", parsed.options.get("key")),
    ...optionalProperty("contentType", parsed.options.get("content-type")),
    ...optionalProperty("type", parsed.options.get("type")),
    ...optionalProperty("source", parsed.options.get("source")),
    ...optionalProperty("subject", parsed.options.get("subject")),
    ...optionalProperty(
      "idempotencyKey",
      parsed.options.get("idempotency-key"),
    ),
  };
}

function parseConsume(tokens: readonly string[]): ParsedCommand {
  const parsed = parseOptions(tokens, [
    "group",
    "consumer-id",
    "max",
    "lease-ms",
  ]);
  assertPositionals(parsed.positionals, 1, "consume <topic> --group <group>");

  return {
    kind: "consume",
    topic: parsed.positionals[0] as string,
    ...optionalProperty("group", parsed.options.get("group")),
    ...optionalProperty("consumerId", parsed.options.get("consumer-id")),
    ...optionalProperty("max", parsed.options.get("max")),
    ...optionalProperty("leaseMs", parsed.options.get("lease-ms")),
  };
}

function parseAck(tokens: readonly string[]): ParsedCommand {
  const parsed = parseOptions(tokens, ["consumer-id"]);
  assertPositionals(parsed.positionals, 1, "ack <delivery-id>");
  return {
    kind: "ack",
    deliveryId: parsed.positionals[0] as string,
    ...optionalProperty("consumerId", parsed.options.get("consumer-id")),
  };
}

function parseNack(tokens: readonly string[]): ParsedCommand {
  const parsed = parseOptions(tokens, ["consumer-id", "reason"]);
  assertPositionals(parsed.positionals, 1, "nack <delivery-id>");
  return {
    kind: "nack",
    deliveryId: parsed.positionals[0] as string,
    ...optionalProperty("consumerId", parsed.options.get("consumer-id")),
    ...optionalProperty("reason", parsed.options.get("reason")),
  };
}

function parseSearch(tokens: readonly string[]): ParsedCommand {
  const parsed = parseOptions(tokens, ["topic", "limit"]);
  assertPositionals(parsed.positionals, 1, "search <query>");
  return {
    kind: "search",
    query: parsed.positionals[0] ?? "",
    ...optionalProperty("topic", parsed.options.get("topic")),
    ...optionalProperty("limit", parsed.options.get("limit")),
  };
}

function parseOptions(
  tokens: readonly string[],
  allowed: readonly string[],
): OptionParseResult {
  const positionals: string[] = [];
  const options = new Map<string, string>();
  let optionSeen = false;

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === undefined) {
      continue;
    }

    if (!token.startsWith("--")) {
      if (optionSeen) {
        throw new ExpectedCliError(
          "Positional arguments must appear before options",
        );
      }
      positionals.push(token);
      continue;
    }

    optionSeen = true;
    const [rawName, inlineValue] = splitOption(token);
    const name = rawName.slice(2);
    if (!allowed.includes(name)) {
      throw new ExpectedCliError(`Unknown option: ${rawName}`);
    }
    if (options.has(name)) {
      throw new ExpectedCliError(`Duplicate option: ${rawName}`);
    }

    if (inlineValue !== undefined) {
      if (inlineValue.length === 0) {
        throw new ExpectedCliError(`${rawName} requires a value`);
      }
      options.set(name, inlineValue);
      continue;
    }

    const value = tokens[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new ExpectedCliError(`${rawName} requires a value`);
    }
    options.set(name, value);
    index += 1;
  }

  return { positionals, options };
}

function splitOption(token: string): [string, string | undefined] {
  const equalsIndex = token.indexOf("=");
  if (equalsIndex === -1) {
    return [token, undefined];
  }
  return [token.slice(0, equalsIndex), token.slice(equalsIndex + 1)];
}

function assertNoExtra(tokens: readonly string[], usage: string): void {
  if (tokens.length > 1) {
    throw new ExpectedCliError(`${usage} does not accept extra arguments`);
  }
}

function assertPositionals(
  positionals: readonly string[],
  expected: number,
  usage: string,
): readonly string[] {
  if (positionals.length !== expected) {
    throw new ExpectedCliError(`Usage: ferrumq ${usage}`);
  }
  return positionals;
}

function rejectOptions(options: Map<string, string>): void {
  const option = options.keys().next().value as string | undefined;
  if (option !== undefined) {
    throw new ExpectedCliError(`Unknown option: --${option}`);
  }
}

function optionalProperty<Key extends string>(
  key: Key,
  value: string | undefined,
): Partial<Record<Key, string>> {
  return value === undefined ? {} : ({ [key]: value } as Record<Key, string>);
}
