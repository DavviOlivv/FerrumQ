import {
  ControlPlaneRequestError,
  createControlPlaneClient,
} from "@ferrumq/protocol";

import type { TuiConfig } from "./config.js";
import type {
  BrokerStatusResponse,
  ControlPlaneClient,
  DlqListResponse,
  FetchLike,
  HttpStatusResponse,
  TopicListResponse,
} from "@ferrumq/protocol";

export interface TuiSnapshot {
  health: HttpStatusResponse;
  readiness: HttpStatusResponse;
  status: BrokerStatusResponse;
  topics: TopicListResponse;
  dlq: DlqListResponse;
  refreshedAt: Date;
}

export interface LoadTuiSnapshotDependencies {
  client?: ControlPlaneClient;
  fetch?: FetchLike;
  now?: () => Date;
}

export class TuiLoadError extends Error {
  readonly failures: readonly unknown[];

  constructor(message: string, failures: readonly unknown[]) {
    super(message);
    this.name = "TuiLoadError";
    this.failures = failures;
  }
}

export async function loadTuiSnapshot(
  config: TuiConfig,
  dependencies: LoadTuiSnapshotDependencies = {},
): Promise<TuiSnapshot> {
  const client =
    dependencies.client ??
    (dependencies.fetch === undefined
      ? createControlPlaneClient(config.controlUrl)
      : createControlPlaneClient(config.controlUrl, dependencies.fetch));

  const [health, readiness, status, topics, dlq] = await Promise.allSettled([
    client.health(),
    client.ready(),
    client.status(),
    client.listTopics(),
    client.listDlq(),
  ]);
  const failures: unknown[] = [];
  for (const result of [health, readiness, status, topics, dlq]) {
    if (result.status === "rejected") {
      failures.push(result.reason);
    }
  }

  if (failures.length > 0) {
    throw new TuiLoadError(formatRefreshFailure(failures), failures);
  }

  return {
    health: unwrap(health),
    readiness: unwrap(readiness),
    status: unwrap(status),
    topics: unwrap(topics),
    dlq: unwrap(dlq),
    refreshedAt: (dependencies.now ?? (() => new Date()))(),
  };
}

export function formatTuiError(error: unknown): string {
  if (error instanceof TuiLoadError) {
    return error.message;
  }

  if (error instanceof ControlPlaneRequestError) {
    return error.message;
  }

  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  return "unexpected error";
}

function formatRefreshFailure(failures: readonly unknown[]): string {
  const first = formatTuiError(failures[0]);
  if (failures.length === 1) {
    return `control API refresh failed: ${first}`;
  }

  return `control API refresh failed: ${first} (${failures.length} total failures)`;
}

function unwrap<T>(result: PromiseSettledResult<T>): T {
  if (result.status === "fulfilled") {
    return result.value;
  }

  throw new Error("unreachable rejected result");
}
