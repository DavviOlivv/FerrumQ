import {
  ControlPlaneRequestError,
  createControlPlaneClient as createProtocolControlPlaneClient,
} from "@ferrumq/protocol";

import { ExpectedCliError } from "./errors.js";

export type {
  ControlPlaneClient,
  FetchLike,
  ResponseLike,
} from "@ferrumq/protocol";

import type { ControlPlaneClient, FetchLike } from "@ferrumq/protocol";

export function createControlPlaneClient(
  controlUrl: string,
  fetchImpl?: FetchLike,
): ControlPlaneClient {
  const client =
    fetchImpl === undefined
      ? createProtocolControlPlaneClient(controlUrl)
      : createProtocolControlPlaneClient(controlUrl, fetchImpl);

  return wrapExpectedCliErrors(client);
}

function wrapExpectedCliErrors(client: ControlPlaneClient): ControlPlaneClient {
  return {
    health: () => withExpectedCliError(client.health()),
    ready: () => withExpectedCliError(client.ready()),
    status: () => withExpectedCliError(client.status()),
    createTopic: (name, partitions) =>
      withExpectedCliError(client.createTopic(name, partitions)),
    getTopic: (name) => withExpectedCliError(client.getTopic(name)),
    listTopics: () => withExpectedCliError(client.listTopics()),
    listDlq: (topic) => withExpectedCliError(client.listDlq(topic)),
    searchMessages: (request) =>
      withExpectedCliError(client.searchMessages(request)),
  };
}

async function withExpectedCliError<T>(promise: Promise<T>): Promise<T> {
  try {
    return await promise;
  } catch (error) {
    if (error instanceof ControlPlaneRequestError) {
      throw new ExpectedCliError(error.message);
    }

    throw error;
  }
}
