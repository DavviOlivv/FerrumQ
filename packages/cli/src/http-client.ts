import {
  brokerStatusResponseSchema,
  dlqListResponseSchema,
  ferrumQErrorEnvelopeSchema,
  httpStatusResponseSchema,
  topicListResponseSchema,
  topicResponseSchema,
} from "@ferrumq/protocol";
import { ZodError, type ZodType } from "zod";

import { ExpectedCliError, errorMessage } from "./errors.js";

import type {
  BrokerStatusResponse,
  DlqListResponse,
  HttpStatusResponse,
  TopicListResponse,
  TopicResponse,
} from "@ferrumq/protocol";

export type FetchLike = (
  input: string,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
  },
) => Promise<ResponseLike>;

export interface ResponseLike {
  ok: boolean;
  status: number;
  statusText: string;
  json(): Promise<unknown>;
}

export interface ControlPlaneClient {
  health(): Promise<HttpStatusResponse>;
  ready(): Promise<HttpStatusResponse>;
  status(): Promise<BrokerStatusResponse>;
  createTopic(name: string, partitions: number): Promise<TopicResponse>;
  getTopic(name: string): Promise<TopicResponse>;
  listTopics(): Promise<TopicListResponse>;
  listDlq(topic?: string): Promise<DlqListResponse>;
}

export function createControlPlaneClient(
  controlUrl: string,
  fetchImpl: FetchLike = fetch as FetchLike,
): ControlPlaneClient {
  return {
    health: () =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        "/health",
        httpStatusResponseSchema,
      ),
    ready: () =>
      request(controlUrl, fetchImpl, "GET", "/ready", httpStatusResponseSchema),
    status: () =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        "/v1/status",
        brokerStatusResponseSchema,
      ),
    createTopic: (name, partitions) =>
      request(
        controlUrl,
        fetchImpl,
        "POST",
        "/v1/topics",
        topicResponseSchema,
        {
          name,
          partitions,
        },
      ),
    getTopic: (name) =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        `/v1/topics/${encodeURIComponent(name)}`,
        topicResponseSchema,
      ),
    listTopics: () =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        "/v1/topics",
        topicListResponseSchema,
      ),
    listDlq: (topic) =>
      request(
        controlUrl,
        fetchImpl,
        "GET",
        topic === undefined
          ? "/v1/dlq"
          : `/v1/dlq?topic=${encodeURIComponent(topic)}`,
        dlqListResponseSchema,
      ),
  };
}

async function request<T>(
  controlUrl: string,
  fetchImpl: FetchLike,
  method: string,
  path: string,
  schema: ZodType<T>,
  body?: unknown,
): Promise<T> {
  const url = buildUrl(controlUrl, path);
  let response: ResponseLike;
  try {
    const init: {
      method: string;
      headers?: Record<string, string>;
      body?: string;
    } = { method };
    if (body !== undefined) {
      init.headers = { "content-type": "application/json" };
      init.body = JSON.stringify(body);
    }
    response = await fetchImpl(url, init);
  } catch (error) {
    throw new ExpectedCliError(
      `Network request failed for ${method} ${url}: ${errorMessage(error)}`,
    );
  }

  const payload = await readJson(response);
  if (!response.ok) {
    const envelope = ferrumQErrorEnvelopeSchema.safeParse(payload);
    if (envelope.success) {
      throw new ExpectedCliError(
        `HTTP ${response.status} ${envelope.data.error.code}: ${envelope.data.error.message}`,
      );
    }

    throw new ExpectedCliError(
      `HTTP ${response.status}: ${response.statusText || "request failed"}`,
    );
  }

  try {
    return schema.parse(payload);
  } catch (error) {
    if (error instanceof ZodError) {
      throw new ExpectedCliError(
        `Unexpected response from control API: ${error.issues[0]?.message}`,
      );
    }
    throw error;
  }
}

function buildUrl(controlUrl: string, path: string): string {
  return new URL(path, `${controlUrl}/`).toString();
}

async function readJson(response: ResponseLike): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return undefined;
  }
}
