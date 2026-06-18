export type FerrumQTransport = "http" | "grpc" | "sdk";

export interface FerrumQErrorOptions {
  code?: string;
  status?: number;
  transport: FerrumQTransport;
  grpcStatus?: string;
  operation?: string;
  topic?: string;
  deliveryId?: string;
  cause?: unknown;
}

export class FerrumQError extends Error {
  readonly code: string | undefined;
  readonly status: number | undefined;
  readonly transport: FerrumQTransport;
  readonly grpcStatus: string | undefined;
  readonly operation: string | undefined;
  readonly topic: string | undefined;
  readonly deliveryId: string | undefined;

  constructor(message: string, options: FerrumQErrorOptions) {
    super(message, { cause: options.cause });
    this.name = "FerrumQError";
    this.code = options.code;
    this.status = options.status;
    this.transport = options.transport;
    this.grpcStatus = options.grpcStatus;
    this.operation = options.operation;
    this.topic = options.topic;
    this.deliveryId = options.deliveryId;
  }
}
