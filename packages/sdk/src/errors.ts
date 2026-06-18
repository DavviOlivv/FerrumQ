export type FerrumQTransport = "http" | "grpc" | "sdk";

export interface FerrumQErrorOptions {
  code?: string;
  status?: number;
  transport: FerrumQTransport;
  cause?: unknown;
}

export class FerrumQError extends Error {
  readonly code: string | undefined;
  readonly status: number | undefined;
  readonly transport: FerrumQTransport;

  constructor(message: string, options: FerrumQErrorOptions) {
    super(message, { cause: options.cause });
    this.name = "FerrumQError";
    this.code = options.code;
    this.status = options.status;
    this.transport = options.transport;
  }
}
