import { validateGrpcUrl, validateHttpUrl } from "./validation.js";

export const cliVersion = "0.1.0";
export const defaultControlUrl = "http://127.0.0.1:8080";
export const defaultGrpcUrl = "http://127.0.0.1:9090";
export const defaultConsumerId = "ferrumq-cli";
export const defaultPublishContentType = "application/json";
export const defaultPublishType = "ferrumq.cli.message";
export const defaultPublishSource = "ferrumq-cli";

export interface GlobalOptions {
  controlUrl?: string;
  grpcUrl?: string;
  json: boolean;
}

export interface ResolvedConfig {
  controlUrl: string;
  grpcUrl: string;
  json: boolean;
}

export interface CliEnvironment {
  FERRUMQ_CONTROL_URL?: string;
  FERRUMQ_GRPC_URL?: string;
}

export function resolveConfig(
  globals: GlobalOptions,
  env: CliEnvironment = {},
): ResolvedConfig {
  const controlUrl =
    globals.controlUrl ?? env.FERRUMQ_CONTROL_URL ?? defaultControlUrl;
  const grpcUrl = globals.grpcUrl ?? env.FERRUMQ_GRPC_URL ?? defaultGrpcUrl;

  return {
    controlUrl: validateHttpUrl(controlUrl, "control URL"),
    grpcUrl: validateGrpcUrl(grpcUrl, "gRPC URL"),
    json: globals.json,
  };
}
