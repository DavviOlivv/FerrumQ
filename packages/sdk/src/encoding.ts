import { FerrumQError } from "./errors.js";

export interface EncodedPayload {
  data: Uint8Array;
  contentType: string;
}

export function encodePayload(payload: unknown): EncodedPayload {
  if (typeof payload === "string") {
    return {
      data: new TextEncoder().encode(payload),
      contentType: "text/plain",
    };
  }

  if (payload instanceof Uint8Array) {
    return { data: payload, contentType: "application/octet-stream" };
  }

  if (
    payload === null ||
    typeof payload === "boolean" ||
    typeof payload === "number" ||
    typeof payload === "object"
  ) {
    let serialized: string | undefined;
    try {
      serialized = JSON.stringify(payload);
    } catch (error) {
      throw new FerrumQError("Failed to serialize payload as JSON", {
        transport: "sdk",
        cause: error,
      });
    }

    if (typeof serialized !== "string") {
      throw new FerrumQError(
        "Failed to serialize payload as JSON: serialization returned undefined",
        { transport: "sdk" },
      );
    }

    return {
      data: new TextEncoder().encode(serialized),
      contentType: "application/json",
    };
  }

  throw new FerrumQError(
    `Unsupported payload type: ${typeof payload}. Supported types: string, Uint8Array, Buffer, and JSON-compatible values.`,
    { transport: "sdk" },
  );
}
