import { createHash, randomUUID } from "node:crypto";

export const CHAT_MESSAGE_VERSION = 1;
export const CHAT_MESSAGE_TYPE = "ferrumq.chat.message.v1";

export const MAX_ROOM_LENGTH = 64;
export const MAX_NAME_LENGTH = 32;
export const MAX_MESSAGE_LENGTH = 4096;
export const MAX_IDENTIFIER_LENGTH = 128;
export const MAX_TIMESTAMP_LENGTH = 128;
export const MAX_CHAT_PAYLOAD_BYTES = 32 * 1024;
export const MAX_FUTURE_TIMESTAMP_MS = 5 * 60 * 1000;

export const ROOM_PATTERN = /^[a-z0-9._-]+$/;
export const NAME_PATTERN = /^[a-zA-Z0-9](?:[a-zA-Z0-9._-]{0,30}[a-zA-Z0-9])?$/;

export const TOPIC_PREFIX = "chat.";

export interface ChatSender {
  id: string;
  name: string;
  sessionId: string;
}

export interface ChatMessageV1 {
  version: 1;
  id: string;
  room: string;
  sender: ChatSender;
  text: string;
  sentAt: string;
}

export interface DisplayMessage {
  id: string;
  senderName: string;
  text: string;
  timestamp: Date;
  isSelf: boolean;
}

export interface ParticipantIdentity {
  id: string;
  name: string;
  sessionId: string;
}

export function generateSessionId(): string {
  return randomUUID();
}

export function generateParticipantId(): string {
  return randomUUID();
}

export function generateMessageId(): string {
  return randomUUID();
}

export function makeConsumerGroup(room: string, sessionId: string): string {
  return `chat.${room}.session.${sessionId}`;
}

export function makeConsumerId(sessionId: string): string {
  return `chat-session-${sessionId}`;
}

export function makeTopicName(room: string): string {
  return `${TOPIC_PREFIX}${room}`;
}

export function validateRoom(raw: string): string {
  const trimmed = raw.trim().toLowerCase();
  if (trimmed.length === 0) {
    throw new DomainError("room name must not be empty");
  }
  if (trimmed.length > MAX_ROOM_LENGTH) {
    throw new DomainError(
      `room name must be at most ${MAX_ROOM_LENGTH} characters`,
    );
  }
  if (!ROOM_PATTERN.test(trimmed)) {
    throw new DomainError(
      "room name may contain only ASCII letters, digits, dots, hyphens, and underscores",
    );
  }
  if (trimmed.startsWith(".") || trimmed.endsWith(".")) {
    throw new DomainError("room name must not start or end with a dot");
  }
  if (trimmed.includes("..")) {
    throw new DomainError("room name must not contain consecutive dots");
  }
  return trimmed;
}

export function validateName(raw: string): string {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    throw new DomainError("display name must not be empty");
  }
  if (trimmed.length > MAX_NAME_LENGTH) {
    throw new DomainError(
      `display name must be at most ${MAX_NAME_LENGTH} characters`,
    );
  }
  if (!NAME_PATTERN.test(trimmed)) {
    throw new DomainError(
      "display name must start and end with alphanumeric and may contain dots, hyphens, and underscores",
    );
  }
  return trimmed;
}

export function validateText(raw: string): string {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    throw new DomainError("message text must not be empty");
  }
  if (trimmed.length > MAX_MESSAGE_LENGTH) {
    throw new DomainError(
      `message text must be at most ${MAX_MESSAGE_LENGTH} characters`,
    );
  }
  return trimmed;
}

export function sanitizeControlChars(value: string): string {
  const cleaned = value
    // biome-ignore lint/suspicious/noControlCharactersInRegex: intentional C0/C1 sanitization regex
    .replace(/[\x00-\x1f\x7f-\x9f]/g, "")
    .replace(/[\u061c\u200e\u200f\u202a-\u202e\u2066-\u2069]/g, "")
    .replace(/[\u200b\u2060\ufeff]/g, "");

  return cleaned.replace(/[\u200c\u200d]/g, "").trim().length === 0
    ? cleaned.replace(/[\u200c\u200d]/g, "")
    : cleaned;
}

export function stripAnsiEscapeSequences(value: string): string {
  return (
    value
      // biome-ignore lint/suspicious/noControlCharactersInRegex: intentional ANSI CSI sanitization
      .replace(/(?:\u001b\[|\u009b)[0-?]*[ -/]*[@-~]/g, "")
      // biome-ignore lint/suspicious/noControlCharactersInRegex: intentional ANSI OSC sanitization
      .replace(/(?:\u001b\]|\u009d)[\s\S]*?(?:\u0007|\u001b\\|\u009c|$)/g, "")
      // biome-ignore lint/suspicious/noControlCharactersInRegex: intentional two-byte ANSI escape sanitization
      .replace(/\u001b[ -/][@-~]/g, "")
      // biome-ignore lint/suspicious/noControlCharactersInRegex: intentional ANSI escape sanitization
      .replace(/\u001b/g, "")
  );
}

export function sanitizeDisplay(value: string): string {
  let cleaned = stripAnsiEscapeSequences(value);
  cleaned = sanitizeControlChars(cleaned);
  return cleaned;
}

export function buildChatMessage(
  participant: ParticipantIdentity,
  room: string,
  text: string,
): ChatMessageV1 {
  const validatedRoom = validateRoom(room);
  const validatedText = validateText(text);
  return {
    version: 1,
    id: generateMessageId(),
    room: validatedRoom,
    sender: {
      id: participant.id,
      name: participant.name,
      sessionId: participant.sessionId,
    },
    text: validatedText,
    sentAt: new Date().toISOString(),
  };
}

export function parseChatMessage(
  raw: string,
  now: Date = new Date(),
): ChatMessageV1 | MalformedMessage {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { kind: "invalid-json" };
  }

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { kind: "not-object" };
  }

  const obj = parsed as Record<string, unknown>;

  if (obj.version !== 1) {
    return { kind: "unsupported-version", version: obj.version };
  }

  if (typeof obj.id !== "string" || obj.id.length === 0) {
    return { kind: "missing-id" };
  }

  if (typeof obj.room !== "string" || obj.room.length === 0) {
    return { kind: "missing-room" };
  }

  if (
    obj.sender === null ||
    typeof obj.sender !== "object" ||
    Array.isArray(obj.sender)
  ) {
    return { kind: "missing-sender" };
  }

  const sender = obj.sender as Record<string, unknown>;
  if (typeof sender.id !== "string" || sender.id.length === 0) {
    return { kind: "missing-sender-id" };
  }
  if (typeof sender.name !== "string" || sender.name.length === 0) {
    return { kind: "missing-sender-name" };
  }
  if (typeof sender.sessionId !== "string" || sender.sessionId.length === 0) {
    return { kind: "missing-sender-session-id" };
  }

  if (typeof obj.text !== "string" || obj.text.length === 0) {
    return { kind: "missing-text" };
  }

  if (typeof obj.sentAt !== "string" || obj.sentAt.length === 0) {
    return { kind: "missing-sent-at" };
  }

  if (obj.id.length > MAX_IDENTIFIER_LENGTH) {
    return { kind: "invalid-id" };
  }
  if (obj.room.length > MAX_ROOM_LENGTH) {
    return { kind: "invalid-room" };
  }
  if ((sender.id as string).length > MAX_IDENTIFIER_LENGTH) {
    return { kind: "invalid-sender-id" };
  }
  if ((sender.name as string).length > MAX_NAME_LENGTH) {
    return { kind: "invalid-sender-name" };
  }
  if ((sender.sessionId as string).length > MAX_IDENTIFIER_LENGTH) {
    return { kind: "invalid-sender-session-id" };
  }
  if (obj.text.length > MAX_MESSAGE_LENGTH) {
    return { kind: "invalid-text" };
  }
  if (obj.sentAt.length > MAX_TIMESTAMP_LENGTH) {
    return { kind: "invalid-sent-at" };
  }

  const sanitizedId = sanitizeDisplay(obj.id).trim();
  const sanitizedRoom = sanitizeDisplay(obj.room);
  const sanitizedSenderId = sanitizeDisplay(sender.id as string).trim();
  const sanitizedSenderName = sanitizeDisplay(sender.name as string);
  const sanitizedSessionId = sanitizeDisplay(sender.sessionId as string).trim();
  const sanitizedText = sanitizeDisplay(obj.text);
  const sanitizedSentAt = sanitizeDisplay(obj.sentAt).trim();

  if (sanitizedId.length === 0) {
    return { kind: "invalid-id" };
  }
  if (
    sanitizedSenderId.length === 0 ||
    sanitizedSenderId.length > MAX_IDENTIFIER_LENGTH
  ) {
    return { kind: "invalid-sender-id" };
  }
  if (
    sanitizedSessionId.length === 0 ||
    sanitizedSessionId.length > MAX_IDENTIFIER_LENGTH
  ) {
    return { kind: "invalid-sender-session-id" };
  }
  if (sanitizedSentAt.length === 0) {
    return { kind: "invalid-sent-at" };
  }

  const sentAt = parseCanonicalTimestamp(sanitizedSentAt, now);
  if (sentAt === null) {
    return { kind: "invalid-sent-at" };
  }

  let validatedRoom: string;
  try {
    validatedRoom = validateRoom(sanitizedRoom);
  } catch {
    return { kind: "invalid-room" };
  }

  let validatedSenderName: string;
  try {
    validatedSenderName = validateName(sanitizedSenderName);
  } catch {
    return { kind: "invalid-sender-name" };
  }

  let validatedText: string;
  try {
    validatedText = validateText(sanitizedText);
  } catch {
    return { kind: "invalid-text" };
  }

  return {
    version: 1,
    id: sanitizedId,
    room: validatedRoom,
    sender: {
      id: sanitizedSenderId,
      name: validatedSenderName,
      sessionId: sanitizedSessionId,
    },
    text: validatedText,
    sentAt,
  };
}

export function parseChatPayload(
  payload: Uint8Array,
  now: Date = new Date(),
): ChatMessageV1 | MalformedMessage {
  if (payload.byteLength > MAX_CHAT_PAYLOAD_BYTES) {
    return { kind: "payload-too-large" };
  }

  let raw: string;
  try {
    raw = new TextDecoder("utf-8", { fatal: true }).decode(payload);
  } catch {
    return { kind: "invalid-utf8" };
  }

  return parseChatMessage(raw, now);
}

export function fingerprintChatMessage(message: ChatMessageV1): string {
  return createHash("sha256").update(JSON.stringify(message)).digest("hex");
}

export function toDisplayMessage(
  msg: ChatMessageV1,
  selfSessionId?: string,
): DisplayMessage {
  const timestamp = parseTimestamp(msg.sentAt);
  return {
    id: msg.id,
    senderName: sanitizeDisplay(msg.sender.name),
    text: sanitizeDisplay(msg.text),
    timestamp,
    isSelf:
      selfSessionId !== undefined
        ? msg.sender.sessionId === selfSessionId
        : false,
  };
}

function parseTimestamp(iso: string): Date {
  const parsed = new Date(iso);
  return Number.isNaN(parsed.getTime()) ? new Date() : parsed;
}

function parseCanonicalTimestamp(value: string, now: Date): string | null {
  const parsed = new Date(value);
  const timestampMs = parsed.getTime();
  if (
    Number.isNaN(timestampMs) ||
    parsed.toISOString() !== value ||
    timestampMs > now.getTime() + MAX_FUTURE_TIMESTAMP_MS
  ) {
    return null;
  }
  return value;
}

export type MalformedMessage =
  | { kind: "invalid-json" }
  | { kind: "not-object" }
  | { kind: "unsupported-version"; version: unknown }
  | { kind: "missing-id" }
  | { kind: "invalid-id" }
  | { kind: "missing-room" }
  | { kind: "invalid-room" }
  | { kind: "missing-sender" }
  | { kind: "missing-sender-id" }
  | { kind: "invalid-sender-id" }
  | { kind: "missing-sender-name" }
  | { kind: "invalid-sender-name" }
  | { kind: "missing-sender-session-id" }
  | { kind: "invalid-sender-session-id" }
  | { kind: "missing-text" }
  | { kind: "invalid-text" }
  | { kind: "missing-sent-at" }
  | { kind: "invalid-sent-at" }
  | { kind: "payload-too-large" }
  | { kind: "invalid-utf8" }
  | { kind: "room-mismatch" }
  | { kind: "parse-error" };

export class DomainError extends Error {
  override name = "DomainError";
}

export class DeduplicationCache {
  private readonly map = new Map<string, string | null>();
  private readonly maxSize: number;

  constructor(maxSize = 2048) {
    if (!Number.isInteger(maxSize) || maxSize <= 0) {
      throw new RangeError("deduplication cache capacity must be positive");
    }
    this.maxSize = maxSize;
  }

  has(id: string): boolean {
    if (!this.map.has(id)) {
      return false;
    }

    const fingerprint = this.map.get(id) ?? null;
    this.map.delete(id);
    this.map.set(id, fingerprint);
    return true;
  }

  observe(id: string, fingerprint: string): DeduplicationResult {
    if (!this.map.has(id)) {
      return "new";
    }

    const existing = this.map.get(id) ?? null;
    this.map.delete(id);
    this.map.set(id, existing);
    return existing === fingerprint ? "duplicate" : "conflict";
  }

  add(id: string, fingerprint: string | null = null): void {
    this.map.delete(id);
    if (this.map.size >= this.maxSize) {
      const first = this.map.keys().next();
      if (!first.done && first.value !== undefined) {
        this.map.delete(first.value);
      }
    }
    this.map.set(id, fingerprint);
  }

  clear(): void {
    this.map.clear();
  }

  get size(): number {
    return this.map.size;
  }
}

export type DeduplicationResult = "new" | "duplicate" | "conflict";
