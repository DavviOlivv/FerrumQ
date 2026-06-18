import { randomUUID } from "node:crypto";

export const CHAT_MESSAGE_VERSION = 1;
export const CHAT_MESSAGE_TYPE = "ferrumq.chat.message.v1";

export const MAX_ROOM_LENGTH = 64;
export const MAX_NAME_LENGTH = 32;
export const MAX_MESSAGE_LENGTH = 4096;
export const MAX_IDENTIFIER_LENGTH = 128;
export const MAX_TIMESTAMP_LENGTH = 128;

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
  // biome-ignore lint/suspicious/noControlCharactersInRegex: intentional sanitization regex
  return value.replace(/[\x00-\x1f\x7f-\x9f]/g, "");
}

export function stripAnsiEscapeSequences(value: string): string {
  return (
    value
      // biome-ignore lint/suspicious/noControlCharactersInRegex: intentional ANSI CSI sanitization
      .replace(/(?:\u001b\[|\u009b)[0-?]*[ -/]*[@-~]/g, "")
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
): ChatMessageV1 | MalformedMessage {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { kind: "invalid-json" };
  }

  if (parsed === null || typeof parsed !== "object") {
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

  if (obj.sender === null || typeof obj.sender !== "object") {
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

  const sanitizedId = sanitizeDisplay(obj.id);
  const sanitizedRoom = sanitizeDisplay(obj.room);
  const sanitizedSenderId = sanitizeDisplay(sender.id as string);
  const sanitizedSenderName = sanitizeDisplay(sender.name as string);
  const sanitizedSessionId = sanitizeDisplay(sender.sessionId as string);
  const sanitizedText = sanitizeDisplay(obj.text);
  const sanitizedSentAt = sanitizeDisplay(obj.sentAt);

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
    sentAt: sanitizedSentAt,
  };
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
  | { kind: "room-mismatch" }
  | { kind: "parse-error" };

export class DomainError extends Error {
  override name = "DomainError";
}

export class DeduplicationCache {
  private readonly map = new Map<string, number>();
  private readonly maxSize: number;

  constructor(maxSize = 2048) {
    this.maxSize = maxSize;
  }

  has(id: string): boolean {
    const seenAt = this.map.get(id);
    if (seenAt === undefined) {
      return false;
    }

    this.map.delete(id);
    this.map.set(id, seenAt);
    return true;
  }

  add(id: string): void {
    if (this.map.size >= this.maxSize) {
      const first = this.map.keys().next();
      if (!first.done && first.value !== undefined) {
        this.map.delete(first.value);
      }
    }
    this.map.set(id, Date.now());
  }

  clear(): void {
    this.map.clear();
  }

  get size(): number {
    return this.map.size;
  }
}
