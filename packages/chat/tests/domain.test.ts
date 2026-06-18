import { describe, expect, it } from "vitest";
import {
  buildChatMessage,
  DeduplicationCache,
  DomainError,
  generateMessageId,
  generateParticipantId,
  generateSessionId,
  MAX_IDENTIFIER_LENGTH,
  MAX_MESSAGE_LENGTH,
  MAX_NAME_LENGTH,
  MAX_ROOM_LENGTH,
  MAX_TIMESTAMP_LENGTH,
  makeConsumerGroup,
  makeConsumerId,
  makeTopicName,
  parseChatMessage,
  sanitizeControlChars,
  sanitizeDisplay,
  stripAnsiEscapeSequences,
  toDisplayMessage,
  validateName,
  validateRoom,
  validateText,
} from "../src/domain.js";

describe("validateRoom", () => {
  it("accepts valid room names", () => {
    expect(validateRoom("a")).toBe("a");
    expect(validateRoom("general")).toBe("general");
    expect(validateRoom("my-room")).toBe("my-room");
    expect(validateRoom("room.name")).toBe("room.name");
    expect(validateRoom("room_name")).toBe("room_name");
    expect(validateRoom("a1")).toBe("a1");
    expect(validateRoom("-room")).toBe("-room");
    expect(validateRoom("room-")).toBe("room-");
    expect(validateRoom("_room")).toBe("_room");
    expect(validateRoom("room_")).toBe("room_");
    expect(validateRoom("-_room.name_-")).toBe("-_room.name_-");
  });

  it("lowercases and trims room names", () => {
    expect(validateRoom("  GENERAL  ")).toBe("general");
    expect(validateRoom("Room-Name")).toBe("room-name");
    expect(makeTopicName(validateRoom("  Room_Name.V1  "))).toBe(
      "chat.room_name.v1",
    );
  });

  it("rejects empty room names", () => {
    expect(() => validateRoom("")).toThrow(DomainError);
    expect(() => validateRoom("   ")).toThrow(DomainError);
  });

  it("enforces the 1-64 character room limit", () => {
    expect(validateRoom("a")).toBe("a");
    expect(validateRoom("a".repeat(64))).toBe("a".repeat(64));
    expect(() => validateRoom("a".repeat(65))).toThrow(DomainError);
  });

  it("rejects room names with special characters", () => {
    expect(() => validateRoom("hello world")).toThrow(DomainError);
    expect(() => validateRoom("hello!")).toThrow(DomainError);
    expect(() => validateRoom("hello@world")).toThrow(DomainError);
    expect(() => validateRoom("salação")).toThrow(DomainError);
  });

  it("rejects broker-invalid dot placement", () => {
    expect(() => validateRoom(".room")).toThrow(DomainError);
    expect(() => validateRoom("room.")).toThrow(DomainError);
    expect(() => validateRoom("room..name")).toThrow(DomainError);
    expect(() => validateRoom("..")).toThrow(DomainError);
    expect(validateRoom("room.name")).toBe("room.name");
    expect(validateRoom("room.-_name")).toBe("room.-_name");
  });
});

describe("validateName", () => {
  it("accepts valid display names", () => {
    expect(validateName("a")).toBe("a");
    expect(validateName("D")).toBe("D");
    expect(validateName("7")).toBe("7");
    expect(validateName("davi")).toBe("davi");
    expect(validateName("Alice.Smith")).toBe("Alice.Smith");
    expect(validateName("user_name-123")).toBe("user_name-123");
    expect(validateName("a".repeat(32))).toBe("a".repeat(32));
    expect(validateName("  a  ")).toBe("a");
  });

  it("rejects empty display names", () => {
    expect(() => validateName("")).toThrow(DomainError);
    expect(() => validateName("   ")).toThrow(DomainError);
  });

  it("rejects display names that are too long", () => {
    const long = "a".repeat(33);
    expect(() => validateName(long)).toThrow(DomainError);
  });

  it("rejects display names with special characters", () => {
    expect(() => validateName("hello!")).toThrow(DomainError);
    expect(() => validateName("name with spaces")).toThrow(DomainError);
    expect(() => validateName("-alice")).toThrow(DomainError);
    expect(() => validateName("alice-")).toThrow(DomainError);
    expect(() => validateName(".")).toThrow(DomainError);
  });
});

describe("validateText", () => {
  it("accepts valid message text", () => {
    expect(validateText("hello")).toBe("hello");
    expect(validateText("  hello world  ")).toBe("hello world");
  });

  it("rejects empty message text", () => {
    expect(() => validateText("")).toThrow(DomainError);
    expect(() => validateText("   ")).toThrow(DomainError);
  });

  it("rejects text that is too long", () => {
    const long = "a".repeat(4097);
    expect(() => validateText(long)).toThrow(DomainError);
  });
});

describe("sanitizeControlChars", () => {
  it("removes control characters", () => {
    expect(sanitizeControlChars("hello\x00world")).toBe("helloworld");
    expect(sanitizeControlChars("test\x1fstring")).toBe("teststring");
    expect(sanitizeControlChars("\x07bell")).toBe("bell");
    expect(sanitizeControlChars("a\nb\rc\td")).toBe("abcd");
    expect(sanitizeControlChars(`a${String.fromCharCode(0x7f)}b`)).toBe("ab");
    expect(sanitizeControlChars(`a${String.fromCharCode(0x80)}b`)).toBe("ab");
    expect(sanitizeControlChars(`a${String.fromCharCode(0x9f)}b`)).toBe("ab");
  });

  it("preserves normal text", () => {
    expect(sanitizeControlChars("hello world 123")).toBe("hello world 123");
  });

  it("preserves unicode", () => {
    expect(sanitizeControlChars("olá mundo")).toBe("olá mundo");
    expect(sanitizeControlChars("🎉 party")).toBe("🎉 party");
  });
});

describe("stripAnsiEscapeSequences", () => {
  it("removes ANSI escape sequences", () => {
    expect(stripAnsiEscapeSequences("\x1b[31mred\x1b[0m")).toBe("red");
    expect(stripAnsiEscapeSequences("\x1b[1;32mbold green\x1b[0m")).toBe(
      "bold green",
    );
    expect(stripAnsiEscapeSequences("\x9b31mred\x9b0m")).toBe("red");
  });

  it("preserves normal text", () => {
    expect(stripAnsiEscapeSequences("normal text")).toBe("normal text");
  });

  it("preserves unicode and emoji", () => {
    expect(stripAnsiEscapeSequences("texto com 🎉 emoji")).toBe(
      "texto com 🎉 emoji",
    );
  });
});

describe("sanitizeDisplay", () => {
  it("removes both ANSI and control chars", () => {
    expect(sanitizeDisplay("\x1b[31mhello\x00world\x1b[0m")).toBe("helloworld");
    expect(sanitizeDisplay("one\ntwo\rthree\tfour")).toBe("onetwothreefour");
    expect(sanitizeDisplay("\x9b31mred\x9b0m")).toBe("red");
  });

  it("preserves accented text and emoji", () => {
    expect(sanitizeDisplay("olá, ação 🎉")).toBe("olá, ação 🎉");
  });
});

describe("buildChatMessage", () => {
  it("builds a valid chat message", () => {
    const msg = buildChatMessage(
      {
        id: "participant-1",
        name: "Alice",
        sessionId: "session-1",
      },
      "general",
      "hello world",
    );

    expect(msg.version).toBe(1);
    expect(msg.room).toBe("general");
    expect(msg.sender.id).toBe("participant-1");
    expect(msg.sender.name).toBe("Alice");
    expect(msg.sender.sessionId).toBe("session-1");
    expect(msg.text).toBe("hello world");
    expect(msg.id).toBeTruthy();
    expect(msg.sentAt).toBeTruthy();
  });

  it("throws on invalid room", () => {
    expect(() =>
      buildChatMessage(
        { id: "p1", name: "Alice", sessionId: "s1" },
        "###",
        "hello",
      ),
    ).toThrow(DomainError);
  });

  it("throws on empty text", () => {
    expect(() =>
      buildChatMessage(
        { id: "p1", name: "Alice", sessionId: "s1" },
        "general",
        "",
      ),
    ).toThrow(DomainError);
  });
});

describe("parseChatMessage", () => {
  const validMessage = {
    version: 1,
    id: "msg-123",
    room: "general",
    sender: {
      id: "participant-1",
      name: "Alice",
      sessionId: "session-1",
    },
    text: "hello world",
    sentAt: "2025-01-01T00:00:00.000Z",
  };
  const validJson = JSON.stringify(validMessage);

  function parseWith(
    update: (message: typeof validMessage) => void,
  ): ReturnType<typeof parseChatMessage> {
    const message = structuredClone(validMessage);
    update(message);
    return parseChatMessage(JSON.stringify(message));
  }

  it("parses a valid chat message", () => {
    const result = parseChatMessage(validJson);
    expect("kind" in result).toBe(false);
    if (!("kind" in result)) {
      expect(result.version).toBe(1);
      expect(result.id).toBe("msg-123");
      expect(result.room).toBe("general");
      expect(result.text).toBe("hello world");
    }
  });

  it("sanitizes sender name and text", () => {
    const malicious = JSON.stringify({
      version: 1,
      id: "msg-1",
      room: "general",
      sender: {
        id: "p1",
        name: "\x1b[31mEvil\x1b[0m",
        sessionId: "s1",
      },
      text: "hello\x00world",
      sentAt: "2025-01-01T00:00:00.000Z",
    });
    const result = parseChatMessage(malicious);
    if (!("kind" in result)) {
      expect(result.sender.name).toBe("Evil");
      expect(result.text).toBe("helloworld");
    }
  });

  it("neutralizes hostile sender and message display payloads", () => {
    const malicious = JSON.stringify({
      version: 1,
      id: "msg-1",
      room: "general",
      sender: {
        id: "p1",
        name: "\x9b31mEvil\nUser\x9b0m",
        sessionId: "s1",
      },
      text: "\x1b[2Jolá\r\tmundo 🎉\x1b[0m",
      sentAt: "2025-01-01T00:00:00.000Z",
    });

    const result = parseChatMessage(malicious);
    expect("kind" in result).toBe(false);
    if (!("kind" in result)) {
      expect(result.sender.name).toBe("EvilUser");
      expect(result.text).toBe("olámundo 🎉");
    }
  });

  it("rejects invalid JSON", () => {
    const result = parseChatMessage("not json");
    expect("kind" in result && result.kind === "invalid-json").toBe(true);
  });

  it("rejects unsupported version", () => {
    const result = parseChatMessage(
      JSON.stringify({
        version: 2,
        id: "x",
        room: "g",
        sender: {},
        text: "t",
        sentAt: "now",
      }),
    );
    expect("kind" in result && result.kind === "unsupported-version").toBe(
      true,
    );
  });

  it("rejects null", () => {
    const result = parseChatMessage("null");
    expect("kind" in result && result.kind === "not-object").toBe(true);
  });

  it("rejects missing sender", () => {
    const result = parseChatMessage(
      JSON.stringify({
        version: 1,
        id: "x",
        room: "g",
        text: "t",
        sentAt: "now",
      }),
    );
    expect("kind" in result && result.kind === "missing-sender").toBe(true);
  });

  it("rejects missing text", () => {
    const result = parseChatMessage(
      JSON.stringify({
        version: 1,
        id: "x",
        room: "g",
        sender: { id: "x", name: "x", sessionId: "x" },
        sentAt: "now",
      }),
    );
    expect("kind" in result && result.kind === "missing-text").toBe(true);
  });

  it("rejects missing id", () => {
    const result = parseChatMessage(
      JSON.stringify({
        version: 1,
        room: "g",
        sender: { id: "x", name: "x", sessionId: "x" },
        text: "t",
        sentAt: "now",
      }),
    );
    expect("kind" in result && result.kind === "missing-id").toBe(true);
  });

  it.each([
    ["C0-only", "\x00\x1f\x7f"],
    ["C1-only", "\x80\x9f"],
    ["ANSI-only", "\x1b[31m\x1b[0m"],
  ])("rejects an ID containing only %s characters", (_label, id) => {
    const result = parseChatMessage(
      JSON.stringify({
        version: 1,
        id,
        room: "g",
        sender: { id: "x", name: "x", sessionId: "x" },
        text: "t",
        sentAt: "now",
      }),
    );
    expect(result).toEqual({ kind: "invalid-id" });
  });

  it("accepts an ID with visible content after sanitization", () => {
    const result = parseChatMessage(
      JSON.stringify({
        version: 1,
        id: "\x00\x1b[31mmsg-1\x1b[0m\x9f",
        room: "g",
        sender: { id: "x", name: "x", sessionId: "x" },
        text: "t",
        sentAt: "now",
      }),
    );
    expect("kind" in result).toBe(false);
    if (!("kind" in result)) {
      expect(result.id).toBe("msg-1");
    }
  });

  it.each([
    ["id", "invalid-id", MAX_IDENTIFIER_LENGTH + 1],
    ["room", "invalid-room", MAX_ROOM_LENGTH + 1],
    ["sender.id", "invalid-sender-id", MAX_IDENTIFIER_LENGTH + 1],
    ["sender.name", "invalid-sender-name", MAX_NAME_LENGTH + 1],
    [
      "sender.sessionId",
      "invalid-sender-session-id",
      MAX_IDENTIFIER_LENGTH + 1,
    ],
    ["text", "invalid-text", MAX_MESSAGE_LENGTH + 1],
    ["sentAt", "invalid-sent-at", MAX_TIMESTAMP_LENGTH + 1],
  ] as const)("rejects oversized raw %s instead of truncating it", (field, kind, length) => {
    const result = parseWith((message) => {
      if (field === "sender.id") {
        message.sender.id = "a".repeat(length);
      } else if (field === "sender.name") {
        message.sender.name = "a".repeat(length);
      } else if (field === "sender.sessionId") {
        message.sender.sessionId = "a".repeat(length);
      } else {
        message[field] = "a".repeat(length);
      }
    });

    expect(result).toEqual({ kind });
  });

  it.each([
    ["id", "invalid-id"],
    ["room", "invalid-room"],
    ["sender.id", "invalid-sender-id"],
    ["sender.name", "invalid-sender-name"],
    ["sender.sessionId", "invalid-sender-session-id"],
    ["text", "invalid-text"],
    ["sentAt", "invalid-sent-at"],
  ] as const)("rejects %s when sanitization removes all content", (field, kind) => {
    const result = parseWith((message) => {
      if (field === "sender.id") {
        message.sender.id = "\x1b[31m\x1b[0m";
      } else if (field === "sender.name") {
        message.sender.name = "\x1b[31m\x1b[0m";
      } else if (field === "sender.sessionId") {
        message.sender.sessionId = "\x1b[31m\x1b[0m";
      } else {
        message[field] = "\x1b[31m\x1b[0m";
      }
    });

    expect(result).toEqual({ kind });
  });

  it("sanitizes every required string before returning it", () => {
    const result = parseChatMessage(
      JSON.stringify({
        version: 1,
        id: "\x00msg-1\x9f",
        room: "\x1b[31m General \x1b[0m",
        sender: {
          id: "\x00participante-á\x9f",
          name: "\x1b[31m Alice \x1b[0m",
          sessionId: "\x00sessão-🎉\x9f",
        },
        text: "\x00 olá, ação 🎉 \x9f",
        sentAt: "\x002025-01-01T00:00:00.000Z\x9f",
      }),
    );

    expect(result).toEqual({
      version: 1,
      id: "msg-1",
      room: "general",
      sender: {
        id: "participante-á",
        name: "Alice",
        sessionId: "sessão-🎉",
      },
      text: "olá, ação 🎉",
      sentAt: "2025-01-01T00:00:00.000Z",
    });
  });

  it.each([
    ["room", "salação", "invalid-room"],
    ["room", "general🎉", "invalid-room"],
    ["sender.name", "João", "invalid-sender-name"],
    ["sender.name", "Alice🎉", "invalid-sender-name"],
  ] as const)("retains ASCII-only validation for %s", (field, value, kind) => {
    const result = parseWith((message) => {
      if (field === "sender.name") {
        message.sender.name = value;
      } else {
        message.room = value;
      }
    });

    expect(result).toEqual({ kind });
  });

  it("applies field-specific whitespace contracts after sanitization", () => {
    expect(
      parseWith((message) => {
        message.room = "  ";
      }),
    ).toEqual({ kind: "invalid-room" });
    expect(
      parseWith((message) => {
        message.sender.name = "  ";
      }),
    ).toEqual({ kind: "invalid-sender-name" });
    expect(
      parseWith((message) => {
        message.text = "  ";
      }),
    ).toEqual({ kind: "invalid-text" });

    const result = parseWith((message) => {
      message.text = "  olá 🎉  ";
      message.sentAt = "not-an-iso-timestamp";
    });
    expect("kind" in result).toBe(false);
    if (!("kind" in result)) {
      expect(result.text).toBe("olá 🎉");
      expect(result.sentAt).toBe("not-an-iso-timestamp");
    }
  });
});

describe("toDisplayMessage", () => {
  const chatMsg = {
    version: 1 as const,
    id: "msg-1",
    room: "general",
    sender: {
      id: "p1",
      name: "Alice",
      sessionId: "session-1",
    },
    text: "hello",
    sentAt: "2025-01-01T12:00:00.000Z",
  };

  it("creates display message", () => {
    const display = toDisplayMessage(chatMsg);
    expect(display.id).toBe("msg-1");
    expect(display.senderName).toBe("Alice");
    expect(display.text).toBe("hello");
    expect(display.timestamp).toBeInstanceOf(Date);
  });

  it("marks self messages", () => {
    const display = toDisplayMessage(chatMsg, "session-1");
    expect(display.isSelf).toBe(true);
  });

  it("does not mark other messages as self", () => {
    const display = toDisplayMessage(chatMsg, "other-session");
    expect(display.isSelf).toBe(false);
  });
});

describe("DeduplicationCache", () => {
  it("tracks seen IDs", () => {
    const cache = new DeduplicationCache(100);
    expect(cache.has("msg-1")).toBe(false);
    cache.add("msg-1");
    expect(cache.has("msg-1")).toBe(true);
  });

  it("evicts oldest entry when full", () => {
    const cache = new DeduplicationCache(3);
    cache.add("a");
    cache.add("b");
    cache.add("c");
    cache.add("d");
    expect(cache.has("a")).toBe(false);
    expect(cache.has("b")).toBe(true);
    expect(cache.has("c")).toBe(true);
    expect(cache.has("d")).toBe(true);
  });

  it("refreshes cache hits before evicting the least-recently-used entry", () => {
    const cache = new DeduplicationCache(3);
    cache.add("a");
    cache.add("b");
    cache.add("c");

    expect(cache.has("a")).toBe(true);
    cache.add("d");

    expect(cache.has("a")).toBe(true);
    expect(cache.has("b")).toBe(false);
    expect(cache.has("c")).toBe(true);
    expect(cache.has("d")).toBe(true);
  });

  it("can be cleared", () => {
    const cache = new DeduplicationCache(100);
    cache.add("msg-1");
    cache.add("msg-2");
    cache.clear();
    expect(cache.has("msg-1")).toBe(false);
    expect(cache.has("msg-2")).toBe(false);
    expect(cache.size).toBe(0);
  });
});

describe("identity generation", () => {
  it("generates unique session IDs", () => {
    const ids = new Set(Array.from({ length: 10 }, () => generateSessionId()));
    expect(ids.size).toBe(10);
  });

  it("generates unique participant IDs", () => {
    const ids = new Set(
      Array.from({ length: 10 }, () => generateParticipantId()),
    );
    expect(ids.size).toBe(10);
  });

  it("generates unique message IDs", () => {
    const ids = new Set(Array.from({ length: 10 }, () => generateMessageId()));
    expect(ids.size).toBe(10);
  });
});

describe("topic and group naming", () => {
  it("creates topic name from room", () => {
    expect(makeTopicName("general")).toBe("chat.general");
    expect(makeTopicName("my-room")).toBe("chat.my-room");
  });

  it("creates consumer group from room and session", () => {
    const group = makeConsumerGroup("general", "abc-123");
    expect(group).toBe("chat.general.session.abc-123");
  });

  it("creates consumer ID from session", () => {
    const id = makeConsumerId("abc-123");
    expect(id).toBe("chat-session-abc-123");
  });
});
