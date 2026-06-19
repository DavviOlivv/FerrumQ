import { Box, Text, useApp, useInput } from "ink";
import {
  type ReactElement,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import type { ChatAppDeps, ChatAppOptions } from "./app.js";
import { ChatApp, type ChatState } from "./app.js";
import type { DisplayMessage } from "./domain.js";

export interface ChatUiConfig {
  httpUrl: string;
  grpcUrl: string;
  room: string;
  name: string;
  timeoutMs?: number;
  pollIntervalMs?: number;
}

export interface ChatUiProps {
  config: ChatUiConfig;
  onExit?: () => void;
  onShutdownReady?: (shutdown: () => Promise<void>) => void;
}

interface ChatAppGeneration {
  app: ChatApp;
  active: boolean;
  stopPromise: Promise<void> | null;
}

interface PendingSubmission {
  generation: ChatAppGeneration;
  snapshot: string;
}

export const MAX_MESSAGE_HISTORY = 500;
export const MAX_RENDERED_MESSAGES = 200;

export function appendMessageHistory(
  previous: DisplayMessage[],
  message: DisplayMessage,
): DisplayMessage[] {
  return [...previous.slice(-(MAX_MESSAGE_HISTORY - 1)), message];
}

export function ChatUi({
  config,
  onExit,
  onShutdownReady,
}: ChatUiProps): ReactElement {
  const { name, room, httpUrl, grpcUrl, timeoutMs, pollIntervalMs } = config;
  const { exit } = useApp();
  const [messages, setMessages] = useState<DisplayMessage[]>([]);
  const [input, setInput] = useState("");
  const [state, setState] = useState<ChatState>({ status: "disconnected" });
  const [error, setError] = useState<string | null>(null);
  const [warning, setWarning] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const mounted = useRef(true);
  const inputRef = useRef("");
  const activeGeneration = useRef<ChatAppGeneration | null>(null);
  const pendingSubmission = useRef<PendingSubmission | null>(null);
  const generations = useRef(new Set<ChatAppGeneration>());
  const latestStopPromise = useRef<Promise<void>>(Promise.resolve());
  const shutdownRequested = useRef(false);
  const shutdownPromise = useRef<Promise<void> | null>(null);

  const stopGeneration = useCallback(
    (generation: ChatAppGeneration): Promise<void> => {
      generation.active = false;
      if (generation.stopPromise === null) {
        generation.stopPromise = generation.app.stop();
      }
      return generation.stopPromise;
    },
    [],
  );

  const shutdown = useCallback((): Promise<void> => {
    shutdownRequested.current = true;
    if (shutdownPromise.current === null) {
      activeGeneration.current = null;
      shutdownPromise.current = Promise.all(
        [...generations.current].map((generation) =>
          stopGeneration(generation),
        ),
      ).then(() => {});
    }
    return shutdownPromise.current;
  }, [stopGeneration]);

  onShutdownReady?.(shutdown);

  useEffect(() => {
    return () => {
      mounted.current = false;
      shutdownRequested.current = true;
      pendingSubmission.current = null;
    };
  }, []);

  useEffect(() => {
    if (shutdownRequested.current) {
      return;
    }

    setMessages([]);
    inputRef.current = "";
    setInput("");
    setState({ status: "disconnected" });
    setError(null);
    setWarning(null);
    setSending(false);
    pendingSubmission.current = null;

    const previousStop = latestStopPromise.current;
    let generation: ChatAppGeneration;
    const isCurrent = (): boolean =>
      mounted.current &&
      !shutdownRequested.current &&
      generation.active &&
      activeGeneration.current === generation;
    const deps: ChatAppDeps = {
      onMessage: (msg: DisplayMessage) => {
        if (isCurrent()) {
          setMessages((prev) => appendMessageHistory(prev, msg));
        }
      },
      onStateChange: (s: ChatState) => {
        if (isCurrent()) {
          setState(s);
          if (s.status === "connected") {
            setError(null);
          }
        }
      },
      onError: (msg: string) => {
        if (isCurrent()) {
          setError(msg);
        }
      },
      onWarning: (msg: string | null) => {
        if (isCurrent()) {
          setWarning(msg);
        }
      },
    };

    const appOptions: ChatAppOptions = {
      httpUrl,
      grpcUrl,
      room,
      name,
    };
    if (timeoutMs !== undefined) {
      appOptions.timeoutMs = timeoutMs;
    }
    if (pollIntervalMs !== undefined) {
      appOptions.pollIntervalMs = pollIntervalMs;
    }

    const app = new ChatApp(appOptions, deps);
    generation = {
      app,
      active: true,
      stopPromise: null,
    };
    generations.current.add(generation);
    activeGeneration.current = generation;

    previousStop
      .then(async () => {
        if (isCurrent()) {
          await app.start();
        }
      })
      .catch((err: unknown) => {
        if (isCurrent()) {
          setError(`Start failed: ${errorMessage(err)}`);
        }
      });

    return () => {
      if (activeGeneration.current === generation) {
        activeGeneration.current = null;
      }
      const stopPromise = stopGeneration(generation);
      latestStopPromise.current = stopPromise;
      stopPromise.catch(() => {});
    };
  }, [name, room, httpUrl, grpcUrl, timeoutMs, pollIntervalMs, stopGeneration]);

  const handleSubmit = useCallback(() => {
    if (pendingSubmission.current !== null) {
      return;
    }

    const snapshot = inputRef.current;
    const trimmed = snapshot.trim();
    if (trimmed.length === 0) {
      return;
    }
    const generation = activeGeneration.current;
    if (generation === null || !generation.active) {
      return;
    }

    const submission: PendingSubmission = { generation, snapshot };
    pendingSubmission.current = submission;
    setSending(true);

    generation.app
      .sendMessage(trimmed)
      .then((ok) => {
        if (
          ok &&
          mounted.current &&
          generation.active &&
          activeGeneration.current === generation &&
          pendingSubmission.current === submission
        ) {
          const current = inputRef.current;
          const next = current.startsWith(snapshot)
            ? current.slice(snapshot.length)
            : current;
          inputRef.current = next;
          setInput(next);
        }
      })
      .catch(() => {})
      .finally(() => {
        if (pendingSubmission.current === submission) {
          pendingSubmission.current = null;
          if (
            mounted.current &&
            generation.active &&
            activeGeneration.current === generation
          ) {
            setSending(false);
          }
        }
      });
  }, []);

  const handleQuit = useCallback(() => {
    onExit?.();
    shutdown().catch(() => {});
    exit();
  }, [onExit, shutdown, exit]);

  useInput((rawInput, key) => {
    if (key.escape) {
      handleQuit();
      return;
    }

    if (key.return) {
      handleSubmit();
      return;
    }

    if (key.backspace || key.delete) {
      const next = inputRef.current.slice(0, -1);
      inputRef.current = next;
      setInput(next);
      return;
    }

    if (rawInput.length > 0 && !key.ctrl && !key.meta) {
      const next = inputRef.current + rawInput;
      inputRef.current = next;
      setInput(next);
    }
  });

  const roomName = room;
  const participantName = name;
  const statusColor =
    state.status === "connected"
      ? "green"
      : state.status === "connecting"
        ? "yellow"
        : "red";

  return (
    <Box flexDirection="column">
      <Box marginBottom={1}>
        <Text bold>FerrumQ Chat</Text>
        <Text> </Text>
        <Text dimColor>room: {roomName} </Text>
        <Text dimColor>as: {participantName} </Text>
        <Text color={statusColor}>
          [{state.status === "connected" ? "online" : state.status}]
        </Text>
      </Box>

      <Box flexDirection="column" marginBottom={1}>
        {messages.length === 0 && (
          <Box>
            <Text dimColor>No messages yet. Type below to start chatting.</Text>
          </Box>
        )}

        {messages.slice(-MAX_RENDERED_MESSAGES).map((msg) => (
          <Box key={msg.id} flexDirection="row">
            <Text dimColor>{formatTime(msg.timestamp)} </Text>
            {msg.isSelf ? (
              <Text bold color="cyan">
                {msg.senderName} (you):{" "}
              </Text>
            ) : (
              <Text>{msg.senderName}: </Text>
            )}
            <Text>{msg.text}</Text>
          </Box>
        ))}
      </Box>

      {warning !== null && (
        <Box>
          <Text color="yellow" dimColor>
            {warning}
          </Text>
        </Box>
      )}

      {error !== null && (
        <Box>
          <Text color="red">Error: {error}</Text>
        </Box>
      )}

      <Box>
        <Text dimColor>{">"} </Text>
        <Text>{input}</Text>
        <Text dimColor>|</Text>
        {sending && <Text dimColor> sending…</Text>}
      </Box>

      <Box>
        <Text dimColor>Enter: send | Esc: quit | Ctrl+C: quit</Text>
      </Box>
    </Box>
  );
}

function formatTime(date: Date): string {
  return date.toLocaleTimeString("en-US", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
