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
  const mounted = useRef(true);
  const activeGeneration = useRef<ChatAppGeneration | null>(null);
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
    };
  }, []);

  useEffect(() => {
    if (shutdownRequested.current) {
      return;
    }

    setMessages([]);
    setInput("");
    setState({ status: "disconnected" });
    setError(null);
    setWarning(null);

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
          setMessages((prev) => [...prev.slice(-499), msg]);
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
      onWarning: (msg: string) => {
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
    const trimmed = input.trim();
    if (trimmed.length === 0) {
      return;
    }
    setInput("");
    activeGeneration.current?.app.sendMessage(trimmed).catch(() => {});
  }, [input]);

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
      setInput((prev) => prev.slice(0, -1));
      return;
    }

    if (rawInput.length > 0 && !key.ctrl && !key.meta) {
      setInput((prev) => prev + rawInput);
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

        {messages.slice(-200).map((msg) => (
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
