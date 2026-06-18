import type { DlqEntryResponse, TopicResponse } from "@ferrumq/protocol";
import { Box, Text, useApp, useInput } from "ink";
import { useCallback, useEffect, useRef, useState } from "react";
import { type TuiConfig, tuiVersion } from "./config.js";
import {
  formatTuiError,
  type LoadTuiSnapshotDependencies,
  loadTuiSnapshot,
  type TuiSnapshot,
} from "./loader.js";

export type TuiView = "dashboard" | "topics" | "dlq" | "help";

export interface TuiState {
  activeView: TuiView;
  snapshot: TuiSnapshot | undefined;
  loading: boolean;
  error: string | undefined;
  refreshCount: number;
  lastRefreshAt: Date | undefined;
}

export interface FerrumQTuiProps {
  config: TuiConfig;
  dependencies?: LoadTuiSnapshotDependencies;
  initialView?: TuiView;
  initialSnapshot?: TuiSnapshot;
  onExit?: () => void;
}

export function FerrumQTui({
  config,
  dependencies,
  initialView = "dashboard",
  initialSnapshot,
  onExit,
}: FerrumQTuiProps) {
  const { exit } = useApp();
  const requestId = useRef(0);
  const mounted = useRef(true);
  const [state, setState] = useState<TuiState>({
    activeView: initialView,
    snapshot: initialSnapshot,
    loading: initialSnapshot === undefined,
    error: undefined,
    refreshCount: initialSnapshot === undefined ? 0 : 1,
    lastRefreshAt: initialSnapshot?.refreshedAt,
  });

  const refresh = useCallback(() => {
    const currentRequest = requestId.current + 1;
    requestId.current = currentRequest;
    setState((previous) => ({ ...previous, loading: true }));

    void loadTuiSnapshot(config, dependencies)
      .then((snapshot) => {
        if (!mounted.current || requestId.current !== currentRequest) {
          return;
        }

        setState((previous) => ({
          ...previous,
          snapshot,
          loading: false,
          error: undefined,
          refreshCount: previous.refreshCount + 1,
          lastRefreshAt: snapshot.refreshedAt,
        }));
      })
      .catch((error: unknown) => {
        if (!mounted.current || requestId.current !== currentRequest) {
          return;
        }

        setState((previous) => ({
          ...previous,
          loading: false,
          error: formatTuiError(error),
          refreshCount: previous.refreshCount + 1,
          lastRefreshAt: (dependencies?.now ?? (() => new Date()))(),
        }));
      });
  }, [config, dependencies]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(
    () => () => {
      mounted.current = false;
    },
    [],
  );

  useInput((input) => {
    const key = input.toLowerCase();
    if (key === "q") {
      onExit?.();
      exit();
      return;
    }

    if (key === "r") {
      refresh();
      return;
    }

    if (input === "1") {
      setState((previous) => ({ ...previous, activeView: "dashboard" }));
      return;
    }

    if (input === "2") {
      setState((previous) => ({ ...previous, activeView: "topics" }));
      return;
    }

    if (input === "3") {
      setState((previous) => ({ ...previous, activeView: "dlq" }));
      return;
    }

    if (input === "?") {
      setState((previous) => ({ ...previous, activeView: "help" }));
    }
  });

  return <TuiFrame config={config} state={state} />;
}

export interface TuiFrameProps {
  config: TuiConfig;
  state: TuiState;
}

export function TuiFrame({ config, state }: TuiFrameProps) {
  return (
    <Box flexDirection="column">
      <Header activeView={state.activeView} loading={state.loading} />
      {state.activeView === "dashboard" ? (
        <DashboardView config={config} state={state} />
      ) : null}
      {state.activeView === "topics" ? (
        state.snapshot === undefined ? (
          <SnapshotStateView state={state} />
        ) : (
          <TopicsView topics={state.snapshot.topics.items} />
        )
      ) : null}
      {state.activeView === "dlq" ? (
        state.snapshot === undefined ? (
          <SnapshotStateView state={state} />
        ) : (
          <DlqView entries={state.snapshot.dlq.items} />
        )
      ) : null}
      {state.activeView === "help" ? <HelpView /> : null}
      <StatusFooter state={state} />
    </Box>
  );
}

function Header({
  activeView,
  loading,
}: {
  activeView: TuiView;
  loading: boolean;
}) {
  return (
    <Box flexDirection="column" marginBottom={1}>
      <Text bold>FerrumQ {tuiVersion}</Text>
      <Text>
        view: {activeView}
        {loading ? " | loading" : ""}
      </Text>
    </Box>
  );
}

export function DashboardView({
  config,
  state,
}: {
  config: TuiConfig;
  state: TuiState;
}) {
  const snapshot = state.snapshot;

  return (
    <Box flexDirection="column">
      <Text>control URL: {config.controlUrl}</Text>
      <Text>gRPC URL: {config.grpcUrl}</Text>
      <Text>health: {snapshot?.health.status ?? "unknown"}</Text>
      <Text>readiness: {snapshot?.readiness.status ?? "unknown"}</Text>
      <Text>broker mode: {snapshot?.status.mode ?? "unknown"}</Text>
      <Text>topic count: {snapshot?.status.topics ?? "unknown"}</Text>
      <Text>DLQ count: {snapshot?.status.dlqEntries ?? "unknown"}</Text>
      <Text>last refresh: {formatTimestamp(state.lastRefreshAt)}</Text>
      <Text>last error: {state.error ?? "none"}</Text>
    </Box>
  );
}

export function TopicsView({ topics }: { topics: readonly TopicResponse[] }) {
  if (topics.length === 0) {
    return <Text>no topics</Text>;
  }

  return (
    <Box flexDirection="column">
      {topics.map((topic) => (
        <Text key={topic.name}>
          {topic.name} partitions={topic.partitions}
        </Text>
      ))}
    </Box>
  );
}

export function DlqView({ entries }: { entries: readonly DlqEntryResponse[] }) {
  if (entries.length === 0) {
    return <Text>no DLQ entries</Text>;
  }

  return (
    <Box flexDirection="column">
      {entries.map((entry) => (
        <Text key={dlqEntryKey(entry)}>
          {entry.topic}[{entry.partition}]@{entry.offset} message=
          {entry.messageId} group={entry.consumerGroupId} reason={entry.reason}{" "}
          attempts={entry.attemptCount} timestamp=
          {new Date(entry.timestamp).toISOString()}
        </Text>
      ))}
    </Box>
  );
}

export function HelpView() {
  return (
    <Box flexDirection="column">
      <Text>1 dashboard</Text>
      <Text>2 topics</Text>
      <Text>3 DLQ</Text>
      <Text>? help</Text>
      <Text>r refresh</Text>
      <Text>q quit</Text>
    </Box>
  );
}

export function StatusFooter({ state }: { state: TuiState }) {
  return (
    <Box flexDirection="column" marginTop={1}>
      <Text dimColor>
        refreshes={state.refreshCount} | keys: 1 dashboard 2 topics 3 DLQ ? help
        r refresh q quit
      </Text>
      {state.error === undefined ? null : (
        <Text color="red">error: {state.error}</Text>
      )}
    </Box>
  );
}

function SnapshotStateView({ state }: { state: TuiState }) {
  if (state.loading) {
    return <Text>Loading broker snapshot...</Text>;
  }

  if (state.error !== undefined) {
    return <Text color="red">{state.error}</Text>;
  }

  return <Text>No broker snapshot loaded</Text>;
}

function dlqEntryKey(entry: DlqEntryResponse): string {
  return [
    entry.topic,
    entry.partition,
    entry.offset,
    entry.messageId,
    entry.consumerGroupId,
  ].join(":");
}

function formatTimestamp(value: Date | undefined): string {
  return value === undefined ? "never" : value.toISOString();
}
