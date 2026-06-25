import type {
  DlqEntryResponse,
  SearchResult,
  TopicResponse,
} from "@ferrumq/protocol";
import { Box, Text, useApp, useInput } from "ink";
import { useCallback, useEffect, useRef, useState } from "react";
import { type TuiConfig, tuiVersion } from "./config.js";
import {
  formatTuiError,
  type LoadTuiSnapshotDependencies,
  loadTuiSnapshot,
  type TuiSnapshot,
} from "./loader.js";

export type TuiView = "dashboard" | "topics" | "dlq" | "search" | "help";
type NonSearchTuiView = Exclude<TuiView, "search">;

export interface TuiState {
  activeView: TuiView;
  snapshot: TuiSnapshot | undefined;
  loading: boolean;
  error: string | undefined;
  refreshCount: number;
  lastRefreshAt: Date | undefined;
  search: SearchUiState;
}

export interface SearchUiState {
  query: string;
  topic: string;
  status: "idle" | "loading" | "ready" | "error" | "unavailable";
  items: readonly SearchResult[];
  errorMessage: string | undefined;
  lastSubmittedQuery: string | undefined;
  lastSubmittedTopic: string | undefined;
}

export interface SearchDependencies {
  searchMessages: (request: {
    query: string;
    topic?: string;
    limit?: number;
  }) => Promise<{ items: readonly SearchResult[] }>;
  now?: () => Date;
}

export interface FerrumQTuiProps {
  config: TuiConfig;
  dependencies?: LoadTuiSnapshotDependencies;
  searchDependencies?: SearchDependencies;
  initialView?: TuiView;
  initialSnapshot?: TuiSnapshot;
  initialSearch?: Partial<SearchUiState>;
  onExit?: () => void;
}

const MAX_SEARCH_QUERY_LENGTH = 256;

export function FerrumQTui({
  config,
  dependencies,
  searchDependencies,
  initialView = "dashboard",
  initialSnapshot,
  initialSearch,
  onExit,
}: FerrumQTuiProps) {
  const { exit } = useApp();
  const requestId = useRef(0);
  const mounted = useRef(true);
  const searchRequestId = useRef(0);
  const previousView = useRef<NonSearchTuiView>(
    initialView === "search" ? "dashboard" : initialView,
  );
  const [state, setState] = useState<TuiState>({
    activeView: initialView,
    snapshot: initialSnapshot,
    loading: initialSnapshot === undefined,
    error: undefined,
    refreshCount: initialSnapshot === undefined ? 0 : 1,
    lastRefreshAt: initialSnapshot?.refreshedAt,
    search: {
      query: initialSearch?.query ?? "",
      topic: initialSearch?.topic ?? "",
      status: "idle",
      items: initialSearch?.items ?? [],
      errorMessage: undefined,
      lastSubmittedQuery: undefined,
      lastSubmittedTopic: undefined,
    },
  });

  const submitSearch = useCallback(() => {
    if (searchDependencies === undefined) {
      setState((previous) => ({
        ...previous,
        search: {
          ...previous.search,
          status: "unavailable",
          errorMessage: "search is not configured",
        },
      }));
      return;
    }
    const trimmedQuery = state.search.query.trim();
    if (trimmedQuery.length === 0) {
      setState((previous) => ({
        ...previous,
        search: {
          ...previous.search,
          status: "error",
          errorMessage:
            "search query must contain at least one alphanumeric character",
        },
      }));
      return;
    }
    const trimmedTopic = state.search.topic.trim();
    const currentRequest = searchRequestId.current + 1;
    searchRequestId.current = currentRequest;
    setState((previous) => ({
      ...previous,
      search: {
        ...previous.search,
        status: "loading",
        errorMessage: undefined,
        lastSubmittedQuery: trimmedQuery,
        lastSubmittedTopic: trimmedTopic,
      },
    }));
    void searchDependencies
      .searchMessages({
        query: trimmedQuery,
        ...(trimmedTopic.length === 0 ? {} : { topic: trimmedTopic }),
        limit: 20,
      })
      .then((response) => {
        if (!mounted.current || searchRequestId.current !== currentRequest) {
          return;
        }
        setState((previous) => ({
          ...previous,
          search: {
            ...previous.search,
            status: "ready",
            items: response.items,
            errorMessage: undefined,
          },
        }));
      })
      .catch((error: unknown) => {
        if (!mounted.current || searchRequestId.current !== currentRequest) {
          return;
        }
        const message = formatTuiError(error);
        const unavailable = /SEARCH_UNAVAILABLE|search is not configured/i.test(
          message,
        );
        setState((previous) => ({
          ...previous,
          search: {
            ...previous.search,
            status: unavailable ? "unavailable" : "error",
            errorMessage: message,
          },
        }));
      });
  }, [searchDependencies, state.search.query, state.search.topic]);

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

  useInput((input, key) => {
    if (key.ctrl && input.toLowerCase() === "c") {
      onExit?.();
      exit();
      return;
    }

    if (state.activeView === "search") {
      if (key.escape) {
        setState((previous) => ({
          ...previous,
          activeView: previousView.current,
        }));
      }
      return;
    }

    const normalizedInput = input.toLowerCase();
    if (normalizedInput === "q") {
      onExit?.();
      exit();
      return;
    }

    if (normalizedInput === "r") {
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

    if (input === "4") {
      setState((previous) => {
        previousView.current = nonSearchViewOrDashboard(previous.activeView);
        return { ...previous, activeView: "search" };
      });
      return;
    }

    if (input === "?") {
      setState((previous) => ({ ...previous, activeView: "help" }));
    }
  });

  return (
    <TuiFrame
      config={config}
      state={state}
      onSubmitSearch={submitSearch}
      onChangeSearchQuery={(query) =>
        setState((previous) => ({
          ...previous,
          search: { ...previous.search, query },
        }))
      }
      onChangeSearchTopic={(topic) =>
        setState((previous) => ({
          ...previous,
          search: { ...previous.search, topic },
        }))
      }
    />
  );
}

function nonSearchViewOrDashboard(view: TuiView): NonSearchTuiView {
  return view === "search" ? "dashboard" : view;
}

export interface TuiFrameProps {
  config: TuiConfig;
  state: TuiState;
  onSubmitSearch: () => void;
  onChangeSearchQuery: (query: string) => void;
  onChangeSearchTopic: (topic: string) => void;
}

export function TuiFrame({
  config,
  state,
  onSubmitSearch,
  onChangeSearchQuery,
  onChangeSearchTopic,
}: TuiFrameProps) {
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
      {state.activeView === "search" ? (
        <SearchView
          search={state.search}
          onChangeQuery={onChangeSearchQuery}
          onChangeTopic={onChangeSearchTopic}
          onSubmit={onSubmitSearch}
        />
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

interface SearchViewProps {
  search: SearchUiState;
  onChangeQuery: (query: string) => void;
  onChangeTopic: (topic: string) => void;
  onSubmit: () => void;
}

export function SearchView({
  search,
  onChangeQuery,
  onChangeTopic,
  onSubmit,
}: SearchViewProps) {
  void onChangeTopic;
  useInput((input, key) => {
    if (key.escape || key.ctrl) {
      return;
    }
    if (key.return) {
      onSubmit();
      return;
    }
    if (key.backspace) {
      onChangeQuery(search.query.slice(0, -1));
      return;
    }
    if (input.length === 0) {
      return;
    }
    if (search.query.length < MAX_SEARCH_QUERY_LENGTH) {
      onChangeQuery(search.query + input);
    }
  });

  return (
    <Box flexDirection="column">
      <Text>query: {search.query || "(type a query, then press Enter)"}</Text>
      <Text dimColor>
        Enter to search; Backspace to edit; Esc back; Ctrl+C quit.
      </Text>
      {renderSearchStatus(search)}
      {renderSearchResults(search)}
    </Box>
  );
}

function renderSearchStatus(search: SearchUiState) {
  switch (search.status) {
    case "loading":
      return <Text>searching...</Text>;
    case "unavailable":
      return (
        <Text color="yellow">
          search unavailable:{" "}
          {search.errorMessage ?? "search is not configured"}
        </Text>
      );
    case "error":
      return <Text color="red">error: {search.errorMessage ?? "unknown"}</Text>;
    case "ready":
      if (search.items.length === 0) {
        return (
          <Text dimColor>
            no results for {search.lastSubmittedQuery ?? search.query}
          </Text>
        );
      }
      return (
        <Text>
          {search.items.length} result{search.items.length === 1 ? "" : "s"}
        </Text>
      );
    default:
      return null;
  }
}

function renderSearchResults(search: SearchUiState) {
  if (search.status !== "ready" || search.items.length === 0) {
    return null;
  }
  return (
    <Box flexDirection="column" marginTop={1}>
      {search.items.map((item) => (
        <Text key={searchResultKey(item)}>
          {item.topic}[{item.partitionId}]@{item.offset} message=
          {item.messageId} event={item.eventType} source={item.source} subject=
          {item.subject ?? ""} content={item.contentType} time={item.timeUnixMs}{" "}
          payload_len={item.payloadLen} sha256=
          {shortenSha256(item.payloadSha256)} rank={item.rank}
        </Text>
      ))}
    </Box>
  );
}

function searchResultKey(item: SearchResult): string {
  return [
    item.topic,
    item.partitionId,
    item.offset,
    item.messageId,
    item.timeUnixMs,
  ].join(":");
}

function shortenSha256(value: string): string {
  if (value.length <= 12) {
    return value;
  }
  return `${value.slice(0, 12)}…`;
}

export function HelpView() {
  return (
    <Box flexDirection="column">
      <Text>1 dashboard</Text>
      <Text>2 topics</Text>
      <Text>3 DLQ</Text>
      <Text>4 search</Text>
      <Text>? help</Text>
      <Text>r refresh</Text>
      <Text>q quit</Text>
      <Text>
        Search mode: Enter search, Backspace edit, Esc back, Ctrl+C quit
      </Text>
    </Box>
  );
}

export function StatusFooter({ state }: { state: TuiState }) {
  return (
    <Box flexDirection="column" marginTop={1}>
      <Text dimColor>
        refreshes={state.refreshCount} | keys: 1 dashboard 2 topics 3 DLQ 4
        search ? help r refresh q quit
      </Text>
      <Text dimColor>
        search: Enter submit Backspace edit Esc back Ctrl+C quit
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
