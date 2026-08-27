// F-11 React Hooks for Store & Daemon (useSyncExternalStore)
import { useEffect, useState, useSyncExternalStore } from "react";
import { ConnectionMeta, getDaemonWsClient } from "../daemon/ws";
import type {
  AgentSummary,
  ConnectionState,
  EventWire,
  RunSummary,
  TaskSummary,
} from "../types";
import { DesktopStore, getDesktopStore, StoreState } from "./store";

export function useDesktopStore(customStore?: DesktopStore): StoreState {
  const store = customStore || getDesktopStore();
  return useSyncExternalStore(store.subscribe, store.getSnapshot);
}

export function useRuns(customStore?: DesktopStore): RunSummary[] {
  const store = customStore || getDesktopStore();
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
  return state.runs;
}

export function useRun(runId: string | null | undefined, customStore?: DesktopStore): RunSummary | undefined {
  const store = customStore || getDesktopStore();
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
  if (!runId) return undefined;
  return state.runs.find((r) => r.runId === runId);
}

export function useTasks(runId?: string | null, customStore?: DesktopStore): TaskSummary[] {
  const store = customStore || getDesktopStore();
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
  if (!runId) return state.tasks;
  return state.tasks.filter((t) => t.runId === runId);
}

export function useTask(taskId: string | null | undefined, customStore?: DesktopStore): TaskSummary | undefined {
  const store = customStore || getDesktopStore();
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
  if (!taskId) return undefined;
  return state.tasks.find((t) => t.taskId === taskId);
}

export function useAgents(runId?: string | null, customStore?: DesktopStore): AgentSummary[] {
  const store = customStore || getDesktopStore();
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
  if (!runId) return state.agents;
  return state.agents.filter((a) => a.runId === runId);
}

export function useAgent(agentId: string | null | undefined, customStore?: DesktopStore): AgentSummary | undefined {
  const store = customStore || getDesktopStore();
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
  if (!agentId) return undefined;
  return state.agents.find((a) => a.agentId === agentId);
}

export interface EventFilter {
  runId?: string | null;
  taskId?: string | null;
  agentId?: string | null;
  eventType?: string | null;
  limit?: number;
}

export function useEvents(filter?: EventFilter, customStore?: DesktopStore): EventWire[] {
  const store = customStore || getDesktopStore();
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);

  let result = state.events;

  if (filter) {
    if (filter.runId) {
      result = result.filter((e) => e.runId === filter.runId);
    }
    if (filter.taskId) {
      result = result.filter((e) => e.taskId === filter.taskId);
    }
    if (filter.agentId) {
      result = result.filter((e) => e.agentId === filter.agentId);
    }
    if (filter.eventType) {
      result = result.filter((e) => e.eventType === filter.eventType);
    }
    if (filter.limit && filter.limit > 0 && result.length > filter.limit) {
      result = result.slice(-filter.limit);
    }
  }

  return result;
}

export function useEventRate(customStore?: DesktopStore): number {
  const store = customStore || getDesktopStore();
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
  return state.eventRate;
}

export function useDaemonConnection(): { state: ConnectionState; meta: ConnectionMeta } {
  const client = getDaemonWsClient();
  const [meta, setMeta] = useState<ConnectionMeta>(() => client.getMeta());

  useEffect(() => {
    return client.onStateChange((_st, updatedMeta) => {
      setMeta({ ...updatedMeta });
    });
  }, [client]);

  return { state: meta.state, meta };
}

const INBOX_EVENT_TYPES = new Set([
  "approval.required",
  "budget.exceeded",
  "ownership.conflict",
  "task.failed",
  "agent.spawn_failed",
  "agent.rate_limit",
  "git.gate_failed",
  "policy.denied",
  "handoff.rejected",
  "run.failed",
]);

export function useInboxEvents(customStore?: DesktopStore): EventWire[] {
  const events = useEvents(undefined, customStore);
  return events.filter((e) => INBOX_EVENT_TYPES.has(e.eventType));
}
