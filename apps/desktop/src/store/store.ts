// F-11 Desktop Normalized Store (docs/F-11-desktop.md §3.3, §4)
import { DaemonWsClient, getDaemonWsClient } from "../daemon/ws";
import type {
  AgentSummary,
  EventWire,
  RunSummary,
  TaskSummary,
} from "../types";
import {
  createEmptyNormalizedState,
  foldEvent,
  NormalizedState,
} from "./fold";

export const MAX_EVENT_RING_BUFFER_SIZE = 2000;
export const FLUSH_INTERVAL_MS = 250; // 4 Hz flush cap for high-frequency telemetry

export interface StoreState {
  runs: RunSummary[];
  tasks: TaskSummary[];
  agents: AgentSummary[];
  events: EventWire[];
  eventRate: number; // events / second
  version: number;
}

export class DesktopStore {
  private normalized: NormalizedState = createEmptyNormalizedState();
  private eventRing: EventWire[] = [];
  /** Highest seq folded into normalized state (replay guard). */
  private foldedSeq = 0;
  /** Highest seq pushed into the event ring (replay guard). */
  private ringMaxSeq = 0;
  private listeners = new Set<() => void>();
  private client: DaemonWsClient;
  private unsubscribeEvents: (() => void) | null = null;
  private unsubscribeState: (() => void) | null = null;

  // Telemetry coalescing
  private pendingEventsQueue: EventWire[] = [];
  private flushTimer: any = null;
  private isFlushScheduled = false;

  // Rate tracking (timestamps of last N events in past 3 seconds)
  private eventTimestamps: number[] = [];
  private rateCalcTimer: any = null;
  private currentEventRate = 0;

  // Snapshot cache for useSyncExternalStore
  private snapshotVersion = 0;
  private cachedSnapshot: StoreState;

  constructor(client?: DaemonWsClient) {
    this.client = client || getDaemonWsClient();
    this.cachedSnapshot = this.buildSnapshot();
    this.init();
  }

  private init(): void {
    // Listen to WS connection state changes
    this.unsubscribeState = this.client.onStateChange((state) => {
      if (state === "connected") {
        this.bootstrap();
      }
    });

    // Subscribe to event notifications
    this.unsubscribeEvents = this.client.subscribe(0, (event) => {
      this.handleIncomingEvent(event);
    });

    // Rate calculation loop every 1s
    this.rateCalcTimer = setInterval(() => {
      this.updateEventRate();
    }, 1000);
  }

  public destroy(): void {
    if (this.unsubscribeEvents) this.unsubscribeEvents();
    if (this.unsubscribeState) this.unsubscribeState();
    if (this.flushTimer) clearTimeout(this.flushTimer);
    if (this.rateCalcTimer) clearInterval(this.rateCalcTimer);
  }

  public async bootstrap(): Promise<void> {
    try {
      // Snapshot fetch from daemon RPC
      const [runsRes, tasksRes, agentsRes, eventsRes] = await Promise.allSettled([
        this.client.call<{ runs: RunSummary[] }>("runs.list", {}),
        this.client.call<{ tasks: TaskSummary[] }>("tasks.list", {}),
        this.client.call<{ agents: AgentSummary[] }>("agents.list", {}),
        this.client.call<{ events: EventWire[]; lastSeq: number }>("events.list", {
          afterSeq: Math.max(0, this.client.getLastSeq() - 200),
          limit: 200,
        }),
      ]);

      if (runsRes.status === "fulfilled" && runsRes.value?.runs) {
        for (const run of runsRes.value.runs) {
          this.normalized.runs.set(run.runId, run);
        }
      }

      if (tasksRes.status === "fulfilled" && tasksRes.value?.tasks) {
        for (const task of tasksRes.value.tasks) {
          this.normalized.tasks.set(task.taskId, task);
        }
      }

      if (agentsRes.status === "fulfilled" && agentsRes.value?.agents) {
        for (const agent of agentsRes.value.agents) {
          this.normalized.agents.set(agent.agentId, agent);
        }
      }

      if (eventsRes.status === "fulfilled" && eventsRes.value?.events) {
        for (const ev of eventsRes.value.events) {
          this.pushToRing(ev);
        }
      }

      this.triggerChange();
    } catch (err) {
      console.warn("Bootstrap snapshot fetch failed (using local projection):", err);
    }
  }

  public handleIncomingEvent(event: EventWire): void {
    // The journal replays on connect, and can replay again on a reconnect
    // or a second subscription. Folding one seq twice double-counts every
    // metric it accumulates — event counts, token totals, cost — and
    // duplicates the feed, so a replayed span must be ignored.
    if (event.seq > 0) {
      if (event.seq <= this.foldedSeq) return;
      this.foldedSeq = event.seq;
    }

    const now = Date.now();
    this.eventTimestamps.push(now);

    // Immediately fold into normalized state in-memory
    foldEvent(this.normalized, event);
    this.pushToRing(event);

    // High frequency events (usage.updated, agent.tool_use) coalesce at <= 4Hz
    const isHighFrequency = event.eventType === "usage.updated" || event.eventType === "agent.tool_use";

    if (isHighFrequency) {
      this.pendingEventsQueue.push(event);
      if (!this.isFlushScheduled) {
        this.isFlushScheduled = true;
        this.flushTimer = setTimeout(() => {
          this.flushPending();
        }, FLUSH_INTERVAL_MS);
      }
    } else {
      // Critical workflow events trigger immediate flush and UI update
      this.flushPending();
      this.triggerChange();
    }
  }

  private flushPending(): void {
    this.isFlushScheduled = false;
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.pendingEventsQueue = [];
    this.triggerChange();
  }

  private pushToRing(event: EventWire): void {
    // The ring has two producers — the bootstrap `events.list` page and the
    // subscription replay — which overlap. Comparing against the previous
    // element only catches an immediate repeat, so track the high-water
    // mark instead: both producers deliver in ascending seq order.
    if (event.seq > 0) {
      if (event.seq <= this.ringMaxSeq) return;
      this.ringMaxSeq = event.seq;
    } else if (this.eventRing.length > 0) {
      const last = this.eventRing[this.eventRing.length - 1];
      if (last.id === event.id) return;
    }

    this.eventRing.push(event);
    if (this.eventRing.length > MAX_EVENT_RING_BUFFER_SIZE) {
      this.eventRing.shift();
    }
  }

  private updateEventRate(): void {
    const now = Date.now();
    const windowMs = 3000;
    this.eventTimestamps = this.eventTimestamps.filter((t) => now - t <= windowMs);
    const rate = this.eventTimestamps.length / (windowMs / 1000);
    this.currentEventRate = Math.round(rate * 10) / 10;
    // Notify listeners if rate changed
    this.triggerChange();
  }

  private triggerChange(): void {
    this.snapshotVersion++;
    this.cachedSnapshot = this.buildSnapshot();
    this.listeners.forEach((listener) => {
      try {
        listener();
      } catch (err) {
        console.error("Store listener error:", err);
      }
    });
  }

  private buildSnapshot(): StoreState {
    return {
      runs: Array.from(this.normalized.runs.values()).sort(
        (a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime()
      ),
      tasks: Array.from(this.normalized.tasks.values()),
      agents: Array.from(this.normalized.agents.values()),
      events: [...this.eventRing],
      eventRate: this.currentEventRate,
      version: this.snapshotVersion,
    };
  }

  public getSnapshot = (): StoreState => {
    return this.cachedSnapshot;
  };

  public subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  public getNormalizedState(): NormalizedState {
    return this.normalized;
  }

  public getClient(): DaemonWsClient {
    return this.client;
  }
}

// Global Singleton DesktopStore
let globalStore: DesktopStore | null = null;

export function getDesktopStore(): DesktopStore {
  if (!globalStore) {
    globalStore = new DesktopStore();
  }
  return globalStore;
}
