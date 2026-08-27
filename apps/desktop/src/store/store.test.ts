// Store and Telemetry Coalescing Unit Tests
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { EventWire } from "../types";
import { DesktopStore, FLUSH_INTERVAL_MS, MAX_EVENT_RING_BUFFER_SIZE } from "./store";

describe("DesktopStore and Telemetry Coalescing", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it("ignores a replayed span instead of folding it twice", () => {
    const fakeClient: any = {
      onStateChange: () => () => {},
      subscribe: () => () => {},
      call: async () => ({}),
      getLastSeq: () => 0,
    };
    const store = new DesktopStore(fakeClient);
    const ev = (seq: number): EventWire => ({
      seq,
      id: `ev-${seq}`,
      eventType: "task.running",
      occurredAt: new Date().toISOString(),
      runId: "run-1",
      traceId: null,
      taskId: null,
      agentId: "agent-1",
      payload: {},
      payloadRef: null,
      payloadHash: null,
      schemaVersion: 1,
    });

    for (const seq of [1, 2, 3]) store.handleIncomingEvent(ev(seq));
    // A reconnect (or a second subscription) replays the same span.
    for (const seq of [1, 2, 3]) store.handleIncomingEvent(ev(seq));

    const snapshot = store.getSnapshot();
    expect(snapshot.events.length).toBe(3);
    // eventCount is accumulated per fold: double-folding inflates it past
    // the number of events the journal actually holds.
    expect(snapshot.agents.find((a) => a.agentId === "agent-1")?.eventCount).toBe(3);
  });

  it("limits event ring buffer to MAX_EVENT_RING_BUFFER_SIZE (2000)", () => {
    const fakeClient: any = {
      onStateChange: () => () => {},
      subscribe: () => () => {},
      call: async () => ({}),
      getLastSeq: () => 0,
    };

    const store = new DesktopStore(fakeClient);

    // Push 2100 events
    for (let i = 1; i <= 2100; i++) {
      const ev: EventWire = {
        seq: i,
        id: `ev-${i}`,
        eventType: "task.running",
        occurredAt: new Date().toISOString(),
        runId: "run-ring",
        traceId: null,
        taskId: `task-${i}`,
        agentId: "agent-ring",
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      };
      store.handleIncomingEvent(ev);
    }

    const snapshot = store.getSnapshot();
    expect(snapshot.events.length).toBe(MAX_EVENT_RING_BUFFER_SIZE);
    expect(snapshot.events[0].seq).toBe(101); // oldest 100 dropped
    expect(snapshot.events[snapshot.events.length - 1].seq).toBe(2100);

    store.destroy();
  });

  it("coalesces high-frequency telemetry (usage.updated) at <= 4Hz", () => {
    const fakeClient: any = {
      onStateChange: () => () => {},
      subscribe: () => () => {},
      call: async () => ({}),
      getLastSeq: () => 0,
    };

    const store = new DesktopStore(fakeClient);
    let subscriberNotificationCount = 0;
    store.subscribe(() => {
      subscriberNotificationCount++;
    });

    // Fire 50 usage.updated events in rapid succession (within 10ms)
    for (let i = 1; i <= 50; i++) {
      const ev: EventWire = {
        seq: i,
        id: `ev-usage-${i}`,
        eventType: "usage.updated",
        occurredAt: new Date().toISOString(),
        runId: "run-usage",
        traceId: null,
        taskId: "task-usage",
        agentId: "agent-usage",
        payload: { costUsd: 0.001, tokensEstimate: 100 },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      };
      store.handleIncomingEvent(ev);
    }

    // High frequency events should NOT immediately fire 50 store notifications
    expect(subscriberNotificationCount).toBe(0);

    // Advance by 250ms (the 4Hz flush interval)
    vi.advanceTimersByTime(FLUSH_INTERVAL_MS);

    // Exactly one batched notification fired
    expect(subscriberNotificationCount).toBe(1);

    // Verify aggregate usage was properly accumulated
    const agent = store.getSnapshot().agents.find((a) => a.agentId === "agent-usage");
    expect(agent).toBeDefined();
    expect(agent?.usage.costUsd).toBeCloseTo(0.05, 4);
    expect(agent?.usage.tokensEstimate).toBe(5000);

    store.destroy();
  });
});
