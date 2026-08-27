// WebSocket Client Unit Tests with Fake WebSocket Mock
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DaemonNotification, DaemonRequest, DaemonResponse } from "../types";
import { DaemonWsClient } from "./ws";

class FakeWebSocket {
  public static instances: FakeWebSocket[] = [];
  public url: string;
  public readyState: number = 0; // CONNECTING
  public onopen: ((ev: any) => void) | null = null;
  public onmessage: ((ev: any) => void) | null = null;
  public onerror: ((ev: any) => void) | null = null;
  public onclose: ((ev: any) => void) | null = null;
  public sentFrames: string[] = [];

  public static OPEN = 1;
  public static CLOSED = 3;
  public static CONNECTING = 0;

  constructor(url: string) {
    this.url = url;
    FakeWebSocket.instances.push(this);
    // Simulate async open
    setTimeout(() => {
      this.readyState = FakeWebSocket.OPEN;
      if (this.onopen) this.onopen({ type: "open" });
    }, 10);
  }

  public send(data: string) {
    this.sentFrames.push(data);
  }

  public close(code = 1000, reason = "") {
    this.readyState = FakeWebSocket.CLOSED;
    if (this.onclose) this.onclose({ code, reason });
  }

  // Helper for tests to push incoming message
  public receiveJson(obj: any) {
    if (this.onmessage) {
      this.onmessage({ data: JSON.stringify(obj) });
    }
  }
}

describe("DaemonWsClient with FakeWebSocket", () => {
  beforeEach(() => {
    FakeWebSocket.instances = [];
    vi.useFakeTimers();
  });

  it("connects, sends RPC requests and correlates responses by id", async () => {
    const client = new DaemonWsClient({
      url: "ws://127.0.0.1:8741",
      WebSocketClass: FakeWebSocket as any,
      autoConnect: false,
    });

    client.connect();
    expect(FakeWebSocket.instances.length).toBe(1);
    const ws = FakeWebSocket.instances[0];

    // Trigger open
    vi.advanceTimersByTime(15);
    expect(client.getState()).toBe("connected");

    // Send RPC call
    const pingPromise = client.call("ping", {});
    expect(ws.sentFrames.length).toBe(1);

    const sentReq: DaemonRequest = JSON.parse(ws.sentFrames[0]);
    expect(sentReq.method).toBe("ping");
    expect(sentReq.id).toBeDefined();

    // Mock daemon response echoing id
    const res: DaemonResponse = {
      id: sentReq.id,
      ok: true,
      result: { pong: true, serverTime: "2026-08-22T10:00:00Z" },
    };
    ws.receiveJson(res);

    const result = await pingPromise;
    expect(result).toEqual({ pong: true, serverTime: "2026-08-22T10:00:00Z" });

    client.disconnect();
  });

  it("handles RPC error responses correctly", async () => {
    const client = new DaemonWsClient({
      url: "ws://127.0.0.1:8741",
      WebSocketClass: FakeWebSocket as any,
      autoConnect: false,
    });

    client.connect();
    vi.advanceTimersByTime(15);

    const callPromise = client.call("git.diff", { repo: "test", base: "a", head: "b" });
    const ws = FakeWebSocket.instances[0];
    const sentReq: DaemonRequest = JSON.parse(ws.sentFrames[0]);

    ws.receiveJson({
      id: sentReq.id,
      ok: false,
      error: { code: "not_supported", message: "git.diff is not supported in v1" },
    });

    await expect(callPromise).rejects.toThrow("git.diff is not supported in v1");

    client.disconnect();
  });

  it("resubscribes gaplessly from lastSeq upon reconnection", async () => {
    const client = new DaemonWsClient({
      url: "ws://127.0.0.1:8741",
      WebSocketClass: FakeWebSocket as any,
      autoConnect: false,
      initialReconnectDelayMs: 50,
    });

    client.connect();
    vi.advanceTimersByTime(15);

    const receivedEvents: any[] = [];
    client.subscribe(0, (event) => {
      receivedEvents.push(event);
    });

    const ws1 = FakeWebSocket.instances[0];
    // Subscribe frame sent
    expect(ws1.sentFrames.length).toBe(1);
    const subReq: DaemonRequest = JSON.parse(ws1.sentFrames[0]);
    expect(subReq.method).toBe("events.subscribe");
    expect(subReq.params.afterSeq).toBe(0);

    // Send subscription confirmation
    ws1.receiveJson({
      id: subReq.id,
      ok: true,
      result: { subscriptionId: "sub-1" },
    });

    // Send notification event seq 42
    const notif: DaemonNotification = {
      notification: "event",
      subscriptionId: "sub-1",
      seq: 42,
      event: {
        seq: 42,
        id: "ev-42",
        eventType: "task.running",
        occurredAt: "2026-08-22T10:00:00Z",
        runId: "run-1",
        traceId: null,
        taskId: "task-1",
        agentId: "agent-1",
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
    };
    ws1.receiveJson(notif);

    expect(receivedEvents.length).toBe(1);
    expect(client.getLastSeq()).toBe(42);

    // Simulate unexpected connection drop (e.g. server restart)
    ws1.close(1006, "Connection dropped");
    expect(client.getState()).toBe("reconnecting");

    // Advance timer for exponential reconnect (accounting for initial delay + jitter)
    vi.advanceTimersByTime(1000);
    expect(FakeWebSocket.instances.length).toBe(2);
    const ws2 = FakeWebSocket.instances[1];

    // Trigger open for second connection
    vi.advanceTimersByTime(15);
    expect(client.getState()).toBe("connected");

    // Check that it re-issued events.subscribe with afterSeq: 42 (gapless catch-up!)
    expect(ws2.sentFrames.length).toBe(1);
    const resubReq: DaemonRequest = JSON.parse(ws2.sentFrames[0]);
    expect(resubReq.method).toBe("events.subscribe");
    expect(resubReq.params.afterSeq).toBe(42);

    client.disconnect();
  });
});
