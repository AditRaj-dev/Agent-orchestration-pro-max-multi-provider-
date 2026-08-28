import { describe, expect, it, vi } from "vitest";
import { DaemonRpc } from "../lib/rpc";

describe("DaemonRpc", () => {
  it("fails requests cleanly while offline", async () => {
    const client = new DaemonRpc("ws://127.0.0.1:1");
    await expect(client.request("ping")).rejects.toThrow("offline");
  });

  it("forwards server notifications to subscribers", () => {
    const client = new DaemonRpc(); const listener = vi.fn(); const dispose = client.subscribe(listener);
    (client as unknown as { handleMessage: (raw: string) => void }).handleMessage(JSON.stringify({ method: "events.appended", params: { payload: { message: "recorded" } } }));
    expect(listener).toHaveBeenCalledWith(expect.objectContaining({ type: "events.appended" })); dispose();
  });

  it("tracks the highest daemon event sequence from the subscription protocol", () => {
    const client = new DaemonRpc(); const listener = vi.fn(); client.subscribe(listener);
    (client as unknown as { handleMessage: (raw: string) => void }).handleMessage(JSON.stringify({ notification: "event", seq: 7, event: { eventType: "artifact.created" } }));
    (client as unknown as { handleMessage: (raw: string) => void }).handleMessage(JSON.stringify({ notification: "event", seq: 4, event: { eventType: "artifact.created" } }));
    expect(client.lastEventSeq).toBe(7);
    expect(listener).toHaveBeenCalledWith(expect.objectContaining({ seq: 7 }));
  });
});
