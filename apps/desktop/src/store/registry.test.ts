// F-13 RegistryStore tests: event folding (chat transcripts, proposals,
// mutation refetch) against a stubbed WS client.
import { describe, expect, it, vi, beforeEach } from "vitest";
import type { EventWire } from "../types";
import { RegistryStore, threadsOf, visibleMessages } from "./registry";

// The journal hands out strictly increasing seqs; the store relies on that
// to drop replayed spans, so the fixture must not fake them randomly.
let nextSeq = 0;

function event(
  eventType: string,
  payload: Record<string, any>,
  agentId: string | null,
  seq = ++nextSeq,
): EventWire {
  return {
    seq,
    id: `${eventType}-${Math.random()}`,
    eventType,
    occurredAt: new Date().toISOString(),
    runId: null,
    traceId: null,
    taskId: null,
    agentId,
    payload,
    payloadRef: null,
    payloadHash: null,
    schemaVersion: 1,
  };
}

/** The daemon's `chat.sessions` fold, in miniature. */
function foldStubSessions(history: EventWire[]) {
  const byId = new Map<string, any>();
  for (const event of history) {
    const sessionId = event.payload?.sessionId;
    if (!sessionId) continue;
    if (event.eventType === "session.spawn") {
      byId.set(sessionId, {
        sessionId,
        agentId: event.agentId,
        provider: event.payload?.provider ?? null,
        model: event.payload?.model ?? null,
        providerSessionId: null,
        title: "",
        startedAt: event.occurredAt,
        lastAt: event.occurredAt,
        turns: 0,
        messageCount: 0,
        status: "live",
        error: null,
        lastSeq: event.seq,
      });
      continue;
    }
    const summary = byId.get(sessionId);
    if (!summary) continue;
    summary.lastAt = event.occurredAt;
    summary.lastSeq = event.seq;
    if (event.eventType === "session.started") {
      summary.providerSessionId = event.payload?.providerSessionId ?? null;
    }
    if (event.eventType === "session.instruction" && !summary.title) {
      summary.title = String(event.payload?.message || "");
    }
    if (event.eventType === "session.finished") {
      summary.turns += 1;
      summary.status = "finished";
    }
  }
  return [...byId.values()].sort((a, b) => b.lastSeq - a.lastSeq);
}

/** The daemon's `chat.transcript` fold, in miniature. */
function foldStubTranscript(history: EventWire[], sessionId: string) {
  const messages: any[] = [];
  for (const event of history) {
    if (event.payload?.sessionId !== sessionId) continue;
    const base = {
      sessionId,
      agentId: event.agentId,
      at: event.occurredAt,
      seq: event.seq,
    };
    if (event.eventType === "session.instruction") {
      messages.push({ ...base, role: "user", text: String(event.payload?.message || "") });
    } else if (event.eventType === "session.finished") {
      messages.push({ ...base, role: "agent", text: String(event.payload?.finalResult || "") });
    } else if (event.eventType === "agent.tool_use") {
      messages.push({
        ...base,
        role: "tool",
        tool: String(event.payload?.tool || ""),
        text: String(event.payload?.argsSummary || ""),
      });
    }
  }
  return messages;
}

function stubClient(history: EventWire[] = []) {
  const listeners = new Set<(event: EventWire) => void>();
  const stateListeners = new Set<() => void>();
  return {
    call: vi.fn(async (method: string, params?: unknown) => {
      if (method === "registry.agents.list") {
        return {
          agents: [
            {
              id: "agent-creator",
              name: "Agent Creator",
              description: "drafts agents",
              adapterId: "antigravity-agy",
              model: "claude-sonnet-4-6",
              effort: null,
              mode: "plan",
              skills: ["agent-creation"],
              toolAllowlist: [],
              toolDenylist: [],
              timeoutSecs: 900,
              builtin: true,
              enabled: true,
              createdAt: new Date().toISOString(),
              updatedAt: new Date().toISOString(),
            },
          ],
        };
      }
      if (method === "registry.skills.list") return { skills: [] };
      if (method === "registry.catalog") return { providers: [] };
      if (method === "agent.session.start") return { sessionId: "chat-1" };
      if (method === "events.list") return { events: history, truncated: false };
      // The daemon folds chat history server-side (F-13b); the stub mirrors
      // that fold over the same journal fixture the test supplies.
      if (method === "chat.sessions") return { sessions: foldStubSessions(history) };
      if (method === "chat.transcript") {
        const sessionId = (params as any)?.sessionId;
        return { messages: foldStubTranscript(history, sessionId) };
      }
      return {};
    }),
    subscribe: vi.fn((_after: number, onEvent: (event: EventWire) => void) => {
      listeners.add(onEvent);
      return () => listeners.delete(onEvent);
    }),
    onStateChange: vi.fn((_cb: () => void) => {
      stateListeners.add(_cb);
      return () => stateListeners.delete(_cb);
    }),
    emit: (e: EventWire) => listeners.forEach((listener) => listener(e)),
    getState: vi.fn(() => "connected"),
    /** Simulate the socket coming up after the store was constructed. */
    connect: () => stateListeners.forEach((listener: any) => listener("connected")),
  };
}

describe("RegistryStore", () => {
  let store: RegistryStore;
  let client: ReturnType<typeof stubClient>;

  beforeEach(() => {
    client = stubClient() as any;
    store = new RegistryStore(client as any);
  });

  it("bootstraps the agent list from the registry RPC", async () => {
    await vi.waitFor(() => expect(store.getSnapshot().loaded).toBe(true));
    expect(store.getSnapshot().agents.map((a) => a.id)).toEqual(["agent-creator"]);
  });

  it("folds chat lifecycle events into a transcript", async () => {
    await vi.waitFor(() => expect(store.getSnapshot().loaded).toBe(true));

    client.emit(
      event("session.spawn", { chat: true, sessionId: "chat-9" }, "agent-creator"),
    );
    client.emit(
      event("session.instruction", { sessionId: "chat-9", message: "I need a SQL reviewer" }, "agent-creator"),
    );
    expect(store.getSnapshot().running["agent-creator"]).toBe(true);
    client.emit(
      event(
        "agent.tool_use",
        { chat: true, sessionId: "chat-9", tool: "Read", argsSummary: "docs/PRD.md" },
        "agent-creator",
      ),
    );
    client.emit(
      event("session.finished", { chat: true, sessionId: "chat-9", finalResult: "Sure — a few questions first." }, "agent-creator"),
    );

    const transcript = store.getSnapshot().transcripts["agent-creator"];
    expect(transcript.map((m) => m.role)).toEqual(["user", "tool", "agent"]);
    expect(transcript[0].text).toContain("SQL reviewer");
    expect(transcript[1].tool).toBe("Read");
    expect(store.getSnapshot().sessionIds["agent-creator"]).toBe("chat-9");
    expect(store.getSnapshot().running["agent-creator"]).toBe(false);
  });

  it("rebuilds transcripts from the journal, not from the live tail alone", async () => {
    // The shared subscription only replays for whoever opens it, so a store
    // built after the socket is already tailing sees no history at all
    // unless it reads the journal itself — which is what a page reload is.
    const history = [
      event("session.spawn", { chat: true, sessionId: "chat-4" }, "agent-creator", 1),
      event("session.instruction", { sessionId: "chat-4", message: "old question" }, "agent-creator", 2),
      event(
        "agent.tool_use",
        { chat: true, sessionId: "chat-4", tool: "Grep", argsSummary: "TODO" },
        "agent-creator",
        3,
      ),
      event("session.finished", { chat: true, sessionId: "chat-4", finalResult: "old answer" }, "agent-creator", 4),
    ];
    const reloaded = new RegistryStore(stubClient(history) as any);

    await vi.waitFor(() => {
      const transcript = reloaded.getSnapshot().transcripts["agent-creator"] || [];
      expect(transcript.map((m) => m.role)).toEqual(["user", "tool", "agent"]);
    });
    expect(reloaded.getSnapshot().transcripts["agent-creator"][2].text).toBe("old answer");
  });

  it("waits for the socket before reading history on a cold start", async () => {
    // The store is built during the first render, while the client is still
    // connecting. Reading history then rejects instantly, and treating that
    // as "no history" left the transcript to be rebuilt from whatever the
    // subscription replay had streamed so far — a partial prefix.
    const history = [
      event("session.spawn", { chat: true, sessionId: "chat-6" }, "agent-creator", 1),
      event("session.instruction", { sessionId: "chat-6", message: "before the socket" }, "agent-creator", 2),
    ];
    const cold = stubClient(history);
    cold.getState = vi.fn(() => "connecting") as any;

    const store2 = new RegistryStore(cold as any);
    expect(cold.call).not.toHaveBeenCalledWith("chat.sessions", expect.anything());

    cold.getState = vi.fn(() => "connected") as any;
    cold.connect();

    await vi.waitFor(() => {
      expect(store2.getSnapshot().transcripts["agent-creator"]?.[0]?.text).toBe(
        "before the socket",
      );
    });
  });

  it("does not double-fold an event delivered by both history and the tail", async () => {
    const spawn = event("session.spawn", { chat: true, sessionId: "chat-5" }, "agent-creator", 10);
    const said = event("session.instruction", { sessionId: "chat-5", message: "once" }, "agent-creator", 11);
    const late = stubClient([spawn, said]);
    const reloaded = new RegistryStore(late as any);
    // The tail delivers the same span while the history read is in flight.
    late.emit(spawn);
    late.emit(said);

    await vi.waitFor(() => expect(reloaded.getSnapshot().loaded).toBe(true));
    expect(reloaded.getSnapshot().transcripts["agent-creator"]).toHaveLength(1);
  });

  it("ignores a replayed span instead of duplicating the transcript", async () => {
    await vi.waitFor(() => expect(store.getSnapshot().loaded).toBe(true));

    const spawn = event("session.spawn", { chat: true, sessionId: "chat-7" }, "agent-creator");
    const said = event("session.instruction", { sessionId: "chat-7", message: "hi" }, "agent-creator");
    client.emit(spawn);
    client.emit(said);
    // A reconnect can re-deliver a span the store has already folded.
    client.emit(spawn);
    client.emit(said);

    expect(store.getSnapshot().transcripts["agent-creator"]).toHaveLength(1);
  });

  it("stopping a session cancels it and ends the turn", async () => {
    await store.startChat("agent-creator", "go");
    expect(store.getSnapshot().running["agent-creator"]).toBe(true);

    await store.cancelChat("agent-creator");
    expect(client.call).toHaveBeenCalledWith("agent.session.cancel", { sessionId: "chat-1" });

    client.emit(
      event(
        "session.cancelled",
        { chat: true, sessionId: "chat-1", reason: "stopped by the operator" },
        "agent-creator",
      ),
    );
    const state = store.getSnapshot();
    expect(state.running["agent-creator"]).toBe(false);
    expect(state.sessionIds["agent-creator"]).toBeUndefined();
    expect(state.transcripts["agent-creator"].at(-1)?.role).toBe("system");
  });

  it("keeps one conversation together even when every turn respawns a session", () => {
    // Some adapters open a fresh provider session per turn. Grouping by
    // session id split a single conversation into one "chat" per reply.
    const m = (sessionId: string, role: any, text: string, at: string) => ({
      sessionId,
      agentId: "a",
      role,
      text,
      at,
    });
    const transcript = [
      m("s1", "user", "build me an agent", "2026-08-23T10:00:00.000Z"),
      m("s1", "agent", "which provider?", "2026-08-23T10:00:01.000Z"),
      m("s2", "user", "claude-code", "2026-08-23T10:00:02.000Z"),
      m("s2", "agent", "which skills?", "2026-08-23T10:00:03.000Z"),
      m("s3", "user", "code review", "2026-08-23T10:00:04.000Z"),
    ];

    const threads = threadsOf(transcript, []);
    expect(threads).toHaveLength(1);
    expect(threads[0].title).toBe("build me an agent");
    expect(threads[0].messageCount).toBe(5);
    expect(threads[0].sessionCount).toBe(3);
    expect(visibleMessages(transcript, [])).toHaveLength(5);
  });

  it("groups a transcript into one conversation per reset", () => {
    const msg = (role: any, text: string, at: string) => ({
      sessionId: "s1",
      agentId: "a",
      role,
      text,
      at,
    });
    const transcript = [
      msg("user", "first question" + String.fromCharCode(10) + "second line", "2026-08-23T10:00:00.000Z"),
      msg("tool", "Read", "2026-08-23T10:00:01.000Z"),
      msg("agent", "an answer", "2026-08-23T10:00:02.000Z"),
      msg("user", "a brand new topic", "2026-08-23T10:10:00.000Z"),
    ];
    const resets = ["2026-08-23T10:05:00.000Z"];

    const threads = threadsOf(transcript, resets);
    expect(threads).toHaveLength(2);
    expect(threads[0].title).toBe("first question");
    expect(threads[0].messageCount).toBe(3);
    expect(threads[0].toolCalls).toBe(1);
    expect(threads[1].title).toBe("a brand new topic");
  });

  it("shows one conversation at a time, and an empty screen after a reset", () => {
    const msg = (text: string, at: string) => ({
      sessionId: "s1",
      agentId: "a",
      role: "user" as const,
      text,
      at,
    });
    const transcript = [
      msg("old", "2026-08-23T10:00:00.000Z"),
      msg("new", "2026-08-23T10:10:00.000Z"),
    ];
    const resets = ["2026-08-23T10:05:00.000Z"];

    expect(visibleMessages(transcript, resets).map((x) => x.text)).toEqual(["new"]);
    expect(visibleMessages(transcript, resets, 0).map((x) => x.text)).toEqual(["old"]);
    // With no resets the whole history is a single conversation.
    expect(visibleMessages(transcript, [])).toHaveLength(2);
    // A reset with nothing said since wipes the screen, keeping the archive.
    const justReset = ["2026-08-23T10:20:00.000Z"];
    expect(visibleMessages(transcript, justReset)).toEqual([]);
    expect(threadsOf(transcript, justReset)[0].messageCount).toBe(2);
  });

  it("newChat wipes the screen but keeps the previous conversation", async () => {
    await store.startChat("agent-creator", "first conversation");
    await store.newChat("agent-creator");

    const state = store.getSnapshot();
    const transcript = state.transcripts["agent-creator"];
    const resets = state.resets["agent-creator"];
    expect(resets).toHaveLength(1);

    // Screen is empty...
    expect(visibleMessages(transcript, resets)).toEqual([]);
    // ...while the finished conversation is archived and reachable.
    expect(threadsOf(transcript, resets)[0].title).toBe("first conversation");
    expect(visibleMessages(transcript, resets, 0)).toHaveLength(1);

    store.viewThread("agent-creator", 0);
    expect(store.getSnapshot().activeThreads["agent-creator"]).toBe(0);
    store.viewThread("agent-creator", null);
    expect(store.getSnapshot().activeThreads["agent-creator"]).toBeUndefined();
  });

  it("answering a decision never opens a second session", async () => {
    await store.startChat("agent-creator", "build me an agent");
    client.emit(
      event(
        "agent.decision",
        {
          chat: true,
          sessionId: "chat-1",
          tool: "AskUserQuestion",
          prompt: "Which provider?",
          options: ["claude-code"],
        },
        "agent-creator",
      ),
    );
    const pending = store.getSnapshot().decisions["agent-creator"][0];
    const startsBefore = client.call.mock.calls.filter(
      (c: any[]) => c[0] === "agent.session.start",
    ).length;

    await store.answerDecision(pending, "claude-code");

    // The answer goes to the asking session, never a fresh one: a new
    // session has none of the interview and starts it over, which is what
    // produced the endless loop of the same questions.
    expect(client.call).toHaveBeenCalledWith("agent.session.send", {
      sessionId: "chat-1",
      message: "claude-code",
    });
    const startsAfter = client.call.mock.calls.filter(
      (c: any[]) => c[0] === "agent.session.start",
    ).length;
    expect(startsAfter).toBe(startsBefore);
  });

  it("newChat leaves another agent's drafts alone", async () => {
    const draft = { id: "d", name: "D", adapterId: "mock" } as any;
    client.emit(event("agent.proposal", { agent: draft }, "agent-creator"));
    client.emit(event("agent.proposal", { agent: draft }, "other-agent"));
    expect(store.getSnapshot().proposals).toHaveLength(2);

    await store.newChat("agent-creator");

    const left = store.getSnapshot().proposals;
    expect(left).toHaveLength(1);
    expect(left[0].agentId).toBe("other-agent");
  });

  it("newChat clears the pending question block", async () => {
    await store.startChat("agent-creator", "build me an agent");
    client.emit(
      event(
        "agent.decision",
        {
          chat: true,
          sessionId: "chat-1",
          tool: "AskUserQuestion",
          prompt: "Which provider?",
          options: ["claude-code", "antigravity-agy"],
        },
        "agent-creator",
      ),
    );
    expect(store.getSnapshot().decisions["agent-creator"]).toHaveLength(1);

    await store.newChat("agent-creator");

    // The question belonged to the conversation that just ended, and its
    // session is gone — answering it could only fail.
    expect(store.getSnapshot().decisions["agent-creator"] || []).toHaveLength(0);
  });

  it("newChat ends the live session so the next message opens a fresh one", async () => {
    await store.startChat("agent-creator", "first conversation");
    expect(store.getSnapshot().sessionIds["agent-creator"]).toBe("chat-1");

    await store.newChat("agent-creator");
    expect(client.call).toHaveBeenCalledWith("agent.session.cancel", { sessionId: "chat-1" });
    // No live session: the next message must spawn one instead of resuming.
    expect(store.getSnapshot().sessionIds["agent-creator"]).toBeUndefined();
    expect(store.getSnapshot().running["agent-creator"]).toBe(false);
    // The history stays — the journal is the record, not the view.
    expect(store.getSnapshot().transcripts["agent-creator"]).toHaveLength(1);
  });

  it("newChat on an already-dead session still clears it", async () => {
    await store.startChat("agent-creator", "hello");
    client.call.mockRejectedValueOnce(new Error("session chat-1 not found"));

    await expect(store.newChat("agent-creator")).resolves.toBeUndefined();
    expect(store.getSnapshot().sessionIds["agent-creator"]).toBeUndefined();
  });

  it("a daemon restart drops dead session handles but keeps the transcript", async () => {
    await store.startChat("agent-creator", "hello");
    client.emit(event("daemon.started", {}, null));

    expect(store.getSnapshot().sessionIds["agent-creator"]).toBeUndefined();
    expect(store.getSnapshot().transcripts["agent-creator"]).toHaveLength(1);
  });

  it("confirms the optimistic echo in place instead of duplicating it", async () => {
    await store.startChat("agent-creator", "hello");
    expect(store.getSnapshot().transcripts["agent-creator"][0].pending).toBe(true);

    client.emit(
      event("session.instruction", { chat: true, sessionId: "chat-1", message: "hello" }, "agent-creator"),
    );

    const transcript = store.getSnapshot().transcripts["agent-creator"];
    expect(transcript).toHaveLength(1);
    expect(transcript[0].pending).toBe(false);
  });

  it("collects valid proposals and invalid-proposal reasons", () => {
    const draft = {
      id: "sql-reviewer",
      name: "SQL Reviewer",
      description: "reviews migrations",
      adapterId: "claude-code",
      model: "claude-sonnet-5",
      effort: null,
      mode: "plan" as const,
      skills: [],
      toolAllowlist: [],
      toolDenylist: [],
      timeoutSecs: 600,
      builtin: false,
      enabled: true,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    client.emit(event("agent.proposal", { agent: draft }, "agent-creator"));
    client.emit(
      event("agent.proposal_invalid", { reason: "id too short" }, "agent-creator"),
    );

    expect(store.getSnapshot().proposals).toHaveLength(1);
    expect(store.getSnapshot().proposals[0].draft.id).toBe("sql-reviewer");
    // Tagged by producer, so resetting one agent cannot discard another's.
    expect(store.getSnapshot().proposals[0].agentId).toBe("agent-creator");
    expect(store.getSnapshot().proposalInvalid.at(-1)?.reason).toBe("id too short");

    store.dismissProposal(0);
    expect(store.getSnapshot().proposals).toHaveLength(0);
  });

  it("refetches the agent list (debounced) after registry mutations", async () => {
    await vi.waitFor(() => expect(store.getSnapshot().loaded).toBe(true));
    expect(client.call).toHaveBeenCalledWith("registry.agents.list", {});

    client.emit(event("agent.created", { agent: {} }, "new-agent"));
    await vi.waitFor(() =>
      expect(client.call.mock.calls.filter(([m]) => m === "registry.agents.list").length).toBeGreaterThanOrEqual(2),
    );
  });

  it("folds plan-mode decisions and answers them as instructions", async () => {
    await store.startChat("agent-creator", "plan it");
    client.emit(
      event(
        "agent.decision",
        {
          chat: true,
          sessionId: "chat-1",
          tool: "AskUserQuestion",
          prompt: "Which database?",
          options: ["Postgres", "SQLite"],
          multiSelect: false,
        },
        "agent-creator",
      ),
    );
    const pending = store.getSnapshot().decisions["agent-creator"];
    expect(pending).toHaveLength(1);
    expect(pending[0].options).toEqual(["Postgres", "SQLite"]);

    await store.answerDecision(pending[0], "Postgres");
    expect(client.call).toHaveBeenCalledWith("agent.session.send", {
      sessionId: "chat-1",
      message: "Postgres",
    });
    expect(store.getSnapshot().decisions["agent-creator"]).toHaveLength(0);
    expect(store.getSnapshot().transcripts["agent-creator"].at(-1)?.text).toBe("Postgres");
  });

  it("ignores decisions with no options", () => {
    client.emit(
      event(
        "agent.decision",
        { chat: true, sessionId: "chat-1", tool: "AskUserQuestion", prompt: "?", options: [] },
        "agent-creator",
      ),
    );
    expect(store.getSnapshot().decisions["agent-creator"] || []).toHaveLength(0);
  });

  it("startChat sends the RPC and mirrors the user message locally", async () => {
    const sessionId = await store.startChat("agent-creator", "hello");
    expect(sessionId).toBe("chat-1");
    expect(client.call).toHaveBeenCalledWith("agent.session.start", {
      agentId: "agent-creator",
      message: "hello",
    });
    expect(store.getSnapshot().transcripts["agent-creator"].at(-1)?.text).toBe("hello");
  });
});
