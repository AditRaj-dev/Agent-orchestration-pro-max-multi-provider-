// F-13 Agent Registry Store: registry agents/skills/catalog plus chat
// transcripts, fed by the daemon's registry.* / agent.session.* surface.
//
// The journal-folded store (store.ts) stays untouched: the registry is a
// mutable table, not a projection, so this store bootstraps via RPC and
// refreshes on `agent.*` journal events (the daemon's audit trail) — one
// refetch per mutation, not an incremental fold.
import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import { getDaemonWsClient } from "../daemon/ws";
import type {
  AgentRecord,
  ChatMessage,
  ChatSessionSummary,
  ChatThread,
  ChatSessionState,
  DecisionPrompt,
  EventWire,
  ProviderCatalogEntry,
  SkillRecord,
} from "../types";

/**
 * Where the user's "New chat" resets are remembered. Conversation
 * boundaries are a view concern, not domain data — the journal already
 * holds every turn — so they live in local storage rather than becoming a
 * new event type.
 */
const RESETS_KEY = "agentos.chat.resets";

function loadResets(): Record<string, string[]> {
  try {
    const raw = globalThis.localStorage?.getItem(RESETS_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function saveResets(resets: Record<string, string[]>): void {
  try {
    globalThis.localStorage?.setItem(RESETS_KEY, JSON.stringify(resets));
  } catch {
    // Private mode or a full quota: boundaries degrade to this session.
  }
}

/**
 * How many past conversations to keep per agent in the session list. The
 * daemon folds the whole journal; the UI only ever shows a strip of it.
 */
const HISTORY_SESSION_LIMIT = 40;

export interface RegistryState {
  agents: AgentRecord[];
  skills: SkillRecord[];
  catalog: ProviderCatalogEntry[];
  loaded: boolean;
  error: string | null;
  transcripts: Record<string, ChatMessage[]>; // agentId -> messages
  /** Latest agent.proposal drafts, newest last, tagged by producer. */
  proposals: { agentId: string; draft: AgentRecord }[];
  proposalInvalid: { agentId: string; reason: string; at: string }[];
  /** agentId -> past conversations, newest first (`chat.sessions`). */
  history: Record<string, ChatSessionSummary[]>;
  /** agentId -> the past session whose transcript is on screen, if any. */
  viewing: Record<string, string | null>;
  /** Skill drafts a creator wants installed, tagged by producer. */
  skillProposals: { agentId: string; draft: SkillRecord }[];
  sessionIds: Record<string, string>; // agentId -> active chat session id
  running: Record<string, boolean>; // agentId -> a turn is in flight
  /**
   * agentId -> index of the conversation the log is showing, or absent for
   * "the current one" (the last).
   */
  activeThreads: Record<string, number>;
  /** agentId -> "New chat" timestamps, ascending: the conversation splits. */
  resets: Record<string, string[]>;
  decisions: Record<string, DecisionPrompt[]>; // agentId -> unanswered choices
}

function emptyState(): RegistryState {
  return {
    agents: [],
    skills: [],
    catalog: [],
    loaded: false,
    error: null,
    transcripts: {},
    proposals: [],
    proposalInvalid: [],
    history: {},
    viewing: {},
    skillProposals: [],
    sessionIds: {},
    running: {},
    activeThreads: {},
    resets: loadResets(),
    decisions: {},
  };
}

export class RegistryStore {
  private state: RegistryState = emptyState();
  private listeners = new Set<() => void>();
  private unsubscribeEvents: (() => void) | null = null;
  private unsubscribeState: (() => void) | null = null;
  private refreshTimer: any = null;
  private lastSeq = 0;
  private historyLoaded = false;
  private historyPromise: Promise<void> | null = null;
  private tailBuffer: EventWire[] = [];

  constructor(client = getDaemonWsClient()) {
    this.client = client;
    this.unsubscribeState = client.onStateChange((state) => {
      if (state === "connected") {
        this.refresh();
        this.loadHistory();
      }
    });
    // Registry mutations and chat traffic arrive as journal events.
    //
    // The shared subscription only replays for whoever opens it, and this
    // store is constructed late (when the Agents view first mounts), so the
    // tail alone would start the transcript at "now" and every earlier
    // conversation would be lost from the UI while sitting in the journal.
    // History is therefore read explicitly, and the tail is buffered until
    // it lands so the two cannot interleave out of order.
    this.unsubscribeEvents = client.subscribe(0, (event: EventWire) => {
      if (!this.historyLoaded) {
        this.tailBuffer.push(event);
        return;
      }
      this.ingest(event);
    });
    // Only fetch once the socket is up. Calling these from the constructor
    // rejects immediately on a cold start (the client is still connecting),
    // which used to mark the history "loaded" after reading nothing and
    // leave the store rebuilding itself from the subscription replay — so
    // the UI showed a partial prefix of the journal until the tail caught
    // up, with sessions that had long since ended still looking live.
    if (client.getState() === "connected") {
      this.refresh();
      this.loadHistory();
    }
  }

  /** Fold one journal event, ignoring spans already folded. */
  private ingest(event: EventWire): void {
    // A reconnect replays a span the store has already seen; the transcript
    // is append-only, so a replayed event must not append a second copy.
    if (event.seq > 0) {
      if (event.seq <= this.lastSeq) return;
      this.lastSeq = event.seq;
    }
    this.applyEvent(
      event.eventType,
      event.agentId,
      event.payload || {},
      event.occurredAt,
    );
  }

  /**
   * Page the journal from the beginning and fold it, then release whatever
   * the live tail buffered meanwhile. Chat history is the journal — nothing
   * is stored twice, and a reload rebuilds every transcript from it.
   */
  private loadHistory(): Promise<void> {
    if (!this.historyPromise) this.historyPromise = this.readHistory();
    return this.historyPromise;
  }

  private async readHistory(): Promise<void> {
    try {
      // Server-side fold: the daemon already folds the journal into chat
      // sessions and transcripts (F-13b `chat.sessions`/`chat.transcript`),
      // so the browser asks for conversations instead of paging thousands
      // of raw events and folding them itself.
      const res = await this.client.call<{ sessions: ChatSessionSummary[] }>(
        "chat.sessions",
        {},
      );
      const sessions = (res?.sessions || []).slice(0, HISTORY_SESSION_LIMIT);
      const history: Record<string, ChatSessionSummary[]> = {};
      for (const session of sessions) {
        (history[session.agentId] ||= []).push(session);
      }
      this.setState({ history });

      // Everything up to the newest folded event is already on screen, so
      // the buffered tail must not fold it a second time. Without this the
      // same turn appears twice: once from the daemon's fold, once from the
      // replay the subscription was holding.
      for (const session of sessions) {
        if (session.lastSeq > this.lastSeq) this.lastSeq = session.lastSeq;
      }

      // Hydrate the newest conversation per agent so switching agents
      // shows something immediately; older ones load when clicked.
      const newest = Object.values(history)
        .map((rows) => rows[0])
        .filter(Boolean);
      await Promise.all(
        newest.map((session) =>
          this.loadTranscript(session.agentId, session.sessionId).catch(() => {
            // One unreadable conversation must not blank the others.
          }),
        ),
      );
    } catch {
      // No history is survivable: the live tail still works, and the
      // transcript simply starts at this session.
    } finally {
      this.historyLoaded = true;
      const buffered = this.tailBuffer;
      this.tailBuffer = [];
      for (const event of buffered) this.ingest(event);
    }
  }

  /** Fetch one conversation's transcript and put it on screen. */
  private async loadTranscript(agentId: string, sessionId: string): Promise<void> {
    const res = await this.client.call<{ messages: ChatMessage[] }>(
      "chat.transcript",
      { sessionId },
    );
    const messages = (res?.messages || []).map((message) => ({
      ...message,
      agentId,
    }));
    this.setState({
      transcripts: { ...this.state.transcripts, [agentId]: messages },
    });
  }

  /** Refresh an agent's conversation list (after a new chat, say). */
  async refreshHistory(agentId: string): Promise<ChatSessionSummary[]> {
    try {
      const res = await this.client.call<{ sessions: ChatSessionSummary[] }>(
        "chat.sessions",
        { agentId },
      );
      const sessions = (res?.sessions || []).slice(0, HISTORY_SESSION_LIMIT);
      this.setState({ history: { ...this.state.history, [agentId]: sessions } });
      return sessions;
    } catch {
      return this.state.history[agentId] || [];
    }
  }

  /**
   * Put a past conversation on screen. It is history, not a live session:
   * `viewing` marks it so the composer offers to CONTINUE it rather than
   * silently talking into a session that ended.
   */
  async openHistorySession(agentId: string, sessionId: string): Promise<void> {
    await this.loadTranscript(agentId, sessionId);
    this.setState({
      viewing: { ...this.state.viewing, [agentId]: sessionId },
      sessionIds: { ...this.state.sessionIds, [agentId]: "" },
    });
  }

  /** Leave the history view and return to the live conversation. */
  clearViewing(agentId: string): void {
    this.setState({ viewing: { ...this.state.viewing, [agentId]: null } });
  }

  /**
   * Continue a past conversation. The daemon opens a NEW session bound to
   * the old provider session, so the agent still remembers everything in
   * the transcript above the composer.
   */
  async reopenSession(
    agentId: string,
    sessionId: string,
    message: string,
  ): Promise<string> {
    const res = await this.client.call<{ sessionId: string }>(
      "agent.session.reopen",
      { sessionId, message },
    );
    this.setState({
      sessionIds: { ...this.state.sessionIds, [agentId]: res.sessionId },
      viewing: { ...this.state.viewing, [agentId]: null },
    });
    void this.refreshHistory(agentId);
    return res.sessionId;
  }

  private client: ReturnType<typeof getDaemonWsClient>;

  destroy(): void {
    if (this.unsubscribeEvents) this.unsubscribeEvents();
    if (this.unsubscribeState) this.unsubscribeState();
    if (this.refreshTimer) clearTimeout(this.refreshTimer);
  }

  private setState(patch: Partial<RegistryState>): void {
    this.state = { ...this.state, ...patch };
    this.listeners.forEach((listener) => listener());
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getSnapshot = (): RegistryState => this.state;

  getClient() {
    return this.client;
  }

  /** Fetch agents + skills + catalog from the daemon. */
  async refresh(): Promise<void> {
    try {
      const [agentsRes, skillsRes, catalogRes] = await Promise.allSettled([
        this.client.call<{ agents: AgentRecord[] }>("registry.agents.list", {}),
        this.client.call<{ skills: SkillRecord[] }>("registry.skills.list", {}),
        this.client.call<{ providers: ProviderCatalogEntry[] }>("registry.catalog", {}),
      ]);
      const patch: Partial<RegistryState> = { loaded: true, error: null };
      if (agentsRes.status === "fulfilled" && agentsRes.value?.agents) {
        patch.agents = agentsRes.value.agents;
      }
      if (skillsRes.status === "fulfilled" && skillsRes.value?.skills) {
        patch.skills = skillsRes.value.skills;
      }
      if (catalogRes.status === "fulfilled" && catalogRes.value?.providers) {
        patch.catalog = catalogRes.value.providers;
      }
      if (
        agentsRes.status === "rejected" &&
        (agentsRes as PromiseRejectedResult).reason
      ) {
        const reason = (agentsRes as PromiseRejectedResult).reason as {
          message?: string;
        };
        // Old daemon without F-13: surface a hint instead of a dead view.
        patch.error =
          reason.message || "registry.agents.list failed (daemon without F-13?)";
      }
      this.setState(patch);
    } catch (err: any) {
      this.setState({ error: err?.message || String(err) });
    }
  }

  /**
   * Fold registry/chat journal events into local state. `occurredAt` is the
   * event's own timestamp — replayed history must carry the time the turn
   * happened, not the time the page was reloaded.
   */
  applyEvent(
    eventType: string,
    agentId: string | null,
    payload: Record<string, any>,
    occurredAt?: string,
  ): void {
    const at = occurredAt || new Date().toISOString();
    const coalesce = (): void => {
      // Mutations trigger a debounced refetch (the table is authoritative).
      if (this.refreshTimer) clearTimeout(this.refreshTimer);
      this.refreshTimer = setTimeout(() => {
        this.refreshTimer = null;
        this.refresh();
      }, 150);
    };

    switch (eventType) {
      case "agent.created":
      case "agent.updated":
      case "agent.deleted":
        coalesce();
        return;
      // The daemon restarted: every session id in the journal belongs to a
      // dead process, so keep the transcripts and drop the live handles.
      case "daemon.started":
        this.setState({ sessionIds: {}, running: {} });
        return;
      case "session.spawn": {
        if (!agentId || !payload.chat) return;
        const sessionId: string | undefined = payload.sessionId;
        if (!sessionId) return;
        this.setState({
          sessionIds: { ...this.state.sessionIds, [agentId]: sessionId },
          running: { ...this.state.running, [agentId]: true },
        });
        return;
      }
      case "session.instruction": {
        if (!agentId) return;
        this.appendMessage(agentId, {
          sessionId: String(payload.sessionId || ""),
          agentId,
          role: "user",
          text: String(payload.message || ""),
          at,
        });
        this.setRunning(agentId, true);
        return;
      }
      // One adapter tool call. `chat` separates chat traffic from the
      // supervisor's run traffic, which shares this event type.
      case "agent.tool_use": {
        if (!agentId || !payload.chat) return;
        const tool = String(payload.tool || "tool");
        this.appendMessage(agentId, {
          sessionId: String(payload.sessionId || ""),
          agentId,
          role: "tool",
          tool,
          text: String(payload.argsSummary || ""),
          at,
        });
        return;
      }
      case "agent.rate_limit": {
        if (!agentId || !payload.chat) return;
        this.appendMessage(agentId, {
          sessionId: String(payload.sessionId || ""),
          agentId,
          role: "system",
          text: `Provider rate limit: ${payload.providerNotice || "throttled"}`,
          at,
        });
        return;
      }
      case "session.finished": {
        if (!agentId || !payload.chat) return;
        // The turn is over either way; only the text is conditional.
        this.setRunning(agentId, false);
        const text = String(payload.finalResult || "");
        if (!text.trim()) return;
        this.appendMessage(agentId, {
          sessionId: String(payload.sessionId || ""),
          agentId,
          role: "agent",
          text,
          at,
        });
        return;
      }
      case "session.cancelled": {
        if (!agentId || !payload.chat) return;
        this.appendMessage(agentId, {
          sessionId: String(payload.sessionId || ""),
          agentId,
          role: "system",
          text: `Session stopped (${payload.reason || "cancelled"}).`,
          at,
        });
        this.endSession(agentId, String(payload.sessionId || ""));
        return;
      }
      case "agent.session_failed":
      case "agent.spawn_failed": {
        if (!agentId || !payload.chat) return;
        this.appendMessage(agentId, {
          sessionId: String(payload.sessionId || ""),
          agentId,
          role: "system",
          text: `Session failed: ${payload.error || "unknown error"}`,
          at,
        });
        this.endSession(agentId, String(payload.sessionId || ""));
        return;
      }
      case "agent.decision": {
        if (!agentId || !payload.chat) return;
        const options: string[] = Array.isArray(payload.options) ? payload.options : [];
        if (options.length === 0) return;
        const pending = this.state.decisions[agentId] || [];
        this.setState({
          decisions: {
            ...this.state.decisions,
            [agentId]: [
              ...pending,
              {
                sessionId: String(payload.sessionId || ""),
                agentId,
                tool: String(payload.tool || ""),
                prompt: String(payload.prompt || ""),
                options,
                multiSelect: Boolean(payload.multiSelect),
                at,
              },
            ],
          },
        });
        return;
      }
      // A creator drafted a skill to install. Installing is the human's
      // call, exactly like registering an agent.
      case "skill.proposal": {
        const draft = payload.skill as SkillRecord | undefined;
        if (!draft || !agentId) return;
        this.setState({
          skillProposals: [...this.state.skillProposals, { agentId, draft }],
        });
        return;
      }
      case "skill.created":
      case "skill.updated":
      case "skill.deleted":
        coalesce();
        return;
      case "agent.proposal": {
        const draft = payload.agent as AgentRecord | undefined;
        if (!draft || !agentId) return;
        this.setState({
          proposals: [...this.state.proposals, { agentId, draft }],
        });
        return;
      }
      case "agent.proposal_invalid": {
        if (!agentId) return;
        this.setState({
          proposalInvalid: [
            ...this.state.proposalInvalid,
            { agentId, reason: String(payload.reason || "invalid proposal"), at },
          ],
        });
        return;
      }
      default:
        return;
    }
  }

  private appendMessage(agentId: string, message: ChatMessage): void {
    const existing = this.state.transcripts[agentId] || [];
    // A user message is echoed locally the moment it is sent and again when
    // its `session.instruction` lands in the journal. Confirm the echo in
    // place rather than showing the same line twice.
    let next: ChatMessage[];
    const echo =
      message.role === "user"
        ? existing.findIndex((m) => m.pending && m.role === "user" && m.text === message.text)
        : -1;
    if (echo >= 0) {
      next = existing.slice();
      next[echo] = { ...message, pending: false };
    } else {
      next = [...existing, message];
    }
    this.setState({
      transcripts: { ...this.state.transcripts, [agentId]: next },
    });
  }

  private setRunning(agentId: string, running: boolean): void {
    if (Boolean(this.state.running[agentId]) === running) return;
    this.setState({ running: { ...this.state.running, [agentId]: running } });
  }

  /** Terminal event: the turn stopped and the session id is spent. */
  private endSession(agentId: string, sessionId: string): void {
    const sessionIds = { ...this.state.sessionIds };
    if (!sessionId || sessionIds[agentId] === sessionId) delete sessionIds[agentId];
    this.setState({
      sessionIds,
      running: { ...this.state.running, [agentId]: false },
    });
  }

  /** Start a chat session with a registry agent. */
  async startChat(agentId: string, message: string): Promise<string> {
    const res = await this.client.call<{ sessionId: string }>(
      "agent.session.start",
      { agentId, message },
    );
    this.appendMessage(agentId, {
      sessionId: res.sessionId,
      agentId,
      role: "user",
      text: message,
      at: new Date().toISOString(),
      pending: true,
    });
    this.setState({
      sessionIds: { ...this.state.sessionIds, [agentId]: res.sessionId },
      running: { ...this.state.running, [agentId]: true },
    });
    return res.sessionId;
  }

  /** Follow-up on a live session. */
  async sendChat(sessionId: string, agentId: string, message: string): Promise<void> {
    await this.client.call("agent.session.send", { sessionId, message });
    this.appendMessage(agentId, {
      sessionId,
      agentId,
      role: "user",
      text: message,
      at: new Date().toISOString(),
      pending: true,
    });
    this.setRunning(agentId, true);
  }

  /**
   * Stop the agent mid-turn. The daemon kills the provider process and
   * journals `session.cancelled`, which is what clears the running flag —
   * except when the session is already gone, where the 404 IS the answer.
   */
  async cancelChat(agentId: string): Promise<void> {
    const sessionId = this.state.sessionIds[agentId];
    if (!sessionId) {
      this.setRunning(agentId, false);
      return;
    }
    try {
      await this.client.call("agent.session.cancel", { sessionId });
    } catch (err: any) {
      this.appendMessage(agentId, {
        sessionId,
        agentId,
        role: "system",
        text: `Nothing to stop — the session had already ended (${err?.message || err}).`,
        at: new Date().toISOString(),
      });
      this.endSession(agentId, sessionId);
    }
  }

  /**
   * Begin a fresh conversation: end whatever session is live so the next
   * message cannot resume it, and drop the id so `startChat` spawns a new
   * one. The transcript is left alone — the journal is the history, and the
   * new session simply opens under its own divider.
   */
  async newChat(agentId: string): Promise<void> {
    const sessionId = this.state.sessionIds[agentId];
    if (sessionId) {
      try {
        await this.client.call("agent.session.cancel", { sessionId });
      } catch {
        // Already finished or timed out: that is the outcome we wanted.
      }
    }
    this.endSession(agentId, sessionId || "");
    // Mark the boundary. The transcript is untouched — everything said so
    // far becomes the previous conversation, and the current one starts
    // empty, which is what wipes the screen.
    // Anchor the boundary strictly after everything said so far. Using the
    // wall clock alone puts a message and the reset in the same millisecond
    // on a fast exchange, and the message lands on the wrong side.
    const transcript = this.state.transcripts[agentId] || [];
    const lastAt = transcript[transcript.length - 1]?.at;
    const marker = new Date(
      Math.max(Date.now(), lastAt ? Date.parse(lastAt) : 0) + 1,
    ).toISOString();
    const resets = {
      ...this.state.resets,
      [agentId]: [...(this.state.resets[agentId] || []), marker],
    };
    saveResets(resets);
    const activeThreads = { ...this.state.activeThreads };
    delete activeThreads[agentId]; // follow the new current conversation
    // The pending questions belonged to the conversation that just ended,
    // and its session is gone — answering them would fail. A fresh chat
    // starts with nothing outstanding.
    const decisions = { ...this.state.decisions };
    delete decisions[agentId];
    this.setState({
      resets,
      activeThreads,
      decisions,
      // Drafts and warnings from the ended conversation go too — but only
      // this agent's, so resetting one chat cannot discard another's work.
      proposals: this.state.proposals.filter((p) => p.agentId !== agentId),
      proposalInvalid: this.state.proposalInvalid.filter((p) => p.agentId !== agentId),
      skillProposals: this.state.skillProposals.filter((p) => p.agentId !== agentId),
    });
  }

  /** Show one conversation by index, or `null` for the current one. */
  viewThread(agentId: string, index: number | null): void {
    const activeThreads = { ...this.state.activeThreads };
    if (index === null) delete activeThreads[agentId];
    else activeThreads[agentId] = index;
    this.setState({ activeThreads });
  }

  /** Forget an agent's transcript (the journal keeps the real record). */
  clearTranscript(agentId: string): void {
    const transcripts = { ...this.state.transcripts };
    delete transcripts[agentId];
    const activeThreads = { ...this.state.activeThreads };
    delete activeThreads[agentId];
    const resets = { ...this.state.resets };
    delete resets[agentId];
    saveResets(resets);
    this.setState({ transcripts, activeThreads, resets });
  }

  /**
   * Answer a plan-mode choice: the option label goes back as an ordinary
   * instruction, and the prompt leaves the pending list. Clicking is the
   * only way a decision clears — a finished session still shows its
   * question, because plan-mode agents ask and then end their turn.
   */
  async answerDecision(decision: DecisionPrompt, option: string): Promise<void> {
    this.clearDecision(decision);
    // The answer belongs to the conversation that asked the question. If
    // the live session cannot take it, say so — starting a fresh session
    // here is what produced the question loop: the new agent had none of
    // the conversation, so it asked its opening questions again, and every
    // answer spawned another session.
    const sessionId = decision.sessionId || this.state.sessionIds[decision.agentId];
    if (sessionId) {
      await this.sendChat(sessionId, decision.agentId, option);
      return;
    }
    await this.startChat(decision.agentId, option);
  }

  /** Drop a pending choice without answering it. */
  clearDecision(decision: DecisionPrompt): void {
    const pending = this.state.decisions[decision.agentId] || [];
    this.setState({
      decisions: {
        ...this.state.decisions,
        [decision.agentId]: pending.filter((entry) => entry !== decision),
      },
    });
  }

  /** Registry mutations (typed wrappers over the F-13 methods). */
  async createAgent(agent: Partial<AgentRecord> & { id: string; name: string; adapterId: string }): Promise<AgentRecord> {
    const res = await this.client.call<{ agent: AgentRecord }>("registry.agents.create", {
      agent,
    });
    await this.refresh();
    return res.agent;
  }

  async updateAgent(agent: AgentRecord): Promise<AgentRecord> {
    const res = await this.client.call<{ agent: AgentRecord }>("registry.agents.update", {
      agent,
    });
    await this.refresh();
    return res.agent;
  }

  async deleteAgent(id: string): Promise<void> {
    await this.client.call("registry.agents.delete", { id });
    await this.refresh();
  }

  async setAgentEnabled(id: string, enabled: boolean): Promise<void> {
    await this.client.call("registry.agents.set-enabled", { id, enabled });
    await this.refresh();
  }

  /**
   * Install a drafted skill. Once it exists it enters the catalog every
   * proposing agent is handed, so the next agent draft may name it.
   */
  async installSkill(draft: Partial<SkillRecord> & { id: string; name: string; body: string }): Promise<SkillRecord> {
    const res = await this.client.call<{ skill: SkillRecord }>("registry.skills.create", {
      skill: {
        description: "",
        builtin: false,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        ...draft,
      },
    });
    await this.refresh();
    return res.skill;
  }

  async updateSkill(skill: SkillRecord): Promise<SkillRecord> {
    const res = await this.client.call<{ skill: SkillRecord }>("registry.skills.update", {
      skill,
    });
    await this.refresh();
    return res.skill;
  }

  async deleteSkill(id: string): Promise<void> {
    await this.client.call("registry.skills.delete", { id });
    await this.refresh();
  }

  /** Drop a pending skill draft from the local list. */
  dismissSkillProposal(index: number): void {
    this.setState({
      skillProposals: this.state.skillProposals.filter((_, i) => i !== index),
    });
  }

  /** Drop a pending proposal from the local list (dismissed in the UI). */
  dismissProposal(index: number): void {
    this.setState({ proposals: this.state.proposals.filter((_, i) => i !== index) });
  }
}

// Global singleton (mirrors DesktopStore's pattern)
let globalRegistryStore: RegistryStore | null = null;

export function getRegistryStore(): RegistryStore {
  if (!globalRegistryStore) {
    globalRegistryStore = new RegistryStore();
  }
  return globalRegistryStore;
}

// -------------------------------------------------------- conversations

/**
 * Split a transcript into conversations, one per provider session, newest
 * last. Messages already arrive in journal order, so a session's block is
 * contiguous and grouping is a single pass.
 */
export function segmentsOf(
  transcript: ChatMessage[],
  resets: string[] = [],
): ChatMessage[][] {
  // One segment per reset boundary, plus the trailing current one. That
  // trailing segment is empty right after "New chat" — the empty screen is
  // the point — and fills up again on the next message.
  const segments: ChatMessage[][] = Array.from(
    { length: resets.length + 1 },
    () => [],
  );
  for (const message of transcript) {
    let index = 0;
    while (index < resets.length && message.at >= resets[index]) index += 1;
    segments[index].push(message);
  }
  return segments;
}

/**
 * The agent's conversations, oldest first. A conversation is everything
 * between two resets, spanning as many provider sessions as it needed.
 */
export function threadsOf(
  transcript: ChatMessage[],
  resets: string[] = [],
): ChatThread[] {
  return segmentsOf(transcript, resets).map((messages, index) => {
    const opener = messages.find((m) => m.role === "user" && m.text.trim());
    const firstLine = opener ? opener.text.trim().split("\n")[0] : "";
    return {
      index,
      title: firstLine ? firstLine.slice(0, 60) : "(empty)",
      startedAt: messages[0]?.at || resets[index - 1] || "",
      messageCount: messages.length,
      toolCalls: messages.filter((m) => m.role === "tool").length,
      sessionCount: new Set(messages.map((m) => m.sessionId)).size,
    };
  });
}

/** The messages the log should render for the selected conversation. */
export function visibleMessages(
  transcript: ChatMessage[],
  resets: string[] = [],
  activeThread?: number,
): ChatMessage[] {
  const segments = segmentsOf(transcript, resets);
  const index = activeThread ?? segments.length - 1;
  return segments[index] || [];
}


// ------------------------------------------------------------- hooks

export function useRegistryState(): RegistryState {
  const store = getRegistryStore();
  return useSyncExternalStore(store.subscribe, store.getSnapshot);
}

export function useRegistryAgents(): AgentRecord[] {
  return useRegistryState().agents;
}

export function useRegistrySkills(): SkillRecord[] {
  return useRegistryState().skills;
}

export function useRegistryCatalog(): ProviderCatalogEntry[] {
  return useRegistryState().catalog;
}

export function useChatTranscript(agentId: string | null): ChatMessage[] {
  const state = useRegistryState();
  if (!agentId) return [];
  return state.transcripts[agentId] || [];
}

/** Past and present conversations with an agent, oldest first. */
export function useChatThreads(agentId: string | null): ChatThread[] {
  const state = useRegistryState();
  return useMemo(
    () =>
      agentId
        ? threadsOf(state.transcripts[agentId] || [], state.resets[agentId] || [])
        : [],
    [agentId, state.transcripts, state.resets],
  );
}

/** Which conversation the log is showing (undefined = the current one). */
export function useActiveThread(agentId: string | null): number | undefined {
  const state = useRegistryState();
  if (!agentId) return undefined;
  return state.activeThreads[agentId];
}

/** Only the selected conversation's messages. */
export function useVisibleTranscript(agentId: string | null): ChatMessage[] {
  const state = useRegistryState();
  return useMemo(() => {
    if (!agentId) return [];
    return visibleMessages(
      state.transcripts[agentId] || [],
      state.resets[agentId] || [],
      state.activeThreads[agentId],
    );
  }, [agentId, state.transcripts, state.resets, state.activeThreads]);
}

/** An agent's past conversations, newest first. */
export function useAgentHistory(agentId: string | null): ChatSessionSummary[] {
  const state = useRegistryState();
  if (!agentId) return [];
  return state.history[agentId] || [];
}

/** The past session being viewed for an agent, if any. */
export function useViewingSession(agentId: string | null): string | null {
  const state = useRegistryState();
  if (!agentId) return null;
  return state.viewing[agentId] || null;
}

/** Unanswered plan-mode choices for an agent, oldest first. */
export function useAgentDecisions(agentId: string | null): DecisionPrompt[] {
  const state = useRegistryState();
  if (!agentId) return [];
  return state.decisions[agentId] || [];
}

export function useChatSessionId(agentId: string | null): string | null {
  const state = useRegistryState();
  if (!agentId) return null;
  return state.sessionIds[agentId] || null;
}

/** Session id + whether a turn is currently in flight. */
export function useChatSession(agentId: string | null): ChatSessionState {
  const state = useRegistryState();
  if (!agentId) return { sessionId: null, running: false };
  return {
    sessionId: state.sessionIds[agentId] || null,
    running: Boolean(state.running[agentId]),
  };
}

/** Connection state for gating the chat send button. */
export function useRegistryConnected(): boolean {
  const [connected, setConnected] = useState(false);
  useEffect(() => {
    const client = getRegistryStore().getClient();
    setConnected(client.getState() === "connected");
    return client.onStateChange((state) => {
      setConnected(state === "connected");
    });
  }, []);
  return connected;
}
