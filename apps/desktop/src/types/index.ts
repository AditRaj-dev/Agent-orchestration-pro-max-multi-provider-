// F-11 Desktop Types — reflects docs/F-11-desktop.md §2, §3, §4

export type RunStatus = "running" | "completed" | "failed";

export type TaskState =
  | "created"
  | "ready"
  | "leased"
  | "running"
  | "output_ready"
  | "review_pending"
  | "approved"
  | "git_queued"
  | "committed"
  | "done"
  | "failed"
  | "human_required"
  | string;

export type AgentStatus =
  | "idle"
  | "planning"
  | "running"
  | "waiting"
  | "reviewing"
  | "blocked"
  | "failed"
  | "complete";

export interface EventWire {
  seq: number;
  id: string;
  eventType: string;
  occurredAt: string;
  runId: string | null;
  traceId: string | null;
  taskId: string | null;
  agentId: string | null;
  payload: Record<string, any> | null;
  payloadRef: string | null;
  payloadHash: string | null;
  schemaVersion: number;
}

export interface TaskCounts {
  total: number;
  done: number;
  failed: number;
  active: number;
}

export interface RunSummary {
  runId: string;
  status: RunStatus;
  workflowId: string | null;
  taskCounts: TaskCounts;
  startedAt: string;
  endedAt: string | null;
  firstSeq: number;
  lastSeq: number;
  eventCount: number;
  budgetExceeded?: boolean;
}

export interface TaskSummary {
  taskId: string;
  runId: string;
  nodeId: string | null;
  state: TaskState;
  attempts: number;
  agentId: string | null;
  dependsOn: string[];
  lastEventType: string;
  lastEventAt: string;
  commitSha: string | null;
  budgetExceeded: boolean;
}

export interface AgentUsage {
  costUsd: number;
  tokensEstimate: number;
}

export interface AgentSummary {
  agentId: string;
  provider: string | null;
  model: string | null;
  status: AgentStatus;
  runId: string | null;
  taskId: string | null;
  lastEventType: string;
  lastEventAt: string;
  eventCount: number;
  usage: AgentUsage;
}

export interface DaemonInfo {
  version: string;
  pid: number;
  journalPath: string;
  eventCount: number;
  lastSeq: number;
  startedAt: string;
}

export interface GitPushRemote {
  name: string;
  pushUrl: string;
  selected: boolean;
}

/** Credential-safe Git push routing for the active project. */
export interface GitPushProfile {
  repo: string;
  pushDefault: string | null;
  identity: {
    name: string | null;
    email: string | null;
  };
  remotes: GitPushRemote[];
}

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting";

export interface DaemonNotificationEvent {
  notification: "event";
  subscriptionId: string;
  seq: number;
  event: EventWire;
}

export interface DaemonNotificationClosed {
  notification: "subscription.closed";
  subscriptionId: string;
  reason: string;
}

export interface DaemonNotificationStopping {
  notification: "daemon.stopping";
}

export type DaemonNotification =
  | DaemonNotificationEvent
  | DaemonNotificationClosed
  | DaemonNotificationStopping;

export interface DaemonRequest<T = any> {
  id: number | string;
  method: string;
  params?: T;
}

export interface DaemonResponseSuccess<T = any> {
  id: number | string;
  ok: true;
  result: T;
}

export interface DaemonResponseError {
  id: number | string | null;
  ok: false;
  error: {
    code: "invalid_request" | "method_not_found" | "invalid_params" | "internal_error" | "not_supported" | string;
    message: string;
  };
}

export type DaemonResponse<T = any> = DaemonResponseSuccess<T> | DaemonResponseError;

export type ViewType =
  | "command_center"
  | "runs_graph"
  | "session"
  | "review"
  | "settings"
  | "inbox"
  | "agents"
  | "mastermind";

// ---------- F-13: dynamic agent registry ----------

/** One registry agent row (registry.agents.list / agent.proposal payload). */
export interface AgentRecord {
  id: string;
  name: string;
  description: string;
  adapterId: string;
  model: string | null;
  effort: "low" | "medium" | "high" | null;
  mode: "plan" | "accept_edits";
  skills: string[];
  toolAllowlist: string[];
  toolDenylist: string[];
  timeoutSecs: number;
  builtin: boolean;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
}

/** One registry skill row (registry.skills.list). */
export interface SkillRecord {
  id: string;
  name: string;
  description: string;
  body: string;
  builtin: boolean;
  createdAt: string;
  updatedAt: string;
}

/** One model in the provider catalog (registry.catalog). */
export interface CatalogModel {
  id: string;
  label: string;
}

/** One provider entry in the catalog (registry.catalog). */
export interface ProviderCatalogEntry {
  id: string;
  version: string | null;
  path: string | null;
  auth: "ready" | "needs_login" | "unknown" | null;
  models: CatalogModel[];
}

/**
 * A plan-mode choice the agent is waiting on: `AskUserQuestion` options or
 * an `ExitPlanMode` plan to approve. The desktop renders the options as
 * buttons; clicking one sends its label back as an ordinary instruction.
 */
export interface DecisionPrompt {
  sessionId: string;
  agentId: string;
  tool: string;
  prompt: string;
  options: string[];
  multiSelect: boolean;
  at: string;
}

/** Who produced a transcript entry. `tool` is one adapter tool call. */
export type ChatRole = "user" | "agent" | "system" | "tool" | "decision";

/**
 * One past (or live) conversation, folded from the journal by the daemon
 * (`chat.sessions`). `providerSessionId` is what makes it continuable:
 * `null` means the chat can be read but never resumed.
 */
export interface ChatSessionSummary {
  sessionId: string;
  agentId: string;
  provider: string | null;
  model: string | null;
  providerSessionId: string | null;
  title: string;
  startedAt: string;
  lastAt: string;
  turns: number;
  messageCount: number;
  status: "live" | "finished" | "failed" | "cancelled";
  error: string | null;
  lastSeq: number;
}

/** A chat turn rendered in the Agents view. */
export interface ChatMessage {
  sessionId: string;
  agentId: string;
  role: ChatRole;
  text: string;
  at: string;
  /** role === "tool": the tool name, `text` is the arg summary. */
  tool?: string;
  /** role === "decision": the options the human was offered. */
  options?: string[];
  /** Locally echoed, not yet confirmed by a journal event. */
  pending?: boolean;
}

/**
 * One conversation with an agent: everything said between two "New chat"
 * resets. Provider sessions are NOT conversation boundaries — some adapters
 * open a fresh session for every single turn — so a conversation spans as
 * many sessions as it needs to.
 */
export interface ChatThread {
  /** Position in the agent's conversation list, oldest first. */
  index: number;
  /** First thing the human said, used as the conversation's label. */
  title: string;
  startedAt: string;
  messageCount: number;
  toolCalls: number;
  /** How many provider sessions this one conversation spanned. */
  sessionCount: number;
}

/** Live state of an agent's current chat session. */
export interface ChatSessionState {
  sessionId: string | null;
  /** A turn is in flight: spawned/instructed and no terminal event yet. */
  running: boolean;
}
