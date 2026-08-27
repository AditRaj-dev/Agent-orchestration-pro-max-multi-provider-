// F-12 Mastermind: project-scoped orchestration workspace.
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { IconAlertTriangle, IconBot, IconCheck, IconFolder } from "../components/Icons";
import { ArtifactPreview } from "../components/ArtifactPreview";
import { ChatMarkdown } from "../components/ChatMarkdown";
import { getDaemonWsClient } from "../daemon/ws";

interface PlanNode {
  nodeId: string;
  nodeType: string;
  pool: string | null;
  dependsOn: string[];
  objective: string | null;
  materialized: boolean;
}

interface RosterEntry {
  id: string;
  name: string;
  description: string;
  adapter: string;
  model?: string | null;
}

export interface SessionSummary {
  sessionId: string;
  goal: string;
  repo: string;
  plannerAdapter?: string;
  plannerModel?: string;
  runId: string | null;
  cycles: number;
  maxCycles: number;
  reviewerPool: string;
  pools: string[];
  roster: RosterEntry[];
  nodes: PlanNode[];
  pendingRejections: number;
  discoveryComplete?: boolean;
  pendingDiscoveryQuestions?: number;
  discoveryAnswers?: number;
  stage?: "discovery" | "discovery-review" | "planning" | "committed";
  phase?: string;
  phaseNumber?: string;
  phaseName?: string;
  phaseStatus?: "active" | "running" | "awaiting-write-approval" | "awaiting-approval" | "needs-revision" | "blocked" | "complete";
  gateStatus?: string;
  canApprove?: boolean;
  canAuthorizeWrite?: boolean;
  writeRequest?: { prompt: string; deliverables: string[] } | null;
  canRecoverArtifact?: boolean;
  recoveryRequest?: { prompt: string; deliverables: string[] } | null;
  canReprepareRevision?: boolean;
  reprepareRequest?: { prompt: string; deliverables: string[] } | null;
  canRetryAuthoring?: boolean;
  retryRequest?: { prompt: string; deliverables: string[] } | null;
  activeAgent?: string;
  phaseSession?: number;
  approvedPhases?: string[];
  deliverables?: string[];
  skillSource?: { path: string; sha256: string; loadedAt: string } | null;
  lastReview?: string | null;
  turns?: ProviderTurn[];
  memory?: {
    provider: "memex";
    projectKey: string;
    latestRevision: number | null;
    status: string;
    error?: string | null;
    openHandoffs?: number;
    taskCount?: number;
  };
}

const MASTERMIND_PHASES = [
  { id: "phase-0-setup", number: "0", label: "Setup" },
  { id: "phase-1-discovery", number: "1", label: "Discover" },
  { id: "phase-2-prd", number: "2", label: "PRD" },
  { id: "phase-3-features", number: "3", label: "Features" },
  { id: "phase-4-implementation-plan", number: "4", label: "Plan" },
  { id: "phase-5-api-record", number: "5", label: "APIs" },
  { id: "phase-6-design", number: "6", label: "Design" },
  { id: "phase-7-mockups", number: "7", label: "Mockups" },
  { id: "phase-7-5-html-to-react", number: "7.5", label: "H2R" },
  { id: "phase-8-build", number: "8", label: "Build" },
  { id: "phase-9-wrap", number: "9", label: "Wrap" },
] as const;

interface RunFailure {
  event: string;
  taskId?: string | null;
  agent?: string | null;
  node?: string | null;
  kind?: string | null;
  detail?: string | null;
}

interface Rejection {
  code: string;
  message: string;
  hint: string;
}

interface CycleReport {
  cycle: number;
  accepted: number;
  rejected: Rejection[];
  modelError: string | null;
  engineError: string | null;
  rawExcerpt?: string;
  decisions?: MastermindDecision[];
}

interface MastermindDecision {
  tool: string;
  prompt: string;
  options: string[];
  multiSelect: boolean;
}

type MastermindMessageRole = "user" | "assistant" | "system";

/// A provider turn carried back by the daemon: the model's own prose for the
/// preparation, authoring, or review call it just made.
interface ProviderTurn {
  kind: "preparation" | "authoring" | "review";
  phase: string;
  phaseName: string;
  agent: string;
  model: string;
  text: string;
  at: string;
}

interface MastermindMessage {
  id: string;
  role: MastermindMessageRole;
  text: string;
  at: string;
  /// Present when this entry is verbatim provider output rather than
  /// Mastermind's own narration. Drives markdown rendering and the byline.
  turn?: { kind: ProviderTurn["kind"]; agent: string; model: string; phaseName: string };
}

interface WorkspaceProject {
  path: string;
  name: string;
  lastOpenedAt: string;
}

interface BuildConversation {
  id: string;
  projectPath: string;
  goal: string;
  createdAt: string;
  updatedAt: string;
  session: SessionSummary;
  lastCycle: CycleReport | null;
  runStatus: string | null;
  messages?: MastermindMessage[];
  pendingDecisions?: MastermindDecision[];
  runFailures?: RunFailure[];
}

interface WorkspaceIndex {
  projects: WorkspaceProject[];
  conversations: BuildConversation[];
}

interface MastermindProps {
  projectPath: string;
  onSelectProject: (path: string) => void;
}

interface ProjectContextMenu {
  project: WorkspaceProject;
  x: number;
  y: number;
}

const STORAGE_KEY = "agentos_mastermind_workspace_v2";
const PREVIOUS_STORAGE_KEY = "agentos_mastermind_workspace_v1";
// The daemon's provider turn ceiling is 300s. Leave enough transport grace
// for it to serialize a timeout/error instead of expiring the RPC first.
const PLAN_TIMEOUT_MS = 330_000;
// Scoped authoring is followed by a separate read-only reviewer turn. Both
// now carry the daemon's 540s ceiling, so the RPC needs room for the pair
// plus serialization grace.
const AUTHOR_TIMEOUT_MS = 1_200_000;
const DRIVE_TIMEOUT_MS = 1_800_000;

type PlannerTier = "advanced" | "general" | "worker";
type PlannerChoice =
  | "codex-sol"
  | "claude-opus"
  | "gemini-pro"
  | "codex-terra"
  | "claude-sonnet"
  | "claude-sonnet-legacy"
  | "gemini-flash-high"
  | "gemini-flash-medium"
  | "gemini-flash-low";

interface PlannerOption {
  id: PlannerChoice;
  adapter: string;
  model: string;
  provider: string;
  label: string;
  tier: PlannerTier;
}

const PLANNERS: PlannerOption[] = [
  {
    id: "codex-sol",
    adapter: "codex",
    model: "gpt-5.6-sol",
    provider: "Codex",
    label: "GPT-5.6 Sol",
    tier: "advanced",
  },
  {
    id: "claude-opus",
    adapter: "claude-code",
    model: "claude-opus-5",
    provider: "Claude Code",
    label: "Opus 5",
    tier: "advanced",
  },
  {
    id: "gemini-pro",
    adapter: "antigravity-agy",
    model: "gemini-3.1-pro-high",
    provider: "Antigravity",
    label: "Gemini 3.1 Pro (High)",
    tier: "advanced",
  },
  {
    id: "codex-terra",
    adapter: "codex",
    model: "gpt-5.6-terra",
    provider: "Codex",
    label: "GPT-5.6 Terra",
    tier: "general",
  },
  {
    id: "claude-sonnet",
    adapter: "claude-code",
    model: "claude-sonnet-5",
    provider: "Claude Code",
    label: "Sonnet 5",
    tier: "general",
  },
  {
    id: "claude-sonnet-legacy",
    adapter: "antigravity-agy",
    model: "claude-sonnet-4-6",
    provider: "Antigravity",
    label: "Claude Sonnet 4.6",
    tier: "worker",
  },
  {
    id: "gemini-flash-high",
    adapter: "antigravity-agy",
    model: "gemini-3.7-flash-high",
    provider: "Antigravity",
    label: "Gemini 3.7 Flash (High)",
    tier: "worker",
  },
  {
    id: "gemini-flash-medium",
    adapter: "antigravity-agy",
    model: "gemini-3.7-flash-medium",
    provider: "Antigravity",
    label: "Gemini 3.7 Flash (Medium)",
    tier: "worker",
  },
  {
    id: "gemini-flash-low",
    adapter: "antigravity-agy",
    model: "gemini-3.7-flash-low",
    provider: "Antigravity",
    label: "Gemini 3.7 Flash (Low)",
    tier: "worker",
  },
];

const PLANNER_TIERS: Array<{ id: PlannerTier; label: string }> = [
  { id: "advanced", label: "Advanced" },
  { id: "general", label: "General use" },
  { id: "worker", label: "Worker" },
];

function plannerForChoice(choice: PlannerChoice) {
  return PLANNERS.find((planner) => planner.id === choice)!;
}

function projectName(path: string) {
  return path.split(/[\\/]/).filter(Boolean).pop() || "Untitled project";
}

function loadWorkspace(): WorkspaceIndex {
  if (typeof window === "undefined") return { projects: [], conversations: [] };
  try {
    const value = JSON.parse(window.localStorage.getItem(STORAGE_KEY) || "null") as WorkspaceIndex | null;
    if (!value || !Array.isArray(value.projects) || !Array.isArray(value.conversations)) {
      return { projects: [], conversations: [] };
    }
    return {
      projects: value.projects,
      conversations: value.conversations.map((conversation) => ({
        ...conversation,
        messages: Array.isArray(conversation.messages) && conversation.messages.length > 0
          ? conversation.messages
          : [
              {
                id: `${conversation.id}-legacy-user`,
                role: "user",
                text: conversation.goal,
                at: conversation.createdAt,
              },
              {
                id: `${conversation.id}-legacy-assistant`,
                role: "assistant",
                text: "This build was created before chat history was enabled. Its current plan and run state are restored below.",
                at: conversation.updatedAt,
              },
            ],
        pendingDecisions: Array.isArray(conversation.pendingDecisions)
          ? conversation.pendingDecisions
          : [],
      })),
    };
  } catch {
    return { projects: [], conversations: [] };
  } finally {
    // Version 2 intentionally starts with an empty Mastermind workspace.
    // Keep previous project paths and conversations out of the new session.
    window.localStorage.removeItem(PREVIOUS_STORAGE_KEY);
  }
}

function message(role: MastermindMessageRole, text: string): MastermindMessage {
  return {
    id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
    role,
    text,
    at: new Date().toISOString(),
  };
}

const TURN_LABELS: Record<ProviderTurn["kind"], string> = {
  preparation: "Preparation",
  authoring: "Authoring",
  review: "Review",
};

/// The provider turns of one RPC, oldest first, as transcript entries.
///
/// This is what makes the panel show the model's actual output instead of
/// only Mastermind's one-line account of it.
function turnMessages(session: SessionSummary): MastermindMessage[] {
  return (session.turns || []).map((turn, index) => ({
    id: `${turn.at}-${turn.kind}-${index}-${Math.random().toString(36).slice(2)}`,
    role: "assistant" as const,
    text: turn.text,
    at: turn.at || new Date().toISOString(),
    turn: {
      kind: turn.kind,
      agent: turn.agent,
      model: turn.model,
      phaseName: turn.phaseName,
    },
  }));
}

function cycleSummary(cycle: CycleReport, session: SessionSummary): string {
  const nodeCount = session.nodes.length;
  if (cycle.modelError) return `I couldn't complete this planning turn: ${cycle.modelError}`;
  if (cycle.engineError) return `The planning engine needs attention: ${cycle.engineError}`;
  if (session.phaseStatus === "awaiting-write-approval") {
    return session.writeRequest?.prompt || `${session.phaseName || "This phase"} is ready for a scoped authoring grant.`;
  }
  if (session.phaseStatus === "awaiting-approval") {
    return `${session.phaseName || "This phase"} is ready for review. Check its deliverables, then approve the phase or describe the revision you want.`;
  }
  if (session.phaseStatus === "needs-revision") {
    return `${session.phaseName || "This phase"} did not pass review yet. Tell me what to revise and I will run a fresh phase session.`;
  }
  if (session.phaseStatus === "blocked") {
    return `${session.phaseName || "This phase"} is blocked. ${session.memory?.error || cycle.engineError || "Check the phase status for details."}`;
  }
  if ((cycle.decisions?.length || 0) > 0 && cycle.accepted === 0) {
    return "I need your input before I can finish this planning stage.";
  }
  if (cycle.rejected.length > 0) {
    return `Planning cycle ${cycle.cycle + 1} updated the draft to ${nodeCount} task${nodeCount === 1 ? "" : "s"}. ${cycle.rejected.length} proposal${cycle.rejected.length === 1 ? " needs" : "s need"} another pass.`;
  }
  if (cycle.accepted === 0) {
    return `Planning cycle ${cycle.cycle + 1} is complete. The ${nodeCount}-task draft did not need another change.`;
  }
  return `Planning cycle ${cycle.cycle + 1} updated the draft to ${nodeCount} task${nodeCount === 1 ? "" : "s"}. Review it below or tell me what to change.`;
}

function conversationState(conversation: BuildConversation) {
  if (conversation.session.phaseName) {
    const suffix = conversation.session.phaseStatus === "awaiting-approval"
      ? " · approval"
      : conversation.session.phaseStatus === "awaiting-write-approval"
        ? " · write permission"
      : conversation.session.phaseStatus === "needs-revision"
        ? " · revision"
        : "";
    return `${conversation.session.phaseName}${suffix}`;
  }
  if (conversation.session.stage === "discovery") return "Discovery";
  if (conversation.session.stage === "discovery-review") return "Discovery review";
  if (conversation.session.runId) return "Plan approved";
  if (conversation.session.nodes.length) return "Plan ready";
  if (conversation.session.cycles) return "Planning";
  return "Draft";
}

function dateLabel(value: string) {
  return new Date(value).toLocaleDateString([], { month: "short", day: "numeric" });
}

function plannerLabel(session: SessionSummary) {
  const configured = PLANNERS.find(
    (planner) => planner.adapter === session.plannerAdapter && planner.model === session.plannerModel,
  );
  if (configured) return `${configured.provider} · ${configured.label}`;
  if (session.plannerAdapter === "codex") return `Codex · ${session.plannerModel || "default"}`;
  if (session.plannerAdapter === "claude-code") return `Claude Code · ${session.plannerModel || "default"}`;
  return session.plannerModel || "Default planner";
}

/// Live planner picker. Repointing is durable session state, not a provider
/// turn, so it preserves the phase and every approved artifact. The daemon
/// refuses a swap while a turn holds the session; this mirrors that so a
/// mid-turn swap is never offered in the first place.
export function PlannerSwap({
  session,
  busy,
  onSwap,
}: {
  session: SessionSummary | null;
  busy: string | null;
  onSwap: (choice: PlannerChoice) => void;
}) {
  const running = session?.phaseStatus === "running";
  return (
    <div className="mastermind-planner-swap" role="group" aria-label="Planning model">
      <select
        className="mastermind-planner-select"
        aria-label="Planning model"
        value={PLANNERS.find((planner) => planner.adapter === session?.plannerAdapter && planner.model === session?.plannerModel)?.id || ""}
        onChange={(event) => onSwap(event.target.value as PlannerChoice)}
        disabled={!session || busy !== null || running}
        title={running ? "A provider turn is in flight; the planner cannot change mid-turn" : "Choose a planning model"}
      >
        {PLANNER_TIERS.map((tier) => (
          <optgroup key={tier.id} label={tier.label}>
            {PLANNERS.filter((planner) => planner.tier === tier.id).map((planner) => (
              <option key={planner.id} value={planner.id}>{planner.provider} · {planner.label}</option>
            ))}
          </optgroup>
        ))}
      </select>
    </div>
  );
}

export function Mastermind({ projectPath, onSelectProject }: MastermindProps) {
  const initial = useMemo(() => loadWorkspace(), []);
  const [projects, setProjects] = useState(initial.projects);
  const [conversations, setConversations] = useState(initial.conversations);
  const [selectedProjectPath, setSelectedProjectPath] = useState(() =>
    initial.projects.some((project) => project.path === projectPath)
      ? projectPath
      : initial.projects[0]?.path || "",
  );
  const [selectedConversationId, setSelectedConversationId] = useState<string | null>(null);
  const [draftGoal, setDraftGoal] = useState("");
  const [chatInput, setChatInput] = useState("");
  const [previewPath, setPreviewPath] = useState<string | null>(null);
  const [plannerChoice, setPlannerChoice] = useState<PlannerChoice>("codex-sol");
  const [newProjectPath, setNewProjectPath] = useState("");
  const [addingProject, setAddingProject] = useState(false);
  const [folderPickerBusy, setFolderPickerBusy] = useState(false);
  const [folderPickerError, setFolderPickerError] = useState<string | null>(null);
  const [projectMenu, setProjectMenu] = useState<ProjectContextMenu | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const chatLogRef = useRef<HTMLDivElement | null>(null);
  const chatInputRef = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ projects, conversations }));
  }, [projects, conversations]);

  useEffect(() => {
    setProjects((current) => {
      const now = new Date().toISOString();
      if (!current.some((project) => project.path === projectPath)) return current;
      return current.map((project) => project.path === projectPath ? { ...project, lastOpenedAt: now } : project);
    });
  }, [projectPath]);

  useEffect(() => {
    const closeMenu = () => setProjectMenu(null);
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") closeMenu();
    };
    window.addEventListener("pointerdown", closeMenu);
    window.addEventListener("keydown", closeOnEscape);
    return () => {
      window.removeEventListener("pointerdown", closeMenu);
      window.removeEventListener("keydown", closeOnEscape);
    };
  }, []);

  const projectConversations = useMemo(
    () => conversations
      .filter((conversation) => conversation.projectPath === selectedProjectPath)
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)),
    [conversations, selectedProjectPath],
  );
  const selectedConversation = conversations.find(({ id }) => id === selectedConversationId) || null;
  const session = selectedConversation?.session || null;
  const committed = Boolean(session?.runId);
  const hasNodes = Boolean(session?.nodes.length);
  const cyclesLeft = session ? session.maxCycles - session.cycles : 0;
  const transcript = selectedConversation?.messages || [];
  const pendingDecisions = selectedConversation?.pendingDecisions || [];
  const phase = session?.phase || "phase-1-discovery";
  const phaseIndex = Math.max(0, MASTERMIND_PHASES.findIndex((item) => item.id === phase));
  const terminalRun = /^(Failed|Completed|Cancelled)/i.test(selectedConversation?.runStatus || "");

  useEffect(() => {
    const log = chatLogRef.current;
    if (!log) return;
    log.scrollTop = log.scrollHeight;
  }, [selectedConversationId, transcript.length, busy, pendingDecisions.length]);

  const updateConversation = useCallback(
    (id: string, update: (conversation: BuildConversation) => BuildConversation) => {
      setConversations((current) => current.map((item) => item.id === id ? update(item) : item));
    },
    [],
  );

  const appendConversationMessage = useCallback(
    (id: string, entry: MastermindMessage) => {
      updateConversation(id, (current) => ({
        ...current,
        messages: [...(current.messages || []), entry],
        updatedAt: new Date().toISOString(),
      }));
    },
    [updateConversation],
  );

  const call = useCallback(
    async <T,>(label: string, method: string, params: unknown, timeoutMs: number): Promise<T | null> => {
      setBusy(label);
      setError(null);
      try {
        return await getDaemonWsClient().call<T>(method, params, timeoutMs);
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : String(caught));
        return null;
      } finally {
        setBusy(null);
      }
    },
    [],
  );

  const selectProject = (path: string) => {
    setSelectedProjectPath(path);
    setSelectedConversationId(null);
    setDraftGoal("");
    setChatInput("");
    setError(null);
    setProjects((current) => current.map((project) =>
      project.path === path ? { ...project, lastOpenedAt: new Date().toISOString() } : project,
    ));
    onSelectProject(path);
  };

  const addProjectPath = async (selectedPath: string) => {
    const path = selectedPath.trim();
    if (!path) return;
    setBusy("setting-up-repository");
    setError(null);
    try {
      await invoke("setup_project_repository", { path });
      setProjects((current) => current.some((project) => project.path === path)
        ? current
        : [{ path, name: projectName(path), lastOpenedAt: new Date().toISOString() }, ...current]);
      setNewProjectPath("");
      setAddingProject(false);
      selectProject(path);
    } catch (caught) {
      setError(`Could not prepare ${projectName(path)} as a Git project. ${caught instanceof Error ? caught.message : String(caught)}`);
    } finally {
      setBusy(null);
    }
  };

  const addProject = () => void addProjectPath(newProjectPath);

  const openProjectFolder = async (project: WorkspaceProject) => {
    setProjectMenu(null);
    setBusy("opening-folder");
    setError(null);
    try {
      await invoke("reveal_project_folder", { path: project.path });
    } catch (caught) {
      setError(`Could not open ${project.name}. ${caught instanceof Error ? caught.message : String(caught)}`);
    } finally {
      setBusy(null);
    }
  };

  const copyProjectPath = async (project: WorkspaceProject) => {
    setProjectMenu(null);
    try {
      await navigator.clipboard.writeText(project.path);
    } catch (caught) {
      setError(`Could not copy the folder path. ${caught instanceof Error ? caught.message : String(caught)}`);
    }
  };

  const renameProjectFolder = async (project: WorkspaceProject) => {
    setProjectMenu(null);
    const newName = window.prompt("Rename folder", project.name)?.trim();
    if (!newName || newName === project.name) return;

    setBusy("renaming-folder");
    setError(null);
    try {
      const newPath = await invoke<string>("rename_project_folder", { path: project.path, newName });
      setProjects((current) => current.map((item) => item.path === project.path
        ? { ...item, path: newPath, name: projectName(newPath), lastOpenedAt: new Date().toISOString() }
        : item));
      setConversations((current) => current.map((item) => item.projectPath === project.path
        ? { ...item, projectPath: newPath, updatedAt: new Date().toISOString() }
        : item));
      if (selectedProjectPath === project.path) {
        setSelectedProjectPath(newPath);
        onSelectProject(newPath);
      }
    } catch (caught) {
      setError(`Could not rename ${project.name}. ${caught instanceof Error ? caught.message : String(caught)}`);
    } finally {
      setBusy(null);
    }
  };

  const removeProjectFromWorkspace = (project: WorkspaceProject) => {
    setProjectMenu(null);
    const remaining = projects.filter((item) => item.path !== project.path);
    if (project.path === selectedProjectPath && remaining.length === 0) {
      setError("Open another folder before removing the only project from the workspace.");
      return;
    }
    setProjects(remaining);
    if (project.path === selectedProjectPath) {
      const next = remaining.sort((a, b) => b.lastOpenedAt.localeCompare(a.lastOpenedAt))[0];
      setSelectedProjectPath(next.path);
      setSelectedConversationId(null);
      setDraftGoal("");
      onSelectProject(next.path);
    }
  };

  const deleteProjectFolder = async (project: WorkspaceProject) => {
    setProjectMenu(null);
    const confirmed = window.confirm(
      `Delete “${project.name}” and every file inside it? This cannot be undone.`,
    );
    if (!confirmed) return;

    setBusy("deleting-folder");
    setError(null);
    try {
      await invoke("delete_project_folder", { path: project.path });
      const remaining = projects.filter((item) => item.path !== project.path);
      setProjects(remaining);
      setConversations((current) => current.filter((item) => item.projectPath !== project.path));
      if (project.path === selectedProjectPath) {
        const next = remaining.sort((a, b) => b.lastOpenedAt.localeCompare(a.lastOpenedAt))[0];
        setSelectedProjectPath(next?.path || "");
        setSelectedConversationId(null);
        setDraftGoal("");
        onSelectProject(next?.path || "");
      }
    } catch (caught) {
      setError(`Could not delete ${project.name}. ${caught instanceof Error ? caught.message : String(caught)}`);
    } finally {
      setBusy(null);
    }
  };

  const browseForProject = async () => {
    setFolderPickerBusy(true);
    setFolderPickerError(null);
    try {
      const selected = await openDialog({
        directory: true,
        multiple: false,
        title: "Open project folder",
      });
      if (typeof selected === "string") await addProjectPath(selected);
    } catch (caught) {
      setFolderPickerError(
        `Could not open the Windows folder picker. ${caught instanceof Error ? caught.message : String(caught)}`,
      );
    } finally {
      setFolderPickerBusy(false);
    }
  };

  const refreshConversation = useCallback(async (conversation: BuildConversation) => {
    const result = await call<{ session: SessionSummary; run: { status: string } | null; failures?: RunFailure[] }>(
      "refreshing",
      "mastermind.status",
      { sessionId: conversation.session.sessionId },
      30_000,
    );
    if (result) {
      updateConversation(conversation.id, (current) => ({
        ...current,
        session: result.session,
        runStatus: result.run?.status || current.runStatus,
        runFailures: result.failures || current.runFailures || [],
        updatedAt: new Date().toISOString(),
      }));
    }
  }, [call, updateConversation]);

  // Repointing the planner is durable session state, not a provider turn, so
  // it keeps the phase and every approved artifact. The daemon refuses while
  // a turn holds the session, and the button mirrors that so a mid-turn swap
  // is never even offered.
  const swapPlanner = useCallback(async (choice: PlannerChoice) => {
    if (!selectedConversation || !session) return;
    const planner = plannerForChoice(choice);
    if (session.plannerAdapter === planner.adapter && session.plannerModel === planner.model) return;
    const result = await call<{ session: SessionSummary }>(
      "switching planner",
      "mastermind.setPlanner",
      { sessionId: session.sessionId, plannerAdapter: planner.adapter, plannerModel: planner.model },
      30_000,
    );
    if (result) {
      updateConversation(selectedConversation.id, (current) => ({
        ...current,
        session: result.session,
        updatedAt: new Date().toISOString(),
      }));
    }
  }, [call, selectedConversation, session, updateConversation]);

  const openConversation = (conversation: BuildConversation) => {
    setSelectedConversationId(conversation.id);
    setDraftGoal("");
    setChatInput("");
    setError(null);
    void refreshConversation(conversation);
  };

  const start = useCallback(async () => {
    const goal = draftGoal.trim();
    if (!goal) {
      setError("Describe the outcome you want the orchestrator to deliver.");
      return;
    }
    const summary = await call<SessionSummary>(
      "starting",
      "mastermind.start",
      {
        goal,
        repo: selectedProjectPath,
        plannerAdapter: plannerForChoice(plannerChoice).adapter,
        plannerModel: plannerForChoice(plannerChoice).model,
      },
      30_000,
    );
    if (!summary) return;
    const now = new Date().toISOString();
    const conversation: BuildConversation = {
      id: summary.sessionId,
      projectPath: selectedProjectPath,
      goal,
      createdAt: now,
      updatedAt: now,
      session: summary,
      lastCycle: null,
      runStatus: null,
      messages: [
        message("user", goal),
        message(
          "assistant",
          `I opened the full Phase 0–9 workflow with ${plannerForChoice(plannerChoice).label}. Start discovery below; every approved boundary is synchronized to Memex before the next fresh provider session begins.`,
        ),
      ],
      pendingDecisions: [],
    };
    setConversations((current) => [conversation, ...current]);
    setSelectedConversationId(conversation.id);
    setDraftGoal("");
  }, [call, draftGoal, plannerChoice, selectedProjectPath]);

  const plan = useCallback(async (instruction?: string, visibleText?: string) => {
    if (!selectedConversation) return;
    const conversationId = selectedConversation.id;
    const guidance = instruction?.trim();
    if (visibleText?.trim()) {
      appendConversationMessage(conversationId, message("user", visibleText.trim()));
    }
    const result = await call<{ cycle: CycleReport; session: SessionSummary }>(
      "planning",
      "mastermind.respond",
      {
        sessionId: selectedConversation.session.sessionId,
        ...(guidance ? { instruction: guidance } : {}),
      },
      PLAN_TIMEOUT_MS,
    );
    if (!result) {
      await refreshConversation(selectedConversation);
      return;
    }
    updateConversation(conversationId, (current) => ({
      ...current,
      session: result.session,
      lastCycle: result.cycle,
      pendingDecisions: result.cycle.decisions || [],
      messages: [
        ...(current.messages || []),
        ...turnMessages(result.session),
        message("assistant", cycleSummary(result.cycle, result.session)),
      ],
      updatedAt: new Date().toISOString(),
    }));
  }, [appendConversationMessage, call, refreshConversation, selectedConversation, updateConversation]);

  const recoverPhaseArtifact = useCallback(async () => {
    if (!selectedConversation) return;
    const conversationId = selectedConversation.id;
    const deliverables = selectedConversation.session.recoveryRequest?.deliverables || [];
    appendConversationMessage(
      conversationId,
      message("user", `Review the existing ${deliverables.join(", ") || "phase artifact"} without authoring it again.`),
    );
    const result = await call<{ cycle: CycleReport; session: SessionSummary }>(
      "recovering-artifact",
      "mastermind.recoverPhaseArtifact",
      { sessionId: selectedConversation.session.sessionId },
      PLAN_TIMEOUT_MS,
    );
    if (!result) {
      await refreshConversation(selectedConversation);
      return;
    }
    updateConversation(conversationId, (current) => ({
      ...current,
      session: result.session,
      lastCycle: result.cycle,
      pendingDecisions: result.cycle.decisions || [],
      messages: [
        ...(current.messages || []),
        ...turnMessages(result.session),
        message("assistant", cycleSummary(result.cycle, result.session)),
      ],
      updatedAt: new Date().toISOString(),
    }));
  }, [appendConversationMessage, call, refreshConversation, selectedConversation, updateConversation]);

  const retryPhaseAuthoring = useCallback(async () => {
    if (!selectedConversation) return;
    const conversationId = selectedConversation.id;
    const deliverables = selectedConversation.session.retryRequest?.deliverables || [];
    appendConversationMessage(
      conversationId,
      message("user", `Retry scoped authoring for ${deliverables.join(", ") || "the current phase deliverable"} in a clean provider conversation.`),
    );
    const result = await call<{ cycle: CycleReport; session: SessionSummary }>(
      "retrying-authoring",
      "mastermind.retryPhaseAuthoring",
      { sessionId: selectedConversation.session.sessionId },
      AUTHOR_TIMEOUT_MS,
    );
    if (!result) {
      await refreshConversation(selectedConversation);
      return;
    }
    updateConversation(conversationId, (current) => ({
      ...current,
      session: result.session,
      lastCycle: result.cycle,
      pendingDecisions: result.cycle.decisions || [],
      messages: [
        ...(current.messages || []),
        ...turnMessages(result.session),
        message("assistant", cycleSummary(result.cycle, result.session)),
      ],
      updatedAt: new Date().toISOString(),
    }));
  }, [appendConversationMessage, call, refreshConversation, selectedConversation, updateConversation]);

  const approvePhase = useCallback(async () => {
    if (!selectedConversation) return;
    const conversationId = selectedConversation.id;
    const phaseName = selectedConversation.session.phaseName || "current phase";
    appendConversationMessage(conversationId, message("user", `Approve ${phaseName} and continue to the next phase.`));
    const result = await call<{ cycle: CycleReport; session: SessionSummary }>(
      "approving-phase",
      "mastermind.approvePhase",
      { sessionId: selectedConversation.session.sessionId },
      PLAN_TIMEOUT_MS,
    );
    if (!result) {
      // A refused gate action means the server already moved past this card
      // (or never offered it). Re-read the real state so the stale card is
      // replaced instead of re-offering an action that cannot succeed.
      await refreshConversation(selectedConversation);
      return;
    }
    updateConversation(conversationId, (current) => ({
      ...current,
      session: result.session,
      lastCycle: result.cycle,
      pendingDecisions: result.cycle.decisions || [],
      messages: [
        ...(current.messages || []),
        ...turnMessages(result.session),
        message("assistant", cycleSummary(result.cycle, result.session)),
      ],
      updatedAt: new Date().toISOString(),
    }));
  }, [appendConversationMessage, call, refreshConversation, selectedConversation, updateConversation]);

  const acceptPhaseAsIs = useCallback(async () => {
    if (!selectedConversation) return;
    const conversationId = selectedConversation.id;
    const phaseName = selectedConversation.session.phaseName || "current phase";
    appendConversationMessage(conversationId, message("user", `Accept ${phaseName} as-is. No revision is needed.`));
    const result = await call<{ cycle: CycleReport; session: SessionSummary }>(
      "accepting-phase-as-is",
      "mastermind.acceptPhaseAsIs",
      { sessionId: selectedConversation.session.sessionId },
      PLAN_TIMEOUT_MS,
    );
    if (!result) {
      // A refused gate action means the server already moved past this card
      // (or never offered it). Re-read the real state so the stale card is
      // replaced instead of re-offering an action that cannot succeed.
      await refreshConversation(selectedConversation);
      return;
    }
    updateConversation(conversationId, (current) => ({
      ...current,
      session: result.session,
      lastCycle: result.cycle,
      pendingDecisions: result.cycle.decisions || [],
      messages: [
        ...(current.messages || []),
        ...turnMessages(result.session),
        message("assistant", cycleSummary(result.cycle, result.session)),
      ],
      updatedAt: new Date().toISOString(),
    }));
  }, [appendConversationMessage, call, refreshConversation, selectedConversation, updateConversation]);

  const repreparePhaseRevision = useCallback(async (guidance?: string) => {
    if (!selectedConversation) return;
    const conversationId = selectedConversation.id;
    const deliverables = selectedConversation.session.reprepareRequest?.deliverables || [];
    appendConversationMessage(
      conversationId,
      message("user", `Re-prepare the Phase 7.5 revision scope for ${deliverables.join(", ") || "the server-derived remediation files"}.`),
    );
    const result = await call<{ cycle: CycleReport; session: SessionSummary }>(
      "repreparing-revision",
      "mastermind.repreparePhaseRevision",
      { sessionId: selectedConversation.session.sessionId, ...(guidance?.trim() ? { guidance: guidance.trim() } : {}) },
      PLAN_TIMEOUT_MS,
    );
    if (!result) {
      await refreshConversation(selectedConversation);
      return;
    }
    updateConversation(conversationId, (current) => ({
      ...current,
      session: result.session,
      lastCycle: result.cycle,
      pendingDecisions: result.cycle.decisions || [],
      messages: [
        ...(current.messages || []),
        ...turnMessages(result.session),
        message("assistant", cycleSummary(result.cycle, result.session)),
      ],
      updatedAt: new Date().toISOString(),
    }));
  }, [appendConversationMessage, call, refreshConversation, selectedConversation, updateConversation]);

  const authorizePhaseWrite = useCallback(async () => {
    if (!selectedConversation) return;
    const conversationId = selectedConversation.id;
    const deliverables = selectedConversation.session.writeRequest?.deliverables || [];
    appendConversationMessage(
      conversationId,
      message("user", `Allow one scoped write turn for ${deliverables.join(", ") || "the current phase deliverable"}.`),
    );
    const result = await call<{ cycle: CycleReport; session: SessionSummary }>(
      "authorizing-write",
      "mastermind.authorizePhaseWrite",
      { sessionId: selectedConversation.session.sessionId },
      AUTHOR_TIMEOUT_MS,
    );
    if (!result) {
      await refreshConversation(selectedConversation);
      return;
    }
    updateConversation(conversationId, (current) => ({
      ...current,
      session: result.session,
      lastCycle: result.cycle,
      pendingDecisions: result.cycle.decisions || [],
      messages: [
        ...(current.messages || []),
        ...turnMessages(result.session),
        message("assistant", cycleSummary(result.cycle, result.session)),
      ],
      updatedAt: new Date().toISOString(),
    }));
  }, [appendConversationMessage, call, refreshConversation, selectedConversation, updateConversation]);

  const drive = useCallback(async () => {
    if (!selectedConversation) return;
    const conversationId = selectedConversation.id;
    appendConversationMessage(conversationId, message("user", "Start or continue the agent run."));
    const result = await call<{ ticks: number; status: string; failures?: RunFailure[]; session: SessionSummary }>(
      "driving",
      "mastermind.drive",
      { sessionId: selectedConversation.session.sessionId },
      DRIVE_TIMEOUT_MS,
    );
    if (result) updateConversation(conversationId, (current) => ({
      ...current,
      session: result.session,
      runStatus: `${result.status} after ${result.ticks} tick(s)`,
      runFailures: result.failures || [],
      messages: [
        ...(current.messages || []),
        ...turnMessages(result.session),
        message("assistant", result.failures?.find((failure) => failure.detail)?.detail
          ? `The worker run is ${result.status.toLowerCase()}. ${result.failures.find((failure) => failure.detail)!.detail}`
          : `The worker run is ${result.status.toLowerCase()} after ${result.ticks} scheduler tick${result.ticks === 1 ? "" : "s"}.`),
      ],
      updatedAt: new Date().toISOString(),
    }));
  }, [appendConversationMessage, call, selectedConversation, updateConversation]);

  const sendChat = useCallback(async () => {
    const text = chatInput.trim();
    if (!selectedConversation || !text || busy !== null || cyclesLeft <= 0) return;
    setChatInput("");
    if (selectedConversation.session.phase === "phase-7-5-html-to-react"
      && selectedConversation.session.phaseStatus === "needs-revision"
      && selectedConversation.session.canReprepareRevision) {
      await repreparePhaseRevision(text);
    } else {
      await plan(text, text);
    }
    chatInputRef.current?.focus();
  }, [busy, chatInput, cyclesLeft, plan, repreparePhaseRevision, selectedConversation]);

  const answerDecision = useCallback(async (answer: string) => {
    if (!selectedConversation || !pendingDecisions[0]) return;
    updateConversation(selectedConversation.id, (current) => ({
      ...current,
      pendingDecisions: (current.pendingDecisions || []).slice(1),
    }));
    await plan(
      answer,
      answer,
    );
  }, [pendingDecisions, plan, selectedConversation, updateConversation]);

  const newConversation = () => {
    setSelectedConversationId(null);
    setDraftGoal("");
    setChatInput("");
    setError(null);
  };

  return (
    <div className={previewPath ? "mastermind-shell has-preview" : "mastermind-shell"}>
      <aside className="mastermind-projects" aria-label="Projects">
        <div className="mastermind-side-header">
          <span>Projects</span>
          <button className="mastermind-new-chat" onClick={() => setAddingProject((value) => !value)}>Enter path</button>
        </div>
        <div className="mastermind-open-folder-wrap">
          <button className="mastermind-open-folder" onClick={() => void browseForProject()} disabled={folderPickerBusy}>
            <IconFolder size={15} />
            <span>{folderPickerBusy ? "Opening Explorer…" : "Open folder"}</span>
          </button>
          <span>Choose a project using Windows File Explorer</span>
        </div>
        {folderPickerError && (
          <div className="mastermind-folder-error">
            <IconAlertTriangle size={12} />
            <span>{folderPickerError}</span>
          </div>
        )}
        {addingProject && (
          <div className="mastermind-add-project">
            <input
              autoFocus
              className="input-text"
              value={newProjectPath}
              placeholder="C:\\work\\project"
              onChange={(event) => setNewProjectPath(event.target.value)}
              onKeyDown={(event) => event.key === "Enter" && addProject()}
            />
            <button className="btn btn-sm btn-primary" onClick={addProject}>Add</button>
          </div>
        )}
        <div className="mastermind-project-list">
          {projects.slice().sort((a, b) => b.lastOpenedAt.localeCompare(a.lastOpenedAt)).map((project) => {
            const count = conversations.filter(({ projectPath: path }) => path === project.path).length;
            return (
              <button
                key={project.path}
                className={`mastermind-project ${project.path === selectedProjectPath ? "active" : ""}`}
                onClick={() => selectProject(project.path)}
                onContextMenu={(event) => {
                  event.preventDefault();
                  setProjectMenu({ project, x: event.clientX, y: event.clientY });
                }}
                title={project.path}
              >
                <IconFolder size={15} />
                <span className="mastermind-project-copy">
                  <strong>{project.name}</strong>
                  <small>{count} {count === 1 ? "conversation" : "conversations"}</small>
                </span>
              </button>
            );
          })}
        </div>
        {projectMenu && (
          <div
            className="mastermind-project-menu"
            role="menu"
            aria-label={`${projectMenu.project.name} folder actions`}
            style={{ left: projectMenu.x, top: projectMenu.y }}
            onPointerDown={(event) => event.stopPropagation()}
          >
            <button role="menuitem" onClick={() => void openProjectFolder(projectMenu.project)} disabled={busy !== null}>Open in File Explorer</button>
            <button role="menuitem" onClick={() => void copyProjectPath(projectMenu.project)} disabled={busy !== null}>Copy folder path</button>
            <button role="menuitem" onClick={() => void renameProjectFolder(projectMenu.project)} disabled={busy !== null}>Rename folder…</button>
            <button role="menuitem" onClick={() => removeProjectFromWorkspace(projectMenu.project)} disabled={busy !== null}>Remove from workspace</button>
            <div className="mastermind-project-menu-separator" />
            <button className="danger" role="menuitem" onClick={() => void deleteProjectFolder(projectMenu.project)} disabled={busy !== null}>Delete folder…</button>
          </div>
        )}
        <div className="mastermind-side-footer">Saved locally on this device.</div>
      </aside>

      <aside className="mastermind-conversations" aria-label="Build conversations">
        <div className="mastermind-side-header">
          <span>Build conversations</span>
          <button className="mastermind-new-chat" onClick={newConversation}>New</button>
        </div>
        <div className="mastermind-conversation-list">
          {projectConversations.length === 0 ? (
            <div className="mastermind-list-empty">No builds here yet. Start a conversation to turn an outcome into a task graph.</div>
          ) : projectConversations.map((conversation) => (
            <button
              key={conversation.id}
              className={`mastermind-conversation ${conversation.id === selectedConversationId ? "active" : ""}`}
              onClick={() => openConversation(conversation)}
            >
              <span className={`mastermind-conversation-dot ${conversationState(conversation).toLowerCase().replace(" ", "-")}`} />
              <span className="mastermind-conversation-copy">
                <strong>{conversation.goal}</strong>
                <small>{conversationState(conversation)} · {plannerLabel(conversation.session)} · {dateLabel(conversation.updatedAt)}</small>
              </span>
            </button>
          ))}
        </div>
      </aside>

      <section className="mastermind-workspace">
        <header className="mastermind-workspace-header">
          <div className="mastermind-breadcrumb">
            <IconFolder size={15} />
            <span>{projectName(selectedProjectPath)}</span>
            <span className="mastermind-path" title={selectedProjectPath}>{selectedProjectPath}</span>
          </div>
          {selectedConversation && (
            <button className="btn btn-sm" disabled={busy !== null} onClick={() => void refreshConversation(selectedConversation)}>
              {busy === "refreshing" ? "Refreshing…" : "Refresh"}
            </button>
          )}
        </header>

        <div className="mastermind-content">
          {!selectedConversation ? (
            <div className="mastermind-welcome">
              <div className="mastermind-welcome-mark"><IconBot size={24} /></div>
              <p className="mastermind-eyebrow">NEW ORCHESTRATED BUILD</p>
              <h1>What do you want to build?</h1>
              <p>Mastermind plans this project, shows you the task graph, and waits for approval before any agent starts writing.</p>
              <div className="mastermind-planner-picker" role="group" aria-label="Planning model">
                <label className="mastermind-planner-label" htmlFor="mastermind-new-planner">Planning model</label>
                <select
                  id="mastermind-new-planner"
                  className="mastermind-planner-select"
                  value={plannerChoice}
                  onChange={(event) => setPlannerChoice(event.target.value as PlannerChoice)}
                >
                  {PLANNER_TIERS.map((tier) => (
                    <optgroup key={tier.id} label={tier.label}>
                      {PLANNERS.filter((planner) => planner.tier === tier.id).map((planner) => (
                        <option key={planner.id} value={planner.id}>{planner.provider} · {planner.label}</option>
                      ))}
                    </optgroup>
                  ))}
                </select>
              </div>
              {error && <div className="mastermind-alert error"><IconAlertTriangle size={14} /><span>{error}</span></div>}
              <label className="mastermind-composer">
                <textarea
                  aria-label="Build goal"
                  rows={5}
                  value={draftGoal}
                  onChange={(event) => setDraftGoal(event.target.value)}
                  placeholder="Build a project dashboard with secure login, tests, and an implementation plan…"
                  onKeyDown={(event) => {
                    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
                      event.preventDefault();
                      void start();
                    }
                  }}
                />
                <div className="mastermind-composer-footer">
                  <span>Ctrl + Enter to start</span>
                  <button className="btn btn-primary" onClick={() => void start()} disabled={busy !== null || !draftGoal.trim()}>
                    {busy === "starting" ? "Starting…" : "Start planning"}
                  </button>
                </div>
              </label>
            </div>
          ) : (
            <div className="mastermind-build mastermind-chat-build">
              <div className="mastermind-build-title">
                <div>
                  <p className="mastermind-eyebrow">{conversationState(selectedConversation)}</p>
                  <h1>{selectedConversation.goal}</h1>
                </div>
                <div className="mastermind-session-meta">
                  <PlannerSwap session={session} busy={busy} onSwap={(choice) => void swapPlanner(choice)} />
                  <span className="mastermind-session-id" title={session?.sessionId}>Session {session?.sessionId.slice(0, 8)}</span>
                </div>
              </div>

              <div className="mastermind-progress" aria-label="Mastermind phases">
                {MASTERMIND_PHASES.map((item, index) => (
                  <span className="mastermind-progress-segment" key={item.id}>
                    {index > 0 && <span className={`mastermind-progress-line ${index <= phaseIndex ? "complete" : ""}`} />}
                    <StageStep
                      number={item.number}
                      label={item.label}
                      active={item.id === phase && session?.phaseStatus !== "complete"}
                      complete={index < phaseIndex || session?.approvedPhases?.includes(item.id) === true}
                    />
                  </span>
                ))}
              </div>

              {error && <div className="mastermind-alert error"><IconAlertTriangle size={14} /><span>{error}</span></div>}

              <div className="mastermind-chat-log" ref={chatLogRef} aria-live="polite">
                {transcript.map((entry) => (
                  <MastermindChatEntry key={entry.id} entry={entry} planner={session ? plannerLabel(session) : "Mastermind"} />
                ))}

                {busy && (
                  <article className="chat-turn assistant working">
                    <header className="chat-turn-byline">
                      <span className="chat-turn-avatar" aria-hidden="true"><IconBot size={13} /></span>
                      <strong>{session?.activeAgent || "Mastermind"}</strong>
                      <span className="chat-turn-kind">Working</span>
                    </header>
                    <div className="chat-turn-status">
                      <span className="chat-spinner" aria-hidden="true" />
                      {busy === "planning" ? "Planning the next pass…" : busy === "authorizing-write" ? "Writing the scoped phase deliverable…" : busy === "retrying-authoring" ? "Retrying the scoped author in a clean conversation…" : busy === "recovering-artifact" ? "Reviewing the existing artifact…" : busy === "accepting-phase-as-is" ? "Accepting the artifact as-is…" : busy === "approving-phase" ? "Approving the phase…" : busy === "committing" ? "Preparing the approved task graph…" : busy === "driving" ? "Worker agents are running…" : "Working…"}
                    </div>
                  </article>
                )}

                {hasNodes && (
                  <section className="mastermind-stage mastermind-chat-artifact">
                    <div className="mastermind-stage-head">
                      <div><span className="mastermind-stage-number">01</span><h2>Task graph</h2></div>
                      <span>{session!.nodes.length} nodes · cycle {session!.cycles}/{session!.maxCycles}</span>
                    </div>
                    <PlanTable nodes={session!.nodes} />
                  </section>
                )}

                {Boolean(session?.deliverables?.length) && (
                  <section className="mastermind-stage compact mastermind-chat-artifact">
                    <div className="mastermind-stage-head">
                      <div><span className="mastermind-stage-number">{session?.phaseNumber || "D"}</span><h2>Phase deliverables</h2></div>
                      <span>{session?.gateStatus || session?.phaseStatus || "Active"}</span>
                    </div>
                    {session?.deliverables?.map((deliverable) => (
                      <button
                        key={deliverable}
                        type="button"
                        className="mastermind-deliverable"
                        disabled={deliverable.includes("*")}
                        title={`${session.repo}\\${deliverable.replaceAll("/", "\\")}`}
                        onClick={() => setPreviewPath(deliverable)}
                      >
                        <code>{session.repo}\\{deliverable.replaceAll("/", "\\")}</code>
                      </button>
                    ))}
                  </section>
                )}

                {session?.memory && (
                  <section className="mastermind-stage compact mastermind-chat-artifact">
                    <div className="mastermind-stage-head">
                      <div><span className="mastermind-stage-number">M</span><h2>Shared memory</h2></div>
                      <span>{session.memory.status}</span>
                    </div>
                    <code>Memex · {session.memory.projectKey} · revision {session.memory.latestRevision ?? "none"}</code>
                    {session.memory.error && <div className="mastermind-alert error">{session.memory.error}</div>}
                    {session.skillSource && (
                      <code title={session.skillSource.path}>Skill SHA-256 · {session.skillSource.sha256.slice(0, 16)}…</code>
                    )}
                  </section>
                )}

                {Boolean(selectedConversation.runFailures?.length) && (
                  <details className="mastermind-stage compact mastermind-chat-artifact" open={terminalRun}>
                    <summary className="mastermind-stage-head">
                      <div><span className="mastermind-stage-number">!</span><h2>Run failures</h2></div>
                      <span>{selectedConversation.runFailures!.length} event(s)</span>
                    </summary>
                    {selectedConversation.runFailures!.slice(-5).map((failure, index) => (
                      <div className="mastermind-rejection" key={`${failure.event}-${failure.taskId || failure.node || index}-${index}`}>
                        <code>{failure.kind || failure.event}</code>
                        <span>{failure.detail || "The task entered a failed state without additional provider detail."}</span>
                        {(failure.node || failure.agent) && <small>{[failure.node, failure.agent].filter(Boolean).join(" · ")}</small>}
                      </div>
                    ))}
                  </details>
                )}

                {selectedConversation.lastCycle && (
                  selectedConversation.lastCycle.modelError ||
                  selectedConversation.lastCycle.engineError ||
                  selectedConversation.lastCycle.rejected.length > 0
                ) && (
                  <details className="mastermind-stage compact mastermind-chat-artifact">
                    <summary className="mastermind-stage-head">
                      <div><span className="mastermind-stage-number">!</span><h2>Planning notes</h2></div>
                      <span>{selectedConversation.lastCycle.rejected.length} issue(s)</span>
                    </summary>
                    {selectedConversation.lastCycle.modelError && <div className="mastermind-alert error">Planning model: {selectedConversation.lastCycle.modelError}</div>}
                    {selectedConversation.lastCycle.engineError && <div className="mastermind-alert error">Engine: {selectedConversation.lastCycle.engineError}</div>}
                    {selectedConversation.lastCycle.rejected.map((rejection, index) => (
                      <div className="mastermind-rejection" key={`${rejection.code}-${index}`}>
                        <code>{rejection.code}</code><span>{rejection.message}</span><small>{rejection.hint}</small>
                      </div>
                    ))}
                  </details>
                )}

                <details className="mastermind-stage compact mastermind-chat-artifact mastermind-roster-details">
                  <summary className="mastermind-stage-head">
                    <div><span className="mastermind-stage-number">02</span><h2>Worker roster</h2></div>
                    <span>{session?.roster.length || 0} agents</span>
                  </summary>
                  <div className="mastermind-roster">
                    {session?.roster.map((entry) => (
                      <span key={entry.id} className="mastermind-roster-entry" title={entry.description}>
                        <strong>{entry.name}</strong><small>{entry.model || entry.adapter}</small>
                      </span>
                    ))}
                  </div>
                </details>
              </div>

              {/* Pinned: the question you are answering never scrolls away. */}
              <div className="mastermind-chat-actions">
                {pendingDecisions[0] && (
                  <MastermindDecisionCard
                    key={`${pendingDecisions[0].tool}-${pendingDecisions[0].prompt}`}
                    decision={pendingDecisions[0]}
                    remaining={pendingDecisions.length - 1}
                    disabled={busy !== null || cyclesLeft <= 0}
                    onAnswer={answerDecision}
                  />
                )}

                {!pendingDecisions[0] && (
                  <StageChoices
                    session={session!}
                    committed={committed}
                    runStatus={selectedConversation.runStatus}
                    cyclesLeft={cyclesLeft}
                    busy={busy}
                    terminalRun={terminalRun}
                    onPlan={() => void plan(undefined, phase === "phase-1-discovery" ? "Begin or continue the discovery interview." : phase === "phase-8-build" ? "Continue the Phase 8 task group." : `Continue ${session?.phaseName || "the current phase"}.`)}
                    onAuthorizeWrite={() => void authorizePhaseWrite()}
                    onReprepareRevision={() => void repreparePhaseRevision()}
                    onRecoverArtifact={() => void recoverPhaseArtifact()}
                    onRetryAuthoring={() => void retryPhaseAuthoring()}
                    onApprovePhase={() => void approvePhase()}
                    onAcceptAsIs={() => void acceptPhaseAsIs()}
                    onDrive={() => void drive()}
                    onRefresh={() => void refreshConversation(selectedConversation)}
                  />
                )}
              </div>

              <div className="mastermind-chat-composer">
                <textarea
                  ref={chatInputRef}
                  rows={2}
                  value={chatInput}
                  disabled={busy !== null || cyclesLeft <= 0}
                  placeholder={
                    cyclesLeft <= 0
                      ? "Planning cycle limit reached"
                      : committed
                        ? "Ask Mastermind to add work or adjust the live run…"
                        : "Tell Mastermind what to change, add, or clarify…"
                  }
                  onChange={(event) => setChatInput(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" && !event.shiftKey) {
                      event.preventDefault();
                      void sendChat();
                    }
                  }}
                />
                <div className="mastermind-chat-composer-footer">
                  <span>Enter to send · Shift + Enter for a new line · {cyclesLeft} planning cycle{cyclesLeft === 1 ? "" : "s"} left</span>
                  <button className="btn btn-primary" disabled={busy !== null || cyclesLeft <= 0 || !chatInput.trim()} onClick={() => void sendChat()}>
                    Send
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </section>

      {previewPath && session?.sessionId && (
        <ArtifactPreview
          key={previewPath}
          sessionId={session.sessionId}
          path={previewPath}
          onClose={() => setPreviewPath(null)}
        />
      )}
    </div>
  );
}

function PlanTable({ nodes }: { nodes: PlanNode[] }) {
  return (
    <div className="mastermind-plan-table">
      <div className="mastermind-plan-row mastermind-plan-head"><span>Task</span><span>Agent</span><span>Depends on</span></div>
      {nodes.map((node) => (
        <div className="mastermind-plan-row" key={node.nodeId}>
          <div><strong>{node.objective || node.nodeId}</strong><small>{node.nodeType} · {node.nodeId}</small></div>
          <span>{node.pool || "Unassigned"}</span>
          <span>{node.dependsOn.join(", ") || "—"}</span>
        </div>
      ))}
    </div>
  );
}

function StageStep({
  number,
  label,
  active,
  complete,
}: {
  number: string;
  label: string;
  active: boolean;
  complete: boolean;
}) {
  return (
    <span className={`mastermind-progress-step ${active ? "active" : ""} ${complete ? "complete" : ""}`}>
      <span>{complete ? <IconCheck size={11} /> : number}</span>
      {label}
    </span>
  );
}

/// Beyond this many characters a provider turn is collapsed behind a
/// "Show more" so one long review cannot bury the rest of the transcript.
const TURN_COLLAPSE_CHARS = 1400;

/// Verdict line a reviewer turn opens with, surfaced as a badge so the
/// outcome is readable without parsing the prose.
function reviewVerdict(text: string): "pass" | "fail" | null {
  const head = text.slice(0, 400).toUpperCase();
  if (head.includes("VERDICT: PASS")) return "pass";
  if (head.includes("VERDICT: FAIL")) return "fail";
  return null;
}

function MastermindChatEntry({
  entry,
  planner,
}: {
  entry: MastermindMessage;
  planner: string;
}) {
  const turn = entry.turn;
  const [expanded, setExpanded] = useState(false);
  const collapsible = Boolean(turn) && entry.text.length > TURN_COLLAPSE_CHARS;
  const verdict = turn?.kind === "review" ? reviewVerdict(entry.text) : null;

  const author = turn
    ? turn.agent
    : entry.role === "assistant"
      ? planner
      : entry.role === "user"
        ? "You"
        : "Mastermind";

  return (
    <article className={`chat-turn ${entry.role}${turn ? " provider" : ""}`}>
      <header className="chat-turn-byline">
        <span className="chat-turn-avatar" aria-hidden="true">
          {entry.role === "assistant" ? <IconBot size={13} /> : entry.role === "user" ? "You" : "i"}
        </span>
        <strong>{author}</strong>
        {turn && <span className="chat-turn-kind">{TURN_LABELS[turn.kind]}</span>}
        {turn?.model && <span className="chat-turn-model">{turn.model}</span>}
        {verdict && <span className={`chat-turn-verdict ${verdict}`}>{verdict === "pass" ? "PASS" : "FAIL"}</span>}
        <time dateTime={entry.at}>
          {new Date(entry.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
        </time>
      </header>
      <div className={`chat-turn-body${collapsible && !expanded ? " clipped" : ""}`}>
        {turn ? <ChatMarkdown text={entry.text} /> : <p className="chat-turn-plain">{entry.text}</p>}
      </div>
      {collapsible && (
        <button type="button" className="chat-turn-more" onClick={() => setExpanded((value) => !value)}>
          {expanded ? "Show less" : `Show full ${TURN_LABELS[turn!.kind].toLowerCase()} output`}
        </button>
      )}
    </article>
  );
}

function MastermindDecisionCard({
  decision,
  remaining,
  disabled,
  onAnswer,
}: {
  decision: MastermindDecision;
  remaining: number;
  disabled: boolean;
  onAnswer: (answer: string) => Promise<void>;
}) {
  const [picked, setPicked] = useState<string[]>([]);
  const [custom, setCustom] = useState("");

  const toggle = (option: string) => {
    setPicked((current) => current.includes(option)
      ? current.filter((value) => value !== option)
      : decision.multiSelect
        ? [...current, option]
        : [option]);
  };

  const submit = (answer: string) => {
    const value = answer.trim();
    if (!value) return;
    setPicked([]);
    setCustom("");
    void onAnswer(value);
  };

  return (
    <div className="mastermind-decision" role="group" aria-label="Mastermind needs a decision">
      <div className="mastermind-decision-kicker">
        {decision.tool === "ExitPlanMode" ? "Plan ready for approval" : "Mastermind needs your input"}
        {remaining > 0 && <span>{remaining} more question{remaining === 1 ? "" : "s"} queued</span>}
      </div>
      <h3>{decision.prompt}</h3>
      <p>{decision.multiSelect ? "Select one or more options." : "Select one option, or write your own answer."}</p>
      <div className="mastermind-decision-options">
        {decision.options.map((option, index) => {
          const selected = picked.includes(option);
          return (
            <button
              type="button"
              key={`${index}-${option}`}
              className={selected ? "selected" : ""}
              aria-pressed={selected}
              disabled={disabled}
              onClick={() => toggle(option)}
              onDoubleClick={() => submit(option)}
            >
              <span>{index + 1}</span>
              <strong>{option}</strong>
              <span className="mastermind-decision-check">{selected ? <IconCheck size={11} /> : ""}</span>
            </button>
          );
        })}
      </div>
      <div className="mastermind-decision-actions">
        <button className="btn btn-sm btn-primary" disabled={disabled || picked.length === 0} onClick={() => submit(picked.join(", "))}>
          Continue with {picked.length || "selection"}
        </button>
        <span>Double-click an option to send it immediately</span>
      </div>
      <div className="mastermind-decision-custom">
        <input
          className="input-text"
          value={custom}
          disabled={disabled}
          placeholder="Or type a different answer…"
          onChange={(event) => setCustom(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              submit(custom);
            }
          }}
        />
        <button className="btn btn-sm" disabled={disabled || !custom.trim()} onClick={() => submit(custom)}>Send</button>
      </div>
    </div>
  );
}

export function StageChoices({
  session,
  committed,
  runStatus,
  cyclesLeft,
  busy,
  terminalRun,
  onPlan,
  onAuthorizeWrite,
  onReprepareRevision,
  onRecoverArtifact,
  onRetryAuthoring,
  onApprovePhase,
  onAcceptAsIs,
  onDrive,
  onRefresh,
}: {
  session: SessionSummary;
  committed: boolean;
  runStatus: string | null;
  cyclesLeft: number;
  busy: string | null;
  terminalRun: boolean;
  onPlan: () => void;
  onAuthorizeWrite: () => void;
  onReprepareRevision?: () => void;
  onRecoverArtifact: () => void;
  onRetryAuthoring: () => void;
  onApprovePhase: () => void;
  onAcceptAsIs: () => void;
  onDrive: () => void;
  onRefresh: () => void;
}) {
  const disabled = busy !== null;
  const isBuild = session.phase === "phase-8-build";
  const awaitingApproval = session.phaseStatus === "awaiting-approval";
  const awaitingWriteApproval = session.phaseStatus === "awaiting-write-approval";
  const needsRevision = session.phaseStatus === "needs-revision";
  const blocked = session.phaseStatus === "blocked";
  const stageLabel = `${session.phaseNumber || ""} ${session.phaseName || "Mastermind"}`.trim();
  const prompt = blocked
    ? session.recoveryRequest?.prompt || session.retryRequest?.prompt || session.memory?.error || "This phase is blocked. Refresh after resolving the synchronization error."
    : awaitingApproval
      ? "The deliverables and review are ready. Approve this gate or describe a revision in chat."
      : awaitingWriteApproval
        ? session.writeRequest?.prompt || "The phase is ready for a scoped authoring grant."
      : needsRevision
        ? session.canReprepareRevision
          ? session.reprepareRequest?.prompt || "Re-prepare a server-derived Phase 7.5 remediation scope before authoring."
          : "The review requested changes. Describe the correction in chat, or accept the artifact as-is if no revision is needed."
        : isBuild && committed
          ? runStatus ? `The last run update was ${runStatus}.` : "The current Phase 8 task group is ready to run."
          : "Continue the current phase in a fresh, live-skill-backed planner turn.";

  return (
    <div className="mastermind-stage-choices">
      <div className="mastermind-stage-choice-copy">
        <span>{stageLabel} stage</span>
        <strong>{prompt}</strong>
      </div>
      <div className="mastermind-stage-choice-options">
        {awaitingApproval && (
          <button className="recommended" disabled={disabled || !session.canApprove} onClick={onApprovePhase}>
            <span>1</span><strong>Approve {session.phaseName || "phase"}</strong><small>Sync Memex, checkpoint, and open the next clean session</small>
          </button>
        )}
        {awaitingWriteApproval && (
          <button className="recommended" disabled={disabled || !session.canAuthorizeWrite} onClick={onAuthorizeWrite}>
            <span>1</span><strong>Allow scoped write</strong><small>{session.writeRequest?.deliverables.join(", ") || "Current phase deliverable only"}</small>
          </button>
        )}
        {needsRevision && (
          session.canReprepareRevision ? (
            <button className="recommended" disabled={disabled} onClick={() => onReprepareRevision?.()}>
              <span>1</span><strong>Re-prepare revision scope</strong><small>{session.reprepareRequest?.deliverables.join(", ") || "Server-derived remediation files only"}</small>
            </button>
          ) : null
        )}
        {needsRevision && (
          <button className="recommended" disabled={disabled} onClick={onAcceptAsIs}>
            <span>{session.canReprepareRevision ? "2" : "1"}</span><strong>Accept as-is</strong><small>Waive the review findings and continue without editing this artifact</small>
          </button>
        )}
        {!awaitingApproval && !awaitingWriteApproval && !needsRevision && !blocked && !(isBuild && committed) && (
          <button disabled={disabled || cyclesLeft <= 0} onClick={onPlan}>
            <span>1</span><strong>{session.phase === "phase-1-discovery" ? "Continue discovery" : "Continue phase"}</strong><small>Use the current checkpoint, relevant Memex state, and live Mastermind skill</small>
          </button>
        )}
        {isBuild && committed && (
          <>
            <button className="recommended" disabled={disabled || terminalRun} onClick={onDrive}>
              <span>1</span><strong>{runStatus ? "Continue run" : "Start agents"}</strong><small>Execute the approved workflow in the selected project</small>
            </button>
            <button disabled={disabled} onClick={onRefresh}>
              <span>2</span><strong>Refresh status</strong><small>Read the latest engine state</small>
            </button>
          </>
        )}
        {blocked && (
          session.canRecoverArtifact ? (
            <button className="recommended" disabled={disabled} onClick={onRecoverArtifact}>
              <span>1</span><strong>Review existing artifact</strong><small>No authoring rerun; use the read-only reviewer</small>
            </button>
          ) : session.canRetryAuthoring ? (
            <button className="recommended" disabled={disabled} onClick={onRetryAuthoring}>
              <span>1</span><strong>Retry scoped authoring</strong><small>{session.retryRequest?.deliverables.join(", ") || "Same phase deliverable scope"}</small>
            </button>
          ) : (
            <button disabled={disabled} onClick={onRefresh}>
              <span>1</span><strong>Refresh synchronization</strong><small>Read the daemon checkpoint and Memex status again</small>
            </button>
          )
        )}
      </div>
    </div>
  );
}
