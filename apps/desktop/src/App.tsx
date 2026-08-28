import { FormEvent, KeyboardEvent, PointerEvent, useEffect, useMemo, useRef, useState } from "react";
import { isTauri } from "@tauri-apps/api/core";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { DaemonEvent, rpc } from "./lib/rpc";
import { baseNodes, validateDag, WorkflowNode } from "./lib/workflow";

type View = "workspace" | "mastermind" | "agents" | "memory" | "artifacts" | "providers" | "settings";
type Connection = "connecting" | "online" | "offline";
type ChatMessage = { id: string; author: "user" | "agent" | "tool"; content: string; time: string };
type Conversation = { id: string; title: string; status?: string; parentSessionId?: string };
type Project = { id: string; name: string; path: string };
type Starter = { id: string; label: string; language: string; safe: boolean };
type CatalogProvider = Record<string, unknown>;
type PreparedSession = { prepared: Record<string, unknown>; prepareToken: string; editConfirmationToken?: string };

const nav: Array<{ id: View; label: string; mark: string }> = [
  { id: "workspace", label: "Workspace", mark: "W" }, { id: "mastermind", label: "Mastermind", mark: "M" },
  { id: "agents", label: "Agent Registry", mark: "A" }, { id: "memory", label: "Memory", mark: "K" },
  { id: "artifacts", label: "Artifacts", mark: "F" }, { id: "providers", label: "Providers", mark: "P" }, { id: "settings", label: "Settings", mark: "S" },
];

const commands = ["/agent", "/provider", "/model", "/effort", "/mode", "/scope", "/skill", "/handoff", "/memory", "/attach", "/artifact", "/context", "/run", "/pause", "/resume", "/cancel", "/retry", "/branch", "/clear", "/help"];
const now = () => new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit" }).format(new Date());
const words = (value: unknown) => typeof value === "string" ? value : "";
const toProject = (value: Record<string, unknown>): Project | undefined => {
  const id = words(value.id) || words(value.projectId); const path = words(value.rootPath) || words(value.path) || words(value.workspace) || words(value.root);
  return id && path ? { id, path, name: words(value.name) || path.split(/[\\/]/).filter(Boolean).pop() || id } : undefined;
};
export const normalizeConversation = (value: Record<string, unknown>): Conversation | undefined => {
  const id = words(value.id) || words(value.sessionId);
  return id ? { id, title: words(value.title) || id, status: words(value.status), parentSessionId: words(value.parentSessionId) || undefined } : undefined;
};

export const projectCreatePayload = (rootPath: string) => ({ rootPath });
export const projectOpenPayload = (id: string) => ({ id });
export const projectScaffoldPayload = (parentPath: string, name: string, starterId: string, initialGoal = "") => ({ parentPath, name, starterId, sessionDefaults: initialGoal.trim() ? { initialGoal: initialGoal.trim() } : {} });
export const mastermindStartPayload = (goal: string, repo: string, plannerAdapter: string, plannerModel: string, maxConcurrency: number) => ({ goal, repo, ...(plannerAdapter ? { plannerAdapter } : {}), ...(plannerModel ? { plannerModel } : {}), maxConcurrency: Math.max(1, Math.min(8, maxConcurrency)) });

function Status({ state }: { state: string }) { return <span className={`status status-${state}`}><i />{state}</span>; }

function FolderPathField({ label, value, onChange, required = false }: { label: string; value: string; onChange: (value: string) => void; required?: boolean }) {
  const [pickerNote, setPickerNote] = useState("");
  const browse = async () => {
    if (!isTauri()) {
      setPickerNote("The native folder picker is available in the desktop app. Paste an absolute path in this browser preview.");
      return;
    }
    setPickerNote("");
    try {
      const selected = await openDialog({ directory: true, multiple: false, title: `Choose ${label.toLowerCase()}` });
      if (typeof selected === "string") onChange(selected);
    } catch (reason) {
      setPickerNote(reason instanceof Error ? reason.message : "The folder picker could not be opened.");
    }
  };
  return <label>{label}<div className="path-picker"><input value={value} onChange={(event) => onChange(event.target.value)} required={required} /><button type="button" className="quiet browse-button" onClick={() => void browse()} aria-label={`Browse for ${label.toLowerCase()}`}><svg viewBox="0 0 20 20" aria-hidden="true"><path d="M2.5 5.5h5l1.5 2h8.5v7.25a1.75 1.75 0 0 1-1.75 1.75H4.25a1.75 1.75 0 0 1-1.75-1.75V5.5Z" /><path d="M2.5 7.5V4.75A1.25 1.25 0 0 1 3.75 3.5H7l1.5 2h7.75A1.25 1.25 0 0 1 17.5 6.75v.75" /></svg>Browse</button></div>{pickerNote && <small className="picker-note" role="status">{pickerNote}</small>}</label>;
}

export function App() {
  const [launched, setLaunched] = useState(false);
  const [view, setView] = useState<View>("workspace");
  const [connection, setConnection] = useState<Connection>("connecting");
  const [theme, setTheme] = useState<"dark" | "light">("dark");
  const [events, setEvents] = useState<DaemonEvent[]>([]);
  const [tasks, setTasks] = useState<Array<Record<string, unknown>>>([]);
  const [agents, setAgents] = useState<Array<Record<string, unknown>>>([]);
  const [registry, setRegistry] = useState<Array<Record<string, unknown>>>([]);
  const [catalog, setCatalog] = useState<CatalogProvider[]>([]);
  const [projects, setProjects] = useState<Project[]>([]);
  const [project, setProject] = useState<Project>();
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [showInspector, setShowInspector] = useState(false);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);
  useEffect(() => {
    rpc.onState = setConnection;
    const unlisten = rpc.subscribe((event) => setEvents((current) => [event, ...current].slice(0, 80)));
    rpc.connect();
    return () => { unlisten(); rpc.close(); };
  }, []);
  useEffect(() => {
    if (connection !== "online") return;
    const load = async () => {
      const settled = await Promise.allSettled([
        rpc.request<{ tasks: Array<Record<string, unknown>> }>("tasks.list"),
        rpc.request<{ agents: Array<Record<string, unknown>> }>("agents.list"),
        rpc.request<{ agents: Array<Record<string, unknown>> }>("registry.agents.list"),
        rpc.request<{ sessions: Conversation[] }>("chat.sessions"),
        rpc.request<{ projects: Array<Record<string, unknown>> }>("projects.list"),
        rpc.request<{ providers: CatalogProvider[] }>("registry.catalog"),
        rpc.request("events.subscribe", { afterSeq: rpc.lastEventSeq }),
      ]);
      const value = <T,>(index: number) => settled[index].status === "fulfilled" ? (settled[index] as PromiseFulfilledResult<T>).value : undefined;
      setTasks(value<{ tasks: Array<Record<string, unknown>> }>(0)?.tasks || []);
      setAgents(value<{ agents: Array<Record<string, unknown>> }>(1)?.agents || []);
      setRegistry(value<{ agents: Array<Record<string, unknown>> }>(2)?.agents || []);
      setConversations((value<{ sessions: Array<Record<string, unknown>> }>(3)?.sessions || []).map(normalizeConversation).filter((item): item is Conversation => Boolean(item)));
      const liveProjects = (value<{ projects: Array<Record<string, unknown>> }>(4)?.projects || []).map(toProject).filter((item): item is Project => Boolean(item));
      setProjects(liveProjects); setProject((current) => current || liveProjects[0]);
      setCatalog(value<{ providers: CatalogProvider[] }>(5)?.providers || []);
    };
    void load();
  }, [connection]);

  if (!launched) return <Launcher connection={connection} projects={projects} onProject={(next) => { setProject(next); setLaunched(true); }} />;
  return <main className="app-shell" data-inspector={showInspector}>
    <aside className="rail" aria-label="Primary navigation">
      <button className="brand" onClick={() => setView("workspace")} aria-label="Agent Engineering OS home">AO</button>
      <div className="rail-nav">{nav.map((item) => <button key={item.id} className={view === item.id ? "active" : ""} onClick={() => setView(item.id)} aria-label={item.label} aria-current={view === item.id ? "page" : undefined}><b>{item.mark}</b><span>{item.label}</span></button>)}</div>
      <button className="rail-settings" onClick={() => setTheme(theme === "dark" ? "light" : "dark")} aria-label="Toggle color theme">◐</button>
    </aside>
    <section className="shell-main">
      <header className="topbar"><div><strong>{project?.name || "No project selected"}</strong><span className="path">{project?.path || "Select a project to begin"}</span></div><div className="top-actions"><Status state={connection} /><button className="quiet" onClick={() => setShowInspector((value) => !value)} aria-expanded={showInspector}>Inspector</button><button className="primary" onClick={() => setView("mastermind")} disabled={!project}>New run</button></div></header>
      {view === "workspace" && <Workspace tasks={tasks} agents={agents} events={events} conversations={conversations} onConversations={setConversations} registry={registry} catalog={catalog} project={project} />}
      {view === "mastermind" && <Mastermind project={project} catalog={catalog} />}
      {view === "agents" && <Registry agents={registry} />}
      {view === "memory" && <Memory events={events} project={project} />}
      {view === "artifacts" && <Artifacts events={events} />}
      {view === "providers" && <Providers catalog={catalog} setCatalog={setCatalog} />}
      {view === "settings" && <Settings theme={theme} setTheme={setTheme} />}
    </section>
    {showInspector && <ProjectInspector events={events} agents={agents} />}
  </main>;
}

function Launcher({ connection, projects, onProject }: { connection: Connection; projects: Project[]; onProject: (project: Project) => void }) {
  const [wizard, setWizard] = useState(false);
  const [kind, setKind] = useState<"scaffold" | "existing">("scaffold");
  const [path, setPath] = useState("");
  const [name, setName] = useState("");
  const [goal, setGoal] = useState("");
  const [starterId, setStarterId] = useState("blank-git");
  const [starters, setStarters] = useState<Starter[]>([]);
  const [message, setMessage] = useState("");
  useEffect(() => {
    if (connection !== "online") return;
    void rpc.request<{ starters: Starter[] }>("projects.starters").then((result) => {
      setStarters(result.starters || []);
      if (result.starters?.length) setStarterId(result.starters[0].id);
    }).catch(() => setStarters([]));
  }, [connection]);
  const open = async (item: Project) => { try { const result = await rpc.request<Record<string, unknown>>("projects.open", projectOpenPayload(item.id)); onProject(toProject(result.project as Record<string, unknown>) || item); } catch (reason) { setMessage(reason instanceof Error ? reason.message : "Could not open project"); } };
  const create = async (event: FormEvent) => {
    event.preventDefault();
    setMessage(kind === "scaffold" ? "Creating the project safely…" : "Checking workspace…");
    try {
      const result = kind === "scaffold"
        ? await rpc.request<Record<string, unknown>>("projects.scaffold", projectScaffoldPayload(path, name, starterId, goal))
        : (await rpc.request("projects.preflight", { rootPath: path }), await rpc.request<Record<string, unknown>>("projects.create", { ...projectCreatePayload(path), sessionDefaults: goal.trim() ? { initialGoal: goal.trim() } : {} }));
      const created = toProject((result.project || result) as Record<string, unknown>);
      if (!created) throw new Error("The daemon did not return a project record");
      onProject(created);
    } catch (reason) { setMessage(reason instanceof Error ? reason.message : "Could not create project"); }
  };
  return <main className="launcher"><div className="launcher-mark">AO</div><section><h1>Operate software work<br />with durable context.</h1><p className="intro">Create a project record, inspect the plan, and run specialists with the authority and provenance each task requires.</p>{!wizard ? <><div className="project-launch-list">{projects.length ? projects.map((item) => <button key={item.id} onClick={() => void open(item)}><b>{item.name}</b><span>{item.path}</span></button>) : <p>{connection === "online" ? "No project records yet." : "Connect the local daemon to load project records."}</p>}</div><div className="launcher-actions"><button className="primary" onClick={() => setWizard(true)} disabled={connection !== "online"}>Create or open project</button></div></> : <form className="wizard" onSubmit={(event) => void create(event)}><div className="wizard-head"><span>Project foundation</span><small>Daemon-verified</small></div><label>Project source<select value={kind} onChange={(event) => setKind(event.target.value as "scaffold" | "existing")}><option value="scaffold">New built-in starter</option><option value="existing">Existing folder</option></select></label>{kind === "scaffold" && <><label>Starter<select value={starterId} onChange={(event) => setStarterId(event.target.value)} required>{starters.map((starter) => <option key={starter.id} value={starter.id}>{starter.label}</option>)}</select></label><label>Project name<input value={name} onChange={(event) => setName(event.target.value)} required placeholder="my-agent-project" /></label></>}<FolderPathField label={kind === "scaffold" ? "Parent folder" : "Repository or workspace path"} value={path} onChange={setPath} required /><label>Initial engineering goal<textarea value={goal} onChange={(event) => setGoal(event.target.value)} placeholder="What should the team deliver?" /></label><p className="helper">{kind === "scaffold" ? "Only audited built-in files are written. The daemon stages, initializes Git, and publishes atomically." : "The daemon inspects metadata and registers the existing folder without running project code."} Discovery still requires an explicit run.</p>{message && <p className="runtime-note">{message}</p>}<div><button className="primary" type="submit">{kind === "scaffold" ? "Create project" : "Open folder"}</button><button className="quiet" type="button" onClick={() => setWizard(false)}>Back</button></div></form>}</section><footer>Loopback daemon · local project records · explicit write authority</footer></main>;
}

function Workspace({ tasks, agents, events, conversations, onConversations, registry, catalog, project }: { tasks: Array<Record<string, unknown>>; agents: Array<Record<string, unknown>>; events: DaemonEvent[]; conversations: Conversation[]; onConversations: (rows: Conversation[]) => void; registry: Array<Record<string, unknown>>; catalog: CatalogProvider[]; project?: Project }) {
  const [tab, setTab] = useState<"flow" | "chat">("flow");
  return <section className="workspace"><div className="workspace-tabs" role="tablist"><button role="tab" aria-selected={tab === "flow"} onClick={() => setTab("flow")}>Run flow</button><button role="tab" aria-selected={tab === "chat"} onClick={() => setTab("chat")}>Conversations</button><span>{tasks.length ? `${tasks.length} task projection${tasks.length === 1 ? "" : "s"}` : "No active daemon run"}</span></div>{tab === "flow" ? <div className="workspace-grid"><Graph tasks={tasks} agents={agents} events={events} /><Activity events={events} /></div> : <Chat conversations={conversations} onConversations={onConversations} events={events} registry={registry} catalog={catalog} project={project} />}</section>;
}

function Graph({ tasks, agents, events }: { tasks: Array<Record<string, unknown>>; agents: Array<Record<string, unknown>>; events: DaemonEvent[] }) {
  const [nodes, setNodes] = useState(baseNodes); const [selected, setSelected] = useState("spec"); const drag = useRef<string>();
  const hasLiveProjection = tasks.length > 0 || agents.length > 0;
  const packets = useMemo(() => events.flatMap((event, index) => {
    const kind = event.type.toLowerCase();
    const isTransport = ["output", "handoff", "artifact", "approval", "review", "git"].some((term) => kind.includes(term));
    const from = words(event.payload?.from) || words(event.payload?.sourceNodeId);
    const to = words(event.payload?.to) || words(event.payload?.targetNodeId);
    return isTransport && from && to ? [{ id: `${event.type}-${index}`, from, to }] : [];
  }).slice(0, 6), [events]);
  const node = nodes.find((item) => item.id === selected) || nodes[0];
  const move = (event: PointerEvent<SVGSVGElement>) => { if (!drag.current) return; const target = event.currentTarget.getBoundingClientRect(); const x = Math.max(10, Math.min(930, (event.clientX - target.left) * 960 / target.width)); const y = Math.max(10, Math.min(330, (event.clientY - target.top) * 360 / target.height)); setNodes((all) => all.map((item) => item.id === drag.current ? { ...item, x, y } : item)); };
  return <section className="flow-pane"><div className="section-head"><div><h2>{hasLiveProjection ? "Live task projection" : "Plan template — awaiting a daemon run"}</h2></div><button className="quiet" onClick={() => setNodes(baseNodes)}>Reset layout</button></div><svg className="graph" viewBox="0 0 960 360" role="img" aria-label="Workflow dependency graph. Equivalent task list below." onPointerMove={move} onPointerUp={() => { drag.current = undefined; }}>
    <defs><marker id="arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0,0 L7,3.5 L0,7z" /></marker></defs>
    {nodes.flatMap((item) => (item.needs || []).map((need) => { const source = nodes.find((candidate) => candidate.id === need)!; return <line key={`${need}-${item.id}`} className="edge" x1={source.x + 105} y1={source.y + 35} x2={item.x} y2={item.y + 35} markerEnd="url(#arrow)" />; }))}
    {packets.map((packet) => { const source = nodes.find((item) => item.id === packet.from); const target = nodes.find((item) => item.id === packet.to); return source && target ? <circle key={packet.id} className="packet" r="5"><animateMotion dur=".5s" path={`M${source.x + 100},${source.y + 35} L${target.x},${target.y + 35}`} fill="freeze" /></circle> : null; })}
    {nodes.map((item) => <g key={item.id} transform={`translate(${item.x},${item.y})`} className={`graph-node ${item.id === selected ? "selected" : ""} ${item.state}`} onPointerDown={(event) => { drag.current = item.id; event.currentTarget.setPointerCapture(event.pointerId); }} onClick={() => setSelected(item.id)} tabIndex={0} role="button" aria-label={`${item.title}, ${item.state}`}><rect width="140" height="70" rx="7" /><text x="12" y="25">{item.title}</text><text className="graph-role" x="12" y="47">{item.role}</text><circle cx="122" cy="18" r="4" /></g>)}
  </svg><div className="graph-equivalent"><b>Selected node</b><span>{node.title} · {node.role} · {node.needs?.length ? `depends on ${node.needs.join(", ")}` : "no dependencies"}</span></div><TaskList tasks={tasks} /></section>;
}

function TaskList({ tasks }: { tasks: Array<Record<string, unknown>> }) { return <div className="task-list" aria-label="Equivalent workflow list">{tasks.length ? tasks.map((task, index) => <div key={String(task.id || index)}><Status state={words(task.status) || "idle"} /><span>{words(task.title) || words(task.id) || "Untitled task"}</span><code>{words(task.agentId) || "unassigned"}</code></div>) : <p>No tasks have been published by the daemon. Start a Mastermind run to replace this planning template with a live projection.</p>}</div>; }

function Activity({ events }: { events: DaemonEvent[] }) { return <aside className="activity"><div className="section-head"><div><h2>Event journal</h2></div><small aria-live="polite">{events.length ? "Updated" : "Quiet"}</small></div>{events.length ? <ol>{events.map((event, index) => <li key={`${event.type}-${index}`}><span className="event-dot" /><div><b>{event.type}</b><p>{words(event.payload?.summary) || words(event.payload?.message) || "Journaled daemon event"}</p><time>{event.timestamp || "now"}</time></div></li>)}</ol> : <div className="empty"><b>No event packets yet</b><p>Events will appear here when the local daemon publishes journal changes.</p></div>}</aside>; }

const providerId = (provider: CatalogProvider) => words(provider.adapterId) || words(provider.id) || words(provider.name);
const providerModels = (provider?: CatalogProvider) => (Array.isArray(provider?.models) ? provider?.models : []).map((model) => typeof model === "string" ? model : words((model as Record<string, unknown>).id) || words((model as Record<string, unknown>).name)).filter(Boolean);
const stringArray = (value: unknown) => Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];

function Chat({ conversations, onConversations, events, registry, catalog, project }: { conversations: Conversation[]; onConversations: (rows: Conversation[]) => void; events: DaemonEvent[]; registry: Array<Record<string, unknown>>; catalog: CatalogProvider[]; project?: Project }) {
  const [active, setActive] = useState<string>(); const [messages, setMessages] = useState<ChatMessage[]>([]); const [draft, setDraft] = useState(""); const [agent, setAgent] = useState(""); const [provider, setProvider] = useState(""); const [model, setModel] = useState(""); const [effort, setEffort] = useState("medium"); const [mode, setMode] = useState<"plan" | "accept_edits">("plan"); const [scope, setScope] = useState<"read_only" | "paths" | "workspace">("read_only"); const [allowedPaths, setAllowedPaths] = useState(""); const [temporarySkills, setTemporarySkills] = useState<string[]>([]); const [temporaryDraft, setTemporaryDraft] = useState(""); const [palette, setPalette] = useState(false); const [pendingStart, setPendingStart] = useState<{ message: string; preparation: PreparedSession }>(); const [workspaceConfirmed, setWorkspaceConfirmed] = useState(false); const [handoff, setHandoff] = useState<unknown>(); const transcript = useRef<HTMLDivElement>(null); const workspaceApproval = useRef(false);
  const selectedAgent = registry.find((item) => words(item.id) === agent);
  useEffect(() => { if (!agent && registry.length) setAgent(words(registry[0].id)); }, [agent, registry]);
  useEffect(() => {
    if (!selectedAgent) return;
    const adapter = words(selectedAgent.adapterId);
    if (adapter) setProvider(adapter);
    setModel(words(selectedAgent.model));
  }, [selectedAgent]);
  const selectedProvider = catalog.find((item) => providerId(item) === provider); const models = providerModels(selectedProvider); const savedSkills = stringArray(selectedAgent?.skills); const allSkills = [...savedSkills, ...temporarySkills];
  useEffect(() => { if (!model && models.length) setModel(models[0]); else if (model && !models.includes(model)) setModel(models[0] || ""); }, [model, models]);
  useEffect(() => { const stream = events.filter((event) => event.type.includes("session") || event.type.includes("chat")).map((event, index) => ({ id: `evt-${index}`, author: "agent" as const, content: words(event.payload?.message) || words(event.payload?.text), time: event.timestamp || now() })).filter((row) => row.content); if (stream.length) setMessages((current) => [...current.filter((row) => !row.id.startsWith("evt-")), ...stream]); }, [events]);
  useEffect(() => { transcript.current?.scrollTo({ top: transcript.current.scrollHeight, behavior: "smooth" }); }, [messages]);
  const openConversation = async (id: string) => { setActive(id); try { const result = await rpc.request<{ messages: Array<{ role?: string; content?: string; text?: string; timestamp?: string }> }>("chat.session.get", { sessionId: id }); setMessages((result.messages || []).map((item, index) => ({ id: `${id}-${index}`, author: item.role === "user" ? "user" : "agent", content: item.content || item.text || "", time: item.timestamp || "" }))); } catch { setMessages([]); } };
  const startPrepared = async () => { const pending = pendingStart; if (!pending) return; setPendingStart(undefined); setDraft(""); setMessages((current) => [...current, { id: crypto.randomUUID(), author: "user", content: pending.message, time: now() }]); try { const response = await rpc.request<{ sessionId: string }>("agent.session.start", { agentId: agent, message: pending.message, prepareToken: pending.preparation.prepareToken, ...(pending.preparation.editConfirmationToken ? { editConfirmationToken: pending.preparation.editConfirmationToken } : {}) }); setActive(response.sessionId); onConversations([{ id: response.sessionId, title: pending.message.slice(0, 48), status: "active" }, ...conversations]); } catch (error) { setMessages((current) => [...current, { id: crypto.randomUUID(), author: "tool", content: `Not sent: ${error instanceof Error ? error.message : "daemon unavailable"}`, time: now() }]); } };
  const send = async (event: FormEvent) => {
    event.preventDefault();
    const message = draft.trim();
    if (!message) return;
    const slash = message.match(/^\/(\S+)(?:\s+(.+))?$/);
    if (slash) {
      const [, command, rawValue = ""] = slash;
      const value = rawValue.trim();
      const note = (content: string) => setMessages((current) => [...current, { id: crypto.randomUUID(), author: "tool", content, time: now() }]);
      if (command === "skill" && value) { setTemporarySkills((current) => current.includes(value) ? current : [...current, value]); setDraft(""); note(`Skill ${value} will be resolved by the daemon for the next new conversation.`); return; }
      if (command === "agent" && registry.some((item) => words(item.id) === value)) { setAgent(value); setDraft(""); note(`Agent set to ${value} for the next new conversation.`); return; }
      if (command === "provider" && catalog.some((item) => providerId(item) === value)) { setProvider(value); setDraft(""); note(`Provider set to ${value} for the next new conversation.`); return; }
      if (command === "model" && value) { setModel(value); setDraft(""); note(`Model override set to ${value} for the next new conversation.`); return; }
      if (command === "effort" && ["low", "medium", "high"].includes(value)) { setEffort(value); setDraft(""); note(`Reasoning effort set to ${value}.`); return; }
      if (command === "mode" && ["plan", "edit", "accept_edits"].includes(value)) { const next = value === "plan" ? "plan" : "accept_edits"; setMode(next); if (next === "plan") setScope("read_only"); setDraft(""); note(`Mode set to ${next === "plan" ? "Plan" : "Edit"} for the next new conversation.`); return; }
      if (command === "scope" && ["read_only", "paths", "workspace"].includes(value)) { setScope(value as "read_only" | "paths" | "workspace"); setDraft(""); note(`Write scope set to ${value} for the next new conversation.`); return; }
      if (command === "handoff") { setDraft(""); await previewHandoff(); return; }
      if (command === "clear") { setDraft(""); setMessages([]); return; }
      if (command === "cancel" && active) { setDraft(""); await rpc.request("agent.session.cancel", { sessionId: active }).then(() => note("Conversation cancellation requested.")).catch((reason: Error) => note(reason.message)); return; }
      if (command === "help") { setDraft(""); note(`Available commands: ${commands.join(", ")}. Configuration commands change the next new conversation; saved agent skills remain automatic.`); return; }
      note(`Unknown or incomplete command: /${command}. Use /help to inspect available commands.`);
      return;
    }
    if (active) { try { await rpc.request("agent.session.send", { sessionId: active, message }); setDraft(""); setMessages((current) => [...current, { id: crypto.randomUUID(), author: "user", content: message, time: now() }]); } catch (reason) { setMessages((current) => [...current, { id: crypto.randomUUID(), author: "tool", content: words(reason instanceof Error ? reason.message : "Daemon unavailable"), time: now() }]); } return; }
    if (!agent || !provider || !model) { setMessages((current) => [...current, { id: crypto.randomUUID(), author: "tool", content: "Choose an available registry agent, provider, and model before starting a session.", time: now() }]); return; }
    if (mode === "accept_edits" && scope === "paths" && !allowedPaths.trim()) { setMessages((current) => [...current, { id: crypto.randomUUID(), author: "tool", content: "Add one or more allowed paths for scoped Edit mode.", time: now() }]); return; }
    if (mode === "accept_edits" && scope === "workspace" && !workspaceApproval.current) { setWorkspaceConfirmed(true); return; }
    workspaceApproval.current = false;
    try { const preparation = await rpc.request<PreparedSession>("agent.session.prepare", { agentId: agent, projectId: project?.id, overrides: { workspace: project?.path, adapterId: provider, model, effort, mode, writeScope: scope, ...(scope === "paths" ? { allowedPaths: allowedPaths.split(",").map((path) => path.trim()).filter(Boolean) } : {}), skills: allSkills, timeoutSecs: 600 } }); setPendingStart({ message, preparation }); } catch (reason) { setMessages((current) => [...current, { id: crypto.randomUUID(), author: "tool", content: `Session preparation failed: ${reason instanceof Error ? reason.message : "daemon unavailable"}`, time: now() }]); }
  };
  const previewHandoff = async () => { if (!active || !project) return; const toAgent = registry.find((item) => words(item.id) !== agent); if (!toAgent) return; try { setHandoff(await rpc.request("conversation.handoff.preview", { sessionId: active, toAgent: words(toAgent.id), summary: "Curated operator handoff from this conversation." })); } catch (reason) { setHandoff({ error: reason instanceof Error ? reason.message : "Preview unavailable" }); } };
  const commitHandoff = async () => { const packet = handoff && typeof handoff === "object" ? (handoff as Record<string, unknown>).handoff as Record<string, unknown> | undefined : undefined; const toAgent = words(packet?.toAgent); if (!active || !project || !packet || !toAgent) return; try { await rpc.request("conversation.handoff.commit", { sessionId: active, toAgent, summary: words(packet.summary), artifactRefs: Array.isArray(packet.artifactRefs) ? packet.artifactRefs : [], projectId: project.id, taskId: `conversation:${active}` }); const preparation = await rpc.request<PreparedSession>("agent.session.prepare", { agentId: toAgent, projectId: project.id, overrides: { projectId: project.id, workspace: project.path } }); const message = `Continue the curated handoff from conversation ${active}. Review the linked handoff context before acting.`; const started = await rpc.request<{ sessionId: string }>("agent.session.start", { agentId: toAgent, message, prepareToken: preparation.prepareToken }); setAgent(toAgent); setActive(started.sessionId); setMessages([{ id: crypto.randomUUID(), author: "tool", content: "Curated handoff committed. This linked conversation is ready for the recipient.", time: now() }]); onConversations([{ id: started.sessionId, title: `Handoff from ${active.slice(0, 8)}`, status: "active", parentSessionId: active }, ...conversations]); setHandoff(undefined); } catch (reason) { setMessages((current) => [...current, { id: crypto.randomUUID(), author: "tool", content: `Handoff not started: ${reason instanceof Error ? reason.message : "daemon unavailable"}`, time: now() }]); } };
  const keydown = (event: KeyboardEvent<HTMLTextAreaElement>) => { if (event.key === "/" && !draft) setPalette(true); if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) { event.preventDefault(); event.currentTarget.form?.requestSubmit(); } };
  return <section className="chat"><aside className="conversations"><div className="section-head"><h2>Conversations</h2><button className="quiet" onClick={() => { setActive(undefined); setMessages([]); }}>New</button></div>{conversations.length ? <ol>{conversations.map((conversation) => <li key={conversation.id}><button className={active === conversation.id ? "active" : ""} onClick={() => void openConversation(conversation.id)}><b>{conversation.title || conversation.id}</b><small>{conversation.parentSessionId ? `handoff from ${conversation.parentSessionId.slice(0, 8)}` : conversation.status || "saved session"}</small></button></li>)}</ol> : <div className="empty"><b>No saved conversations</b><p>Start a message to establish a durable agent session.</p></div>}</aside><div className="chat-main"><div className="authority"><label>Agent<select value={agent} onChange={(event) => setAgent(event.target.value)} disabled={!registry.length}><option value="">{registry.length ? "Select agent" : "No registry agents"}</option>{registry.filter((item) => item.enabled !== false).map((item) => <option key={words(item.id)} value={words(item.id)}>{words(item.name) || words(item.id)}</option>)}</select></label><label>Provider<select value={provider} onChange={(event) => { setProvider(event.target.value); setModel(""); }} disabled={!catalog.length}><option value="">{catalog.length ? "Select provider" : "No provider catalog"}</option>{catalog.map((item) => <option key={providerId(item)} value={providerId(item)}>{providerId(item)}</option>)}</select></label><label>Model<select value={model} onChange={(event) => setModel(event.target.value)} disabled={!models.length}><option value="">{models.length ? "Select model" : "No catalog model"}</option>{models.map((item) => <option key={item}>{item}</option>)}</select></label><label>Effort<select value={effort} onChange={(event) => setEffort(event.target.value)}><option>low</option><option>medium</option><option>high</option></select></label><label>Mode<select value={mode} onChange={(event) => { const next = event.target.value as "plan" | "accept_edits"; setMode(next); if (next === "plan") setScope("read_only"); }}><option value="plan">Plan</option><option value="accept_edits">Edit</option></select></label><label>Write scope<select value={scope} disabled={mode === "plan"} onChange={(event) => setScope(event.target.value as "read_only" | "paths" | "workspace")}><option value="read_only">read-only</option><option value="paths">specific paths</option><option value="workspace">whole workspace</option></select></label></div>{mode === "accept_edits" && scope === "paths" && <label className="allowed-paths">Allowed paths<input value={allowedPaths} onChange={(event) => setAllowedPaths(event.target.value)} placeholder="src, tests/integration" /></label>}<div className="skill-row"><span>Saved skills</span>{savedSkills.length ? savedSkills.map((skill) => <span className="chip" key={skill}>{skill}</span>) : <span>No saved skills</span>}{temporarySkills.map((skill) => <button className="chip" key={skill} onClick={() => setTemporarySkills((all) => all.filter((item) => item !== skill))}>{skill} ×</button>)}<input value={temporaryDraft} onChange={(event) => setTemporaryDraft(event.target.value)} placeholder="Temporary skill or use /skill" /><button className="chip add" onClick={() => { if (temporaryDraft.trim()) { setTemporarySkills((all) => [...all, temporaryDraft.trim()]); setTemporaryDraft(""); } }}>Add</button></div><div className="transcript" ref={transcript} aria-live="polite">{messages.length ? messages.map((message) => <article key={message.id} className={`message ${message.author}`}><header><b>{message.author === "user" ? "You" : message.author === "agent" ? agent : "Runtime"}</b><time>{message.time}</time></header><p>{message.content}</p></article>) : <div className="empty transcript-empty"><b>Begin with a scoped request</b><p>The daemon will resolve agent authority before the first message starts a session.</p></div>}</div><form className="composer" onSubmit={(event) => void send(event)}><textarea aria-label="Message to agent" value={draft} onChange={(event) => setDraft(event.target.value)} onKeyDown={keydown} placeholder="Describe the engineering outcome…  Type / for commands" /><div><span>{mode} · {scope} · {allSkills.length} skills</span><button type="button" className="quiet" onClick={() => void previewHandoff()} disabled={!active || !project}>Handoff preview</button><button type="button" className="quiet" onClick={() => setPalette(true)}>Commands</button><button type="submit" className="primary">Send</button></div></form>{handoff !== undefined && <HandoffPreview data={handoff} onCommit={() => void commitHandoff()} />}{palette && <CommandPalette onPick={(command) => { setDraft(`${command} `); setPalette(false); }} onClose={() => setPalette(false)} />}{workspaceConfirmed && !pendingStart && <div className="modal-backdrop"><section className="modal" role="dialog" aria-modal="true"><h2>Confirm whole-workspace edits</h2><p>This grants the prepared session authority across {project?.path || "the selected workspace"}. The daemon will still return the resolved authority for review.</p><div><button className="primary" onClick={() => { workspaceApproval.current = true; setWorkspaceConfirmed(false); void send({ preventDefault() {} } as FormEvent); }}>Prepare with workspace scope</button><button className="quiet" onClick={() => setWorkspaceConfirmed(false)}>Cancel</button></div></section></div>}{pendingStart && <div className="modal-backdrop"><section className="modal" role="dialog" aria-modal="true"><h2>Resolved session authority</h2><p>{JSON.stringify(pendingStart.preparation.prepared)}</p><p>Review the daemon-resolved authority before creating this session.</p><div><button className="primary" onClick={() => void startPrepared()}>Start session</button><button className="quiet" onClick={() => setPendingStart(undefined)}>Cancel</button></div></section></div>}</div><ChatInspector active={active} events={events} /></section>;
}

function CommandPalette({ onPick, onClose }: { onPick: (command: string) => void; onClose: () => void }) { const [query, setQuery] = useState(""); const visible = commands.filter((command) => command.includes(query.toLowerCase())); return <div className="command-palette" role="dialog" aria-label="Command palette"><div><input autoFocus value={query} onChange={(event) => setQuery(event.target.value)} onKeyDown={(event) => { if (event.key === "Escape") onClose(); }} placeholder="Search command" /><button className="quiet" onClick={onClose}>Close</button></div><ol>{visible.map((command) => <li key={command}><button onClick={() => onPick(command)}><code>{command}</code><span>{command === "/handoff" ? "Create curated context preview" : "Configure or control this session"}</span></button></li>)}</ol></div>; }

function ChatInspector({ active, events }: { active?: string; events: DaemonEvent[] }) { const [manifest, setManifest] = useState<Record<string, unknown>>(); useEffect(() => { if (!active) { setManifest(undefined); return; } void rpc.request<{ contextManifest?: Record<string, unknown> }>("chat.session.get", { sessionId: active }).then((result) => setManifest(result.contextManifest)).catch(() => setManifest(undefined)); }, [active]); const skills = stringArray(manifest?.skills); const artifacts = Array.isArray(manifest?.artifacts) ? manifest?.artifacts : []; return <aside className="chat-inspector"><h2>Session context</h2>{active ? <dl><div><dt>Session</dt><dd><code>{active}</code></dd></div><div><dt>Skills in manifest</dt><dd>{skills.length}</dd></div><div><dt>Artifact references</dt><dd>{artifacts.length}</dd></div></dl> : <p>No selected session.</p>}<details><summary>Context manifest</summary><p>{manifest ? JSON.stringify(manifest) : "The daemon has not supplied a context manifest."}</p></details><details><summary>Recent runtime activity ({events.length})</summary><p>{events.length ? events[0].type : "No event receipt"}</p></details></aside>; }

function Mastermind({ project, catalog }: { project?: Project; catalog: CatalogProvider[] }) {
  const phases = ["Discovery", "Specification", "Design", "Implementation", "Review", "Verification"];
  const [phase, setPhase] = useState(0); const [goal, setGoal] = useState(""); const [repo, setRepo] = useState(""); const [plannerAdapter, setPlannerAdapter] = useState(""); const [plannerModel, setPlannerModel] = useState(""); const [maxConcurrency, setMaxConcurrency] = useState(4); const [sessionId, setSessionId] = useState(""); const [nodes, setNodes] = useState(baseNodes); const [message, setMessage] = useState(""); const error = validateDag(nodes);
  useEffect(() => { if (project?.path) setRepo(project.path); }, [project?.path]);
  useEffect(() => { if (!plannerAdapter && catalog.length) setPlannerAdapter(providerId(catalog[0])); }, [catalog, plannerAdapter]);
  const plannerProvider = catalog.find((provider) => providerId(provider) === plannerAdapter); const plannerModels = providerModels(plannerProvider);
  useEffect(() => { if (!plannerModel || !plannerModels.includes(plannerModel)) setPlannerModel(plannerModels[0] || ""); }, [plannerModel, plannerModels]);
  const start = async (event: FormEvent) => { event.preventDefault(); if (error) { setMessage(error); return; } setMessage("Starting discovery run…"); try { const result = await rpc.request<{ sessionId?: string }>("mastermind.start", mastermindStartPayload(goal, repo, plannerAdapter, plannerModel, maxConcurrency)); setSessionId(result.sessionId || ""); setMessage("Discovery run created. The daemon owns all execution state."); } catch (reason) { setMessage(reason instanceof Error ? reason.message : "Unable to start run"); } };
  const advance = async () => { if (!sessionId) return; try { await rpc.request("mastermind.approvePhase", { sessionId }); setPhase((value) => Math.min(phases.length - 1, value + 1)); setMessage("Phase approval recorded."); } catch (reason) { setMessage(reason instanceof Error ? reason.message : "Approval could not be recorded"); } };
  return <section className="mastermind"><header className="page-head"><div><h1>Phase-gated execution</h1><p>Plan changes remain editable and validated before execution. Approvals never imply write authority.</p></div><Status state={sessionId ? "active" : "idle"} /></header><ol className="phase-flow">{phases.map((item, index) => <li key={item} className={index === phase ? "active" : index < phase ? "done" : ""}><button onClick={() => setPhase(index)}><span>{String(index +1).padStart(2, "0")}</span>{item}</button></li>)}</ol>{!sessionId ? <form className="run-form" onSubmit={(event) => void start(event)}><label>Engineering goal<textarea value={goal} onChange={(event) => setGoal(event.target.value)} required placeholder="Describe the user outcome, constraints, and definition of done." /></label><FolderPathField label="Repository path" value={repo} onChange={setRepo} required /><label>Planner adapter<select value={plannerAdapter} onChange={(event) => setPlannerAdapter(event.target.value)}><option value="">Use daemon default</option>{catalog.map((provider) => <option key={providerId(provider)} value={providerId(provider)}>{providerId(provider)}</option>)}</select></label><label>Planner model<select value={plannerModel} onChange={(event) => setPlannerModel(event.target.value)} disabled={!plannerModels.length}><option value="">Use adapter default</option>{plannerModels.map((model) => <option key={model} value={model}>{model}</option>)}</select></label><label>Maximum concurrency <output>{maxConcurrency}</output><input type="range" min="1" max="8" value={maxConcurrency} onChange={(event) => setMaxConcurrency(Number(event.target.value))} /></label><button className="primary" type="submit" disabled={!project || Boolean(error)}>Start discovery</button>{message && <p className="runtime-note" aria-live="polite">{message}</p>}</form> : <div className="phase-work"><div><h2>{phases[phase]}</h2><p>Review the phase output and plan topology. The server is authoritative for phase state.</p><div className="phase-actions"><button className="primary" onClick={() => void advance()}>Approve phase</button><button className="quiet" onClick={() => void rpc.request("mastermind.revisePhase", { sessionId, guidance: "Please revise the current phase with the operator's constraints." }).catch((reason: Error) => setMessage(reason.message))}>Request revision</button></div>{message && <p className="runtime-note" aria-live="polite">{message}</p>}</div></div>}<DagEditor nodes={nodes} setNodes={setNodes} error={error} /></section>;
}

function DagEditor({ nodes, setNodes, error }: { nodes: WorkflowNode[]; setNodes: (nodes: WorkflowNode[]) => void; error?: string }) { const [draft, setDraft] = useState(""); const addNode = () => { const id = draft.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-"); if (!id) return; setNodes([...nodes, { id, title: draft.trim(), role: "Unassigned", state: "idle", x: 200, y: 300, needs: [] }]); setDraft(""); }; return <section className="dag-editor"><div className="section-head"><div><h2>Execution topology</h2></div><span className={error ? "validation error" : "validation"}>{error || "Valid acyclic dependency graph"}</span></div><div className="dag-table">{nodes.map((node) => <div key={node.id}><code>{node.id}</code><input aria-label={`${node.id} title`} value={node.title} onChange={(event) => setNodes(nodes.map((item) => item.id === node.id ? { ...item, title: event.target.value } : item))} /><input aria-label={`${node.id} dependencies`} value={(node.needs || []).join(", ")} onChange={(event) => setNodes(nodes.map((item) => item.id === node.id ? { ...item, needs: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) } : item))} /><button className="quiet danger" onClick={() => setNodes(nodes.filter((item) => item.id !== node.id))}>Remove</button></div>)}</div><div className="add-node"><input value={draft} onChange={(event) => setDraft(event.target.value)} placeholder="New node title" /><button className="quiet" onClick={addNode}>Add node</button><small>Dependencies are comma-separated node IDs. Changes remain local until the daemon accepts a plan mutation API.</small></div></section>; }

function HandoffPreview({ data, onCommit }: { data: unknown; onCommit?: () => void }) { const response = data && typeof data === "object" ? data as Record<string, unknown> : {}; const object = response.handoff && typeof response.handoff === "object" ? response.handoff as Record<string, unknown> : response; return <aside className="handoff-preview"><h3>Curated handoff preview</h3><dl><div><dt>From</dt><dd>{words(object.fromAgent) || "—"}</dd></div><div><dt>Recipient</dt><dd>{words(object.toAgent) || "—"}</dd></div><div><dt>Artifact references</dt><dd>{Array.isArray(object.artifactRefs) ? object.artifactRefs.length : 0}</dd></div><div><dt>Skill bindings</dt><dd>{Array.isArray(object.skillBindings) ? object.skillBindings.length : 0}</dd></div></dl>{words(response.error) && <p>{words(response.error)}</p>}{onCommit && <button className="quiet" onClick={onCommit} disabled={!words(object.toAgent)}>Commit & start linked conversation</button>}</aside>; }

function Registry({ agents }: { agents: Array<Record<string, unknown>> }) { return <section className="resource-page"><header className="page-head"><div><h1>Specialists and durable skills</h1><p>Built-ins are editable but their runtime permissions remain visible.</p></div><button className="primary" disabled>New agent</button></header>{agents.length ? <div className="records">{agents.map((agent, index) => <article key={String(agent.id || index)}><Status state={words(agent.enabled) === "false" ? "idle" : "active"} /><div><h2>{words(agent.name) || words(agent.id) || "Unnamed agent"}</h2><p>{words(agent.description) || "No operator description supplied."}</p></div><code>{words(agent.adapterId) || "adapter not set"} · {words(agent.model) || "model not set"}</code></article>)}</div> : <div className="empty large"><b>No registry projection received</b><p>Connect the daemon to manage persisted agents and their automatically applied skills.</p></div>}</section>; }

function Memory({ events, project }: { events: DaemonEvent[]; project?: Project }) { const [query, setQuery] = useState(""); const [memories, setMemories] = useState<Array<Record<string, unknown>>>([]); const [handoffs, setHandoffs] = useState<Array<Record<string, unknown>>>([]); const [error, setError] = useState(""); const loadHandoffs = async () => { if (!project) return; try { const result = await rpc.request<{ handoffs: Array<Record<string, unknown>> }>("handoffs.list", { projectId: project.id }); setHandoffs(result.handoffs || []); } catch (reason) { setError(reason instanceof Error ? reason.message : "Handoffs unavailable"); } }; useEffect(() => { void loadHandoffs(); }, [project?.id]); const search = async (event: FormEvent) => { event.preventDefault(); if (!project || !query.trim()) return; try { const result = await rpc.request<{ memories: Array<Record<string, unknown>> }>("memory.search", { projectId: project.id, query: query.trim(), limit: 30 }); setMemories(result.memories || []); setError(""); } catch (reason) { setError(reason instanceof Error ? reason.message : "Memory search unavailable"); } }; return <section className="resource-page"><header className="page-head"><div><h1>Scoped, attributable context</h1><p>Project memory is distinct from agent scratch space and is never silently added to a handoff.</p></div></header>{!project ? <div className="empty large"><b>Select a project</b><p>Memory queries are always tied to a daemon project record.</p></div> : <><form className="settings-form" onSubmit={(event) => void search(event)}><label>Search project memory<input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Decisions, files, constraints…" /></label><button className="primary" type="submit" disabled={!query.trim()}>Search memory</button>{error && <p className="runtime-note">{error}</p>}</form><div className="records"><article><Status state="idle" /><div><h2>{memories.length} memory result{memories.length === 1 ? "" : "s"}</h2><p>{memories.length ? JSON.stringify(memories) : "Run a query to load attributable memory records."}</p></div></article><article><Status state="idle" /><div><h2>{handoffs.length} handoff record{handoffs.length === 1 ? "" : "s"}</h2><p>{handoffs.length ? JSON.stringify(handoffs) : "No handoff records returned for this project."}</p></div></article><article><Status state="idle" /><div><h2>{events.length} local journal receipt{events.length === 1 ? "" : "s"}</h2><p>Journal receipts are not silently promoted to semantic memory.</p></div></article></div></>}</section>; }

function Artifacts({ events }: { events: DaemonEvent[] }) { const artifactEvents = events.filter((event) => event.type.includes("artifact") || event.type.includes("handoff")); return <section className="resource-page"><header className="page-head"><div><h1>Reviewable outputs</h1><p>Artifacts and handoffs are referenced by immutable IDs and source events.</p></div></header>{artifactEvents.length ? <div className="records">{artifactEvents.map((event, index) => <article key={index}><Status state="done" /><div><h2>{event.type}</h2><p>{words(event.payload?.summary) || "Event-carried artifact receipt"}</p></div><code>{event.timestamp || "current session"}</code></article>)}</div> : <div className="empty large"><b>No artifact receipt yet</b><p>Artifacts appear after the daemon journals an output or curated handoff.</p></div>}</section>; }

function Providers({ catalog, setCatalog }: { catalog: CatalogProvider[]; setCatalog: (catalog: CatalogProvider[]) => void }) { const [error, setError] = useState(""); const refresh = async () => { try { const result = await rpc.request<{ providers: CatalogProvider[] }>("registry.catalog"); setCatalog(result.providers || []); setError(""); } catch (reason) { setError(reason instanceof Error ? reason.message : "Catalog unavailable"); } }; return <section className="resource-page"><header className="page-head"><div><h1>Model routing</h1><p>Catalog availability is queried from the daemon; credentials are not rendered in the desktop client.</p></div><button className="quiet" onClick={() => void refresh()}>Refresh catalog</button></header>{error ? <div className="empty large"><b>Catalog unavailable</b><p>{error}</p></div> : catalog.length ? <div className="records">{catalog.map((provider) => <article key={providerId(provider)}><Status state="online" /><div><h2>{providerId(provider)}</h2><p>{providerModels(provider).join(", ") || "No model list returned by this adapter."}</p></div></article>)}</div> : <div className="empty large"><b>Waiting for provider catalog</b><p>Connect the daemon to inspect registered providers and supported models.</p></div>}</section>; }

function Settings({ theme, setTheme }: { theme: "dark" | "light"; setTheme: (theme: "dark" | "light") => void }) { return <section className="resource-page"><header className="page-head"><div><h1>Local operator preferences</h1><p>Connection configuration follows the workspace environment endpoint when supplied.</p></div></header><div className="settings-form"><label>Theme<select value={theme} onChange={(event) => setTheme(event.target.value as "dark" | "light")}><option value="dark">Dark</option><option value="light">Light</option></select></label><label>Daemon WebSocket endpoint<input readOnly value={import.meta.env.AGENTOS_WS_ADDR || "ws://127.0.0.1:8741"} /></label><p className="helper">The endpoint is resolved at launch. This UI reconnects safely when the daemon restarts.</p></div></section>; }

function ProjectInspector({ events, agents }: { events: DaemonEvent[]; agents: Array<Record<string, unknown>> }) { return <aside className="project-inspector"><div className="section-head"><div><h2>Authority & context</h2></div></div><dl><div><dt>Runtime agents</dt><dd>{agents.length || "—"}</dd></div><div><dt>Journal receipts</dt><dd>{events.length}</dd></div><div><dt>Default scope</dt><dd>read-only</dd></div></dl><details open><summary>Scope resolution</summary><p>Paths are explicitly selected in chat. A run does not gain write access from a phase approval.</p></details></aside>; }
