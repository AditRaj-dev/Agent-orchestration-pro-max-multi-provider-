// F-13 Agents View: the dynamic agent registry — roster, editor, and chat
// with the agent-creator / researcher. Agents are data: provider, model,
// skills and permissions are rows, editable here, journaled by the daemon.
import { useEffect, useMemo, useRef, useState } from "react";
import { IconAgents, IconBot, IconCheck, IconClose } from "../components/Icons";
import { Modal } from "../components/Modal";
import { parseDraft } from "../store/draft";
import {
  getRegistryStore,
  useActiveThread,
  useChatSession,
  useChatThreads,
  useVisibleTranscript,
  useAgentDecisions,
  useAgentHistory,
  useViewingSession,
  useRegistryAgents,
  useRegistryCatalog,
  useRegistryConnected,
  useRegistrySkills,
  useRegistryState,
} from "../store/registry";
import type { AgentRecord, ChatMessage, DecisionPrompt } from "../types";

/** A blank draft with the safe defaults mirrored from the daemon. */
function blankDraft(): Partial<AgentRecord> {
  return {
    id: "",
    name: "",
    description: "",
    adapterId: "antigravity-agy",
    model: null,
    effort: null,
    mode: "plan",
    skills: [],
    toolAllowlist: [],
    toolDenylist: [],
    timeoutSecs: 600,
    builtin: false,
    enabled: true,
  };
}

export function Agents() {
  const registry = useRegistryState();
  const store = getRegistryStore();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [draft, setDraft] = useState<Partial<AgentRecord> | null>(null);
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);
  const [pasteOpen, setPasteOpen] = useState(false);
  const [pasteText, setPasteText] = useState("");
  const [pasteError, setPasteError] = useState<string | null>(null);
  const [draftWarnings, setDraftWarnings] = useState<string[]>([]);

  const selected = registry.agents.find((a) => a.id === selectedId) || null;

  const startCreate = () => {
    setSelectedId(null);
    setDraft(blankDraft());
    setDraftWarnings([]);
    setFormError(null);
  };

  const startEdit = (agent: AgentRecord) => {
    setSelectedId(agent.id);
    setDraft({ ...agent });
    setDraftWarnings([]);
    setFormError(null);
  };

  const loadProposal = (proposal: AgentRecord) => {
    setSelectedId(null);
    setDraftWarnings([]);
    // The draft keeps the creator's slug unless it collides — the daemon
    // refuses duplicates with the reason, and the user can rename.
    setDraft({ ...proposal, builtin: false });
    setFormError(null);
  };

  const loadPasted = () => {
    setPasteError(null);
    try {
      const { draft: parsed, warnings } = parseDraft(
        pasteText,
        registry.skills.map((skill) => skill.id),
      );
      setSelectedId(null);
      setDraft(parsed);
      setDraftWarnings(warnings);
      setFormError(null);
      setPasteOpen(false);
      setPasteText("");
    } catch (err: any) {
      setPasteError(err?.message || String(err));
    }
  };

  const save = async () => {
    if (!draft) return;
    setSaving(true);
    setFormError(null);
    try {
      if (selected) {
        await store.updateAgent(draft as AgentRecord);
      } else {
        const created = await store.createAgent(
          draft as Partial<AgentRecord> & { id: string; name: string; adapterId: string },
        );
        setSelectedId(created.id);
      }
      setDraft(null);
    } catch (err: any) {
      setFormError(err?.message || String(err));
    } finally {
      setSaving(false);
    }
  };

  const remove = async (agent: AgentRecord) => {
    try {
      await store.deleteAgent(agent.id);
      if (selectedId === agent.id) {
        setSelectedId(null);
        setDraft(null);
      }
    } catch (err: any) {
      setFormError(err?.message || String(err));
    }
  };

  return (
    <div className="view-body agents-view">
      {/* Roster */}
      <section className="panel agents-roster">
        <div className="panel-header">
          <div className="panel-title">
            <IconAgents size={15} />
            <span>Agent Registry ({registry.agents.length})</span>
          </div>
          <div style={{ display: "flex", gap: 6 }}>
            <button
              className="btn btn-sm"
              title="Paste the JSON block an agent gave you"
              onClick={() => {
                setPasteError(null);
                setPasteOpen(true);
              }}
            >
              Paste draft
            </button>
            <button className="btn btn-primary btn-sm" onClick={startCreate}>
              + New Agent
            </button>
          </div>
        </div>
        {registry.error && (
          <div className="seam-notice" style={{ margin: "0 16px 8px" }}>
            <div>
              <div className="seam-notice-title">Registry unavailable</div>
              <div>{registry.error}</div>
            </div>
          </div>
        )}
        <div className="agents-roster-list">
          {registry.agents.map((agent) => (
            <div
              key={agent.id}
              className={`agent-card ${selectedId === agent.id ? "selected" : ""} ${
                agent.enabled ? "" : "disabled"
              } ${registry.running[agent.id] ? "running" : ""}`}
              onClick={() => startEdit(agent)}
            >
              <div className="agent-card-head">
                <span className="agent-card-name">
                  {agent.name}
                  {agent.builtin && <span className="agent-builtin-tag" title="Built-in: editable, not deletable">built-in</span>}
                </span>
                <label
                  className="agent-toggle"
                  title={agent.enabled ? "Routing enabled" : "Routing disabled"}
                  onClick={(e) => e.stopPropagation()}
                >
                  <input
                    type="checkbox"
                    checked={agent.enabled}
                    onChange={(e) =>
                      store.setAgentEnabled(agent.id, e.target.checked).catch(() => undefined)
                    }
                  />
                  <span>{agent.enabled ? "on" : "off"}</span>
                </label>
              </div>
              <div className="agent-card-desc" title={agent.description}>
                {agent.description}
              </div>
              <div className="agent-card-meta">
                <span className="agent-chip provider">{agent.adapterId}</span>
                {agent.model && <span className="agent-chip model">{agent.model}</span>}
                <span className={`agent-chip mode-${agent.mode}`}>{agent.mode}</span>
              </div>
              {/* Only agents holding a provider session get this row, so an
                  idle roster stays compact. Stopping from here kills the
                  process without having to switch the chat to that agent. */}
              {registry.sessionIds[agent.id] && (
                <div
                  className="agent-card-session"
                  onClick={(e) => e.stopPropagation()}
                >
                  <span
                    className={`chat-session-dot ${
                      registry.running[agent.id] ? "running" : "idle"
                    }`}
                  />
                  <span className="hint">
                    {registry.running[agent.id] ? "running a turn" : "session idle"}
                  </span>
                  <button
                    className={`btn btn-sm ${registry.running[agent.id] ? "btn-danger" : ""}`}
                    title={
                      registry.running[agent.id]
                        ? "Kill the provider process for this turn"
                        : "End the live provider session"
                    }
                    onClick={() => {
                      store.cancelChat(agent.id).catch(() => undefined);
                    }}
                  >
                    Stop
                  </button>
                </div>
              )}
              {agent.skills.length > 0 && (
                <div className="agent-card-skills">
                  {agent.skills.slice(0, 2).map((skill) => (
                    <span key={skill} className="agent-chip skill">
                      {skill}
                    </span>
                  ))}
                  {agent.skills.length > 2 && (
                    <span className="agent-chip skill" title={agent.skills.join(", ")}>
                      +{agent.skills.length - 2}
                    </span>
                  )}
                </div>
              )}
            </div>
          ))}
          {registry.loaded && registry.agents.length === 0 && (
            <div className="agents-empty">No agents — create one or talk to the Agent Creator.</div>
          )}
        </div>
      </section>

      {/* Editor / detail */}
      <section className="panel agents-editor">
        {draft ? (
          <AgentEditor
            draft={draft}
            editing={!!selected}
            saving={saving}
            error={formError}
            warnings={draftWarnings}
            onChange={setDraft}
            onSave={save}
            onCancel={() => {
              setDraft(null);
              setFormError(null);
            }}
            onDelete={selected && !selected.builtin ? () => selected && remove(selected) : null}
          />
        ) : selected ? (
          <AgentDetail agent={selected} onEdit={() => startEdit(selected)} />
        ) : (
          <div className="agents-editor-empty">
            <IconBot size={28} />
            <p>Select an agent to inspect it, or create one — manually or through the Agent Creator chat.</p>
            <p className="hint">
              Agents are rows: provider, model, skills and tool permissions. The orchestrator
              routes work to them by id; the daemon journals every change.
            </p>
          </div>
        )}
      </section>

      {/* Chat */}
      <section className="panel agents-chat">
        <ChatPanel onProposal={loadProposal} />
      </section>

      <Modal
        isOpen={pasteOpen}
        onClose={() => setPasteOpen(false)}
        title="Paste an agent draft"
      >
        <p className="hint" style={{ marginBottom: 8 }}>
          Paste the JSON block an agent gave you — with or without the ```json
          fence. It loads into the form, where you can fix anything before
          registering.
        </p>
        <textarea
          className="input-text"
          rows={14}
          autoFocus
          spellCheck={false}
          style={{ width: "100%", fontFamily: "var(--font-mono, monospace)", fontSize: 12 }}
          placeholder={'{\n  "id": "sql-reviewer",\n  "name": "SQL Reviewer",\n  ...\n}'}
          value={pasteText}
          onChange={(e) => setPasteText(e.target.value)}
        />
        {pasteError && (
          <div className="agents-form-error" style={{ margin: "8px 0 0" }}>
            {pasteError}
          </div>
        )}
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 10 }}>
          <button className="btn btn-sm" onClick={() => setPasteOpen(false)}>
            Cancel
          </button>
          <button
            className="btn btn-sm btn-primary"
            disabled={!pasteText.trim()}
            onClick={loadPasted}
          >
            Load into form
          </button>
        </div>
      </Modal>
    </div>
  );
}

// ------------------------------------------------------------- detail view

function AgentDetail({ agent, onEdit }: { agent: AgentRecord; onEdit: () => void }) {
  const skills = useRegistrySkills();
  return (
    <div className="agents-detail">
      <div className="panel-header">
        <div className="panel-title">
          <IconBot size={15} />
          <span>{agent.name}</span>
          <span className="agent-chip model">{agent.id}</span>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button className="btn btn-sm" onClick={onEdit}>
            Edit
          </button>
          {!agent.builtin && (
            <button
              className="btn btn-sm btn-danger"
              onClick={() => {
                getRegistryStore().deleteAgent(agent.id).catch(() => undefined);
              }}
            >
              Delete
            </button>
          )}
        </div>
      </div>
      <div className="agents-detail-body">
        <p className="agents-detail-desc">{agent.description}</p>
        <table className="agents-detail-table">
          <tbody>
            <tr>
              <th>Provider</th>
              <td>{agent.adapterId}</td>
            </tr>
            <tr>
              <th>Model</th>
              <td>{agent.model || "adapter default"}</td>
            </tr>
            <tr>
              <th>Effort</th>
              <td>{agent.effort || "—"}</td>
            </tr>
            <tr>
              <th>Mode</th>
              <td>{agent.mode}</td>
            </tr>
            <tr>
              <th>Timeout</th>
              <td>{agent.timeoutSecs}s</td>
            </tr>
            <tr>
              <th>Tool denylist</th>
              <td>{agent.toolDenylist.length ? agent.toolDenylist.join(", ") : "—"}</td>
            </tr>
            <tr>
              <th>Routing</th>
              <td>{agent.enabled ? "enabled" : "disabled"}</td>
            </tr>
          </tbody>
        </table>
        <h4>Skills</h4>
        {agent.skills.length === 0 && <p className="hint">No skills assigned.</p>}
        {agent.skills.map((id) => {
          const skill = skills.find((s) => s.id === id);
          return (
            <div key={id} className="agent-skill-block">
              <div className="agent-skill-name">
                {skill?.name || id} <span className="hint">({id})</span>
              </div>
              <div className="hint">{skill?.description || "unknown skill"}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ------------------------------------------------------------- editor form

function AgentEditor({
  draft,
  editing,
  saving,
  error,
  warnings,
  onChange,
  onSave,
  onCancel,
  onDelete,
}: {
  draft: Partial<AgentRecord>;
  editing: boolean;
  saving: boolean;
  error: string | null;
  warnings: string[];
  onChange: (draft: Partial<AgentRecord>) => void;
  onSave: () => void;
  onCancel: () => void;
  onDelete: (() => void) | null;
}) {
  const catalog = useRegistryCatalog();
  const skills = useRegistrySkills();
  const provider = catalog.find((p) => p.id === draft.adapterId);
  const isAgy = draft.adapterId === "antigravity-agy";

  const set = (patch: Partial<AgentRecord>) => onChange({ ...draft, ...patch });

  const toggleSkill = (id: string) => {
    const has = (draft.skills || []).includes(id);
    set({
      skills: has
        ? (draft.skills || []).filter((s) => s !== id)
        : [...(draft.skills || []), id],
    });
  };

  const valid =
    draft.id && draft.id.length >= 2 && draft.name && draft.name.trim() && draft.adapterId;

  return (
    <div className="agents-editor-form">
      <div className="panel-header">
        <div className="panel-title">{editing ? `Edit: ${draft.name}` : "New Agent"}</div>
        <div style={{ display: "flex", gap: 8 }}>
          {onDelete && (
            <button className="btn btn-sm btn-danger" onClick={onDelete}>
              Delete
            </button>
          )}
          <button className="btn btn-sm" onClick={onCancel}>
            Cancel
          </button>
          <button className="btn btn-sm btn-primary" disabled={!valid || saving} onClick={onSave}>
            {saving ? "Saving…" : editing ? "Save changes" : "Register agent"}
          </button>
        </div>
      </div>

      {error && <div className="agents-form-error">{error}</div>}
      {warnings.map((warning, i) => (
        <div key={i} className="agents-form-warning">
          {warning}
        </div>
      ))}

      <div className="agents-form-grid">
        <label>
          <span>Agent id (slug)</span>
          <input
            className="input-text"
            value={draft.id || ""}
            disabled={editing}
            placeholder="e.g. sql-reviewer"
            onChange={(e) => set({ id: e.target.value.trim().toLowerCase() })}
          />
        </label>
        <label>
          <span>Display name</span>
          <input
            className="input-text"
            value={draft.name || ""}
            placeholder="SQL Reviewer"
            onChange={(e) => set({ name: e.target.value })}
          />
        </label>
        <label>
          <span>Provider</span>
          <select
            className="input-text"
            value={draft.adapterId || ""}
            onChange={(e) => set({ adapterId: e.target.value, model: null, effort: null })}
          >
            {(catalog.length
              ? catalog.map((p) => p.id)
              : ["claude-code", "antigravity-agy", "mock"]
            ).map((id) => (
              <option key={id} value={id}>
                {id}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>Model</span>
          <input
            className="input-text"
            list="agent-model-options"
            value={draft.model || ""}
            placeholder="adapter default"
            onChange={(e) => set({ model: e.target.value.trim() || null })}
          />
          <datalist id="agent-model-options">
            {(provider?.models || []).map((model) => (
              <option key={model.id} value={model.id}>
                {model.label}
              </option>
            ))}
          </datalist>
        </label>
        {isAgy && (
          <label>
            <span>Effort</span>
            <select
              className="input-text"
              value={draft.effort || ""}
              onChange={(e) =>
                set({ effort: (e.target.value || null) as AgentRecord["effort"] })
              }
            >
              <option value="">provider default</option>
              <option value="low">low</option>
              <option value="medium">medium</option>
              <option value="high">high</option>
            </select>
          </label>
        )}
        <label>
          <span>Mode</span>
          <select
            className="input-text"
            value={draft.mode || "plan"}
            onChange={(e) => set({ mode: e.target.value as AgentRecord["mode"] })}
          >
            <option value="plan">plan (read-only)</option>
            <option value="accept_edits">accept_edits (may write)</option>
          </select>
        </label>
        <label>
          <span>Timeout (seconds)</span>
          <input
            className="input-text"
            type="number"
            min={30}
            max={86400}
            value={draft.timeoutSecs ?? 600}
            onChange={(e) => set({ timeoutSecs: Number(e.target.value) || 600 })}
          />
        </label>
        <label className="agents-form-wide">
          <span>Description (routing signal for the orchestrator)</span>
          <textarea
            className="input-text"
            rows={2}
            maxLength={500}
            value={draft.description || ""}
            placeholder="What is this agent for?"
            onChange={(e) => set({ description: e.target.value })}
          />
        </label>
        <label className="agents-form-wide">
          <span>Tool denylist (comma-separated, e.g. WebFetch,Bash)</span>
          <input
            className="input-text"
            value={(draft.toolDenylist || []).join(",")}
            onChange={(e) =>
              set({
                toolDenylist: e.target.value
                  .split(",")
                  .map((t) => t.trim())
                  .filter(Boolean),
              })
            }
          />
        </label>
      </div>

      <h4>Skills</h4>
      <div className="agents-skill-picker">
        {skills.map((skill) => {
          const active = (draft.skills || []).includes(skill.id);
          return (
            <button
              key={skill.id}
              type="button"
              className={`skill-pick ${active ? "active" : ""}`}
              title={skill.description}
              onClick={() => toggleSkill(skill.id)}
            >
              {active ? <IconCheck size={12} /> : null}
              {skill.name}
              <span className="hint">{skill.id}</span>
            </button>
          );
        })}
        {skills.length === 0 && <p className="hint">Skill list unavailable.</p>}
      </div>
    </div>
  );
}

// ------------------------------------------------------------- chat panel

function ChatPanel({ onProposal }: { onProposal: (proposal: AgentRecord) => void }) {
  const registry = useRegistryState();
  const agents = useRegistryAgents();
  const connected = useRegistryConnected();
  const [chatWith, setChatWith] = useState<string>("agent-creator");
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [chatError, setChatError] = useState<string | null>(null);
  const transcript = useVisibleTranscript(chatWith);
  const threads = useChatThreads(chatWith);
  const activeThread = useActiveThread(chatWith);
  const currentIndex = threads.length - 1;
  // Reading an earlier conversation is read-only: replying there would
  // silently continue the current one instead of what is on screen.
  const viewingPast = activeThread !== undefined && activeThread !== currentIndex;
  // The current conversation is empty right after a reset.
  const isNewThread = !viewingPast && transcript.length === 0 && threads.length > 1;
  const decisions = useAgentDecisions(chatWith);
  const history = useAgentHistory(chatWith);
  const viewingSession = useViewingSession(chatWith);
  const { sessionId, running } = useChatSession(chatWith);
  const store = getRegistryStore();
  const logRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLTextAreaElement | null>(null);

  // Pin to the newest turn unless the reader has scrolled up to read back.
  useEffect(() => {
    const log = logRef.current;
    if (!log) return;
    const nearBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 120;
    if (nearBottom) log.scrollTop = log.scrollHeight;
  }, [transcript.length, running]);

  const agent = agents.find((a) => a.id === chatWith);
  const proposalsForAgent = useMemo(
    () =>
      registry.proposals
        .map((entry, index) => ({ ...entry, index }))
        .filter((entry) => entry.agentId === chatWith)
        .slice(-3),
    [registry.proposals, chatWith],
  );
  const lastInvalid = useMemo(
    () => registry.proposalInvalid.filter((p) => p.agentId === chatWith).at(-1),
    [registry.proposalInvalid, chatWith],
  );
  const skillDrafts = useMemo(
    () =>
      registry.skillProposals
        .map((entry, index) => ({ ...entry, index }))
        .filter((entry) => entry.agentId === chatWith),
    [registry.skillProposals, chatWith],
  );
  const [installing, setInstalling] = useState<string | null>(null);

  const stop = async () => {
    try {
      await store.cancelChat(chatWith);
    } catch (err: any) {
      setChatError(err?.message || String(err));
    }
  };

  const newChat = async () => {
    setChatError(null);
    try {
      await store.newChat(chatWith);
    } catch (err: any) {
      setChatError(err?.message || String(err));
    }
    inputRef.current?.focus();
  };

  const send = async () => {
    const message = input.trim();
    if (!message || busy) return;
    setBusy(true);
    setChatError(null);
    try {
      if (sessionId) {
        // No silent restart. A new session remembers nothing, so respawning
        // here made the agent re-ask its opening questions — and every
        // answer spawned another session. Surface the failure and let the
        // human decide whether to start over with New chat.
        await store.sendChat(sessionId, chatWith, message);
      } else {
        await store.startChat(chatWith, message);
      }
      setInput("");
    } catch (err: any) {
      setChatError(err?.message || String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="agents-chat-inner">
      <div className="panel-header">
        <div className="panel-title">
          <IconAgents size={15} />
          <span>Chat</span>
        </div>
        <div className="agents-chat-actions">
          <span className={`chat-session-dot ${running ? "running" : sessionId ? "idle" : "none"}`} />
          <select
            className="input-text"
            style={{ width: "auto" }}
            value={chatWith}
            onChange={(e) => setChatWith(e.target.value)}
          >
            {agents.map((a) => (
              <option key={a.id} value={a.id}>
                {a.name} ({a.id})
              </option>
            ))}
          </select>
          {running ? (
            <button
              className="btn btn-sm btn-danger"
              onClick={stop}
              title="Kill the provider process for this turn"
            >
              Stop
            </button>
          ) : (
            <button
              className="btn btn-sm"
              onClick={newChat}
              disabled={viewingPast || (!sessionId && transcript.length === 0)}
              title={
                sessionId
                  ? "End the live session and start a fresh conversation"
                  : "Start a fresh conversation"
              }
            >
              New chat
            </button>
          )}
          {threads.length > 0 && (
            <select
              className="input-text agents-thread-picker"
              title="Conversations with this agent"
              value={activeThread ?? currentIndex}
              onChange={(e) => {
                const index = Number(e.target.value);
                store.viewThread(chatWith, index === currentIndex ? null : index);
              }}
            >
              {threads
                .slice()
                .reverse()
                .map((thread) => (
                  <option key={thread.index} value={thread.index}>
                    {thread.index === currentIndex
                      ? "Current"
                      : new Date(thread.startedAt).toLocaleString([], {
                          month: "short",
                          day: "numeric",
                          hour: "2-digit",
                          minute: "2-digit",
                        })}
                    {" · "}
                    {thread.title}
                    {thread.messageCount > 0 ? ` (${thread.messageCount})` : ""}
                  </option>
                ))}
            </select>
          )}
        </div>
      </div>

      {agent && (
        <div className="agents-chat-context hint">
          Talking to <strong>{agent.name}</strong> — {agent.adapterId}
          {agent.model ? ` / ${agent.model}` : ""}.{" "}
          {agent.id === "agent-creator"
            ? "Describe your needs; it interviews you and drafts a proposal you register with one click."
            : "Every reply is a real provider session (billed); usage lands in the journal."}
        </div>
      )}

      {viewingPast && (
        <div className="agents-chat-archived">
          <span>Viewing an earlier conversation — read only.</span>
          <button className="btn btn-sm" onClick={() => store.viewThread(chatWith, null)}>
            Back to current
          </button>
        </div>
      )}

      <div className="agents-chat-log" ref={logRef}>
        {transcript.length === 0 && (
          <div className="agents-empty">
            {isNewThread
              ? `New conversation — ${agent?.name || "the agent"} starts fresh, with no memory of the previous one.`
              : chatWith === "agent-creator"
                ? "Tell the Agent Creator what kind of agent you need."
                : `Ask ${agent?.name || "the agent"} anything.`}
          </div>
        )}
        {transcript.map((message, index) => (
          <ChatEntry key={`${message.sessionId}-${index}`} message={message} agentId={chatWith} />
        ))}
        {running && (
          <div className="chat-msg system chat-msg-working">
            Working… <span className="chat-spinner" aria-hidden="true" /> live provider session
          </div>
        )}
      </div>

      {/* One question at a time: answering the oldest pending decision
          clears it and the next one takes its place, instead of stacking
          every outstanding question into a wall of cards. */}
      {decisions[0] && (
        <DecisionCard
          key={`${decisions[0].sessionId}-${decisions[0].at}`}
          decision={decisions[0]}
          remaining={decisions.length - 1}
          disabled={!connected || busy}
          onAnswer={async (answer) => {
            setBusy(true);
            setChatError(null);
            try {
              await store.answerDecision(decisions[0], answer);
            } catch (err: any) {
              setChatError(err?.message || String(err));
            } finally {
              setBusy(false);
            }
          }}
          onDismiss={() => store.clearDecision(decisions[0])}
        />
      )}

      {lastInvalid && (
        <div className="agents-chat-warning" title={lastInvalid.reason}>
          <IconClose size={12} /> Proposal invalid: {lastInvalid.reason}
        </div>
      )}
      {skillDrafts.length > 0 && (
        <div className="agents-proposals">
          {skillDrafts.map((entry) => {
            const exists = registry.skills.some((skill) => skill.id === entry.draft.id);
            return (
              <div key={`${entry.draft.id}-${entry.index}`} className="agents-proposal skill">
                <div style={{ minWidth: 0 }}>
                  New skill: <strong>{entry.draft.name}</strong>{" "}
                  <span className="hint">({entry.draft.id})</span>
                  <div className="hint" title={entry.draft.description}>
                    {entry.draft.description || "no description"}
                  </div>
                  <details className="agents-skill-body">
                    <summary className="hint">
                      Read the method ({entry.draft.body.length} chars)
                    </summary>
                    <pre>{entry.draft.body}</pre>
                  </details>
                </div>
                <div style={{ display: "flex", gap: 6, flexShrink: 0 }}>
                  <button
                    className="btn btn-sm btn-primary"
                    disabled={exists || installing === entry.draft.id}
                    title={
                      exists
                        ? "A skill with this id already exists"
                        : "Install it so agents can hold this skill"
                    }
                    onClick={async () => {
                      setInstalling(entry.draft.id);
                      setChatError(null);
                      try {
                        await store.installSkill(entry.draft);
                        store.dismissSkillProposal(entry.index);
                      } catch (err: any) {
                        setChatError(err?.message || String(err));
                      } finally {
                        setInstalling(null);
                      }
                    }}
                  >
                    {exists ? "Already installed" : installing === entry.draft.id ? "Installing…" : "Install skill"}
                  </button>
                  <button
                    className="btn btn-sm"
                    onClick={() => store.dismissSkillProposal(entry.index)}
                  >
                    Dismiss
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {proposalsForAgent.length > 0 && (
        <div className="agents-proposals">
          {proposalsForAgent.map((entry) => (
            <div key={`${entry.draft.id}-${entry.index}`} className="agents-proposal">
              <div>
                Draft: <strong>{entry.draft.name}</strong>{" "}
                <span className="hint">
                  ({entry.draft.id} · {entry.draft.adapterId}
                  {entry.draft.model ? ` / ${entry.draft.model}` : ""} · {entry.draft.mode})
                </span>
              </div>
              <div style={{ display: "flex", gap: 6 }}>
                <button
                  className="btn btn-sm btn-primary"
                  onClick={() => {
                    onProposal(entry.draft);
                    store.dismissProposal(entry.index);
                  }}
                >
                  Review &amp; register
                </button>
                <button
                  className="btn btn-sm"
                  onClick={() => store.dismissProposal(entry.index)}
                >
                  Dismiss
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      {chatError && <div className="agents-form-error">{chatError}</div>}

      {history.length > 0 && (
        <div className="agents-history">
          <div className="agents-history-head">
            Past chats
            <span className="hint"> · restored from the journal</span>
          </div>
          <div className="agents-history-rows">
            {history.slice(0, 8).map((session) => {
              const open = session.sessionId === viewingSession;
              const resumable = Boolean(session.providerSessionId);
              return (
                <div
                  key={session.sessionId}
                  className={`agents-history-row${open ? " open" : ""}`}
                >
                  <button
                    className="agents-history-open"
                    title={`${session.turns} turn(s) · ${session.status}${
                      session.error ? ` · ${session.error}` : ""
                    }`}
                    onClick={async () => {
                      setChatError(null);
                      try {
                        await store.openHistorySession(chatWith, session.sessionId);
                      } catch (err: any) {
                        setChatError(err?.message || String(err));
                      }
                    }}
                  >
                    <span className={`chat-session-dot ${session.status}`} />
                    <span className="agents-history-title">
                      {session.title || "(no opening message)"}
                    </span>
                    <span className="hint">
                      {new Date(session.lastAt).toLocaleString()} · {session.turns}t
                    </span>
                  </button>
                  <button
                    className="btn btn-sm"
                    disabled={!resumable || busy}
                    title={
                      resumable
                        ? "Continue this conversation — the agent still remembers it"
                        : "This provider never reported a resumable session id"
                    }
                    onClick={async () => {
                      const message = input.trim();
                      if (!message) {
                        setChatError(
                          "Type the message you want to continue with, then press Continue.",
                        );
                        inputRef.current?.focus();
                        return;
                      }
                      setBusy(true);
                      setChatError(null);
                      try {
                        await store.reopenSession(chatWith, session.sessionId, message);
                        setInput("");
                      } catch (err: any) {
                        setChatError(err?.message || String(err));
                      } finally {
                        setBusy(false);
                      }
                    }}
                  >
                    Continue
                  </button>
                </div>
              );
            })}
          </div>
          {viewingSession && (
            <button
              className="btn btn-sm"
              onClick={() => store.clearViewing(chatWith)}
              title="Back to the live conversation"
            >
              Back to current
            </button>
          )}
        </div>
      )}

      <div className="agents-chat-input">
        <textarea
          ref={inputRef}
          rows={2}
          value={input}
          placeholder={
            !connected
              ? "Daemon disconnected"
              : viewingPast
                ? "Read-only — go back to the current conversation to reply"
                : "Type a message… (Enter to send, Shift+Enter for a newline)"
          }
          disabled={!connected || viewingPast}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              send();
            }
          }}
        />
        {running ? (
          <button className="btn btn-danger" onClick={stop} title="Kill the provider process for this turn">
            Stop
          </button>
        ) : (
          <button
            className="btn btn-primary"
            disabled={!connected || busy || viewingPast || !input.trim()}
            onClick={send}
          >
            {busy ? "…" : "Send"}
          </button>
        )}
      </div>
    </div>
  );
}

/**
 * One transcript entry. Tool calls collapse to a single line — the point of
 * showing them is "what is it doing right now", not a full argument dump.
 */
function ChatEntry({ message, agentId }: { message: ChatMessage; agentId: string }) {
  // No per-session divider: some adapters open a fresh provider session for
  // every single turn, so one conversation spans many of them and a divider
  // between each would fire on every reply.
  if (message.role === "tool") {
    return (
      <div className="chat-msg tool" title={message.text}>
        <span className="chat-tool-name">{message.tool}</span>
        {message.text && <span className="chat-tool-args">{message.text}</span>}
      </div>
    );
  }

  return (
    <div className={`chat-msg ${message.role} ${message.pending ? "pending" : ""}`}>
        <div className="chat-msg-role">
          {message.role === "user" ? "you" : message.role === "agent" ? agentId : "system"}
          <span className="chat-msg-time">
            {new Date(message.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
          </span>
        </div>
      <div className="chat-msg-text">{message.text}</div>
    </div>
  );
}

// ---------------------------------------------------------- decision card

/**
 * A stacked choice list, the shape a terminal agent uses: one option per
 * row, numbered, arrow keys and 1-9 to tick, space to tick, Enter to send —
 * plus a free text box, because the right answer is often none of the
 * offered ones.
 *
 * Every option is a checkbox regardless of what the agent asked for. An
 * agent's `multiSelect` flag says how many answers IT expects, which is not
 * the same as how many the human has to give: "both of these" is often the
 * true answer to a question posed as either/or. The flag survives as a hint
 * on the header so the mismatch is visible, never as a limit.
 */
function DecisionCard({
  decision,
  remaining,
  disabled,
  onAnswer,
  onDismiss,
}: {
  decision: DecisionPrompt;
  /** Questions still queued behind this one. */
  remaining: number;
  disabled: boolean;
  onAnswer: (answer: string) => Promise<void>;
  onDismiss: () => void;
}) {
  const { options, multiSelect } = decision;
  const [cursor, setCursor] = useState(0);
  const [picked, setPicked] = useState<string[]>([]);
  const [custom, setCustom] = useState("");
  const listRef = useRef<HTMLDivElement | null>(null);

  // A new question reuses this card: reset the cursor, the picks and the
  // half-typed custom answer so the previous question's state cannot leak
  // into the next one.
  useEffect(() => {
    setCursor(0);
    setPicked([]);
    setCustom("");
    listRef.current?.focus();
  }, [decision.prompt, decision.at]);

  const toggle = (option: string) => {
    setPicked((prev) =>
      prev.includes(option) ? prev.filter((o) => o !== option) : [...prev, option],
    );
  };

  const submitPicked = () => {
    if (picked.length === 0) return;
    // The options are sent as the agent's own labels, comma-joined — the
    // shape AskUserQuestion's multi-select answers come back in.
    const answer = picked.join(", ");
    setPicked([]);
    void onAnswer(answer);
  };

  const submitCustom = () => {
    const answer = custom.trim();
    if (!answer) return;
    setCustom("");
    void onAnswer(answer);
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      setCursor((c) => (c + (e.key === "ArrowDown" ? 1 : options.length - 1)) % options.length);
      return;
    }
    // Space ticks the box under the cursor; Enter sends whatever is ticked,
    // falling back to the cursor's option so a single-answer reply is still
    // two keys rather than three.
    if (e.key === " ") {
      e.preventDefault();
      toggle(options[cursor]);
      return;
    }
    if (e.key === "Enter") {
      e.preventDefault();
      if (picked.length > 0) submitPicked();
      else void onAnswer(options[cursor]);
      return;
    }
    const digit = Number(e.key);
    if (digit >= 1 && digit <= options.length) {
      e.preventDefault();
      setCursor(digit - 1);
      toggle(options[digit - 1]);
    }
  };

  return (
    <div className="agents-decision">
      <div className="agents-decision-head">
        {decision.tool === "ExitPlanMode" ? "Plan ready for approval" : "The agent needs a decision"}
        <span className="hint">
          {multiSelect ? " · several answers allowed" : " · the agent expects one answer"}
        </span>
        {remaining > 0 && <span className="hint"> · {remaining} more after this</span>}
      </div>
      <div className="agents-decision-prompt">{decision.prompt}</div>

      <div
        className="agents-decision-options"
        ref={listRef}
        tabIndex={0}
        role="group"
        aria-label="Answer options (tick any number)"
        onKeyDown={onKeyDown}
      >
        {options.map((option, i) => {
          const checked = picked.includes(option);
          return (
            <button
              key={option}
              type="button"
              role="checkbox"
              aria-checked={checked}
              className={`decision-option ${i === cursor ? "cursor" : ""} ${
                checked ? "picked" : ""
              }`}
              disabled={disabled}
              onMouseEnter={() => setCursor(i)}
              onClick={() => toggle(option)}
            >
              <span className={`decision-mark check ${checked ? "on" : ""}`} aria-hidden="true">
                {checked && <IconCheck size={10} />}
              </span>
              <span className="decision-option-key">{i + 1}</span>
              <span className="decision-option-label">{option}</span>
            </button>
          );
        })}
      </div>

      <div className="agents-decision-submit">
        <button
          className="btn btn-sm btn-primary"
          disabled={disabled || picked.length === 0}
          onClick={submitPicked}
        >
          Send {picked.length || ""} answer{picked.length === 1 ? "" : "s"}
        </button>
        <span className="hint">1-9 or space ticks · enter sends</span>
      </div>

      <div className="agents-decision-custom">
        <input
          className="input-text"
          value={custom}
          placeholder="…or type your own answer"
          disabled={disabled}
          onChange={(e) => setCustom(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              e.preventDefault();
              submitCustom();
            }
          }}
        />
        <button
          className="btn btn-sm btn-primary"
          disabled={disabled || !custom.trim()}
          onClick={submitCustom}
        >
          Send
        </button>
        <button className="btn btn-sm" onClick={onDismiss} title="Answer in the chat box instead">
          Dismiss
        </button>
      </div>
    </div>
  );
}
