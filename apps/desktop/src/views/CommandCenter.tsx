// F-11 CommandCenter View (Multi-Agent Command Center UX-02)
import { useEffect, useRef, useState } from "react";
import { IconAlertTriangle, IconBot, IconDashboard } from "../components/Icons";
import { JsonViewer } from "../components/JsonViewer";
import { Modal } from "../components/Modal";
import { StatusBadge } from "../components/StatusBadge";
import { useAgents, useEvents, useRuns, useTasks } from "../store/hooks";
import { getRegistryStore, useRegistryState } from "../store/registry";
import type { AgentSummary, EventWire, ViewType } from "../types";

export interface CommandCenterProps {
  onNavigate: (view: ViewType, params?: { runId?: string; taskId?: string; agentId?: string }) => void;
  selectedRunId: string | null;
  onSelectRun: (runId: string | null) => void;
}

export function CommandCenter({ onNavigate, selectedRunId, onSelectRun }: CommandCenterProps) {
  const allRuns = useRuns();
  const allTasks = useTasks(selectedRunId);
  const allAgents = useAgents(selectedRunId);
  const events = useEvents({ runId: selectedRunId, limit: 300 });
  // Live chat sessions come from the registry store, which tracks the ids
  // the daemon can actually cancel. A supervisor-run agent has no entry
  // here and gets no Stop button — there would be nothing to cancel.
  const registry = useRegistryState();

  const [autoScroll, setAutoScroll] = useState(true);
  const [isHovered, setIsHovered] = useState(false);
  const [inspectEvent, setInspectEvent] = useState<EventWire | null>(null);
  const feedScrollRef = useRef<HTMLDivElement>(null);

  // Auto-scroll feed on new events unless paused or hovered
  useEffect(() => {
    if (autoScroll && !isHovered && feedScrollRef.current) {
      feedScrollRef.current.scrollTop = feedScrollRef.current.scrollHeight;
    }
  }, [events.length, autoScroll, isHovered]);

  // Aggregate metrics
  const activeRunsCount = allRuns.filter((r) => r.status === "running").length;
  const activeAgentsCount = allAgents.filter((a) => a.status === "running" || a.status === "planning").length;
  const activeTasksCount = allTasks.filter((t) => ["ready", "leased", "running", "output_ready"].includes(t.state)).length;
  const totalCost = allAgents.reduce((sum, a) => sum + (a.usage?.costUsd || 0), 0);
  const totalTokens = allAgents.reduce((sum, a) => sum + (a.usage?.tokensEstimate || 0), 0);

  // Blocked / Human Required Tasks (UX-02 <= 2 clicks to identify blocked task and its dependency)
  const blockedTasks = allTasks.filter((t) => t.state === "human_required" || t.state === "blocked");

  // The chip carries a tone, not a colour. Colour pairs (text plus its own
  // wash) live in the stylesheet, where each theme can pick a text shade
  // that stays legible — a saturated accent used as ink reads at ~2.4:1 on
  // a light canvas.
  const getEventChipTone = (eventType: string) => {
    if (eventType.startsWith("task.")) return "blue";
    if (eventType.startsWith("agent.")) return "cyan";
    if (eventType.startsWith("git.")) return "purple";
    if (eventType.startsWith("review.")) return "purple";
    if (eventType.startsWith("approval.") || eventType.startsWith("budget.")) return "amber";
    if (eventType.endsWith(".failed") || eventType.includes("crash")) return "red";
    if (eventType.endsWith(".completed") || eventType.endsWith(".done")) return "green";
    return "neutral";
  };

  const formatPayloadSummary = (event: EventWire): string => {
    if (!event.payload) return "";
    const p = event.payload;
    if (p.message) return String(p.message);
    if (p.tool) return `Tool: ${p.tool}`;
    if (p.commitSha) return `Commit: ${p.commitSha}`;
    if (p.workflowId) return `Workflow: ${p.workflowId}`;
    if (p.costUsd !== undefined || p.tokensEstimate !== undefined) {
      return `Cost: $${Number(p.costUsd || 0).toFixed(4)} | Tokens: ${p.tokensEstimate || 0}`;
    }
    const keys = Object.keys(p);
    if (keys.length > 0) {
      return keys.slice(0, 3).map((k) => `${k}: ${JSON.stringify(p[k])}`).join(" • ");
    }
    return "";
  };

  return (
    <div className="view-body">
      {/* Top Stats Cards */}
      <div className="stats-row">
        <div className="stat-card">
          <span className="stat-label">Active Runs</span>
          <span className="stat-value" style={{ color: "var(--accent-blue)" }}>
            {activeRunsCount} <span style={{ fontSize: "12px", color: "var(--text-muted)", fontWeight: 400 }}>/ {allRuns.length} total</span>
          </span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Active Agents</span>
          <span className="stat-value" style={{ color: "var(--accent-cyan)" }}>
            {activeAgentsCount} <span style={{ fontSize: "12px", color: "var(--text-muted)", fontWeight: 400 }}>/ {allAgents.length}</span>
          </span>
        </div>
        <div className="stat-card">
          <span className="stat-label">In-Flight Tasks</span>
          <span className="stat-value" style={{ color: "var(--accent-amber)" }}>
            {activeTasksCount} <span style={{ fontSize: "12px", color: "var(--text-muted)", fontWeight: 400 }}>/ {allTasks.length}</span>
          </span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Estimated Usage</span>
          <span className="stat-value" style={{ color: "var(--accent-green)" }}>
            ${totalCost.toFixed(4)}
          </span>
          <span style={{ fontSize: "10px", color: "var(--text-muted)" }}>
            ~{(totalTokens / 1000).toFixed(1)}k tokens
          </span>
        </div>
      </div>

      {/* Blocked / Action Required Quick Alert (UX-02 requirement: <= 2 clicks) */}
      {blockedTasks.length > 0 && (
        <div
          style={{
            backgroundColor: "rgba(245, 165, 36, 0.1)",
            border: "1px solid rgba(245, 165, 36, 0.4)",
            borderRadius: "var(--radius-lg)",
            padding: "12px 16px",
            display: "flex",
            flexDirection: "column",
            gap: "8px",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "8px", color: "var(--accent-amber)", fontWeight: 600 }}>
            <IconAlertTriangle size={16} />
            <span>Attention Required: {blockedTasks.length} Blocked Task{blockedTasks.length > 1 ? "s" : ""}</span>
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
            {blockedTasks.map((task) => (
              <div
                key={task.taskId}
                style={{
                  backgroundColor: "var(--bg-card)",
                  border: "1px solid var(--border-default)",
                  borderRadius: "var(--radius-md)",
                  padding: "6px 12px",
                  display: "flex",
                  alignItems: "center",
                  gap: "10px",
                  fontSize: "12px",
                }}
              >
                <span style={{ fontFamily: "var(--font-mono)", fontWeight: 600 }}>
                  Task {task.nodeId || task.taskId.slice(0, 8)}
                </span>
                <StatusBadge status={task.state} size="sm" />
                {task.dependsOn && task.dependsOn.length > 0 && (
                  <span style={{ color: "var(--text-muted)", fontSize: "11px" }}>
                    depends on: {task.dependsOn.join(", ")}
                  </span>
                )}
                <button
                  className="btn-primary"
                  style={{ padding: "3px 8px", fontSize: "11px" }}
                  onClick={() => onNavigate("runs_graph", { runId: task.runId, taskId: task.taskId })}
                >
                  View in Graph
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Active Runs Strip */}
      <div className="panel">
        <div className="panel-header">
          <div className="panel-title">
            <IconDashboard size={16} />
            <span>Run Pipeline History</span>
          </div>
          {selectedRunId && (
            <button
              className="btn-secondary"
              style={{ padding: "2px 8px", fontSize: "11px" }}
              onClick={() => onSelectRun(null)}
            >
              Clear Filter
            </button>
          )}
        </div>
        <div className="runs-strip">
          {allRuns.length === 0 ? (
            <div style={{ color: "var(--text-muted)", fontSize: "12px", padding: "8px 0" }}>
              No runs recorded in journal yet. Connect daemon or seed demo events.
            </div>
          ) : (
            allRuns.map((run) => {
              const isSelected = run.runId === selectedRunId;
              const { total, done, failed } = run.taskCounts;
              const progressPct = total > 0 ? Math.round((done / total) * 100) : 0;

              return (
                <div
                  key={run.runId}
                  className={`run-chip ${isSelected ? "active" : ""}`}
                  onClick={() => onSelectRun(isSelected ? null : run.runId)}
                >
                  <div className="run-chip-top">
                    <span className="run-chip-id">{run.runId.slice(0, 10)}</span>
                    <StatusBadge status={run.status} size="sm" />
                  </div>
                  <div style={{ fontSize: "11px", color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>
                    {run.workflowId ? `workflow: ${run.workflowId}` : "adhoc run"}
                  </div>
                  {/* Progress bar */}
                  <div style={{ width: "100%", height: "4px", backgroundColor: "var(--bg-input)", borderRadius: "2px", overflow: "hidden" }}>
                    <div
                      style={{
                        width: `${progressPct}%`,
                        height: "100%",
                        backgroundColor: failed > 0 ? "var(--status-failed)" : "var(--status-complete)",
                      }}
                    />
                  </div>
                  <div className="run-chip-details">
                    <span>
                      {done}/{total} done {failed > 0 && `(${failed} failed)`}
                    </span>
                    <span>{run.eventCount} events</span>
                  </div>
                </div>
              );
            })
          )}
        </div>
      </div>

      {/* Multi-Agent Cards Grid */}
      <div className="panel">
        <div className="panel-header">
          <div className="panel-title">
            <IconBot size={16} />
            <span>Agent Instances {selectedRunId && `(filtered by run)`}</span>
          </div>
          <span style={{ fontSize: "11px", color: "var(--text-secondary)" }}>
            {allAgents.length} agent{allAgents.length === 1 ? "" : "s"}
          </span>
        </div>

        {allAgents.length === 0 ? (
          <div style={{ color: "var(--text-muted)", fontSize: "12px", padding: "16px 0", textAlign: "center" }}>
            No active agents. Agents appear here as they are spawned or leased by the daemon.
          </div>
        ) : (
          <div className="agent-grid">
            {allAgents.map((agent: AgentSummary) => {
              const normStatus = (agent.status || "idle").toLowerCase();
              return (
                <div
                  key={agent.agentId}
                  className={`agent-card ${normStatus}`}
                >
                  <div className="agent-card-header">
                    <span className="agent-name">
                      <IconBot size={15} />
                      {agent.agentId}
                    </span>
                    <StatusBadge status={agent.status} />
                  </div>

                  <div style={{ display: "flex", gap: "6px", alignItems: "center" }}>
                    <span className="agent-model-pill">
                      {agent.provider || "provider"}: {agent.model || "default"}
                    </span>
                  </div>

                  <div className="agent-task-info">
                    <span className="agent-task-label">Current / Last Task</span>
                    <span className="agent-task-val">
                      {agent.taskId ? agent.taskId.slice(0, 16) : "No active task assigned"}
                    </span>
                    <div style={{ display: "flex", justifyContent: "space-between", marginTop: "2px", fontSize: "10px", color: "var(--text-muted)" }}>
                      <span>last: {agent.lastEventType || "idle"}</span>
                      <span>{agent.eventCount} events</span>
                    </div>
                  </div>

                  <div className="agent-metrics">
                    <span>
                      Usage: <strong>${(agent.usage?.costUsd || 0).toFixed(4)}</strong>
                    </span>
                    <span>
                      ~{((agent.usage?.tokensEstimate || 0) / 1000).toFixed(1)}k tokens
                    </span>
                  </div>

                  <div className="agent-footer">
                    <span style={{ fontSize: "10px", color: "var(--text-muted)" }}>
                      {agent.lastEventAt ? new Date(agent.lastEventAt).toLocaleTimeString() : ""}
                    </span>
                    <div className="agent-footer-actions">
                      {registry.sessionIds[agent.agentId] && (
                        <button
                          className={`btn btn-sm ${registry.running[agent.agentId] ? "btn-danger" : ""}`}
                          title={
                            registry.running[agent.agentId]
                              ? "Kill the provider process for this turn"
                              : "End the live provider session"
                          }
                          onClick={() => {
                            getRegistryStore().cancelChat(agent.agentId).catch(() => undefined);
                          }}
                        >
                          Stop
                        </button>
                      )}
                      <button
                        className="btn-open-session"
                        onClick={() => onNavigate("session", { agentId: agent.agentId, taskId: agent.taskId || undefined })}
                      >
                        Open Session →
                      </button>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Live Activity Feed */}
      <div className="activity-feed-container">
        <div className="activity-feed-header">
          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <span style={{ fontWeight: 600, fontSize: "13px" }}>Live Journal Activity Feed</span>
            <span style={{ fontSize: "11px", color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>
              ({events.length} events loaded)
            </span>
          </div>

          <div className="feed-controls">
            <label style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "11px", color: "var(--text-secondary)", cursor: "pointer" }}>
              <input
                type="checkbox"
                checked={autoScroll}
                onChange={(e) => setAutoScroll(e.target.checked)}
              />
              Auto-scroll
            </label>
          </div>
        </div>

        <div
          className="feed-scroll"
          ref={feedScrollRef}
          onMouseEnter={() => setIsHovered(true)}
          onMouseLeave={() => setIsHovered(false)}
        >
          {events.length === 0 ? (
            <div style={{ padding: "20px", color: "var(--text-muted)", textAlign: "center" }}>
              Waiting for events on WebSocket stream...
            </div>
          ) : (
            events.map((event) => {
              const timeStr = event.occurredAt ? new Date(event.occurredAt).toISOString().slice(11, 23) : "";
              const chipTone = getEventChipTone(event.eventType);

              return (
                <div
                  key={`${event.seq}-${event.id}`}
                  className="event-row"
                  onClick={() => setInspectEvent(event)}
                  title="Click to view event payload"
                >
                  <span className="event-seq">#{event.seq}</span>
                  <span className="event-time">{timeStr}</span>
                  <span className={`event-type-chip tone-${chipTone}`}>
                    {event.eventType}
                  </span>
                  {event.agentId && (
                    <span style={{ color: "var(--text-secondary)", fontSize: "11px" }}>
                      [{event.agentId}]
                    </span>
                  )}
                  <span className="event-payload-summary">
                    {formatPayloadSummary(event)}
                  </span>
                </div>
              );
            })
          )}
        </div>
      </div>

      {/* Inspect Event Payload Modal */}
      <Modal
        isOpen={inspectEvent !== null}
        onClose={() => setInspectEvent(null)}
        title={`Event #${inspectEvent?.seq}: ${inspectEvent?.eventType}`}
      >
        {inspectEvent && (
          <div style={{ display: "flex", flexDirection: "column", gap: "14px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px", fontSize: "12px" }}>
              <div>
                <strong style={{ color: "var(--text-secondary)" }}>Occurred At:</strong>{" "}
                <span style={{ fontFamily: "var(--font-mono)" }}>{inspectEvent.occurredAt}</span>
              </div>
              <div>
                <strong style={{ color: "var(--text-secondary)" }}>Event ID:</strong>{" "}
                <span style={{ fontFamily: "var(--font-mono)" }}>{inspectEvent.id}</span>
              </div>
              <div>
                <strong style={{ color: "var(--text-secondary)" }}>Run ID:</strong>{" "}
                <span style={{ fontFamily: "var(--font-mono)" }}>{inspectEvent.runId || "—"}</span>
              </div>
              <div>
                <strong style={{ color: "var(--text-secondary)" }}>Task ID:</strong>{" "}
                <span style={{ fontFamily: "var(--font-mono)" }}>{inspectEvent.taskId || "—"}</span>
              </div>
              <div>
                <strong style={{ color: "var(--text-secondary)" }}>Agent ID:</strong>{" "}
                <span style={{ fontFamily: "var(--font-mono)" }}>{inspectEvent.agentId || "—"}</span>
              </div>
              <div>
                <strong style={{ color: "var(--text-secondary)" }}>Schema Version:</strong>{" "}
                <span style={{ fontFamily: "var(--font-mono)" }}>{inspectEvent.schemaVersion}</span>
              </div>
            </div>

            {inspectEvent.payloadRef && (
              <div style={{ fontSize: "11px", color: "var(--text-code)", fontFamily: "var(--font-mono)" }}>
                Payload Ref: {inspectEvent.payloadRef} (Hash: {inspectEvent.payloadHash || "none"})
              </div>
            )}

            <div>
              <div style={{ marginBottom: "6px", fontSize: "12px", fontWeight: 600 }}>Payload:</div>
              <JsonViewer data={inspectEvent.payload} initialExpanded={true} />
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
}
