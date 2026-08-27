// F-11 Interactive Agent Session View (UX-03)
import { useMemo, useState } from "react";
import { IconBot, IconSession } from "../components/Icons";
import { JsonViewer } from "../components/JsonViewer";
import { StatusBadge } from "../components/StatusBadge";
import { useAgents, useEvents, useTasks } from "../store/hooks";
import type { EventWire, ViewType } from "../types";

export interface SessionProps {
  onNavigate: (view: ViewType, params?: { runId?: string; taskId?: string; agentId?: string }) => void;
  selectedAgentId?: string | null;
  selectedTaskId?: string | null;
}

export function Session({
  onNavigate: _onNavigate,
  selectedAgentId: initialAgentId,
  selectedTaskId: initialTaskId,
}: SessionProps) {
  const agents = useAgents();
  const tasks = useTasks();

  const [activeAgentId, setActiveAgentId] = useState<string | null>(
    initialAgentId || (agents.length > 0 ? agents[0].agentId : null)
  );
  const [activeTaskId, setActiveTaskId] = useState<string | null>(initialTaskId || null);
  const [eventFilter, setEventFilter] = useState<string>("all");
  const [selectedEventForInspect, setSelectedEventForInspect] = useState<EventWire | null>(null);

  const activeAgent = useMemo(
    () => agents.find((a) => a.agentId === activeAgentId),
    [agents, activeAgentId]
  );

  const rawEvents = useEvents({
    agentId: activeAgentId || undefined,
    taskId: activeTaskId || undefined,
  });

  const filteredEvents = useMemo(() => {
    if (eventFilter === "all") return rawEvents;
    if (eventFilter === "tool_use") {
      return rawEvents.filter((e) => e.eventType.includes("tool"));
    }
    if (eventFilter === "usage") {
      return rawEvents.filter((e) => e.eventType.includes("usage"));
    }
    if (eventFilter === "errors") {
      return rawEvents.filter(
        (e) => e.eventType.includes("fail") || e.eventType.includes("crash") || e.eventType.includes("denied")
      );
    }
    return rawEvents;
  }, [rawEvents, eventFilter]);

  return (
    <div className="view-body">
      {/* Session Header / Selector Strip */}
      <div className="panel" style={{ padding: "12px 16px" }}>
        <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "12px" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <IconSession size={18} />
            <span style={{ fontWeight: 600, fontSize: "14px" }}>Agent Session Timeline & Terminal</span>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: "14px" }}>
            {/* Agent Select */}
            <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
              <span style={{ fontSize: "11px", color: "var(--text-secondary)" }}>Agent:</span>
              <select
                className="run-select"
                value={activeAgentId || ""}
                onChange={(e) => setActiveAgentId(e.target.value || null)}
              >
                <option value="">All Agents</option>
                {agents.map((a) => (
                  <option key={a.agentId} value={a.agentId}>
                    {a.agentId} ({a.status})
                  </option>
                ))}
              </select>
            </div>

            {/* Task Select */}
            <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
              <span style={{ fontSize: "11px", color: "var(--text-secondary)" }}>Task:</span>
              <select
                className="run-select"
                value={activeTaskId || ""}
                onChange={(e) => setActiveTaskId(e.target.value || null)}
              >
                <option value="">All Tasks</option>
                {tasks.map((t) => (
                  <option key={t.taskId} value={t.taskId}>
                    {t.nodeId || t.taskId.slice(0, 10)} ({t.state})
                  </option>
                ))}
              </select>
            </div>

            {/* Event Filter */}
            <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
              <span style={{ fontSize: "11px", color: "var(--text-secondary)" }}>Type:</span>
              <select
                className="run-select"
                value={eventFilter}
                onChange={(e) => setEventFilter(e.target.value)}
              >
                <option value="all">All Events</option>
                <option value="tool_use">Tool Calls Only</option>
                <option value="usage">Usage Telemetry</option>
                <option value="errors">Errors / Warnings</option>
              </select>
            </div>
          </div>
        </div>
      </div>

      {/* Main split: Terminal stream + Agent inspector */}
      <div style={{ display: "flex", gap: "16px", flex: 1, minHeight: 0 }}>
        {/* Terminal / Conversation Window */}
        <div style={{ flex: "1 1 65%", display: "flex", flexDirection: "column", gap: "10px" }}>
          <div className="terminal-window" style={{ flex: 1 }}>
            <div className="terminal-header">
              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                <span style={{ color: "var(--text-muted)" }}>● ● ●</span>
                <span>session_stream://{activeAgentId || "global"}</span>
              </div>
              <span style={{ fontSize: "11px", color: "var(--text-muted)" }}>
                {filteredEvents.length} events logged
              </span>
            </div>

            <div className="terminal-content">
              {filteredEvents.length === 0 ? (
                <div style={{ color: "var(--text-muted)", padding: "20px", textAlign: "center" }}>
                  No session events recorded for this selection.
                </div>
              ) : (
                filteredEvents.map((ev, index) => {
                  const isTool = ev.eventType === "agent.tool_use";
                  const isError = ev.eventType.includes("fail") || ev.eventType.includes("crash");
                  const isUsage = ev.eventType === "usage.updated";

                  return (
                    <div
                      key={`${ev.seq}-${index}`}
                      className="terminal-line"
                      onClick={() => setSelectedEventForInspect(ev)}
                      style={{
                        cursor: "pointer",
                        padding: "2px 4px",
                        borderRadius: "2px",
                        backgroundColor:
                          selectedEventForInspect?.id === ev.id ? "rgba(124, 92, 255, 0.2)" : "transparent",
                      }}
                    >
                      <span className="terminal-line-num">
                        {String(index + 1).padStart(3, "0")}
                      </span>
                      <div className="terminal-line-text" style={{ flex: 1 }}>
                        <span style={{ color: "var(--text-muted)", marginRight: "8px" }}>
                          [{new Date(ev.occurredAt).toLocaleTimeString()}]
                        </span>
                        <span
                          style={{
                            color: isError
                              ? "var(--accent-red)"
                              : isTool
                              ? "var(--accent-cyan)"
                              : isUsage
                              ? "var(--accent-green)"
                              : "var(--accent-blue)",
                            fontWeight: 600,
                            marginRight: "8px",
                          }}
                        >
                          {ev.eventType}
                        </span>

                        {/* Formatted Turn / Payload rendering */}
                        {ev.payload?.tool && (
                          <span style={{ color: "var(--text-code)" }}>
                            call <strong>{ev.payload.tool}</strong>
                            {ev.payload.args ? ` (${JSON.stringify(ev.payload.args)})` : ""}
                          </span>
                        )}

                        {ev.payload?.message && (
                          <span style={{ color: "var(--text-primary)" }}>
                            {ev.payload.message}
                          </span>
                        )}

                        {isUsage && (
                          <span style={{ color: "var(--accent-green)" }}>
                            Cost: +${Number(ev.payload?.costUsd || 0).toFixed(4)} | Tokens: +{ev.payload?.tokensEstimate || 0}
                          </span>
                        )}

                        {!ev.payload?.tool && !ev.payload?.message && !isUsage && (
                          <span style={{ color: "var(--text-secondary)" }}>
                            {JSON.stringify(ev.payload || {})}
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>

          {/* Seam Notice regarding write-path steering */}
          <div className="seam-notice">
            <span style={{ fontSize: "14px" }}>ℹ️</span>
            <div>
              <div className="seam-notice-title">Interactive Intervention Seam (F-11 §6)</div>
              <div>
                User steering instructions and terminal input belong to the policy/supervisor surface. In v1 this is a read-only stream projection from journal events.
              </div>
            </div>
          </div>
        </div>

        {/* Right Inspector & Usage Panel */}
        <div style={{ flex: "0 0 350px", display: "flex", flexDirection: "column", gap: "14px" }}>
          {/* Agent Metadata & Telemetry */}
          <div className="panel">
            <div className="panel-header">
              <div className="panel-title">
                <IconBot size={16} />
                <span>Agent Status</span>
              </div>
              {activeAgent && <StatusBadge status={activeAgent.status} />}
            </div>

            {activeAgent ? (
              <div style={{ display: "flex", flexDirection: "column", gap: "10px", fontSize: "12px" }}>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "var(--text-secondary)" }}>Agent ID:</span>
                  <span style={{ fontFamily: "var(--font-mono)", fontWeight: 600 }}>{activeAgent.agentId}</span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "var(--text-secondary)" }}>Provider/Model:</span>
                  <span style={{ fontFamily: "var(--font-mono)" }}>
                    {activeAgent.provider || "default"}:{activeAgent.model || "default"}
                  </span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "var(--text-secondary)" }}>Assigned Task:</span>
                  <span style={{ fontFamily: "var(--font-mono)", color: "var(--text-code)" }}>
                    {activeAgent.taskId || "None"}
                  </span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "var(--text-secondary)" }}>Event Count:</span>
                  <span>{activeAgent.eventCount}</span>
                </div>

                <div style={{ borderTop: "1px solid var(--border-subtle)", paddingTop: "8px", marginTop: "4px" }}>
                  <div style={{ fontWeight: 600, marginBottom: "6px" }}>Resource Usage Rollup</div>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span style={{ color: "var(--text-secondary)" }}>Total Cost:</span>
                    <strong style={{ color: "var(--accent-green)" }}>
                      ${(activeAgent.usage?.costUsd || 0).toFixed(4)}
                    </strong>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: "2px" }}>
                    <span style={{ color: "var(--text-secondary)" }}>Tokens Estimated:</span>
                    <strong style={{ fontFamily: "var(--font-mono)" }}>
                      {activeAgent.usage?.tokensEstimate || 0}
                    </strong>
                  </div>
                </div>
              </div>
            ) : (
              <div style={{ color: "var(--text-muted)", fontSize: "12px" }}>
                Select an agent above to view metadata and usage.
              </div>
            )}
          </div>

          {/* Selected Event Payload Inspector */}
          <div className="panel" style={{ flex: 1, overflowY: "auto" }}>
            <div className="panel-header">
              <span className="panel-title">Payload Inspector</span>
              {selectedEventForInspect && (
                <span style={{ fontSize: "10px", color: "var(--text-muted)" }}>
                  Seq #{selectedEventForInspect.seq}
                </span>
              )}
            </div>

            {selectedEventForInspect ? (
              <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                <div style={{ fontSize: "11px", color: "var(--text-secondary)" }}>
                  <strong>Type:</strong> {selectedEventForInspect.eventType}
                </div>
                <JsonViewer data={selectedEventForInspect.payload} initialExpanded={true} />
              </div>
            ) : (
              <div style={{ color: "var(--text-muted)", fontSize: "12px", textAlign: "center", padding: "20px 0" }}>
                Click any line in the session terminal stream to inspect its raw payload.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
