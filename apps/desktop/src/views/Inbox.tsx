// F-11 Notification & Intervention Center View (UX-06-lite)
import { useState } from "react";
import { IconCheck, IconInbox } from "../components/Icons";
import { JsonViewer } from "../components/JsonViewer";
import { useInboxEvents } from "../store/hooks";
import type { EventWire, ViewType } from "../types";

export interface InboxProps {
  onNavigate: (view: ViewType, params?: { runId?: string; taskId?: string; agentId?: string }) => void;
}

const STORAGE_KEY_QUIET_MODE = "agentos_inbox_quiet_mode";
const STORAGE_KEY_DISMISSED_EVENTS = "agentos_inbox_dismissed";

export function Inbox({ onNavigate }: InboxProps) {
  const rawInboxEvents = useInboxEvents();

  const [quietMode, setQuietMode] = useState<boolean>(() => {
    if (typeof window !== "undefined" && window.localStorage) {
      return window.localStorage.getItem(STORAGE_KEY_QUIET_MODE) === "true";
    }
    return false;
  });

  const [dismissedIds, setDismissedIds] = useState<Set<string>>(() => {
    if (typeof window !== "undefined" && window.localStorage) {
      try {
        const saved = window.localStorage.getItem(STORAGE_KEY_DISMISSED_EVENTS);
        if (saved) return new Set(JSON.parse(saved));
      } catch {
        // ignore
      }
    }
    return new Set();
  });

  const [categoryFilter, setCategoryFilter] = useState<string>("all");

  const toggleQuietMode = () => {
    const next = !quietMode;
    setQuietMode(next);
    if (typeof window !== "undefined" && window.localStorage) {
      window.localStorage.setItem(STORAGE_KEY_QUIET_MODE, String(next));
    }
  };

  const handleDismiss = (id: string) => {
    const next = new Set(dismissedIds);
    next.add(id);
    setDismissedIds(next);
    if (typeof window !== "undefined" && window.localStorage) {
      window.localStorage.setItem(STORAGE_KEY_DISMISSED_EVENTS, JSON.stringify(Array.from(next)));
    }
  };

  const handleClearAllDismissed = () => {
    setDismissedIds(new Set());
    if (typeof window !== "undefined" && window.localStorage) {
      window.localStorage.removeItem(STORAGE_KEY_DISMISSED_EVENTS);
    }
  };

  // Filter events
  const activeEvents = rawInboxEvents
    .filter((e) => !dismissedIds.has(e.id))
    .filter((e) => {
      if (quietMode && (e.eventType === "agent.rate_limit" || e.eventType === "handoff.rejected")) {
        return false;
      }
      if (categoryFilter === "all") return true;
      if (categoryFilter === "approvals") return e.eventType.startsWith("approval.");
      if (categoryFilter === "budget") return e.eventType.startsWith("budget.");
      if (categoryFilter === "conflicts") return e.eventType.includes("conflict");
      if (categoryFilter === "failures") return e.eventType.includes("fail") || e.eventType.includes("crashed");
      return true;
    });

  const getSeverity = (eventType: string): { label: string; color: string; bg: string } => {
    if (eventType.startsWith("approval.") || eventType.startsWith("budget.")) {
      return { label: "ACTION REQUIRED", color: "var(--accent-amber)", bg: "rgba(245, 165, 36, 0.15)" };
    }
    if (eventType.includes("conflict") || eventType.includes("fail") || eventType.includes("crash")) {
      return { label: "CRITICAL", color: "var(--status-failed)", bg: "rgba(229, 72, 77, 0.15)" };
    }
    return { label: "NOTICE", color: "var(--accent-blue)", bg: "rgba(79, 140, 255, 0.15)" };
  };

  return (
    <div className="view-body" style={{ maxWidth: "1000px", margin: "0 auto", width: "100%" }}>
      {/* Inbox Header */}
      <div className="panel" style={{ padding: "12px 16px" }}>
        <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "12px" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <IconInbox size={18} />
            <span style={{ fontWeight: 600, fontSize: "14px" }}>Notification & Intervention Center</span>
            <span
              style={{
                backgroundColor: activeEvents.length > 0 ? "var(--accent-red)" : "var(--border-default)",
                color: "#ffffff",
                fontSize: "11px",
                fontWeight: 700,
                padding: "2px 8px",
                borderRadius: "12px",
              }}
            >
              {activeEvents.length}
            </span>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
            {/* Category Filter */}
            <select
              className="run-select"
              value={categoryFilter}
              onChange={(e) => setCategoryFilter(e.target.value)}
            >
              <option value="all">All Alerts</option>
              <option value="approvals">Human Approvals</option>
              <option value="budget">Budget Limits</option>
              <option value="conflicts">Ownership Conflicts</option>
              <option value="failures">Task/Agent Failures</option>
            </select>

            {/* Quiet Mode Toggle */}
            <button
              className="btn-secondary"
              onClick={toggleQuietMode}
              style={{
                borderColor: quietMode ? "var(--accent-purple)" : undefined,
                color: quietMode ? "var(--accent-purple)" : undefined,
                display: "flex",
                alignItems: "center",
                gap: "6px",
              }}
            >
              <span>{quietMode ? "🔕 Quiet Mode: ON" : "🔔 Quiet Mode: OFF"}</span>
            </button>

            {dismissedIds.size > 0 && (
              <button
                className="btn-secondary"
                style={{ fontSize: "11px", padding: "4px 8px" }}
                onClick={handleClearAllDismissed}
              >
                Restore Dismissed ({dismissedIds.size})
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Alerts Feed */}
      <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
        {activeEvents.length === 0 ? (
          <div
            className="panel"
            style={{
              padding: "40px",
              textAlign: "center",
              color: "var(--text-muted)",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              gap: "8px",
            }}
          >
            <IconCheck size={32} />
            <span style={{ fontSize: "14px", fontWeight: 600, color: "var(--text-primary)" }}>
              Inbox Clear — No Interventions Required
            </span>
            <span style={{ fontSize: "12px" }}>
              Human approval requests, budget threshold warnings, conflicts, and failures will appear here.
            </span>
          </div>
        ) : (
          activeEvents.map((ev: EventWire) => {
            const sev = getSeverity(ev.eventType);

            return (
              <div
                key={ev.id}
                className="panel"
                style={{
                  borderLeft: `4px solid ${sev.color}`,
                  display: "flex",
                  flexDirection: "column",
                  gap: "10px",
                }}
              >
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                    <span
                      style={{
                        backgroundColor: sev.bg,
                        color: sev.color,
                        fontSize: "10px",
                        fontWeight: 700,
                        padding: "2px 8px",
                        borderRadius: "var(--radius-sm)",
                        letterSpacing: "0.5px",
                      }}
                    >
                      {sev.label}
                    </span>
                    <span style={{ fontFamily: "var(--font-mono)", fontWeight: 700, fontSize: "13px" }}>
                      {ev.eventType}
                    </span>
                  </div>

                  <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                    <span style={{ fontSize: "11px", color: "var(--text-muted)" }}>
                      {new Date(ev.occurredAt).toLocaleString()}
                    </span>
                    <button
                      className="btn-secondary"
                      style={{ fontSize: "11px", padding: "2px 8px" }}
                      onClick={() => handleDismiss(ev.id)}
                    >
                      Dismiss
                    </button>
                  </div>
                </div>

                {/* Event Context & Deep Links */}
                <div style={{ display: "flex", flexWrap: "wrap", gap: "14px", fontSize: "12px", color: "var(--text-secondary)" }}>
                  {ev.runId && (
                    <div>
                      <span>Run ID:</span>{" "}
                      <strong style={{ fontFamily: "var(--font-mono)", color: "var(--text-primary)" }}>
                        {ev.runId.slice(0, 12)}
                      </strong>
                    </div>
                  )}
                  {ev.taskId && (
                    <div>
                      <span>Task ID:</span>{" "}
                      <strong style={{ fontFamily: "var(--font-mono)", color: "var(--text-code)" }}>
                        {ev.taskId.slice(0, 12)}
                      </strong>
                    </div>
                  )}
                  {ev.agentId && (
                    <div>
                      <span>Agent:</span>{" "}
                      <strong style={{ fontFamily: "var(--font-mono)", color: "var(--text-primary)" }}>
                        {ev.agentId}
                      </strong>
                    </div>
                  )}
                </div>

                {/* Payload content */}
                {ev.payload && (
                  <div style={{ marginTop: "4px" }}>
                    <JsonViewer data={ev.payload} initialExpanded={true} />
                  </div>
                )}

                {/* Action Deep Links */}
                <div style={{ display: "flex", gap: "8px", marginTop: "4px" }}>
                  {ev.taskId && (
                    <button
                      className="btn-primary"
                      style={{ fontSize: "11px", padding: "4px 12px" }}
                      onClick={() => onNavigate("runs_graph", { runId: ev.runId || undefined, taskId: ev.taskId || undefined })}
                    >
                      View in Graph →
                    </button>
                  )}
                  {ev.agentId && (
                    <button
                      className="btn-secondary"
                      style={{ fontSize: "11px", padding: "4px 12px" }}
                      onClick={() => onNavigate("session", { agentId: ev.agentId || undefined, taskId: ev.taskId || undefined })}
                    >
                      Open Agent Session →
                    </button>
                  )}
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
