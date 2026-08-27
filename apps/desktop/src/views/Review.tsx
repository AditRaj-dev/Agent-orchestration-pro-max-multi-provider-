// F-11 Git Diff & Review Workspace (UX-05)
import { useMemo, useState } from "react";
import { IconGit } from "../components/Icons";
import { JsonViewer } from "../components/JsonViewer";
import { useEvents, useTasks } from "../store/hooks";
import type { ViewType } from "../types";

export interface ReviewProps {
  onNavigate: (view: ViewType, params?: { runId?: string; taskId?: string; agentId?: string }) => void;
  selectedRunId: string | null;
}

export function Review({ onNavigate: _onNavigate, selectedRunId }: ReviewProps) {
  const events = useEvents({ runId: selectedRunId });
  const tasks = useTasks(selectedRunId);

  // Filter for Git & Review events
  const reviewEvents = useMemo(() => {
    return events.filter(
      (e) =>
        e.eventType.startsWith("git.") ||
        e.eventType.startsWith("review.") ||
        (e.payload && (e.payload.commitSha || e.payload.filesChanged || e.payload.files))
    );
  }, [events]);

  const [selectedEventId, setSelectedEventId] = useState<string | null>(
    reviewEvents.length > 0 ? reviewEvents[0].id : null
  );

  const selectedEvent = useMemo(
    () => reviewEvents.find((e) => e.id === selectedEventId) || reviewEvents[0] || null,
    [reviewEvents, selectedEventId]
  );

  const relatedTask = useMemo(() => {
    if (!selectedEvent || !selectedEvent.taskId) return null;
    return tasks.find((t) => t.taskId === selectedEvent.taskId);
  }, [selectedEvent, tasks]);

  // Extract changed files list from payload
  const changedFiles: string[] = useMemo(() => {
    if (!selectedEvent || !selectedEvent.payload) return [];
    const p = selectedEvent.payload;
    if (Array.isArray(p.filesChanged)) return p.filesChanged;
    if (Array.isArray(p.files)) return p.files;
    if (Array.isArray(p.changedFiles)) return p.changedFiles;
    if (p.file) return [p.file];
    return [];
  }, [selectedEvent]);

  return (
    <div className="view-body">
      {/* Review Header */}
      <div className="panel" style={{ padding: "12px 16px" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <IconGit size={18} />
            <span style={{ fontWeight: 600, fontSize: "14px" }}>Diff & Review Workspace</span>
          </div>
          <span style={{ fontSize: "11px", color: "var(--text-secondary)" }}>
            {reviewEvents.length} git/review event{reviewEvents.length === 1 ? "" : "s"} logged
          </span>
        </div>
      </div>

      {/* Main Review Layout */}
      <div style={{ display: "flex", gap: "16px", flex: 1, minHeight: 0 }}>
        {/* Left: Git & Review Events List */}
        <div className="panel" style={{ flex: "0 0 340px", overflowY: "auto" }}>
          <div className="panel-header">
            <span className="panel-title">Commit & Review Feed</span>
          </div>

          {reviewEvents.length === 0 ? (
            <div style={{ padding: "20px 0", color: "var(--text-muted)", fontSize: "12px", textAlign: "center" }}>
              No git or review events recorded yet.
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
              {reviewEvents.map((ev) => {
                const isSelected = (selectedEvent?.id === ev.id);
                const isCommitted = ev.eventType === "git.committed";
                const isApproved = ev.eventType === "review.approved";
                const sha = ev.payload?.commitSha || ev.payload?.sha;

                return (
                  <div
                    key={ev.id}
                    onClick={() => setSelectedEventId(ev.id)}
                    style={{
                      backgroundColor: isSelected ? "var(--bg-card-hover)" : "var(--bg-card)",
                      border: `1px solid ${isSelected ? "var(--accent-purple)" : "var(--border-subtle)"}`,
                      borderRadius: "var(--radius-md)",
                      padding: "10px 12px",
                      cursor: "pointer",
                      display: "flex",
                      flexDirection: "column",
                      gap: "4px",
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <span
                        style={{
                          fontWeight: 600,
                          fontSize: "12px",
                          color: isCommitted
                            ? "var(--accent-green)"
                            : isApproved
                            ? "var(--status-reviewing)"
                            : "var(--accent-blue)",
                        }}
                      >
                        {ev.eventType}
                      </span>
                      <span style={{ fontSize: "10px", color: "var(--text-muted)" }}>
                        #{ev.seq}
                      </span>
                    </div>

                    {sha && (
                      <div style={{ fontFamily: "var(--font-mono)", fontSize: "11px", color: "var(--text-code)" }}>
                        sha: {String(sha).slice(0, 10)}
                      </div>
                    )}

                    <div style={{ display: "flex", justifyContent: "space-between", fontSize: "10px", color: "var(--text-secondary)", marginTop: "2px" }}>
                      <span>Agent: {ev.agentId || "daemon"}</span>
                      <span>{new Date(ev.occurredAt).toLocaleTimeString()}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Right: Selected Commit / Diff Details */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: "14px", overflowY: "auto" }}>
          {selectedEvent ? (
            <>
              {/* Event Metadata Card */}
              <div className="panel">
                <div className="panel-header">
                  <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                    <span style={{ fontWeight: 700, fontSize: "15px" }}>{selectedEvent.eventType}</span>
                    <span style={{ fontSize: "11px", color: "var(--text-muted)", fontFamily: "var(--font-mono)" }}>
                      (Event ID: {selectedEvent.id})
                    </span>
                  </div>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", fontSize: "12px" }}>
                  <div>
                    <span style={{ color: "var(--text-secondary)" }}>Attributed Agent:</span>{" "}
                    <strong>{selectedEvent.agentId || "orchestrator"}</strong>
                  </div>
                  <div>
                    <span style={{ color: "var(--text-secondary)" }}>Associated Task:</span>{" "}
                    <strong style={{ fontFamily: "var(--font-mono)" }}>
                      {relatedTask ? (relatedTask.nodeId || relatedTask.taskId) : selectedEvent.taskId || "—"}
                    </strong>
                  </div>
                  <div>
                    <span style={{ color: "var(--text-secondary)" }}>Commit SHA:</span>{" "}
                    <span style={{ fontFamily: "var(--font-mono)", color: "var(--accent-green)", fontWeight: 600 }}>
                      {selectedEvent.payload?.commitSha || selectedEvent.payload?.sha || "None"}
                    </span>
                  </div>
                  <div>
                    <span style={{ color: "var(--text-secondary)" }}>Timestamp:</span>{" "}
                    <span>{selectedEvent.occurredAt}</span>
                  </div>
                </div>
              </div>

              {/* Changed Files Set */}
              <div className="panel">
                <div className="panel-header">
                  <span className="panel-title">Files Changed in Candidate Set ({changedFiles.length})</span>
                </div>

                {changedFiles.length === 0 ? (
                  <div style={{ color: "var(--text-muted)", fontSize: "12px" }}>
                    No explicit file lists attached to this event payload.
                  </div>
                ) : (
                  <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
                    {changedFiles.map((file, idx) => (
                      <div
                        key={idx}
                        style={{
                          backgroundColor: "var(--bg-input)",
                          padding: "6px 10px",
                          borderRadius: "var(--radius-sm)",
                          border: "1px solid var(--border-subtle)",
                          fontFamily: "var(--font-mono)",
                          fontSize: "12px",
                          color: "var(--text-code)",
                          display: "flex",
                          alignItems: "center",
                          gap: "8px",
                        }}
                      >
                        <span style={{ color: "var(--accent-green)" }}>M</span>
                        <span>{file}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* git.diff Seam Notice & Disabled Full Diff Affordance */}
              <div className="seam-notice">
                <span style={{ fontSize: "16px" }}>🔒</span>
                <div style={{ flex: 1 }}>
                  <div className="seam-notice-title">
                    Daemon git.diff Seam (docs/F-11-desktop.md §1, §6)
                  </div>
                  <div style={{ fontSize: "12px", lineHeight: "1.5" }}>
                    Full syntax-highlighted git diff plumbing (`git.diff` RPC) returns <code>not_supported</code> in v1. UX-05 displays verified event-carried attribution and changed-file manifests above.
                  </div>
                  <div style={{ marginTop: "8px" }}>
                    <button
                      className="btn-secondary"
                      disabled
                      style={{ opacity: 0.5, cursor: "not-allowed", fontSize: "11px" }}
                      title="Requires agentos-git integration in daemon"
                    >
                      View Full Unified Diff (v1 seam)
                    </button>
                  </div>
                </div>
              </div>

              {/* Raw Payload Inspection */}
              <div className="panel">
                <span className="panel-title" style={{ marginBottom: "8px" }}>Event Payload</span>
                <JsonViewer data={selectedEvent.payload} initialExpanded={true} />
              </div>
            </>
          ) : (
            <div style={{ padding: "40px", color: "var(--text-muted)", textAlign: "center" }}>
              Select a git or review event on the left to inspect attribution and file sets.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
