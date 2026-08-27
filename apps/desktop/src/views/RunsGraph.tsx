// F-11 Workflow Graph View (UX-04)
import { useMemo, useState } from "react";
import { IconGraph } from "../components/Icons";
import { StatusBadge } from "../components/StatusBadge";
import { useEvents, useRuns, useTasks } from "../store/hooks";
import type { TaskSummary, ViewType } from "../types";

export interface RunsGraphProps {
  onNavigate: (view: ViewType, params?: { runId?: string; taskId?: string; agentId?: string }) => void;
  selectedRunId: string | null;
  onSelectRun: (runId: string | null) => void;
  initialSelectedTaskId?: string | null;
}

interface NodeLayout {
  task: TaskSummary;
  x: number;
  y: number;
  width: number;
  height: number;
}

export function RunsGraph({
  onNavigate,
  selectedRunId,
  onSelectRun,
  initialSelectedTaskId,
}: RunsGraphProps) {
  const allRuns = useRuns();
  const effectiveRunId = selectedRunId || (allRuns.length > 0 ? allRuns[0].runId : null);
  const tasks = useTasks(effectiveRunId);
  const allEvents = useEvents({ runId: effectiveRunId });

  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(initialSelectedTaskId || null);

  // Topological / Layered DAG Layout Computation
  const { nodeLayouts, edgePaths, canvasWidth, canvasHeight } = useMemo(() => {
    if (tasks.length === 0) {
      return { nodeLayouts: new Map<string, NodeLayout>(), edgePaths: [], canvasWidth: 800, canvasHeight: 500 };
    }

    const nodeWidth = 200;
    const nodeHeight = 85;
    const layerSpacingX = 260;
    const nodeSpacingY = 120;
    const paddingX = 60;
    const paddingY = 60;

    // Map by taskId and by nodeId for dependency resolution
    const taskMap = new Map<string, TaskSummary>();
    const nodeToTaskId = new Map<string, string>();
    for (const t of tasks) {
      taskMap.set(t.taskId, t);
      if (t.nodeId) {
        nodeToTaskId.set(t.nodeId, t.taskId);
      }
    }

    // Compute depth/layer for each task
    const depthMap = new Map<string, number>();

    const getDepth = (taskId: string, visited: Set<string>): number => {
      if (depthMap.has(taskId)) return depthMap.get(taskId)!;
      if (visited.has(taskId)) return 0; // cycle guard
      visited.add(taskId);

      const t = taskMap.get(taskId);
      if (!t || !t.dependsOn || t.dependsOn.length === 0) {
        depthMap.set(taskId, 0);
        return 0;
      }

      let maxParentDepth = -1;
      for (const depId of t.dependsOn) {
        const parentTaskId = taskMap.has(depId) ? depId : nodeToTaskId.get(depId);
        if (parentTaskId && taskMap.has(parentTaskId)) {
          const pDepth = getDepth(parentTaskId, new Set(visited));
          if (pDepth > maxParentDepth) maxParentDepth = pDepth;
        }
      }

      const depth = maxParentDepth + 1;
      depthMap.set(taskId, depth);
      return depth;
    };

    for (const t of tasks) {
      getDepth(t.taskId, new Set());
    }

    // Group tasks by layer
    const layers = new Map<number, TaskSummary[]>();
    for (const t of tasks) {
      const d = depthMap.get(t.taskId) || 0;
      if (!layers.has(d)) layers.set(d, []);
      layers.get(d)!.push(t);
    }

    const layouts = new Map<string, NodeLayout>();
    let maxLayerIndex = 0;
    let maxNodesInLayer = 0;

    layers.forEach((layerTasks, layerIdx) => {
      if (layerIdx > maxLayerIndex) maxLayerIndex = layerIdx;
      if (layerTasks.length > maxNodesInLayer) maxNodesInLayer = layerTasks.length;

      layerTasks.forEach((task, indexInLayer) => {
        const x = paddingX + layerIdx * layerSpacingX;
        const y = paddingY + indexInLayer * nodeSpacingY;
        layouts.set(task.taskId, {
          task,
          x,
          y,
          width: nodeWidth,
          height: nodeHeight,
        });
      });
    });

    // Build edge curves
    const edges: Array<{ id: string; d: string; from: string; to: string }> = [];

    for (const targetTask of tasks) {
      const targetLayout = layouts.get(targetTask.taskId);
      if (!targetLayout) continue;

      for (const dep of targetTask.dependsOn) {
        const sourceTaskId = taskMap.has(dep) ? dep : nodeToTaskId.get(dep);
        if (!sourceTaskId) continue;
        const sourceLayout = layouts.get(sourceTaskId);
        if (!sourceLayout) continue;

        const startX = sourceLayout.x + sourceLayout.width;
        const startY = sourceLayout.y + sourceLayout.height / 2;
        const endX = targetLayout.x;
        const endY = targetLayout.y + targetLayout.height / 2;

        const dx = (endX - startX) / 2;
        const d = `M ${startX} ${startY} C ${startX + dx} ${startY}, ${endX - dx} ${endY}, ${endX} ${endY}`;
        edges.push({
          id: `${sourceTaskId}->${targetTask.taskId}`,
          d,
          from: sourceTaskId,
          to: targetTask.taskId,
        });
      }
    }

    const w = Math.max(900, paddingX * 2 + (maxLayerIndex + 1) * layerSpacingX);
    const h = Math.max(550, paddingY * 2 + maxNodesInLayer * nodeSpacingY);

    return { nodeLayouts: layouts, edgePaths: edges, canvasWidth: w, canvasHeight: h };
  }, [tasks]);

  const selectedTask = selectedTaskId ? tasks.find((t) => t.taskId === selectedTaskId) : null;
  const taskEvents = selectedTaskId
    ? allEvents.filter((e) => e.taskId === selectedTaskId)
    : [];

  const getBorderColor = (state: string) => {
    switch (state) {
      case "running":
      case "leased":
        return "var(--status-running)";
      case "done":
      case "committed":
        return "var(--status-complete)";
      case "failed":
        return "var(--status-failed)";
      case "human_required":
      case "waiting":
        return "var(--status-waiting)";
      case "review_pending":
      case "approved":
        return "var(--status-reviewing)";
      default:
        return "var(--border-default)";
    }
  };

  return (
    <div className="view-body" style={{ position: "relative" }}>
      {/* Top controls: Run selector */}
      <div className="panel" style={{ padding: "10px 16px" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
            <IconGraph size={18} />
            <span style={{ fontWeight: 600, fontSize: "14px" }}>Workflow Task Dependency Graph</span>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <span style={{ fontSize: "12px", color: "var(--text-secondary)" }}>Select Run:</span>
            <select
              className="run-select"
              value={effectiveRunId || ""}
              onChange={(e) => {
                onSelectRun(e.target.value);
                setSelectedTaskId(null);
              }}
            >
              {allRuns.map((r) => (
                <option key={r.runId} value={r.runId}>
                  {r.runId.slice(0, 12)} ({r.status}) — {r.workflowId || "workflow"}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {/* DAG Graph Area + Detail Drawer Split */}
      <div style={{ display: "flex", gap: "16px", flex: 1, minHeight: 0 }}>
        {/* Graph Canvas */}
        <div
          className="dag-container"
          style={{
            flex: selectedTask ? "1 1 65%" : "1 1 100%",
            overflow: "auto",
            position: "relative",
            minHeight: "550px",
          }}
        >
          {tasks.length === 0 ? (
            <div style={{ padding: "40px", textAlign: "center", color: "var(--text-muted)" }}>
              No tasks found for this run. Graph will render as workflow tasks are registered.
            </div>
          ) : (
            <div
              style={{
                position: "relative",
                width: `${canvasWidth}px`,
                height: `${canvasHeight}px`,
                minWidth: "100%",
                minHeight: "100%",
              }}
            >
              {/* SVG Edges Layer */}
              <svg
                style={{
                  position: "absolute",
                  inset: 0,
                  width: "100%",
                  height: "100%",
                  pointerEvents: "none",
                }}
              >
                <defs>
                  <marker
                    id="arrowhead"
                    markerWidth="8"
                    markerHeight="6"
                    refX="7"
                    refY="3"
                    orient="auto"
                  >
                    <polygon points="0 0, 8 3, 0 6" fill="var(--text-muted)" />
                  </marker>
                </defs>

                {edgePaths.map((edge) => (
                  <path
                    key={edge.id}
                    d={edge.d}
                    fill="none"
                    stroke="var(--border-default)"
                    strokeWidth="2"
                    markerEnd="url(#arrowhead)"
                  />
                ))}
              </svg>

              {/* Task Nodes */}
              {Array.from(nodeLayouts.values()).map(({ task, x, y, width, height }) => {
                const isSelected = selectedTaskId === task.taskId;
                const borderColor = getBorderColor(task.state);

                return (
                  <div
                    key={task.taskId}
                    className={`dag-node ${isSelected ? "selected" : ""}`}
                    style={{
                      left: `${x}px`,
                      top: `${y}px`,
                      width: `${width}px`,
                      height: `${height}px`,
                      borderLeft: `4px solid ${borderColor}`,
                      borderColor: isSelected ? "var(--accent-purple)" : undefined,
                    }}
                    onClick={() => setSelectedTaskId(task.taskId)}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                      <span className="dag-node-id">
                        {task.nodeId || task.taskId.slice(0, 10)}
                      </span>
                      <StatusBadge status={task.state} size="sm" />
                    </div>

                    <div style={{ fontSize: "11px", color: "var(--text-secondary)", marginTop: "4px" }}>
                      Agent: {task.agentId ? task.agentId : <em style={{ color: "var(--text-muted)" }}>unassigned</em>}
                    </div>

                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: "4px", fontSize: "10px", color: "var(--text-muted)" }}>
                      <span>{task.attempts > 0 ? `retry #${task.attempts}` : "attempt 0"}</span>
                      {task.commitSha && (
                        <span style={{ color: "var(--text-code)", fontFamily: "var(--font-mono)" }}>
                          sha: {task.commitSha.slice(0, 7)}
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Task Detail Drawer (Right side) */}
        {selectedTask && (
          <div
            className="panel"
            style={{
              flex: "0 0 380px",
              overflowY: "auto",
              maxHeight: "750px",
            }}
          >
            <div className="panel-header">
              <div>
                <span style={{ fontSize: "11px", color: "var(--text-muted)", textTransform: "uppercase" }}>
                  Task Node Contract
                </span>
                <h4 style={{ fontFamily: "var(--font-mono)", fontSize: "14px", fontWeight: 700, marginTop: "2px" }}>
                  {selectedTask.nodeId || selectedTask.taskId}
                </h4>
              </div>
              <button
                className="btn-secondary"
                style={{ padding: "2px 8px", fontSize: "11px" }}
                onClick={() => setSelectedTaskId(null)}
              >
                Close
              </button>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "12px", fontSize: "12px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <span style={{ color: "var(--text-secondary)" }}>State:</span>
                <StatusBadge status={selectedTask.state} />
              </div>

              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-secondary)" }}>Task ID:</span>
                <span style={{ fontFamily: "var(--font-mono)", color: "var(--text-code)" }}>
                  {selectedTask.taskId}
                </span>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-secondary)" }}>Run ID:</span>
                <span style={{ fontFamily: "var(--font-mono)" }}>{selectedTask.runId}</span>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-secondary)" }}>Assigned Agent:</span>
                <span style={{ fontFamily: "var(--font-mono)" }}>
                  {selectedTask.agentId || "None"}
                </span>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--text-secondary)" }}>Attempts / Crashes:</span>
                <span>{selectedTask.attempts}</span>
              </div>

              {selectedTask.commitSha && (
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "var(--text-secondary)" }}>Committed SHA:</span>
                  <span style={{ fontFamily: "var(--font-mono)", color: "var(--accent-green)" }}>
                    {selectedTask.commitSha}
                  </span>
                </div>
              )}

              {/* Dependencies section with <= 2 click jumping */}
              <div>
                <div style={{ color: "var(--text-secondary)", marginBottom: "4px" }}>Depends On:</div>
                {selectedTask.dependsOn.length === 0 ? (
                  <span style={{ color: "var(--text-muted)", fontSize: "11px" }}>No upstream dependencies (Root)</span>
                ) : (
                  <div style={{ display: "flex", flexWrap: "wrap", gap: "6px" }}>
                    {selectedTask.dependsOn.map((dep) => (
                      <button
                        key={dep}
                        className="btn-secondary"
                        style={{ padding: "2px 8px", fontSize: "11px", fontFamily: "var(--font-mono)" }}
                        onClick={() => {
                          const target = tasks.find((t) => t.taskId === dep || t.nodeId === dep);
                          if (target) setSelectedTaskId(target.taskId);
                        }}
                      >
                        → {dep}
                      </button>
                    ))}
                  </div>
                )}
              </div>

              {/* Action buttons */}
              <div style={{ display: "flex", gap: "8px", marginTop: "8px" }}>
                {selectedTask.agentId && (
                  <button
                    className="btn-primary"
                    style={{ flex: 1 }}
                    onClick={() =>
                      onNavigate("session", {
                        agentId: selectedTask.agentId || undefined,
                        taskId: selectedTask.taskId,
                      })
                    }
                  >
                    Open Session
                  </button>
                )}
              </div>

              {/* Task Event History */}
              <div style={{ marginTop: "12px", borderTop: "1px solid var(--border-subtle)", paddingTop: "10px" }}>
                <div style={{ fontWeight: 600, fontSize: "12px", marginBottom: "8px" }}>
                  Task Event History ({taskEvents.length})
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: "6px", maxHeight: "250px", overflowY: "auto" }}>
                  {taskEvents.map((ev) => (
                    <div
                      key={ev.seq}
                      style={{
                        backgroundColor: "var(--bg-input)",
                        border: "1px solid var(--border-subtle)",
                        borderRadius: "var(--radius-sm)",
                        padding: "6px 8px",
                        fontSize: "11px",
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", color: "var(--text-muted)" }}>
                        <span>#{ev.seq}</span>
                        <span>{new Date(ev.occurredAt).toLocaleTimeString()}</span>
                      </div>
                      <div style={{ color: "var(--text-primary)", fontWeight: 600, marginTop: "2px" }}>
                        {ev.eventType}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
