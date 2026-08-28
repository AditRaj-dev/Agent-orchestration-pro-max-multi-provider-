import { describe, expect, it } from "vitest";
import { baseNodes, workflowNodesFromTasks } from "../lib/workflow";

describe("workspace workflow projection", () => {
  it("keeps the illustrative topology available before a run", () => {
    expect(baseNodes).toHaveLength(6);
  });

  it("projects daemon tasks into a positioned, stateful dependency graph", () => {
    const nodes = workflowNodesFromTasks([
      { taskId: "task-spec", runId: "run-current", nodeId: "spec", state: "done", agentId: "spec-writer", dependsOn: [] },
      { taskId: "task-ui", runId: "run-current", nodeId: "frontend", state: "running", agentId: "nextjs-engineer", dependsOn: ["spec"] },
      { taskId: "task-api", runId: "run-current", nodeId: "backend", state: "queued", agentId: "node-engineer", dependsOn: ["spec"] },
      { taskId: "task-old", runId: "run-old", nodeId: "obsolete", state: "failed", agentId: "old-agent", dependsOn: [] },
    ]);

    expect(nodes.map((node) => node.id)).toEqual(["spec", "frontend", "backend"]);
    expect(nodes.find((node) => node.id === "spec")?.state).toBe("done");
    expect(nodes.find((node) => node.id === "frontend")?.state).toBe("active");
    expect(nodes.find((node) => node.id === "backend")?.needs).toEqual(["spec"]);
    expect(nodes.find((node) => node.id === "frontend")?.x).toBeGreaterThan(nodes.find((node) => node.id === "spec")?.x || 0);
  });
});
