import { describe, expect, it } from "vitest";
import { addWorkflowNode, baseNodes, removeWorkflowNode, toggleWorkflowDependency, validateDag } from "../lib/workflow";

describe("workflow validation", () => {
  it("accepts the default execution topology", () => {
    expect(validateDag(baseNodes)).toBeUndefined();
  });

  it("rejects cycles and unknown dependency references", () => {
    expect(validateDag([{ ...baseNodes[0], id: "a", needs: ["b"] }, { ...baseNodes[1], id: "b", needs: ["a"] }])).toMatch(/acyclic/);
    expect(validateDag([{ ...baseNodes[0], id: "a", needs: ["missing"] }])).toMatch(/acyclic/);
  });

  it("adds uniquely identified visual stages", () => {
    const first = addWorkflowNode(baseNodes, "API integration");
    const second = addWorkflowNode(first.nodes, "API integration");
    expect(first.id).toBe("api-integration");
    expect(second.id).toBe("api-integration-2");
    expect(second.nodes).toHaveLength(baseNodes.length + 2);
  });

  it("toggles visual connections and prunes them when a stage is deleted", () => {
    const connected = toggleWorkflowDependency(baseNodes, "verify", "design");
    expect(connected.find((node) => node.id === "verify")?.needs).toContain("design");
    const removed = removeWorkflowNode(connected, "design");
    expect(removed.some((node) => node.id === "design")).toBe(false);
    expect(removed.every((node) => !(node.needs || []).includes("design"))).toBe(true);
  });
});
