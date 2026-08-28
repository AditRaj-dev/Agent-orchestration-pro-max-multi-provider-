export type WorkflowNode = { id: string; title: string; role: string; state: "idle" | "active" | "blocked" | "done"; x: number; y: number; needs?: string[] };

export const baseNodes: WorkflowNode[] = [
  { id: "intent", title: "Intent", role: "Operator", state: "done", x: 46, y: 147 },
  { id: "spec", title: "Specification", role: "Mastermind", state: "active", x: 237, y: 65, needs: ["intent"] },
  { id: "design", title: "Design", role: "Architect", state: "idle", x: 437, y: 65, needs: ["spec"] },
  { id: "build", title: "Implement", role: "Builder", state: "idle", x: 437, y: 226, needs: ["spec"] },
  { id: "review", title: "Review", role: "Reviewer", state: "idle", x: 647, y: 147, needs: ["design", "build"] },
  { id: "verify", title: "Verify", role: "Verifier", state: "idle", x: 846, y: 147, needs: ["review"] },
];

export const validateDag = (nodes: WorkflowNode[]) => {
  const ids = new Set(nodes.map((node) => node.id));
  if (ids.size !== nodes.length) return "Every node requires a unique identifier.";
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (id: string): boolean => {
    if (visiting.has(id)) return true;
    if (visited.has(id)) return false;
    visiting.add(id);
    const node = nodes.find((item) => item.id === id);
    if ((node?.needs || []).some((need) => !ids.has(need) || visit(need))) return true;
    visiting.delete(id); visited.add(id); return false;
  };
  return nodes.some((node) => visit(node.id)) ? "Dependencies must form an acyclic graph." : undefined;
};
