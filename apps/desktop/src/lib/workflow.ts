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

export const addWorkflowNode = (nodes: WorkflowNode[], title: string): { nodes: WorkflowNode[]; id?: string } => {
  const cleanTitle = title.trim();
  if (!cleanTitle) return { nodes };
  const root = cleanTitle.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "") || "task";
  let id = root;
  let suffix = 2;
  while (nodes.some((node) => node.id === id)) id = `${root}-${suffix++}`;
  const column = nodes.length % 4;
  const row = Math.floor(nodes.length / 4) % 2;
  return {
    id,
    nodes: [...nodes, { id, title: cleanTitle, role: "Unassigned", state: "idle", x: 55 + column * 215, y: 62 + row * 180, needs: [] }],
  };
};

export const removeWorkflowNode = (nodes: WorkflowNode[], id: string) => nodes
  .filter((node) => node.id !== id)
  .map((node) => ({ ...node, needs: (node.needs || []).filter((dependency) => dependency !== id) }));

export const toggleWorkflowDependency = (nodes: WorkflowNode[], nodeId: string, dependencyId: string) => nodes.map((node) => {
  if (node.id !== nodeId || nodeId === dependencyId) return node;
  const dependencies = node.needs || [];
  return { ...node, needs: dependencies.includes(dependencyId) ? dependencies.filter((id) => id !== dependencyId) : [...dependencies, dependencyId] };
});
