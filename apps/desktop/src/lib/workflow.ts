export type WorkflowNode = { id: string; title: string; role: string; state: "idle" | "active" | "blocked" | "done"; x: number; y: number; needs?: string[] };

export const baseNodes: WorkflowNode[] = [
  { id: "intent", title: "Intent", role: "Operator", state: "done", x: 46, y: 147 },
  { id: "spec", title: "Specification", role: "Mastermind", state: "active", x: 237, y: 65, needs: ["intent"] },
  { id: "design", title: "Design", role: "Architect", state: "idle", x: 437, y: 65, needs: ["spec"] },
  { id: "build", title: "Implement", role: "Builder", state: "idle", x: 437, y: 226, needs: ["spec"] },
  { id: "review", title: "Review", role: "Reviewer", state: "idle", x: 647, y: 147, needs: ["design", "build"] },
  { id: "verify", title: "Verify", role: "Verifier", state: "idle", x: 846, y: 147, needs: ["review"] },
];

export const workflowNodesFromTasks = (tasks: Array<Record<string, unknown>>): WorkflowNode[] => {
  const text = (value: unknown) => typeof value === "string" ? value : "";
  const list = (value: unknown) => Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
  const latestRun = tasks.map((task) => text(task.runId)).find(Boolean);
  const current = latestRun ? tasks.filter((task) => text(task.runId) === latestRun) : tasks;
  const raw = current.flatMap((task) => {
    const id = text(task.nodeId) || text(task.taskId) || text(task.id);
    if (!id) return [];
    const state = text(task.state) || text(task.status);
    const visualState: WorkflowNode["state"] = state === "done" ? "done" : ["leased", "running", "reviewing", "committing"].includes(state) ? "active" : ["failed", "cancelled", "human_required", "blocked"].includes(state) ? "blocked" : "idle";
    return [{
      id,
      title: id.split(/[-_]/).filter(Boolean).map((part) => part[0]?.toUpperCase() + part.slice(1)).join(" ") || id,
      role: text(task.agentId) || "Awaiting assignment",
      state: visualState,
      needs: list(task.dependsOn).length ? list(task.dependsOn) : list(task.dependencies),
      x: 0,
      y: 0,
    } satisfies WorkflowNode];
  });
  const unique = [...new Map(raw.map((node) => [node.id, node])).values()];
  const ids = new Set(unique.map((node) => node.id));
  const waves = new Map<string, number>();
  const waveOf = (id: string, seen = new Set<string>()): number => {
    if (waves.has(id)) return waves.get(id)!;
    if (seen.has(id)) return 0;
    seen.add(id);
    const node = unique.find((candidate) => candidate.id === id);
    const dependencies = (node?.needs || []).filter((dependency) => ids.has(dependency));
    const wave = dependencies.length ? Math.max(...dependencies.map((dependency) => waveOf(dependency, new Set(seen)))) + 1 : 0;
    waves.set(id, wave);
    return wave;
  };
  unique.forEach((node) => waveOf(node.id));
  const maxWave = Math.max(1, ...waves.values());
  const groups = new Map<number, WorkflowNode[]>();
  unique.forEach((node) => { const wave = waves.get(node.id) || 0; groups.set(wave, [...(groups.get(wave) || []), node]); });
  return unique.map((node) => {
    const wave = waves.get(node.id) || 0;
    const peers = groups.get(wave) || [node];
    const row = peers.findIndex((peer) => peer.id === node.id);
    return { ...node, x: 25 + wave * (770 / maxWave), y: peers.length === 1 ? 158 : 24 + row * (300 / (peers.length - 1)) };
  });
};
