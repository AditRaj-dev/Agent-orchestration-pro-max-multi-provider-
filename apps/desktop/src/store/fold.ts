// F-11 Client-Side Projection Fold (docs/F-11-desktop.md §3.3)
import type {
  AgentSummary,
  EventWire,
  RunStatus,
  RunSummary,
  TaskCounts,
  TaskState,
  TaskSummary,
} from "../types";

export interface NormalizedState {
  runs: Map<string, RunSummary>;
  tasks: Map<string, TaskSummary>;
  agents: Map<string, AgentSummary>;
}

export function createEmptyNormalizedState(): NormalizedState {
  return {
    runs: new Map(),
    tasks: new Map(),
    agents: new Map(),
  };
}

export function isTaskActive(state: TaskState): boolean {
  return [
    "ready",
    "leased",
    "running",
    "output_ready",
    "review_pending",
    "approved",
    "git_queued",
    "human_required",
  ].includes(state);
}

export function isTaskDone(state: TaskState): boolean {
  return state === "done" || state === "committed";
}

export function isTaskFailed(state: TaskState): boolean {
  return state === "failed";
}

export function computeTaskCounts(runId: string, tasks: Map<string, TaskSummary>): TaskCounts {
  let total = 0;
  let done = 0;
  let failed = 0;
  let active = 0;

  for (const task of tasks.values()) {
    if (task.runId === runId) {
      total++;
      if (isTaskDone(task.state)) {
        done++;
      } else if (isTaskFailed(task.state)) {
        failed++;
      } else if (isTaskActive(task.state)) {
        active++;
      }
    }
  }

  return { total, done, failed, active };
}

/**
 * Folds a single event into the normalized state according to docs/F-11-desktop.md §3.3.
 * Total function: unknown event types leave state unchanged and only update timestamps/counts.
 */
export function foldEvent(state: NormalizedState, event: EventWire): void {
  const { eventType, seq, occurredAt, runId, taskId, agentId, payload } = event;

  // 1. Run Fold Handling
  if (eventType === "run.created") {
    const rId = runId || payload?.runId || event.id;
    if (rId) {
      const existing = state.runs.get(rId);
      if (!existing) {
        state.runs.set(rId, {
          runId: rId,
          status: "running",
          workflowId: payload?.workflowId || null,
          taskCounts: computeTaskCounts(rId, state.tasks),
          startedAt: occurredAt,
          endedAt: null,
          firstSeq: seq,
          lastSeq: seq,
          eventCount: 1,
          budgetExceeded: false,
        });
      } else {
        existing.lastSeq = Math.max(existing.lastSeq, seq);
        existing.eventCount++;
      }
    }
  } else if (runId && state.runs.has(runId)) {
    const run = state.runs.get(runId)!;
    run.lastSeq = Math.max(run.lastSeq, seq);
    run.eventCount++;

    if (eventType === "workflow.started") {
      if (payload?.workflowId) {
        run.workflowId = payload.workflowId;
      }
      // Populate dependsOn if workflow nodes graph is provided
      if (Array.isArray(payload?.nodes)) {
        for (const node of payload.nodes) {
          const nId = node.id || node.taskId || node.nodeId;
          const deps = Array.isArray(node.dependsOn) ? node.dependsOn : [];
          if (nId) {
            // Find task by taskId or nodeId
            for (const task of state.tasks.values()) {
              if (task.runId === runId && (task.taskId === nId || task.nodeId === nId)) {
                task.dependsOn = deps;
              }
            }
          }
        }
      }
    } else if (eventType === "run.completed" || eventType === "run.failed") {
      // Run status derivation: any task failed -> failed
      let hasFailedTask = false;
      for (const task of state.tasks.values()) {
        if (task.runId === runId && task.state === "failed") {
          hasFailedTask = true;
          break;
        }
      }
      const newStatus: RunStatus = hasFailedTask || eventType === "run.failed" ? "failed" : "completed";
      run.status = newStatus;
      run.endedAt = occurredAt;

      // Terminal run transitions active agents to complete (or idle)
      for (const agent of state.agents.values()) {
        if (agent.runId === runId && agent.status !== "failed") {
          agent.status = "complete";
        }
      }
    } else if (eventType === "budget.exceeded") {
      run.budgetExceeded = true;
    }
  }

  // 2. Task Fold Handling
  if (eventType === "task.created") {
    const tId = taskId || payload?.taskId || event.id;
    const rId = runId || payload?.runId || "";
    if (tId) {
      let task = state.tasks.get(tId);
      if (!task) {
        task = {
          taskId: tId,
          runId: rId,
          nodeId: payload?.nodeId || null,
          state: "created",
          attempts: 0,
          agentId: agentId || payload?.agentId || null,
          dependsOn: Array.isArray(payload?.dependsOn) ? payload.dependsOn : [],
          lastEventType: eventType,
          lastEventAt: occurredAt,
          commitSha: null,
          budgetExceeded: false,
        };
        state.tasks.set(tId, task);
      } else {
        task.lastEventType = eventType;
        task.lastEventAt = occurredAt;
      }

      if (rId && state.runs.has(rId)) {
        state.runs.get(rId)!.taskCounts = computeTaskCounts(rId, state.tasks);
      }
    }
  } else if (taskId && state.tasks.has(taskId)) {
    const task = state.tasks.get(taskId)!;
    task.lastEventType = eventType;
    task.lastEventAt = occurredAt;

    if (agentId) {
      task.agentId = agentId;
    }

    // Task State Transitions
    switch (eventType) {
      case "task.ready":
        task.state = "ready";
        break;
      case "agent.leased":
        task.state = "leased";
        if (agentId) task.agentId = agentId;
        break;
      case "task.running":
        task.state = "running";
        if (agentId) task.agentId = agentId;
        break;
      case "task.output_ready":
        task.state = "output_ready";
        break;
      case "review.requested":
        task.state = "review_pending";
        break;
      case "review.approved":
        task.state = "approved";
        break;
      case "git.queued":
        task.state = "git_queued";
        break;
      case "git.committed":
        task.state = "committed";
        if (payload?.commitSha || payload?.sha) {
          task.commitSha = payload.commitSha || payload.sha;
        }
        break;
      case "task.done":
        task.state = "done";
        break;
      case "task.failed":
        task.state = "failed";
        break;
      case "approval.required":
        if (payload?.blocking === true || payload?.blocking === undefined) {
          task.state = "human_required";
        }
        break;
      case "agent.crashed":
        task.attempts++;
        break;
      case "budget.exceeded":
        task.budgetExceeded = true;
        break;
    }

    // Update run's task counts when task state changes
    if (task.runId && state.runs.has(task.runId)) {
      state.runs.get(task.runId)!.taskCounts = computeTaskCounts(task.runId, state.tasks);
    }
  }

  // 3. Agent Fold Handling
  const effectiveAgentId = agentId || (payload?.agentId as string | undefined);
  if (effectiveAgentId) {
    let agent = state.agents.get(effectiveAgentId);
    if (!agent) {
      agent = {
        agentId: effectiveAgentId,
        provider: payload?.provider || null,
        model: payload?.model || null,
        status: "idle",
        runId: runId || null,
        taskId: taskId || null,
        lastEventType: eventType,
        lastEventAt: occurredAt,
        eventCount: 0,
        usage: { costUsd: 0, tokensEstimate: 0 },
      };
      state.agents.set(effectiveAgentId, agent);
    }

    agent.lastEventType = eventType;
    agent.lastEventAt = occurredAt;
    agent.eventCount++;
    if (runId) agent.runId = runId;
    if (taskId) agent.taskId = taskId;
    if (payload?.provider && !agent.provider) agent.provider = payload.provider;
    if (payload?.model && !agent.model) agent.model = payload.model;

    // Agent status transitions
    switch (eventType) {
      case "session.spawn":
        agent.status = "planning";
        break;
      case "session.started":
        agent.status = "running";
        break;
      case "agent.leased":
      case "task.running":
        agent.status = "running";
        break;
      case "agent.tool_use":
        agent.status = "running";
        break;
      case "agent.rate_limit":
        agent.status = "waiting";
        break;
      // Terminal arms, mirroring the daemon projection. Without these an
      // agent that ever reached `running` stayed running for the life of
      // the journal, so finished and cancelled sessions read as live.
      case "session.finished":
        agent.status = "complete";
        break;
      case "session.cancelled":
        agent.status = "idle";
        break;
      case "agent.session_failed":
      case "agent.spawn_failed":
        agent.status = "failed";
        break;
      // A chat session stays instructable after a finished turn: the next
      // instruction puts the agent back on the running ladder.
      case "session.instruction":
        agent.status = "running";
        break;
      case "review.requested":
        agent.status = "reviewing";
        break;
      case "task.done":
      case "run.completed":
        agent.status = "complete";
        break;
      case "usage.updated": {
        const cost = Number(payload?.costUsd ?? payload?.cost ?? 0);
        const tokens = Number(payload?.tokensEstimate ?? payload?.tokens ?? 0);
        if (!isNaN(cost)) agent.usage.costUsd += cost;
        if (!isNaN(tokens)) agent.usage.tokensEstimate += tokens;
        break;
      }
    }
  }
}

/**
 * Folds an array of events into a NormalizedState in sequential order.
 */
export function foldEvents(events: EventWire[], initialState?: NormalizedState): NormalizedState {
  const state = initialState || createEmptyNormalizedState();
  for (const event of events) {
    foldEvent(state, event);
  }
  return state;
}
