// Fold Parity Unit Tests (docs/F-11-desktop.md §3.3)
import { describe, expect, it } from "vitest";
import type { EventWire } from "../types";
import { createEmptyNormalizedState, foldEvent, foldEvents } from "./fold";

describe("F-11 Client-Side Projection Fold", () => {
  it("folds a happy path workflow from run.created to run.completed", () => {
    const runId = "run-001";
    const taskId = "task-001";
    const agentId = "agent-alpha";

    const events: EventWire[] = [
      {
        seq: 1,
        id: "ev-1",
        eventType: "run.created",
        occurredAt: "2026-08-22T10:00:00.000Z",
        runId,
        traceId: null,
        taskId: null,
        agentId: null,
        payload: { workflowId: "wf-spec-build" },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 2,
        id: "ev-2",
        eventType: "workflow.started",
        occurredAt: "2026-08-22T10:00:01.000Z",
        runId,
        traceId: null,
        taskId: null,
        agentId: null,
        payload: {
          workflowId: "wf-spec-build",
          nodes: [{ id: taskId, dependsOn: [] }],
        },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 3,
        id: "ev-3",
        eventType: "task.created",
        occurredAt: "2026-08-22T10:00:02.000Z",
        runId,
        traceId: null,
        taskId,
        agentId: null,
        payload: { nodeId: "node-1" },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 4,
        id: "ev-4",
        eventType: "task.ready",
        occurredAt: "2026-08-22T10:00:03.000Z",
        runId,
        traceId: null,
        taskId,
        agentId: null,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 5,
        id: "ev-5",
        eventType: "session.spawn",
        occurredAt: "2026-08-22T10:00:04.000Z",
        runId,
        traceId: null,
        taskId,
        agentId,
        payload: { provider: "anthropic", model: "claude-3-5-sonnet" },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 6,
        id: "ev-6",
        eventType: "agent.leased",
        occurredAt: "2026-08-22T10:00:05.000Z",
        runId,
        traceId: null,
        taskId,
        agentId,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 7,
        id: "ev-7",
        eventType: "task.running",
        occurredAt: "2026-08-22T10:00:06.000Z",
        runId,
        traceId: null,
        taskId,
        agentId,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 8,
        id: "ev-8",
        eventType: "agent.tool_use",
        occurredAt: "2026-08-22T10:00:07.000Z",
        runId,
        traceId: null,
        taskId,
        agentId,
        payload: { tool: "write_file", args: { path: "src/main.rs" } },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 9,
        id: "ev-9",
        eventType: "task.output_ready",
        occurredAt: "2026-08-22T10:00:08.000Z",
        runId,
        traceId: null,
        taskId,
        agentId,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 10,
        id: "ev-10",
        eventType: "review.requested",
        occurredAt: "2026-08-22T10:00:09.000Z",
        runId,
        traceId: null,
        taskId,
        agentId: "agent-reviewer",
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 11,
        id: "ev-11",
        eventType: "review.approved",
        occurredAt: "2026-08-22T10:00:10.000Z",
        runId,
        traceId: null,
        taskId,
        agentId: "agent-reviewer",
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 12,
        id: "ev-12",
        eventType: "git.queued",
        occurredAt: "2026-08-22T10:00:11.000Z",
        runId,
        traceId: null,
        taskId,
        agentId,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 13,
        id: "ev-13",
        eventType: "git.committed",
        occurredAt: "2026-08-22T10:00:12.000Z",
        runId,
        traceId: null,
        taskId,
        agentId,
        payload: { commitSha: "abc1234def5678" },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 14,
        id: "ev-14",
        eventType: "task.done",
        occurredAt: "2026-08-22T10:00:13.000Z",
        runId,
        traceId: null,
        taskId,
        agentId,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 15,
        id: "ev-15",
        eventType: "run.completed",
        occurredAt: "2026-08-22T10:00:14.000Z",
        runId,
        traceId: null,
        taskId: null,
        agentId: null,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
    ];

    const state = foldEvents(events);

    // Verify Run Summary
    const run = state.runs.get(runId);
    expect(run).toBeDefined();
    expect(run?.status).toBe("completed");
    expect(run?.workflowId).toBe("wf-spec-build");
    expect(run?.taskCounts.total).toBe(1);
    expect(run?.taskCounts.done).toBe(1);
    expect(run?.taskCounts.failed).toBe(0);
    expect(run?.taskCounts.active).toBe(0);
    expect(run?.firstSeq).toBe(1);
    expect(run?.lastSeq).toBe(15);

    // Verify Task Summary
    const task = state.tasks.get(taskId);
    expect(task).toBeDefined();
    expect(task?.state).toBe("done");
    expect(task?.nodeId).toBe("node-1");
    expect(task?.agentId).toBe(agentId);
    expect(task?.commitSha).toBe("abc1234def5678");

    // Verify Agent Summary
    const agent = state.agents.get(agentId);
    expect(agent).toBeDefined();
    expect(agent?.provider).toBe("anthropic");
    expect(agent?.model).toBe("claude-3-5-sonnet");
    expect(agent?.status).toBe("complete"); // terminal run marks agent complete
  });

  it("takes an agent off `running` when its session ends", () => {
    // A chat agent never reaches a run/task terminal, so without session
    // terminal arms it stayed `running` forever and the Command Center
    // showed cancelled sessions as RUNNING.
    const agentId = "agent-chat";
    let seq = 0;
    const ev = (eventType: string): EventWire => ({
      seq: ++seq,
      id: `ev-${seq}`,
      eventType,
      occurredAt: "2026-08-22T10:00:00.000Z",
      runId: null,
      traceId: null,
      taskId: null,
      agentId,
      payload: { chat: true, sessionId: "chat-1" },
      payloadRef: null,
      payloadHash: null,
      schemaVersion: 1,
    });

    const statusAfter = (terminal: string) => {
      seq = 0;
      const state = foldEvents([ev("session.spawn"), ev("session.started"), ev(terminal)]);
      return state.agents.get(agentId)?.status;
    };

    expect(statusAfter("session.finished")).toBe("complete");
    expect(statusAfter("session.cancelled")).toBe("idle");
    expect(statusAfter("agent.session_failed")).toBe("failed");

    // The next turn puts it back on the running ladder.
    seq = 0;
    const resumed = foldEvents([
      ev("session.spawn"),
      ev("session.started"),
      ev("session.finished"),
      ev("session.instruction"),
    ]);
    expect(resumed.agents.get(agentId)?.status).toBe("running");
  });

  it("handles crash-retry by incrementing task attempts", () => {
    const state = createEmptyNormalizedState();
    const runId = "run-crash";
    const taskId = "task-crash";

    foldEvent(state, {
      seq: 1,
      id: "ev-1",
      eventType: "run.created",
      occurredAt: "2026-08-22T10:00:00.000Z",
      runId,
      traceId: null,
      taskId: null,
      agentId: null,
      payload: {},
      payloadRef: null,
      payloadHash: null,
      schemaVersion: 1,
    });

    foldEvent(state, {
      seq: 2,
      id: "ev-2",
      eventType: "task.created",
      occurredAt: "2026-08-22T10:00:01.000Z",
      runId,
      traceId: null,
      taskId,
      agentId: null,
      payload: {},
      payloadRef: null,
      payloadHash: null,
      schemaVersion: 1,
    });

    foldEvent(state, {
      seq: 3,
      id: "ev-3",
      eventType: "task.running",
      occurredAt: "2026-08-22T10:00:02.000Z",
      runId,
      traceId: null,
      taskId,
      agentId: "agent-1",
      payload: {},
      payloadRef: null,
      payloadHash: null,
      schemaVersion: 1,
    });

    expect(state.tasks.get(taskId)?.attempts).toBe(0);

    foldEvent(state, {
      seq: 4,
      id: "ev-4",
      eventType: "agent.crashed",
      occurredAt: "2026-08-22T10:00:03.000Z",
      runId,
      traceId: null,
      taskId,
      agentId: "agent-1",
      payload: { reason: "OOM killer" },
      payloadRef: null,
      payloadHash: null,
      schemaVersion: 1,
    });

    expect(state.tasks.get(taskId)?.attempts).toBe(1);
  });

  it("handles human parking via approval.required with blocking: true", () => {
    const state = createEmptyNormalizedState();
    const runId = "run-park";
    const taskId = "task-park";

    foldEvents([
      {
        seq: 1,
        id: "ev-1",
        eventType: "run.created",
        occurredAt: "2026-08-22T10:00:00.000Z",
        runId,
        traceId: null,
        taskId: null,
        agentId: null,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 2,
        id: "ev-2",
        eventType: "task.created",
        occurredAt: "2026-08-22T10:00:01.000Z",
        runId,
        traceId: null,
        taskId,
        agentId: null,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 3,
        id: "ev-3",
        eventType: "approval.required",
        occurredAt: "2026-08-22T10:00:02.000Z",
        runId,
        traceId: null,
        taskId,
        agentId: "agent-1",
        payload: { blocking: true, reason: "Sensitive migration" },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
    ], state);

    expect(state.tasks.get(taskId)?.state).toBe("human_required");
  });

  it("rolls usage into AgentSummary.usage correctly", () => {
    const state = createEmptyNormalizedState();
    const agentId = "agent-metered";

    foldEvents([
      {
        seq: 1,
        id: "ev-1",
        eventType: "session.started",
        occurredAt: "2026-08-22T10:00:00.000Z",
        runId: "run-1",
        traceId: null,
        taskId: "task-1",
        agentId,
        payload: {},
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 2,
        id: "ev-2",
        eventType: "usage.updated",
        occurredAt: "2026-08-22T10:00:01.000Z",
        runId: "run-1",
        traceId: null,
        taskId: "task-1",
        agentId,
        payload: { costUsd: 0.015, tokensEstimate: 4200 },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
      {
        seq: 3,
        id: "ev-3",
        eventType: "usage.updated",
        occurredAt: "2026-08-22T10:00:02.000Z",
        runId: "run-1",
        traceId: null,
        taskId: "task-1",
        agentId,
        payload: { costUsd: 0.025, tokensEstimate: 6800 },
        payloadRef: null,
        payloadHash: null,
        schemaVersion: 1,
      },
    ], state);

    const agent = state.agents.get(agentId);
    expect(agent).toBeDefined();
    expect(agent?.usage.costUsd).toBeCloseTo(0.040, 5);
    expect(agent?.usage.tokensEstimate).toBe(11000);
  });

  it("is total: unknown event types leave state unchanged but update counts and seq", () => {
    const state = createEmptyNormalizedState();
    const runId = "run-future";

    foldEvent(state, {
      seq: 1,
      id: "ev-1",
      eventType: "run.created",
      occurredAt: "2026-08-22T10:00:00.000Z",
      runId,
      traceId: null,
      taskId: null,
      agentId: null,
      payload: {},
      payloadRef: null,
      payloadHash: null,
      schemaVersion: 1,
    });

    foldEvent(state, {
      seq: 2,
      id: "ev-2",
      eventType: "custom.quantum.teleport",
      occurredAt: "2026-08-22T10:00:05.000Z",
      runId,
      traceId: null,
      taskId: null,
      agentId: null,
      payload: { qubit: 42 },
      payloadRef: null,
      payloadHash: null,
      schemaVersion: 99,
    });

    const run = state.runs.get(runId);
    expect(run).toBeDefined();
    expect(run?.status).toBe("running");
    expect(run?.lastSeq).toBe(2);
    expect(run?.eventCount).toBe(2);
  });
});
