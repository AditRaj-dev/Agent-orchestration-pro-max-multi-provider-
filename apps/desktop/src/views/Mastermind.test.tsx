import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import { PlannerSwap, StageChoices, type SessionSummary } from "./Mastermind";

function writeGateSession(overrides: Partial<SessionSummary> = {}): SessionSummary {
  return {
    sessionId: "mastermind-test",
    goal: "Plan a demo",
    repo: "C:/project",
    runId: null,
    cycles: 4,
    maxCycles: 12,
    reviewerPool: "code-reviewer",
    pools: [],
    roster: [],
    nodes: [],
    pendingRejections: 0,
    phase: "phase-1-discovery",
    phaseNumber: "1",
    phaseName: "Discovery",
    phaseStatus: "awaiting-write-approval",
    canAuthorizeWrite: true,
    writeRequest: {
      prompt: "Discovery is complete. Mastermind is still read-only.",
      deliverables: ["docs/DISCOVERY.md"],
    },
    ...overrides,
  };
}

describe("Mastermind scoped authoring approval card", () => {
  it("shows the daemon-owned prompt and exact deliverable instead of a generic continue action", () => {
    const markup = renderToStaticMarkup(
      createElement(StageChoices, {
        session: writeGateSession(),
        committed: false,
        runStatus: null,
        cyclesLeft: 8,
        busy: null,
        terminalRun: false,
        onPlan: vi.fn(),
        onAuthorizeWrite: vi.fn(),
        onRecoverArtifact: vi.fn(),
        onRetryAuthoring: vi.fn(),
        onApprovePhase: vi.fn(),
        onAcceptAsIs: vi.fn(),
        onDrive: vi.fn(),
        onRefresh: vi.fn(),
      }),
    );

    expect(markup).toContain("Discovery is complete. Mastermind is still read-only.");
    expect(markup).toContain("Allow scoped write");
    expect(markup).toContain("docs/DISCOVERY.md");
    expect(markup).not.toContain("Continue discovery");
  });

  it("disables authorization when the daemon says the grant is unavailable", () => {
    const markup = renderToStaticMarkup(
      createElement(StageChoices, {
        session: writeGateSession({ canAuthorizeWrite: false }),
        committed: false,
        runStatus: null,
        cyclesLeft: 8,
        busy: null,
        terminalRun: false,
        onPlan: vi.fn(),
        onAuthorizeWrite: vi.fn(),
        onRecoverArtifact: vi.fn(),
        onRetryAuthoring: vi.fn(),
        onApprovePhase: vi.fn(),
        onAcceptAsIs: vi.fn(),
        onDrive: vi.fn(),
        onRefresh: vi.fn(),
      }),
    );

    expect(markup).toMatch(/<button[^>]*disabled=""[^>]*>.*Allow scoped write/s);
  });

  it("offers an accept-as-is override instead of a broken revision-without-guidance action", () => {
    const markup = renderToStaticMarkup(
      createElement(StageChoices, {
        session: writeGateSession({
          phase: "phase-2-prd",
          phaseName: "Product requirements",
          phaseStatus: "needs-revision",
          canAuthorizeWrite: false,
          writeRequest: null,
        }),
        committed: false,
        runStatus: null,
        cyclesLeft: 8,
        busy: null,
        terminalRun: false,
        onPlan: vi.fn(),
        onAuthorizeWrite: vi.fn(),
        onRecoverArtifact: vi.fn(),
        onRetryAuthoring: vi.fn(),
        onApprovePhase: vi.fn(),
        onAcceptAsIs: vi.fn(),
        onDrive: vi.fn(),
        onRefresh: vi.fn(),
      }),
    );

    expect(markup).toContain("Accept as-is");
    expect(markup).toContain("Waive the review findings");
    expect(markup).not.toContain("Revise phase");
  });

  it("offers read-only review when authoring timed out after creating the artifact", () => {
    const markup = renderToStaticMarkup(
      createElement(StageChoices, {
        session: writeGateSession({
          phase: "phase-3-features",
          phaseName: "Feature specifications",
          phaseStatus: "blocked",
          canAuthorizeWrite: false,
          writeRequest: null,
          canRecoverArtifact: true,
          recoveryRequest: {
            prompt: "The scoped authoring turn timed out, but docs/features/*.md exists.",
            deliverables: ["docs/features"],
          },
        }),
        committed: false,
        runStatus: null,
        cyclesLeft: 8,
        busy: null,
        terminalRun: false,
        onPlan: vi.fn(),
        onAuthorizeWrite: vi.fn(),
        onRecoverArtifact: vi.fn(),
        onRetryAuthoring: vi.fn(),
        onApprovePhase: vi.fn(),
        onAcceptAsIs: vi.fn(),
        onDrive: vi.fn(),
        onRefresh: vi.fn(),
      }),
    );

    expect(markup).toContain("Review existing artifact");
    expect(markup).toContain("No authoring rerun");
    expect(markup).not.toContain("Allow scoped write");
  });

  it("offers a clean same-scope retry when authoring produced no artifact", () => {
    const markup = renderToStaticMarkup(
      createElement(StageChoices, {
        session: writeGateSession({
          phase: "phase-4-implementation-plan",
          phaseName: "Implementation plan",
          phaseStatus: "blocked",
          canAuthorizeWrite: false,
          writeRequest: null,
          canRecoverArtifact: false,
          recoveryRequest: null,
          canRetryAuthoring: true,
          retryRequest: {
            prompt: "The authoring turn ended before producing docs/IMPLEMENTATION_PLAN.md.",
            deliverables: ["docs/IMPLEMENTATION_PLAN.md"],
          },
        }),
        committed: false,
        runStatus: null,
        cyclesLeft: 8,
        busy: null,
        terminalRun: false,
        onPlan: vi.fn(),
        onAuthorizeWrite: vi.fn(),
        onRecoverArtifact: vi.fn(),
        onRetryAuthoring: vi.fn(),
        onApprovePhase: vi.fn(),
        onAcceptAsIs: vi.fn(),
        onDrive: vi.fn(),
        onRefresh: vi.fn(),
      }),
    );

    expect(markup).toContain("Retry scoped authoring");
    expect(markup).toContain("docs/IMPLEMENTATION_PLAN.md");
    expect(markup).not.toContain("Review existing artifact");
  });
});

describe("Mastermind planner picker", () => {
  const sol = { plannerAdapter: "codex", plannerModel: "gpt-5.6-sol" };
  const opus = { plannerAdapter: "claude-code", plannerModel: "claude-opus-5" };

  it("selects the configured planner and groups every available model by use class", () => {
    const markup = renderToStaticMarkup(
      createElement(PlannerSwap, {
        session: writeGateSession({ ...sol, phaseStatus: "blocked" }),
        busy: null,
        onSwap: vi.fn(),
      }),
    );

    expect(markup).toContain("GPT-5.6 Sol");
    expect(markup).toContain("Opus 5");
    expect(markup).toContain("Gemini 3.1 Pro (High)");
    expect(markup).toContain("General use");
    expect(markup).toContain("Worker");
    expect(markup).toMatch(/<option value="codex-sol" selected="">/);
    expect(markup).not.toContain("disabled");
  });

  it("refuses a mid-turn swap the daemon would reject anyway", () => {
    const markup = renderToStaticMarkup(
      createElement(PlannerSwap, {
        session: writeGateSession({ ...opus, phaseStatus: "running" }),
        busy: null,
        onSwap: vi.fn(),
      }),
    );

    expect(markup).toMatch(/<select[^>]*disabled=""/);
    expect(markup).toContain("A provider turn is in flight");
  });

  it("disables both options while any other call is in flight", () => {
    const markup = renderToStaticMarkup(
      createElement(PlannerSwap, {
        session: writeGateSession({ ...sol, phaseStatus: "blocked" }),
        busy: "refreshing",
        onSwap: vi.fn(),
      }),
    );

    expect(markup).toMatch(/<select[^>]*disabled=""/);
  });
});

describe("Mastermind approval gate card", () => {
  function render(session: SessionSummary) {
    return renderToStaticMarkup(
      createElement(StageChoices, {
        session,
        committed: false,
        runStatus: null,
        cyclesLeft: 8,
        busy: null,
        terminalRun: false,
        onPlan: vi.fn(),
        onAuthorizeWrite: vi.fn(),
        onRecoverArtifact: vi.fn(),
        onRetryAuthoring: vi.fn(),
        onApprovePhase: vi.fn(),
        onAcceptAsIs: vi.fn(),
        onDrive: vi.fn(),
        onRefresh: vi.fn(),
      }),
    );
  }

  it("offers Approve once the accept has moved the phase to awaiting-approval", () => {
    const markup = render(
      writeGateSession({
        phase: "phase-5-api-record",
        phaseName: "API record",
        phaseNumber: "5",
        phaseStatus: "awaiting-approval",
        canApprove: true,
        canAuthorizeWrite: false,
        writeRequest: undefined,
      }),
    );

    expect(markup).toContain("Approve API record");
    // The needs-revision affordance must be gone; re-offering it is what
    // stranded the user on an already-accepted gate.
    expect(markup).not.toContain("Accept as-is");
  });

  it("does not offer Approve while the daemon withholds it", () => {
    const markup = render(
      writeGateSession({
        phase: "phase-5-api-record",
        phaseName: "API record",
        phaseNumber: "5",
        phaseStatus: "awaiting-approval",
        canApprove: false,
        canAuthorizeWrite: false,
        writeRequest: undefined,
      }),
    );

    expect(markup).toContain("Approve API record");
    expect(markup).toContain("disabled=\"\"");
  });
});
