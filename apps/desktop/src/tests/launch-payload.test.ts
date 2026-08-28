import { describe, expect, it } from "vitest";
import { mastermindStartPayload, projectCreatePayload, projectOpenPayload, projectScaffoldPayload } from "../App";

describe("daemon launch payloads", () => {
  it("uses daemon-owned project field names", () => {
    expect(projectCreatePayload("D:/work/repo")).toEqual({ rootPath: "D:/work/repo" });
    expect(projectOpenPayload("project-1")).toEqual({ id: "project-1" });
  });

  it("builds a safe built-in scaffold request and trims the initial goal", () => {
    expect(projectScaffoldPayload("D:/work", "new-app", "nextjs", "  Ship the app  ")).toEqual({
      parentPath: "D:/work",
      name: "new-app",
      starterId: "nextjs",
      sessionDefaults: { initialGoal: "Ship the app" },
    });
  });

  it("keeps Mastermind concurrency inside the daemon contract", () => {
    expect(mastermindStartPayload("Goal", "D:/work/repo", "codex", "gpt-5", 17)).toEqual({ goal: "Goal", repo: "D:/work/repo", plannerAdapter: "codex", plannerModel: "gpt-5", maxConcurrency: 8 });
    expect(mastermindStartPayload("Goal", "D:/work/repo", "", "", 0)).toEqual({ goal: "Goal", repo: "D:/work/repo", maxConcurrency: 1 });
  });
});
