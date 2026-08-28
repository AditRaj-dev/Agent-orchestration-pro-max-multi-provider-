import { describe, expect, it } from "vitest";
import { baseNodes, validateDag } from "../lib/workflow";

describe("workflow validation", () => {
  it("accepts the default execution topology", () => {
    expect(validateDag(baseNodes)).toBeUndefined();
  });

  it("rejects cycles and unknown dependency references", () => {
    expect(validateDag([{ ...baseNodes[0], id: "a", needs: ["b"] }, { ...baseNodes[1], id: "b", needs: ["a"] }])).toMatch(/acyclic/);
    expect(validateDag([{ ...baseNodes[0], id: "a", needs: ["missing"] }])).toMatch(/acyclic/);
  });
});
