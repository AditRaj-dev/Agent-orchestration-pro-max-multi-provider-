// Pasted-draft parsing: the manual route into the registry when a proposal
// card never appears.
import { describe, expect, it } from "vitest";
import { parseDraft, unfence } from "./draft";

const SKILLS = ["react-dev", "typescript-specialist", "code-graph-discipline", "decision-protocol"];

describe("parseDraft", () => {
  it("accepts the fenced block an agent actually outputs", () => {
    const text = [
      "Here is the draft you asked for.",
      "",
      "```json",
      JSON.stringify({
        id: "react-native-expo-specialist",
        name: "React Native Expo Specialist",
        description: "Builds Expo apps.",
        adapterId: "antigravity-agy",
        model: "gpt-oss-120b-medium",
        effort: "medium",
        mode: "accept_edits",
        skills: ["react-dev", "typescript-specialist"],
        timeoutSecs: 900,
      }),
      "```",
      "",
      "Draft ready for review.",
    ].join("\n");

    const { draft, warnings } = parseDraft(text, SKILLS);
    expect(draft.id).toBe("react-native-expo-specialist");
    expect(draft.name).toBe("React Native Expo Specialist");
    expect(draft.adapterId).toBe("antigravity-agy");
    expect(draft.effort).toBe("medium");
    expect(draft.mode).toBe("accept_edits");
    expect(draft.timeoutSecs).toBe(900);
    expect(draft.skills).toEqual(["react-dev", "typescript-specialist"]);
    expect(warnings).toEqual([]);
  });

  it("drops invented skills instead of guaranteeing a failed save", () => {
    // The exact shape that produced "skill expo-api-routes does not exist"
    // and left the user holding a JSON block with nowhere to put it.
    const { draft, warnings } = parseDraft(
      JSON.stringify({
        id: "rn-expo",
        name: "RN Expo",
        skills: ["react-dev", "expo-api-routes", "react-native-architecture"],
      }),
      SKILLS,
    );
    expect(draft.skills).toEqual(["react-dev"]);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("expo-api-routes");
    expect(warnings[0]).toContain("react-native-architecture");
  });

  it("takes raw JSON with no fence", () => {
    const { draft } = parseDraft('{"id":"sql-reviewer","name":"SQL Reviewer"}', SKILLS);
    expect(draft.id).toBe("sql-reviewer");
    // Safe defaults so the form is immediately valid.
    expect(draft.mode).toBe("plan");
    expect(draft.timeoutSecs).toBe(600);
    expect(draft.builtin).toBe(false);
  });

  it("defaults unknown mode and effort rather than refusing the draft", () => {
    const { draft, warnings } = parseDraft(
      JSON.stringify({ id: "a", name: "A", mode: "yolo", effort: "extreme" }),
      SKILLS,
    );
    expect(draft.mode).toBe("plan");
    expect(draft.effort).toBeNull();
    expect(warnings.join(" ")).toContain("yolo");
    expect(warnings.join(" ")).toContain("extreme");
  });

  it("explains what is wrong instead of throwing raw parser noise", () => {
    expect(() => parseDraft("not json at all", SKILLS)).toThrow(/not valid JSON/);
    expect(() => parseDraft("[]", SKILLS)).toThrow(/single JSON object/);
    expect(() => parseDraft("{}", SKILLS)).toThrow(/no "id"/);
    expect(() => parseDraft("   ", SKILLS)).toThrow(/Nothing to parse/);
  });

  it("unfences only the first block", () => {
    expect(unfence("```json\n{\"a\":1}\n```\ntrailing")).toBe('{"a":1}');
    expect(unfence('{"a":1}')).toBe('{"a":1}');
  });
});
