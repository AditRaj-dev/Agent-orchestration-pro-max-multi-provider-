// Parsing an agent draft the user pastes in by hand.
//
// The Agent Creator's deliverable is a fenced ```json block. When the daemon
// accepts it, a proposal card appears and the human never touches the JSON.
// When it does not — an unknown skill id rejects the whole draft — the human
// is left holding a JSON block with nowhere to put it. This is that place.
import type { AgentRecord } from "../types";

export interface ParsedDraft {
  draft: Partial<AgentRecord>;
  /** Non-fatal problems worth showing: fields dropped or defaulted. */
  warnings: string[];
}

/** Pull the first fenced block out of a chat reply, or return the text. */
export function unfence(text: string): string {
  const fence = text.match(/```(?:json)?\s*\n([\s\S]*?)```/);
  return (fence ? fence[1] : text).trim();
}

const MODES = ["plan", "accept_edits"];
const EFFORTS = ["low", "medium", "high"];

/**
 * Parse a pasted draft into an editor-ready record. Unknown skills are
 * dropped rather than carried: the registry rejects the whole draft on the
 * first one, so keeping them guarantees a failed save, while dropping them
 * leaves a draft the human can fix in the form.
 */
export function parseDraft(text: string, knownSkillIds: string[] = []): ParsedDraft {
  const source = unfence(text);
  if (!source) throw new Error("Nothing to parse — paste the agent's JSON block.");

  let raw: any;
  try {
    raw = JSON.parse(source);
  } catch (err: any) {
    throw new Error(`That is not valid JSON: ${err?.message || err}`);
  }
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("Expected a single JSON object describing one agent.");
  }

  const warnings: string[] = [];
  const str = (value: unknown) => (typeof value === "string" ? value.trim() : "");

  const skillsIn: string[] = Array.isArray(raw.skills) ? raw.skills.filter((s: unknown) => typeof s === "string") : [];
  const skills = knownSkillIds.length
    ? skillsIn.filter((id) => knownSkillIds.includes(id))
    : skillsIn;
  const dropped = skillsIn.filter((id) => !skills.includes(id));
  if (dropped.length) {
    warnings.push(
      `Dropped skill${dropped.length === 1 ? "" : "s"} that do not exist in this registry: ${dropped.join(", ")}. ` +
        "Pick real ones below before registering.",
    );
  }

  const mode = str(raw.mode);
  if (mode && !MODES.includes(mode)) {
    warnings.push(`Unknown mode ${JSON.stringify(mode)} — defaulted to plan.`);
  }
  const effort = str(raw.effort);
  if (effort && !EFFORTS.includes(effort)) {
    warnings.push(`Unknown effort ${JSON.stringify(effort)} — cleared.`);
  }

  const timeout = Number(raw.timeoutSecs ?? raw.timeout_secs);
  if (raw.timeoutSecs !== undefined && !Number.isFinite(timeout)) {
    warnings.push("timeoutSecs was not a number — defaulted to 600.");
  }

  const draft: Partial<AgentRecord> = {
    id: str(raw.id).toLowerCase(),
    name: str(raw.name),
    description: str(raw.description),
    adapterId: str(raw.adapterId ?? raw.adapter_id) || "antigravity-agy",
    model: str(raw.model) || null,
    effort: (EFFORTS.includes(effort) ? effort : null) as AgentRecord["effort"],
    mode: (MODES.includes(mode) ? mode : "plan") as AgentRecord["mode"],
    skills,
    toolAllowlist: Array.isArray(raw.toolAllowlist) ? raw.toolAllowlist : [],
    toolDenylist: Array.isArray(raw.toolDenylist) ? raw.toolDenylist : [],
    timeoutSecs: Number.isFinite(timeout) && timeout > 0 ? timeout : 600,
    builtin: false,
    enabled: true,
  };

  if (!draft.id) throw new Error('The draft has no "id" — that is the agent slug, e.g. "sql-reviewer".');
  if (!draft.name) warnings.push('No "name" in the draft — add one below.');

  return { draft, warnings };
}
