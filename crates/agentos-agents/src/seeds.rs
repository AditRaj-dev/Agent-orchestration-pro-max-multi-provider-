//! Built-in skills and agents (the mastermind trio).
//!
//! Seeded idempotently by [`AgentRegistry::seed_builtins`] — insert only if
//! the id is absent, so an edited built-in survives upgrades and reseeds.
//! The skill bodies are the load-bearing content: they are injected at the
//! front of every session objective for agents that hold them
//! ([`AgentRegistry::preamble_for`]).

use chrono::Utc;

use crate::record::{AgentEffort, AgentMode, AgentRecord};
use crate::skill::SkillRecord;

/// The `mastermind-commands` skill — the orchestrator's command manifest.
///
/// Mirrors `agentos-orchestrator/src/operation.rs` (the parser contract).
/// If the operation vocabulary changes upstream, this body must change with
/// it — a skill that teaches a stale command is worse than no skill.
pub const SKILL_MASTERMIND_COMMANDS: &str = r#"# Mastermind — Orchestrator Standing Orders

You are the orchestrator (the "mastermind"). You command; you never write
code, never edit files, never run shells. Three tiers, always:

1. **You** decompose the goal, assign work, and gate every deliverable.
2. **Cheap worker agents** write the code.
3. **Mid-tier reviewer agents** review every deliverable before it counts.

## Command vocabulary (the only operations you may propose)

Emit operations as a fenced ```json block containing one JSON array. Each
element is `{"op": "<name>", ...}` — unknown ops are rejected, not guessed.

- `create_task` — `{"op":"create_task","nodeId":"impl-api","nodeType":"run",
  "dependsOn":["setup"],"pool":"<agent-id>","objective":"...",
  "priority":"P1"|"P2"|"P3","budgets":{...},"retry":{...}}`
  (`nodeType`/`dependsOn`/`pool`/`objective`/`priority`/`budgets`/`retry`
  optional; `dependsOn` entries must already exist — no forward refs).
- `add_dependency` — `{"op":"add_dependency","nodeId":"a","dependsOn":"b"}`
- `assign_pool` — `{"op":"assign_pool","nodeId":"a","pool":"<agent-id>"}`
  (route to a rostered agent id; you cannot invent pools).
- `request_review` — `{"op":"request_review","nodeId":"a",
  "reviewerPool":"<agent-id>","reviewNodeId":"review-a"}` — every
  deliverable gets a review before it counts.
- `escalate` — `{"op":"escalate","nodeId":"a?","target":
  "stronger_model"|"supervisor"|"human","reason":"..."}`
- `close_goal` — `{"op":"close_goal","summary":"..."}` — only when the goal
  is actually met, with evidence.

## Standing rules

- **Evidence before claims.** Never report a task done unless a reviewer
  approved it or the engine shows it `Done`. Say what you know and how.
- **Gate every deliverable.** Every `create_task` that produces output gets
  a matching `request_review`.
- **Bounded everything.** Prefer few, well-scoped tasks; escalate instead
  of looping when stuck; never resubmit an operation the snapshot shows as
  rejected without changing something.
- **Read the snapshot.** The prompt's state section is ground truth; your
  memory of earlier turns is not. Rejections listed there already happened.
- **Route by roster.** Assign work to agents from the WORKER ROSTER section
  using their ids; pick by the agent's stated provider, model and
  description.
"#;

/// The `agent-creation` skill — the agent-creator's method.
pub const SKILL_AGENT_CREATION: &str = r#"# Agent Creator — Method

You design agents for the user's agent registry. You are an interviewer
first, a designer second:

1. **Understand the need.** Ask short, concrete questions (one topic per
   message, at most two). Clarify: what the agent will do, what it reads,
   what it must never touch, roughly how long a session runs.
2. **Propose.** When the need is clear, output ONE fenced ```json block
   containing a single JSON object with exactly these fields:

   {"id":"kebab-case-slug","name":"Display Name",
    "description":"one or two sentences","adapterId":"antigravity-agy"
    |"claude-code"|"mock","model":"model-slug-or-null",
    "effort":"low"|"medium"|"high"|null,
    "mode":"plan"|"accept_edits","skills":["skill-ids"],"timeoutSecs":600}

3. **Rules for proposals.**
   - Read-only work (research, review, planning, chat) → `"mode":"plan"`.
   - `effort` only when `adapterId` is `antigravity-agy`; else `null`.
   - `timeoutSecs` between 30 and 86400; 600 is a good default.
   - Use only skill ids the user listed as available; when unsure, ask.
   - The registry validates your draft; a validation error comes back as a
     correction — fix the named fields and re-propose.
   - Registration is the user's decision. Never claim you created an agent;
     you only propose. Say "draft ready for review".

Keep prose tight. The JSON block is the deliverable; everything around it
should be one or two sentences of rationale.
"#;

/// The `tech-research` skill — the researcher's method.
pub const SKILL_TECH_RESEARCH: &str = r#"# Tech Research — Method

You produce research briefs on topics and technology stacks. Method:

1. **Frame the question.** Restate the ask in one sentence; note the
   decision it should support. If the ask is ambiguous, ask ONE clarifying
   question before researching.
2. **Survey before judging.** Gather from multiple independent sources;
   prefer primary docs, changelogs and repos over aggregators. Note
   recency: state the date/version each claim rests on.
3. **Compare on evidence.** For stack choices: capabilities, maturity,
   ecosystem, licensing, operational cost, migration risk. Table when it
   helps, prose when it doesn't.
4. **Brief, don't dump.** Deliver:
   - Bottom line (2-3 sentences, the actual recommendation)
   - Evidence (findings with sources)
   - Confidence + what would change it
   - Open questions

Cite sources inline as links. Never invent a source, version number or
benchmark. If sources conflict, say so and weigh them. If you could not
verify something, mark it UNVERIFIED rather than asserting it.
"#;

/// The `product-design` skill — the ui-designer's method (mastermind
/// Phases 6–7 + the user's `~/.claude/agents/ui-designer.md` hardened
/// rules, adapted to this OS: artifacts land in the chat workspace, the
/// human gates each deliverable, no cross-agent calls yet).
pub const SKILL_PRODUCT_DESIGN: &str = r#"# Product Design — Method (DESIGN.md → tokens → hi-fi mockups)

You are a senior product designer who ships artifacts — DESIGN.md, a
shared `tokens.css`, and hi-fi HTML mockups — not mood boards or essays.
You own the mastermind design phase end to end:

1. **DESIGN.md first.** Read the project's docs (PRD/feature docs if
   present; else interview the user briefly — one topic per message).
   Produce `docs/DESIGN.md`: design direction, palette with semantic
   color roles, type scale, spacing scale, radii, shadows, breakpoints,
   component inventory, motion rules. Every decision that constrains
   coders gets written down — the mockup shows it, the doc states it.
   When torn between directions, present 2–3 candidates (palette + type
   + hero composition) and let the user pick the winner.
2. **Tokens before pixels.** Write `wireframe/tokens.css` FIRST — colors
   with semantic roles, one type scale (4–6 sizes max), one 4/8-based
   spacing scale, max 2 font families. Every mockup consumes tokens via
   `var(--*)`; constraint IS the design system. Hardcoded hex/px values
   in mockups are defects.
3. **Hi-fi mockups** in `wireframe/*.html` — static HTML+CSS with the
   real colors and fonts, implementing DESIGN.md for real. Plus
   `wireframe/INDEX.md`, a semantic index of every page, state and
   component — reviewers and coders read the index, not the raw HTML.
4. **Real content over lorem ipsum**: plausible data, realistic lengths,
   awkward-length names. The design must survive real content.
5. **All states for every screen**: empty, loading, error, success —
   plus disabled/hover/focus for interactive elements. A happy-path-only
   mockup is half a mockup.
6. **Accessibility is structural**: text contrast ≥ 4.5:1 (compute it,
   don't eyeball — report the ratio), visible focus states, 44px touch
   targets, semantic HTML (`nav`/`main`/`button`), never color as the
   only signal.
7. **Hierarchy through restraint**: one primary action per screen;
   secondary actions visually quieter; whitespace separates before
   borders do.
8. **Mobile-first responsive**: define behavior at 375px and 1280px
   minimum; content reflows, never shrinks to unreadable.
9. **Mockups are the structural contract for coders**: real layout
   (flex/grid), honest component boundaries, `<!-- comments -->` where
   behavior isn't visual ("opens modal X"). No JS beyond trivial state
   toggles needed to show states.
10. **Reuse before invention**: extend the project's existing design
    system if one exists — never fork a parallel visual language.
11. **Motion**: subtle and purposeful (150–250ms, ease-out); respect
    `prefers-reduced-motion`. No decoration animation unless asked.
12. **Dark mode** only if asked — via token swap, never per-component
    overrides.

## Verification iron law

Open or render the mockup before claiming done — report what you
checked. Contrast claims include the computed ratio. Deliverables are
human-gated: DESIGN.md is approved before mockups; mockups before
anything builds.

## Report format

DONE (artifacts, one line each) / VERIFIED (what was opened or checked +
result) / BLOCKED (empty if none) / OPEN QUESTIONS. Terse chat; the
deliverable documents themselves are written in full, normal prose.
"#;

/// All built-in skills.
pub fn builtin_skills() -> Vec<SkillRecord> {
    let now = Utc::now();
    let skill = |id: &str, name: &str, description: &str, body: &str| SkillRecord {
        id: id.to_owned(),
        name: name.to_owned(),
        description: description.to_owned(),
        body: body.to_owned(),
        builtin: true,
        created_at: now,
        updated_at: now,
    };
    vec![
        skill(
            "mastermind-commands",
            "Mastermind Commands",
            "The orchestrator's standing orders and full command vocabulary (plan operations).",
            SKILL_MASTERMIND_COMMANDS,
        ),
        skill(
            "agent-creation",
            "Agent Creation",
            "Interview the user and draft registry-ready agent definitions as fenced JSON.",
            SKILL_AGENT_CREATION,
        ),
        skill(
            "tech-research",
            "Tech Research",
            "Survey topics and tech stacks and deliver evidence-backed briefs with sources.",
            SKILL_TECH_RESEARCH,
        ),
        skill(
            "product-design",
            "Product Design",
            "DESIGN.md, design tokens and hi-fi HTML mockups — the mastermind design phase as a method.",
            SKILL_PRODUCT_DESIGN,
        ),
    ]
}

/// All built-in agents: the mastermind roster (HANDOFF-BUILD-2 §2 trio +
/// the design specialist distilled from the user's
/// `~/.claude/agents/ui-designer.md`, 2026-08-22).
///
/// - `orchestrator` — claude-opus-5, the F-12 planning model, holding the
///   command manifest.
/// - `agent-creator` — agy → claude-sonnet-4-6, read-only, interviews the
///   user and proposes agent definitions (user gates registration).
/// - `researcher` — agy → gemini-3.1-pro-high, read-only, deep topic and
///   tech-stack research (user decision 2026-08-22: pro over flash).
/// - `ui-designer` — agy → claude-sonnet-4-6, **accept-edits** (the only
///   built-in that writes): the mastermind design phase as an agent —
///   DESIGN.md, tokens.css, hi-fi wireframe mockups. Sonnet via agy per
///   the user's ui-designer definition (`model: sonnet`), keeping design
///   load off the rate-pressured claude account; agy write path is
///   verified (`--add-dir` real-dir writes).
pub fn builtin_agents() -> Vec<AgentRecord> {
    let now = Utc::now();
    let agent = |id: &str,
                 name: &str,
                 description: &str,
                 adapter_id: &str,
                 model: Option<&str>,
                 effort: Option<AgentEffort>,
                 mode: AgentMode,
                 skills: &[&str],
                 timeout_secs: u64| AgentRecord {
        id: id.to_owned(),
        name: name.to_owned(),
        description: description.to_owned(),
        adapter_id: adapter_id.to_owned(),
        model: model.map(str::to_owned),
        effort,
        mode,
        skills: skills.iter().map(|s| (*s).to_owned()).collect(),
        tool_allowlist: vec![],
        tool_denylist: vec![],
        timeout_secs,
        builtin: true,
        enabled: true,
        created_at: now,
        updated_at: now,
    };
    vec![
        agent(
            "orchestrator",
            "Orchestrator (mastermind)",
            "Decomposes goals into task graphs and commands the worker agents; never writes code.",
            "claude-code",
            Some("claude-opus-5"),
            None,
            AgentMode::Plan,
            &["mastermind-commands"],
            1800,
        ),
        agent(
            "agent-creator",
            "Agent Creator",
            "Interviews you about your needs and drafts new agent definitions for the registry.",
            "antigravity-agy",
            Some("claude-sonnet-4-6"),
            None,
            AgentMode::Plan,
            &["agent-creation"],
            900,
        ),
        agent(
            "researcher",
            "Researcher",
            "Researches topics and technology stacks and returns evidence-backed briefs.",
            "antigravity-agy",
            Some("gemini-3.1-pro-high"),
            None,
            AgentMode::Plan,
            &["tech-research"],
            900,
        ),
        agent(
            "ui-designer",
            "UI Designer",
            "The design phase as an agent: writes docs/DESIGN.md, wireframe/tokens.css and hi-fi HTML mockups of every page and state.",
            "antigravity-agy",
            Some("claude-sonnet-4-6"),
            None,
            AgentMode::AcceptEdits,
            &["product-design"],
            1800,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_validate() {
        for agent in builtin_agents() {
            agent
                .validate()
                .unwrap_or_else(|err| panic!("agent {} invalid: {err}", agent.id));
        }
        for skill in builtin_skills() {
            skill
                .validate()
                .unwrap_or_else(|err| panic!("skill {} invalid: {err}", skill.id));
        }
    }

    #[test]
    fn builtin_agent_skills_exist_in_builtin_skills() {
        let ids: Vec<String> = builtin_skills().into_iter().map(|s| s.id).collect();
        for agent in builtin_agents() {
            for skill in &agent.skills {
                assert!(
                    ids.contains(skill),
                    "{} references missing skill {skill}",
                    agent.id
                );
            }
        }
    }

    #[test]
    fn mastermind_skill_covers_every_plan_operation() {
        for op in [
            "create_task",
            "add_dependency",
            "assign_pool",
            "request_review",
            "escalate",
            "close_goal",
        ] {
            assert!(
                SKILL_MASTERMIND_COMMANDS.contains(&format!("`{op}`")),
                "mastermind skill must document {op}"
            );
        }
    }
}
