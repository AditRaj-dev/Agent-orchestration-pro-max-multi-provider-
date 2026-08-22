# F-13 — Dynamic Agent Registry: agents as data, the Agent Creator, the Researcher

Status: implemented 2026-08-22 · Crates: `crates/agentos-agents` (new),
`crates/agentos-daemon` (registry API + chat sessions), `crates/agentos-runtime`
(registry-driven routing), `crates/agentos-orchestrator` (roster + skill
preamble), `apps/desktop` (Agents view) · Canon: `F-00-CONVENTIONS.md` §3/§4/§5,
`docs/F-11-desktop.md` §2/§3 + §7 (F-13 addendum), `docs/HANDOFF-BUILD-2.md` §2
(mastermind three-tier), `D:\OP\handoff.md` §"SESSION 4 DECISIONS" (user
decisions: creator = agy→claude-sonnet-4-6, researcher = agy→gemini-3.1-pro).

## 1. Purpose

Before F-13, "which agent runs what" was code: the supervisor's static
`role_adapters` map, the orchestrator's hardcoded `DEFAULT_POOLS`, and no way
to create or change a worker without recompiling. F-13 makes the system
**dynamic the way a registry is dynamic**:

- Every spawnable agent is a **row** (`agents` table): provider adapter, model
  slug, reasoning effort, access mode, assigned skills, tool allow/deny lists,
  timeout. Created, edited, enabled/disabled and deleted at runtime over the
  daemon's WS API; the desktop Agents screen is the editor.
- **Skills are registry-owned markdown**, injected as a prompt preamble ahead
  of the session objective — provider-agnostic by construction (the F-03
  adapter delivers the objective on stdin; F-05 as the equals-form
  `--print=<objective>`; both carry the preamble with zero adapter changes).
- The **Agent Creator** (built-in agent, agy → `claude-sonnet-4-6`) interviews
  the user and drafts agent definitions; a **human clicks Register** — the
  mastermind gate, applied to the registry itself.
- The **Researcher** (built-in agent, agy → `gemini-3.1-pro-high`, read-only)
  researches topics and tech stacks on demand.
- The **UI Designer** (built-in agent, agy → `claude-sonnet-4-6`, write mode)
  is the mastermind design phase as an agent: DESIGN.md → tokens.css →
  hi-fi HTML mockups (§7).
- The **orchestrator** reads its worker roster and pool list from the enabled
  agents, and its planning prompt carries the `mastermind-commands` skill
  (the command manifest) as the preamble.

## 2. Module map

| Crate / module | Role |
|---|---|
| `agentos-agents/src/record.rs` | `AgentRecord` (+ `AgentMode`, `AgentEffort`, `KNOWN_ADAPTERS`). Wire-friendly serde defaults: timestamps default, `mode` defaults to **plan** (read-only — a wire payload that names no mode cannot accidentally gain write access), `builtin` forced false at the API edge. |
| `agentos-agents/src/skill.rs` | `SkillRecord` (id, name, description, body ≤ 12k chars, builtin). |
| `agentos-agents/src/seeds.rs` | Built-in skills `mastermind-commands` / `agent-creation` / `tech-research` and agents `orchestrator` / `agent-creator` / `researcher`. |
| `agentos-agents/src/registry.rs` | `AgentRegistry` over SQLite (private F-01 canon copy). CRUD, builtin guards (editable, never deletable), skill-reference guard (a skill assigned to an agent cannot be deleted), idempotent `seed_builtins()` (insert-if-absent — edited built-ins survive reseeds), `resolve()` (enabled only), `enabled_roster()`, `preamble_for()`. |
| `agentos-daemon/src/agent_sessions.rs` | `AgentSessions` + `AdapterSet`: chat sessions over the wired adapters (claude-code / antigravity-agy / mock), the fenced-JSON proposal parser, and the free provider catalog. |
| `agentos-daemon/src/server.rs` | The `registry.*` + `agent.session.*` method arms (§3) and the `agent.created/updated/deleted` audit journaling. |
| `agentos-runtime` `supervisor.rs` | `SupervisorConfig::agents_db` + registry overlay on routing and spec composition (§5). |
| `agentos-orchestrator` `snapshot.rs`/`plan.rs`/`model.rs` | `RosterEntry` + WORKER ROSTER prompt section, `PlanPolicy::for_pools`, `ClaudePlanningModel::with_skill_preamble`. |
| `apps/desktop/src/store/registry.ts` | `RegistryStore` (bootstrap via RPC, refresh on `agent.*` events, chat transcripts, proposals) + hooks. |
| `apps/desktop/src/views/Agents.tsx` | The Agents screen: roster, editor form, chat panel with proposal registration. |

## 3. WS API surface (F-11 §7 addendum is binding)

All methods use the §2.2 envelope. Domain rejections (validation, duplicate,
not-found, builtin-protected) map to `invalid_params`; storage failures to
`internal_error`.

| Method | Params | Result |
|---|---|---|
| `registry.agents.list` | — | `{agents: AgentRecord[]}` (built-ins first, then name) |
| `registry.agents.create` | `{agent}` (minimal: `id`,`name`,`description`,`adapterId`; the rest defaults) | `{agent}` — `builtin` forced false |
| `registry.agents.update` | `{agent}` (full record; `builtin`/`createdAt` preserved from the stored row) | `{agent}` |
| `registry.agents.delete` | `{id}` | `{deleted: true}` — built-ins refused |
| `registry.agents.set-enabled` | `{id, enabled: bool}` | `{agent}` |
| `registry.skills.list` | — | `{skills: SkillRecord[]}` |
| `registry.catalog` | — | `{providers: [{id, version, path, auth, models[]}]}` — **free probes only** (agy = `agy models`, 60s cache; claude = observed slugs; mock static) |
| `agent.session.start` | `{agentId, message}` | `{sessionId}` (`chat-…`); session runs async, events journal (§4) |
| `agent.session.send` | `{sessionId, message}` | `{sent: true}` — live sessions only (adapter resume path: claude `--resume`, agy `--conversation`) |
| `agent.session.cancel` | `{sessionId}` | `{cancelled: true}` — process-kill semantics |

Every registry mutation journals an audit event (`agent.created` /
`agent.updated` / `agent.deleted`, payload = the full record, `agent_id` = the
agent slug). The tables are mutable; the journal is the history.

## 4. Chat sessions

`AgentSessions::start(agentId, message)` resolves the (enabled) record,
composes `objective = preamble_for(record) + message`, maps the record onto
`SpawnSpec` (model, tool lists, timeout; mode → `allowed_paths` per the F-05
contract: empty ⇒ `--mode plan`; accept-edits agents name their workspace;
claude plan-mode agents get `Write/Edit/MultiEdit/NotebookEdit` denied since
F-03 pins `acceptEdits`), and spawns the record's adapter. A driver task
translates adapter events into journal events — the **same vocabulary the
supervisor's executor uses** (`session.spawn` with `provider`, `session.started`
with `model`, `agent.tool_use`, `usage.updated`, `session.finished`,
`agent.spawn_failed`) — with `agent_id` = the registry slug and one `trace_id`
per conversation. Chat sessions are **not** run/task-scoped (no fabricated
projections); finished sessions are removed from the active map and refuse
follow-ups (`invalid_params`).

Cost note (user-facing): every chat turn is a real provider session — agy
carries the fixed ~37k-token core-prompt overhead per session (handoff §"agy"),
tokens-not-dollars. The mock adapter exists for billing-free UI testing.

### 4.1 Proposals (the creator's contract)

When the session's agent holds the `agent-creation` skill, the driver treats
the final text as a potential proposal: fenced JSON blocks are extracted
(total parse — no fenced object with an `id` is silently dropped; a malformed
candidate journals `agent.proposal_invalid` with the reason), validated into
an `AgentRecord` draft (shape + skill existence + id-collision), and journaled
as `agent.proposal` (`payload.agent` = the draft). **Nothing touches the
registry**: registration is an explicit `registry.agents.create` from the
human — the mastermind gate applied to the registry itself.

## 5. Supervisor routing (the registry overlay)

`SupervisorConfig::agents_db: Option<PathBuf>` (`with_agents_db`). When set:

- `adapter_for(role)` resolves the role as a registry agent id **first**;
  static `role_adapters` and the default follow. Registry *read* failures
  degrade to static routing with a loud warn (the registry is an overlay, not
  a load-bearing replacement); a corrupt registry at `Supervisor::new` is a
  hard startup failure (misrouting silently is worse).
- The `SpawnSpec` takes the record's `model`; the objective becomes
  `preamble + contract.objective`; the record's denylist entries merge
  (dedup) with policy-compiled constraints — **deny-only, additive**: a
  record can never widen what policy compiled.
- `session.spawn` payloads now carry `model` and `objectivePreview`
  (first 2 000 chars — preambles included), making routing auditable.
- Disabled agents do not route (`resolve` is enabled-only) — the static
  default serves the spawn.

## 6. Orchestrator integration

- `PlanPolicy::for_pools(pools, reviewer_pool)` builds a policy over the
  enabled roster's ids (the daemon composes this from `enabled_roster()`; the
  orchestrator crate stays registry-agnostic).
- `PlanSnapshot::with_roster(Vec<RosterEntry>)` renders a **WORKER ROSTER**
  prompt section (id — name [adapter, model] — description) *and* carries it
  in the snapshot's `policies.workerRoster`: descriptions are routing signal,
  ids alone are not.
- `ClaudePlanningModel::with_skill_preamble(Option<String>)` prepends skill
  text to every planning prompt — the intended use is the registry's
  `mastermind-commands` body, loaded by the caller that wires the daemon to
  the orchestrator (the daemon↔orchestrator wiring itself remains the
  documented next slice).

## 7. Seeded built-ins (user decisions, 2026-08-22)

| Agent | Adapter | Model | Mode | Skills | Notes |
|---|---|---|---|---|---|
| `orchestrator` | claude-code | `claude-opus-5` | plan | `mastermind-commands` | F-12's model as a registry citizen. |
| `agent-creator` | antigravity-agy | `claude-sonnet-4-6` | plan | `agent-creation` | Interviews; drafts; never registers. |
| `researcher` | antigravity-agy | `gemini-3.1-pro-high` | plan | `tech-research` | Exact catalog slug from the free `agy models` probe. |
| `ui-designer` | antigravity-agy | `claude-sonnet-4-6` | **accept_edits** | `product-design` | The mastermind design phase (Phases 6–7) as an agent, distilled from the user's `~/.claude/agents/ui-designer.md`: writes `docs/DESIGN.md`, `wireframe/tokens.css` and hi-fi `wireframe/*.html` + `INDEX.md` into the chat workspace. The only built-in with write access (design ships artifacts); 1800s timeout; PRD fit: §6's "Domain Supervisors (Frontend, Backend, QA, Research, etc.)" Level-2 roster and the §"design system" context node (line 914). Sonnet via agy keeps design load off the rate-pressured claude account; the agy real-dir write path is verified (`--add-dir`). |

Built-ins: editable, never deletable; `seed_builtins()` never overwrites an
edited row — an existing `agents.db` picks the designer up on the next daemon
start (insert-if-absent).

## 8. Security posture

- Records and skills are plain local rows surfaced over the loopback-only WS
  API; **no secrets** in either (secrets stay in the F-10 secrets interface —
  documented rule, `SkillRecord`'s docs say so).
- `mode: plan` is the wire default and the deny-only merge rule means an agent
  row can only *narrow* what policy allows.
- Chat sessions inherit the F-00 isolation stance (no `~/.agents` mounting;
  skills are injected text), and the F-05 filesystem semantics (print-mode
  writes virtualize; `--add-dir` is the workspace only).
- The proposal parser is total and side-effect-free; the only path from
  proposal to registry is a human's `registry.agents.create`.

## 9. Verification

`cargo test --workspace` (2026-08-22): agentos-agents 28 (CRUD, guards, seed
idempotency, preamble composition, wire round-trips), agentos-daemon 42 incl.
3 new WS e2e (registry CRUD over WS incl. builtin refusal + `agent.*`
journaling; catalog shape; mock chat session lifecycle through the live
subscription path) and the `#[ignore]`d billable researcher e2e behind
`AGENTOS_AGY_E2E=1`; agentos-runtime 13 e2e incl. registry-driven routing
(model + preamble + denylist merge proven through a full driven run) and the
disabled-agent fallback; agentos-orchestrator 65+8 incl. roster rendering and
skill-preamble ordering. `cargo clippy --workspace --all-targets -- -D
warnings` clean. Frontend: `npm test` 15 green (registry store folding:
bootstrap, chat transcripts, proposals, mutation refetch, startChat mirror),
`npm run build` (tsc + vite) clean.

Known seams (next slices): daemon↔orchestrator wiring (goal → plan →
approve → run, the F-14 candidate), skill authoring UI (skills are creatable
over RPC but the editor ships read-only), `registry.skills.create/update/delete`
methods (table exists, WS arms not exposed), and per-provider write-path
verification of the creator/researcher against live agy (opt-in e2e above).
