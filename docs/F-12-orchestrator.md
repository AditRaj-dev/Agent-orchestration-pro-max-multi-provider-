# F-12 — Master Orchestrator: engine-validated plan commands over claude-opus-5

Status: implemented 2026-08-22 · Crate: `crates/agentos-orchestrator` · Canon: `F-00-CONVENTIONS.md` §3 (event rules) / §4 (provider canon) / §5 (working agreements), `docs/HANDOFF-BUILD.md` §5, `docs/HANDOFF-BUILD-2.md` §2 + §5.3, PRD §6.2/§6.3, **§9 OR-01 (lines 678–708)**, §25 Appendix F (line 2267), Appendix G, `docs/F-06-workflow-engine.md`, `docs/F-03-claude-adapter.md`, `D:\OP\handoff.md` §"SESSION 4 DECISIONS".

## 1. Purpose

F-12 is the level-1 agent of PRD §6.1: a stronger reasoning model decomposes a
goal, routes work to pools, requests reviews, escalates exceptions and closes the
goal — **without being able to bypass the deterministic engine**.

The whole crate exists to hold two PRD sentences true:

| PRD | Line | How F-12 holds it |
|---|---|---|
| "It emits structured plan operations validated by workflow engine schemas." | 696 | The model's only vocabulary is six `PlanOperation`s; nothing else parses. |
| "All state mutations are commands validated by deterministic engine, never arbitrary direct DB writes." | 698 | `PlanSink` is the only door to durable state, and it has no "write a task row" method. |
| "Invalid orchestrator commands are rejected with machine-readable reason." | 706 | 20 typed `RejectionReason` codes, serde-tagged, each with a self-correction `hint()`. |
| "A run can continue deterministically if orchestrator is temporarily unavailable for already-planned tasks." | 708 | The orchestrator holds no lease, executes nothing, gates nothing. Model/engine failures are *reported* in `PlanCycleReport`, never returned as `Err`. Proven by three e2e tests. |
| "Orchestrator authority: structured commands validated by engine — no reason to relax; safety invariant." | 2267 | DAG legality is never re-implemented here; `agentos_workflow::validate` decides and its `ValidationError` is carried back verbatim. |

Shape is the mastermind three-tier pattern (HANDOFF-BUILD-2 §2, binding):
**opus-5 commands and never writes code → cheap worker pools code → sonnet-class
pools review**, with the user gating the phase transition.

## 2. Module map

| Module | Role |
|---|---|
| `src/operation.rs` | The six `PlanOperation`s as serde data (`{"op":"create_task","nodeId":…}` — internally-tagged snake_case discriminator, camelCase fields per F-00 §3). Payload structs are `deny_unknown_fields`. Node-id alphabet + length rules. |
| `src/parse.rs` | **Total** model-output parsing. Fenced blocks, bare arrays, `{"operations":[…]}` envelopes, single objects, JSON lines. Never errors, never panics, never silently drops a JSON value. |
| `src/plan.rs` | The draft `Plan`: nodes, decision ledger, escalations, priorities, closed/committed gates. `apply()` is all-or-nothing per operation; DAG checks delegate to the engine. |
| `src/sink.rs` | `PlanSink` — the only door to durable state: `commit` (validate + materialize), `run_view` (read back), `escalate` (raise priority). Implemented for `WorkflowSink` (store only) and for `WorkflowEngine` itself. |
| `src/snapshot.rs` | The OR-01 compact state snapshot (line 694) and the prompt rendered from it. |
| `src/model.rs` | `PlanningModel` boundary, `ClaudePlanningModel` (opus-5 over F-03), `ScriptedPlanningModel` (credential-free double). |
| `src/orchestrator.rs` | The bounded cycle: refresh → snapshot → propose → parse → apply → report. |
| `src/error.rs` | `RejectionReason` (data: a refused proposal) vs `OrchestratorError` (machinery: storage/engine/model). |

## 3. Contracts

### 3.1 The operation vocabulary (PRD line 702)

```json
{"op":"create_task","nodeId":"impl-api","nodeType":"run","dependsOn":["spec"],
 "pool":"backend","objective":"…","priority":"p1","budgets":{…},"retry":{…}}
{"op":"add_dependency","nodeId":"review-api","dependsOn":"impl-ui"}
{"op":"assign_pool","nodeId":"impl-ui","pool":"frontend"}
{"op":"request_review","nodeId":"impl-api","reviewerPool":"coding_review","reviewNodeId":"…"}
{"op":"escalate","nodeId":"impl-ui","target":"supervisor|stronger_agent|human","reason":"…"}
{"op":"close_goal","summary":"…"}
```

`nodeType` is F-06's `NodeType` verbatim (`run`/`parallel`/`review`/`git_gate`/
`human_approval`/`branch`/`{"loop":{"maxIterations":n}}`); `budgets`/`retry` are
F-06's `Budgets`/`RetryPolicy`; `priority` is core's `Priority`. The orchestrator
defines **no parallel vocabulary** — its operations compile into exactly the
engine's own types.

`create_task` and `request_review` add nodes; `add_dependency` adds one edge;
`assign_pool` sets `NodeSpec::agent_role`; `request_review` inserts a
`NodeType::Review` node depending on its target (default id `review-<nodeId>`,
default pool `coding_review`); `escalate` records a decision and raises priority;
`close_goal` seals the plan.

`escalate` and `close_goal` are the only operations legal on a live run
(`PlanOperation::is_structural()` is the split).

### 3.2 Rejection taxonomy (PRD line 706)

`RejectionReason` serializes as `{"code": "<snake_case>", …fields}` — the same
shape `agentos-workflow` uses for `ValidationError`, so a consumer branches on
`code` instead of parsing prose. Every variant carries the fields needed to fix
the problem, plus `hint()`, a corrective instruction replayed into the next
prompt.

| Group | Codes |
|---|---|
| Shape | `unparseable_output`, `not_an_object`, `missing_op`, `unknown_operation`, `malformed_operation`, `invalid_field` |
| Plan semantics | `duplicate_node`, `unknown_node`, `unknown_pool`, `duplicate_dependency`, `self_dependency` |
| Bounds | `plan_too_large`, `operation_budget_exceeded` |
| Engine authority | `run_already_started`, `goal_closed`, `nothing_to_close`, `run_not_terminal`, `empty_plan`, `spec_invalid`, `engine_rejected` |

`spec_invalid` **nests the workflow engine's own `ValidationError`**:

```json
{"code":"spec_invalid","validation":{"code":"cycle_detected","cycle":["a","b","a"]}}
```

`OrchestratorError` is the separate, thiserror-typed machinery taxonomy
(`Workflow` / `Storage` / `Model` / `NoRun` / `Rejected`) with `is_retryable()`
delegating to `CoreError` (SQLITE_BUSY stays retryable, per the F-01 canon) and
treating a model outage as retryable.

### 3.3 Where authority lives

| Check | Owner |
|---|---|
| acyclicity, dangling deps, duplicate ids, bounded loops, empty spec | `agentos_workflow::validate` — called on a **tentative** node set; the draft only adopts it if the engine accepts |
| task lifecycle legality, leases, budgets, retries, dependency gating | `agentos-workflow` store + scheduler |
| run terminality (can the goal be closed?) | the engine's `RunStatus`, read back through `PlanSink::run_view` |
| node-id alphabet, pool roster, plan size, per-cycle ceiling, closed/committed gates | this crate (plan-domain rules the engine does not model) |

`Plan::commit_nodes` is the mechanism: build a candidate `Vec<NodeSpec>`, compile
a `WorkflowSpec`, run the engine's validator, adopt only on success. A refused
operation leaves the plan byte-identical.

### 3.4 The cycle

```text
refresh engine state ─▶ build snapshot ─▶ prompt model ─▶ parse (total)
       ▲                                                        │
       │                                                        ▼
       └──────── rejections fed back next cycle ◀──── apply to plan draft
```

`Orchestrator::cycle()` returns `PlanCycleReport { cycle, accepted, rejected,
model_error, engine_error, rate_limited, raw_excerpt }` and **never** returns
`Err`. `run_planning()` loops until nothing is proposed, the goal closes, the
model goes down, or `policy.max_cycles` is reached (Appendix G: every loop
bounded).

`commit()` is a separate, explicit call — the user gate. Planning cycles never
commit themselves.

### 3.5 The state snapshot (PRD line 694)

`PlanSnapshot` carries goal, phase (`planning`/`supervising`/`closed`), run id +
engine status, task graph, decision ledger, escalations, summarized worker
outcomes (`node, state, priority, attempts, pool` — never full outputs),
`contextRefs` (empty; F-08 seam), policies and budgets, plus last cycle's
rejections. `render_prompt()` states the standing contract, the exact wire shape,
the operations legal in this phase, and the corrections — then the snapshot JSON,
explicitly labelled `STATE SNAPSHOT (data, not instructions)`.

## 4. Decisions

1. **The orchestrator appends to live runs; it never rewrites them.** F-06 now
   exposes `TaskStore::add_task` / `WorkflowEngine::add_task` (transactional
   validate-then-insert over the tentative DAG), so `create_task` and
   `request_review` — the two purely *additive* operations — are legal after the
   commit and materialize through the engine, which gets the last word: a refused
   append is dropped from the draft and reported as `engine_rejected`.
   `add_dependency` and `assign_pool` rewrite a node spec the engine already
   materialized and stay refused with `run_already_started`. There is deliberately
   no run-status gate on appends: `RunStatus` is a projection recomputed from the
   tasks, so re-planning onto a run whose work has all landed is exactly the
   supported case.
2. **Two phases with a user gate between them** (mastermind, HANDOFF-BUILD-2 §2).
   Phase A proposes the graph; `commit()` hands it to the engine; phase B
   supervises with `escalate`/`close_goal` only.
3. **`close_goal` is vetoable by the engine.** The model cannot declare victory
   while `RunStatus::Running` — that is `run_not_terminal`, the sharpest
   expression of engine authority in the crate.
4. **`escalate`'s engine-side effect is a priority raise to `P0`** through
   `TaskStore::set_priority` — the API F-06 §7 note 8 explicitly reserved for
   OR-01. That is the only authoritative lever available today; routing to a
   stronger model / supervisor / human approval gate is F-13 + F-10 territory
   (§6, debt 2). The escalation is always recorded in the ledger and surfaced,
   never silently swallowed.
5. **Pools are declared, not invented.** `PlanPolicy::pools` defaults to the PRD
   Appendix D roster (`frontend`, `backend`, `coding_review`, line 2246);
   routing to anything else is `unknown_pool` with the roster attached.
6. **Manual tag-splitting instead of serde's enum deserializer.** `op` is read by
   hand so *not an object*, *unknown operation* and *malformed payload* stay three
   distinct rejection codes. Payload structs carry `deny_unknown_fields`: a
   misspelled key (`node_id` for `nodeId`, `poool` for `pool`) is a correctable
   error, never a silent drop of intent.
7. **An explicit `[]` is a valid answer.** The parser counts decoded *documents*
   separately from expanded operations, so "I propose nothing" is distinguishable
   from garbled output. Without this the planning loop can never terminate
   cleanly.
8. **Echoed model text is bounded** at `EXCERPT_MAX_CHARS = 240` everywhere it
   re-enters a prompt, a log line or the ledger (F-00 §3: large payloads never
   travel inline).
9. **The model is behind a one-method trait.** `PlanningModel::propose(prompt) ->
   text`. The model can therefore never reach the store, and every test runs
   without a provider.

## 5. Integration notes & discovered constraints

1. **`SpawnSpec` has no permission-mode field.** F-03 pins `acceptEdits`
   (documented F-03 decision), so the orchestrator's "never writes code" posture
   is expressed *only* through `tool_denylist`, rendered as the F-00 §4 canon
   equals form `--disallowedTools=Bash,WebFetch,WebSearch,Write,…`. Asserted by
   `the_spec_renders_the_observed_equals_form_claude_argv`.
2. **Only three denylist tokens are observed-verified.** Probe T2 verified
   `Bash`, `WebFetch`, `WebSearch` (zero tool uses, graceful exit 0). The
   write-tool names in `ORCHESTRATOR_TOOL_DENYLIST` (`Write`, `Edit`,
   `MultiEdit`, `NotebookEdit`) are *unverified on this install* — an unknown
   token is a silent no-op, not an error. Smoke-test on adapter upgrades.
3. **`SessionHandle::events()` has no replay, and F-02 adapters spawn their
   driving task inside `start_session`.** A late subscriber can miss early
   events. `ClaudePlanningModel` therefore reads the final text from the terminal
   `Finished { final_result }` (which arrives after process exit) and treats
   accumulated `TextDelta`s only as a fallback.
4. **Rate-limit pressure is a signal, not a failure.** `AdapterEvent::RateLimit`
   sets `ModelResponse::rate_limited` / `PlanCycleReport::rate_limited` rather
   than failing the turn — the observed claude behavior at seven-day utilization
   0.86 (handoff §"SESSION 4"). Backoff policy is the caller's.
5. **Model pin.** `claude-opus-5`; alias `opus` accepted; `opus-5` 404s (F-00 §4).
6. **`create_run` stamps every task `P2`.** `PlanSink::commit` applies the plan's
   per-node priorities afterwards through `set_priority`, which is why priority is
   a plan annotation rather than a `NodeSpec` field.
7. **`WorkflowEngine` implements `PlanSink` directly** (local trait, foreign
   type — orphan rules allow it), so commits go through `start_run` verbatim and
   the e2e tests exercise the real engine, not a stand-in.
8. **No workspace changes were needed.** The crate builds on the deps already
   pre-wired in its `Cargo.toml`; root `Cargo.toml`, `Cargo.lock` and every other
   crate are untouched.

## 6. Known debts / seams

1. ~~**Re-planning is new-run granularity.**~~ **CLOSED** — F-06 grew
   `add_task`, and live appends go through it (§decision 1). What remains
   unsupported is *rewriting* a materialized node (`add_dependency`,
   `assign_pool` post-commit): the durable store has no node-spec update, and a
   spec swap under a leased task would race the executor. Re-shaping an existing
   node still means a new run.
2. **Escalation routing is a priority raise only.** `EscalationTarget::Human`
   should resolve through F-10's approval store and
   `EscalationTarget::StrongerAgent` through a capability-tiered pool (F-13).
   Today all three targets raise `P0` and record the decision.
3. **`contextRefs` in the snapshot is empty** — F-08's compiled context packs are
   not wired into the prompt yet. The field exists so the wire shape does not
   change when they are.
4. **Domain supervisors (OR-02) are not modelled.** The plan is flat: orchestrator
   → pools. `EscalationTarget::Supervisor` is vocabulary awaiting F-13.
5. **No journal emission.** F-12 logs via `tracing` (accepted/rejected operations,
   commits, escalations); writing `EventType::*` records into the daemon's
   append-only journal belongs to the embedder (same split as F-06 §7 note 4).
6. **Budgets are node-count/cycle-count only.** Token and USD ceilings for the
   *orchestrator's own* turns are not enforced here; `ModelResponse::usage` is
   surfaced for the F-07 usage ledger to consume.
7. **The live-CLI probe is discovery-only.** A real planning turn is billable, so
   the suite never runs one. `AGENTOS_ORCHESTRATOR_E2E=1` runs `detect` +
   auth-file existence + capability checks (free, per F-00 §5).

## 7. Test inventory (fresh output, 2026-08-22)

`CARGO_TARGET_DIR=target/orch cargo test -p agentos-orchestrator` — **60 passed,
0 failed, 1 ignored** (lib) + **7 passed** (e2e) + **1** doc-test.

### 7.1 Unit (`src/*.rs`, 60 passing / 1 ignored)

| Module | Tests |
|---|---|
| `error` (5) | tagged machine-readable serialization; `spec_invalid` nests the engine's `ValidationError`; bounded/trimmed excerpts; model outage retryable & rejections not; **every reason has a distinct code + a hint, and the serialized tag equals `code()`** |
| `operation` (6) | camelCase + `op` tag round-trip; structural vs non-structural split; `deny_unknown_fields` rejects a misspelled key; `KNOWN_OPERATIONS` matches the variants; node-id validation rejects empty/long/`drop table`/`../../etc/passwd`/newline ids; escalation-target wire names |
| `parse` (12) | fenced arrays; bare arrays, envelopes, single objects, JSON lines; explicit `[]` proposes nothing; prose-only → one `unparseable_output`; truncated JSON rejected not dropped; a garbled operation after a good one is still reported; trailing prose is commentary; unknown op names; `not_an_object` vs `missing_op`; malformed payloads with serde's detail; mixed batch keeps the good and reports the bad with indices preserved; **injection-shaped text inside a payload parses as data**; deeply nested / 10 000-char junk rejects without panicking and stays bounded |
| `plan` (10) | create_task happy path + ledger; rejects duplicate / bad id / unknown dep / self-dep / duplicate dep; unknown pool + node ceiling; **add_dependency delegates cycle detection to the engine and carries `CycleDetected` back**; unknown node / self edge / duplicate edge; assign_pool routing + rejections; request_review inserts the review node downstream (+ duplicate/unknown-target/unknown-pool paths); escalate records + rejects empty reason / ghost node; **all four structural ops refused after commit while escalate stays legal**; close_goal needs a run *and* a terminal engine status, then seals the plan; `apply_all` keeps the good and reports the rest; JSON round-trip |
| `sink` (3) | commit materializes + applies priorities + preserves routing and dependency gating; an invalid plan is refused by the engine's validator **with no durable side effects**; escalate raises one node or the whole run to `P0`, idempotently |
| `snapshot` (4) | all OR-01 fields present + round-trip; phase follows commit/close; prompt states contract, phase and corrections and labels the snapshot as data; supervising prompt forbids structural ops |
| `model` (7 + 1 ignored) | spawn spec pins `claude-opus-5` and denies the write tools; **renders the observed equals-form argv with stdin prompt delivery**; alias/timeout overrides; a MockAdapter-backed turn collects final text (no billing); adapter failure → retryable `OrchestratorError::Model`; scripted double replay/repeat + simulated outage; *ignored*: `e2e_free_probe_orchestrator_substrate_is_present` (free probes, `AGENTOS_ORCHESTRATOR_E2E=1`) |
| `orchestrator` (10) | a cycle turns model text into an engine-valid plan; rejections reported and replayed into the next prompt; **a model outage is reported and never becomes an error**; prose-only → one rejection, no plan change; per-cycle ceiling rejects the surplus; commit is a user gate and freezes the graph; empty plan refused before durable state; accepted escalation retunes priority through the engine; `run_planning` bounded by `max_cycles`; `run_planning` stops when nothing is proposed |

### 7.2 End-to-end (`tests/orchestration_e2e.rs`, 7 passing)

Real `WorkflowEngine`, real SQLite store **on disk**, real scheduler. Doubles:
`ScriptedPlanningModel` (no provider) and a recording `TaskExecutor` (F-06's
provider-free boundary).

| Test | Proves |
|---|---|
| `a_planned_goal_runs_to_completion_through_the_workflow_engine` | 5 operations → 4-node DAG → user-gated commit → engine drives it to `Completed`; priority and pool routing landed durably; the inserted review node ran after **both** its dependencies (the orchestrator's `add_dependency` took effect in the engine); the goal then closes |
| `the_run_completes_with_no_orchestrator_attached_at_all` | **PRD line 708**: orchestrator + model dropped right after commit; the engine still completes the run |
| `a_model_outage_mid_supervision_does_not_stall_the_run` | a mid-run proposer failure is reported, not raised, and the run still completes |
| `the_engine_vetoes_closing_a_goal_whose_run_is_still_working` | `close_goal` → `run_not_terminal` while running; the identical proposal is accepted once the engine finishes |
| `a_rejected_cycle_feeds_the_correction_into_the_next_proposal` | `unknown_pool` + `unknown_node` rejected, plan still a valid DAG, corrections appear in the next prompt, cycle 2 fixes both and the run completes |
| `an_escalation_on_a_live_run_retunes_priority_through_the_engine` | escalation raises the durable row to `P0` **without** jumping the lifecycle (task stays `Planned`, still dependency-gated) |
| `a_committed_plan_survives_a_restart_of_everything` | orchestrator, model, engine and store all dropped; a fresh engine over the same db file finishes the run |

### 7.3 Command output

```
running 61 tests
...
test result: ok. 60 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.02s

     Running tests\orchestration_e2e.rs
running 7 tests
test an_escalation_on_a_live_run_retunes_priority_through_the_engine ... ok
test a_rejected_cycle_feeds_the_correction_into_the_next_proposal ... ok
test a_planned_goal_runs_to_completion_through_the_workflow_engine ... ok
test the_run_completes_with_no_orchestrator_attached_at_all ... ok
test the_engine_vetoes_closing_a_goal_whose_run_is_still_working ... ok
test a_model_outage_mid_supervision_does_not_stall_the_run ... ok
test a_committed_plan_survives_a_restart_of_everything ... ok
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.14s

   Doc-tests agentos_orchestrator
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s
```

```
$ CARGO_TARGET_DIR=target/orch cargo clippy -p agentos-orchestrator --all-targets -- -D warnings
    Checking agentos-orchestrator v0.1.0 (D:\OP\agent-engineering-os\crates\agentos-orchestrator)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 2.68s
```

`cargo fmt -p agentos-orchestrator -- --check`: clean.

**Billing:** zero billable calls in the suite. Every planning turn is either a
scripted string or the credential-free F-02 `MockAdapter`; the only live-binary
test is `#[ignore]`d, env-gated, and runs discovery probes only.
