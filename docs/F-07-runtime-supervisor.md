# F-07 — Runtime supervisor & composition

Status: implemented (this PR). Scope: `crates/agentos-runtime` only — typed handoff
packets, full task contracts, the per-run usage ledger, and the supervisor that makes
MockAdapter → workflow engine → git mutation queue → agent ledger → event journal run
as one loop.

Binding sources: `F-00-CONVENTIONS.md` §3 (event rules), PRD §6.2 (task lifecycle),
§9 (OR-03/04/05/08), §11 (HO-01 typed handoffs, HO-03 queues), §12 (GIT-01..05),
§25 Appendices B/C (canonical contract and handoff JSON) and E (event sequence).

## 1. Design

The supervisor is a *composition root*, not a second scheduler. All scheduling,
leasing, retry and budget-attempt semantics stay in `agentos-workflow` (F-06); all
git semantics stay in `agentos-git` (F-09); the journal stays in `agentos-daemon`
(F-01). `Supervisor` wires them together and owns everything the engine's
`TaskExecutor` boundary needs:

| Member | Role |
|---|---|
| `Journal` (agentos-daemon `events` schema, held by path) | append-only event journal |
| `WorkflowEngine` over `TaskStore` | durable runs/tasks, leases, retries |
| `Vec<Arc<dyn RuntimeAdapter>>` | adapter registry (`agentos-adapters`) |
| `WorktreeManager` | per-task worktree isolation (GIT-04) |
| `MutationQueue` | serialized git mutations (GIT-02/HO-03) |
| `AgentLedger` | commit attribution (GIT-03) |
| `OwnershipMap` | exclusive path holds (GIT-05) |
| `UsageLedger` | per-task usage rows + budget verdicts (this crate) |

`Supervisor::new(config, adapters)` builds all of it; `start_run(spec, goal,
contracts)` validates contracts, materializes the durable run and persists a
**run manifest** (`<state_dir>/runs/<run_id>.json`: run/trace ids + the full
contract per node); `drive(run_id, max_ticks)` is the bounded loop — one engine
`tick` per iteration, then a post-tick journal projection and a cost-escalation
scan, until the run is terminal or a pass does no work.

The executor (`SupervisorExecutor`, an `Arc<dyn TaskExecutor>` moved into the
engine) shares a `SupervisorCore` with the supervisor, which is why the journal
is held **by path** (each append opens a connection): the engine's executor runs
inside `tick` awaits, and a stored `rusqlite::Connection` would have to cross
them. WAL + the F-01 busy canon make per-append opens safe.

### Wiring diagram

```text
                       Supervisor::start_run(spec, goal, contracts)
                         ├─ validate contracts (per node type) + base_commit
                         ├─ engine.start_run  ──────────► TaskStore (workflow.db)
                         ├─ persist run manifest ───────► state/runs/<run>.json
                         └─ journal: run.created, workflow.started, task.created[, task.ready]
                                                    │
   ┌────────────────────────────────────────────────┘
   ▼  Supervisor::drive(run_id)  (per tick: engine.tick() → post-tick projection)
┌────────────────────────── WorkflowEngine.tick (F-06) ─────────────────────────┐
│ 1 reclaim expired leases (crash recovery)   2 promote deps-satisfied tasks    │
│ 3 budget gate + grant leases                4 CAS Leased→Running, spawn        │
│   SupervisorExecutor.run(task)                 executors, await all           │
│ 5 record outcomes via CAS (requeue / fail)   6 refresh run statuses            │
└───────────────────────────────────┬───────────────────────────────────────────┘
                                    ▼ per leased task, by node type
  Run / Branch / Loop ──►  budget pre-check ─► ownership.acquire(allowed_paths,
                          exclusive) ─► worktree at base_commit ─► adapter
                          (by agent_role, mock default) ─► SpawnSpec ─► session
                          stream: journal relay + UsageLedger.consume + heartbeat
                          ─► Finished → HandoffPacket.validate() ─► content-
                          addressed artifact + task.output_ready ─► release holds
  Parallel ─────────────►  sync point: Success (engine's dependency gate is the
                          semantics)
  Review ───────────────►  stub reviewer (see seams): review.requested →
                          review.approved / review.failed
  GitGate ──────────────►  MutationQueue.enqueue(Commit, approved=true) →
                          git.queued → claim_next (single consumer) → stale-base
                          gate → commit audit bundle on gate branch →
                          queue.complete(sha) → AgentLedger.record(sha → task,
                          agent, reviewers, context versions, workflow) →
                          git.committed
  HumanApproval ────────►  approval.required + failure (F-10 seam; never
                          self-approves)
                                    ▼ after each tick
  post_tick: task.ready / task.done / task.failed diff events ─► cost-escalation
  scan (Ready + spend ≥ max_cost_usd → budget.exceeded + CAS Failed + park
  dependents) ─► terminal: run.completed / run.failed
```

## 2. Schemas

### 2.1 Full `TaskContract` (PRD §25 Appendix B, camelCase wire)

Fields: `version` (default 0 = legacy/unversioned; builders stamp 1), `id`,
`objective`, `allowedPaths[]`, `forbiddenPaths[]` (default), `dependencies[]`
(default), `contextRefs[]` (default), `acceptanceCriteria[]` (default),
`requiredChecks[]` (default), `budgets {maxMinutes, maxAttempts}`,
`baseCommit`, `expectedArtifacts[]` (default), `gitPolicy`
(`"no-direct-git"` | `"direct-allowed"`). Defaults exist exactly for the fields
the Appendix B example omits; everything the example carries is required.

Validation (`validate` / `validate_for(node, type)`): non-empty id, objective,
base_commit; budgets `>= 1` on both legs; no empty path entries; and — for
write-capable node types (`run`, `branch`, `loop`) — `allowedPaths` must be
non-empty. Review/gate/parallel nodes may keep it empty.

**Immutable-after-lease:** when the executor is invoked it clones the manifest's
contract once and everything downstream (SpawnSpec, review input, git gate) reads
that snapshot. `TaskContract::amend(change)` produces a *new* value with
`version + 1` after re-validation; the original is never mutated. The supervisor
exposes no API that amends a running run — orchestrator-driven amendment wired
into re-lease is the F-10+ seam.

### 2.2 `HandoffPacket` (PRD §25 Appendix C, camelCase wire)

Fields: `id` (generated on parse when absent), `fromAgent`, `taskId`, `status`
(closed set: `completed`/`output_ready`/`blocked`/`failed`), `summary`,
`filesChanged[]`, `artifacts[]` (default), `contextRefs[]`, `decisions[]`,
`tests[]` (`{name, status: passed|failed|skipped, count?}`), `unresolved[]`,
`requestedAction` (closed set: `review`/`merge`/`escalate`/`none`),
`transcriptRef` (optional handle).

HO-01 enforcement in `validate()` (+ serde):

- required fields must be present and non-empty (missing `summary`/`taskId`/
  `fromAgent`/`status`/`requestedAction` fail to parse);
- statuses/actions are closed sets — unknown strings fail to parse;
- artifacts are references only: `ArtifactRef` is `deny_unknown_fields` (a
  `content`/`data` key fails deserialization) and validation rejects ref strings
  that look like inline payloads (`data:` URIs, control chars/newlines, length
  > 512);
- transcripts are on-demand only: the packet has **no** content field — just the
  optional `transcriptRef` handle (`adapter-session:<id>`), held to the same
  reference-shape rules.

The mock adapter's `Finished` structured packet maps into a handoff via
`HandoffPacket::from_adapter_finish` (tolerant JSON extraction; `tests` accepts
a single object or an array).

### 2.3 Storage layout under `state_dir/`

```
runs/<run_id>.json        run manifest (trace id + full contracts per node)
handoffs/<task_id>.json   accepted handoff packets (task-keyed lookup)
artifacts/<sha256>.json   content-addressed packet artifacts (the journal's
                          payload_ref resolves here)
journal.db                agentos-daemon event journal
workflow.db               agentos-workflow task store
queue.db / ledger.db      agentos-git mutation queue / agent ledger
```

All of it is reopen-able: a reconstructed supervisor continues runs started by
its predecessor. `payload_ref`/`payload_hash` on `task.output_ready` are
`sha256:<hex>` computed by a dependency-free SHA-256 (`src/digest.rs`; the
crate's manifest was frozen without a hash dependency — verified against the
standard vectors).

## 3. Event mapping (adapter/journal)

Every event carries `run_id` + `trace_id` (one trace id per run; asserted in the
E2E test) and a small inline payload; the handoff packet itself is offloaded
(ref + hash) per F-00 §3.

| Source | Journal event | Notes |
|---|---|---|
| `start_run` | `run.created`, `workflow.started`, `task.created` × N, `task.ready` (initially-ready) | Appendix E opening |
| executor entry | `agent.leased`, `task.running` (payload: attempt) | lease owner as agent id |
| `AdapterEvent::Started` | `session.started` | session id + model |
| `AdapterEvent::ToolUse` | `agent.tool_use` | tool + args summary only |
| `AdapterEvent::RateLimit` | `agent.rate_limit` | provider notice verbatim |
| `AdapterEvent::UsageUpdate` | `usage.updated` (payload = snapshot) and `UsageLedger.consume` | cost gate evaluated right after |
| `AdapterEvent::TextDelta` | — (not journaled) | chatty; reachable via `transcript_ref` |
| `Finished` → packet accepted | `task.output_ready` (+ `payload_ref`/`payload_hash`) | packet validated before accept |
| `Finished` → packet invalid | `handoff.rejected` | task fails as `ReasoningFailure` |
| `Failed(SpawnFailure)` | `agent.crashed` | process died pre-model |
| `Failed(other)` | `task.failed` | classified kind + detail |
| ownership conflict | `ownership.conflict` | retryable (`TransientFailure`) |
| session timeout / spend ceiling | `budget.exceeded` | timeout ⇒ TransientFailure; spend ⇒ fail |
| post-tick diffs | `task.ready`, `task.done`, `task.failed` | projection of durable states |
| cost-escalation scan | `budget.exceeded` (`escalated: true`) + CAS to `Failed` | see §4 |
| review node | `review.requested` → `review.approved`/`review.failed` | stub reviewer |
| git gate | `git.queued` → `git.committed` | plus `git.gate_failed` / `git.stale_base` on the error paths |
| drive terminal | `run.completed` / `run.failed` | Appendix E closing |

Extension strings ride `EventType::Other`, which round-trips verbatim
(agentos-core's forward-compat rule).

## 4. Usage ledger & budgets

`UsageLedger` is keyed by task; `consume` folds `UsageSnapshot`s into token
counters, top-line cost (claude `total_cost_usd` canon), **per-model rows**
(`modelUsage` canon, merged by model id) and the **fixed per-session preamble
line** (`session_overhead_tokens`, the ~22k/37k core prompt of F-00 §4).
`run_total` rolls a run up. `status_of(usage, budgets, elapsed)` enforces
`Budgets { max_cost_usd, max_elapsed_secs }` as **Ok / Warn (≥ 80 %) / Exceeded
(≥ ceiling)**, mirroring the scheduler's `>=` semantics; unknown cost never
trips the cost leg (OR-05 "no cost data" rule).

Escalation is the supervisor's job, never silent: the executor checks
pre-session and after every usage update (mid-session breach cancels the
session and fails the attempt); the post-tick scan CASes any `Ready` task whose
spend crossed its ceiling to `Failed`, parks its dependents and emits
`budget.exceeded`. `UsageLedger` implements `agentos_workflow::CostLedger`, so
it is a drop-in scheduler cost source the moment the engine exposes
cost-ledger injection.

## 5. Crash recovery

Leases are durable rows. On restart, `drive`'s first tick runs the engine's
expired-lease reclaim (phase 1): `Leased` ghosts go back to `Ready` with the
attempt consumed; `Running` ghosts take the `Running → Retryable → Ready` walk.
The successor supervisor reloads manifests and handoffs from disk (ownership
holds are attempt-scoped in memory, so a dead supervisor leaves none behind).
Worktrees are reused by identity (see §7) and retained for the GC/retention
rules — runs never delete them.

## 6. Reviewer (deterministic stub)

`review_verdict` approves iff every transitive dependency packet has: no
`unresolved` items, **non-empty** test evidence, and **no failed** test. A
skipped-with-reason test counts as non-failing evidence (the mock adapter
reports `tests: {status: "skipped"}` — treating skips as blockers would make
every credential-free e2e run unreviewable). Empty packet set, unresolved
items and failing tests block. The real reviewer pool replaces this in a later
PR; its output contract is exactly these two events plus the reviewer id that
flows into ledger attribution.

## 7. Integration notes & discovered constraints

- **F-09 branch-name collisions (found by the e2e suite, since FIXED in F-09):**
  `WorktreeManager` used to derive branch names from the *first 8 hex* of
  run/task UUIDs, but the engine mints every task UUIDv7 of a run in one
  millisecond burst, so those prefixes — and thus `agentos/<run8>/<task8>`
  branches — collided and `git worktree add -b` failed. F-09 now derives both
  shorts from the UUID **tail** (the random leg); F-07's byte-swap shim is
  removed and the task id is used directly as the worktree key.
- **Journal by path:** `agentos-daemon` returns `rusqlite::Connection` without
  re-exporting rusqlite, and F-07's manifest has no rusqlite dependency; the
  journal therefore opens per append. Cheap at this scale and avoids smuggling
  a non-`Sync` connection across engine awaits.
- **Engine cost-gate seam:** `WorkflowEngine::new` constructs its scheduler
  with `NoCostLedger` and offers no injection point, so the OR-08 cost leg
  cannot fire engine-side yet; F-07 enforces cost at its own layer (executor +
  post-tick scan). Wiring `UsageLedger` into the scheduler is a one-liner once
  the engine takes a cost source.
- **HumanApproval nodes fail loudly** (`approval.required` + failure) rather
  than self-approving — the human-gate plumbing is F-10's.

## 8. Seam register

| Seam | Lands with |
|---|---|
| Git-gate approval (`approved=true` at enqueue; push still queue-gated) | F-10 approval plumbing |
| Deterministic stub reviewer | later PR (reviewer pool) |
| Cross-stage ownership holds (until commit, not per attempt) | F-10+ orchestration |
| Contract amendments re-leased mid-run | F-10+ orchestrator |
| Engine-side cost gate (`CostLedger` injection) | agentos-workflow follow-up |
| Worktree GC | F-09 retention/GC path |
| Real CLI adapters behind the same registry | F-03..F-05 runtime wiring |

## 9. Tests & evidence

`crates/agentos-runtime`: 30 unit tests (contract Appendix-B round-trip +
validation + amend versioning; handoff Appendix-C round-trip + missing-field /
inline-blob / closed-set rejections + mock-packet mapping; usage-ledger
accumulation + Ok/Warn/Exceeded ladders + `CostLedger` hook; SHA-256 vectors;
journal round-trip; stub-review verdict; base-commit verification) and 5 e2e
tests against a real temp git repo (`tests/e2e.rs`):

- `e2e_happy_path_spec_parallel_review_gitgate` — 6-node workflow
  (run → parallel[run, run] → review → git_gate) driven to `Completed`;
  asserts validated handoff packets for both parallel branches (+ the spec
  node's), git request `Done` with the sha, ledger attribution
  (task/agent/reviewers/context versions/workflow), the Appendix E sequence as
  an ordered subsequence, one trace id on every event, the content-addressed
  artifact behind `task.output_ready`, usage roll-up (≥ 3 × 22k overhead), and
  retained worktrees.
- `e2e_flaky_transient_failure_retries_to_completion` — `FlakyThenSuccess`:
  transient failure journaled (`agent.rate_limit`, `task.failed`), retried
  within budget, run completes.
- `e2e_cost_budget_exceeded_escalates_to_failed` — $0.02 session vs $0.01
  ceiling: `budget.exceeded` journaled, task `Failed`, dependent `Blocked`,
  run `Failed`.
- `e2e_crash_recovery_reclaims_expired_lease_and_completes` — drive one tick,
  ghost-lease a branch with a zero-ttl lease, drop the supervisor, reconstruct
  over the same dbs: engine reclaims (attempt consumed exactly once), run
  completes, pre-crash packets remain readable, journal spans both
  supervisors.
- `e2e_ownership_conflict_is_journaled_and_retried` — two concurrent runs on
  one path scope: `ownership.conflict` journaled, conflicted attempt retried
  after release, run completes.

Fresh verification (Windows reference platform, `CARGO_TARGET_DIR=target/f07`):

```text
$ cargo test -p agentos-runtime
test result: ok. 30 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.34s

     Running tests\e2e.rs (target/f07\debug\deps\e2e-da9ed877259633eb.exe)

running 5 tests
test e2e_cost_budget_exceeded_escalates_to_failed ... ok
test e2e_ownership_conflict_is_journaled_and_retried ... ok
test e2e_happy_path_spec_parallel_review_gitgate ... ok
test e2e_crash_recovery_reclaims_expired_lease_and_completes ... ok
test e2e_flaky_transient_failure_retries_to_completion ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.26s

$ cargo clippy -p agentos-runtime --all-targets -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.24s

$ cargo fmt -p agentos-runtime -- --check   # clean
```

No new dependencies: `agentos-runtime` builds on the five sibling crates plus
the manifest it was scaffolded with (`tempfile` as the only dev-dependency).
