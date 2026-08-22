# F-07 — Runtime supervisor & composition

Status: implemented; **F-10 wired in** (policy PR). Scope: `crates/agentos-runtime`
only — typed handoff packets, full task contracts, the per-run usage ledger, and the
supervisor that makes MockAdapter → workflow engine → **policy gate** → git mutation
queue → agent ledger → event journal run as one loop.

Binding sources: `F-00-CONVENTIONS.md` §3 (event rules), PRD §6.2 (task lifecycle),
§9 (OR-03/04/05/08), §11 (HO-01 typed handoffs, HO-03 queues), §12 (GIT-01..05),
§14 (SEC-01/04/05 via F-10), §23.3 (MVP release gates),
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
| `PolicyGate` (`src/policy.rs`, over agentos-policy) | approval store + audit log + per-task approval-request records (F-10) |

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
                          (by agent_role, mock default) ─► PermissionSet
                          (role map | derived from contract) ─► compile_to_
                          spawn_spec → SpawnConstraints (fail closed) ─►
                          SpawnSpec ─► session stream: journal relay +
                          UsageLedger.consume + heartbeat ─► Finished →
                          HandoffPacket.validate() ─► content-addressed
                          artifact + task.output_ready ─► release holds
  Parallel ─────────────►  sync point: Success (engine's dependency gate is the
                          semantics)
  Review ───────────────►  stub reviewer (see seams): review.requested →
                          review.approved / review.failed
  GitGate ──────────────►  ApprovalStore.is_approved(git gate, operation
                          fingerprint) + git_gate_check(gate role, Commit) →
                          denied ⇒ policy.denied + git.gate_failed + audit,
                          NOTHING created; approved ⇒ approval.granted →
                          MutationQueue.enqueue(Commit, approved=<verdict>) →
                          git.queued → claim_next (single consumer) → stale-base
                          gate → commit audit bundle on gate branch →
                          queue.complete(sha) → AgentLedger.record(sha → task,
                          agent, reviewers, context versions, workflow) →
                          git.committed
  HumanApproval ────────►  ApprovalStore verdict: approved ⇒ approval.granted +
                          Success; pending/missing ⇒ approval.required +
                          AwaitingApproval (parks in HumanRequired, no
                          attempt consumed); denied/expired/mutated ⇒
                          approval.denied + ReasoningFailure
                          (never self-approves)
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
into re-lease is the orchestrator seam. (An amendment *does* invalidate a
`HumanApproval` node's approval: the contract id/version/objective/base commit
are part of the approved operation, §5.2.)

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
approvals/<task_id>.json  which approval request covers this task's gate
                          operation (a POINTER, never an authorization)
journal.db                agentos-daemon event journal
workflow.db               agentos-workflow task store
queue.db / ledger.db      agentos-git mutation queue / agent ledger
approvals.db / audit.db   agentos-policy approval store / audit log
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
| policy gate satisfied | `approval.granted` | gate + operation fingerprint |
| policy gate open | `approval.required` | gate + request id + fingerprint + expiry |
| policy gate refused | `approval.denied` (human node) / `policy.denied` (git gate, compile) | reason ∈ `pending`/`denied`/`expired`/`operation_mutated`/`no_approval_requested` |
| git gate | `git.queued` (payload carries `approved`) → `git.committed` | plus `git.gate_failed` / `git.stale_base` on the error paths |
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

## 5. Policy enforcement (F-10 wiring)

`src/policy.rs` is the seam between F-10's decisions and F-07's actions. It
adds no policy logic of its own: permissions, fingerprints, approvals and the
audit log all come from `agentos-policy`. Everything below **fails closed** —
a store error, a missing record, an elapsed ttl, a mutated operation or a
contract that claims more than policy grants all resolve to "not authorized",
never to "proceed".

### 5.1 Permission compilation at spawn (SEC-01)

Before a session starts, the executor resolves the task's `PermissionSet` —
`config.role_permissions[agent_role]` when the node declares a mapped role,
otherwise **derived from the leased contract snapshot**
(`policy::derive_permissions`: `worker_write(allowedPaths)` when the contract
declares write scopes, `worker_read_only()` when it does not; neither grants
git actions, since every mutation belongs to the git manager, GIT-01). Least
privilege is therefore the unconfigured behavior.

`policy::compile_constraints` then refuses the spawn unless policy and
contract agree, and only afterwards calls
`agentos_policy::compile_to_spawn_spec`:

1. every `allowedPaths` glob the contract claims must be covered by a write
   glob of the permission set — a contract may never widen policy;
2. no write glob of the permission set may reach a `forbiddenPaths` glob — a
   role may never write where the contract forbids writes;
3. the compiled `allowed_paths` must be non-empty.

A refusal emits `policy.denied` (`stage: "compile"`), writes a
`permission.denied` audit row and fails the attempt as a `ReasoningFailure`
**before any adapter is started** — no billable work happens on a
misconfigured policy. On success the four compiled fields become the
`SpawnSpec`'s `allowed_paths` / `forbidden_paths` / `tool_allowlist` /
`tool_denylist` (the contract's raw globs are no longer passed through), with
the `gitPolicy: "no-direct-git"` denials (`Bash(git commit:*)`,
`Bash(git push:*)`) unioned on top — deny beats allow. The compiled lists ride
the `session.spawn` event so a run's journal shows exactly what the adapter
was constrained to.

### 5.2 Gate operations and fingerprints (SEC-04)

Gate nodes are authorized against an **operation payload** that is a pure
function of durable state:

| Node | Gate | Operation fields |
|---|---|---|
| `GitGate` | `Gate::GitPush` (see §8) | `kind`, `action`, `repo`, `runId`, `taskId`, `node`, `workflowId`, `baseCommit` |
| `HumanApproval` | `config.human_approval_gate` (default `Gate::ProdAction`) | `kind`, `runId`, `taskId`, `node`, `workflowId`, `contractId`, `contractVersion`, `objective`, `baseCommit` |

Because the payload is deterministic, a human can approve **before** the node
is ever leased and the gate recomputes the identical fingerprint when it runs.
`Supervisor::gate_operation(run_id, node)` exposes it;
`Supervisor::request_gate_approval(run_id, node, requested_by)` opens the
request (audited, journaled as `approval.required`) and a human resolves it
through `Supervisor::approvals()`. Any drift in the mutation — a moved base
commit, a different task, a different action, an amended contract — yields a
different fingerprint, so the old approval authorizes nothing.

`PolicyGate::evaluate` asks `ApprovalStore::is_approved` first (fingerprint +
expiry checked in SQL); the task-keyed record under
`<state_dir>/approvals/<task_id>.json` is only a **pointer** used to explain a
negative answer, never to authorize. It yields one of
`Approved` / `Pending` / `Denied` / `Expired` / `Mutated` / `Missing`.

### 5.3 The git gate (PRD §23.3, proved through the real queue)

Authorization happens **before any side effect** — an unauthorized mutation
creates no worktree, no branch and no queue row:

1. `git_gate_check(git_gate_permissions, Commit, verdict.is_approved())` —
   permission trumps approval: a gate role without the action is denied even
   with a live, exactly-matching approval;
2. the supervisor then tightens F-10's push-only approval leg to **every**
   queued mutation: no live approval ⇒ `PolicyDenial::ApprovalRequired`.
   Everything this gate enqueues lands in the governed repository, so commit
   is gated exactly like push.

`MutationQueue::enqueue(..., approved)` now carries that verdict — the
`approved = true` literal is gone. A denial writes `permission.denied` +
`git.gate` audit rows, emits `policy.denied` + `git.gate_failed`, and (when
the missing piece is an approval rather than a permission) opens a request so
the decision is discoverable instead of lost. An approval-store failure is a
`TransientFailure` (retry), never an approval.

### 5.4 `HumanApproval` nodes

Resolved against the same store, mapped onto the **existing** lifecycle — no
new `TaskState` and no new `Outcome`:

| Verdict | Outcome | Lifecycle effect |
|---|---|---|
| `Approved` | `Success` | `Running → Done`; emits `approval.granted` |
| `Pending` / `Missing` | `AwaitingApproval` | `Running → HumanRequired`: an **open park**, no attempt consumed, lease released; the engine will not re-run it until someone CASes it out (`→ Ready` resume, `→ Approved` sign-off, `→ Failed`/`→ Cancelled` abandon); emits `approval.required` |
| `Denied` / `Expired` / `Mutated` | `ReasoningFailure` | governed by `retry.reasoning_retries` (default 1 ⇒ 2 attempts), then `Failed`; emits `approval.denied` carrying the typed `RuntimeError::PolicyDenied` text |

The node never self-approves, and a failed gate parks its dependents through
the engine's ordinary `block_dependents_of` path.

### 5.5 Audit (SEC-05)

Decisions are recorded through `agentos-policy`'s own helpers —
`record_approval` (requested), `record_permission_denial`, `record_git_gate`
(allowed/denied with the verdict reason) — plus `AuditStore::append` for the
resolved-approval and spawn-refusal rows. `Supervisor::audit()` exposes the
store, so `export_bundle(Some(run_id))` yields the run's policy bundle.
Audit-append failures are logged loudly but never flip a denial into an
approval.

## 6. Crash recovery

Leases are durable rows. On restart, `drive`'s first tick runs the engine's
expired-lease reclaim (phase 1): `Leased` ghosts go back to `Ready` with the
attempt consumed; `Running` ghosts take the `Running → Retryable → Ready` walk.
The successor supervisor reloads manifests and handoffs from disk (ownership
holds are attempt-scoped in memory, so a dead supervisor leaves none behind).
Worktrees are reused by identity (see §8) and retained for the GC/retention
rules — runs never delete them.

## 7. Reviewer (deterministic stub)

`review_verdict` approves iff every transitive dependency packet has: no
`unresolved` items, **non-empty** test evidence, and **no failed** test. A
skipped-with-reason test counts as non-failing evidence (the mock adapter
reports `tests: {status: "skipped"}` — treating skips as blockers would make
every credential-free e2e run unreviewable). Empty packet set, unresolved
items and failing tests block. The real reviewer pool replaces this in a later
PR; its output contract is exactly these two events plus the reviewer id that
flows into ledger attribution.

## 8. Integration notes & discovered constraints

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
- **No `Gate::GitCommit` in F-10 (worked around, not edited):** `Gate` names
  exactly one git gate (`GitPush`). The supervisor binds commit approvals to
  it and puts the `action` in the *operation payload*, so a commit approval
  and a push approval have different fingerprints and never transfer — the
  security property holds, but the gate column reads `git_push` for a commit.
  Adding a `Gate::GitCommit` variant is an `agentos-policy` change and was out
  of this PR's file scope.
- **`git_gate_check` only gates push; the supervisor gates every mutation.**
  F-10's contract is "push requires a live approval"; F-07 tightens it because
  everything its gate enqueues lands in the governed repository. The
  permission leg is F-10's verbatim (permission trumps approval).
- **Approval consumption is still not implemented (F-10 §3).** An approval
  bound to a fingerprint stays valid for its whole ttl, so a *retried* attempt
  of the same task reuses it. That is intended for retries (same operation),
  but a one-time-use gate would need F-10 to add consumption.
- **A pending human approval parks, it does not retry.** F-06 grew
  `Outcome::AwaitingApproval`, so the supervisor returns it and the engine
  CASes `Running → HumanRequired` without consuming an attempt or holding the
  lease. Resuming is an explicit transition by whoever resolves the approval
  (`→ Ready` rework/resume, `→ Approved` sign-off, `→ Failed`/`→ Cancelled`
  abandon), and the driver goes idle instead of spinning on a parked task. An
  approval **store** that cannot answer is still `TransientFailure` — that is
  infrastructure, not a human. `GitGate` is unchanged: no live approval there
  is a denial, not a park.
- **Approvals survive a supervisor crash**: both the SQLite row and the
  task-keyed pointer live under `state_dir`, so the successor supervisor
  inherits them (asserted by the crash-recovery e2e, which approves the gate
  before the crash).

## 9. Seam register

| Seam | Lands with |
|---|---|
| ~~Git-gate approval (`approved=true` at enqueue)~~ | **CLOSED (this PR)** — verdict from `git_gate_check` + `ApprovalStore` |
| ~~HumanApproval node plumbing~~ | **CLOSED (this PR)** — resolved against the approval store |
| ~~Permission compilation into `SpawnSpec`~~ | **CLOSED (this PR)** — `compile_to_spawn_spec`, fail-closed |
| `Gate::GitCommit` (commit approvals ride `GitPush`) | agentos-policy follow-up |
| ~~`Outcome::AwaitingApproval`~~ | **CLOSED** — F-06 has it; HumanApproval parks in `HumanRequired` |
| One-time approval consumption | agentos-policy follow-up (F-10 §3) |
| Deterministic stub reviewer | later PR (reviewer pool) |
| Cross-stage ownership holds (until commit, not per attempt) | orchestration PR |
| Contract amendments re-leased mid-run | orchestrator PR |
| Engine-side cost gate (`CostLedger` injection) | agentos-workflow follow-up |
| Worktree GC | F-09 retention/GC path |
| Real CLI adapters behind the same registry | F-03..F-05 runtime wiring |
| Secrets broker (`SecretsBroker`) wired into the spawn env | F-10 keychain backend |

## 10. Tests & evidence

`crates/agentos-runtime`: 37 unit tests (contract Appendix-B round-trip +
validation + amend versioning; handoff Appendix-C round-trip + missing-field /
inline-blob / closed-set rejections + mock-packet mapping; usage-ledger
accumulation + Ok/Warn/Exceeded ladders + `CostLedger` hook; SHA-256 vectors;
journal round-trip; stub-review verdict; base-commit verification; **and 7
policy tests** — derived permissions, compiled constraints, the three
fail-closed compile refusals, operation determinism/change-sensitivity, the
full verdict ladder (missing → pending → approved → denied → expired), the
mutated-operation verdict, and gate non-transfer) and 11 e2e tests against a
real temp git repo (`tests/e2e.rs`):

- `e2e_happy_path_spec_parallel_review_gitgate` — 6-node workflow
  (run → parallel[run, run] → review → git_gate) driven to `Completed` **with
  the git mutation approved up front**; asserts validated handoff packets for
  both parallel branches (+ the spec node's), git request `Done` with the sha
  and `approved` carrying the policy verdict, ledger attribution
  (task/agent/reviewers/context versions/workflow), the Appendix E sequence
  (incl. `approval.granted` before `git.queued`) as an ordered subsequence,
  the audit bundle containing `approval.requested` + `git.gate`, the resolved
  approval still reading `approved`, the compiled spawn constraints on
  `session.spawn` (network tools denied, `Edit` allowed, the workspace-joined
  write root granted), one trace id on every event, the content-addressed
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

Policy boundary (each asserts *through the real mutation queue* that nothing
was queued, nothing committed, and `git rev-parse HEAD` never moved):

- `e2e_git_gate_blocks_without_an_approval` — no approval: run `Failed`,
  `policy.denied` with reason `no_approval_requested`, `approval.required`
  opened for a human, empty agent ledger, `permission.denied` + `git.gate`
  audit rows.
- `e2e_git_gate_blocks_when_the_approval_covers_a_mutated_operation` — a
  **live, approved** row for the same gate and task but a different
  `baseCommit` authorizes nothing.
- `e2e_git_gate_blocks_when_the_approval_has_expired` — the exact operation
  approved with a zero ttl: expiry is enforced at check time.
- `e2e_git_gate_denies_a_role_without_git_permission_despite_a_live_approval`
  — PRD §23.3 verbatim: gate role = `worker_write(**)` (no git actions), the
  approval is genuine, live and exactly matching; the permission leg still
  refuses (`policy.denied` reason `approved`, error "not permitted").
- `e2e_human_approval_node_resolves_both_ways` — three sub-runs: approved
  (node `Done`, run `Completed`, `approval.granted` carrying the
  `fnv1a64:` fingerprint), unresolved (3 attempts = the transient wait
  budget, then `Failed`, dependent `Blocked`, no `approval.granted`), denied
  (2 attempts = the reasoning budget, `approval.denied` naming the request).
- `e2e_spawn_is_refused_when_policy_cannot_cover_the_contract` — role
  permissions `src/narrow/**` vs a contract claiming `src/wide/**`:
  `policy.denied` (`stage: "compile"`) and **no** `session.started` — the
  adapter is never spawned.

Fresh verification (Windows reference platform, `CARGO_TARGET_DIR=target/wire`):

```text
$ cargo test -p agentos-runtime
test result: ok. 37 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.38s

     Running tests\e2e.rs (target/wire\debug\deps\e2e-418852a9a6e4a9fb.exe)

running 11 tests
test e2e_spawn_is_refused_when_policy_cannot_cover_the_contract ... ok
test e2e_cost_budget_exceeded_escalates_to_failed ... ok
test e2e_ownership_conflict_is_journaled_and_retried ... ok
test e2e_crash_recovery_reclaims_expired_lease_and_completes ... ok
test e2e_git_gate_blocks_when_the_approval_covers_a_mutated_operation ... ok
test e2e_git_gate_blocks_without_an_approval ... ok
test e2e_git_gate_blocks_when_the_approval_has_expired ... ok
test e2e_git_gate_denies_a_role_without_git_permission_despite_a_live_approval ... ok
test e2e_happy_path_spec_parallel_review_gitgate ... ok
test e2e_flaky_transient_failure_retries_to_completion ... ok
test e2e_human_approval_node_resolves_both_ways ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.81s

$ cargo clippy -p agentos-runtime --all-targets -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.38s

$ cargo fmt -p agentos-runtime -- --check   # clean
```

No new dependencies: `agentos-runtime` builds on the six sibling crates it was
already wired to (`agentos-policy` included since scaffold) plus `tempfile` as
the only dev-dependency. No billable provider calls: every test drives the
`MockAdapter`.
