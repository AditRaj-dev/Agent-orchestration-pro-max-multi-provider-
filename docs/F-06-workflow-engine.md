# F-06 — Workflow Engine: versioned DAG, durable scheduler, leases, budgets

Status: implemented 2026-08-22 · Crate: `crates/agentos-workflow` · Canon: `F-00-CONVENTIONS.md` §3/§5, `docs/HANDOFF-BUILD.md` §4 (SQLite canon), PRD §6 (task lifecycle), §9 OR-03/OR-04/OR-05/OR-08.

## 1. Scope and design summary

F-06 delivers the deterministic workflow engine — no providers, no daemon loop:

- **`src/spec.rs`** — the versioned DAG definition: `WorkflowSpec { id, version, nodes }`, `NodeSpec { id, node_type, depends_on, agent_role, budgets, retry }`, `NodeType` (Run / Parallel / Review / GitGate / HumanApproval / Branch / Loop{max_iterations}), `Budgets` (maxAttempts/maxElapsed/maxCost), `RetryPolicy` (transient vs reasoning), and the OR-04 `TaskContract` subset (objective, allowedPaths, forbiddenPaths, acceptanceCriteria, requiredChecks). All JSON-serializable (camelCase fields, snake_case node types, mirroring the PRD data model).
- **`src/validate.rs`** — pre-execution validation (OR-03): non-empty, unique node ids, resolvable `dependsOn`, acyclic (Kahn topological sort; a concrete cycle path is extracted by DFS for the error), loops bounded (`max_iterations >= 1`). Rejections are the machine-readable `ValidationError` (serde-tagged `{"code": "cycle_detected", "cycle": [...]}`).
- **`src/store.rs`** — durable SQLite task store with a **private copy of the F-01 canon open helper** (busy_timeout first; journal_mode read before set; versioned migrations in an explicit transaction). State changes are CAS statements delegating legality to agentos-core's `TaskState::can_transition`.
- **`src/scheduler.rs`** — ready queue (Priority P0→P3, then age), leases + heartbeats + expiry reclaim (OR-05), and the budget gate (OR-08) with a `CostLedger` hook for `max_cost_usd`.
- **`src/executor.rs`** — the provider-free `#[async_trait] TaskExecutor` boundary (`Outcome = Success{packet} | ReasoningFailure | TransientFailure`) and `WorkflowEngine` (`start_run`, `tick`, `run_until_idle`) — composable primitives plus a bounded tokio driver, **not** a daemon loop; embedders call `tick` on their own cadence.

## 2. Schema (migration v1, `PRAGMA user_version`)

```sql
CREATE TABLE IF NOT EXISTS runs (
    id                TEXT PRIMARY KEY,   -- run id (UUIDv7)
    workflow_id       TEXT NOT NULL,
    workflow_version  INTEGER NOT NULL,
    goal              TEXT NOT NULL,
    status            TEXT NOT NULL,      -- running | completed | failed
    created_at        TEXT NOT NULL       -- RFC 3339
);

CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,    -- task id (UUIDv7)
    run_id           TEXT NOT NULL REFERENCES runs(id),
    workflow_id      TEXT NOT NULL,
    node_id          TEXT NOT NULL,
    state            TEXT NOT NULL,       -- TaskState wire string (core)
    priority         TEXT NOT NULL,       -- p0..p3 (core)
    lease_owner      TEXT,
    lease_expires_at TEXT,                -- RFC 3339
    heartbeat_at     TEXT,                -- RFC 3339
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    contract         TEXT NOT NULL,       -- TaskContract JSON (OR-04)
    node             TEXT NOT NULL,       -- NodeSpec JSON: type, deps, budgets, retry
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    UNIQUE (run_id, node_id)
);
CREATE INDEX idx_tasks_state_priority_created ON tasks (state, priority, created_at);
CREATE INDEX idx_tasks_run_id                 ON tasks (run_id);
CREATE INDEX idx_tasks_lease_expiry           ON tasks (state, lease_expires_at);
```

Enums, contracts and node specs shuttle through their own serde impls (column value = wire string/JSON), so the stored format is canon-by-construction: a `TaskState` unknown to a future build fails loudly rather than silently misreading. `UNIQUE (run_id, node_id)` makes a run's materialization 1:1 with its spec.

## 3. SQLite canon (HANDOFF-BUILD §4) — satisfied

| Canon rule | Implementation |
|---|---|
| `busy_timeout` on EVERY connection | First statement in the private `open_db` (`5000 ms`), before anything that can lock. |
| Never re-issue `PRAGMA journal_mode` unconditionally | `ensure_wal` reads the mode; WAL is requested only when not already `wal`. |
| journal_mode may return SQLITE_BUSY without honoring the busy handler | The conditional WAL set retries 5× with 200 ms delays; exhaustion surfaces as `CoreError::SqliteBusy`. |
| Explicit transactions around multi-write ops | `create_run` (run + N tasks + zero-dep promotions), `record_success` (7-hop walk), `record_failure` (attempt burn + park + requeue/escalate), migrations. Single-row CAS ops are single atomic statements. |
| SQLITE_BUSY retryable, not generic | `is_sqlite_busy` matches primary code 5 **or** any extended code with primary byte 5 (`SQLITE_BUSY_SNAPSHOT`, …) → `CoreError::SqliteBusy`; `WorkflowError::Storage(CoreError::is_retryable)` stays true. Proven by `busy_maps_to_retryable_core_error`: a write under a held `BEGIN IMMEDIATE` yields busy, the identical retry succeeds after release. |

## 4. State machine mapping (core owns legality; the store enforces durably)

Every state change goes through `TaskState::can_transition` (agentos-core) **before** SQL, then a CAS `UPDATE tasks SET state = ? WHERE id = ? AND state = ?expected`. Zero rows affected ⇒ the row is re-read and reported precisely: `CoreError::IllegalTransition { from: <actual>, to }` or `CoreError::NotFound`.

- **Creation**: `start_run` validates first; then one transaction inserts the run + tasks as `Planned`, promoting zero-dependency nodes to `Ready`.
- **Dependency gate**: `Planned -> Ready` when all `depends_on` are `Done` (`promote_unblocked`). On a task failure, transitive `Planned` dependents are parked `Blocked`.
- **Lease lifecycle (OR-05)**: `grant_lease` = CAS `Ready -> Leased` with owner, `now+ttl` expiry, initial heartbeat. The lease columns persist through `Leased -> Running` (heartbeats must work during execution) and are released on every hop out of {Leased, Running}. `heartbeat(task, owner)` renews the expiry by the *granted* ttl — recovered durably as `lease_expires_at - heartbeat_at`, no extra column. Foreign heartbeats fail with `WorkflowError::LeaseOwnerMismatch{holder, owner}`.
- **Crash recovery**: `reclaim_expired_leases(now)` — an expired `Leased` task CAS `Leased -> Ready` with `attempt_count + 1`; an expired `Running` task (hung/dead executor) takes the legal `Running -> Retryable -> Ready` walk. Over-budget results escalate to `Failed`.
- **Success**: core's machine has **no shortcut to `Done`** — `record_success` walks `Running -> OutputReady -> ReviewPending -> Approved -> GitQueued -> Committed -> Done` as one CAS chain in one transaction. Per-node-type execution policies (dispatching a real review, serializing git via F-09) arrive with those crates; F-06's executor abstracts "the bounded work of the node", and the walk documents exactly which lifecycle stations a completed node has passed.
- **Failure (OR-08)**: `record_failure` burns the attempt (`attempt_count + 1`), parks `Running -> Retryable`, then requeues (`Retryable -> Ready`) or escalates (`Retryable -> Failed`) per the retry policy below.

## 5. Scheduling, budgets and retry rules

- **ready_tasks()**: state `Ready` AND all node dependencies in terminal success (`Done`), ordered by `Priority` (P0 first) then `created_at` (age), id as final tiebreak — deterministic. Tasks default to `P2` (NodeSpec carries no priority field; OR-01's orchestrator retunes via `set_priority`).
- **tick()** phase order: (1) reclaim expired leases → (2) promote unblocked → (3) budget gate + grant leases → (4) CAS `Leased -> Running`, spawn executor futures (tokio `JoinSet`), await them → (5) record outcomes via CAS → (6) refresh run-status projections. One clock per pass feeds every gate.
- **Budget gate before every lease** (OR-08): `attempt_count < max_attempts`; wall-clock since task creation `< max_elapsed_secs`; spend `< max_cost_usd` through the `CostLedger` hook (`NoCostLedger` stub until F-07 — `None` never fails the gate). Over-budget ⇒ `Ready -> Failed`, dependents `Blocked`, run `failed`; never a silent re-run.
- **Attempt accounting**: `attempt_count` increments whenever an attempt ends without success — reported failure (transient or reasoning) or lease-expiry reclaim. A lease is granted only while `attempt_count < max_attempts`, so `max_attempts` bounds total leases; the retry policy refines *requeue* decisions: a transient failure requeues while `count <= transient_retries`, a reasoning failure while `count <= reasoning_retries`; whichever bound binds first escalates to `Failed`.
- **Executor crash**: a panicked executor future leaves the task `Running` with its lease; `tick` reports it (`FailureKind::ExecutorCrashed`) and lease expiry drives recovery — the same OR-05 path as any crashed worker. CAS conflicts against another engine are reported (`conflicted`), and the durable row always wins.
- **Driver**: `run_until_idle(max_ticks)` ticks until a pass does no work; exhausting the ceiling with work pending is `WorkflowError::MaxTicksExceeded` (a runaway guard, not silent truncation).

## 6. Test evidence (fresh output, 2026-08-22)

`CARGO_TARGET_DIR=target/workflow cargo test -p agentos-workflow` — **29 passed, 0 failed**:

```
running 13 tests                                   (lib unit tests)
test error::tests::busy_storage_error_is_retryable_others_are_not ... ok
test spec::tests::node_types_serialize_as_snake_case ... ok
test validate::tests::self_dependency_is_reported_as_a_cycle ... ok
test spec::tests::default_contract_names_node_type_id_and_goal ... ok
test store::tests::extended_busy_codes_map_to_sqlite_busy ... ok
test error::tests::validation_error_serializes_as_tagged_machine_readable_code ... ok
test validate::tests::topological_order_is_deterministic_and_wave_stable ... ok
test spec::tests::prd_example_workflow_round_trips_through_json ... ok
test store::tests::cas_transition_reports_actual_state_on_miss ... ok
test store::tests::task_record_round_trips_all_fields ... ok
test store::tests::cas_transition_out_of_leased_clears_lease ... ok
test store::tests::record_success_walks_the_legal_chain ... ok
test store::tests::busy_maps_to_retryable_core_error ... ok
test result: ok. 13 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.38s

running 9 tests                                    (tests/engine.rs)
test start_run_rejects_invalid_specs_without_side_effects ... ok
test elapsed_budget_exhaustion_fails_before_first_lease ... ok
test heartbeat_renews_and_rejects_foreign_owners ... ok
test ready_tasks_order_priority_then_age ... ok
test lease_expiry_reclaims_and_re_leases ... ok
test attempts_budget_exhaustion_escalates_to_failed ... ok
test happy_path_spec_review_git_gate ... ok
test restart_durability_continues_the_run ... ok
test parallel_fan_out_runs_before_review ... ok
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.20s

running 7 tests                                    (tests/validation.rs)
test empty_workflow_is_rejected ... ok
test task_contract_round_trips_fully_populated ... ok
test dangling_dependency_is_rejected ... ok
test duplicate_node_id_is_rejected ... ok
test cycle_is_rejected_with_the_cycle_path ... ok
test unbounded_loop_is_rejected ... ok
test prd_example_workflow_is_valid ... ok
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

`CARGO_TARGET_DIR=target/workflow cargo clippy -p agentos-workflow --all-targets -- -D warnings`:

```
    Checking agentos-workflow v0.1.0 (D:\OP\agent-engineering-os\crates\agentos-workflow)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.21s
```

`cargo fmt -p agentos-workflow --check`: clean.

Required scenarios covered: validation rejections (cycle incl. self-loop with the path reported, dangling dep, duplicate id, unbounded loop, empty); happy path spec→review→git_gate (one node per tick, review ready only after spec Done); parallel fan-out with proven concurrency (max simultaneous executors == 2 via an instrumented mock); lease expiry → reclaim → re-lease with the crashed attempt consumed (`attempt_count == 1`); budget exhaustion → escalation to `Failed` with a `Blocked` dependent, `failed` run, and **no further leases** on subsequent ticks; restart durability (drop engine + store, reopen the db file, mid-flight ghost lease and all states intact, fresh engine completes the run).

## 7. Deviations and open notes

1. **`node` JSON column added** beyond the deliverable's column list: each task row embeds its `NodeSpec` (node type, `depends_on`, `Budgets`, `RetryPolicy`), making the record self-describing so the scheduler enforces governance after a restart without re-reading the original spec — the durable reading of OR-03's "restarts do not lose run state". The brief's columns are all present and unchanged.
2. **`async-trait` added to the crate's Cargo.toml** (already a workspace dependency; workspace root untouched) — `#[async_trait]` per the deliverable, and object-safe `Arc<dyn TaskExecutor>` for the engine.
3. **Success packets are returned, not persisted**: `Outcome::Success{packet}` surfaces in `TickReport`. Durable artifact storage is the content-addressed store's job (F-07/F-10), keeping the tasks table strictly lifecycle state.
4. **No event journal here** (F-00 §3 / the daemon's append-only `events` table own that); F-06 logs lifecycle transitions via `tracing` (`run started`, `task done`, budget/lease warnings) for the daemon to journal when it embeds the engine.
5. **Contracts are synthesized** (`TaskContract::for_node`) with empty path/criteria lists — the OR-04 policy engine and OR-01 orchestrator inject real values; the objective carries node type, node id and the run goal.
6. **Loop nodes** carry a statically validated bound (`max_iterations >= 1`); runtime iteration counting inside a loop-body execution is executor-side (F-02 adapter) — the engine bounds it with the same budgets as every other node.
7. **Non-busy SQLite errors** map to `CoreError::Serialization("sqlite: …")` exactly as in F-01 (agentos-core keeps a minimal taxonomy; `db::map_sqlite_error`'s single switch point applies here too).
8. **Priority** defaults to `P2` for all materialized tasks; `NodeSpec` deliberately carries no priority field per the F-06 brief, and `TaskStore::set_priority` exists for the OR-01 orchestrator's `PlanOperation`s.
