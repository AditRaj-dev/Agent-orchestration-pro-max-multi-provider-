# F-09 — Git Manager: Worktrees, Mutation Queue, Agent Ledger, Ownership

Status: **implemented** · Crate: `crates/agentos-git` · Date: 2026-08-22
Canon: `F-00-CONVENTIONS.md` §1/§5, PRD §12 (GIT-01..GIT-05), `docs/HANDOFF-BUILD.md` §4 (SQLite canon).

## 1. Scope & design

The dedicated git manager (GIT-01) is the only component that commits, merges,
rebases, or pushes. Workers operate in isolated worktrees and submit requests
to a serialized queue; attribution lives in a harness-DB ledger, never in
commit messages. Storage is the `git` CLI via `std::process` (F-00 §1 — MVP
uses the CLI, no new dependencies) plus SQLite for queue/ledger state.

Module map:

| Module | Responsibility | PRD |
|---|---|---|
| `src/cli.rs` | Thin `git` CLI wrapper; argv is `Vec<OsString>`, never a shell string (Windows reference platform). Typed `GitCliError { args, exit_code, stderr }`. Helpers: `init`, `rev_parse_head`, `current_branch`, `add_all_and_commit(author, msg)`, `worktree_add/remove/list`, `branch_exists`, `merge_base`, `diff_name_only`. | GIT-01 |
| `src/worktree.rs` | `WorktreeManager`: create/list/remove per leased write task, retention-gated GC stub. | GIT-04 |
| `src/queue.rs` | `MutationQueue`: per-repo ordered queue, single mutating consumer, leases, stale-base gate, approval-gated push. | GIT-01/02 |
| `src/ledger.rs` | `AgentLedger`: commit → task/agent/orchestrator/reviewers/context versions/workflow. | GIT-03 |
| `src/ownership.rs` | `OwnershipMap`: exclusive/advisory holds on path-glob sets. | GIT-05 |
| `src/store.rs` (private) | F-01 SQLite open canon shared by queue + ledger: `busy_timeout(5s)` on every connection; `journal_mode` **read first**, switch to WAL only when it differs; `SQLITE_BUSY` → retryable `CoreError::SqliteBusy` via `error::db`. Timestamps are fixed-width RFC 3339 UTC millis so lexicographic (BINARY collation) comparison is valid time comparison. | F-01 |

Error taxonomy (`src/error.rs`): `GitError` wraps `CoreError`
(`SqliteBusy` retryable, `NotFound`), `GitCliError` (exit code + stderr),
io/json errors, plus typed `PushNotApproved` and `Invalid(String)`.

## 2. Branch naming rule (GIT-04)

```
agentos / <run_id_short> / <task_id_short>
```

- Both `<*_short>` are the **last 8 hex characters** of the parsed UUID
  (simple form, no hyphens) — the TAIL, because a run's tasks are minted as
  UUIDv7s in one millisecond burst, so head-derived shorts collide and
  `git worktree add -b` fails.
- Both ids **must parse as UUIDs** (`Uuid::parse_str`); anything else is
  rejected with `GitError::Invalid`. This structurally guarantees no
  model-controlled free text can ever reach a ref name — the model never
  supplies a branch name, only the harness-generated run/task ids do.
- Worktree path: `<repo>/.agentos-worktrees/<task_id>` (full UUID — unique
  per leased task).
- The worktree dir is hidden by appending `.agentos-worktrees/` to
  `.git/info/exclude` (created best-effort). `.git/info/exclude` is local
  ignore metadata — the user's `.gitignore` is never edited without opt-in.
  Failure to write it only logs; worktree creation proceeds.
- `remove(path)` tries `git worktree remove`, falls back to `--force`
  (dirty worktrees). The **branch and commits survive removal** — they are
  reclaimed only by retention-gated GC.
- `gc_eligible(entry, rules)` stub honoring the retention hook
  `RetentionRules { min_age, require_merged_into }`: eligible only if the
  entry lives under the managed root, is at least `min_age` old (fs mtime),
  and — when `require_merged_into` is set — its `HEAD` is an ancestor of the
  target commit-ish (work already merged).

## 3. Mutation queue (GIT-02)

### Schema (`git_requests`)

```sql
CREATE TABLE IF NOT EXISTS git_requests (
    id TEXT PRIMARY KEY,            -- UUID v7 (time-ordered)
    repo_path TEXT NOT NULL,        -- use one canonical absolute form per repo
    task_id TEXT NOT NULL,
    base_commit TEXT NOT NULL,      -- integration head when enqueued
    action TEXT NOT NULL,           -- commit | merge | rebase | push
    status TEXT NOT NULL,           -- pending | in_progress | done | rejected | conflict
    lease_owner TEXT,               -- consumer id while in_progress
    lease_expires_at TEXT,          -- RFC 3339 UTC millis
    approved INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    result_sha TEXT,                -- commit produced on success
    error TEXT                      -- rejection reason
);
CREATE INDEX IF NOT EXISTS idx_git_requests_repo_status ON git_requests(repo_path, status);
CREATE TABLE IF NOT EXISTS queue_config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
```

`queue_config` persists the `allow_unapproved_push` override.

### Single-consumer semantics

`claim_next(repo_path, owner, ttl)` runs in one `IMMEDIATE` transaction:

1. expired `in_progress` leases (`lease_expires_at <= now`) return to
   `pending` — dead-consumer recovery;
2. if any live lease remains for the repo → `Ok(None)`, **even when other
   items are pending** — exactly one mutating consumer per repo;
3. otherwise the oldest `pending` item (insertion order via `rowid`) is
   leased to `owner` and returned as `in_progress`.

Queues of different repos are independent (a lease on repo A does not block
repo B). `complete(id, sha)` and `reject(id, reason)` only transition
`pending`/`in_progress` rows (terminal rows are a no-op returning `false`).

### Stale-base gate

`stale_base_check(id, integration_head)` runs before executing an item. An
item is **fresh only while the integration head is exactly the recorded
`base_commit`**. Any commit that landed after enqueue invalidates the item's
assumptions — *including a fast-forward that keeps `base_commit` an
ancestor* (GIT-02: the queue blocks integration until the expected base is
available; a moved head means rebase). Stale items are marked `rejected`
with error `stale_base`; the merge-base of the two commits is computed to
classify routing (ancestor → plain rebase path; diverged → review path).
Unknown ids surface `CoreError::NotFound`.

### Approval-gated push (GIT-01)

`enqueue(..., Push, approved=false)` returns `GitError::PushNotApproved`
unless the `allow_unapproved_push` override was **explicitly set** via
`set_allow_unapproved_push(true)` (persisted in `queue_config`, survives
reopen). Enforcement is in code, never prompt-based. The `approved` flag is
recorded on every row for audit; non-push actions never require approval.

## 4. Agent ledger (GIT-03)

Schema (`ledger`): `commit_sha` PK, `task_id`, `agent_instance`,
`orchestrator`, `reviewers` (JSON array), `context_versions` (JSON object),
`workflow_id`, `recorded_at` (+ index on `task_id`).

- `record(entry)` stamps `recorded_at` and upserts on `commit_sha`
  (idempotent — crash-retry loops cannot poison the ledger).
- `by_commit(sha)` / `by_task(task_id)` (oldest first).
- Harness-DB metadata only: commit messages stay human; git
  notes/custom-refs provenance is a deliberate non-goal for F-09.

## 5. Ownership map (GIT-05)

`OwnershipMap::acquire(task_id, path_globs, exclusive)` — all-or-nothing:

- **exclusive** request conflicts with any overlapping hold (advisory or
  exclusive) held by another task;
- **advisory** request conflicts only with overlapping **exclusive** holds;
- a task never conflicts with its own holds; re-acquiring a held glob
  replaces it (upgrade/downgrade).
- `release(task_id)` drops all holds of the task (completion/cancellation
  path) and reports whether anything was dropped.
- `holds_for_path(glob)` lists holds overlapping the glob; a concrete path
  is a legal degenerate glob.

**In-memory for F-09** (interior mutability, `&self` API, process-wide
shareable); persistence — including crash-timeout release — arrives with the
daemon event journal.

Glob grammar and limitations: `*` matches within one path segment, `**` as a
whole segment spans segments (`src/**` also matches `src` itself — a
documented divergence from gitignore); backslashes are normalized. Overlap
detection is exact for concrete/directory globs but **conservative** within
a segment: any segment containing `*` is treated as potentially overlapping
any other segment (e.g. `a*` vs `b*` reports overlap), so real conflicts are
never missed at the cost of occasional false ones.

## 6. Test evidence

Commands (custom target dir to avoid lock contention with parallel builds):

```
cd /d/OP/agent-engineering-os
CARGO_TARGET_DIR=target/git cargo test -p agentos-git
CARGO_TARGET_DIR=target/git cargo clippy -p agentos-git --all-targets -- -D warnings
cargo fmt -p agentos-git --check
```

Fresh output tails (2026-08-22, git 2.49.0.windows.1):

```
test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s   # unit

     Running tests\ledger.rs
test ledger_survives_reopen ... ok
test ledger_round_trip_by_commit_and_by_task ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s

     Running tests\ownership.rs
test invalid_inputs_are_rejected ... ok
test advisory_holds_stack_on_the_same_paths ... ok
test exclusive_conflicts_with_everything_advisory_coexists ... ok
test release_frees_paths_and_reports_whether_it_dropped_holds ... ok
test same_task_may_reacquire_and_upgrade_its_own_holds ... ok
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests\queue.rs
test fifo_order_with_single_consumer_enforcement ... ok
test expired_lease_is_reclaimed_by_next_consumer ... ok
test push_is_approval_gated_by_default ... ok
test queues_of_different_repos_are_independent ... ok
test stale_base_is_rejected_when_integration_moves_on ... ok
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.77s

     Running tests\worktrees.rs
test worktree_create_branch_name_list_remove_round_trip ... ok
test two_tasks_get_isolated_worktrees_and_gc_honors_retention ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.24s
```

`clippy --all-targets -- -D warnings`: clean (no diagnostics).
`cargo fmt --check`: clean.

Integration tests run the real `git` CLI on `tempfile` temp repos (paths
passed as `OsString`/`Path`, auto-cleaned) and cover: worktree
create/branch-name-format/list/remove round-trip with `--force` fallback and
retention-gated GC; queue FIFO + single-consumer enforcement; zero-ttl lease
expiry → reclaim; stale-base rejection after an integration commit;
unapproved push rejected / approved push enqueued / override persisted across
reopen; per-repo queue independence; ledger round-trips in both query
directions; ownership exclusive-vs-advisory conflicts, release, and
self-upgrade.

## 6a. Model advisor (`git-manager` agent)

The git manager has an optional model half, configured like any other agent:
the `git-manager` registry record (seeded as **agy / `gemini-3.1-pro-high`**,
mode `plan`, 300s). No record and no `role_adapters` entry means no advisor,
and every path below falls back to the behavior F-09 shipped with. The
advisor is an addition to the git manager, never a dependency of it.

It contributes exactly two things, both bounded:

**Commit detail.** `commit_message` keeps the deterministic subject
(`agentos: audit bundle for run <run_id>`) and appends a sanitized body
describing what changed, derived from `git status --porcelain` paths. The
sanitizer drops control characters and leading `-`/`*`/`#`/`>`, caps the body
at 5 lines / 400 bytes, and yields nothing when nothing survives. GIT-03 is
unchanged: attribution still lives in the ledger, never in commit text. A
slow or failed advisor leaves the subject alone — a commit never fails
because a model was unavailable.

**Stale-base routing.** On a `stale_base` rejection the advisor answers with
one line: `REBASE <sha>` or `ESCALATE <reason>`. The sha must be hex, 7–40
chars, and must prefix-match one of the two commits the harness already
named (recorded `base_commit` or current head) — a commit it was not offered
escalates. The chosen sha is then re-resolved through
`git rev-parse --verify <rev>^{commit}` before any argv is built, so no model
text reaches the CLI. The replay runs as
`git rebase --onto <resolved> <base_commit>` **inside the task's own isolated
worktree**, and aborts itself on failure; the mutation is retried either way,
so a successful rebase only means the retry finds a fresh base. Push remains
approval-gated (GIT-01) and branch names remain UUID-derived (GIT-04) — the
advisor cannot touch either.

The routing value on the `git.stale_base` event widens accordingly:
`rebase-or-review` (no advisor, or the replay failed), `rebased`, or
`review`, with the advisor's reason carried alongside in `advisor`.

## 7. Known limitations / next steps

- Queue consumer loop, merge/rebase execution, and remote push are the
  F-10+/daemon integration surface; F-09 ships the queue mechanics.
- Ownership crash-timeout release and persistence ride the daemon journal.
- `stale_base_check` trusts the caller-supplied integration head; wiring it
  to the daemon's repo watch is future work.
- Glob overlap within a segment is conservative (see §5).
- The model advisor (§6a) is covered by unit tests over its sanitizer and
  answer parser; the supervisor wiring has no end-to-end test against a live
  provider, and an unconfigured advisor is the tested default.
