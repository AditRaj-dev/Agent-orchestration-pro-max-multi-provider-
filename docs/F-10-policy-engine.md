# F-10 — Policy Engine: Permissions, Approval Gates, Secrets Interface, Audit Trail

Status: **implemented** · Crate: `crates/agentos-policy` · Date: 2026-08-22
Canon: `F-00-CONVENTIONS.md` §3/§5, PRD §14 (SEC-01..SEC-05), PRD §3 (least privilege, no hidden mutation), PRD §23.3 (MVP release gates), `docs/HANDOFF-BUILD.md` §4 (SQLite canon).

## 1. Scope & design

The policy engine is the **governance authority** (PRD §1): permissions,
approvals, secret access, and audit. Enforcement is deterministic and
process/contract-level — never prompt text. A model claiming "I was approved"
changes nothing: permission lives in `PermissionSet` values owned by the
harness, approvals live in SQLite bound to operation fingerprints, and the
whole is compiled into adapter-level constraints that become CLI
flags/sandbox boundaries.

Module map:

| Module | Responsibility | PRD |
|---|---|---|
| `src/permission.rs` | `PermissionSet` (read/write path globs, `ShellMode`, `NetworkPolicy`, `GitAction` set, secret scopes, tool allowlist, approval rules), glob matcher + overlap detector, role presets, `overlapping_write_conflict`. | SEC-01/02 |
| `src/compile.rs` | `compile_to_spawn_spec` → `SpawnConstraints` (the adapter-seam bridge); `git_gate_check` + `PolicyDenial`; derived tool denylists. | SEC-01, §23.3 |
| `src/approval.rs` | `Gate` set (git push / package install / prod action / destructive shell / secret access), `ApprovalRequest`, canonical-JSON + FNV-1a64 operation fingerprint, SQLite-backed `ApprovalStore`. | SEC-04 |
| `src/secrets.rs` | `SecretsBroker` trait (the OS-keychain seam), `SecretString` (redacting Debug/Display, NUL-zeroing Drop, no Serialize), `SecretLease`, `EphemeralBroker`. | SEC-03 |
| `src/audit.rs` | Append-only `AuditStore` (UPDATE/DELETE blocked by triggers), `record_*` helpers, `export_bundle` (SEC-05 run bundle). | SEC-05 |
| `src/error.rs` / `src/store.rs` (plumbing) | `PolicyError` with retryable `CoreError::SqliteBusy`; F-01 SQLite open canon (`busy_timeout(5s)`, read-first `journal_mode`, WAL). | §14, F-01 |

### Permission semantics (SEC-01)

- `can_read`/`can_write` match workspace-relative globs (`*` within a
  segment, `**` as a whole segment — the same grammar as F-09 ownership, so
  one path language across the workspace). **Read and write scopes are
  independent**: write does not imply read and vice versa.
- `allows_shell()` is true for `Allowed` and `AskApproval` (capability
  exists, maybe gated); only `Denied` removes the capability — and `Denied`
  is what puts shell tools on the compiled denylist.
- `allows_network(host)`: `Offline` denies all; `Allowlist` matches exact
  case-insensitive host names (no wildcards/ports/CIDR — those are
  sandbox/proxy concerns per SEC-02); `Unrestricted` allows all.
- `allows_tool(name)` is **fail-closed**: an empty allowlist allows nothing.
  The PRD's allowlist narrows; it cannot enumerate-deny, so total tool
  denial is expressed by leaving it empty — and the compile step then denies
  the file-write tool family at the seam.
- Serde defaults are fail-closed: `{}` deserializes to `deny_all()`
  (offline, no shell, nothing readable/writable).
- Presets: `worker_read_only()` (repo-wide reads, no writes/shell/network,
  read tools only), `worker_write(paths)` (reads+writes exactly the given
  globs, shell behind approval, offline by default, **no git actions** — all
  mutations flow through the git manager, package-install and
  destructive-shell gates configured), `git_manager()` (all four git actions,
  shell for the git CLI, `Unrestricted` network because remotes are
  user-configured and not enumerable by a preset — push stays
  approval-gated), `deny_all()` (quarantine).
- `overlapping_write_conflict(a, b)` reports whether two sets' write globs
  can touch a common file (conservative within a segment, same as F-09
  `glob_overlap`); the scheduler serializes such tasks — this feeds the
  ownership map (F-09 GIT-05) and later the workflow engine.

## 2. Enforcement map: policy → SpawnSpec → CLI flags

`compile_to_spawn_spec(&PermissionSet, TaskScope{workspace})` produces
`SpawnConstraints { allowed_paths, forbidden_paths, tool_allowlist,
tool_denylist }` — exactly the four policy-bearing fields of
`agentos_adapters::SpawnSpec` (task identity/objective/model/timeout/
isolated_home are orthogonal and stay with the adapter). This is the
"enforced outside model text" bridge: adapters own the final hop from these
lists to concrete flags.

| Policy source | Compiled field | Adapter seam (per F-00 §4 canon) |
|---|---|---|
| workspace root | `allowed_paths[0]` | process cwd (worktree); claude/agy run there |
| literal root of each read/write glob | `allowed_paths` (joined to the workspace, sorted, deduped) | claude: additional read dirs / permission rules; agy: `--add-dir <path>` |
| readable root with **no** write overlap | `forbidden_paths` (read-only markers) | claude: `Edit`/`Write` deny rules for those paths; agy: dir simply not passed via `--add-dir` (writes are virtualized outside `--add-dir`); codex: sandbox write-ro set |
| path absent from `allowed_paths` | (not granted) | adapter never passes it; sandbox roots access at `allowed_paths` |
| `ShellMode::Denied` | shell tool family (`Bash`, `Shell`) on `tool_denylist` | claude `--disallowedTools=Bash` (equals form); deny beats allow |
| `NetworkPolicy::Offline` | network tool family (`WebFetch`, `WebSearch`, `NetFetch`) on `tool_denylist` | claude `--disallowedTools=WebFetch,WebSearch`; host-level enforcement is the sandbox/proxy (SEC-02) |
| empty tool allowlist (fail-closed) | file-write family (`Edit`, `Write`, `NotebookEdit`) additionally denied | claude `--disallowedTools=Edit,Write,NotebookEdit` |
| non-empty `tool_allowlist` | passed through minus denylist entries | claude `--allowedTools=Bash(echo:*)` form (equals form) |

Semantics of `forbidden_paths` ⊆ `allowed_paths`: **"readable but not
writable"**, not "unreadable". Fully off-limits paths are the ones absent
from `allowed_paths` (they are never granted). A read root that only
*partially* overlaps a write glob is deliberately not marked forbidden —
write scoping then defers to the adapter's write-rooting. For
`worker_read_only` the marker is the workspace root itself.

`git_gate_check(&PermissionSet, action, approved_by_policy)` is the
synchronous git seam feeding `agentos_git::MutationQueue`'s approval-gated
push (the queue's `approved` flag comes from here):

1. the action must be in `git_actions` — **permission trumps approval**: a
   worker without `Push` is denied even when the flag is `true` (forged,
   stale, or model-asserted — PRD §23.3: "a worker without Git permission
   cannot commit/push even if instructed in prompt");
2. `Push` additionally requires a live approval
   (`ApprovalStore::is_approved` — fingerprint + expiry checked there).

Both denials are `PolicyDenial` facts for the audit trail, not retryable
errors.

## 3. Approval gates (SEC-04)

Schema (`approvals`): `id` (UUID v7), `gate`, `operation_fingerprint`,
`requested_by`, `status` (`pending|approved|denied`), `created_at`,
`expires_at`, `resolved_at` (+ index on gate/fingerprint/status). Requests
are immutable once resolved — re-approving a changed operation is a NEW
request; history is never rewritten.

Fingerprint semantics:

- **Canonical JSON**: object keys sorted recursively, compact separators;
  arrays are order-sensitive (reordering an argument list is a different
  operation); numbers by their serde_json representation (`1` ≠ `1.0` —
  strict change detection).
- **Hash**: FNV-1a 64-bit, dependency-free by design, formatted
  `fnv1a64:<16 hex>`. This is a *change-detection* hash, not a security
  primitive: collisions are astronomically unlikely for a local daemon's
  operation sets, and exploiting one would require an operation identical in
  canonical form to a human-approved one inside the ttl. `blake3` is already
  a workspace dependency elsewhere; swapping the two constants and the
  prefix is the upgrade path — the prefixed string format is the only
  contract.
- **Check-time enforcement**: `is_approved(gate, operation)` resolves true
  only when an `approved` row exists for that exact gate **and** fingerprint
  and `expires_at > now` (parsed and compared as `DateTime<Utc>`; a
  wall-clock jump after resolve cannot resurrect a stale approval). Any
  mutation — changed value, added key, reordered array, different gate —
  yields a different fingerprint and therefore no matching row.
- **Scope**: an approval is reusable (fingerprint + expiry only) or
  **single-use**. `request_once` marks it single-use and `consume(gate,
  operation, by)` spends it — `UPDATE ... WHERE status = 'approved'`, so two
  racing consumers cannot both cash one decision. A consumed row leaves
  `is_approved` false forever after, inside ttl or not, and reads as
  `ApprovalStatus::Consumed`. Consumers call it *after* the authorized side
  effect, so a failed attempt never burns a human's decision. Reusable stays
  the default: a gate that governs a task's *progression* asks the same
  question on every retry. The schema migration is additive and idempotent,
  so existing approval databases upgrade in place.
- The gate itself is a separate SQL column, so an approval never transfers
  across gates even for an identical operation payload.

## 4. Secrets safety properties (SEC-03)

- `SecretString` implements **no** `Serialize`/`Deserialize` (a lease cannot
  flow through serde into handoffs, events, or context artifacts), is not
  `Clone` (no multiplication of un-zeroed copies), and its `Debug`/`Display`
  render `***` — `{:?}`/`{}` in a log line cannot leak the value. Reading
  requires the explicitly named `expose()` (ephemeral env / side-channel
  injection only).
- `Drop` calls `zero_out()`, which overwrites the backing buffer with NUL
  bytes in place (length preserved, so the overwrite is observable in
  tests). Best-effort without a `zeroize` dependency: the stores go through
  safe code before the buffer is freed, defeating casual reuse-heap leaks;
  compiler elision of the stores is not contractually barred (documented
  trade-off; a keyed backend can harden this).
- `SecretLease` carries scope/task/expiry plus the value; it also does not
  implement Serialize.
- `SecretsBroker::grant(scope, task_id, ttl)` fails for unknown scopes and
  negative ttls; `revoke` is idempotent; `redact(text)` replaces every known
  value with `***[scope]`, longest value first (a value that prefixes
  another cannot shield it). Redaction covers leaked values regardless of
  lease state — an expired lease's value is still scrubbed.
- **Keychain seam**: `SecretsBroker` is the trait a Windows Credential
  Manager / secret-service backend implements later; `EphemeralBroker`
  (programmatic values) covers tests/dev. Values are never written to any
  repo file or transcript (F-00 §5).
- Audit records a grant by **scope only** — `AuditStore::record_secret_grant`
  takes no value parameter, so the leak is unrepresentable in the blessed
  path (and covered by a test asserting the value string never appears in
  the exported bundle).

## 5. Audit trail (SEC-05)

Schema (`audit_log`): `id` (UUID v7), `ts` (fixed-width RFC 3339 UTC millis
— lexicographic sort = chronological), `actor`, `action_kind`, `resource`,
`details` (JSON object, enforced), `run_id` (nullable; index).

- **Append-only at the storage layer**: `BEFORE UPDATE`/`BEFORE DELETE`
  triggers `RAISE(ABORT, ...)` — even a rogue direct connection cannot
  rewrite history without dropping the schema (tested with a second raw
  connection). Corrections are new events (F-00 §3).
- Kinds recorded by the helpers: `permission.denied`, `secret.granted`,
  `secret.revoked`, `approval.requested`, `approval.resolved`, `git.gate`.
- `export_bundle(run_id)` emits the run bundle (SEC-05):
  `{runId, generatedAt, eventCount, events[]}` with each event as
  `{id, ts, actor, actionKind, resource, details, runId}` in chronological
  order; `run_id = None` exports the whole log, unknown runs export an
  empty bundle (not an error).
- Wiring responsibility: the daemon calls the helpers at each seam (denials
  from `git_gate_check`/compile, approvals from `request`/`resolve`, grants
  from the broker) — the stores are deliberately decoupled so each stays
  independently testable.

## 6. Test evidence

Commands (custom target dir to avoid lock contention with parallel builds):

```
cd /d/OP/agent-engineering-os
CARGO_TARGET_DIR=target/f10 cargo test -p agentos-policy
CARGO_TARGET_DIR=target/f10 cargo clippy -p agentos-policy --all-targets -- -D warnings
cargo fmt -p agentos-policy --check
```

Fresh output (2026-08-22, cargo 1.94.1):

```
running 52 tests
test result: ok. 52 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.11s
```

(The 5s runtime is dominated by the real-lock contention test below.)

Clippy `--all-targets -- -D warnings`: clean. `cargo fmt --check`: clean.

Coverage highlights (all inline `#[cfg(test)]`):

- **Globs** (`permission`): 20-row matching table incl. `**` zero-segment
  (`src/**` matches `src`), single-segment `*` confinement, nested
  `src/**/*.rs`, Windows separator/`./` normalization; overlap exactness +
  symmetry + the documented conservative `a*`/`b*` case.
- **Scope independence**: write-without-read and read-without-write both
  hold; presets assert exact capability envelopes; `{}` deserializes to
  `deny_all()` (fail-closed serde).
- **Compile** (`compile`): read-only worker → workspace read-only marker +
  shell/network denylist; write worker → scoped `allowed_paths`, gated shell
  kept off the denylist; readable-but-unwritable roots → `forbidden_paths`;
  partial overlap deliberately unmarked; `deny_all` → maximally restrictive
  lists; deny-beats-allow for a contradictory allowlist+`ShellMode::Denied`.
- **Release gate (PRD §23.3)**: `release_gate_scenario_forged_and_stale_approvals_do_not_push`
  — a genuinely approved push operation (live store row) still fails
  `git_gate_check(Push)` for a worker without `Push` permission; the git
  manager passes with the approval, and a one-field mutation (`force: true`
  added) or a different gate loses the approval instantly.
- **Fingerprints** (`approval`): key-order independence, value/array/added-key
  sensitivity, gate non-transfer, expiry enforced at check time (ttl 0),
  denied/unknown never approved, resolved rows are terminal.
- **Busy** (`approval`, `error`): a held `BEGIN IMMEDIATE` on a second
  connection makes `request()` fail with **`CoreError::SqliteBusy`**
  (retryable) and succeed after lock release; synthetic tests pin the whole
  `SQLITE_BUSY` family mapping (`5`, `5|1<<8`) and that other rusqlite
  errors stay typed.
- **Secrets** (`secrets`): Debug/Display redaction (standalone and nested in
  a lease), `expose()` round-trip, `zero_out` NUL-fill observability,
  grant/re-grant/revoke lifecycle with idempotent revoke, unknown-scope and
  negative-ttl rejection, transcript scrubbing incl. longest-match-first.
- **Audit** (`audit`): bundle shape/counts/order per run and unfiltered,
  rogue-connection UPDATE/DELETE aborted by triggers with the row surviving,
  all six `record_*` kinds in one chronological bundle, non-object details
  rejected, and secret values never present in any export.

## 7. Known limitations / next steps

- `forbidden_paths` cannot express the *complement* of a read scope (not
  enumerable as a path list); containment beyond the granted roots relies on
  the adapter sandbox (SEC-01 "sandbox/workspace boundaries"). An adapter
  that cannot express "readable but not writable" must deny writes entirely
  — over-restrictive, never under-restrictive.
- True host-level network enforcement (proxy/sandbox) and the OS-keychain
  `SecretsBroker` backend are the next seams; `EphemeralBroker` is for
  tests/dev only.
- ~~Approval consumption (one-time use)~~ — implemented (see §3);
  `Gate::GitCommit` now exists too, so commit and push are separate gates and
  `git_gate_check` demands a live approval for both (merge/rebase stay
  permission-only — they rewrite a task branch, not integration history).
- FNV-1a 64 is change-detection, not collision-resistant against an
  adversary; swap to the workspace `blake3` when approvals cross a trust
  boundary (format is the contract: `<alg>:<hex>`).
- Wiring the `record_*` audit calls and `git_gate_check` into the daemon's
  enqueue path (`agentos-git`'s `approved` flag) is the F-10 integration PR.
- No shell/network enforcement for tools outside the denylist families
  (e.g. an MCP tool that fetches); MCP tool policy rides `tool_allowlist`
  once MCP lands (F-00 §1: MCP is interface-only).
