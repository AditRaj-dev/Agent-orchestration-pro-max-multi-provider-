# F-08 — Context Compiler & Shared Memory (CTX-01/02/04/05)

Status: implemented in `crates/agentos-context`. Binding inputs: PRD §10
(CTX-01, CTX-02, CTX-04, CTX-05, MEM-02 refs), `F-00-CONVENTIONS.md` §1/§3,
`docs/HANDOFF-BUILD.md` §4 (F-01 SQLite canon).

Crate map:

| Module | PRD item | Role |
|---|---|---|
| `src/db.rs` (private) | F-01 canon | SQLite open helper: busy_timeout first, read-then-set WAL, versioned migrations (`user_version`), SQLITE_BUSY → retryable `CoreError::SqliteBusy` |
| `src/file_index.rs` | CTX-01 | Repository file cache: BLAKE3 hashes, language detection, hash history, fast-path rescan + full `verify_all` |
| `src/graph.rs` | CTX-02 | Context nodes in normalized tables, provenance enforced on write, transitive `dependents_of`, symbol stub |
| `src/invalidation.rs` | CTX-05 | Dirty-state machine over the graph, `context.invalidated` journal events, recompute queue |
| `src/compiler.rs` | CTX-04 | Role-weighted deterministic retrieval, strict token budget, manifest, materialized ephemeral context file |

`ContextStore` (in `lib.rs`) is the facade: one repository root + one SQLite
database, with borrowed views per component. MEM-02 decision ledger lives in
F-10; this crate stores **refs only** (`node_decisions.decision_ref`).

## 1. SQLite canon (F-01, verbatim from the daemon)

Every connection opened by `db::open_db`:

1. `busy_timeout(5000ms)` **before anything else that can lock**.
2. `PRAGMA journal_mode` is **read first**; WAL is requested only when the
   stored mode is not already `wal`, retried up to 5×/200 ms because this
   pragma can return `SQLITE_BUSY` *without honoring the busy handler* (the
   observed race behind the canon).
3. Migrations run inside an explicit transaction, versioned via
   `PRAGMA user_version` (currently `1`).

Any `SQLITE_BUSY`-family error (extended code low byte `5`, incl.
`SQLITE_BUSY_SNAPSHOT`) maps to `ContextError::Core(CoreError::SqliteBusy)` —
retryable and distinctly surfaced, never flattened into a generic failure.
Multi-table writes (node upsert/delete, `mark_clean`) use explicit
transactions (`unchecked_transaction` over the shared `&Connection`).

## 2. Schema (migration v1)

```sql
files              (path PK, content_hash, size, language, parsed_version,
                    dirty, mtime_secs, mtime_nanos, first_seen_at, last_hashed_at)
file_hash_history  (path, content_hash, seen_at) PK(path, content_hash)
context_nodes      (id PK, topic, version, summary, invalidation_state,
                    created_at, updated_at)
node_source_files  (node_id, path, hash)        PK(node_id, path)   -- provenance
node_symbols       (node_id, symbol)            PK(node_id, symbol)
node_dependencies  (node_id, depends_on)        PK(node_id, depends_on)
node_decisions     (node_id, decision_ref)      PK(node_id, decision_ref)
```

Conventions: paths are repository-relative, `/`-separated (F-00 §5, Windows
reference platform). Timestamps are fixed-width RFC 3339 UTC with millis —
lexicographically sortable under SQLite `BINARY` collation. Raw parse/symbol
results are keyed by content hash (`file_hash_history`), separate from
model-generated summaries (which live in `context_nodes`) per CTX-01.

## 3. CTX-01 file cache

- `FileIndex::scan` — full walk, hash everything (initial index).
- `FileIndex::rescan` — incremental: files whose cached **size + mtime**
  match skip re-hashing. This is a fast path **only**, never a validity
  proof: a same-size rewrite with a restored mtime is invisible to it by
  design.
- `FileIndex::verify_all` — audit pass, re-hashes everything; this is the
  canonical validity check (CTX-01: "never assume cache validity from
  timestamps alone"). Verified by test
  `verify_all_catches_mtime_spoofed_same_size_change` (spoof via
  `File::set_times`, fast path fooled → `verify_all` flags the change).
- Walk is deterministic (children sorted by name). Ignored by default:
  `.git/ target/ node_modules/ .agentos-worktrees/ .agentos/` (the last is
  the daemon's own state dir, so the index never hashes its own database);
  binary extensions and files over 1 MiB are skipped (both configurable in
  `ScanOptions`).
- `language_of(path)` is a pure extension map (rs/ts/tsx/js/py/md/toml/
  json/yaml/sql/css/html/shell; unknown → `Other(ext)`).
- Hashing is BLAKE3 (`blake3::hash(...).to_hex()`), deterministic by
  construction (test: two scans → identical entries).
- Added/changed/deleted paths from a pass are handed to the invalidation
  engine (conservatively, added and deleted included: a node may reference a
  path that only now appeared or just vanished); the resulting events ride
  on the `ScanReport` for the caller to append.

## 4. CTX-02 context graph

`ContextNode { id, topic, version, summary, source_files, source_hashes,
symbols, dependencies, decisions, invalidation_state }` persisted across the
normalized tables above.

- **Provenance is mandatory on write**: every source file must carry a
  non-empty content hash and vice versa (no orphan hashes); empty id/topic
  and `version < 1` are rejected; self-dependencies are rejected;
  dependencies must reference existing nodes. All enforced in
  `graph::validate_node` before any SQL runs.
- `upsert_node` is a full replace (row + child tables) in one transaction;
  `get`/`by_topic`/`all` reassemble nodes with child rows; `delete` cascades
  the child tables.
- `dependents_of(id)` is the transitive reverse-edge closure, cycle-safe,
  sorted.

**Symbols — documented stub.** `symbols_in_source(source, language)` is a
line-scanner: Rust `fn/struct/enum/trait/mod` (with `pub`/`pub(crate)`/
`async` stripped), TS/TSX/JS `function/const/class/interface/type` (with
`export/declare/async` stripped), Python `def/class` (with `async`
stripped); duplicates deduped in first-seen order. `extract_symbols(path,
language)` reads a file and delegates. The seam for replacement by
tree-sitter (F-00 §1 "Parsing") is this single pure function plus the
`node_symbols` table; nothing else in the crate depends on how symbols were
produced.

## 5. CTX-05 invalidation engine

Dirty states (stored in `context_nodes.invalidation_state`):

```text
                file change on a sourced path
   clean ─────────────────────────────────────────> direct-dirty
     ^                                                │
     │ mark_clean(id, new_hashes)                     │ transitive dependents
     │                                                v
   clean <────────────────────────────────────── dependency-dirty
   needs-review: sticky; set via mark_needs_review, untouched by automatic
   propagation until an explicit mark_clean clears it.
```

- `on_files_changed(paths)`: sourcing nodes → `direct-dirty` (unless
  `needs-review`); transitive dependents → `dependency-dirty` (only from
  `clean`, so a direct hit is never downgraded). Conservative per CTX-02.
- Emits `context.invalidated` events — **returned, not appended** (the
  caller owns the append-only journal, F-00 §3): one event per reason group
  with payload `{nodeIds: [...], reason: "direct-dirty"|"dependency-dirty",
  changedFiles: [...]}` and `agentId: "agentos-context"`.
- `states()` — full state map; `recompute_queue()` — direct-dirty first,
  then dependency-dirty (by id within groups, deterministic); this queue is
  the input the CTX-05 "prioritize recomputation for nodes needed by ready
  tasks" hook filters against (the ready-task set arrives from F-06 at the
  call site).
- `mark_clean(id, new_hashes)` — bumps `version`, sets `clean`, upserts the
  given (path, hash) provenance; `mark_needs_review(id)` — sticky flag.

## 6. CTX-04 pack compiler

`PackRequest { role, allowed_paths, context_refs, token_budget }` →
`ContextPack { manifest, materialized_path }`.

**Deterministic retrieval (semantic stage is a stub seam; no model calls):**

1. files under `allowed_paths`, taken from the CTX-01 index (deterministic
   reuse of the cache, no ad-hoc walk);
2. source files of referenced nodes;
3. summaries of referenced nodes **and** of their transitive dependency
   closure (dependencies contribute summaries, not full sources);
4. decision refs of all of the above (MEM-02: refs only).

**Ranking** — explicit score, descending:

```text
score = base(kind) + role_bonus(role, path) − depth(path)
base: referenced node summary 100, decision ref 90, scoped file 85,
      node source file 80, dependency summary 60
```

Role bonus tables (heuristics, not canon):

| Role | Weights |
|---|---|
| frontend | tsx +10, ts +8, css +6, html +4, md/json +2, rs/py/sql −3 |
| backend | rs +8, py/sql +6, toml +2, json +1, ts/tsx −2, html −2, css −4 |
| security | path containing auth/crypto/secret/token/permission/session/key +8; else rs/py +2, ts/tsx/sql +1, md −2, css −4 |
| qa | path containing test/spec +8; else rs/ts/tsx/py +1, css −3 |
| general | 0 |

Ties break by target asc, then kind. Duplicates (same kind + target, e.g. a
node source that is also scope-allowed) keep the higher score.

**Budget** — strict: chunks are packed greedily in rank order while
`running + est ≤ token_budget`; everything else is omitted with reason
`"budget"`. Unknown node refs and missing files are omitted with reasons
`"unknown-node"` / `"missing-file"` rather than silently dropped.

**Token approximation caveat.** `est_tokens = ceil(bytes / 4)`. This is a
deliberately coarse heuristic (roughly one token per 4 bytes of UTF-8-ish
text); real tokenizers vary by language and content by roughly ±30%. The
budget is therefore enforced against the approximation, not a tokenizer —
callers that need a hard model-context guarantee should budget with margin.
No false precision is claimed or exposed beyond this division.

**Materialization.** One ephemeral markdown file per compile
(`context-pack-<uuidv7>.md`) written into a caller-provided output dir:
header with budget accounting, omitted list, context node summaries (each
carrying `id — topic (vN, state)` so workers see freshness per CTX-05),
decision refs, then file contents behind `### \`path\`` headers in
four-backtick fences (safe against triple-backtick content) tagged by
language. The manifest (`included`/`omitted` refs with per-chunk est and
reason, total `est_tokens ≤ token_budget`) is the structured projection of
the same decisions.

## 7. Deviations & notes

- `Language` and `InvalidationState` carry hand-written serde impls
  (string form). `Language` deserialization is total (unknown → `Other`) so
  rows written by newer builds still parse; `InvalidationState`
  deserialization is strict because it is internal state, and unknown
  values read from the DB degrade to `needs-review` (conservative).
- The file index never indexes its own database: `.agentos/` is in the
  default ignore set, and tests place the store DB under
  `<root>/.agentos/ctx.sqlite3`.
- SQLITE_BUSY injection is not directly testable without a contending
  writer; the canon path is covered by construction (rule ordering in
  `open_db`) plus the busy-family mapping unit tests, as in F-01/F-09.

## 8. Test evidence

`cd /d/OP/agent-engineering-os && CARGO_TARGET_DIR=target/f08 cargo test -p agentos-context`

23 tests covering: WAL/migration/reopen + busy mapping; language map;
determinism + change detection + fast path; mtime-spoof detection via
`verify_all`; ignore/binary/oversize skipping; new/deleted tracking;
`mark_parsed`; graph CRUD round-trip; provenance enforcement (missing /
empty / orphan hashes, empty id/topic, version 0); dependency existence +
transitive dependents; symbol stub (Rust/TS/Python, dedup, disk read);
invalidation cascade + event payloads + queue order + version bump;
needs-review stickiness; empty-batch no-op; rescan-driven invalidation;
pack compile under budget + well-formed markdown; strict budget trimming;
role weighting between equal-sized files; unknown-node/missing-file
reporting; end-to-end store happy path.

```text
running 23 tests
test db::tests::busy_error_family_maps_to_retryable_core_busy ... ok
test file_index::tests::verify_all_catches_mtime_spoofed_same_size_change ... ok
test invalidation::tests::cascade_direct_and_transitive_dependency_dirty ... ok
test compiler::tests::budget_is_enforced_strictly_and_trims_lowest_ranked_first ... ok
test compiler::tests::role_weighting_decides_between_equal_sized_files ... ok
test file_index::tests::scan_is_deterministic_and_change_detection_works ... ok
test graph::tests::provenance_is_enforced_on_write ... ok
test tests::store_happy_path_scan_invalidate_compile ... ok
... (23 total)

test result: ok. 23 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.15s
```

`CARGO_TARGET_DIR=target/f08 cargo clippy -p agentos-context --all-targets -- -D warnings`
→ clean. `cargo fmt -p agentos-context --check` → clean.
