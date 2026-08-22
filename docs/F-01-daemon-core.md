# F-01 — Daemon Core: SQLite journal + append-only event store

Status: implemented 2026-08-22 · Crate: `crates/agentos-daemon` · Canon: `F-00-CONVENTIONS.md` §3, `docs/HANDOFF-BUILD.md` §4, PRD §18.1–§18.3.

## 1. Scope and design summary

F-01 delivers the daemon's storage core and lifecycle skeleton — no IPC/WebSocket, no supervision yet (F-02+):

- **`src/db.rs`** — `open_db(&Path) -> Result<Connection, CoreError>`: opens/creates the database and brings it to schema version 1 under the F-01 SQLite canon (busy timeout first, read-first journal-mode check, versioned migrations). Also exposes the shared rusqlite→`CoreError` mapping (`map_sqlite_error`, `is_sqlite_busy`).
- **`src/events.rs`** — the append-only journal: `append_event` (explicit transaction, returns `seq`), `events_for_run` (per-run read, index `run_id, seq`), `tail` (projection feed: `seq > after_seq`, `LIMIT`, ordered by `seq`).
- **`src/lib.rs` / `src/main.rs`** — pub modules; a minimal tokio binary: tracing_subscriber with env filter (default `info`), journal path from `AGENTOS_DB` or the OS default, parent-dir creation, `daemon.started` marker event, `ctrl_c` await, graceful-shutdown log.
- **`tests/events_roundtrip.rs`** — the smoke test (append across 2 runs, reopen, round-trip equality, UPDATE/DELETE rejection, busy mapping).

**Row mapping strategy:** `Event` ⇄ row conversion shuttles through agentos-core's own serde implementation — append serializes the `Event` once and splits the wire object (camelCase keys, RFC 3339 `occurredAt`, string event types) into columns; read reassembles that exact wire shape and deserializes. The PRD wire conventions — including `EventType::Other` round-tripping verbatim and tolerance of unknown JSON keys — are therefore guaranteed by construction rather than re-implemented. This also avoids adding a direct `chrono` dependency to the crate.

## 2. Schema (migration v1, guarded by `PRAGMA user_version`)

```sql
CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    id             TEXT    NOT NULL UNIQUE,   -- event id (UUIDv7)
    event_type     TEXT    NOT NULL,          -- dotted snake_case wire string
    occurred_at    TEXT    NOT NULL,          -- RFC 3339
    run_id         TEXT,
    trace_id       TEXT,
    task_id        TEXT,
    agent_id       TEXT,
    payload        TEXT    NOT NULL,          -- JSON ('null' when offloaded)
    payload_ref    TEXT,                      -- content-addressed artifact ref
    payload_hash   TEXT,                      -- integrity hash of offloaded payload
    schema_version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_run_id_seq     ON events (run_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_event_type_seq ON events (event_type, seq);

-- PRD §18.2: immutable after append; corrections are new events.
CREATE TRIGGER IF NOT EXISTS events_no_update
BEFORE UPDATE ON events BEGIN
    SELECT RAISE(ABORT, 'events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete
BEFORE DELETE ON events BEGIN
    SELECT RAISE(ABORT, 'events is append-only'); END;
```

Migrations run inside one explicit transaction (`CREATE`/index/trigger batch + `user_version` bump), so a crash mid-migration cannot leave a half-migrated database. Databases with a `user_version` newer than this build are refused with a clear error rather than silently downgraded.

## 3. How the SQLite canon (HANDOFF-BUILD §4) is satisfied

| Canon rule | Implementation |
|---|---|
| `busy_timeout` on EVERY connection | First statement applied in `open_db`, before anything that can lock: `conn.busy_timeout(5000ms)`. |
| NEVER re-issue `PRAGMA journal_mode` unconditionally | `ensure_wal` first *reads* `PRAGMA journal_mode`; only if the result is not already `wal` does it issue `journal_mode=WAL`. |
| journal_mode can return SQLITE_BUSY without honoring the busy handler | The conditional WAL set is retried up to 5× with 200 ms delays; exhausted retries surface as `CoreError::SqliteBusy`. |
| Explicit transactions around multi-table writes | Migration batch: one `transaction()`..`commit()`. `append_event`: `BEGIN IMMEDIATE` → INSERT → `COMMIT`, `ROLLBACK` on failure. `BEGIN IMMEDIATE` takes the write lock up front (WAL citizenship). |
| Surface SQLITE_BUSY as retryable | `is_sqlite_busy` matches primary code `SQLITE_BUSY` via `ErrorCode::DatabaseBusy` **or** any extended code with primary byte 5 (`SQLITE_BUSY_RECOVERY`, `SQLITE_BUSY_SNAPSHOT`, …) → `CoreError::SqliteBusy`, for which `CoreError::is_retryable()` is true. Proven by `busy_maps_to_retryable_core_error`: append under a held write lock yields `SqliteBusy`, and the identical append succeeds after the lock is released. |
| Event log append-only (PRD §18.2) | Enforced by the storage engine (triggers), not caller discipline; UI state stays a projection (`tail` feed, never mutated rows). |

## 4. Event-model rules (PRD §18.2 / F-00 §3)

- **Append-only, corrections are new events** — DB triggers abort UPDATE/DELETE with `'events is append-only'`; asserted in the integration test.
- **`run_id` where applicable + `trace_id`** — nullable columns; `run_id` indexes the per-run read. The daemon's own `daemon.started` marker carries no `run_id` (daemon lifecycle is not run-scoped — "where applicable") and does carry a fresh `trace_id`.
- **Idempotent consumers** — consumer obligation; the store supports redelivery via deterministic `tail(after_seq, limit)` paging keyed on the monotonic `seq`.
- **Large payloads → artifacts with refs + hashes** — `payload` / `payload_ref` / `payload_hash` columns mirror the `Event` fields; offloaded payloads store JSON `null` inline with ref+hash, round-trip tested in unit and integration tests.

## 5. Binary behavior

- `AGENTOS_DB` env overrides the journal path; default `%LOCALAPPDATA%/agentos/daemon.db` on Windows, `$HOME/.local/state/agentos/daemon.db` elsewhere. Parent directories are created.
- Startup logs the resolved journal path, appends the `daemon.started` marker, then awaits `ctrl_c` and logs a graceful shutdown. Exit code 0 on the graceful path, 1 on any startup failure.

## 6. Smoke-test evidence (fresh output, 2026-08-22)

`CARGO_TARGET_DIR=target/daemon cargo test -p agentos-daemon`:

```
running 3 tests
test events::tests::offloaded_payload_flattens_to_ref_and_hash ... ok
test events::tests::event_deserialization_tolerates_unknown_keys ... ok
test events::tests::other_event_type_survives_column_flattening ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
running 2 tests
test events_round_trip_across_reopened_database ... ok
test busy_maps_to_retryable_core_error ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.16s
```

`CARGO_TARGET_DIR=target/daemon cargo clippy -p agentos-daemon --all-targets -- -D warnings`:

```
    Checking agentos-daemon v0.1.0 (D:\OP\agent-engineering-os\crates\agentos-daemon)
    Finished `dev` profile [unoptimized +debuginfo] target(s) in 0.63s
```

`cargo fmt -p agentos-daemon --check`: clean.

Binary smoke run (`AGENTOS_DB=<tmp>/f01-smoke/daemon.db target/daemon/debug/agentos-daemon.exe`):

```
2026-08-22T08:37:09.537811Z INFO agentos_daemon: journal open (SQLite, WAL, busy_timeout=5000ms) journal=C:/Users/study/AppData/Local/Temp/tmp.Vtb7XDtAbC/f01-smoke/daemon.db
2026-08-22T08:37:09.540020Z INFO agentos_daemon: startup marker appended seq=1 event_type=daemon.started
2026-08-22T08:37:09.540073Z INFO agentos_daemon: agentos-daemon ready; waiting for ctrl-c
```

Side observations: `daemon.db-wal` / `daemon.db-shm` files confirm WAL; a second start against the same file appended its marker at `seq=2` (journal continuity across restarts).

## 7. Deviations and open notes

1. **Non-busy SQLite errors** map to `CoreError::Serialization("sqlite: …")` because agentos-core deliberately keeps a minimal taxonomy (no generic DB-error variant) and F-01 must not edit that crate. Such errors are non-retryable and greppable via the `sqlite:` prefix. If a generic variant lands in core later, `db::map_sqlite_error` is the single switch point.
2. **No `chrono` dependency added** to this crate; timestamps flow through agentos-core's serde (RFC 3339) via the JSON shuttle described in §1.
3. **Graceful ctrl-c path not smoke-proven in this environment.** Programmatic delivery of a real `CTRL_C_EVENT` to the console-less daemon was attempted three ways (`GenerateConsoleCtrlEvent` broadcast, `CREATE_NEW_PROCESS_GROUP` + group-targeted event, `FreeConsole`/`AttachConsole`); MSYS/background constraints defeated all of them (CTRL_BREAK *is* delivered and terminates the process, with `0xC000013A` — tokio's `ctrl_c` claims only CTRL_C). Verified instead: startup, WAL journal creation, marker append, and process exit (via taskkill and via console control event). The `ctrl_c().await` branch is the canonical tokio pattern; interactive Ctrl+C in a real console remains to be observed by a human.
4. **Startup marker** is `EventType::Other("daemon.started")` — a clean fit for the total-parsing `Other` variant; no synthetic `run.created` was forced onto a run-less event.
