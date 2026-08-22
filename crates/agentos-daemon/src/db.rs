//! SQLite open/init helper — the F-01 canon (HANDOFF-BUILD §4) lives here.
//!
//! Canon, in the exact order [`open_db`] applies it:
//!
//! 1. `busy_timeout` is set **first** on every connection, so every later
//!    lock acquisition honors the busy handler instead of failing fast.
//! 2. `journal_mode` is **read first** and WAL is only requested when the
//!    database is not already in WAL. Re-issuing the pragma unconditionally
//!    can return `SQLITE_BUSY` *without honoring the busy handler* (the
//!    observed race this canon comes from), so the conditional set is retried
//!    a few times with a small delay.
//! 3. Schema migrations run inside an explicit transaction, versioned via
//!    `PRAGMA user_version`.
//!
//! SQLITE_BUSY (including `SQLITE_BUSY_SNAPSHOT` and friends — any extended
//! code whose primary code is `SQLITE_BUSY`) maps to
//! [`CoreError::SqliteBusy`], which [`CoreError::is_retryable`] reports as
//! retryable so callers can back off instead of failing the operation.

use std::path::Path;
use std::time::Duration;

use agentos_core::CoreError;
use rusqlite::{Connection, ErrorCode};

/// Busy-handler wait applied to every connection opened by [`open_db`].
const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

/// Retries for the conditional `journal_mode=WAL` set. The busy handler does
/// not apply to this pragma, so contention is resolved by re-trying.
const WAL_SET_ATTEMPTS: u32 = 5;

/// Delay between `journal_mode=WAL` retries.
const WAL_SET_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Highest schema version this build understands. Migrations are versioned
/// via `PRAGMA user_version`; v1 creates the append-only `events` journal.
const SCHEMA_VERSION: i64 = 1;

/// Migration v1: the append-only event journal (PRD §18.1 "Event" entity,
/// §18.2 immutability rules).
const MIGRATION_V1: &str = r#"
-- Append-only event journal (PRD §18.1). Corrections are new events,
-- never UPDATEs or DELETEs — enforced by the triggers below.
CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    id             TEXT    NOT NULL UNIQUE,
    event_type     TEXT    NOT NULL,
    occurred_at    TEXT    NOT NULL,
    run_id         TEXT,
    trace_id       TEXT,
    task_id        TEXT,
    agent_id       TEXT,
    payload        TEXT    NOT NULL,
    payload_ref    TEXT,
    payload_hash   TEXT,
    schema_version INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_run_id_seq
    ON events (run_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_event_type_seq
    ON events (event_type, seq);

-- PRD §18.2: events are immutable after append.
CREATE TRIGGER IF NOT EXISTS events_no_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS events_no_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events is append-only');
END;
"#;

/// Open (creating if needed) the daemon database at `path` and bring it to
/// the current schema version.
///
/// Applies the F-01 canon in order: busy timeout first, then a read-first
/// check of `journal_mode` (WAL only if not already WAL), then versioned
/// migrations. See the [module docs](self) for the reasoning behind the
/// journal-mode dance.
pub fn open_db(path: &Path) -> Result<Connection, CoreError> {
    let mut conn = Connection::open(path).map_err(map_sqlite_error)?;

    // Canon rule 1: busy handler before anything else that can lock.
    conn.busy_timeout(BUSY_TIMEOUT).map_err(map_sqlite_error)?;

    // Canon rule 2: read journal_mode; only set WAL when it is not already
    // active. Never re-issue the pragma unconditionally.
    ensure_wal(&conn)?;

    // Canon rule 3: explicit transaction around the (multi-statement)
    // schema migration.
    migrate(&mut conn)?;

    Ok(conn)
}

/// Read `journal_mode`; if it is not already `wal`, set it, retrying a few
/// times because this pragma can return SQLITE_BUSY without honoring the
/// busy handler.
fn ensure_wal(conn: &Connection) -> Result<(), CoreError> {
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(map_sqlite_error)?;
    if mode.eq_ignore_ascii_case("wal") {
        return Ok(());
    }

    for attempt in 0..WAL_SET_ATTEMPTS {
        if attempt > 0 {
            std::thread::sleep(WAL_SET_RETRY_DELAY);
        }
        match conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0)) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => return Ok(()),
            Ok(mode) => {
                return Err(CoreError::Serialization(format!(
                    "sqlite: PRAGMA journal_mode=WAL did not take effect (reported {mode})"
                )));
            }
            // SQLITE_BUSY here bypasses the busy handler; retry after a delay.
            Err(err) if is_sqlite_busy(&err) => continue,
            Err(err) => return Err(map_sqlite_error(err)),
        }
    }

    Err(CoreError::SqliteBusy)
}

/// Apply pending schema migrations, versioned via `PRAGMA user_version`.
/// Migration DDL runs inside an explicit transaction so a crash mid-migration
/// cannot leave a half-migrated database.
fn migrate(conn: &mut Connection) -> Result<(), CoreError> {
    let current: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(map_sqlite_error)?;

    if current > SCHEMA_VERSION {
        return Err(CoreError::Serialization(format!(
            "sqlite: database user_version {current} is newer than this build supports \
             (schema version {SCHEMA_VERSION}); upgrade the daemon first"
        )));
    }
    if current == SCHEMA_VERSION {
        return Ok(());
    }

    let tx = conn.transaction().map_err(map_sqlite_error)?;
    tx.execute_batch(MIGRATION_V1).map_err(map_sqlite_error)?;
    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))
        .map_err(map_sqlite_error)?;
    tx.commit().map_err(map_sqlite_error)?;
    Ok(())
}

/// Whether a rusqlite error is any flavour of SQLITE_BUSY (primary code 5,
/// including SQLITE_BUSY_RECOVERY and SQLITE_BUSY_SNAPSHOT).
pub fn is_sqlite_busy(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(ffi_err, _) => {
            ffi_err.code == ErrorCode::DatabaseBusy
                || (ffi_err.extended_code & 0xff) == rusqlite::ffi::SQLITE_BUSY
        }
        _ => false,
    }
}

/// Translate a rusqlite error into the shared [`CoreError`] taxonomy.
///
/// SQLITE_BUSY maps to [`CoreError::SqliteBusy`] (retryable). agentos-core
/// deliberately keeps a minimal taxonomy — there is no generic
/// "database error" variant — so every non-busy SQLite failure is surfaced
/// as [`CoreError::Serialization`] with an explicit `sqlite:` prefix on the
/// detail string, keeping such failures non-retryable and greppable. See the
/// deviations note in `docs/F-01-daemon-core.md`.
pub fn map_sqlite_error(err: rusqlite::Error) -> CoreError {
    if is_sqlite_busy(&err) {
        CoreError::SqliteBusy
    } else {
        CoreError::Serialization(format!("sqlite: {err}"))
    }
}
