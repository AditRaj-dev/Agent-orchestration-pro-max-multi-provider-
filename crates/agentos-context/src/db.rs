//! Private SQLite open/init helper — the F-01 canon (`docs/HANDOFF-BUILD.md`
//! §4) lives here, mirroring `agentos-daemon/src/db.rs`:
//!
//! 1. `busy_timeout` is set **first** on every connection, so every later
//!    lock acquisition honors the busy handler instead of failing fast.
//! 2. `journal_mode` is **read first** and WAL is only requested when the
//!    database is not already in WAL. Re-issuing the pragma unconditionally
//!    can return `SQLITE_BUSY` *without honoring the busy handler* (the
//!    observed race this canon comes from), so the conditional set is
//!    retried a few times with a small delay.
//! 3. Schema migrations run inside an explicit transaction, versioned via
//!    `PRAGMA user_version`.
//!
//! Any flavour of `SQLITE_BUSY` maps to [`CoreError::SqliteBusy`] (retryable,
//! surfaced distinctly) via [`map_sqlite`].

use std::path::Path;
use std::time::Duration;

use agentos_core::CoreError;
use chrono::{SecondsFormat, Utc};
use rusqlite::{Connection, ErrorCode};

use crate::ContextError;

/// Busy-handler wait applied to every connection opened by [`open_db`].
const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

/// Retries for the conditional `journal_mode=WAL` set. The busy handler does
/// not apply to this pragma, so contention is resolved by re-trying.
const WAL_SET_ATTEMPTS: u32 = 5;

/// Delay between `journal_mode=WAL` retries.
const WAL_SET_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Highest schema version this build understands, versioned via
/// `PRAGMA user_version`. v1 creates the F-08 context schema: the CTX-01
/// file cache, the CTX-02 normalized context graph, and its child tables.
const SCHEMA_VERSION: i64 = 1;

/// Migration v1: file cache + context graph schema (CTX-01/CTX-02).
const MIGRATION_V1: &str = r#"
-- CTX-01 repository file cache. Paths are repository-relative with '/'
-- separators (F-00 §5: POSIX paths in code on the Windows reference
-- platform). mtime columns exist ONLY as a rescan fast path; validity is
-- always content-hash based (never trust timestamps alone).
CREATE TABLE IF NOT EXISTS files (
    path            TEXT    PRIMARY KEY,
    content_hash    TEXT    NOT NULL,
    size            INTEGER NOT NULL,
    language        TEXT    NOT NULL,
    parsed_version  INTEGER NOT NULL DEFAULT 1,
    dirty           INTEGER NOT NULL DEFAULT 0,
    mtime_secs      INTEGER NOT NULL DEFAULT 0,
    mtime_nanos     INTEGER NOT NULL DEFAULT 0,
    first_seen_at   TEXT    NOT NULL,
    last_hashed_at  TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_files_content_hash ON files (content_hash);

-- Content-hash history per path (CTX-01: cache parse/symbol results keyed by
-- the content hash, separately from model-generated summaries).
CREATE TABLE IF NOT EXISTS file_hash_history (
    path         TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    seen_at      TEXT NOT NULL,
    PRIMARY KEY (path, content_hash)
);

-- CTX-02 context nodes. invalidation_state is the CTX-05 dirty state:
-- clean | direct-dirty | dependency-dirty | needs-review.
CREATE TABLE IF NOT EXISTS context_nodes (
    id                 TEXT    PRIMARY KEY,
    topic              TEXT    NOT NULL,
    version            INTEGER NOT NULL DEFAULT 1,
    summary            TEXT    NOT NULL DEFAULT '',
    invalidation_state TEXT    NOT NULL DEFAULT 'clean',
    created_at         TEXT    NOT NULL,
    updated_at         TEXT    NOT NULL
);

-- Provenance: every sourced node maps to >= 1 (path, content_hash) row.
CREATE TABLE IF NOT EXISTS node_source_files (
    node_id TEXT NOT NULL,
    path    TEXT NOT NULL,
    hash    TEXT NOT NULL,
    PRIMARY KEY (node_id, path)
);

CREATE INDEX IF NOT EXISTS idx_node_source_files_path ON node_source_files (path);

CREATE TABLE IF NOT EXISTS node_symbols (
    node_id TEXT NOT NULL,
    symbol  TEXT NOT NULL,
    PRIMARY KEY (node_id, symbol)
);

-- Edge (node_id depends_on depends_on_id): node_id's summary is invalid
-- (conservatively) when depends_on changes.
CREATE TABLE IF NOT EXISTS node_dependencies (
    node_id    TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    PRIMARY KEY (node_id, depends_on)
);

CREATE INDEX IF NOT EXISTS idx_node_dependencies_depends_on
    ON node_dependencies (depends_on);

-- MEM-02 decision ledger refs only; the ledger itself lives elsewhere.
CREATE TABLE IF NOT EXISTS node_decisions (
    node_id       TEXT NOT NULL,
    decision_ref TEXT NOT NULL,
    PRIMARY KEY (node_id, decision_ref)
);
"#;

/// Open (creating if needed) the context database at `path` and bring it to
/// the current schema version, applying the F-01 canon in order.
pub(crate) fn open_db(path: &Path) -> Result<Connection, ContextError> {
    let mut conn = Connection::open(path).map_err(map_sqlite)?;

    // Canon rule 1: busy handler before anything else that can lock.
    conn.busy_timeout(BUSY_TIMEOUT).map_err(map_sqlite)?;

    // Canon rule 2: read journal_mode; only set WAL when it is not already
    // active. Never re-issue the pragma unconditionally.
    ensure_wal(&conn)?;

    // Canon rule 3: explicit transaction around the schema migration.
    migrate(&mut conn)?;

    Ok(conn)
}

/// Read `journal_mode`; if it is not already `wal`, set it, retrying a few
/// times because this pragma can return SQLITE_BUSY without honoring the
/// busy handler.
fn ensure_wal(conn: &Connection) -> Result<(), ContextError> {
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(map_sqlite)?;
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
                return Err(ContextError::Validation(format!(
                    "sqlite: PRAGMA journal_mode=WAL did not take effect (reported {mode})"
                )));
            }
            // SQLITE_BUSY here bypasses the busy handler; retry after a delay.
            Err(err) if is_sqlite_busy(&err) => continue,
            Err(err) => return Err(map_sqlite(err)),
        }
    }

    Err(ContextError::Core(CoreError::SqliteBusy))
}

/// Apply pending schema migrations, versioned via `PRAGMA user_version`.
/// Migration DDL runs inside an explicit transaction so a crash mid-migration
/// cannot leave a half-migrated database.
fn migrate(conn: &mut Connection) -> Result<(), ContextError> {
    let current: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(map_sqlite)?;

    if current > SCHEMA_VERSION {
        return Err(ContextError::Validation(format!(
            "sqlite: database user_version {current} is newer than this build supports \
             (schema version {SCHEMA_VERSION}); upgrade agentos-context first"
        )));
    }
    if current == SCHEMA_VERSION {
        return Ok(());
    }

    let tx = conn.transaction().map_err(map_sqlite)?;
    tx.execute_batch(MIGRATION_V1).map_err(map_sqlite)?;
    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))
        .map_err(map_sqlite)?;
    tx.commit().map_err(map_sqlite)?;
    Ok(())
}

/// Whether a rusqlite error is any flavour of SQLITE_BUSY (primary code 5,
/// including SQLITE_BUSY_RECOVERY and SQLITE_BUSY_SNAPSHOT).
pub(crate) fn is_sqlite_busy(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(ffi_err, _) => {
            ffi_err.code == ErrorCode::DatabaseBusy
                || (ffi_err.extended_code & 0xff) == rusqlite::ffi::SQLITE_BUSY
        }
        _ => false,
    }
}

/// Translate a rusqlite error into the shared [`ContextError`] taxonomy,
/// routing the SQLITE_BUSY family to the retryable, distinctly surfaced
/// [`CoreError::SqliteBusy`].
pub(crate) fn map_sqlite(err: rusqlite::Error) -> ContextError {
    if is_sqlite_busy(&err) {
        ContextError::Core(CoreError::SqliteBusy)
    } else {
        ContextError::Sqlite(err)
    }
}

/// Canonical UTC timestamp for TEXT columns. Fixed-width RFC 3339 with
/// millisecond precision and a `Z` suffix sorts lexicographically in SQLite's
/// BINARY collation, so string comparison is a valid time comparison for
/// values produced by this crate.
pub(crate) fn now_ts() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_in_wal_mode_migrates_and_is_reopenable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("context.sqlite3");

        let conn = open_db(&db_path).expect("open");
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal mode");
        assert_eq!(mode.to_ascii_lowercase(), "wal");
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user version");
        assert_eq!(version, SCHEMA_VERSION);
        // Reopening an already-WAL, already-migrated database is a no-op.
        drop(conn);
        let _again = open_db(&db_path).expect("reopen");
    }

    #[test]
    fn busy_error_family_maps_to_retryable_core_busy() {
        let plain = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                extended_code: 5,
            },
            None,
        );
        let snapshot = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                extended_code: 5 | (1 << 8),
            },
            None,
        );
        assert!(matches!(
            map_sqlite(plain),
            ContextError::Core(CoreError::SqliteBusy)
        ));
        assert!(matches!(
            map_sqlite(snapshot),
            ContextError::Core(CoreError::SqliteBusy)
        ));
        assert!(matches!(
            map_sqlite(rusqlite::Error::QueryReturnedNoRows),
            ContextError::Sqlite(_)
        ));
    }
}
