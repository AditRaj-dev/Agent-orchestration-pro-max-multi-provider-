//! SQLite access canon shared by the mutation queue and the agent ledger
//! (F-01 canon, `docs/HANDOFF-BUILD.md` §4):
//!
//! - `busy_timeout` on every connection;
//! - never re-issue `PRAGMA journal_mode` unconditionally — read it first,
//!   switch to WAL only when the stored mode differs (re-issuing the pragma
//!   can return `SQLITE_BUSY` without honoring `busy_timeout`);
//! - `SQLITE_BUSY` surfaces as retryable [`agentos_core::CoreError::SqliteBusy`]
//!   via [`crate::error::db`].

use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use rusqlite::Connection;

use crate::error::{db, GitError};

/// Busy timeout applied to every connection opened by this crate.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Open (creating if needed) a SQLite database following the F-01 canon.
pub(crate) fn open_db(path: &Path) -> Result<Connection, GitError> {
    let conn = Connection::open(path).map_err(db)?;
    conn.busy_timeout(BUSY_TIMEOUT).map_err(db)?;
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(db)?;
    if !mode.eq_ignore_ascii_case("wal") {
        conn.execute_batch("PRAGMA journal_mode=WAL;").map_err(db)?;
    }
    Ok(conn)
}

/// Canonical UTC timestamp for TEXT columns. Fixed-width RFC 3339 with
/// millisecond precision and a `Z` suffix sorts lexicographically in SQLite's
/// BINARY collation, so string comparison is a valid time comparison for
/// values produced by this crate.
pub(crate) fn now_ts() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Lock a mutex holding shared state, recovering from poisoning: a panicked
/// previous owner leaves an intact `Connection`/map, and SQLite itself (via
/// `busy_timeout`) remains the arbiter of on-disk consistency, so recovery
/// beats cascading the panic.
pub(crate) fn lock_guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_in_wal_mode_and_is_reopenable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("store.sqlite3");
        let conn = open_db(&db_path).expect("open");
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal mode");
        assert_eq!(mode.to_ascii_lowercase(), "wal");
        // Reopening an already-WAL database must not re-issue the switch.
        drop(conn);
        let _again = open_db(&db_path).expect("reopen");
    }

    #[test]
    fn timestamps_are_fixed_width_and_sortable() {
        let a = now_ts();
        let b = now_ts();
        assert!(a <= b, "RFC 3339 millis strings must sort chronologically");
        assert!(a.ends_with('Z'));
    }
}
