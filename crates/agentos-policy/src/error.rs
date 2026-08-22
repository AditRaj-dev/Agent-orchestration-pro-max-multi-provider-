//! Error taxonomy for the F-10 policy engine.

use agentos_core::CoreError;

/// Raw SQLite busy code. The whole `SQLITE_BUSY` family
/// (`SQLITE_BUSY`, `SQLITE_BUSY_SNAPSHOT`, ...) shares the low byte `5`.
const SQLITE_BUSY_BASE: i32 = 5;

/// Errors surfaced by `agentos-policy`.
///
/// [`PolicyError::Core`] carries [`CoreError::SqliteBusy`] for every
/// `SQLITE_BUSY` outcome (F-01 canon, `docs/HANDOFF-BUILD.md` §4: busy is
/// retryable, never a generic failure) — see [`db`].
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    /// A shared core error (e.g. retryable `SQLITE_BUSY`, or `NotFound`).
    #[error(transparent)]
    Core(#[from] CoreError),
    /// A non-busy SQLite failure.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// JSON (de)serialization failure for approval/audit payloads.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A caller-supplied argument failed validation (unknown secret scope,
    /// negative ttl, ...). Deterministic — never retry.
    #[error("invalid argument: {0}")]
    Invalid(String),
}

/// Map a `rusqlite` error into [`PolicyError`], routing the `SQLITE_BUSY`
/// family to the retryable [`CoreError::SqliteBusy`] (F-01 canon).
pub(crate) fn db(err: rusqlite::Error) -> PolicyError {
    if let rusqlite::Error::SqliteFailure(ffi, _) = &err {
        if ffi.extended_code & 0xFF == SQLITE_BUSY_BASE {
            return PolicyError::Core(CoreError::SqliteBusy);
        }
    }
    PolicyError::Sqlite(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_busy_family_maps_to_retryable_core_error() {
        let err = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                extended_code: 5,
            },
            None,
        );
        assert!(matches!(db(err), PolicyError::Core(CoreError::SqliteBusy)));
        // The snapshot variant (busy | 1<<8) must stay in the busy family.
        let snapshot = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                extended_code: 5 | (1 << 8),
            },
            None,
        );
        assert!(matches!(
            db(snapshot),
            PolicyError::Core(CoreError::SqliteBusy)
        ));
    }

    #[test]
    fn other_sqlite_errors_stay_typed() {
        let err = rusqlite::Error::QueryReturnedNoRows;
        assert!(matches!(db(err), PolicyError::Sqlite(_)));
    }
}
