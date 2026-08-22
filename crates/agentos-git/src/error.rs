//! Error taxonomy for the F-09 Git manager.

use agentos_core::CoreError;

/// Raw SQLite busy code. The whole `SQLITE_BUSY` family
/// (`SQLITE_BUSY`, `SQLITE_BUSY_SNAPSHOT`, ...) shares the low byte `5`.
const SQLITE_BUSY_BASE: i32 = 5;

/// Errors surfaced by `agentos-git`.
///
/// [`GitError::Core`] carries [`CoreError::SqliteBusy`] for every
/// `SQLITE_BUSY` outcome (F-01 canon: busy is retryable, never a generic
/// failure) — see [`db`].
#[derive(Debug, thiserror::Error)]
pub enum GitError {
    /// A shared core error (e.g. retryable `SQLITE_BUSY`, or `NotFound`).
    #[error(transparent)]
    Core(#[from] CoreError),
    /// A non-busy SQLite failure.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// The `git` CLI failed.
    #[error(transparent)]
    Cli(#[from] crate::cli::GitCliError),
    /// Filesystem failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON (de)serialization failure for ledger/queue payloads.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A push request arrived without approval while push gating is active
    /// (GIT-01: push is approval-gated by default, enforced in code).
    #[error("push denied: push requests require explicit approval (approval-gated by default)")]
    PushNotApproved,
    /// A caller-supplied argument failed validation.
    #[error("invalid argument: {0}")]
    Invalid(String),
}

/// Map a `rusqlite` error into [`GitError`], routing the `SQLITE_BUSY`
/// family to the retryable [`CoreError::SqliteBusy`] (F-01 canon).
pub(crate) fn db(err: rusqlite::Error) -> GitError {
    if let rusqlite::Error::SqliteFailure(ffi, _) = &err {
        if ffi.extended_code & 0xFF == SQLITE_BUSY_BASE {
            return GitError::Core(CoreError::SqliteBusy);
        }
    }
    GitError::Sqlite(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_busy_maps_to_retryable_core_error() {
        let err = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                extended_code: 5,
            },
            None,
        );
        assert!(matches!(db(err), GitError::Core(CoreError::SqliteBusy)));
        let snapshot = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                extended_code: 5 | (1 << 8),
            },
            None,
        );
        assert!(matches!(
            db(snapshot),
            GitError::Core(CoreError::SqliteBusy)
        ));
    }

    #[test]
    fn other_sqlite_errors_stay_typed() {
        let err = rusqlite::Error::QueryReturnedNoRows;
        assert!(matches!(db(err), GitError::Sqlite(_)));
    }
}
