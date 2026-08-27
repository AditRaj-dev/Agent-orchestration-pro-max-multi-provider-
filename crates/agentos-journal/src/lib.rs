//! # agentos-journal
//!
//! The F-01 storage floor, split out of `agentos-daemon` so both the daemon
//! and the runtime supervisor can sit on it without a dependency cycle
//! (the daemon needs the supervisor for the F-12 mastermind service; the
//! supervisor has always needed the journal).
//!
//! - [`db`] opens/initializes the database under the F-01 SQLite canon
//!   (HANDOFF-BUILD §4): `busy_timeout` first on every connection, a
//!   read-first `journal_mode` check before ever setting WAL, versioned
//!   migrations via `PRAGMA user_version`, and SQLITE_BUSY surfaced as
//!   retryable [`agentos_core::CoreError::SqliteBusy`].
//! - [`events`] is the append-only journal: appends inside explicit
//!   transactions, per-run reads, and `tail` feeds for projections.
//!
//! `agentos_daemon::db` and `agentos_daemon::events` remain valid paths —
//! the daemon re-exports both modules verbatim.

#![forbid(unsafe_code)]

pub mod db;
pub mod events;
