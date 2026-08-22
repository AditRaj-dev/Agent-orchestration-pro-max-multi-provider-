//! # agentos-daemon
//!
//! F-01 daemon core: SQLite state store and the append-only event journal
//! (PRD §18, F-00 §3 event rules).
//!
//! - [`db`] opens/initializes the database under the F-01 SQLite canon
//!   (HANDOFF-BUILD §4): `busy_timeout` first on every connection, a
//!   read-first `journal_mode` check before ever setting WAL, versioned
//!   migrations via `PRAGMA user_version`, and SQLITE_BUSY surfaced as
//!   retryable [`agentos_core::CoreError::SqliteBusy`].
//! - [`events`] is the append-only journal: appends inside explicit
//!   transactions, per-run reads, and a `tail` feed for projections. Row
//!   mapping reuses agentos-core's serde conventions for
//!   [`agentos_core::Event`] so the wire format stays canonical.
//!
//! Later features (F-02+ supervision, IPC/WebSocket) build on these two
//! modules; this crate intentionally contains no business logic yet.

#![forbid(unsafe_code)]

pub mod db;
pub mod events;
pub mod projection;
pub mod server;
