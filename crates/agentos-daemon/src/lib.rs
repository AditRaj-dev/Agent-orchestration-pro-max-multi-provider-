//! # agentos-daemon
//!
//! F-01 daemon core: SQLite state store and the append-only event journal
//! (PRD §18, F-00 §3 event rules), plus the F-11a read-only WebSocket API
//! desktop clients talk to.
//!
//! - [`db`] opens/initializes the database under the F-01 SQLite canon
//!   (HANDOFF-BUILD §4): `busy_timeout` first on every connection, a
//!   read-first `journal_mode` check before ever setting WAL, versioned
//!   migrations via `PRAGMA user_version`, and SQLITE_BUSY surfaced as
//!   retryable [`agentos_core::CoreError::SqliteBusy`].
//! - [`events`] is the append-only journal: appends inside explicit
//!   transactions, per-run reads, and `tail` feeds for projections. Row
//!   mapping reuses agentos-core's serde conventions for
//!   [`agentos_core::Event`] so the wire format stays canonical; the
//!   F-11a seq-carrying variants (`tail_with_seq`, `journal_stats`) live
//!   here too.
//! - [`projection`] folds the journal into the F-11 §3.3 UI summaries
//!   (runs/tasks/agents) — a pure read-side projection, never a second
//!   source of truth.
//! - [`agent_sessions`] is the F-13 chat-session service: registry agents
//!   (creator, researcher, …) spawned over their record's adapter with
//!   skill preambles composed ahead of the message, adapter events
//!   translated into journal events.
//! - [`mastermind`] is the F-12 join: the orchestrator's planning cycles,
//!   the user-gated commit, and the supervisor drive that actually spawns
//!   registry agents, exposed as the `mastermind.*` methods.
//! - [`server`] is the loopback WebSocket JSON API (framing, method table,
//!   replay-then-tail subscriptions, `daemon.stopping` broadcast) — since
//!   F-13 it also carries the registry write methods and the chat-session
//!   methods over [`agent_sessions`].
//! - [`seed`] appends the frozen demo fixture into an explicit throwaway
//!   journal (F-11 §3.4) for UI development without a live supervisor.
//!
//! Later features (F-02+ supervision in-process, IPC surface) build on
//! these modules.

#![forbid(unsafe_code)]

pub mod agent_sessions;
pub mod chat_history;
pub mod conversation;
pub mod mastermind;
pub mod memex;
pub mod projection;
pub mod projects;
pub mod seed;
pub mod server;
pub mod skill_import;
pub mod usage_meters;

/// The F-01 storage floor, now owned by `agentos-journal` and re-exported
/// here so every existing `agentos_daemon::db` / `agentos_daemon::events`
/// path keeps working.
pub use agentos_journal::{db, events};
