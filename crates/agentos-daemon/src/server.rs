//! F-11a: the daemon's read-only WebSocket JSON API over the journal.
//!
//! CONTRACT: `docs/F-11-desktop.md` §2–§3 (framing, methods, projections).
//! The desktop app is a pure client of this surface; drift between this
//! module and the F-doc is a bug, not an evolution.
//!
//! Pre-wired placeholder — the F-11a build agent implements:
//!
//! - the frame codec (request/response/notification, `id` echo, error codes
//!   `invalid_request | method_not_found | invalid_params | internal_error |
//!   not_supported`),
//! - the method table (`ping`, `daemon.info`, `events.list`,
//!   `events.subscribe`/`unsubscribe`, `runs.list`, `tasks.list`,
//!   `agents.list`, `git.diff` → `not_supported`),
//! - per-subscription replay-then-tail loops polling `events::tail` every
//!   250 ms (WAL readers never block writers),
//! - loopback-only bind (`127.0.0.1:8741`, `AGENTOS_WS_ADDR` override),
//!   `daemon.stopping` broadcast, close code 1001.

/// Default bind address (loopback only — F-11 §2).
pub const DEFAULT_WS_ADDR: &str = "127.0.0.1:8741";

/// Environment variable overriding the bind address.
pub const WS_ADDR_ENV: &str = "AGENTOS_WS_ADDR";

/// Subscription tail poll interval (F-11 §3.2 contract).
pub const TAIL_POLL_INTERVAL_MS: u64 = 250;
