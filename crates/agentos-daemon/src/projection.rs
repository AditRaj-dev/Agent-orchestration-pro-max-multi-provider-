//! F-11a: journal → UI projections (runs / tasks / agents).
//!
//! CONTRACT: `docs/F-11-desktop.md` §3.3 — the fold table is binding. UI
//! state is a projection, never a second source of truth (F-00 §3): these
//! types are re-derived from the append-only journal on demand; nothing
//! here ever writes.
//!
//! Pre-wired placeholder — the F-11a build agent implements `RunSummary`,
//! `TaskSummary`, `AgentSummary` and the fold, with serde camelCase wire
//! shapes exactly as specified in the F-doc, tested against synthetic
//! journals (including unknown event types — the fold must be total).
