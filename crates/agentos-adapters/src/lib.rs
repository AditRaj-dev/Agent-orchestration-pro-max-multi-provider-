//! # agentos-adapters
//!
//! F-02: the provider-independent [`RuntimeAdapter`] contract, the
//! [`AdapterEvent`] streaming model, the [`AdapterFailure`] taxonomy with
//! its exit-code/final-event [`Classifier`], and a credential-free
//! [`MockAdapter`] that makes end-to-end runs possible from day one.
//!
//! Canon sources (binding): `F-00-CONVENTIONS.md` §4 (provider-adapter
//! canon table), `handoff.md` §3 + addenda (observed CLI contracts:
//! claude result schema, agy JSON contract, zcode typed provider business
//! errors), PRD §8 (RT-01 adapter interface, RT-06 health/capability
//! discovery — health checks are never billable).
//!
//! Concrete CLI adapters land in later PRs: Claude Code (F-03), codex
//! (F-04), agy + zcode (F-05/F-05b). They implement the same trait and
//! reuse the taxonomy and classifier defined here.
//!
//! ```no_run
//! use agentos_adapters::mock::{MockAdapter, MockBehavior};
//! use agentos_adapters::{RuntimeAdapter, SpawnSpec};
//! use std::path::PathBuf;
//! use uuid::Uuid;
//!
//! # async fn demo() {
//! let adapter = MockAdapter::new(MockBehavior::Success {
//!     turns: 2,
//!     files_changed: vec!["src/lib.rs".to_owned()],
//! });
//! let spec = SpawnSpec {
//!     task_id: Uuid::new_v4(),
//!     objective: "add a feature".to_owned(),
//!     workspace: PathBuf::from("worktrees/task-1"),
//!     allowed_paths: vec![],
//!     forbidden_paths: vec![],
//!     tool_allowlist: vec![],
//!     tool_denylist: vec![],
//!     model: None,
//!     timeout_secs: 600,
//!     isolated_home: None,
//! };
//! let handle = adapter.start_session(spec).await.unwrap();
//! let mut events = handle.events();
//! while let Ok(event) = events.recv().await {
//!     if event.is_terminal() { break; }
//! }
//! # }
//! ```

#![forbid(unsafe_code)]

pub mod adapter;
pub mod agy;
pub mod claude;
pub mod codex;
pub mod decision;
pub mod error;
pub mod events;
pub mod mock;
pub mod types;

pub use adapter::{RuntimeAdapter, SessionBackend, SessionHandle};
pub use error::{AdapterError, AdapterFailure, Classifier};
pub use events::AdapterEvent;
pub use mock::{MockAdapter, MockBehavior};
pub use types::{
    AuthStatus, Capabilities, HealthReport, ModelUsageRow, RuntimeInfo, SpawnSpec, UsageSnapshot,
};
