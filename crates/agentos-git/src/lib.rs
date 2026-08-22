//! # agentos-git — F-09 Git manager
//!
//! Worktree isolation, serialized mutation queue, and agent ledger for the
//! Agent Engineering OS (PRD §12). The dedicated git manager is the only
//! component with commit/merge/rebase/push capability; workers submit
//! requests and operate in isolated worktrees (GIT-01/GIT-04).
//!
//! Modules:
//!
//! - [`cli`]: thin `git` CLI wrapper — argv is `Vec<OsString>`, never a
//!   shell string (Windows is the reference platform);
//! - [`worktree`]: one worktree per leased write task, branch names derived
//!   exclusively from run/task UUIDs (GIT-04);
//! - [`queue`]: per-repo ordered mutation queue with strict single-consumer
//!   leases, base-commit staleness rejection, and approval-gated push
//!   (GIT-01/GIT-02);
//! - [`ledger`]: commit → task/agent/orchestrator/reviewers/context
//!   versions attribution, stored as harness-DB metadata — never inside
//!   commit messages (GIT-03);
//! - [`ownership`]: exclusive/advisory holds over path globs (GIT-05).

#![forbid(unsafe_code)]

pub mod cli;
pub mod error;
pub mod ledger;
pub mod ownership;
pub mod queue;
pub mod worktree;

mod store;

pub use cli::{GitCliError, WorktreeEntry};
pub use error::GitError;
