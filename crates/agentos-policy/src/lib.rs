//! # agentos-policy — F-10: the governance authority
//!
//! Permissions, approval gates, secrets interface, and audit trail (PRD
//! §14 SEC-01..SEC-05; PRD §3 "least privilege" and "no hidden mutation").
//! The workflow engine and policy engine stay **authoritative** (PRD §1
//! key architectural rule): the master model can propose and route, but it
//! cannot silently bypass these deterministic constraints.
//!
//! Enforcement is process/contract-level, never prompt text: a
//! [`PermissionSet`] is *compiled* ([`compile_to_spawn_spec`]) into
//! [`SpawnConstraints`] — the four policy-bearing fields of
//! `agentos_adapters::SpawnSpec` — which adapters translate into CLI
//! flags/sandbox boundaries. Sensitive transitions go through code gates
//! ([`git_gate_check`], [`ApprovalStore`]) whose outcomes land in an
//! append-only [`AuditStore`].
//!
//! Module map:
//!
//! | Module | Responsibility | PRD |
//! |---|---|---|
//! | [`permission`] | [`PermissionSet`], shell/network/git/tool/secret-scope modes, glob matching, role presets, write-scope overlap. | SEC-01/02 |
//! | [`compile`] | [`compile_to_spawn_spec`] (the adapter-seam bridge), [`git_gate_check`] + [`PolicyDenial`]. | SEC-01, §23.3 |
//! | [`approval`] | [`Gate`]-based approval requests bound to operation fingerprints with expiry, SQLite-backed. | SEC-04 |
//! | [`secrets`] | [`SecretsBroker`] trait (keychain seam), [`SecretString`] (redacting, zero-on-drop), [`EphemeralBroker`]. | SEC-03 |
//! | [`audit`] | Append-only [`AuditStore`] with tamper triggers, `record_*` helpers, [`AuditStore::export_bundle`]. | SEC-05 |
//! | `error`/`store` (plumbing) | [`PolicyError`] with retryable `CoreError::SqliteBusy`; F-01 SQLite open canon. | §14, F-01 |
//!
//! SQLite usage follows the F-01 canon (`docs/HANDOFF-BUILD.md` §4):
//! `busy_timeout` on every connection, `journal_mode` read before any
//! switch to WAL, `SQLITE_BUSY` surfaced as retryable
//! [`agentos_core::CoreError::SqliteBusy`].

#![forbid(unsafe_code)]

pub mod approval;
pub mod audit;
pub mod compile;
pub mod error;
pub mod permission;
pub mod secrets;

mod store;

pub use approval::{
    canonical_json, operation_fingerprint, ApprovalDecision, ApprovalRequest, ApprovalStatus,
    ApprovalStore, Gate,
};
pub use audit::{AuditEntry, AuditStore};
pub use compile::{
    compile_to_spawn_spec, git_gate_check, PolicyDenial, SpawnConstraints, TaskScope,
};
pub use error::PolicyError;
pub use permission::{
    glob_matches, glob_overlap, overlapping_write_conflict, ApprovalRule, GitAction, NetworkPolicy,
    PermissionSet, ShellMode,
};
pub use secrets::{EphemeralBroker, SecretLease, SecretString, SecretsBroker};
