//! # agentos-runtime
//!
//! F-07: supervisor/runtime composition. This crate is the composition root
//! that makes MockAdapter → workflow engine → git mutation queue → agent
//! ledger → event journal run as one loop:
//!
//! - [`contract`]: the full `TaskContract` (PRD §9 OR-04 / §25 Appendix B)
//!   with immutable-after-lease semantics (builder + versioning `amend`),
//!   replacing the F-06 subset the workflow store persists.
//! - [`handoff`]: the typed `HandoffPacket` (PRD §11 HO-01 / §25 Appendix C)
//!   with JSON-schema-style validation before queueing, immutable artifact
//!   references, and transcript access on demand only (a handle, never the
//!   transcript).
//! - [`usage_ledger`]: per-run budget/usage ledger consuming
//!   `AdapterEvent::UsageUpdate`s, accumulating per-model cost rows and the
//!   fixed per-session preamble overhead, enforcing
//!   `Budgets { max_cost_usd, max_elapsed_secs }` as Ok/Warn/Exceeded.
//! - [`supervisor`]: the [`Supervisor`] — one [`agentos_workflow`]
//!   `WorkflowEngine` driven over a registry of
//!   [`agentos_adapters::RuntimeAdapter`]s, with exclusive path ownership
//!   (GIT-05), per-task worktree isolation (GIT-04), the serialized git
//!   mutation queue (GIT-02), commit attribution in the agent ledger
//!   (GIT-03), and every step appended to the daemon's append-only event
//!   journal (F-00 §3 event rules: append-only, run_id + trace_id on every
//!   event, large payloads offloaded to content-addressed artifacts that the
//!   event references by `payload_ref` + `payload_hash`).
//! - [`policy`]: the F-10 wiring — fingerprint-bound approval gates
//!   (`GitGate` and `HumanApproval` nodes), permission compilation into
//!   adapter `SpawnConstraints`, and the policy audit trail. Every path
//!   fails closed.
//! - [`digest`]: a dependency-free SHA-256 used for the content addressing
//!   of handoff artifacts.
//!
//! Seams deliberately left open (see `docs/F-07-runtime-supervisor.md`):
//! the deterministic stub reviewer stands in for the real reviewer pool, and
//! cross-stage ownership holds (until commit rather than per attempt) land
//! with the orchestrator PR.

#![forbid(unsafe_code)]

pub mod contract;
pub mod digest;
pub mod error;
pub mod git_advisor;
pub mod graphify;
pub mod handoff;
pub mod policy;
pub mod supervisor;
pub mod usage_ledger;

pub use agentos_workflow::ReopenReport;
pub use contract::{ContractBudgets, ContractRule, GitPolicy, TaskContract, TaskContractBuilder};
pub use digest::sha256_hex;
pub use error::RuntimeError;
pub use git_advisor::{CommitFacts, GitAdvisor, RebaseAdvice, StaleBaseFacts, GIT_ADVISOR_ROLE};
pub use graphify::{
    ChangeContext, GraphRefresh, Graphifier, GraphifyOutcome, GraphifySkip, GRAPHIFY_BIN_ENV,
    GRAPHIFY_OUT_DIR,
};
pub use handoff::{
    ArtifactRef, HandoffPacket, HandoffRule, HandoffStatus, RequestedAction, TestReport, TestStatus,
};
pub use policy::{
    compile_constraints, derive_permissions, GateVerdict, PolicyGate, OP_KIND_GIT, OP_KIND_HUMAN,
};
pub use supervisor::{DriveSummary, Supervisor, SupervisorConfig};

// The F-10 vocabulary the supervisor's gate API speaks, re-exported so
// embedders (and tests) resolve one gate/decision type, not two.
pub use agentos_policy::{
    ApprovalDecision, ApprovalRequest, ApprovalStatus, ApprovalStore, AuditStore, Gate, GitAction,
    PermissionSet, PolicyDenial, SpawnConstraints,
};
pub use usage_ledger::{BudgetStatus, ModelUsage, TaskUsage, UsageLedger};
