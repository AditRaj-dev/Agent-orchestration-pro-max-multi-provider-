//! # agentos-workflow
//!
//! F-06: the deterministic workflow engine, scheduler, lease manager and
//! budget/retry controller (PRD §9 OR-03/OR-04/OR-05/OR-08).
//!
//! Composition:
//!
//! - [`spec`] — the versioned DAG definition (`WorkflowSpec`, `NodeSpec`,
//!   `NodeType`, `Budgets`, `RetryPolicy`) and the OR-04 [`TaskContract`]
//!   subset.
//! - [`validate`] — everything checkable BEFORE execution: non-empty,
//!   unique ids, resolvable dependencies, acyclicity (with the cycle
//!   reported), bounded loops.
//! - [`store`] — the durable SQLite task/run store: private copy of the
//!   F-01 SQLite canon, CAS state transitions delegating legality to
//!   agentos-core's `TaskState::can_transition`.
//! - [`scheduler`] — ready-queue (priority then age), leases + heartbeats +
//!   expiry reclaim, and the OR-08 budget gate with a cost-ledger hook.
//! - [`executor`] — the provider-free [`TaskExecutor`] boundary and the
//!   [`WorkflowEngine`] (`start_run`, `tick`, `run_until_idle`).
//!
//! The engine is deterministic composable primitives plus a bounded driver
//! method — it is NOT a daemon loop; embedders (the F-01 daemon, tests,
//! future CLI) call `tick` on their own cadence.

#![forbid(unsafe_code)]

pub mod error;
pub mod executor;
pub mod scheduler;
pub mod spec;
pub mod store;
pub mod validate;

pub use error::{ValidationError, WorkflowError};
pub use executor::{
    DriverReport, FailureKind, Outcome, TaskExecutor, TaskFailure, TaskSuccess, TickReport,
    WorkflowEngine,
};
pub use scheduler::{CostLedger, NoCostLedger, Scheduler, DEFAULT_LEASE_TTL};
pub use spec::{Budgets, NodeSpec, NodeType, RetryPolicy, TaskContract, WorkflowSpec};
pub use store::{ReopenReport, RunRecord, RunStatus, TaskRecord, TaskStore};
pub use validate::{topological_order, validate};
