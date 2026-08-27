//! # agentos-orchestrator
//!
//! F-12: the master orchestrator (PRD §9 **OR-01**). A larger reasoning
//! model — `claude-opus-5` through the F-03 Claude Code adapter — decomposes
//! a goal into a task graph by emitting **structured plan operations** that
//! the deterministic workflow engine validates:
//!
//! ```text
//! PlanOperation = create_task | add_dependency | assign_pool
//!               | request_review | escalate | close_goal
//! ```
//!
//! ## The two invariants this crate exists to hold
//!
//! **1. The engine is authoritative.** OR-01's implementation rule is
//! "all state mutations are commands validated by deterministic engine,
//! never arbitrary direct DB writes"; Appendix F lists "orchestrator
//! authority: structured commands validated by engine" as a safety
//! invariant with "no reason to relax". So:
//!
//! - DAG legality (acyclicity, dangling deps, duplicate ids, bounded loops)
//!   is decided by `agentos_workflow::validate`, never re-implemented here.
//!   Its [`ValidationError`](agentos_workflow::ValidationError) travels back
//!   to the model inside [`RejectionReason::SpecInvalid`].
//! - Materialization goes through
//!   [`WorkflowEngine::start_run`](agentos_workflow::WorkflowEngine::start_run)
//!   (or the store's `create_run`), which validates again before creating
//!   any durable state.
//! - Once a plan is committed the materialized node specs are frozen:
//!   `add_dependency` and `assign_pool` are refused with
//!   `run_already_started`. Appends (`create_task`, `request_review`) stay
//!   legal and go through the engine's own `add_task`, which validates the
//!   tentative DAG on insert; a refused append is dropped from the draft
//!   and reported as `engine_rejected`.
//! - Every refusal is a machine-readable [`RejectionReason`] carrying the
//!   fields — and a [`hint`](RejectionReason::hint) — the model needs to
//!   self-correct on the next cycle.
//!
//! **2. The orchestrator is a proposer, never a liveness dependency.**
//! OR-01's second acceptance criterion is that a run continues
//! deterministically if the orchestrator is unavailable. Nothing here holds
//! a lease, executes a task or gates a transition: after
//! [`Orchestrator::commit`] the engine's scheduler owns the run. A model
//! outage produces a [`PlanCycleReport`] with `model_error` set — never an
//! `Err` that could propagate into engine control flow.
//!
//! ## Shape (mastermind three tiers, HANDOFF-BUILD-2 §2)
//!
//! opus-5 **commands** and never writes code → cheap worker pools code →
//! sonnet-class pools review. The user gates the phase transition: planning
//! cycles propose, and [`Orchestrator::commit`] is an explicit call.
//!
//! ## Module map
//!
//! | Module | Role |
//! |---|---|
//! | [`operation`] | the six [`PlanOperation`]s as serde data (camelCase, `deny_unknown_fields`) |
//! | [`parse`] | **total** model-output parsing — unknown or garbled input becomes a typed rejection, never a panic and never a silent drop |
//! | [`plan`] | the draft plan and the application/rejection path; DAG checks delegate to `agentos-workflow` |
//! | [`sink`] | the only door to durable state: validate + materialize, read back, raise priority |
//! | [`snapshot`] | the OR-01 compact state snapshot and the prompt rendered from it |
//! | [`model`] | the [`PlanningModel`] boundary, the opus-5 Claude implementation, and a scripted test double |
//! | [`orchestrator`] | the bounded cycle that ties them together |
//! | [`error`] | [`RejectionReason`] (data) vs [`OrchestratorError`] (machinery) |
//!
//! ## Trust boundary
//!
//! Model output is untrusted input, not instructions to the harness. It is
//! parsed into a closed vocabulary of six operations, every field is
//! validated, unknown keys are refused rather than ignored, and echoed
//! fragments are bounded ([`error::EXCERPT_MAX_CHARS`]). Text inside an
//! operation — an escalation reason, an objective — is data that is stored
//! and displayed, never acted on.
//!
//! ## Billing
//!
//! No test in this crate makes a billable call: the planning model is a
//! scripted stub or the credential-free F-02 `MockAdapter`. The single live
//! probe is `#[ignore]`d and gated behind `AGENTOS_ORCHESTRATOR_E2E=1`, and
//! it only runs the free discovery surface (F-00 §5: health checks are never
//! billable).
//!
//! ```no_run
//! use std::sync::Arc;
//! use agentos_orchestrator::{Orchestrator, PlanPolicy, ScriptedPlanningModel, WorkflowSink};
//! use agentos_workflow::TaskStore;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(TaskStore::open_in_memory()?);
//! let sink = Arc::new(WorkflowSink::new(store));
//! let model = Arc::new(ScriptedPlanningModel::new([
//!     r#"[{"op":"create_task","nodeId":"spec","pool":"backend"}]"#,
//! ]));
//!
//! let mut orchestrator = Orchestrator::new("ship the API", PlanPolicy::default(), model, sink);
//! let reports = orchestrator.run_planning().await;      // propose
//! assert!(reports[0].rejected.is_empty());
//! let run_id = orchestrator.commit()?;                  // user gate -> engine owns it
//! # let _ = run_id;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

pub mod desk;
pub mod error;
pub mod model;
pub mod operation;
pub mod orchestrator;
pub mod parse;
pub mod plan;
pub mod sink;
pub mod snapshot;

pub use desk::{ApprovalDesk, EscalationDesk};
pub use error::{OrchestratorError, Rejection, RejectionReason};
pub use model::{
    verify_denylist_tokens, ClaudePlanningModel, ModelResponse, PlanningModel,
    ScriptedPlanningModel, ORCHESTRATOR_E2E_ENV, ORCHESTRATOR_MODEL, ORCHESTRATOR_TOOL_DENYLIST,
};
pub use operation::{
    AddDependency, AssignPool, CloseGoal, CreateTask, Escalate, Escalation, EscalationTarget,
    PlanOperation, RequestReview, KNOWN_OPERATIONS,
};
pub use orchestrator::{Orchestrator, OrchestratorCheckpoint, PlanCycleReport};
pub use parse::{operation_from_value, parse_operations, ParsedOperations};
pub use plan::{Plan, PlanPolicy, PlannedNode, DEFAULT_POOLS, DEFAULT_REVIEWER_POOL};
pub use sink::{PlanSink, RunView, TaskView, WorkflowSink};
pub use snapshot::{Phase, PlanSnapshot, SnapshotBudgets, SnapshotPolicies};
