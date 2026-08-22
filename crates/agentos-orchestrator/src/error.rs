//! Error taxonomy for the orchestrator (PRD §9 OR-01 acceptance criterion:
//! "invalid orchestrator commands are rejected with machine-readable
//! reason").
//!
//! Two layers:
//!
//! - [`RejectionReason`] — why one *proposed* operation was refused. It is
//!   serde-tagged data (`{"code": "unknown_node", "node": "…", …}`), never a
//!   string to parse, and every variant carries the fields the model needs
//!   to self-correct on the next cycle (see [`RejectionReason::hint`]).
//!   A rejection is a normal outcome, not a program error.
//! - [`OrchestratorError`] — the orchestrator's own machinery failing
//!   (storage, engine validation at commit time, the model process). These
//!   never reach the workflow engine: a down orchestrator must not stop a
//!   run (OR-01 acceptance criterion 2).

use agentos_core::CoreError;
use agentos_workflow::{ValidationError, WorkflowError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Longest raw model fragment echoed back into a rejection. Model output is
/// untrusted input; excerpts are bounded so a hostile or garbled response
/// cannot inflate the decision ledger or the next prompt (F-00 §3: large
/// payloads never travel inline).
pub const EXCERPT_MAX_CHARS: usize = 240;

/// Truncate untrusted text to [`EXCERPT_MAX_CHARS`] characters for echoing
/// back to the model or into a log line.
pub fn excerpt(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= EXCERPT_MAX_CHARS {
        trimmed.to_owned()
    } else {
        let head: String = trimmed.chars().take(EXCERPT_MAX_CHARS).collect();
        format!("{head}…")
    }
}

/// Why one proposed [`crate::PlanOperation`] was refused.
///
/// Serializes as an internally-tagged object so a consumer (the UI, the
/// journal, or the orchestrator model itself on the next cycle) branches on
/// `code` without parsing prose — the same shape agentos-workflow uses for
/// [`ValidationError`].
///
/// The variants fall into four groups:
///
/// 1. *Shape* — the output was not a parseable operation at all
///    ([`RejectionReason::UnparseableOutput`] … [`RejectionReason::InvalidField`]).
/// 2. *Plan semantics* — the operation contradicts the draft plan
///    ([`RejectionReason::DuplicateNode`] … [`RejectionReason::SelfDependency`]).
/// 3. *Bounds* — the operation exceeds a declared ceiling
///    ([`RejectionReason::PlanTooLarge`], [`RejectionReason::OperationBudgetExceeded`]).
/// 4. *Engine authority* — the deterministic engine, not the model, owns
///    this state ([`RejectionReason::RunAlreadyStarted`],
///    [`RejectionReason::RunNotTerminal`], [`RejectionReason::SpecInvalid`],
///    [`RejectionReason::EngineRejected`]).
#[derive(Debug, Clone, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum RejectionReason {
    /// No JSON operation could be extracted from the model's output.
    #[error("model output contained no parseable operation: {detail}")]
    #[serde(rename_all = "camelCase")]
    UnparseableOutput {
        /// Parser detail (serde message or "no JSON found").
        detail: String,
        /// Bounded excerpt of the offending output.
        excerpt: String,
    },
    /// A parsed JSON value was not an object (operations are objects).
    #[error("operation must be a JSON object, got: {excerpt}")]
    NotAnObject {
        /// Bounded excerpt of the offending value.
        excerpt: String,
    },
    /// The object carried no `op` discriminator.
    #[error("operation object has no string `op` field: {excerpt}")]
    MissingOp {
        /// Bounded excerpt of the offending object.
        excerpt: String,
    },
    /// The `op` value is not one of the six PRD OR-01 operations.
    #[error("unknown operation `{op}`; known: {known:?}")]
    UnknownOperation {
        /// The unrecognized discriminator, verbatim (bounded).
        op: String,
        /// The operations this build accepts.
        known: Vec<String>,
    },
    /// The operation is known but its payload did not deserialize (missing
    /// required field, wrong type, or an unknown extra field — the payload
    /// structs are `deny_unknown_fields` so a misspelled key is reported
    /// rather than silently dropped).
    #[error("malformed `{op}` payload: {detail}")]
    MalformedOperation {
        /// The operation name.
        op: String,
        /// Serde's message, verbatim.
        detail: String,
    },
    /// A field deserialized but is semantically invalid (empty id, illegal
    /// characters, empty escalation reason).
    #[error("`{op}` field `{field}` is invalid: {detail}")]
    InvalidField {
        /// The operation name.
        op: String,
        /// The offending field, in its camelCase wire spelling.
        field: String,
        /// What is wrong with it.
        detail: String,
    },
    /// A node with this id already exists in the draft plan.
    #[error("node `{node}` already exists in the plan")]
    DuplicateNode {
        /// The colliding node id.
        node: String,
    },
    /// The operation references a node the plan does not contain.
    #[error("unknown node `{node}`; plan has {known:?}")]
    UnknownNode {
        /// The dangling reference.
        node: String,
        /// Node ids currently in the plan.
        known: Vec<String>,
    },
    /// The operation routes work to a pool outside the configured roster
    /// (PRD Appendix D team config: pools are declared, not invented).
    #[error("unknown pool `{pool}`; configured pools: {known:?}")]
    UnknownPool {
        /// The requested pool.
        pool: String,
        /// The configured roster.
        known: Vec<String>,
    },
    /// The dependency edge already exists.
    #[error("node `{node}` already depends on `{dependency}`")]
    DuplicateDependency {
        /// The dependent node.
        node: String,
        /// The dependency.
        dependency: String,
    },
    /// A node was made to depend on itself.
    #[error("node `{node}` cannot depend on itself")]
    SelfDependency {
        /// The offending node.
        node: String,
    },
    /// The plan already holds its maximum node count.
    #[error("plan already holds the maximum of {limit} nodes")]
    PlanTooLarge {
        /// The configured ceiling.
        limit: usize,
    },
    /// The model emitted more operations in one cycle than the policy
    /// allows. Surplus operations are *rejected*, never silently dropped.
    #[error("operation {index} exceeds the per-cycle ceiling of {limit}")]
    OperationBudgetExceeded {
        /// The configured ceiling.
        limit: usize,
        /// Zero-based index of the surplus operation.
        index: usize,
    },
    /// A structural operation arrived after the plan was materialized into
    /// durable engine state. The engine owns the run from that point: the
    /// orchestrator re-plans by proposing a *new* run, never by mutating a
    /// live task graph behind the engine's back.
    #[error("`{op}` rejected: run {run_id} already started; re-plan as a new run")]
    #[serde(rename_all = "camelCase")]
    RunAlreadyStarted {
        /// The rejected operation name.
        op: String,
        /// The live run.
        run_id: String,
    },
    /// The goal was already closed; the plan is sealed.
    #[error("`{op}` rejected: the goal is already closed")]
    GoalClosed {
        /// The rejected operation name.
        op: String,
    },
    /// `close_goal` with no committed run to close.
    #[error("close_goal rejected: no run has been committed")]
    NothingToClose,
    /// `close_goal` while the engine still reports the run as running. The
    /// model does not get to declare victory over the engine's own state.
    #[error("close_goal rejected: engine reports run status `{status}`")]
    RunNotTerminal {
        /// The engine's run status wire string.
        status: String,
    },
    /// The plan has no nodes.
    #[error("plan is empty")]
    EmptyPlan,
    /// The resulting DAG was refused by the workflow engine's own
    /// validator (cycle, dangling dependency, duplicate id, unbounded
    /// loop). The engine's machine-readable [`ValidationError`] is carried
    /// through verbatim — the orchestrator does not re-implement DAG rules.
    #[error("workflow engine rejected the resulting spec: {validation}")]
    SpecInvalid {
        /// The engine's own rejection.
        validation: ValidationError,
    },
    /// The durable store refused the operation (illegal transition, missing
    /// row, busy database).
    #[error("engine rejected the operation: {detail}")]
    EngineRejected {
        /// Storage-layer detail.
        detail: String,
    },
}

impl RejectionReason {
    /// Stable machine-readable code, identical to the serialized `code`
    /// tag. Consumers switch on this; the `Display` text is for humans.
    pub fn code(&self) -> &'static str {
        match self {
            RejectionReason::UnparseableOutput { .. } => "unparseable_output",
            RejectionReason::NotAnObject { .. } => "not_an_object",
            RejectionReason::MissingOp { .. } => "missing_op",
            RejectionReason::UnknownOperation { .. } => "unknown_operation",
            RejectionReason::MalformedOperation { .. } => "malformed_operation",
            RejectionReason::InvalidField { .. } => "invalid_field",
            RejectionReason::DuplicateNode { .. } => "duplicate_node",
            RejectionReason::UnknownNode { .. } => "unknown_node",
            RejectionReason::UnknownPool { .. } => "unknown_pool",
            RejectionReason::DuplicateDependency { .. } => "duplicate_dependency",
            RejectionReason::SelfDependency { .. } => "self_dependency",
            RejectionReason::PlanTooLarge { .. } => "plan_too_large",
            RejectionReason::OperationBudgetExceeded { .. } => "operation_budget_exceeded",
            RejectionReason::RunAlreadyStarted { .. } => "run_already_started",
            RejectionReason::GoalClosed { .. } => "goal_closed",
            RejectionReason::NothingToClose => "nothing_to_close",
            RejectionReason::RunNotTerminal { .. } => "run_not_terminal",
            RejectionReason::EmptyPlan => "empty_plan",
            RejectionReason::SpecInvalid { .. } => "spec_invalid",
            RejectionReason::EngineRejected { .. } => "engine_rejected",
        }
    }

    /// A corrective instruction for the proposing model.
    ///
    /// The typed fields already say *what* is wrong; the hint says what a
    /// correct next proposal looks like, so a cycle's rejections are a
    /// self-correction signal rather than a dead end.
    pub fn hint(&self) -> String {
        match self {
            RejectionReason::UnparseableOutput { .. } => {
                "Reply with a JSON array of operation objects and nothing else.".to_owned()
            }
            RejectionReason::NotAnObject { .. } => {
                "Every element of the array must be an object with an `op` field.".to_owned()
            }
            RejectionReason::MissingOp { .. } => {
                "Add an `op` field naming one of the six plan operations.".to_owned()
            }
            RejectionReason::UnknownOperation { known, .. } => {
                format!("Use one of: {}.", known.join(", "))
            }
            RejectionReason::MalformedOperation { op, .. } => {
                format!("Re-emit `{op}` with exactly the documented fields (camelCase, no extras).")
            }
            RejectionReason::InvalidField { field, .. } => {
                format!("Supply a valid `{field}` value.")
            }
            RejectionReason::DuplicateNode { node } => format!(
                "Pick a different node id, or use add_dependency/assign_pool to amend `{node}`."
            ),
            RejectionReason::UnknownNode { known, .. } => {
                format!(
                    "Create the node first; existing nodes: {}.",
                    known.join(", ")
                )
            }
            RejectionReason::UnknownPool { known, .. } => {
                format!(
                    "Route to one of the configured pools: {}.",
                    known.join(", ")
                )
            }
            RejectionReason::DuplicateDependency { .. } => {
                "The edge is already in the plan; no operation is needed.".to_owned()
            }
            RejectionReason::SelfDependency { .. } => "Depend on a different node.".to_owned(),
            RejectionReason::PlanTooLarge { .. } => {
                "Close or commit the current plan before adding more nodes.".to_owned()
            }
            RejectionReason::OperationBudgetExceeded { .. } => {
                "Emit fewer operations per cycle; the remainder can follow next cycle.".to_owned()
            }
            RejectionReason::RunAlreadyStarted { .. } => {
                "The run is live and owned by the engine. Only escalate and close_goal apply; \
                 further structural changes belong to a new run."
                    .to_owned()
            }
            RejectionReason::GoalClosed { .. } => {
                "The goal is closed; start a new goal to plan further work.".to_owned()
            }
            RejectionReason::NothingToClose => {
                "Commit the plan into a run before closing the goal.".to_owned()
            }
            RejectionReason::RunNotTerminal { .. } => {
                "Wait for the engine to finish the run; it decides when work is done.".to_owned()
            }
            RejectionReason::EmptyPlan => {
                "Propose at least one create_task before committing.".to_owned()
            }
            RejectionReason::SpecInvalid { validation } => match validation {
                ValidationError::CycleDetected { cycle } => {
                    format!("The dependency edge closes the cycle {cycle:?}; drop or reverse it.")
                }
                other => format!("Fix the workflow shape: {other}"),
            },
            RejectionReason::EngineRejected { .. } => {
                "The durable engine refused the change; re-read the snapshot before retrying."
                    .to_owned()
            }
        }
    }
}

/// One refused proposal: the offending payload, where it appeared, and why.
///
/// `raw` is the model's own value (bounded when echoed) so the rejection is
/// auditable — a run's decision ledger records what was proposed *and*
/// refused, not just what was accepted (PRD §17 observability: orchestration
/// bugs are impossible to fix from final output alone).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rejection {
    /// Zero-based position of the operation within the cycle's output.
    pub index: usize,
    /// The proposed payload verbatim (`Value::String` for fragments that
    /// never reached JSON).
    pub raw: Value,
    /// The machine-readable reason.
    pub reason: RejectionReason,
}

impl Rejection {
    /// Build a rejection for the operation at `index`.
    pub fn new(index: usize, raw: Value, reason: RejectionReason) -> Self {
        Self { index, raw, reason }
    }

    /// Build a rejection whose payload never parsed as JSON.
    pub fn from_text(index: usize, raw: &str, reason: RejectionReason) -> Self {
        Self {
            index,
            raw: Value::String(excerpt(raw)),
            reason,
        }
    }
}

/// Failures of the orchestrator's own machinery.
///
/// Deliberately *not* used for refused proposals — those are
/// [`RejectionReason`]s, which are data. And deliberately never surfaced to
/// the workflow engine: [`OrchestratorError::Model`] means the proposer is
/// unavailable, which the engine must survive (PRD §9 OR-01: "a run can
/// continue deterministically if orchestrator is temporarily unavailable").
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum OrchestratorError {
    /// The workflow engine refused the commit (spec validation, storage).
    #[error("workflow engine: {0}")]
    Workflow(#[from] WorkflowError),
    /// A durable-store failure outside a commit.
    #[error("storage: {0}")]
    Storage(#[from] CoreError),
    /// The planning model could not be reached or produced no output. The
    /// orchestrator degrades to "no new proposals"; the engine is untouched.
    #[error("planning model unavailable: {detail}")]
    Model {
        /// Provider/adapter detail, never credential-bearing.
        detail: String,
    },
    /// An operation was attempted that needs a committed run.
    #[error("no run has been committed for this plan")]
    NoRun,
    /// The plan was refused before any durable state was created.
    #[error("plan rejected: {0}")]
    Rejected(#[from] RejectionReason),
    /// The escalation desk (F-10 approval store) could not be reached. The
    /// escalation is still recorded in the plan ledger and reported; only
    /// the human-facing hand-off failed.
    #[error("escalation desk: {detail}")]
    Desk {
        /// Store detail, never credential-bearing.
        detail: String,
    },
}

/// `PolicyError` is neither `Clone` nor `PartialEq`, so it is flattened to
/// its message rather than carried — this error type stays comparable for
/// the report types that embed it.
impl From<agentos_policy::PolicyError> for OrchestratorError {
    fn from(error: agentos_policy::PolicyError) -> Self {
        OrchestratorError::Desk {
            detail: error.to_string(),
        }
    }
}

impl OrchestratorError {
    /// Whether a retry with backoff could succeed. Storage busy is
    /// retryable (SQLite canon); a model outage is retryable (rate limits,
    /// transient CLI failures); deterministic rejections are not.
    pub fn is_retryable(&self) -> bool {
        match self {
            OrchestratorError::Workflow(inner) => inner.is_retryable(),
            OrchestratorError::Storage(inner) => inner.is_retryable(),
            OrchestratorError::Model { .. } => true,
            // The approval store is SQLite: contention is transient, so
            // the next cycle can re-raise the escalation.
            OrchestratorError::Desk { .. } => true,
            OrchestratorError::NoRun | OrchestratorError::Rejected(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejection_reason_serializes_as_a_tagged_machine_readable_code() {
        let reason = RejectionReason::UnknownNode {
            node: "ghost".to_owned(),
            known: vec!["spec".to_owned()],
        };
        let wire = serde_json::to_value(&reason).unwrap();
        assert_eq!(wire["code"], json!("unknown_node"));
        assert_eq!(wire["node"], json!("ghost"));
        assert_eq!(wire["known"], json!(["spec"]));
        let parsed: RejectionReason = serde_json::from_value(wire).unwrap();
        assert_eq!(parsed, reason);
        assert_eq!(parsed.code(), "unknown_node");
        assert!(!parsed.hint().is_empty());
    }

    #[test]
    fn spec_invalid_carries_the_engines_own_validation_error() {
        let reason = RejectionReason::SpecInvalid {
            validation: ValidationError::CycleDetected {
                cycle: vec!["a".to_owned(), "b".to_owned(), "a".to_owned()],
            },
        };
        let wire = serde_json::to_value(&reason).unwrap();
        assert_eq!(wire["code"], json!("spec_invalid"));
        // The engine's own tagged error travels through unmodified.
        assert_eq!(wire["validation"]["code"], json!("cycle_detected"));
        assert!(reason.hint().contains("cycle"));
    }

    #[test]
    fn excerpts_are_bounded_and_trimmed() {
        let long = "x".repeat(EXCERPT_MAX_CHARS * 2);
        let cut = excerpt(&long);
        assert_eq!(cut.chars().count(), EXCERPT_MAX_CHARS + 1); // + the ellipsis
        assert!(cut.ends_with('…'));
        assert_eq!(excerpt("  hi  "), "hi");
    }

    #[test]
    fn model_outage_is_retryable_and_rejections_are_not() {
        assert!(OrchestratorError::Model {
            detail: "spawn failed".to_owned()
        }
        .is_retryable());
        assert!(OrchestratorError::Storage(CoreError::SqliteBusy).is_retryable());
        assert!(!OrchestratorError::NoRun.is_retryable());
        assert!(!OrchestratorError::Rejected(RejectionReason::EmptyPlan).is_retryable());
    }

    #[test]
    fn every_reason_has_a_distinct_code_and_a_hint() {
        let reasons = vec![
            RejectionReason::UnparseableOutput {
                detail: "d".to_owned(),
                excerpt: "e".to_owned(),
            },
            RejectionReason::NotAnObject {
                excerpt: "e".to_owned(),
            },
            RejectionReason::MissingOp {
                excerpt: "e".to_owned(),
            },
            RejectionReason::UnknownOperation {
                op: "o".to_owned(),
                known: vec!["create_task".to_owned()],
            },
            RejectionReason::MalformedOperation {
                op: "o".to_owned(),
                detail: "d".to_owned(),
            },
            RejectionReason::InvalidField {
                op: "o".to_owned(),
                field: "f".to_owned(),
                detail: "d".to_owned(),
            },
            RejectionReason::DuplicateNode {
                node: "n".to_owned(),
            },
            RejectionReason::UnknownNode {
                node: "n".to_owned(),
                known: vec![],
            },
            RejectionReason::UnknownPool {
                pool: "p".to_owned(),
                known: vec![],
            },
            RejectionReason::DuplicateDependency {
                node: "n".to_owned(),
                dependency: "d".to_owned(),
            },
            RejectionReason::SelfDependency {
                node: "n".to_owned(),
            },
            RejectionReason::PlanTooLarge { limit: 1 },
            RejectionReason::OperationBudgetExceeded { limit: 1, index: 2 },
            RejectionReason::RunAlreadyStarted {
                op: "o".to_owned(),
                run_id: "r".to_owned(),
            },
            RejectionReason::GoalClosed { op: "o".to_owned() },
            RejectionReason::NothingToClose,
            RejectionReason::RunNotTerminal {
                status: "running".to_owned(),
            },
            RejectionReason::EmptyPlan,
            RejectionReason::SpecInvalid {
                validation: ValidationError::EmptyWorkflow,
            },
            RejectionReason::EngineRejected {
                detail: "d".to_owned(),
            },
        ];
        let mut codes: Vec<&str> = reasons.iter().map(RejectionReason::code).collect();
        let total = codes.len();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), total, "codes must be distinct");
        for reason in &reasons {
            assert!(!reason.hint().is_empty(), "{reason:?} has no hint");
            // The serialized tag must equal `code()` for every variant.
            let wire = serde_json::to_value(reason).unwrap();
            assert_eq!(wire["code"], json!(reason.code()));
        }
    }
}
