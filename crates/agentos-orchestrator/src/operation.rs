//! The orchestrator's command vocabulary (PRD §9 OR-01 key state):
//!
//! ```text
//! PlanOperation = create_task | add_dependency | assign_pool
//!               | request_review | escalate | close_goal
//! ```
//!
//! These six are the *only* way the master orchestrator changes plan state.
//! OR-01's implementation rule is explicit: "all state mutations are
//! commands validated by deterministic engine, never arbitrary direct DB
//! writes" — so this module defines data, never behavior. Application and
//! rejection live in [`crate::plan`]; DAG legality is delegated to
//! `agentos-workflow`.
//!
//! Wire form follows the F-00 §3 event canon: camelCase JSON keys, an
//! internally-tagged `op` discriminator in snake_case:
//!
//! ```json
//! {"op": "create_task", "nodeId": "impl-api", "nodeType": "run",
//!  "dependsOn": ["spec"], "pool": "backend", "priority": "p1"}
//! ```
//!
//! Every payload struct is `deny_unknown_fields`. Model output is an
//! untrusted trust boundary: a misspelled key must surface as a correctable
//! rejection, never be silently ignored.

use agentos_core::Priority;
use agentos_workflow::{Budgets, NodeType, RetryPolicy};
use serde::{Deserialize, Serialize};

/// The six operation names this build accepts, in PRD order.
pub const KNOWN_OPERATIONS: [&str; 6] = [
    "create_task",
    "add_dependency",
    "assign_pool",
    "request_review",
    "escalate",
    "close_goal",
];

/// Longest accepted node id. Ids are plan-local labels, not free text.
pub const NODE_ID_MAX_CHARS: usize = 64;

/// Longest accepted escalation reason / goal summary.
pub const REASON_MAX_CHARS: usize = 512;

/// One structured plan command proposed by the orchestrator model.
///
/// `PlanOperation` deserializes through [`crate::parse`] rather than serde's
/// enum machinery so that "unknown op" and "malformed payload" stay
/// *distinct* machine-readable rejections. The derive here is what makes
/// operations serializable into the decision ledger and the snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PlanOperation {
    /// Add a task node to the plan.
    CreateTask(CreateTask),
    /// Add a dependency edge between two existing nodes.
    AddDependency(AddDependency),
    /// Route a node to a worker pool (PRD Appendix D `pools:`).
    AssignPool(AssignPool),
    /// Insert an independent review gate downstream of a node (PRD §6.3
    /// REVIEW; the mastermind tier-3 reviewer).
    RequestReview(RequestReview),
    /// Raise a node (or the whole run) to a stronger agent, a supervisor,
    /// or a human (PRD §6.3 ESCALATE).
    Escalate(Escalate),
    /// Declare the goal complete.
    CloseGoal(CloseGoal),
}

impl PlanOperation {
    /// The operation's wire name.
    pub fn op(&self) -> &'static str {
        match self {
            PlanOperation::CreateTask(_) => "create_task",
            PlanOperation::AddDependency(_) => "add_dependency",
            PlanOperation::AssignPool(_) => "assign_pool",
            PlanOperation::RequestReview(_) => "request_review",
            PlanOperation::Escalate(_) => "escalate",
            PlanOperation::CloseGoal(_) => "close_goal",
        }
    }

    /// Whether this operation changes the shape of the task graph.
    ///
    /// Structural operations are refused once the plan is materialized into
    /// durable engine state: from that moment the workflow engine owns the
    /// graph, and re-planning means proposing a new run.
    pub fn is_structural(&self) -> bool {
        matches!(
            self,
            PlanOperation::CreateTask(_)
                | PlanOperation::AddDependency(_)
                | PlanOperation::AssignPool(_)
                | PlanOperation::RequestReview(_)
        )
    }
}

/// `create_task` payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateTask {
    /// Node id, unique within the plan.
    pub node_id: String,
    /// Execution primitive; defaults to `run` (PRD §6.3).
    #[serde(default = "default_node_type")]
    pub node_type: NodeType,
    /// Nodes that must finish first. Every entry must already exist —
    /// forward references are rejected so the model gets a local, fixable
    /// error instead of a whole-spec validation failure at commit time.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Worker pool to route this node to (must be in the configured
    /// roster).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// Human-readable objective recorded on the plan; the engine
    /// synthesizes the durable `TaskContract` from the node at run
    /// materialization (F-06 `TaskContract::for_node`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    /// Scheduling priority; defaults to the store's `P2` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<Priority>,
    /// Per-node attempt/time/cost ceilings (OR-08). Omitted ⇒ engine
    /// defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budgets: Option<Budgets>,
    /// Transient-vs-reasoning retry allowance. Omitted ⇒ engine defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
}

fn default_node_type() -> NodeType {
    NodeType::Run
}

/// `add_dependency` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AddDependency {
    /// The dependent node.
    pub node_id: String,
    /// The node it must wait for.
    pub depends_on: String,
}

/// `assign_pool` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssignPool {
    /// The node to route.
    pub node_id: String,
    /// Target worker pool (must be in the configured roster).
    pub pool: String,
}

/// `request_review` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestReview {
    /// The node whose output must be reviewed.
    pub node_id: String,
    /// Reviewer pool; defaults to the policy's reviewer pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_pool: Option<String>,
    /// Id for the inserted review node; defaults to `review-<nodeId>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_node_id: Option<String>,
}

/// `escalate` payload (PRD §6.3 ESCALATE: raise to stronger
/// agent/supervisor/human).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Escalate {
    /// The node to raise; `None` escalates the whole run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// Who to raise it to.
    pub target: EscalationTarget,
    /// Why — recorded in the decision ledger and shown in the UI (OR-01
    /// "explain current plan and major decisions").
    pub reason: String,
}

/// Escalation target (PRD §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationTarget {
    /// A domain supervisor (PRD OR-02).
    Supervisor,
    /// A stronger reasoning model in a higher-capability pool.
    StrongerAgent,
    /// A human operator (blocks on the approval surface, PRD SEC-04).
    Human,
}

impl EscalationTarget {
    /// The canonical wire string.
    pub fn as_str(self) -> &'static str {
        match self {
            EscalationTarget::Supervisor => "supervisor",
            EscalationTarget::StrongerAgent => "stronger_agent",
            EscalationTarget::Human => "human",
        }
    }
}

/// `close_goal` payload.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloseGoal {
    /// Optional closing summary for the audit bundle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// A recorded escalation (the plan's escalation ledger).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Escalation {
    /// The escalated node, when the escalation was node-scoped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// Who it was raised to.
    pub target: EscalationTarget,
    /// The stated reason (bounded).
    pub reason: String,
    /// When it was recorded.
    pub at: chrono::DateTime<chrono::Utc>,
}

/// Whether `id` is an acceptable plan-local node id.
///
/// Node ids end up as SQL row values, branch-name fragments and UI labels,
/// so they are restricted to a conservative, obviously-safe alphabet
/// (ASCII alphanumerics plus `-`, `_`, `.`). This is input validation at a
/// trust boundary, not cosmetics.
pub fn is_valid_node_id(id: &str) -> bool {
    !id.is_empty()
        && id.chars().count() <= NODE_ID_MAX_CHARS
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_task_round_trips_in_camel_case_with_an_op_tag() {
        let op = PlanOperation::CreateTask(CreateTask {
            node_id: "impl-api".to_owned(),
            node_type: NodeType::Run,
            depends_on: vec!["spec".to_owned()],
            pool: Some("backend".to_owned()),
            objective: Some("implement the REST surface".to_owned()),
            priority: Some(Priority::P1),
            budgets: None,
            retry: None,
        });
        let wire = serde_json::to_value(&op).unwrap();
        assert_eq!(
            wire,
            json!({
                "op": "create_task",
                "nodeId": "impl-api",
                "nodeType": "run",
                "dependsOn": ["spec"],
                "pool": "backend",
                "objective": "implement the REST surface",
                "priority": "p1"
            })
        );
        let parsed: PlanOperation = serde_json::from_value(wire).unwrap();
        assert_eq!(parsed, op);
        assert_eq!(parsed.op(), "create_task");
        assert!(parsed.is_structural());
    }

    #[test]
    fn escalate_and_close_goal_are_not_structural() {
        let escalate = PlanOperation::Escalate(Escalate {
            node_id: Some("impl-api".to_owned()),
            target: EscalationTarget::Human,
            reason: "two reasoning failures in a row".to_owned(),
        });
        assert!(!escalate.is_structural());
        assert_eq!(
            serde_json::to_value(&escalate).unwrap(),
            json!({
                "op": "escalate",
                "nodeId": "impl-api",
                "target": "human",
                "reason": "two reasoning failures in a row"
            })
        );
        assert!(!PlanOperation::CloseGoal(CloseGoal::default()).is_structural());
    }

    #[test]
    fn payloads_reject_unknown_fields() {
        // A misspelled key must be an error, not a silent drop: `node_id`
        // (snake) instead of `nodeId` would otherwise mean "no node id".
        let err = serde_json::from_value::<CreateTask>(json!({
            "nodeId": "a",
            "node_id": "a"
        }))
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    #[test]
    fn known_operations_match_the_variant_names() {
        let ops = [
            PlanOperation::CreateTask(CreateTask {
                node_id: "a".to_owned(),
                node_type: NodeType::Run,
                depends_on: vec![],
                pool: None,
                objective: None,
                priority: None,
                budgets: None,
                retry: None,
            }),
            PlanOperation::AddDependency(AddDependency {
                node_id: "a".to_owned(),
                depends_on: "b".to_owned(),
            }),
            PlanOperation::AssignPool(AssignPool {
                node_id: "a".to_owned(),
                pool: "backend".to_owned(),
            }),
            PlanOperation::RequestReview(RequestReview {
                node_id: "a".to_owned(),
                reviewer_pool: None,
                review_node_id: None,
            }),
            PlanOperation::Escalate(Escalate {
                node_id: None,
                target: EscalationTarget::Supervisor,
                reason: "r".to_owned(),
            }),
            PlanOperation::CloseGoal(CloseGoal::default()),
        ];
        let names: Vec<&str> = ops.iter().map(PlanOperation::op).collect();
        assert_eq!(names, KNOWN_OPERATIONS.to_vec());
        for op in &ops {
            let wire = serde_json::to_value(op).unwrap();
            assert_eq!(wire["op"], json!(op.op()));
        }
    }

    #[test]
    fn node_id_validation_rejects_empty_long_and_exotic_ids() {
        assert!(is_valid_node_id("impl-api_v2.1"));
        assert!(!is_valid_node_id(""));
        assert!(!is_valid_node_id(&"a".repeat(NODE_ID_MAX_CHARS + 1)));
        assert!(!is_valid_node_id("drop table tasks"));
        assert!(!is_valid_node_id("../../etc/passwd"));
        assert!(!is_valid_node_id("node\nid"));
    }

    #[test]
    fn escalation_targets_use_snake_case_wire_names() {
        for (target, wire) in [
            (EscalationTarget::Supervisor, "supervisor"),
            (EscalationTarget::StrongerAgent, "stronger_agent"),
            (EscalationTarget::Human, "human"),
        ] {
            assert_eq!(serde_json::to_value(target).unwrap(), json!(wire));
            assert_eq!(target.as_str(), wire);
        }
    }
}
