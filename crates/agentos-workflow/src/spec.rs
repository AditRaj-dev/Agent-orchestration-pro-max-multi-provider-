//! Workflow definition types (PRD §9 OR-03/OR-04/OR-08) — the versioned DAG
//! shape the engine validates before execution, persists as durable task
//! records, and schedules.
//!
//! Everything here is plain serde data (JSON-serializable), matching OR-03's
//! "persist workflow as versioned DAG in JSON" requirement. Field names
//! serialize in camelCase (`dependsOn`, `agentRole`, `maxAttempts`) to match
//! the PRD data model; node types serialize as lowercase snake_case strings
//! (`"git_gate"`, `"human_approval"`, `{"loop":{"maxIterations":3}}`).

use serde::{Deserialize, Serialize};

/// A versioned workflow definition: a named DAG of nodes (PRD §9 OR-03).
///
/// `id` + `version` identify the template (`Feature Build v2`); a *run*
/// materializes one spec into durable task records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowSpec {
    /// Template identity, e.g. `feature-build`.
    pub id: String,
    /// Template version; bumps are new definitions, never in-place edits.
    pub version: u32,
    /// The DAG nodes. Must be non-empty, acyclic, with resolvable
    /// dependencies and bounded loops — see [`crate::validate`].
    pub nodes: Vec<NodeSpec>,
}

/// One node of the workflow DAG (PRD §9 OR-03 key state).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSpec {
    /// Node identity, unique within a spec.
    pub id: String,
    /// Execution primitive (PRD §6.3 subset).
    pub node_type: NodeType,
    /// Node ids that must reach terminal success before this node runs.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Agent role that should execute the node (routing hint for adapters).
    #[serde(default)]
    pub agent_role: Option<String>,
    /// Attempt/time/cost ceilings enforced before every lease (OR-08).
    #[serde(default)]
    pub budgets: Budgets,
    /// Transient-vs-reasoning retry allowance (OR-08).
    #[serde(default)]
    pub retry: RetryPolicy,
}

/// The execution primitives a workflow node may declare (PRD §6.3 subset
/// taken by OR-03: run, parallel, review, approval, gate, branch, loop).
///
/// Wire form is lowercase snake_case: `"run"`, `"parallel"`, `"review"`,
/// `"git_gate"`, `"human_approval"`, `"branch"`, and
/// `{"loop":{"maxIterations":n}}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    /// Execute one bounded task.
    Run,
    /// Fan-in point for independent branches (the fan-out itself is simply
    /// nodes without mutual dependencies).
    Parallel,
    /// Independent quality gate.
    Review,
    /// Serialized git mutation checkpoint.
    GitGate,
    /// Block mutation until a human approves.
    HumanApproval,
    /// Choose path based on state/result.
    Branch,
    /// Repeat with a bounded condition; `max_iterations` MUST be >= 1 —
    /// enforced by [`crate::validate`] before any execution.
    Loop {
        /// Hard upper bound on iterations.
        #[serde(rename = "maxIterations")]
        max_iterations: u32,
    },
}

impl NodeType {
    /// The canonical snake_case wire name for this node type.
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeType::Run => "run",
            NodeType::Parallel => "parallel",
            NodeType::Review => "review",
            NodeType::GitGate => "git_gate",
            NodeType::HumanApproval => "human_approval",
            NodeType::Branch => "branch",
            NodeType::Loop { .. } => "loop",
        }
    }
}

/// Per-node budget ceilings (PRD §9 OR-08: "workflow nodes declare
/// maxAttempts/maxElapsed/maxCost").
///
/// The scheduler enforces all three before every lease; a task found over
/// budget is escalated to `Failed`, never silently re-run.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Budgets {
    /// Maximum lease attempts the task may ever consume.
    pub max_attempts: u32,
    /// Maximum wall-clock seconds from task creation before escalation.
    pub max_elapsed_secs: u64,
    /// Optional spend ceiling in USD; checked through the scheduler's
    /// [`crate::CostLedger`] hook (stubbed until the F-07 budget ledger).
    #[serde(default)]
    pub max_cost_usd: Option<f64>,
}

impl Default for Budgets {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            max_elapsed_secs: 3600,
            max_cost_usd: None,
        }
    }
}

/// Retry allowance distinguishing transient infrastructure errors from
/// reasoning failures (PRD §9 OR-08).
///
/// `transient_retries`/`reasoning_retries` count *re-queues after failures*:
/// a failure outcome may requeue while the task's failure count is <= the
/// allowance; beyond that (or beyond [`Budgets::max_attempts`], whichever
/// binds first) the task escalates to `Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    /// Re-queues allowed after transient (infrastructure) failures.
    pub transient_retries: u32,
    /// Re-queues allowed after reasoning failures.
    pub reasoning_retries: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            transient_retries: 2,
            reasoning_retries: 1,
        }
    }
}

/// Task contract (PRD §9 OR-04) — the typed subset F-06 carries durably on
/// every task row and hands to the executor.
///
/// The full OR-04 contract adds `dependencies`, `contextRefs`, `baseCommit`
/// and `expectedArtifacts`; those belong to the policy engine (OR-04
/// "policy engine injects permission constraints") and the context compiler
/// (CTX), and are deliberately left out of F-06's subset. Likewise, F-06
/// synthesizes contracts with empty path/criteria lists via
/// [`TaskContract::for_node`]; the orchestrator (OR-01) and policy engine
/// fill the real values later.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskContract {
    /// What the worker must accomplish.
    pub objective: String,
    /// Filesystem paths the worker may touch.
    pub allowed_paths: Vec<String>,
    /// Filesystem paths the worker must not touch.
    pub forbidden_paths: Vec<String>,
    /// Conditions under which the objective is met.
    pub acceptance_criteria: Vec<String>,
    /// Checks that must pass before the output is accepted.
    pub required_checks: Vec<String>,
}

impl TaskContract {
    /// Synthesize the default contract for a workflow node: the objective
    /// names the node, its type, and the run goal; scoping lists stay empty
    /// until the policy engine (OR-04) injects them.
    pub fn for_node(node: &NodeSpec, goal: &str) -> Self {
        Self {
            objective: format!("{} `{}` for goal: {goal}", node.node_type.as_str(), node.id),
            allowed_paths: Vec::new(),
            forbidden_paths: Vec::new(),
            acceptance_criteria: Vec::new(),
            required_checks: Vec::new(),
        }
    }
}

impl WorkflowSpec {
    /// Look up a node by id.
    pub fn node(&self, id: &str) -> Option<&NodeSpec> {
        self.nodes.iter().find(|node| node.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The PRD §9 OR-03 example workflow, in its camelCase wire shape:
    /// spec (run) -> build (parallel) -> review -> commit (git_gate).
    #[test]
    fn prd_example_workflow_round_trips_through_json() {
        let wire = json!({
            "id": "feature-build",
            "version": 1,
            "nodes": [
                { "id": "spec",   "nodeType": "run",
                  "agentRole": "spec_agent" },
                { "id": "build",  "nodeType": "parallel", "dependsOn": ["spec"] },
                { "id": "review", "nodeType": "review",   "dependsOn": ["build"] },
                { "id": "commit", "nodeType": "git_gate", "dependsOn": ["review"] }
            ]
        });

        let spec: WorkflowSpec = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(spec.id, "feature-build");
        assert_eq!(spec.nodes.len(), 4);
        assert_eq!(spec.nodes[0].agent_role.as_deref(), Some("spec_agent"));
        assert_eq!(spec.nodes[1].depends_on, vec!["spec".to_owned()]);
        assert_eq!(spec.nodes[3].node_type, NodeType::GitGate);
        // Omitted budgets/retry resolve to the documented defaults.
        assert_eq!(spec.nodes[0].budgets, Budgets::default());
        assert_eq!(spec.nodes[0].retry, RetryPolicy::default());

        // Round-trip: value equality with the parsed spec (defaults included).
        let re_serialized = serde_json::to_value(&spec).unwrap();
        let round_tripped: WorkflowSpec = serde_json::from_value(re_serialized).unwrap();
        assert_eq!(round_tripped, spec);
    }

    #[test]
    fn node_types_serialize_as_snake_case() {
        let cases = [
            (NodeType::Run, json!("run")),
            (NodeType::Parallel, json!("parallel")),
            (NodeType::Review, json!("review")),
            (NodeType::GitGate, json!("git_gate")),
            (NodeType::HumanApproval, json!("human_approval")),
            (NodeType::Branch, json!("branch")),
            (
                NodeType::Loop { max_iterations: 3 },
                json!({"loop": {"maxIterations": 3}}),
            ),
        ];
        for (node_type, wire) in cases {
            assert_eq!(serde_json::to_value(node_type).unwrap(), wire);
            let parsed: NodeType = serde_json::from_value(wire).unwrap();
            assert_eq!(parsed, node_type);
        }
    }

    #[test]
    fn default_contract_names_node_type_id_and_goal() {
        let node = NodeSpec {
            id: "review".to_owned(),
            node_type: NodeType::Review,
            depends_on: vec!["build".to_owned()],
            agent_role: None,
            budgets: Budgets::default(),
            retry: RetryPolicy::default(),
        };
        let contract = TaskContract::for_node(&node, "ship F-06");
        assert_eq!(contract.objective, "review `review` for goal: ship F-06");
        assert!(contract.allowed_paths.is_empty());
        assert!(contract.forbidden_paths.is_empty());
        assert!(contract.acceptance_criteria.is_empty());
        assert!(contract.required_checks.is_empty());
    }
}
