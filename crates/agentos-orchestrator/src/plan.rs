//! The plan draft and the command-application path (PRD §9 OR-01).
//!
//! A [`Plan`] is the orchestrator's *proposal surface*: a
//! [`WorkflowSpec`](agentos_workflow::WorkflowSpec) under construction plus
//! the routing, priority and escalation decisions that go with it. Nothing
//! here is durable and nothing here schedules work — committing a plan
//! ([`crate::sink::PlanSink`]) hands it to the workflow engine, which
//! validates it again and owns every task record from that moment on.
//!
//! ## Where authority lives
//!
//! | Check | Owner |
//! |---|---|
//! | acyclicity, dangling dependencies, duplicate ids, bounded loops | `agentos-workflow` [`validate`] — called on a **tentative** spec before any mutation lands |
//! | task lifecycle legality, leases, budgets, retries | `agentos-workflow` store/scheduler |
//! | node-id shape, pool roster, plan size, closed/committed gates | this module (plan-domain rules the engine does not model) |
//!
//! The orchestrator never re-implements a rule the engine already owns: an
//! `add_dependency` that would close a cycle is refused because
//! [`validate`] said so, and the engine's own [`ValidationError`] travels
//! back to the model inside
//! [`RejectionReason::SpecInvalid`](crate::RejectionReason::SpecInvalid).
//!
//! ## Committed plans are append-only
//!
//! Once a plan is materialized into a run, the *existing* node specs are
//! frozen: `add_dependency` and `assign_pool` rewrite a materialized node
//! and are rejected with `run_already_started`. Appending is different —
//! `create_task` and `request_review` go through the engine's own
//! `add_task`, which validates the tentative DAG (cycles, dangling
//! dependencies, duplicate ids) inside the insert transaction and can
//! refuse. A refused append is dropped from the draft and reported as
//! `engine_rejected`, so the draft never claims work that has no durable
//! task. `escalate` retunes priority through
//! [`TaskStore::set_priority`](agentos_workflow::TaskStore::set_priority)
//! and `close_goal` stays vetoable with `run_not_terminal`.

use std::collections::BTreeSet;

use agentos_core::Priority;
use agentos_workflow::{
    validate, Budgets, NodeSpec, NodeType, RetryPolicy, RunStatus, WorkflowError, WorkflowSpec,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Rejection, RejectionReason};
use crate::operation::{
    is_valid_node_id, AddDependency, AssignPool, CloseGoal, CreateTask, Escalate, Escalation,
    PlanOperation, RequestReview, NODE_ID_MAX_CHARS, REASON_MAX_CHARS,
};

/// Default worker-pool roster, taken from the PRD Appendix D team config
/// (`pools: frontend / backend / coding_review`). Pools are *declared*
/// infrastructure; the orchestrator routes to them and cannot invent new
/// ones.
pub const DEFAULT_POOLS: [&str; 3] = ["frontend", "backend", "coding_review"];

/// Default reviewer pool used by `request_review` when none is named — the
/// mastermind third tier (sonnet-class review, HANDOFF-BUILD-2 §2).
pub const DEFAULT_REVIEWER_POOL: &str = "coding_review";

/// Bounds on what a plan may become (PRD Appendix G: "every retry/loop is
/// bounded by count, time or budget" — planning is no exception).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanPolicy {
    /// Workflow template id stamped on the committed spec.
    pub workflow_id: String,
    /// Workflow template version.
    pub version: u32,
    /// The worker pools this deployment actually has.
    pub pools: Vec<String>,
    /// Pool used for `request_review` when the model names none.
    pub reviewer_pool: String,
    /// Pool an `escalate` to a **stronger agent** retargets tasks at.
    /// `None` (the default) means the deployment declares no higher-capability
    /// tier, and such an escalation raises priority only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation_pool: Option<String>,
    /// Pool an `escalate` to a **supervisor** retargets tasks at (PRD OR-02
    /// domain supervisors). `None` until those pools exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervisor_pool: Option<String>,
    /// Hard ceiling on plan nodes.
    pub max_nodes: usize,
    /// Hard ceiling on operations accepted from one model response.
    pub max_operations_per_cycle: usize,
    /// Hard ceiling on planning cycles per goal.
    pub max_cycles: u32,
}

impl Default for PlanPolicy {
    fn default() -> Self {
        Self {
            workflow_id: "orchestrated-goal".to_owned(),
            version: 1,
            pools: DEFAULT_POOLS.iter().map(|p| (*p).to_owned()).collect(),
            reviewer_pool: DEFAULT_REVIEWER_POOL.to_owned(),
            escalation_pool: None,
            supervisor_pool: None,
            max_nodes: 64,
            max_operations_per_cycle: 32,
            max_cycles: 4,
        }
    }
}

impl PlanPolicy {
    /// Build a policy over a dynamic roster (F-13): pools are the enabled
    /// registry agent ids, and the reviewer pool is one of them. The
    /// daemon composes this from `AgentRegistry::enabled_roster()`; the
    /// orchestrator crate stays registry-agnostic (strings in, strings
    /// out).
    pub fn for_pools(pools: Vec<String>, reviewer_pool: impl Into<String>) -> Self {
        Self {
            pools,
            reviewer_pool: reviewer_pool.into(),
            ..Self::default()
        }
    }

    /// Declare the pool a `stronger_agent` escalation retargets tasks at.
    pub fn with_escalation_pool(mut self, pool: impl Into<String>) -> Self {
        self.escalation_pool = Some(pool.into());
        self
    }

    /// Declare the pool a `supervisor` escalation retargets tasks at.
    pub fn with_supervisor_pool(mut self, pool: impl Into<String>) -> Self {
        self.supervisor_pool = Some(pool.into());
        self
    }
}

/// A node of the draft plan: the workflow node spec plus the orchestrator's
/// own annotations (priority, stated objective).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlannedNode {
    /// The spec that will be handed to the engine verbatim.
    pub spec: NodeSpec,
    /// Priority to apply after materialization via
    /// [`TaskStore::set_priority`](agentos_workflow::TaskStore::set_priority).
    /// `None` leaves the store default (`P2`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<Priority>,
    /// The orchestrator's stated objective for the node (UI + audit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    /// Whether a durable task exists for this node yet. Nodes drafted
    /// before the commit are materialized by it; nodes appended to a live
    /// run are materialized one at a time through the sink.
    #[serde(default)]
    pub materialized: bool,
}

/// The orchestrator's plan for one goal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    /// The user goal being decomposed.
    pub goal: String,
    /// Bounds and routing roster.
    pub policy: PlanPolicy,
    /// Draft nodes, in creation order (order is stable so the compiled spec
    /// and every snapshot are deterministic).
    nodes: Vec<PlannedNode>,
    /// Accepted operations in order — the OR-01 decision ledger.
    ledger: Vec<PlanOperation>,
    /// Escalations raised so far.
    escalations: Vec<Escalation>,
    /// The run this plan was materialized into, once committed.
    run_id: Option<Uuid>,
    /// Last run status observed from the engine (refreshed by the
    /// orchestrator before each cycle; the engine remains the source of
    /// truth).
    run_status: Option<RunStatus>,
    /// Closing summary once the goal is closed.
    closed: Option<CloseGoal>,
}

impl Plan {
    /// Start an empty plan for `goal`.
    pub fn new(goal: impl Into<String>, policy: PlanPolicy) -> Self {
        Self {
            goal: goal.into(),
            policy,
            nodes: Vec::new(),
            ledger: Vec::new(),
            escalations: Vec::new(),
            run_id: None,
            run_status: None,
            closed: None,
        }
    }

    /// The draft nodes.
    pub fn nodes(&self) -> &[PlannedNode] {
        &self.nodes
    }

    /// The accepted-operation ledger (OR-01: the orchestrator's decisions
    /// are auditable, not implicit in the resulting graph).
    pub fn ledger(&self) -> &[PlanOperation] {
        &self.ledger
    }

    /// Escalations raised against this plan.
    pub fn escalations(&self) -> &[Escalation] {
        &self.escalations
    }

    /// The committed run, if any.
    pub fn run_id(&self) -> Option<Uuid> {
        self.run_id
    }

    /// The last run status observed from the engine.
    pub fn run_status(&self) -> Option<RunStatus> {
        self.run_status
    }

    /// Whether the goal has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.is_some()
    }

    /// The closing summary, once closed.
    pub fn closing_summary(&self) -> Option<&str> {
        self.closed
            .as_ref()
            .and_then(|close| close.summary.as_deref())
    }

    /// Record the run this plan materialized into. Called by the sink after
    /// a successful commit; from here on structural operations are refused.
    pub fn mark_committed(&mut self, run_id: Uuid) {
        self.run_id = Some(run_id);
        self.run_status = Some(RunStatus::Running);
        for node in &mut self.nodes {
            node.materialized = true;
        }
    }

    /// Draft nodes with no durable task yet, in insertion order — what the
    /// orchestrator still has to push into a live run.
    pub fn pending_materialization(&self) -> Vec<PlannedNode> {
        self.nodes
            .iter()
            .filter(|node| !node.materialized)
            .cloned()
            .collect()
    }

    /// Record that the engine created a durable task for `node_id`.
    pub fn mark_materialized(&mut self, node_id: &str) {
        if let Some(node) = self.nodes.iter_mut().find(|node| node.spec.id == node_id) {
            node.materialized = true;
        }
    }

    /// Drop a node the engine refused, so the draft never claims work that
    /// does not exist durably. Any draft edge into it is dropped with it —
    /// the engine would reject those adds as dangling anyway.
    pub fn drop_node(&mut self, node_id: &str) {
        self.nodes.retain(|node| node.spec.id != node_id);
        for node in &mut self.nodes {
            node.spec.depends_on.retain(|dep| dep != node_id);
        }
    }

    /// Refresh the cached engine run status.
    pub fn observe_run_status(&mut self, status: RunStatus) {
        self.run_status = Some(status);
    }

    /// Look up a draft node.
    pub fn node(&self, id: &str) -> Option<&PlannedNode> {
        self.nodes.iter().find(|node| node.spec.id == id)
    }

    /// Node ids currently in the plan (used to make `unknown_node`
    /// rejections actionable).
    pub fn node_ids(&self) -> Vec<String> {
        self.nodes.iter().map(|node| node.spec.id.clone()).collect()
    }

    /// Compile the draft into a workflow spec for the engine.
    pub fn to_spec(&self) -> WorkflowSpec {
        WorkflowSpec {
            id: self.policy.workflow_id.clone(),
            version: self.policy.version,
            nodes: self.nodes.iter().map(|node| node.spec.clone()).collect(),
        }
    }

    /// Run the workflow engine's own validator over the compiled spec.
    ///
    /// This is the same function `WorkflowEngine::start_run` calls, so a
    /// plan that passes here fails at commit only for reasons outside the
    /// spec (storage).
    pub fn validate(&self) -> Result<(), WorkflowError> {
        validate(&self.to_spec())
    }

    /// Apply one proposed operation.
    ///
    /// `Ok(())` means the operation was accepted and the draft (or, for
    /// escalations, the escalation ledger) changed. `Err(reason)` means the
    /// operation was refused with a machine-readable reason and the plan is
    /// **unchanged** — application is all-or-nothing per operation, so a
    /// rejected batch never leaves a half-applied graph.
    pub fn apply(&mut self, operation: &PlanOperation) -> Result<(), RejectionReason> {
        if self.is_closed() {
            return Err(RejectionReason::GoalClosed {
                op: operation.op().to_owned(),
            });
        }
        // A committed run absorbs appended nodes (the engine validates the
        // tentative DAG on insert), but never a rewrite of a node spec it
        // already materialized.
        if operation.is_structural() && !operation.is_additive() {
            if let Some(run_id) = self.run_id {
                return Err(RejectionReason::RunAlreadyStarted {
                    op: operation.op().to_owned(),
                    run_id: run_id.to_string(),
                });
            }
        }
        match operation {
            PlanOperation::CreateTask(payload) => self.create_task(payload)?,
            PlanOperation::AddDependency(payload) => self.add_dependency(payload)?,
            PlanOperation::AssignPool(payload) => self.assign_pool(payload)?,
            PlanOperation::RequestReview(payload) => self.request_review(payload)?,
            PlanOperation::Escalate(payload) => self.escalate(payload)?,
            PlanOperation::CloseGoal(payload) => self.close_goal(payload)?,
        }
        self.ledger.push(operation.clone());
        tracing::debug!(op = operation.op(), "plan operation accepted");
        Ok(())
    }

    /// Apply a batch, collecting rejections. Accepted operations take
    /// effect in order; a rejection never aborts the batch (later
    /// operations may be independent) and never mutates the plan.
    pub fn apply_all<'a, I>(&mut self, operations: I) -> Vec<Rejection>
    where
        I: IntoIterator<Item = (usize, &'a PlanOperation)>,
    {
        let mut rejections = Vec::new();
        for (index, operation) in operations {
            if let Err(reason) = self.apply(operation) {
                tracing::warn!(
                    index,
                    op = operation.op(),
                    code = reason.code(),
                    "plan operation rejected"
                );
                let raw = serde_json::to_value(operation).unwrap_or(serde_json::Value::Null);
                rejections.push(Rejection::new(index, raw, reason));
            }
        }
        rejections
    }

    // -------------------------------------------------------- operations

    fn create_task(&mut self, payload: &CreateTask) -> Result<(), RejectionReason> {
        self.check_node_id("create_task", &payload.node_id)?;
        if self.node(&payload.node_id).is_some() {
            return Err(RejectionReason::DuplicateNode {
                node: payload.node_id.clone(),
            });
        }
        if self.nodes.len() >= self.policy.max_nodes {
            return Err(RejectionReason::PlanTooLarge {
                limit: self.policy.max_nodes,
            });
        }
        // Forward references are refused here rather than at commit time so
        // the model gets a local, fixable error naming the missing node.
        let mut seen = BTreeSet::new();
        for dependency in &payload.depends_on {
            if dependency == &payload.node_id {
                return Err(RejectionReason::SelfDependency {
                    node: payload.node_id.clone(),
                });
            }
            if self.node(dependency).is_none() {
                return Err(RejectionReason::UnknownNode {
                    node: dependency.clone(),
                    known: self.node_ids(),
                });
            }
            if !seen.insert(dependency.clone()) {
                return Err(RejectionReason::DuplicateDependency {
                    node: payload.node_id.clone(),
                    dependency: dependency.clone(),
                });
            }
        }
        let pool = match &payload.pool {
            Some(pool) => Some(self.check_pool(pool)?),
            None => None,
        };
        if let Some(objective) = &payload.objective {
            self.check_reason("create_task", "objective", objective)?;
            if defers_human_discovery(objective) {
                return Err(RejectionReason::DeferredHumanDiscovery {
                    node: payload.node_id.clone(),
                });
            }
        }
        let node = PlannedNode {
            spec: NodeSpec {
                id: payload.node_id.clone(),
                node_type: payload.node_type,
                depends_on: payload.depends_on.clone(),
                agent_role: pool,
                budgets: payload.budgets.unwrap_or_default(),
                retry: payload.retry.unwrap_or_default(),
            },
            priority: payload.priority,
            objective: payload.objective.clone(),
            materialized: false,
        };
        self.commit_nodes({
            let mut candidate = self.nodes.clone();
            candidate.push(node);
            candidate
        })
    }

    fn add_dependency(&mut self, payload: &AddDependency) -> Result<(), RejectionReason> {
        if payload.node_id == payload.depends_on {
            return Err(RejectionReason::SelfDependency {
                node: payload.node_id.clone(),
            });
        }
        for id in [&payload.node_id, &payload.depends_on] {
            if self.node(id).is_none() {
                return Err(RejectionReason::UnknownNode {
                    node: id.clone(),
                    known: self.node_ids(),
                });
            }
        }
        let mut candidate = self.nodes.clone();
        let node = candidate
            .iter_mut()
            .find(|node| node.spec.id == payload.node_id)
            .expect("node presence checked above");
        if node.spec.depends_on.contains(&payload.depends_on) {
            return Err(RejectionReason::DuplicateDependency {
                node: payload.node_id.clone(),
                dependency: payload.depends_on.clone(),
            });
        }
        node.spec.depends_on.push(payload.depends_on.clone());
        // Cycle detection is the workflow engine's job, not ours.
        self.commit_nodes(candidate)
    }

    fn assign_pool(&mut self, payload: &AssignPool) -> Result<(), RejectionReason> {
        if self.node(&payload.node_id).is_none() {
            return Err(RejectionReason::UnknownNode {
                node: payload.node_id.clone(),
                known: self.node_ids(),
            });
        }
        let pool = self.check_pool(&payload.pool)?;
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.spec.id == payload.node_id)
            .expect("node presence checked above");
        node.spec.agent_role = Some(pool);
        Ok(())
    }

    fn request_review(&mut self, payload: &RequestReview) -> Result<(), RejectionReason> {
        if self.node(&payload.node_id).is_none() {
            return Err(RejectionReason::UnknownNode {
                node: payload.node_id.clone(),
                known: self.node_ids(),
            });
        }
        let review_id = payload
            .review_node_id
            .clone()
            .unwrap_or_else(|| format!("review-{}", payload.node_id));
        self.check_node_id("request_review", &review_id)?;
        if self.node(&review_id).is_some() {
            return Err(RejectionReason::DuplicateNode { node: review_id });
        }
        if self.nodes.len() >= self.policy.max_nodes {
            return Err(RejectionReason::PlanTooLarge {
                limit: self.policy.max_nodes,
            });
        }
        let reviewer = match &payload.reviewer_pool {
            Some(pool) => self.check_pool(pool)?,
            None => self.policy.reviewer_pool.clone(),
        };
        let mut candidate = self.nodes.clone();
        candidate.push(PlannedNode {
            spec: NodeSpec {
                id: review_id.clone(),
                node_type: NodeType::Review,
                depends_on: vec![payload.node_id.clone()],
                agent_role: Some(reviewer),
                budgets: Budgets::default(),
                retry: RetryPolicy::default(),
            },
            priority: None,
            objective: Some(format!("independent review of `{}`", payload.node_id)),
            materialized: false,
        });
        self.commit_nodes(candidate)
    }

    fn escalate(&mut self, payload: &Escalate) -> Result<(), RejectionReason> {
        self.check_reason("escalate", "reason", &payload.reason)?;
        if let Some(node_id) = &payload.node_id {
            // A node-scoped escalation must name something the plan (and
            // therefore the run) actually contains.
            if self.node(node_id).is_none() {
                return Err(RejectionReason::UnknownNode {
                    node: node_id.clone(),
                    known: self.node_ids(),
                });
            }
        }
        self.escalations.push(Escalation {
            node_id: payload.node_id.clone(),
            target: payload.target,
            reason: payload.reason.clone(),
            at: Utc::now(),
        });
        tracing::info!(
            node = payload.node_id.as_deref().unwrap_or("<run>"),
            target = payload.target.as_str(),
            "orchestrator escalation recorded"
        );
        Ok(())
    }

    fn close_goal(&mut self, payload: &CloseGoal) -> Result<(), RejectionReason> {
        let Some(_run_id) = self.run_id else {
            return Err(RejectionReason::NothingToClose);
        };
        // The engine decides when work is finished. A model that declares
        // victory over a still-running run is refused.
        match self.run_status {
            Some(RunStatus::Completed) | Some(RunStatus::Failed) => {}
            Some(RunStatus::Running) | None => {
                return Err(RejectionReason::RunNotTerminal {
                    status: self
                        .run_status
                        .map(|status| status.as_str().to_owned())
                        .unwrap_or_else(|| "unknown".to_owned()),
                })
            }
        }
        if let Some(summary) = &payload.summary {
            self.check_reason("close_goal", "summary", summary)?;
        }
        self.closed = Some(payload.clone());
        Ok(())
    }

    // ------------------------------------------------------------ helpers

    /// Validate a candidate node set through the workflow engine, adopting
    /// it only if the engine accepts it.
    fn commit_nodes(&mut self, candidate: Vec<PlannedNode>) -> Result<(), RejectionReason> {
        let spec = WorkflowSpec {
            id: self.policy.workflow_id.clone(),
            version: self.policy.version,
            nodes: candidate.iter().map(|node| node.spec.clone()).collect(),
        };
        validate(&spec).map_err(|err| match err {
            // The engine's own machine-readable spec rejection, carried
            // through verbatim so the model can self-correct.
            WorkflowError::Validation(validation) => RejectionReason::SpecInvalid { validation },
            other => RejectionReason::EngineRejected {
                detail: other.to_string(),
            },
        })?;
        self.nodes = candidate;
        Ok(())
    }

    fn check_node_id(&self, op: &str, id: &str) -> Result<(), RejectionReason> {
        if is_valid_node_id(id) {
            Ok(())
        } else {
            Err(RejectionReason::InvalidField {
                op: op.to_owned(),
                field: "nodeId".to_owned(),
                detail: format!("node ids must be 1..={NODE_ID_MAX_CHARS} chars of [A-Za-z0-9._-]"),
            })
        }
    }

    fn check_pool(&self, pool: &str) -> Result<String, RejectionReason> {
        if self.policy.pools.iter().any(|known| known == pool) {
            Ok(pool.to_owned())
        } else {
            Err(RejectionReason::UnknownPool {
                pool: crate::error::excerpt(pool),
                known: self.policy.pools.clone(),
            })
        }
    }

    fn check_reason(&self, op: &str, field: &str, text: &str) -> Result<(), RejectionReason> {
        if text.trim().is_empty() {
            return Err(RejectionReason::InvalidField {
                op: op.to_owned(),
                field: field.to_owned(),
                detail: "must not be empty".to_owned(),
            });
        }
        if text.chars().count() > REASON_MAX_CHARS {
            return Err(RejectionReason::InvalidField {
                op: op.to_owned(),
                field: field.to_owned(),
                detail: format!("must be at most {REASON_MAX_CHARS} characters"),
            });
        }
        Ok(())
    }
}

fn defers_human_discovery(objective: &str) -> bool {
    let normalized = objective.to_ascii_lowercase();
    [
        "interview the user",
        "interview user to",
        "ask the user",
        "clarify with the user",
        "collect requirements from the user",
        "gather requirements from the user",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::EscalationTarget;

    fn plan() -> Plan {
        Plan::new("ship the API", PlanPolicy::default())
    }

    fn create(node: &str, deps: &[&str]) -> PlanOperation {
        PlanOperation::CreateTask(CreateTask {
            node_id: node.to_owned(),
            node_type: NodeType::Run,
            depends_on: deps.iter().map(|d| (*d).to_owned()).collect(),
            pool: None,
            objective: None,
            priority: None,
            budgets: None,
            retry: None,
        })
    }

    #[test]
    fn create_task_builds_a_valid_spec_and_records_the_ledger() {
        let mut plan = plan();
        plan.apply(&create("spec", &[])).unwrap();
        plan.apply(&create("build", &["spec"])).unwrap();
        assert_eq!(plan.node_ids(), vec!["spec", "build"]);
        assert_eq!(plan.ledger().len(), 2);
        plan.validate().expect("engine must accept the plan");
        let spec = plan.to_spec();
        assert_eq!(spec.nodes[1].depends_on, vec!["spec".to_owned()]);
    }

    #[test]
    fn create_task_cannot_defer_product_discovery_to_a_worker() {
        let mut plan = plan();
        let operation = PlanOperation::CreateTask(CreateTask {
            node_id: "product-spec".to_owned(),
            node_type: NodeType::Run,
            depends_on: vec![],
            pool: Some("backend".to_owned()),
            objective: Some(
                "Interview user to resolve session pairing, authentication, and deployment."
                    .to_owned(),
            ),
            priority: None,
            budgets: None,
            retry: None,
        });

        let reason = plan
            .apply(&operation)
            .expect_err("discovery must stay human-gated");
        assert!(matches!(
            reason,
            RejectionReason::DeferredHumanDiscovery { ref node } if node == "product-spec"
        ));
        assert!(plan.nodes().is_empty());
    }

    #[test]
    fn create_task_rejects_duplicates_bad_ids_unknown_deps_and_self_deps() {
        let mut plan = plan();
        plan.apply(&create("spec", &[])).unwrap();

        assert_eq!(
            plan.apply(&create("spec", &[])).unwrap_err().code(),
            "duplicate_node"
        );
        assert_eq!(
            plan.apply(&create("bad id!", &[])).unwrap_err().code(),
            "invalid_field"
        );
        assert_eq!(
            plan.apply(&create("build", &["ghost"])).unwrap_err().code(),
            "unknown_node"
        );
        assert_eq!(
            plan.apply(&create("loop", &["loop"])).unwrap_err().code(),
            "self_dependency"
        );
        assert_eq!(
            plan.apply(&create("dupe", &["spec", "spec"]))
                .unwrap_err()
                .code(),
            "duplicate_dependency"
        );
        // Every rejection left the plan untouched.
        assert_eq!(plan.node_ids(), vec!["spec"]);
        assert_eq!(plan.ledger().len(), 1);
    }

    #[test]
    fn create_task_rejects_unknown_pools_and_respects_the_node_ceiling() {
        let mut plan = Plan::new(
            "small",
            PlanPolicy {
                max_nodes: 1,
                ..PlanPolicy::default()
            },
        );
        let mut with_pool = create("a", &[]);
        if let PlanOperation::CreateTask(payload) = &mut with_pool {
            payload.pool = Some("nonexistent".to_owned());
        }
        assert_eq!(plan.apply(&with_pool).unwrap_err().code(), "unknown_pool");

        plan.apply(&create("a", &[])).unwrap();
        assert_eq!(
            plan.apply(&create("b", &[])).unwrap_err().code(),
            "plan_too_large"
        );
    }

    #[test]
    fn add_dependency_delegates_cycle_detection_to_the_engine() {
        let mut plan = plan();
        plan.apply(&create("a", &[])).unwrap();
        plan.apply(&create("b", &["a"])).unwrap();

        let cycle = PlanOperation::AddDependency(AddDependency {
            node_id: "a".to_owned(),
            depends_on: "b".to_owned(),
        });
        let reason = plan.apply(&cycle).unwrap_err();
        assert_eq!(reason.code(), "spec_invalid");
        match reason {
            RejectionReason::SpecInvalid { validation } => {
                // The engine's own machine-readable error, carried through.
                assert!(
                    matches!(
                        validation,
                        agentos_workflow::ValidationError::CycleDetected { .. }
                    ),
                    "{validation:?}"
                );
            }
            other => panic!("expected spec_invalid, got {other:?}"),
        }
        // Refused edge did not land.
        assert!(plan.node("a").unwrap().spec.depends_on.is_empty());
        assert!(plan.validate().is_ok());
    }

    #[test]
    fn add_dependency_rejects_unknown_nodes_self_edges_and_duplicates() {
        let mut plan = plan();
        plan.apply(&create("a", &[])).unwrap();
        plan.apply(&create("b", &["a"])).unwrap();

        let edge = |node: &str, dep: &str| {
            PlanOperation::AddDependency(AddDependency {
                node_id: node.to_owned(),
                depends_on: dep.to_owned(),
            })
        };
        assert_eq!(
            plan.apply(&edge("ghost", "a")).unwrap_err().code(),
            "unknown_node"
        );
        assert_eq!(
            plan.apply(&edge("a", "ghost")).unwrap_err().code(),
            "unknown_node"
        );
        assert_eq!(
            plan.apply(&edge("a", "a")).unwrap_err().code(),
            "self_dependency"
        );
        assert_eq!(
            plan.apply(&edge("b", "a")).unwrap_err().code(),
            "duplicate_dependency"
        );
    }

    #[test]
    fn assign_pool_routes_known_pools_and_rejects_unknown_ones() {
        let mut plan = plan();
        plan.apply(&create("api", &[])).unwrap();
        plan.apply(&PlanOperation::AssignPool(AssignPool {
            node_id: "api".to_owned(),
            pool: "backend".to_owned(),
        }))
        .unwrap();
        assert_eq!(
            plan.node("api").unwrap().spec.agent_role.as_deref(),
            Some("backend")
        );

        assert_eq!(
            plan.apply(&PlanOperation::AssignPool(AssignPool {
                node_id: "api".to_owned(),
                pool: "quantum".to_owned(),
            }))
            .unwrap_err()
            .code(),
            "unknown_pool"
        );
        assert_eq!(
            plan.apply(&PlanOperation::AssignPool(AssignPool {
                node_id: "ghost".to_owned(),
                pool: "backend".to_owned(),
            }))
            .unwrap_err()
            .code(),
            "unknown_node"
        );
        // Routing unchanged after the rejections.
        assert_eq!(
            plan.node("api").unwrap().spec.agent_role.as_deref(),
            Some("backend")
        );
    }

    #[test]
    fn request_review_inserts_a_review_node_downstream_of_its_target() {
        let mut plan = plan();
        plan.apply(&create("api", &[])).unwrap();
        plan.apply(&PlanOperation::RequestReview(RequestReview {
            node_id: "api".to_owned(),
            reviewer_pool: None,
            review_node_id: None,
        }))
        .unwrap();
        let review = plan.node("review-api").expect("review node inserted");
        assert_eq!(review.spec.node_type, NodeType::Review);
        assert_eq!(review.spec.depends_on, vec!["api".to_owned()]);
        assert_eq!(
            review.spec.agent_role.as_deref(),
            Some(DEFAULT_REVIEWER_POOL)
        );
        plan.validate().unwrap();

        // Second request for the same node collides on the derived id.
        assert_eq!(
            plan.apply(&PlanOperation::RequestReview(RequestReview {
                node_id: "api".to_owned(),
                reviewer_pool: None,
                review_node_id: None,
            }))
            .unwrap_err()
            .code(),
            "duplicate_node"
        );
        // Unknown target and unknown reviewer pool.
        assert_eq!(
            plan.apply(&PlanOperation::RequestReview(RequestReview {
                node_id: "ghost".to_owned(),
                reviewer_pool: None,
                review_node_id: None,
            }))
            .unwrap_err()
            .code(),
            "unknown_node"
        );
        assert_eq!(
            plan.apply(&PlanOperation::RequestReview(RequestReview {
                node_id: "api".to_owned(),
                reviewer_pool: Some("nope".to_owned()),
                review_node_id: Some("review-2".to_owned()),
            }))
            .unwrap_err()
            .code(),
            "unknown_pool"
        );
    }

    #[test]
    fn escalate_records_the_decision_and_rejects_empty_reasons_or_ghost_nodes() {
        let mut plan = plan();
        plan.apply(&create("api", &[])).unwrap();
        plan.apply(&PlanOperation::Escalate(Escalate {
            node_id: Some("api".to_owned()),
            target: EscalationTarget::Human,
            reason: "two reasoning failures".to_owned(),
        }))
        .unwrap();
        assert_eq!(plan.escalations().len(), 1);
        assert_eq!(plan.escalations()[0].target, EscalationTarget::Human);

        assert_eq!(
            plan.apply(&PlanOperation::Escalate(Escalate {
                node_id: None,
                target: EscalationTarget::Supervisor,
                reason: "   ".to_owned(),
            }))
            .unwrap_err()
            .code(),
            "invalid_field"
        );
        assert_eq!(
            plan.apply(&PlanOperation::Escalate(Escalate {
                node_id: Some("ghost".to_owned()),
                target: EscalationTarget::Supervisor,
                reason: "stuck".to_owned(),
            }))
            .unwrap_err()
            .code(),
            "unknown_node"
        );
        assert_eq!(plan.escalations().len(), 1);
    }

    #[test]
    fn spec_rewrites_are_refused_once_the_engine_owns_the_run_but_appends_are_not() {
        let mut plan = plan();
        plan.apply(&create("api", &[])).unwrap();
        plan.mark_committed(Uuid::now_v7());

        for operation in [
            PlanOperation::AddDependency(AddDependency {
                node_id: "api".to_owned(),
                depends_on: "api2".to_owned(),
            }),
            PlanOperation::AssignPool(AssignPool {
                node_id: "api".to_owned(),
                pool: "backend".to_owned(),
            }),
        ] {
            assert_eq!(
                plan.apply(&operation).unwrap_err().code(),
                "run_already_started",
                "{} rewrites a materialized node spec and must be refused",
                operation.op()
            );
        }

        // Appends are legal on a live run and land unmaterialized, waiting
        // for the orchestrator to push them through the engine.
        plan.apply(&PlanOperation::RequestReview(RequestReview {
            node_id: "api".to_owned(),
            reviewer_pool: None,
            review_node_id: None,
        }))
        .expect("appending a review node to a live run is legal");
        let pending = plan.pending_materialization();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].spec.id, "review-api");
        assert!(
            plan.node("api").expect("api").materialized,
            "the committed node keeps its durable task"
        );
        // Escalation stays legal on a live run — that is its purpose.
        plan.apply(&PlanOperation::Escalate(Escalate {
            node_id: Some("api".to_owned()),
            target: EscalationTarget::StrongerAgent,
            reason: "blocked on an unclear contract".to_owned(),
        }))
        .unwrap();
    }

    #[test]
    fn close_goal_needs_a_run_and_a_terminal_engine_status() {
        let mut plan = plan();
        plan.apply(&create("api", &[])).unwrap();

        assert_eq!(
            plan.apply(&PlanOperation::CloseGoal(CloseGoal::default()))
                .unwrap_err()
                .code(),
            "nothing_to_close"
        );

        plan.mark_committed(Uuid::now_v7());
        // The engine says "running"; the model does not get to overrule it.
        let reason = plan
            .apply(&PlanOperation::CloseGoal(CloseGoal::default()))
            .unwrap_err();
        assert_eq!(reason.code(), "run_not_terminal");
        assert!(!plan.is_closed());

        plan.observe_run_status(RunStatus::Completed);
        plan.apply(&PlanOperation::CloseGoal(CloseGoal {
            summary: Some("shipped".to_owned()),
        }))
        .unwrap();
        assert!(plan.is_closed());
        assert_eq!(plan.closing_summary(), Some("shipped"));

        // Sealed: every further operation is refused.
        assert_eq!(
            plan.apply(&PlanOperation::CloseGoal(CloseGoal::default()))
                .unwrap_err()
                .code(),
            "goal_closed"
        );
        assert_eq!(
            plan.apply(&PlanOperation::Escalate(Escalate {
                node_id: None,
                target: EscalationTarget::Human,
                reason: "late thought".to_owned(),
            }))
            .unwrap_err()
            .code(),
            "goal_closed"
        );
    }

    #[test]
    fn apply_all_keeps_the_good_operations_and_reports_the_rest() {
        let mut plan = plan();
        let operations = [
            create("spec", &[]),
            create("spec", &[]),            // duplicate
            create("build", &["spec"]),     // fine
            create("ghost-dep", &["nope"]), // unknown dependency
        ];
        let indexed: Vec<(usize, &PlanOperation)> = operations.iter().enumerate().collect();
        let rejections = plan.apply_all(indexed);
        assert_eq!(plan.node_ids(), vec!["spec", "build"]);
        assert_eq!(rejections.len(), 2);
        assert_eq!(rejections[0].index, 1);
        assert_eq!(rejections[0].reason.code(), "duplicate_node");
        assert_eq!(rejections[1].index, 3);
        assert_eq!(rejections[1].reason.code(), "unknown_node");
        assert_eq!(plan.ledger().len(), 2);
    }

    #[test]
    fn plan_round_trips_through_json() {
        let mut plan = plan();
        plan.apply(&create("spec", &[])).unwrap();
        plan.apply(&PlanOperation::AssignPool(AssignPool {
            node_id: "spec".to_owned(),
            pool: "backend".to_owned(),
        }))
        .unwrap();
        let wire = serde_json::to_value(&plan).unwrap();
        assert_eq!(
            wire["nodes"][0]["spec"]["nodeType"],
            serde_json::json!("run")
        );
        let parsed: Plan = serde_json::from_value(wire).unwrap();
        assert_eq!(parsed, plan);
    }
}
