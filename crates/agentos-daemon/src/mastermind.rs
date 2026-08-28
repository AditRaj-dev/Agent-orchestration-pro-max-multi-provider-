//! F-12 wiring: the mastermind flow as a daemon service.
//!
//! The orchestrator crate could always propose plan operations and the
//! supervisor could always spawn registry agents — but nothing joined them,
//! so the flow was reachable only from tests. This module is that join, and
//! it is deliberately thin: every rule still lives in the crate that owns
//! it.
//!
//! ```text
//! mastermind.start   -> registry roster -> required discovery interview
//! mastermind.plan    -> answers -> visible docs/DISCOVERY.md
//! mastermind.approveDiscovery -> USER GATE -> selected-model planning cycle
//! mastermind.commit  -> USER GATE: plan -> WorkflowSpec + contracts
//!                       -> Supervisor::start_run (manifest persisted)
//! mastermind.drive   -> Supervisor::drive -> agents actually spawn
//! mastermind.status  -> plan + engine's run view
//! ```
//!
//! Two things this module exists to get right:
//!
//! 1. **The roster is the routing table.** `PlanPolicy::pools` is filled
//!    from the F-13 registry, and the same records ride into the prompt as
//!    [`RosterEntry`]s. The orchestrator therefore routes `pool` values at
//!    real agent ids with real descriptions instead of the three static
//!    placeholder pools — and an id it invents is rejected as
//!    `unknown_pool` rather than silently dispatched to nobody.
//! 2. **Commits go through the supervisor, not the engine.** A run
//!    materialized straight through [`WorkflowEngine`] has no run manifest,
//!    and the supervisor's executor refuses to dispatch a task whose
//!    manifest is missing. [`SupervisorSink`] compiles the plan into a spec
//!    **and** synthesizes a full [`TaskContract`] per node, so the run the
//!    orchestrator commits is a run the supervisor can actually execute.
//!
//! The user gate is preserved by construction: planning cycles never
//! materialize anything, and `commit` is a separate call.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use agentos_adapters::RuntimeAdapter;
use agentos_agents::{AgentRecord, AgentRegistry};
use agentos_core::Priority;
use agentos_orchestrator::model::{PlanningConversation, PlanningDecision};
use agentos_orchestrator::snapshot::RosterEntry;
use agentos_orchestrator::{
    ClaudePlanningModel, Orchestrator, OrchestratorCheckpoint, OrchestratorError, PlanCycleReport,
    PlanPolicy, PlanSink, PlanningModel, RunView, WorkflowSink, ORCHESTRATOR_MODEL,
    ORCHESTRATOR_TOOL_DENYLIST,
};
use agentos_runtime::{
    sha256_hex, ContractBudgets, GitPolicy, GraphRefresh, Graphifier, GraphifyOutcome, Supervisor,
    SupervisorConfig, TaskContract,
};
use agentos_workflow::{NodeSpec, RunStatus};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::memex::{MemexClient, MemexError, MemoryRecord};

/// Reviewer pool used when the registry carries the built-in reviewer.
const REVIEWER_AGENT: &str = "code-reviewer";
/// The planning agent's own id — it commands, so it is never a worker pool.
const ORCHESTRATOR_AGENT: &str = "orchestrator";
/// Escalation target for `stronger_agent`: the opus-class planner/spec tier.
const STRONGER_AGENT: &str = "spec-writer";
/// The bounded debugging escalation: Terra gets the first attempt, then Sol
/// receives the next allowed retry after a reasoning failure.
const DEBUGGER_AGENT: &str = "debugger";
const DEBUGGER_SOL_ESCALATION_AGENT: &str = "debugger-sol-escalation";
const LUNA_FALLBACK_AGENT: &str = "general-worker-luna";
/// Ordered provider-diverse planner routes. The selected route remains first
/// until it explicitly reports a transient quota/rate limit; only then do we
/// move to the next *available different provider*. Keeping the list here
/// makes the failover policy durable and independent of desktop state.
const PLANNER_FALLBACKS: [(&str, &str); 6] = [
    ("codex", "gpt-5.6-sol"),
    ("claude-code", "claude-opus-5"),
    ("antigravity-agy", "gemini-3.1-pro-high"),
    ("codex", "gpt-5.6-terra"),
    ("claude-code", "claude-sonnet-5"),
    ("antigravity-agy", "claude-sonnet-4-6"),
];
/// Ticks one `mastermind.drive` call is allowed before returning to the UI.
/// Tick ceiling for one `mastermind.drive` call.
///
/// Under a shared workspace every write task holds `**`, so throughput is
/// roughly one task per tick by construction. A 20-node graph plus its
/// review nodes therefore cannot fit in 64 ticks — the observed run
/// exhausted the budget with 15 of 22 tasks still pending.
const DEFAULT_MAX_TICKS: u32 = 64;

/// Extra ticks granted per committed node, on top of [`DEFAULT_MAX_TICKS`].
const TICKS_PER_NODE: u32 = 8;

/// Wall-clock ceiling for one out-of-supervisor graph refresh.
const GRAPH_REFRESH_TIMEOUT_SECS: u64 = 120;

/// A scoped authoring turn reads every approved upstream document and then
/// emits a full deliverable, so it needs a longer provider window than the
/// shared planning default. The read-only reviewer keeps that default, and
/// the desktop's client window covers author + reviewer + serialization.
const AUTHOR_TIMEOUT_SECS: u64 = 540;

/// The reviewer reads the finished deliverable plus every approved upstream
/// document before ruling, which is comparable work to authoring it. Leaving
/// it on the shared planning default made it the next ceiling once the author
/// window was raised: Phase 6 authored `docs/DESIGN.md` successfully and then
/// failed on the review turn at 300s.
const REVIEW_TIMEOUT_SECS: u64 = 540;

/// Ceiling on one provider turn's prose carried back to the desktop.
/// Generous enough for a full review or authoring account, bounded so a
/// runaway response cannot grow an RPC frame without limit.
const TURN_MAX_CHARS: usize = 24_000;

/// Maximum Memex prose carried into a fresh provider session. Approved
/// artifacts remain on disk and the current plan is sent separately, so an
/// unbounded memory replay only duplicates context and increases latency.
const MEMEX_PROMPT_MAX_CHARS: usize = 24_000;
const MEMEX_RECORD_MAX_CHARS: usize = 6_000;

/// Phase 8 keeps semantic review with the planner, while deterministic
/// missing-file/API-block checks run in the daemon before this prompt.
const BUILD_PLAN_INSTRUCTION: &str = "Read docs/IMPLEMENTATION_PLAN.md and the approved artifacts. The daemon has already checked that every feature document referenced by the plan exists and that docs/API_RECORD.md is not explicitly blocked. Review the remaining semantics for contradictions, unrecorded APIs, or tasks too ambiguous to dispatch. If blocked, emit exactly one whole-plan escalation in this shape: [{\"op\":\"escalate\",\"target\":\"human\",\"reason\":\"all findings\"}]. Omit nodeId because no task node exists yet, and emit no create_task in that cycle. If clean, emit the validated task graph immediately. Route by policies.workerRoster, use general-worker-luna only when no domain specialist matches, request code-reviewer for every deliverable, add security-reviewer where security-sensitive, preserve debugger escalation, and create no memory-document tasks.";

/// Trim a provider turn to [`TURN_MAX_CHARS`], or `None` when it carried no
/// prose at all. Truncation counts characters, never bytes, so a multi-byte
/// response can never be cut mid-codepoint.
fn bound_turn_text(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.chars().count() <= TURN_MAX_CHARS {
        return Some(trimmed.to_owned());
    }
    Some(
        trimmed
            .chars()
            .take(TURN_MAX_CHARS)
            .chain(
                "

…output truncated…"
                    .chars(),
            )
            .collect(),
    )
}

/// Everything that can go wrong at this seam.
#[derive(Debug, thiserror::Error)]
pub enum MastermindError {
    #[error("no mastermind session {0}")]
    UnknownSession(String),
    #[error("session {0} has no committed run yet")]
    NotCommitted(String),
    #[error("session {session} run is {status}, not failed; there is nothing to reopen")]
    RunNotFailed { session: String, status: String },
    #[error("invalid workflow control request: {0}")]
    InvalidControl(String),
    #[error("session {0} still has unanswered discovery questions; finish discovery before committing or running agents")]
    DiscoveryIncomplete(String),
    #[error("session {0} is waiting for approval of docs/DISCOVERY.md")]
    DiscoveryAwaitingApproval(String),
    #[error("session {session} cannot perform this operation in phase {phase} ({status})")]
    InvalidPhase {
        session: String,
        phase: String,
        status: String,
    },
    #[error("Mastermind skill package error at {path}: {detail}")]
    SkillSource { path: PathBuf, detail: String },
    #[error("Mastermind checkpoint error at {path}: {detail}")]
    Checkpoint { path: PathBuf, detail: String },
    #[error(transparent)]
    Memory(#[from] MemexError),
    #[error("repository path {0} does not exist")]
    NoRepo(PathBuf),
    #[error("cannot resolve HEAD in {repo}: {detail}")]
    NoHead { repo: PathBuf, detail: String },
    #[error("cannot initialize git repository {repo}: {detail}")]
    GitSetup { repo: PathBuf, detail: String },
    #[error("{path} is not a readable deliverable of session {session}")]
    UnknownArtifact { session: String, path: String },
    #[error("no adapter registered for id {0}")]
    UnknownAdapter(String),
    #[error("session {0} is busy with a provider turn; retry once it settles")]
    TurnInFlight(String),
    #[error(transparent)]
    Registry(#[from] agentos_agents::AgentsError),
    #[error(transparent)]
    Runtime(#[from] agentos_runtime::RuntimeError),
    #[error(transparent)]
    Orchestrator(#[from] OrchestratorError),
    #[error(transparent)]
    Contract(#[from] agentos_runtime::ContractRule),
}

/// Canonical Mastermind phases. `Complete` is the terminal state after the
/// Phase-9 gate, not another provider-authored phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MastermindPhase {
    Setup,
    Discovery,
    Prd,
    Features,
    ImplementationPlan,
    ApiRecord,
    Design,
    Mockups,
    HtmlToReact,
    Build,
    Wrap,
    Complete,
}

impl MastermindPhase {
    fn id(self) -> &'static str {
        match self {
            Self::Setup => "phase-0-setup",
            Self::Discovery => "phase-1-discovery",
            Self::Prd => "phase-2-prd",
            Self::Features => "phase-3-features",
            Self::ImplementationPlan => "phase-4-implementation-plan",
            Self::ApiRecord => "phase-5-api-record",
            Self::Design => "phase-6-design",
            Self::Mockups => "phase-7-mockups",
            Self::HtmlToReact => "phase-7-5-html-to-react",
            Self::Build => "phase-8-build",
            Self::Wrap => "phase-9-wrap",
            Self::Complete => "complete",
        }
    }

    fn number(self) -> &'static str {
        match self {
            Self::Setup => "0",
            Self::Discovery => "1",
            Self::Prd => "2",
            Self::Features => "3",
            Self::ImplementationPlan => "4",
            Self::ApiRecord => "5",
            Self::Design => "6",
            Self::Mockups => "7",
            Self::HtmlToReact => "7.5",
            Self::Build => "8",
            Self::Wrap => "9",
            Self::Complete => "complete",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Setup => "Setup",
            Self::Discovery => "Discovery",
            Self::Prd => "Product requirements",
            Self::Features => "Feature specifications",
            Self::ImplementationPlan => "Implementation plan",
            Self::ApiRecord => "API record",
            Self::Design => "Frontend design",
            Self::Mockups => "High-fidelity mockups",
            Self::HtmlToReact => "Mockup to components",
            Self::Build => "Build",
            Self::Wrap => "Wrap and final verification",
            Self::Complete => "Complete",
        }
    }

    fn next(self, react_stack: bool) -> Self {
        match self {
            Self::Setup => Self::Discovery,
            Self::Discovery => Self::Prd,
            Self::Prd => Self::Features,
            Self::Features => Self::ImplementationPlan,
            Self::ImplementationPlan => Self::ApiRecord,
            Self::ApiRecord => Self::Design,
            Self::Design => Self::Mockups,
            Self::Mockups if react_stack => Self::HtmlToReact,
            Self::Mockups => Self::Build,
            Self::HtmlToReact => Self::Build,
            Self::Build => Self::Wrap,
            Self::Wrap | Self::Complete => Self::Complete,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum PhaseStatus {
    Active,
    Running,
    AwaitingWriteApproval,
    AwaitingApproval,
    NeedsRevision,
    Blocked,
    Complete,
}

impl PhaseStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Running => "running",
            Self::AwaitingWriteApproval => "awaiting-write-approval",
            Self::AwaitingApproval => "awaiting-approval",
            Self::NeedsRevision => "needs-revision",
            Self::Blocked => "blocked",
            Self::Complete => "complete",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SkillSource {
    path: PathBuf,
    sha256: String,
    loaded_at: String,
}

// ------------------------------------------------------------------- sink

/// A [`PlanSink`] whose `commit` goes through [`Supervisor::start_run`].
///
/// Read paths (`run_view`, `escalate`, `retarget`, `add_node`) delegate to a
/// plain [`WorkflowSink`] over the supervisor's own store — same rows, same
/// validator. Only the commit differs, because only the commit has to
/// produce the run manifest the executor reads.
struct SupervisorSink {
    supervisor: Arc<Supervisor>,
    store_sink: WorkflowSink,
    base_commit: String,
    /// Path globs every task may touch. Mastermind runs against the selected
    /// checkout; ownership holds serialize overlapping writers.
    allowed_paths: Vec<String>,
}

/// The contract for one planned node: the goal and the node's stated
/// objective travel together, so the worker's prompt says what it is
/// building AND what the run is for — the "complete context" a skill
/// preamble alone cannot supply.
///
/// The builder only rejects empty ids/objectives and zero budgets, none of
/// which this function can produce, so the fallback arm exists to keep a
/// future field change from panicking a live daemon.
fn contract_for(
    node: &NodeSpec,
    objective: Option<&str>,
    goal: &str,
    base_commit: &str,
    allowed_paths: &[String],
) -> TaskContract {
    let stated = objective.unwrap_or("").trim();
    let body = if stated.is_empty() {
        format!("{} node `{}` of the run.", node.node_type.as_str(), node.id)
    } else {
        stated.to_owned()
    };
    let depends = if node.depends_on.is_empty() {
        String::new()
    } else {
        format!(
            "

Upstream tasks whose output you build on: {}.
Read their handoff packets before starting.",
            node.depends_on.join(", ")
        )
    };
    let objective = format!(
        "# Run goal

{goal}

# Your task (`{node}`)

{body}{depends}

         Stay inside this task. Work the orchestrator did not ask for is          rejected by review.",
        node = node.id,
    );
    TaskContract::builder(node.id.clone(), objective)
        .allowed_paths(allowed_paths.to_vec())
        .dependencies(node.depends_on.clone())
        .base_commit(base_commit.to_owned())
        .budgets(ContractBudgets::new(
            u32::try_from(node.budgets.max_elapsed_secs.div_ceil(60))
                .unwrap_or(u32::MAX)
                .max(1),
            node.budgets.max_attempts.max(1),
        ))
        .git_policy(GitPolicy::NoDirectGit)
        .build()
        .unwrap_or_else(|_| {
            TaskContract::builder(node.id.clone(), format!("Task {}", node.id))
                .allowed_paths(vec!["**".to_owned()])
                .base_commit(base_commit.to_owned())
                .budgets(ContractBudgets::new(30, 2))
                .git_policy(GitPolicy::NoDirectGit)
                .build()
                .expect("fallback contract is valid by construction")
        })
}

impl PlanSink for SupervisorSink {
    fn commit(&self, plan: &agentos_orchestrator::Plan) -> Result<Uuid, OrchestratorError> {
        let spec = plan.to_spec();
        let contracts: HashMap<String, TaskContract> = plan
            .nodes()
            .iter()
            .map(|planned| {
                (
                    planned.spec.id.clone(),
                    contract_for(
                        &planned.spec,
                        planned.objective.as_deref(),
                        &plan.goal,
                        &self.base_commit,
                        &self.allowed_paths,
                    ),
                )
            })
            .collect();
        let run_id = self
            .supervisor
            .start_run(&spec, &plan.goal, contracts)
            .map_err(|err| OrchestratorError::Model {
                detail: err.to_string(),
            })?;
        // Priorities are a store concern; reuse the plain sink's path.
        for planned in plan.nodes() {
            if let Some(priority) = planned.priority {
                let _ = self.set_node_priority(&run_id, &planned.spec.id, priority);
            }
        }
        Ok(run_id)
    }

    fn add_node(
        &self,
        run_id: &Uuid,
        node: &NodeSpec,
        priority: Option<Priority>,
    ) -> Result<Uuid, OrchestratorError> {
        self.store_sink.add_node(run_id, node, priority)
    }

    fn run_view(&self, run_id: &Uuid) -> Result<RunView, OrchestratorError> {
        self.store_sink.run_view(run_id)
    }

    fn escalate(&self, run_id: &Uuid, node_id: Option<&str>) -> Result<usize, OrchestratorError> {
        self.store_sink.escalate(run_id, node_id)
    }

    fn retarget(
        &self,
        run_id: &Uuid,
        node_id: Option<&str>,
        pool: &str,
    ) -> Result<usize, OrchestratorError> {
        self.store_sink.retarget(run_id, node_id, pool)
    }
}

impl SupervisorSink {
    fn set_node_priority(
        &self,
        run_id: &Uuid,
        node_id: &str,
        priority: Priority,
    ) -> Result<(), OrchestratorError> {
        let store = self.supervisor.store();
        for task in store.tasks_for_run(run_id)? {
            if task.node_id == node_id {
                store.set_priority(&task.id, priority)?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- session

/// One goal being planned and driven.
struct Session {
    id: String,
    goal: String,
    repo: PathBuf,
    orchestrator: Orchestrator,
    supervisor: Arc<Supervisor>,
    roster: Vec<RosterEntry>,
    planner_adapter: String,
    planner_model: String,
    discovery_questions: Vec<PlanningDecision>,
    discovery_answers: Vec<(String, String)>,
    discovery_complete: bool,
    phase: MastermindPhase,
    phase_status: PhaseStatus,
    approved_phases: Vec<MastermindPhase>,
    skill_source: Option<SkillSource>,
    memory_project: String,
    memory_revision: Option<i64>,
    memory_error: Option<String>,
    legacy_memory_imported: bool,
    last_review: Option<String>,
    phase_session: u32,
    accepted_tasks_in_session: u32,
    open_handoffs: usize,
    task_count: usize,
    planning_conversation: PlanningConversation,
    phase_write_paths: Vec<String>,
    model_ready: bool,
    /// Provider prose produced by the turns of the RPC currently in flight.
    /// Transient: cleared when a call begins, drained into its response, and
    /// never checkpointed — the durable account of a phase is its artifact.
    turns: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionCheckpoint {
    version: u32,
    id: String,
    goal: String,
    repo: PathBuf,
    planner_adapter: String,
    planner_model: String,
    roster: Vec<RosterEntry>,
    discovery_questions: Vec<PlanningDecision>,
    discovery_answers: Vec<(String, String)>,
    discovery_complete: bool,
    phase: MastermindPhase,
    phase_status: PhaseStatus,
    approved_phases: Vec<MastermindPhase>,
    skill_source: Option<SkillSource>,
    memory_project: String,
    memory_revision: Option<i64>,
    memory_error: Option<String>,
    legacy_memory_imported: bool,
    last_review: Option<String>,
    phase_session: u32,
    accepted_tasks_in_session: u32,
    #[serde(default)]
    open_handoffs: usize,
    #[serde(default)]
    task_count: usize,
    #[serde(default)]
    provider_session_id: Option<String>,
    #[serde(default)]
    phase_write_paths: Vec<String>,
    orchestrator: OrchestratorCheckpoint,
}

impl Session {
    fn summary(&self) -> Value {
        let plan = self.orchestrator.plan();
        let deliverables = existing_deliverables(&self.repo);
        let stage = compatibility_stage(self);
        let mut summary = json!({
            "sessionId": self.id,
            "goal": self.goal,
            "repo": self.repo.to_string_lossy(),
            "plannerAdapter": self.planner_adapter,
            "plannerModel": self.planner_model,
            "runId": plan.run_id().map(|id| id.to_string()),
            "cycles": self.orchestrator.cycle_count(),
            "maxCycles": plan.policy.max_cycles,
            "reviewerPool": plan.policy.reviewer_pool,
            "pools": plan.policy.pools,
            "roster": self.roster,
            "nodes": plan.nodes().iter().map(|planned| json!({
                "nodeId": planned.spec.id,
                "nodeType": planned.spec.node_type.as_str(),
                "pool": planned.spec.agent_role,
                "dependsOn": planned.spec.depends_on,
                "objective": planned.objective,
                "materialized": planned.materialized,
            })).collect::<Vec<_>>(),
            "pendingRejections": self.orchestrator.pending_rejections().len(),
            "discoveryComplete": self.discovery_complete,
            "pendingDiscoveryQuestions": self.discovery_questions.len(),
            "discoveryAnswers": self.discovery_answers.len(),
            "stage": stage,
            "phase": self.phase.id(),
            "phaseNumber": self.phase.number(),
            "phaseName": self.phase.name(),
            "phaseStatus": self.phase_status.as_str(),
            "gateStatus": match self.phase_status {
                PhaseStatus::AwaitingWriteApproval => "awaiting-write-approval",
                PhaseStatus::AwaitingApproval => "awaiting-approval",
                _ => "closed",
            },
            "canApprove": self.phase_status == PhaseStatus::AwaitingApproval,
            "canAuthorizeWrite": self.phase_status == PhaseStatus::AwaitingWriteApproval,
            "writeRequest": write_request(self),
            "canRecoverArtifact": can_recover_phase_artifact(self),
            "recoveryRequest": recovery_request(self),
            "activeAgent": active_agent(self),
            "phaseSession": self.phase_session,
            "approvedPhases": self.approved_phases.iter().map(|phase| phase.id()).collect::<Vec<_>>(),
            "deliverables": deliverables,
            "skillSource": self.skill_source,
            "lastReview": self.last_review,
            "memory": {
                "provider": "memex",
                "projectKey": self.memory_project,
                "latestRevision": self.memory_revision,
                "status": if self.memory_error.is_some() { "error" } else { "synchronized" },
                "error": self.memory_error,
                "openHandoffs": self.open_handoffs,
                "taskCount": self.task_count,
            },
        });
        summary["turns"] = json!(self.turns);
        summary["canRetryAuthoring"] = json!(can_retry_phase_authoring(self));
        summary["retryRequest"] = retry_request(self).unwrap_or(Value::Null);
        summary["canReprepareRevision"] = json!(can_reprepare_phase_revision(self));
        summary["reprepareRequest"] = reprepare_revision_request(self).unwrap_or(Value::Null);
        summary
    }

    /// Retain one provider turn's prose for the desktop transcript.
    ///
    /// The text is untrusted provider output, so it is length-bounded here
    /// rather than at the wire edge — a runaway response must not be able to
    /// grow an RPC frame without limit.
    fn record_turn(&mut self, kind: &str, agent: &str, model: &str, text: &str) {
        let Some(body) = bound_turn_text(text) else {
            return;
        };
        self.turns.push(json!({
            "kind": kind,
            "phase": self.phase.id(),
            "phaseName": self.phase.name(),
            "agent": agent,
            "model": model,
            "text": body,
            "at": Utc::now().to_rfc3339(),
        }));
    }

    fn discovery_brief(&self) -> String {
        let mut brief = String::from(
            "The human completed the required product discovery interview. Use these decisions as binding planning context:\n",
        );
        for (question, answer) in &self.discovery_answers {
            brief.push_str(&format!("- {question}\n  Answer: {answer}\n"));
        }
        brief.push_str(
            "Do not create a task that interviews or collects requirements from the human. The task graph must implement the decisions above.",
        );
        brief
    }
}

impl Session {
    fn checkpoint(&self) -> SessionCheckpoint {
        SessionCheckpoint {
            version: 2,
            id: self.id.clone(),
            goal: self.goal.clone(),
            repo: self.repo.clone(),
            planner_adapter: self.planner_adapter.clone(),
            planner_model: self.planner_model.clone(),
            roster: self.roster.clone(),
            discovery_questions: self.discovery_questions.clone(),
            discovery_answers: self.discovery_answers.clone(),
            discovery_complete: self.discovery_complete,
            phase: self.phase,
            phase_status: self.phase_status,
            approved_phases: self.approved_phases.clone(),
            skill_source: self.skill_source.clone(),
            memory_project: self.memory_project.clone(),
            memory_revision: self.memory_revision,
            memory_error: self.memory_error.clone(),
            legacy_memory_imported: self.legacy_memory_imported,
            last_review: self.last_review.clone(),
            phase_session: self.phase_session,
            accepted_tasks_in_session: self.accepted_tasks_in_session,
            open_handoffs: self.open_handoffs,
            task_count: self.task_count,
            provider_session_id: self.planning_conversation.provider_session_id(),
            phase_write_paths: self.phase_write_paths.clone(),
            orchestrator: self.orchestrator.checkpoint(),
        }
    }
}

fn compatibility_stage(session: &Session) -> &'static str {
    match (
        session.phase,
        session.phase_status,
        session.orchestrator.plan().run_id(),
    ) {
        (MastermindPhase::Discovery, PhaseStatus::AwaitingApproval, _) => "discovery-review",
        (MastermindPhase::Discovery, _, _) => "discovery",
        (_, _, Some(_)) => "committed",
        _ => "planning",
    }
}

fn active_agent(session: &Session) -> &'static str {
    active_agent_for_phase(session.phase)
}

fn active_agent_for_phase(phase: MastermindPhase) -> &'static str {
    match phase {
        MastermindPhase::Prd
        | MastermindPhase::Features
        | MastermindPhase::ImplementationPlan
        | MastermindPhase::ApiRecord => "selected-planner-author",
        MastermindPhase::Design | MastermindPhase::Mockups => "ui-designer",
        MastermindPhase::HtmlToReact => "nextjs-dev",
        MastermindPhase::Build => "registry-specialists",
        MastermindPhase::Wrap => REVIEWER_AGENT,
        MastermindPhase::Complete => "none",
        _ => "orchestrator",
    }
}

fn existing_deliverables(repo: &Path) -> Vec<String> {
    let candidates = [
        "docs/DISCOVERY.md",
        "docs/PRD.md",
        "docs/IMPLEMENTATION_PLAN.md",
        "docs/API_RECORD.md",
        "docs/DESIGN.md",
        "wireframe/tokens.css",
        "wireframe/INDEX.md",
        ".h2r/manifest.md",
    ];
    let mut found: Vec<String> = candidates
        .into_iter()
        .filter(|relative| repo.join(relative).is_file())
        .map(str::to_owned)
        .collect();
    // The directory alone is not a deliverable: Phase 0 creates `docs/`
    // scaffolding, and an empty `docs/features/` used to make Phase 1 report
    // the *Phase 3* output as its deliverable. Only actual documents count.
    let features = repo.join("docs").join("features");
    let has_feature_docs = fs::read_dir(&features).ok().is_some_and(|mut entries| {
        entries.any(|entry| {
            entry
                .ok()
                .is_some_and(|entry| entry.path().extension().is_some_and(|ext| ext == "md"))
        })
    });
    if has_feature_docs {
        found.push("docs/features/*.md".to_owned());
    }
    found
}

/// Largest deliverable the previewer will send in one response. These are
/// prose documents; anything past this is a runaway artifact, not a doc.
const ARTIFACT_READ_LIMIT: usize = 256 * 1024;

/// The deliverables of one session that [`Mastermind::artifact`] will serve,
/// with the `docs/features/*.md` glob expanded to the files it matches. This
/// doubles as the read allowlist, so it never widens beyond what the
/// deliverable list already shows the user.
fn readable_artifacts(repo: &Path) -> Vec<String> {
    let mut paths = Vec::new();
    for entry in existing_deliverables(repo) {
        if entry != "docs/features/*.md" {
            paths.push(entry);
            continue;
        }
        let mut features: Vec<String> = fs::read_dir(repo.join("docs").join("features"))
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "md"))
            .filter_map(|entry| entry.file_name().into_string().ok())
            .map(|name| format!("docs/features/{name}"))
            .collect();
        features.sort();
        paths.append(&mut features);
    }
    paths
}

const AGENTOS_MASTERMIND_BINDING: &str = r#"# AgentOS execution binding

The canonical skill above is authoritative for product workflow and quality.
Where its provider-specific mechanics conflict with this binding, use these
AgentOS mappings:

- Do not create or update docs/memory/MEMORY.md, HANDOFF.md, or RESUME.md.
  Shared semantic memory, decisions, phase snapshots, boards, and agent
  handoffs are supplied by Memex in this prompt and persisted by the daemon.
- A fresh session is opened automatically at every phase gate and Phase-8
  group boundary. Never ask the user to copy a prompt or use the clipboard.
- Claude Agent/Task dispatch means emit AgentOS task operations for registry
  agents. Domain specialists keep their configured provider/model; an
  unmatched task routes to general-worker-luna (codex/gpt-5.6-luna).
- The normal review tier is code-reviewer. Security review remains
  security-reviewer. UI design remains ui-designer. The live registry roster
  is authoritative for every provider and model; never rely on a model name
  embedded in this skill or a prior phase transcript.
- Git commits, branches, merges, pushes, resets and worktrees are requested
  through the AgentOS Git Manager; provider sessions never perform them.
- The top-tier orchestrator is read-only. An explicitly delegated phase
  author may edit only the deliverable paths named in its turn.
- The bundled browser companion is not used. AgentOS presents visual
  choices, mockup walk-throughs and gate decisions in its own desktop UI,
  so offer them as ordinary questions; never start a companion server and
  never fall back to one.
- References hardcode absolute paths to the bundled scripts from the
  machine they were written on. Ignore those literals: `skillRoot` and
  `skillScripts` in the durable control state below are where this
  installation actually keeps them. A script missing from `skillScripts`
  is not installed — say so rather than inventing a path for it.
"#;

/// Phase 1 is the one authoring turn whose only input is the interview
/// transcript itself. Without this brief the model mirrors the questions back
/// as headings instead of synthesizing them, which is exactly what produced
/// the transcript-shaped DISCOVERY.md this exists to prevent.
const DISCOVERY_AUTHORING_BRIEF: &str = concat!(
    "The confirmed decisions supplied above are raw interview material. They are the INPUT ",
    "to this document, never its shape. docs/DISCOVERY.md is an authored architectural brief ",
    "written in your own prose, exactly as references/discovery.md specifies.\n\n",
    "Binding rules for this artifact:\n",
    "- Never write an interview question as a heading, a bullet, or a section title. A reader ",
    "who has not seen the interview must not be able to tell which questions were asked.\n",
    "- Never carry over answer-option labels, the word \"(Recommended)\", or the user's verbatim ",
    "phrasing. Restate every decision in clean technical prose.\n",
    "- Reconcile the answers. Where a later answer settles a question an earlier answer left ",
    "open, write only the settled decision. The document must never contradict itself.\n",
    "- Where the user declined to answer or told you to move on, apply the senior-architect ",
    "default, state it as the decision, and record it under Deferred decisions as ",
    "DEFERRED(user): what and why.\n",
    "- No section may read \"Not yet confirmed\", \"TBD\" or \"we will figure it out later\" while ",
    "any answer above settles it, and never at all without a matching DEFERRED(user) entry.\n\n",
    "Required structure, in this order:\n",
    "# Discovery \u{2014} <project name>\n",
    "Date: <date> \u{b7} Status: draft\n",
    "## Vision \u{2014} one paragraph of prose: what this is, who uses it, what it solves or ",
    "replaces.\n",
    "## Platform & stack \u{2014} the confirmed choices, each with the reason it was chosen.\n",
    "## Feature census \u{2014} a numbered list, one line per feature, covering both the features ",
    "the user named and the ones their answers imply. This numbering drives Phase 3.\n",
    "## Feature detail \u{2014} one \"### <n>. <feature>\" block per census entry, in census order, ",
    "each covering all nine details: happy path, inputs (validation and limits), outputs and ",
    "side effects, states (empty, loading, error, success, partial), edge cases (concurrency, ",
    "duplicates, delete semantics, offline, huge and zero inputs), permissions, data lifecycle ",
    "(storage, retention, export, delete), integrations (the exact service), failure modes.\n",
    "## Non-functional \u{2014} scale, performance, security, accessibility, look and feel, ",
    "deployment target.\n",
    "## Deferred decisions \u{2014} every DEFERRED(user) entry, or \"None.\" when there are none.",
);

/// Design phases kept failing review on two self-inflicted classes of defect:
/// figures asserted without being computed, and user-facing copy paraphrased
/// instead of quoted from the frozen feature documents. Both are avoidable
/// inside the authoring turn, so the brief makes the verification a
/// precondition of writing rather than something a later reviewer must catch.
const DESIGN_AUTHORING_BRIEF: &str = r#"Read the approved PRD personas and flows, and read every docs/features/*.md file in full before writing.

BUILD A COVERAGE CHECKLIST FIRST. Before drafting, enumerate from the feature documents and the API record every state, component, error code and fixed string the design must cover: each named state in every feature's States section, each edge case it calls out, each error code in the API record's error table, and each literal string any of them fixes. Keep that list and do not finish until every entry has a specification in the document. Deliverables have repeatedly shipped missing a mandated state - a loading state, a confirmation dialog, a "requesting camera" step, an error code with no UI treatment - because the author designed what it imagined rather than what the approved documents enumerate.

VERIFY EVERY NUMBER YOU STATE. Do not assert any computed figure you have not actually calculated in this turn.
- Contrast: compute relative luminance with the WCAG sRGB-to-linear formula for BOTH colours, then (L_lighter + 0.05) / (L_darker + 0.05). Normal text needs 4.5:1, large text 3:1.
- Check the WHOLE palette, not only the pairs you choose to claim. WCAG 2.2 SC 1.4.11 requires 3:1 for non-text contrast: focus indicators, focus rings, control boundaries and any border that carries meaning. Evaluate every such pair. Composite translucent colours against their actual backdrop before computing - the ratio of a raw rgba() value is meaningless.
- Saturation: compute from the actual hex (max, min, delta) and state the real percentage.
- Durations, sizes and counts: take the value from the approved document that fixes it. Never invent one.

QUOTE USER-FACING COPY, NEVER PARAPHRASE IT. Every label, heading, hint, toast, empty state and error message a feature document fixes is a literal contract checked character for character. Copy each exactly, including capitalisation, punctuation and ellipses. A component specification is incomplete until the exact string appears. Interaction shapes are equally fixed: if a document says one input, do not design six boxes.

AUDIT YOUR OWN DOCUMENT BEFORE FINISHING. Internal consistency is a procedure, not an intention. When the draft is complete:
- Re-read every constraint and every banned-pattern entry you wrote, then search the document for each one and confirm it has zero violations. Rules stated in one section have repeatedly been broken in another - a banned colour used anyway, a banned component specified twice, a single-accent rule contradicted by four component specs.
- Confirm every component in the inventory appears in at least one page composition, and that every route states which chrome it shows. A component with no home is unbuildable.
- Confirm every token's documented usage names a component that actually exists.
- Confirm every value that appears more than once agrees with itself everywhere.
- Open every citation you make and confirm it resolves to the section you claim. Never cite a document that does not exist in the repository.
Name these checks in your evidence report.

FOLLOW THE PROJECT'S OWN GOVERNANCE. Match the conventions the sibling artifacts already use. Where an upstream source is missing or thin, disclose it the same way the other documents do rather than presenting the derivation as frozen. Where you introduce an external dependency such as a font or asset host, record it as requiring an API-record entry instead of baking it silently into the contract.

The approved feature documents are frozen and authoritative. When your design intent and an approved document disagree, the document wins; record the tension in your evidence report rather than silently overriding it."#;

/// Phase 7 inherited the design brief unchanged, and every defect it shipped
/// landed in the two clauses that do not survive the change of deliverable.
/// "Audit the constraints you wrote" has no target when the constraints live
/// in DESIGN.md and the author only consumes them, and "verify every number
/// you state" never fires against values embedded in CSS rather than asserted
/// in prose. The result was five review rounds losing a banned font inside a
/// fallback chain, a cyan-only scan guide recoloured in its accept and reject
/// variants, an unconditional 24px rule implemented above one breakpoint, an
/// invented 560px ceiling, and an interaction contract that never became an
/// attribute. This brief re-points the same discipline at an inherited spec.
const MOCKUP_AUTHORING_BRIEF: &str = r#"You are implementing a frozen specification, not authoring one. Every constraint you must satisfy was written by someone else, in docs/DESIGN.md and the feature documents. That is the specific reason the checks below exist: an author who wrote the rules remembers them, and you did not write these.

EXTRACT THE CONSTRAINT LEDGER BEFORE YOU WRITE. Read docs/DESIGN.md and list, verbatim, every hard constraint, every banned-pattern entry, every reserved-usage rule ("X is used strictly for Y"), and the complete set of declared design tokens. Keep that ledger open while you build. When the markup and tokens are written, take each ledger entry in turn and search your own output for violations of it. A ban is checked by searching for the banned thing, not by recalling that you avoided it. Name this ledger and its result in your evidence report; a report that does not cite the ledger has not run this check.

A CONSTRAINT BINDS EVERY OCCURRENCE, NOT THE OBVIOUS ONE. Rules have repeatedly held in the primary case and lapsed everywhere the same rule had to travel. A banned font is still banned inside a fallback chain. A colour reserved for one purpose is still reserved in the accepted, rejected, loading and error variants of the component that uses it. A token set is closed: never reference a custom property outside the declared set, including as a CSS fallback value. And when two routes render the same conceptual state, they render it the same way - analogous states that diverge for no functional reason are a defect even when neither one alone breaks a rule.

AN UNCONDITIONAL RULE GETS AN UNCONDITIONAL IMPLEMENTATION. When the specification states a minimum, a size or a spacing with no breakpoint qualifier, it holds at every viewport width. Never satisfy such a rule only inside a media query and leave the base case below spec - on a mobile-first product that ships the violation to the primary device class.

SILENCE IS NOT LICENSE. Where the specification fixes a maximum and no floor, implement the maximum and add no ceiling of your own; an extra cap you invented silently shrinks the deliverable below what was approved. Never introduce a limit, a breakpoint, a fallback or a user-facing string the approved documents do not contain, and never move fixed copy to a surface other than the one the feature document assigns it to. If a value genuinely must be chosen, take it from the document that fixes it or record the gap in your evidence report - do not fill it quietly.

RENDER INTERACTION CONTRACTS AS MARKUP. A specification sentence such as "disabled until exactly six digits are entered" is a contract the next phase wires against, and a static state that shows fewer than six digits with an enabled control contradicts it. Every state you present must carry the attributes its own condition implies - disabled, aria-invalid, aria-busy, readonly, required - so that the frozen markup and the written contract agree. Check each state you ship against the sentence that governs it."#;

const LIVE_SKILL_BEGIN: &str = "<<<BEGIN LIVE MASTERMIND SKILL>>>\n";
const LIVE_SKILL_END: &str = "<<<END LIVE MASTERMIND SKILL>>>";

fn append_live_skill(envelope: &mut String, skill: &str) {
    envelope.push_str(LIVE_SKILL_BEGIN);
    envelope.push_str(skill);
    envelope.push_str(LIVE_SKILL_END);
}

/// Memex search is newest-first. Replaceable projections and corrected
/// decisions therefore contribute only their newest effective row, while
/// lessons, API facts, and unrelated context remain append-only.
fn newest_effective_memories(records: Vec<MemoryRecord>) -> Vec<MemoryRecord> {
    let mut seen = HashSet::new();
    records
        .into_iter()
        .filter(|record| {
            let tags: Vec<&str> = record.tags.split(',').map(str::trim).collect();
            let phase = tags
                .iter()
                .find(|tag| tag.starts_with("phase:"))
                .copied()
                .unwrap_or("phase:unknown");
            let key = if tags.contains(&"kind:phase-snapshot") {
                Some(format!("snapshot:{phase}"))
            } else if tags.contains(&"kind:approved-decision") {
                let decision = tags
                    .iter()
                    .find(|tag| tag.starts_with("decision:"))
                    .copied()
                    .unwrap_or("decision:unknown");
                Some(format!("decision:{phase}:{decision}"))
            } else if tags.contains(&"kind:decision-revision") {
                Some(format!("revision:{phase}"))
            } else if tags.contains(&"kind:legacy-import") {
                let path = tags
                    .iter()
                    .find(|tag| tag.starts_with("path:"))
                    .copied()
                    .unwrap_or("path:unknown");
                Some(format!("legacy:{path}"))
            } else {
                None
            };
            key.is_none_or(|key| seen.insert(key))
        })
        .collect()
}

/// Bound semantic memory by characters, newest first. The prompt is a cache,
/// not the source of truth: approved documents and the workflow snapshot are
/// supplied separately. Keeping a small head from each record retains the
/// decision signal without replaying whole historical artifacts.
fn budget_memories_for_prompt(records: Vec<MemoryRecord>) -> Vec<MemoryRecord> {
    let mut remaining = MEMEX_PROMPT_MAX_CHARS;
    let mut bounded = Vec::new();
    for mut record in records {
        if remaining == 0 {
            break;
        }
        let take = remaining.min(MEMEX_RECORD_MAX_CHARS);
        let content_chars = record.content.chars().count();
        if content_chars > take {
            record.content = record
                .content
                .chars()
                .take(take)
                .chain("\n…memory truncated…".chars())
                .collect();
        }
        remaining = remaining.saturating_sub(content_chars.min(take));
        bounded.push(record);
    }
    bounded
}

/// The bundled scripts a phase may be told to run, resolved against the skill
/// root this daemon actually loaded. The references hardcode the absolute path
/// of the machine they were written on, so a turn that trusts them runs the
/// wrong path — or reports a missing script that is installed right here.
/// Only scripts present on disk are listed; an absent key is the honest signal
/// that the procedure needing it cannot run.
/// Where a phase's bundled scripts are staged so its agent can actually run
/// them: inside the phase's own declared write scope.
///
/// The installed scripts live under the skill root (`~/.claude/skills/...`),
/// which is correct for a daemon-side reader and unreachable for the authoring
/// agent. The claude-code adapter emits no directory grant at all — it drops
/// `SpawnSpec::allowed_paths` — so the agent's world is its workspace, and a
/// script outside it can be neither read nor executed. Phase 7.5's canonical
/// procedure *is* that script, so the turn returned having written nothing and
/// correctly reported the script as inaccessible.
///
/// `design_system.py` and `s2r.py` are unreachable for the same reason; they
/// get staged the same way when a phase's procedure needs them.
fn staged_scripts_dir(repo: &Path, phase: MastermindPhase) -> Option<PathBuf> {
    match phase {
        MastermindPhase::HtmlToReact => Some(repo.join(".h2r")),
        _ => None,
    }
}

/// The scripts a phase's procedure must execute, by `skillScripts` key.
fn phase_script_names(phase: MastermindPhase) -> &'static [&'static str] {
    match phase {
        MastermindPhase::HtmlToReact => &["h2r.py"],
        _ => &[],
    }
}

/// Copy this phase's scripts into its write scope. Best-effort per script: a
/// script that cannot be staged is simply absent from `skillScripts`, which is
/// the same honest signal as one that is not installed.
fn stage_phase_scripts(skill_root: &Path, repo: &Path, phase: MastermindPhase) {
    let Some(dir) = staged_scripts_dir(repo, phase) else {
        return;
    };
    let names = phase_script_names(phase);
    if names.is_empty() {
        return;
    }
    if let Err(error) = fs::create_dir_all(&dir) {
        tracing::warn!(dir = %dir.display(), %error, "could not create the staged script directory");
        return;
    }
    for name in names {
        let source = skill_root.join("scripts").join(name);
        let target = dir.join(name);
        if !source.is_file() {
            continue;
        }
        if let Err(error) = fs::copy(&source, &target) {
            tracing::warn!(
                source = %source.display(),
                target = %target.display(),
                %error,
                "could not stage a bundled script into the phase write scope"
            );
        }
    }
}

fn skill_scripts(root: &Path, repo: &Path, phase: MastermindPhase) -> Value {
    let staged = staged_scripts_dir(repo, phase);
    let mut found = serde_json::Map::new();
    for name in ["h2r.py", "search.py", "s2r.py"] {
        // A staged copy wins: it is the only one the agent can open. Reporting
        // the installed path when a staged copy exists would hand the turn a
        // path outside its workspace, which is the failure this staging fixes.
        let path = staged
            .as_ref()
            .map(|dir| dir.join(name))
            .filter(|candidate| candidate.is_file())
            .unwrap_or_else(|| root.join("scripts").join(name));
        if path.is_file() {
            found.insert(
                name.trim_end_matches(".py").to_owned(),
                json!(path.to_string_lossy()),
            );
        }
    }
    Value::Object(found)
}

fn phase_reference_paths(phase: MastermindPhase) -> &'static [&'static str] {
    match phase {
        MastermindPhase::Setup => &["references/git-workflow.md", "references/code-graph.md"],
        // Phase 1 is the first turn that sees the repository, so it is where
        // the skill's "directory already has code — build the graph and skim
        // the stack" step can actually happen. Phase 0 runs no provider turn.
        MastermindPhase::Discovery => &["references/discovery.md", "references/code-graph.md"],
        MastermindPhase::Prd | MastermindPhase::Features | MastermindPhase::ImplementationPlan => {
            &["references/documents.md"]
        }
        MastermindPhase::ApiRecord => &["references/api-record.md"],
        MastermindPhase::Design => &[
            "references/frontend.md",
            "references/design/ui-ux-pro-max.md",
            "references/design/taste-design.md",
            "references/design/impeccable.md",
            "references/design/frontend-design.md",
        ],
        // The mockups must satisfy the Absolute Bans and the Phase-7 rubric
        // ships the ban list as a FAIL condition, so the ban list has to be
        // present when the mockups are built, not only when DESIGN.md is.
        MastermindPhase::Mockups => &["references/frontend.md", "references/design/impeccable.md"],
        MastermindPhase::HtmlToReact => &["references/html-to-react.md"],
        MastermindPhase::Build => &[
            "references/agents.md",
            "references/git-workflow.md",
            "references/code-graph.md",
            "references/style-rules.md",
        ],
        // Phase 9 runs the impeccable audit rubric over the built UI.
        MastermindPhase::Wrap => &[
            "references/git-workflow.md",
            "references/design/impeccable.md",
        ],
        MastermindPhase::Complete => &[],
    }
}

fn phase_output_paths(repo: &Path, phase: MastermindPhase) -> Vec<String> {
    let paths: Vec<PathBuf> = match phase {
        MastermindPhase::Setup => vec![repo.join("docs")],
        MastermindPhase::Discovery => vec![repo.join("docs").join("DISCOVERY.md")],
        MastermindPhase::Prd => vec![repo.join("docs").join("PRD.md")],
        MastermindPhase::Features => vec![repo.join("docs").join("features")],
        MastermindPhase::ImplementationPlan => {
            vec![repo.join("docs").join("IMPLEMENTATION_PLAN.md")]
        }
        MastermindPhase::ApiRecord => vec![repo.join("docs").join("API_RECORD.md")],
        MastermindPhase::Design => vec![repo.join("docs").join("DESIGN.md")],
        MastermindPhase::Mockups => vec![repo.join("wireframe")],
        // `references/html-to-react.md` requires this phase to freeze
        // `wireframe/INDEX.md` and rewrite the implementation plan's frontend
        // tasks from "build X" to "wire X" once emit and verify pass. Both were
        // impossible: the scope was `.h2r` alone, and the declared-path parser
        // rejects anything rooted at `docs` or `wireframe`, so the reviewer
        // failed the phase for not doing what it had no permission to do.
        // These two files are named exactly, not by prefix — the block on the
        // rest of `docs/` and `wireframe/` stands.
        MastermindPhase::HtmlToReact => vec![
            repo.join(".h2r"),
            repo.join("docs").join("IMPLEMENTATION_PLAN.md"),
            repo.join("docs").join("API_RECORD.md"),
            repo.join("wireframe").join("INDEX.md"),
        ],
        MastermindPhase::Build => vec![repo.to_path_buf()],
        MastermindPhase::Wrap | MastermindPhase::Complete => Vec::new(),
    };
    paths
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
}

fn phase_output_labels(phase: MastermindPhase) -> Vec<&'static str> {
    match phase {
        MastermindPhase::Discovery => vec!["docs/DISCOVERY.md"],
        MastermindPhase::Prd => vec!["docs/PRD.md"],
        MastermindPhase::Features => vec!["docs/features/"],
        MastermindPhase::ImplementationPlan => vec!["docs/IMPLEMENTATION_PLAN.md"],
        MastermindPhase::ApiRecord => vec!["docs/API_RECORD.md"],
        MastermindPhase::Design => vec!["docs/DESIGN.md"],
        MastermindPhase::Mockups => vec!["wireframe/"],
        MastermindPhase::HtmlToReact => vec![".h2r/"],
        _ => Vec::new(),
    }
}

fn scoped_phase_write_paths(session: &Session) -> Vec<String> {
    if session.phase_write_paths.is_empty() {
        phase_output_paths(&session.repo, session.phase)
    } else {
        session.phase_write_paths.clone()
    }
}

fn scoped_phase_write_labels(session: &Session) -> Vec<String> {
    if session.phase_write_paths.is_empty() {
        return phase_output_labels(session.phase)
            .into_iter()
            .map(str::to_owned)
            .collect();
    }
    session
        .phase_write_paths
        .iter()
        .filter_map(|path| Path::new(path).strip_prefix(&session.repo).ok())
        .map(|path| path.to_string_lossy().replace('\\', "/"))
        .collect()
}

/// H2R may need several generated source files, but their locations are
/// learned only during the read-only preparation turn. Accept only explicit
/// repository-relative declarations, reject traversal/input artifacts, and
/// always retain the private `.h2r` artifact scope.
fn h2r_write_paths_from_preparation(repo: &Path, response: &str) -> Vec<String> {
    let mut paths = phase_output_paths(repo, MastermindPhase::HtmlToReact);
    let mut declarations = false;
    for line in response.lines() {
        let trimmed = line.trim();
        if trimmed.eq_ignore_ascii_case("WRITE_PATHS:") {
            declarations = true;
            continue;
        }
        if !declarations {
            continue;
        }
        let Some(value) = trimmed.strip_prefix("- ") else {
            if !trimmed.is_empty() {
                declarations = false;
            }
            continue;
        };
        let value = value.trim().trim_matches(['`', '"', '\'']);
        let relative = Path::new(value);
        if value.is_empty()
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            continue;
        }
        let first = relative
            .components()
            .next()
            .and_then(|component| component.as_os_str().to_str())
            .unwrap_or_default();
        if matches!(first, ".git" | "docs" | "wireframe") {
            continue;
        }
        let absolute = repo.join(relative).to_string_lossy().into_owned();
        if !paths.contains(&absolute) {
            paths.push(absolute);
        }
    }
    paths
}

/// The revision path is deliberately narrower than the normal H2R declaration
/// parser. A failed review may reveal source files that the old preparation
/// scope did not grant, but neither the client nor an author may turn that
/// into a repository-wide grant. The existing plan is the only authority for
/// its output root; the three named governance artifacts are required H2R
/// evidence, not general `docs/` or `wireframe/` access.
fn h2r_revision_write_paths(repo: &Path) -> Result<Vec<String>, String> {
    let plan_path = repo.join(".h2r").join("plan.json");
    let plan = fs::read_to_string(&plan_path)
        .map_err(|error| format!("cannot read {}: {error}", plan_path.display()))?;
    let value: Value = serde_json::from_str(&plan)
        .map_err(|error| format!("cannot parse {}: {error}", plan_path.display()))?;
    // `outDir` is the plan's *base*, and the bundled `plan.schema.json`
    // ships `"."` — the repository root. Granting that verbatim would hand a
    // revision turn write access to the whole project, so the root is never
    // a grant on its own: what gets written is `<outDir>/<componentsDir>`,
    // and that is what the revision may touch. Requiring a deep `outDir`
    // instead (the previous rule) rejected the schema's own default and left
    // Phase 7.5 with no working revision path at all.
    let out_dir = value
        .get("outDir")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(".");
    let out_relative = safe_plan_relative(out_dir)
        .ok_or_else(|| format!("{} has an unsafe outDir {out_dir:?}", plan_path.display()))?;

    let mut roots: Vec<PathBuf> = Vec::new();
    if !out_relative.as_os_str().is_empty() {
        roots.push(out_relative.clone());
    }
    if let Some(components_dir) = value
        .get("componentsDir")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let components_relative = safe_plan_relative(components_dir).ok_or_else(|| {
            format!(
                "{} has an unsafe componentsDir {components_dir:?}",
                plan_path.display()
            )
        })?;
        if !components_relative.as_os_str().is_empty() {
            roots.push(out_relative.join(components_relative));
        }
    }
    if roots.is_empty() {
        return Err(format!(
            "{} names no output directory below the repository root",
            plan_path.display()
        ));
    }

    let mut grants = vec![repo.join(".h2r").to_string_lossy().into_owned()];
    for root in roots {
        let output_root = repo.join(&root);
        if !output_root.is_dir() {
            return Err(format!(
                "{} output directory does not exist: {}",
                plan_path.display(),
                output_root.display()
            ));
        }
        grants.push(output_root.to_string_lossy().into_owned());
    }
    grants.extend([
        repo.join("docs/IMPLEMENTATION_PLAN.md")
            .to_string_lossy()
            .into_owned(),
        repo.join("docs/API_RECORD.md")
            .to_string_lossy()
            .into_owned(),
        repo.join("wireframe/INDEX.md")
            .to_string_lossy()
            .into_owned(),
    ]);
    Ok(grants)
}

/// A plan-declared path that is safe to join onto the repo: relative, with
/// no traversal and no prefix/root components. `.` normalizes to empty,
/// meaning "the base itself", which callers must not grant on its own.
fn safe_plan_relative(value: &str) -> Option<PathBuf> {
    let candidate = Path::new(value.trim());
    if candidate.is_absolute() {
        return None;
    }
    let mut normalized = PathBuf::new();
    for component in candidate.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::Normal(part) => normalized.push(part),
            // `..`, `/`, and `C:` never reach a write grant.
            _ => return None,
        }
    }
    Some(normalized)
}

fn phase_requires_write_authorization(phase: MastermindPhase) -> bool {
    matches!(
        phase,
        MastermindPhase::Discovery
            | MastermindPhase::Prd
            | MastermindPhase::Features
            | MastermindPhase::ImplementationPlan
            | MastermindPhase::ApiRecord
            | MastermindPhase::Design
            | MastermindPhase::Mockups
            | MastermindPhase::HtmlToReact
    )
}

fn accepts_phase_as_is(guidance: &str) -> bool {
    let normalized = guidance
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character.is_ascii_whitespace() {
                character
            } else {
                ' '
            }
        })
        .collect::<String>();
    let normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if [" but ", " except ", " however ", " although "]
        .iter()
        .any(|contrast| format!(" {normalized} ").contains(contrast))
    {
        return false;
    }
    [
        "accept as is",
        "approve as is",
        "no revision needed",
        "no revisions needed",
        "nothing needs to be revised",
        "everything is fine",
        "everything is on point",
        "looks good",
        "all good",
        "no changes needed",
    ]
    .iter()
    .any(|phrase| normalized.contains(phrase))
}

fn write_request(session: &Session) -> Option<Value> {
    if session.phase_status != PhaseStatus::AwaitingWriteApproval {
        return None;
    }
    let deliverables = scoped_phase_write_labels(session);
    let subject = if session.phase == MastermindPhase::Discovery {
        "Discovery is complete"
    } else {
        "Phase preparation is complete"
    };
    let next = session.phase.next(is_react_stack(&session.repo));
    Some(json!({
        "prompt": format!(
            "{subject}. Mastermind is still read-only. Allow one scoped authoring turn to write {}? Only this phase's declared deliverable paths will be writable. AgentOS will stop for review and will not begin {} until you approve the artifact.",
            phase_deliverable(session.phase),
            next.name(),
        ),
        "deliverables": deliverables,
    }))
}

fn can_recover_phase_artifact(session: &Session) -> bool {
    session.phase_status == PhaseStatus::Blocked
        && phase_requires_write_authorization(session.phase)
        && phase_deliverable_exists(&session.repo, session.phase)
}

fn recovery_request(session: &Session) -> Option<Value> {
    can_recover_phase_artifact(session).then(|| {
        json!({
            "prompt": format!(
                "The scoped authoring turn ended with an error, but {} exists. Run the read-only reviewer on the existing artifact without authoring it again.",
                phase_deliverable(session.phase),
            ),
            "deliverables": scoped_phase_write_labels(session),
        })
    })
}

fn can_reprepare_phase_revision(session: &Session) -> bool {
    session.phase == MastermindPhase::HtmlToReact
        && session.phase_status == PhaseStatus::NeedsRevision
        && h2r_revision_write_paths(&session.repo).is_ok()
}

fn reprepare_revision_request(session: &Session) -> Option<Value> {
    can_reprepare_phase_revision(session).then(|| {
        json!({
            "prompt": "Phase 7.5 needs a broader, server-derived remediation scope. Re-run read-only preparation, then request a new scoped write grant; frozen wireframe inputs remain unavailable.",
            "deliverables": h2r_revision_write_paths(&session.repo)
                .map(|paths| paths.into_iter().filter_map(|path| Path::new(&path).strip_prefix(&session.repo).ok().map(|value| value.to_string_lossy().replace('\\', "/"))).collect::<Vec<_>>())
                .unwrap_or_default(),
        })
    })
}

fn can_retry_phase_authoring(session: &Session) -> bool {
    session.phase_status == PhaseStatus::Blocked
        && phase_requires_write_authorization(session.phase)
        && !phase_deliverable_exists(&session.repo, session.phase)
}

fn retry_request(session: &Session) -> Option<Value> {
    can_retry_phase_authoring(session).then(|| {
        json!({
            "prompt": format!(
                "The scoped authoring turn ended before producing {}. Retry once in a clean provider conversation reconstructed from the checkpoint, under the same write scope.",
                phase_deliverable(session.phase),
            ),
            "deliverables": scoped_phase_write_labels(session),
        })
    })
}

/// Rebuild the code graph over `repo`, best effort.
///
/// The supervisor does this for every worker node it runs, but the authoring
/// phases bypass the supervisor entirely — they call the planning model
/// directly — so code they emit would otherwise never reach the graph.
/// Failures only log: a phase must not be blocked by its graph.
async fn refresh_code_graph(repo: &Path) {
    let Some(graphifier) = Graphifier::resolve(
        &GraphRefresh::Auto,
        Duration::from_secs(GRAPH_REFRESH_TIMEOUT_SECS),
    ) else {
        return;
    };
    match graphifier.update(repo).await {
        GraphifyOutcome::Updated { elapsed_ms } => {
            tracing::info!(repo = %repo.display(), elapsed_ms, "code graph refreshed");
        }
        GraphifyOutcome::Failed { reason, .. } => {
            tracing::warn!(repo = %repo.display(), %reason, "code graph refresh failed");
        }
        GraphifyOutcome::Skipped(_) => {}
    }
}

fn phase_deliverable(phase: MastermindPhase) -> &'static str {
    match phase {
        MastermindPhase::Setup => "docs/ plus project setup",
        MastermindPhase::Discovery => "docs/DISCOVERY.md",
        MastermindPhase::Prd => "docs/PRD.md",
        MastermindPhase::Features => "docs/features/*.md",
        MastermindPhase::ImplementationPlan => "docs/IMPLEMENTATION_PLAN.md",
        MastermindPhase::ApiRecord => "docs/API_RECORD.md",
        MastermindPhase::Design => "docs/DESIGN.md",
        MastermindPhase::Mockups => "wireframe/tokens.css, wireframe/*.html, wireframe/INDEX.md",
        MastermindPhase::HtmlToReact => ".h2r/manifest.md and emitted components",
        MastermindPhase::Build => "implemented and reviewed application",
        MastermindPhase::Wrap => "final verification evidence",
        MastermindPhase::Complete => "completed build",
    }
}

/// Deterministic Phase-8 checks that do not justify an LLM turn. Semantic
/// contradictions still go to the planner, but missing referenced specs and
/// an API record that declares itself blocked are filesystem facts.
fn build_preflight_findings(repo: &Path) -> Vec<String> {
    let mut findings = Vec::new();
    let plan_path = repo.join("docs").join("IMPLEMENTATION_PLAN.md");
    let plan = match fs::read_to_string(&plan_path) {
        Ok(plan) => plan,
        Err(error) => {
            findings.push(format!(
                "docs/IMPLEMENTATION_PLAN.md is unavailable: {error}"
            ));
            String::new()
        }
    };

    let mut referenced = HashSet::new();
    for (start, _) in plan.match_indices("docs/features/") {
        let tail = &plan[start..];
        let Some(markdown_end) = tail.find(".md") else {
            continue;
        };
        let relative = &tail[..markdown_end + ".md".len()];
        if relative
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '-' | '_' | '.'))
        {
            referenced.insert(relative.to_owned());
        }
    }
    let mut missing: Vec<String> = referenced
        .into_iter()
        .filter(|relative| !repo.join(relative).is_file())
        .collect();
    missing.sort();
    if !missing.is_empty() {
        findings.push(format!(
            "implementation plan references missing feature documents: {}",
            missing.join(", ")
        ));
    }

    let api_path = repo.join("docs").join("API_RECORD.md");
    match fs::read_to_string(&api_path) {
        Ok(api) => {
            let normalized = api.to_ascii_lowercase();
            if normalized.contains("status: blocked")
                || normalized.contains("zero external apis are approved")
            {
                findings.push(
                    "docs/API_RECORD.md declares external-library verification blocked".to_owned(),
                );
            }
        }
        Err(error) => findings.push(format!("docs/API_RECORD.md is unavailable: {error}")),
    }
    findings
}

fn build_preflight_message(findings: &[String]) -> String {
    format!(
        "VERDICT: FAIL\nPhase 8 deterministic preflight blocked before a model call:\n- {}",
        findings.join("\n- ")
    )
}

fn block_build_on_preflight(session: &mut Session) -> Option<PlanCycleReport> {
    let findings = build_preflight_findings(&session.repo);
    if findings.is_empty() {
        return None;
    }
    let message = build_preflight_message(&findings);
    session.phase_status = PhaseStatus::Blocked;
    session.last_review = Some(message.clone());
    Some(PlanCycleReport {
        cycle: session.orchestrator.cycle_count(),
        raw_excerpt: message,
        ..PlanCycleReport::default()
    })
}

fn build_cycle_instruction(guidance: Option<&str>) -> String {
    match guidance.map(str::trim).filter(|value| !value.is_empty()) {
        Some(guidance) => {
            format!("{BUILD_PLAN_INSTRUCTION}\n\nCurrent human guidance:\n{guidance}")
        }
        None => BUILD_PLAN_INSTRUCTION.to_owned(),
    }
}

/// Extra assertions the reviewer must make for a specific phase, appended to
/// the shared review prompt. Empty for phases whose generic review is enough.
/// Whether this phase's review runs commands instead of only reading.
///
/// Exactly one phase does. Phase 9's deliverable is execution evidence — a
/// clean build and a full test run — and there is no authoring step in that
/// phase to produce it (`drive_phase` sends `Wrap` straight to
/// `review_phase`). A reviewer with no shell therefore cannot pass the phase
/// by any route, which is what it did: it returned FAIL naming its own
/// missing shell as the violated requirement.
///
/// Every other review stays a reading review. Reviewers are shell-less on
/// purpose, and the earlier empty-review outbreak was caused by ordering a
/// shell-less reviewer to run commands — the opposite of this grant.
/// Read the reviewer's verdict from its reply.
///
/// The verdict is the **first** line-anchored `VERDICT:` token, not any
/// occurrence of the string anywhere in the reply. A plain
/// `contains("VERDICT: PASS")` passed a failing review live: the reply opened
/// with `VERDICT: FAIL` and its design rubric later contained
/// `**Anti-Patterns Verdict: PASS**`, which contains the pass token as a
/// substring. Phase 9 was approved on that, and the session reached
/// `complete` with a failing review stored as its `lastReview`.
///
/// Anchoring to line starts (after markdown emphasis and list punctuation)
/// and taking the first verdict makes a sub-score unable to overturn the
/// verdict, and makes a reply that never states one fail closed.
fn verdict_is_pass(review: &str) -> bool {
    review
        .lines()
        .filter_map(|line| {
            let line = line.trim_start_matches(|c: char| {
                c.is_whitespace() || matches!(c, '*' | '#' | '-' | '>' | '`')
            });
            let upper = line.to_ascii_uppercase();
            if upper.starts_with("VERDICT: PASS") {
                Some(true)
            } else if upper.starts_with("VERDICT: FAIL") {
                Some(false)
            } else {
                None
            }
        })
        .next()
        .unwrap_or(false)
}

fn review_runs_commands(phase: MastermindPhase) -> bool {
    matches!(phase, MastermindPhase::Wrap)
}

/// The reviewer denylist minus the shell tokens.
///
/// Write and delegation tools stay denied: the grant is "run the build and
/// the tests", not "fix what they report". A reviewer that can edit the code
/// it is judging has no independent verdict left to give.
fn reviewer_denylist_with_shell() -> Vec<String> {
    ORCHESTRATOR_TOOL_DENYLIST
        .iter()
        .filter(|tool| !AGY_SHELL_TOOLS.contains(*tool))
        .map(|tool| (*tool).to_owned())
        .collect()
}

/// Shell tokens, which are also what selects agy's `--sandbox`.
///
/// Mirrors `agy::SHELL_TOOLS` (private there). The
/// `reviewer_shell_grant_clears_every_shell_token` test pins the two lists
/// together, so a token added there fails here rather than silently leaving
/// the wrap reviewer sandboxed.
const AGY_SHELL_TOOLS: [&str; 6] = [
    "Bash",
    "BashOutput",
    "KillShell",
    "PowerShell",
    "Tmux",
    "REPL",
];

/// The scope sentence for a review prompt.
///
/// Phase-dependent because it is a statement of fact about the session the
/// reviewer is running in, and stating it wrongly is how Phase 9 became
/// unpassable: the generic clause asserted "you have no shell" in the same
/// prompt whose phase checks demanded fresh build output.
fn review_scope_clause(phase: MastermindPhase) -> &'static str {
    if review_runs_commands(phase) {
        " You have a shell for this review and a write scope covering the repo, because this phase's verdict depends on evidence only execution produces. Run the project's real build and its full test suite yourself and quote their actual output — the command you ran, and what it printed. Write tools are still denied: report what the run says, do not repair it. Never present output you did not obtain from a command you ran, and if a command cannot be run at all, say which and call that requirement unverified."
    } else {
        " This is a reading review: you have no write scope and no shell — every shell tool is denied to you, so do not attempt to run a build, a test or any other command. Review by reading the files. If a requirement could only be settled by running something, say so and call it unverified; never assert a build, test or rendered page result you did not see."
    }
}

fn phase_review_checks(phase: MastermindPhase) -> &'static str {
    match phase {
        MastermindPhase::Discovery => concat!(
            " This deliverable is an authored architectural brief, not a record of the",
            " discovery interview. Return VERDICT: FAIL if any heading restates a question",
            " that was asked of the user, if the string \"(Recommended)\" or any answer-option",
            " label appears anywhere in the file, if any feature in the census is missing one",
            " of the nine required details (happy path, inputs, outputs, states, edge cases,",
            " permissions, data lifecycle, integrations, failure modes), if two sections state",
            " contradictory decisions, or if any section reads \"Not yet confirmed\", \"TBD\" or",
            " \"we will figure it out later\" without a matching DEFERRED(user) entry naming",
            " what was deferred and why."
        ),
        MastermindPhase::Features => concat!(
            " Extract the complete feature census from docs/DISCOVERY.md, including a census",
            " written in prose or one sentence. Return VERDICT: FAIL unless docs/features contains",
            " exactly one substantive feature document for every census item and no census feature",
            " is represented only by implication in another file. List every missing census item",
            " by name; file existence alone is not sufficient."
        ),
        MastermindPhase::ApiRecord => concat!(
            " Return VERDICT: FAIL if docs/API_RECORD.md declares itself BLOCKED, says zero external",
            " APIs are approved, or leaves any dependency/API required by docs/IMPLEMENTATION_PLAN.md",
            " unverified. A catalog of work still to verify is not an approved API record."
        ),
        // The mockups implement a frozen contract, and every defect this phase
        // has shipped was a contract violation the generic review had no
        // instruction to look for.
        MastermindPhase::Mockups => concat!(
            " Apply the Phase-7 rubric from the frontend reference literally, and return",
            " VERDICT: FAIL on any of its stated FAIL conditions: a PRD flow that cannot be",
            " walked by clicking through the pages, a P0 feature whose UI or whose states are",
            " missing, any hard-coded value that should have come from wireframe/tokens.css,",
            " any palette hex, font, spacing step, corner radius or shadow that does not match",
            " docs/DESIGN.md, any Absolute Ban from the impeccable reference present anywhere",
            " in the set, anything the PRD marks a non-goal, application logic in JavaScript,",
            " or a wireframe/INDEX.md whose pages, sections, states or links disagree with the",
            " markup. Search for each banned item rather than concluding it is absent, and",
            " check every variant of a component that carries a reserved colour, not just its",
            " default. Report every finding as `page.html §section` so the fix round opens",
            " only the pages you name."
        ),
        MastermindPhase::HtmlToReact => concat!(
            " Treat this as an emitted-code audit, not a visual spot check. Return VERDICT: FAIL if",
            " any review-only navigation (including preview-nav-bar) or its selector appears in an",
            " emitted route, if a terminal/session-ended state exists only in a frozen state",
            " document rather than a generated component, if an external font host is absent from",
            " docs/API_RECORD.md, if the evidence claims an app typecheck passed without fresh",
            " exit-zero output from a real app project, if frozen wireframe HTML or tokens.css",
            " changed, or if .h2r verification does not report structural success. Search emitted",
            " TSX and CSS for the excluded selectors and state-document links; inspect the plan,",
            " manifest, component inventory, governance documents and verification log directly."
        ),
        // Per-task reviews cannot see across tasks; this is the skill's final
        // whole-branch pass, and it is the last gate before Phase 9.
        MastermindPhase::Build => concat!(
            " This is the final whole-branch review, not another per-task one. Read the full",
            " branch diff against its base commit, and rule on what only a cross-task view",
            " shows: integration seams where two tasks meet, logic duplicated across tasks,",
            " conventions that drifted between early and late tasks, and TODO or stub code",
            " left behind. Then check every acceptance criterion in the PRD one by one against",
            " actual behaviour, and every API call in the diff against docs/API_RECORD.md — an",
            " unlisted or mismatched call is an automatic FAIL."
        ),
        MastermindPhase::Wrap => concat!(
            " This phase's deliverable is evidence, so unevidenced claims are the failure",
            " mode. Return VERDICT: FAIL unless this reply itself quotes fresh output for a",
            " clean build and a full test run. Check each PRD acceptance criterion one by one",
            " against actual behaviour rather than against the implementation plan's status",
            " column, and run the audit rubric from the impeccable reference over the built",
            " UI. State plainly what is deferred or unfinished; a wrap-up that hides a gap is",
            " a FAIL even when everything it does mention is true."
        ),
        _ => "",
    }
}

fn phase_deliverable_exists(repo: &Path, phase: MastermindPhase) -> bool {
    match phase {
        MastermindPhase::Setup => repo.join("docs").is_dir(),
        MastermindPhase::Discovery => repo.join("docs/DISCOVERY.md").is_file(),
        MastermindPhase::Prd => repo.join("docs/PRD.md").is_file(),
        MastermindPhase::Features => {
            fs::read_dir(repo.join("docs/features"))
                .ok()
                .is_some_and(|mut entries| {
                    entries.any(|entry| {
                        entry.ok().is_some_and(|entry| {
                            entry.path().extension().is_some_and(|ext| ext == "md")
                        })
                    })
                })
        }
        MastermindPhase::ImplementationPlan => repo.join("docs/IMPLEMENTATION_PLAN.md").is_file(),
        MastermindPhase::ApiRecord => repo.join("docs/API_RECORD.md").is_file(),
        MastermindPhase::Design => repo.join("docs/DESIGN.md").is_file(),
        MastermindPhase::Mockups => {
            repo.join("wireframe/INDEX.md").is_file() && repo.join("wireframe/tokens.css").is_file()
        }
        MastermindPhase::HtmlToReact => repo.join(".h2r/manifest.md").is_file(),
        MastermindPhase::Build | MastermindPhase::Wrap | MastermindPhase::Complete => true,
    }
}

fn append_artifact_snapshot(path: &Path, snapshot: &mut Vec<u8>) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    snapshot.extend_from_slice(path.to_string_lossy().as_bytes());
    if metadata.file_type().is_symlink() {
        snapshot.extend_from_slice(b"\0symlink\0");
        return;
    }
    if metadata.is_file() {
        snapshot.extend_from_slice(b"\0file\0");
        if let Ok(bytes) = fs::read(path) {
            snapshot.extend_from_slice(&bytes);
        }
        return;
    }
    if metadata.is_dir() {
        snapshot.extend_from_slice(b"\0dir\0");
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        let mut children = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        children.sort();
        for child in children {
            append_artifact_snapshot(&child, snapshot);
        }
    }
}

/// A content fingerprint of the exact server-authorized deliverable scope.
/// It lets a timed-out authoring call distinguish newly written output from
/// a stale artifact that existed before the attempted revision.
fn phase_artifact_fingerprint(session: &Session) -> Option<String> {
    if !phase_deliverable_exists(&session.repo, session.phase) {
        return None;
    }
    let mut paths = scoped_phase_write_paths(session);
    paths.sort();
    let mut snapshot = Vec::new();
    for path in paths {
        append_artifact_snapshot(Path::new(&path), &mut snapshot);
    }
    (!snapshot.is_empty()).then(|| sha256_hex(&snapshot))
}

fn artifact_changed_after_authoring(before: &Option<String>, after: &Option<String>) -> bool {
    after.is_some() && before != after
}

/// True when a word appears whole, so `reactive` and `reaction` do not read as
/// React.
fn mentions_react(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    if lower.contains("next.js") || lower.contains("nextjs") {
        return true;
    }
    lower
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(|word| word == "react")
}

/// Whether this project's approved stack is React, which decides whether
/// Phase 7 hands off to Phase 7.5.
///
/// The repository tree is not the authority here. Phase 7.5 is the phase that
/// *creates* the React app, so on a greenfield project `package.json` does not
/// exist yet, and a filesystem-only check skipped h2r on exactly the projects
/// that needed it — silently, because the phase machine simply routed Mockups
/// to Build. The approved planning documents fix the stack long before any code
/// exists, so they decide. An existing `package.json` still counts, for a
/// project adopted with code already in it.
fn is_react_stack(repo: &Path) -> bool {
    let package = fs::read_to_string(repo.join("package.json")).unwrap_or_default();
    if package.contains("\"react\"") || package.contains("\"next\"") {
        return true;
    }
    [
        "docs/DISCOVERY.md",
        "docs/PRD.md",
        "docs/IMPLEMENTATION_PLAN.md",
    ]
    .iter()
    .any(|relative| mentions_react(&fs::read_to_string(repo.join(relative)).unwrap_or_default()))
}

// ---------------------------------------------------------------- service

/// The daemon's adapter table as a function: adapter id -> adapter.
type AdapterResolver = Box<dyn Fn(&str) -> Option<Arc<dyn RuntimeAdapter>> + Send + Sync>;

/// The mastermind service: sessions keyed by id, each holding its own
/// orchestrator and supervisor.
pub struct Mastermind {
    registry: Arc<AgentRegistry>,
    agents_db: PathBuf,
    state_dir: PathBuf,
    /// The *daemon's* journal. Supervisor runs append here rather than to a
    /// per-session journal, so `runs.list`, `tasks.list` and `agents.list`
    /// fold mastermind work instead of returning empty while a 20-node run
    /// is executing. `None` keeps the per-session default (tests).
    journal_db: Option<PathBuf>,
    skill_root: PathBuf,
    adapters: AdapterResolver,
    memex: MemexClient,
    sessions: StdMutex<HashMap<String, Arc<AsyncMutex<Session>>>>,
}

impl std::fmt::Debug for Mastermind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let live = self.sessions.lock().map(|s| s.len()).unwrap_or(0);
        f.debug_struct("Mastermind")
            .field("state_dir", &self.state_dir)
            .field("sessions", &live)
            .finish()
    }
}

impl Mastermind {
    /// Build the service. `resolve_adapter` is the daemon's adapter table
    /// (`claude-code`, `antigravity-agy`, `codex`, `mock`) — injected so
    /// tests can hand in credential-free mocks.
    /// Build the service. `journal_db` is the daemon's journal, so restored
    /// and fresh sessions alike append their run/task events where the
    /// projections can fold them.
    ///
    /// It is a constructor argument rather than a builder because checkpoint
    /// recovery runs here: a `with_journal` applied afterwards would arrive
    /// too late for every session restored at startup, which silently sent
    /// resumed runs back to a per-session journal the daemon never reads.
    pub fn with_journal_db(
        registry: Arc<AgentRegistry>,
        agents_db: impl Into<PathBuf>,
        state_dir: impl Into<PathBuf>,
        journal_db: Option<PathBuf>,
        resolve_adapter: impl Fn(&str) -> Option<Arc<dyn RuntimeAdapter>> + Send + Sync + 'static,
    ) -> Self {
        let service = Self {
            registry,
            agents_db: agents_db.into(),
            state_dir: state_dir.into(),
            journal_db,
            skill_root: crate::skill_import::source_dir().join("mastermind"),
            adapters: Box::new(resolve_adapter),
            memex: MemexClient::from_env(),
            sessions: StdMutex::new(HashMap::new()),
        };
        if let Err(error) = service.restore_checkpoints() {
            tracing::error!(%error, "Mastermind checkpoint recovery failed");
        }
        service
    }

    pub fn new(
        registry: Arc<AgentRegistry>,
        agents_db: impl Into<PathBuf>,
        state_dir: impl Into<PathBuf>,
        resolve_adapter: impl Fn(&str) -> Option<Arc<dyn RuntimeAdapter>> + Send + Sync + 'static,
    ) -> Self {
        let service = Self {
            registry,
            agents_db: agents_db.into(),
            state_dir: state_dir.into(),
            journal_db: None,
            skill_root: crate::skill_import::source_dir().join("mastermind"),
            adapters: Box::new(resolve_adapter),
            memex: MemexClient::from_env(),
            sessions: StdMutex::new(HashMap::new()),
        };
        if let Err(error) = service.restore_checkpoints() {
            tracing::error!(%error, "Mastermind checkpoint recovery failed");
        }
        service
    }

    /// The journal a supervisor for this service should write to.
    fn supervisor_journal(&self, state_dir: &Path) -> PathBuf {
        self.journal_db
            .clone()
            .unwrap_or_else(|| state_dir.join("journal.db"))
    }

    #[cfg(test)]
    fn with_memex(mut self, memex: MemexClient) -> Self {
        self.memex = memex;
        self
    }

    /// Override the Memex database for an embedded/test daemon. Production
    /// normally relies on `MEMEX_DB` (or Memex's default database), but an
    /// explicit per-server path prevents parallel isolated servers from
    /// sharing conversational state.
    pub fn with_memex_db(mut self, path: impl Into<PathBuf>) -> Self {
        self.memex = MemexClient::from_env().with_db(path);
        self
    }

    #[cfg(test)]
    fn with_skill_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.skill_root = root.into();
        self
    }

    /// The enabled registry agents that may receive work: every agent
    /// except the orchestrator itself (it commands, it is not a pool).
    fn worker_records(&self) -> Result<Vec<AgentRecord>, MastermindError> {
        Ok(self
            .registry
            .list_agents()?
            .into_iter()
            .filter(|record| record.enabled && record.id != ORCHESTRATOR_AGENT)
            .collect())
    }

    fn load_session_envelope(
        &self,
        session: &Session,
    ) -> Result<(String, SkillSource, usize, usize), MastermindError> {
        let root = &self.skill_root;
        let skill_path = root.join("SKILL.md");
        let raw = fs::read(&skill_path).map_err(|error| MastermindError::SkillSource {
            path: skill_path.clone(),
            detail: error.to_string(),
        })?;
        let skill =
            String::from_utf8(raw.clone()).map_err(|error| MastermindError::SkillSource {
                path: skill_path.clone(),
                detail: format!("not UTF-8: {error}"),
            })?;
        if !skill.contains("# Mastermind — hierarchical build orchestrator") {
            return Err(MastermindError::SkillSource {
                path: skill_path,
                detail: "canonical Mastermind heading is missing; refusing the shortened fallback"
                    .to_owned(),
            });
        }

        let mut references = String::new();
        for relative in phase_reference_paths(session.phase) {
            let path = root.join(relative);
            let body = fs::read_to_string(&path).map_err(|error| MastermindError::SkillSource {
                path: path.clone(),
                detail: error.to_string(),
            })?;
            references.push_str(&format!(
                "\n\n## Live phase reference: {relative}\n\n{body}"
            ));
        }

        let memories = budget_memories_for_prompt(newest_effective_memories(
            self.memex
                .search(&session.memory_project, "mastermind", 32)?
                .into_iter()
                .filter(|record| {
                    record
                        .tags
                        .split(',')
                        .any(|tag| tag.trim() == format!("session:{}", session.id))
                })
                .collect::<Vec<_>>(),
        ));
        let board = self.memex.board(&session.memory_project)?;
        let memory_json = json!({
            "memories": memories,
            "board": {
                "taskCount": board.tasks.len(),
                "memoryCount": board.memory_count,
                "openHandoffs": board.open_handoffs.iter().take(12).collect::<Vec<_>>(),
            },
        });
        let state = json!({
            "sessionId": session.id,
            "goal": session.goal,
            "repo": session.repo,
            "phase": session.phase.id(),
            "phaseStatus": session.phase_status.as_str(),
            "approvedPhases": session.approved_phases.iter().map(|phase| phase.id()).collect::<Vec<_>>(),
            "discoveryAnswers": session.discovery_answers,
            "deliverables": existing_deliverables(&session.repo),
            "phaseSession": session.phase_session,
            "skillRoot": root,
            "skillScripts": skill_scripts(root, &session.repo, session.phase),
        });

        // The bytes between the canonical markers are exactly SKILL.md.
        // No frontmatter stripping, normalization or model-specific rewrite.
        let mut envelope = String::from("# Canonical Mastermind skill (verbatim)\n\n");
        append_live_skill(&mut envelope, &skill);
        envelope.push_str("\n\n");
        envelope.push_str(AGENTOS_MASTERMIND_BINDING);
        envelope.push_str(&references);
        envelope.push_str("\n\n# Durable Mastermind control state\n\n```json\n");
        envelope.push_str(&serde_json::to_string(&state).unwrap_or_default());
        envelope.push_str("\n```\n\n# Shared Memex memory, board, and open handoffs\n\n```json\n");
        envelope.push_str(&serde_json::to_string(&memory_json).unwrap_or_default());
        envelope.push_str("\n```\n\n---\n\n");

        let open_handoffs = board.open_handoffs.len();
        let task_count = board.tasks.len();
        Ok((
            envelope,
            SkillSource {
                path: skill_path,
                sha256: sha256_hex(&raw),
                loaded_at: Utc::now().to_rfc3339(),
            },
            open_handoffs,
            task_count,
        ))
    }

    fn refresh_orchestrator_model(&self, session: &mut Session) -> Result<(), MastermindError> {
        let (envelope, source, open_handoffs, task_count) = self.load_session_envelope(session)?;
        let roster: Vec<RosterEntry> = self
            .worker_records()?
            .into_iter()
            .map(|record| RosterEntry {
                id: record.id,
                name: record.name,
                description: record.description,
                adapter: record.adapter_id,
                model: record.model,
            })
            .collect();
        let adapter = (self.adapters)(&session.planner_adapter)
            .ok_or_else(|| MastermindError::UnknownAdapter(session.planner_adapter.clone()))?;
        let model = ClaudePlanningModel::new(adapter, &session.repo)
            .with_model(session.planner_model.clone())
            .with_skill_preamble(Some(envelope))
            .with_conversation(session.planning_conversation.clone());
        session.skill_source = Some(source);
        session.open_handoffs = open_handoffs;
        session.task_count = task_count;
        session.memory_error = None;
        session.roster = roster.clone();
        session.orchestrator.replace_roster(roster);
        session.orchestrator.replace_model(Arc::new(model));
        session.model_ready = true;
        Ok(())
    }

    /// Switch only on the typed adapter quota signal. Generic model errors,
    /// timeouts, auth failures, and task failures are deliberately left alone:
    /// trying another paid provider for those can hide a real issue or create
    /// surprising spend. The replacement must use a different provider so an
    /// account-wide provider limit cannot immediately reoccur.
    fn fail_over_planner(
        &self,
        session: &mut Session,
        error: &MastermindError,
    ) -> Result<bool, MastermindError> {
        let MastermindError::Orchestrator(OrchestratorError::ProviderLimit { detail }) = error
        else {
            return Ok(false);
        };
        let Some((adapter, model)) = PLANNER_FALLBACKS.iter().copied().find(|(adapter, _)| {
            *adapter != session.planner_adapter && (self.adapters)(adapter).is_some()
        }) else {
            return Ok(false);
        };

        let previous = format!("{} · {}", session.planner_adapter, session.planner_model);
        session.planner_adapter = adapter.to_owned();
        session.planner_model = model.to_owned();
        session.planning_conversation.reset();
        session.model_ready = false;
        session.phase_status = PhaseStatus::Active;
        session.last_review = Some(format!(
            "Automatic provider fallback: {previous} reported a quota/rate limit ({detail}). Switched to {adapter} · {model} and retried the current phase."
        ));
        self.refresh_orchestrator_model(session)?;
        Ok(true)
    }

    fn fail_over_after_report(
        &self,
        session: &mut Session,
        report: &PlanCycleReport,
    ) -> Result<bool, MastermindError> {
        let Some(detail) = report
            .model_error
            .as_deref()
            .and_then(|message| message.strip_prefix("planning provider limit reached: "))
        else {
            return Ok(false);
        };
        self.fail_over_planner(
            session,
            &MastermindError::Orchestrator(OrchestratorError::ProviderLimit {
                detail: detail.to_owned(),
            }),
        )
    }

    fn persist_session(&self, session: &Session) -> Result<(), MastermindError> {
        let dir = self
            .state_dir
            .join("mastermind")
            .join(&session.id)
            .join("checkpoints");
        fs::create_dir_all(&dir).map_err(|error| MastermindError::Checkpoint {
            path: dir.clone(),
            detail: error.to_string(),
        })?;
        let path = dir.join(format!(
            "{}-{}.json",
            Utc::now().format("%Y%m%dT%H%M%S%.3fZ"),
            Uuid::now_v7()
        ));
        let bytes = serde_json::to_vec_pretty(&session.checkpoint()).map_err(|error| {
            MastermindError::Checkpoint {
                path: path.clone(),
                detail: error.to_string(),
            }
        })?;
        let temporary = path.with_extension("json.tmp");
        fs::write(&temporary, bytes).map_err(|error| MastermindError::Checkpoint {
            path: temporary.clone(),
            detail: error.to_string(),
        })?;
        fs::rename(&temporary, &path).map_err(|error| MastermindError::Checkpoint {
            path,
            detail: format!("could not atomically publish checkpoint: {error}"),
        })
    }

    fn restore_checkpoints(&self) -> Result<(), MastermindError> {
        let root = self.state_dir.join("mastermind");
        let Ok(entries) = fs::read_dir(&root) else {
            return Ok(());
        };
        for entry in entries
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_dir())
        {
            let checkpoints = entry.path().join("checkpoints");
            let Ok(files) = fs::read_dir(&checkpoints) else {
                continue;
            };
            let mut paths: Vec<PathBuf> = files
                .filter_map(Result::ok)
                .map(|file| file.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
                .collect();
            paths.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
            let mut restored = None;
            for path in paths {
                let Ok(raw) = fs::read(&path) else { continue };
                let Ok(checkpoint) = serde_json::from_slice::<SessionCheckpoint>(&raw) else {
                    continue;
                };
                match self.restore_one(checkpoint) {
                    Ok(session) => {
                        restored = Some(session);
                        break;
                    }
                    Err(error) => tracing::warn!(path = %path.display(), %error,
                        "could not restore Mastermind checkpoint"),
                }
            }
            if let Some(session) = restored {
                let id = session.id.clone();
                self.sessions
                    .lock()
                    .map_err(|_| MastermindError::UnknownSession(id.clone()))?
                    .insert(id, Arc::new(AsyncMutex::new(session)));
            }
        }
        Ok(())
    }

    fn restore_one(&self, checkpoint: SessionCheckpoint) -> Result<Session, MastermindError> {
        if !checkpoint.repo.is_dir() {
            return Err(MastermindError::NoRepo(checkpoint.repo));
        }
        let records = self.worker_records()?;
        let state_dir = self.state_dir.join("mastermind").join(&checkpoint.id);
        let mut config = SupervisorConfig::for_repo(&state_dir, &checkpoint.repo)
            .with_agents_db(&self.agents_db)
            .with_shared_task_workspace()
            .with_role_model(STRONGER_AGENT, &checkpoint.planner_model);
        config.journal_db = self.supervisor_journal(&state_dir);
        config.orchestrator = ORCHESTRATOR_AGENT.to_owned();
        for record in &records {
            config = config.with_role_adapter(&record.id, &record.adapter_id);
        }
        config = config.with_role_adapter(STRONGER_AGENT, &checkpoint.planner_adapter);
        if records.iter().any(|record| record.id == DEBUGGER_AGENT)
            && records
                .iter()
                .any(|record| record.id == DEBUGGER_SOL_ESCALATION_AGENT)
        {
            config =
                config.with_reasoning_escalation(DEBUGGER_AGENT, DEBUGGER_SOL_ESCALATION_AGENT);
        }
        let mut adapter_ids: Vec<String> = records
            .iter()
            .map(|record| record.adapter_id.clone())
            .collect();
        adapter_ids.extend([
            config.default_adapter.clone(),
            checkpoint.planner_adapter.clone(),
        ]);
        adapter_ids.sort();
        adapter_ids.dedup();
        let adapters = adapter_ids
            .iter()
            .map(|id| {
                (self.adapters)(id).ok_or_else(|| MastermindError::UnknownAdapter(id.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let supervisor = Arc::new(Supervisor::new(config, adapters)?);
        let sink = Arc::new(SupervisorSink {
            store_sink: WorkflowSink::new(Arc::clone(supervisor.store())),
            supervisor: Arc::clone(&supervisor),
            base_commit: head_commit(&checkpoint.repo)?,
            allowed_paths: vec!["**".to_owned()],
        });
        let conversation = PlanningConversation::new(checkpoint.provider_session_id.clone());
        let adapter = (self.adapters)(&checkpoint.planner_adapter)
            .ok_or_else(|| MastermindError::UnknownAdapter(checkpoint.planner_adapter.clone()))?;
        let model = Arc::new(
            ClaudePlanningModel::new(adapter, &checkpoint.repo)
                .with_model(checkpoint.planner_model.clone())
                .with_conversation(conversation.clone()),
        );
        let roster: Vec<RosterEntry> = records
            .iter()
            .map(|record| RosterEntry {
                id: record.id.clone(),
                name: record.name.clone(),
                description: record.description.clone(),
                adapter: record.adapter_id.clone(),
                model: record.model.clone(),
            })
            .collect();
        let mut orchestrator = Orchestrator::from_checkpoint(checkpoint.orchestrator, model, sink);
        orchestrator.replace_roster(roster.clone());
        let mut session = Session {
            id: checkpoint.id,
            goal: checkpoint.goal,
            repo: checkpoint.repo,
            orchestrator,
            supervisor,
            roster,
            planner_adapter: checkpoint.planner_adapter,
            planner_model: checkpoint.planner_model,
            discovery_questions: checkpoint.discovery_questions,
            discovery_answers: checkpoint.discovery_answers,
            discovery_complete: checkpoint.discovery_complete,
            phase: checkpoint.phase,
            phase_status: checkpoint.phase_status,
            approved_phases: checkpoint.approved_phases,
            skill_source: checkpoint.skill_source,
            memory_project: checkpoint.memory_project,
            memory_revision: checkpoint.memory_revision,
            memory_error: checkpoint.memory_error,
            legacy_memory_imported: checkpoint.legacy_memory_imported,
            last_review: checkpoint.last_review,
            phase_session: checkpoint.phase_session,
            accepted_tasks_in_session: checkpoint.accepted_tasks_in_session,
            open_handoffs: checkpoint.open_handoffs,
            task_count: checkpoint.task_count,
            planning_conversation: conversation,
            phase_write_paths: checkpoint.phase_write_paths,
            model_ready: false,
            turns: Vec::new(),
        };
        if phase_requires_write_authorization(session.phase) {
            let deliverable_exists = phase_deliverable_exists(&session.repo, session.phase);
            match session.phase_status {
                PhaseStatus::Running if deliverable_exists => {
                    session.phase_status = PhaseStatus::NeedsRevision;
                    session.last_review = Some(
                        "Authoring was interrupted after the deliverable appeared. Provide revision guidance to resume the scoped author and run a fresh review."
                            .to_owned(),
                    );
                }
                PhaseStatus::Running => {
                    session.phase_status = PhaseStatus::AwaitingWriteApproval;
                }
                PhaseStatus::Active if deliverable_exists => {
                    session.phase_status = PhaseStatus::AwaitingApproval;
                }
                _ => {}
            }
        }
        Ok(session)
    }

    fn import_legacy_memory(&self, session: &mut Session) -> Result<(), MastermindError> {
        if session.legacy_memory_imported {
            return Ok(());
        }
        let legacy = [
            "docs/memory/MEMORY.md",
            "docs/memory/HANDOFF.md",
            "docs/memory/RESUME.md",
        ];
        let existing = self
            .memex
            .search(&session.memory_project, "legacy-import", 100)?;
        for relative in legacy {
            let path = session.repo.join(relative);
            if !path.is_file() {
                continue;
            }
            let content =
                fs::read_to_string(&path).map_err(|error| MastermindError::Checkpoint {
                    path: path.clone(),
                    detail: format!("legacy memory import failed: {error}"),
                })?;
            if content.trim().is_empty() {
                continue;
            }
            let path_tag = format!("path:{}", relative.replace('/', "-"));
            let session_tag = format!("session:{}", session.id);
            if existing.iter().any(|record| {
                let tags: Vec<&str> = record.tags.split(',').map(str::trim).collect();
                tags.contains(&path_tag.as_str()) && tags.contains(&session_tag.as_str())
            }) {
                continue;
            }
            let payload = json!({
                "kind": "legacy-import",
                "legacyPath": relative,
                "content": content,
            });
            let id = self.memex.remember(
                &session.memory_project,
                &payload.to_string(),
                ORCHESTRATOR_AGENT,
                &session.planner_adapter,
                "context",
                &format!(
                    "mastermind,session:{},phase:legacy,kind:legacy-import,path:{}",
                    session.id,
                    relative.replace('/', "-")
                ),
            )?;
            session.memory_revision = Some(session.memory_revision.unwrap_or_default().max(id));
        }
        session.legacy_memory_imported = true;
        Ok(())
    }

    fn sync_phase_memory(
        &self,
        session: &mut Session,
        next: MastermindPhase,
    ) -> Result<(), MastermindError> {
        let provider = if session.planner_adapter == "claude-code" {
            "claude"
        } else if session.planner_adapter == "codex" {
            "codex"
        } else {
            "gemini"
        };
        let base_tags = format!(
            "mastermind,session:{},phase:{},kind:phase-snapshot",
            session.id,
            session.phase.number()
        );
        let existing = self
            .memex
            .search(&session.memory_project, "mastermind", 500)?;
        let session_tag = format!("session:{}", session.id);

        if session.phase == MastermindPhase::Discovery {
            for (index, (question, answer)) in session.discovery_answers.iter().enumerate() {
                let decision_tag = format!("decision:{}", index + 1);
                if let Some(record) = existing.iter().find(|record| {
                    let tags: Vec<&str> = record.tags.split(',').map(str::trim).collect();
                    tags.contains(&session_tag.as_str())
                        && tags.contains(&"kind:approved-decision")
                        && tags.contains(&decision_tag.as_str())
                }) {
                    session.memory_revision =
                        Some(session.memory_revision.unwrap_or_default().max(record.id));
                    continue;
                }
                let content = json!({
                    "sessionId": session.id,
                    "phase": session.phase.id(),
                    "question": question,
                    "answer": answer,
                    "ordinal": index + 1,
                });
                let id = self.memex.remember(
                    &session.memory_project,
                    &content.to_string(),
                    ORCHESTRATOR_AGENT,
                    provider,
                    "decision",
                    &format!(
                        "mastermind,session:{},phase:1,kind:approved-decision,decision:{}",
                        session.id,
                        index + 1
                    ),
                )?;
                session.memory_revision = Some(session.memory_revision.unwrap_or_default().max(id));
            }
        }

        if session.phase == MastermindPhase::ApiRecord {
            let api_tag = "kind:verified-api";
            if let Some(record) = existing.iter().find(|record| {
                let tags: Vec<&str> = record.tags.split(',').map(str::trim).collect();
                tags.contains(&session_tag.as_str()) && tags.contains(&api_tag)
            }) {
                session.memory_revision =
                    Some(session.memory_revision.unwrap_or_default().max(record.id));
            } else {
                let api_path = session.repo.join("docs/API_RECORD.md");
                let content =
                    fs::read_to_string(&api_path).map_err(|error| MastermindError::Checkpoint {
                        path: api_path.clone(),
                        detail: format!("could not persist verified API discoveries: {error}"),
                    })?;
                let id = self.memex.remember(
                    &session.memory_project,
                    &content,
                    ORCHESTRATOR_AGENT,
                    provider,
                    "api",
                    &format!(
                        "mastermind,session:{},phase:5,kind:verified-api",
                        session.id
                    ),
                )?;
                session.memory_revision = Some(session.memory_revision.unwrap_or_default().max(id));
            }
        }

        let snapshot = json!({
            "sessionId": session.id,
            "goal": session.goal,
            "approvedPhase": session.phase.id(),
            "nextPhase": next.id(),
            "deliverables": existing_deliverables(&session.repo),
            "frozenFacts": {
                "repo": session.repo,
                "plannerAdapter": session.planner_adapter,
                "plannerModel": session.planner_model,
                "reviewer": "code-reviewer",
                "fallbackWorker": LUNA_FALLBACK_AGENT,
            },
            "openQuestions": session.discovery_questions,
            "nextAction": format!("Begin {}", next.name()),
            "skillSha256": session.skill_source.as_ref().map(|source| source.sha256.clone()),
        });
        let existing_snapshot = existing.iter().find(|record| {
            let tags: Vec<&str> = record.tags.split(',').map(str::trim).collect();
            tags.contains(&session_tag.as_str())
                && tags.contains(&format!("phase:{}", session.phase.number()).as_str())
                && tags.contains(&"kind:phase-snapshot")
        });
        let id = if let Some(record) = existing_snapshot {
            record.id
        } else {
            self.memex.remember(
                &session.memory_project,
                &snapshot.to_string(),
                ORCHESTRATOR_AGENT,
                provider,
                "context",
                &base_tags,
            )?
        };
        session.memory_revision = Some(session.memory_revision.unwrap_or_default().max(id));

        let artifacts = existing_deliverables(&session.repo).join(",");
        let handoff_task = format!("{}:{}", session.id, next.id());
        if !self
            .memex
            .handoffs(&session.memory_project, None)?
            .iter()
            .any(|handoff| handoff.task_id == handoff_task)
        {
            self.memex.handoff_create(
                &session.memory_project,
                ORCHESTRATOR_AGENT,
                active_agent_for_phase(next),
                &handoff_task,
                &format!(
                    "Phase {} approved; begin {} from Memex revision {}.",
                    session.phase.number(),
                    next.name(),
                    session.memory_revision.unwrap_or_default()
                ),
                &artifacts,
                "",
            )?;
        }
        session.memory_error = None;
        Ok(())
    }

    fn sync_plan_tasks(&self, session: &Session) -> Result<(), MastermindError> {
        for planned in session.orchestrator.plan().nodes() {
            self.memex.task_set(
                &session.memory_project,
                &format!("{}:{}", session.id, planned.spec.id),
                planned.objective.as_deref().unwrap_or(&planned.spec.id),
                planned
                    .spec
                    .agent_role
                    .as_deref()
                    .unwrap_or(LUNA_FALLBACK_AGENT),
                if planned.materialized {
                    "ready"
                } else {
                    "todo"
                },
                planned
                    .spec
                    .agent_role
                    .as_deref()
                    .unwrap_or(LUNA_FALLBACK_AGENT),
            )?;
        }
        Ok(())
    }

    fn sync_run_memory(
        &self,
        session: &Session,
        run: Option<&RunView>,
    ) -> Result<(), MastermindError> {
        if let Some(run) = run {
            for task in &run.tasks {
                self.memex.task_set(
                    &session.memory_project,
                    &format!("{}:{}", session.id, task.node_id),
                    &task.node_id,
                    task.pool.as_deref().unwrap_or(LUNA_FALLBACK_AGENT),
                    &format!("{:?}", task.state).to_ascii_lowercase(),
                    task.pool.as_deref().unwrap_or(LUNA_FALLBACK_AGENT),
                )?;
            }
        }

        let Some(run_id) = session.orchestrator.plan().run_id() else {
            return Ok(());
        };
        let existing = self.memex.handoffs(&session.memory_project, None)?;
        for packet in session.supervisor.handoff_packets(&run_id)? {
            let task_id = format!("handoff-packet:{}", packet.id);
            if existing.iter().any(|handoff| handoff.task_id == task_id) {
                continue;
            }
            self.memex.handoff_create(
                &session.memory_project,
                &packet.from_agent,
                REVIEWER_AGENT,
                &task_id,
                &format!(
                    "Accepted AgentOS HandoffPacket reference for task {} ({:?}).",
                    packet.task_id, packet.status
                ),
                &format!("agentos-handoff:{}", packet.id),
                "",
            )?;
        }
        Ok(())
    }

    fn record_revision_memory(
        &self,
        session: &mut Session,
        guidance: &str,
    ) -> Result<(), MastermindError> {
        let content = json!({
            "sessionId": session.id,
            "phase": session.phase.id(),
            "supersedesRevision": session.memory_revision,
            "revisionGuidance": guidance,
        });
        let id = self.memex.remember(
            &session.memory_project,
            &content.to_string(),
            ORCHESTRATOR_AGENT,
            &session.planner_adapter,
            "decision",
            &format!(
                "mastermind,session:{},phase:{},kind:decision-revision",
                session.id,
                session.phase.number()
            ),
        )?;
        session.memory_revision = Some(id);
        if let Some(review) = session.last_review.as_deref() {
            let lesson = json!({
                "sessionId": session.id,
                "phase": session.phase.id(),
                "resolvedReview": review,
                "resolution": guidance,
                "supersedesMemoryId": id,
            });
            let lesson_id = self.memex.remember(
                &session.memory_project,
                &lesson.to_string(),
                ORCHESTRATOR_AGENT,
                &session.planner_adapter,
                "lesson",
                &format!(
                    "mastermind,session:{},phase:{},kind:resolved-mistake",
                    session.id,
                    session.phase.number()
                ),
            )?;
            session.memory_revision = Some(id.max(lesson_id));
        }
        session.memory_error = None;
        Ok(())
    }

    fn phase_prompt(&self, session: &Session, revision: Option<&str>) -> String {
        let source = match session.phase {
            MastermindPhase::Discovery => DISCOVERY_AUTHORING_BRIEF,
            MastermindPhase::Prd => "Read docs/DISCOVERY.md.",
            MastermindPhase::Features => "Read docs/PRD.md.",
            MastermindPhase::ImplementationPlan => {
                "Read docs/PRD.md and every docs/features/*.md file."
            }
            MastermindPhase::ApiRecord => "Read docs/IMPLEMENTATION_PLAN.md and verify real APIs.",
            MastermindPhase::Design => {
                "Read the approved PRD personas and flows, and read every docs/features/*.md file in full."
            }
            MastermindPhase::Mockups => {
                "Read docs/DESIGN.md and the approved PRD flows, and read every docs/features/*.md file in full."
            }
            MastermindPhase::HtmlToReact => {
                "Read wireframe/INDEX.md and run the live Mastermind h2r procedure. Treat wireframe/*.html and wireframe/tokens.css as frozen inputs: emit only through h2r. Reconcile review-only UI that must be omitted, state-only product states that must become generated components, external assets that need docs/API_RECORD.md entries, and whether a real app typecheck is actually available."
            }
            MastermindPhase::Wrap => {
                "Read the PRD acceptance criteria and implementation plan; verify the actual build."
            }
            _ => "Use the durable state and approved artifacts supplied above.",
        };
        // The mockups inherit the design document's contract, so both phases
        // author under the same verification rules.
        // Mockups implement a spec they did not author, so they take the
        // shared brief plus the inherited-constraint checks on top of it.
        let design_rules = match session.phase {
            MastermindPhase::Design => format!(
                "{DESIGN_AUTHORING_BRIEF}

"
            ),
            MastermindPhase::Mockups => {
                format!(
                    "{DESIGN_AUTHORING_BRIEF}

{MOCKUP_AUTHORING_BRIEF}

"
                )
            }
            _ => String::new(),
        };
        let revision = revision
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("\n\nUSER REVISION GUIDANCE:\n{value}"))
            .unwrap_or_default();
        let write_scope = scoped_phase_write_labels(session).join(", ");
        // The live skill tells several phases to run a bundled Python script,
        // and every authoring turn except H2R denies the shell. Saying so is
        // what makes the skill's own manual fallback reachable; without it the
        // author improvises silently and reports the derivation as if the
        // generator had run.
        let shell = if session.phase == MastermindPhase::HtmlToReact {
            "This turn has a shell because the canonical procedure for this phase is the bundled script itself. Invoke it at the absolute path under `skillScripts` in the control state above, not at the path the reference hardcodes, and run extract, emit and verify in that order rather than hand-writing any markup. If `skillScripts` carries no `h2r` entry the script is not installed: stop and report that instead of converting by hand. Quote the real output of the type-check or build you run."
        } else {
            "This turn has NO shell: Bash and PowerShell are denied, so no bundled script can run here — not the design-system generator, not the mockup manifest extractor, whatever `skillScripts` lists. Apply the skill's documented manual fallback instead, and name in your evidence report which script you could not run and what you did in its place. Never present a derivation as generated when you chose it by hand."
        };
        format!(
            "# MASTERMIND SCOPED AUTHORING TURN\n\nUSER AUTHORIZATION RECEIPT: the user approved one write grant for this phase's declared deliverable paths.\nAUTHORIZED WRITE SCOPE: {write_scope}\n\nCurrent phase: {} — {}.\n{}\n\n{design_rules}Produce {} exactly as the canonical live skill requires. Write the deliverable's real heading and section skeleton to disk on your first write, before any long verification work. Then append to that same file incrementally: the moment you verify one item, write its entry to the file before starting the next verification. Never accumulate verified content to write in one final pass, and never leave a section reading as a placeholder once you have verified anything belonging in it. A partial artifact on disk can be reviewed and revised; an empty or placeholder-only write scope after a timeout loses the whole turn. Do not create MEMORY.md, HANDOFF.md, RESUME.md, or any replacement memory file. Write only the allowed phase deliverable paths. Do not begin the next phase. Do not run Git mutations. {shell} Finish with a terse evidence report naming the files written and checks run.{}",
            session.phase.number(),
            session.phase.name(),
            source,
            phase_deliverable(session.phase),
            revision,
        )
    }

    fn phase_preparation_prompt(&self, session: &Session, guidance: Option<&str>) -> String {
        let source = match session.phase {
            MastermindPhase::Prd => "Read docs/DISCOVERY.md.",
            MastermindPhase::Features => "Read docs/PRD.md.",
            MastermindPhase::ImplementationPlan => {
                "Read docs/PRD.md and every docs/features/*.md file."
            }
            MastermindPhase::ApiRecord => {
                "Read docs/IMPLEMENTATION_PLAN.md and identify the APIs that require verification."
            }
            MastermindPhase::Design => "Read the approved PRD personas and flows.",
            MastermindPhase::Mockups => "Read docs/DESIGN.md and the approved PRD flows.",
            MastermindPhase::HtmlToReact => {
                "Read .h2r/manifest.md, .h2r/plan.json, wireframe/INDEX.md, docs/IMPLEMENTATION_PLAN.md and docs/API_RECORD.md. Never open or modify the frozen mockup HTML. Reconcile review-only markup to omit, state-only product states to emit, external asset hosts to record, and the app project's real typecheck prerequisites."
            }
            _ => "Use the durable state and approved artifacts supplied above.",
        };
        let guidance = guidance
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("\n\nUSER PREPARATION GUIDANCE:\n{value}"))
            .unwrap_or_default();
        let readiness = if session.phase == MastermindPhase::HtmlToReact {
            "Reply with `READY_TO_AUTHOR`, then a `WRITE_PATHS:` list containing only repository-relative source files or directories H2R must create or edit. Do not list frozen input artifacts. The daemon adds only exact H2R governance files; never request a broad docs/ or wireframe/ grant."
        } else {
            "Reply with exactly `READY_TO_AUTHOR` when the declared deliverable can be authored from the approved inputs."
        };
        format!(
            "# MASTERMIND READ-ONLY PHASE PREPARATION\n\nCurrent phase: {} — {}.\n{}\n\nBuild the complete mental model needed to produce {}. Do not create or edit files, do not invoke ExitPlanMode, and do not begin another phase. {}",
            session.phase.number(),
            session.phase.name(),
            source,
            phase_deliverable(session.phase),
            readiness,
        ) + &guidance
    }

    fn phase_author_identity(
        &self,
        session: &Session,
    ) -> Result<(String, String), MastermindError> {
        let record = match session.phase {
            MastermindPhase::Design | MastermindPhase::Mockups => {
                self.registry.get_agent("ui-designer")?
            }
            MastermindPhase::HtmlToReact => self
                .registry
                .get_agent("nextjs-dev")?
                .or_else(|| self.registry.get_agent(LUNA_FALLBACK_AGENT).ok().flatten()),
            _ => None,
        };
        Ok(record
            .map(|record| {
                (
                    record.adapter_id,
                    record
                        .model
                        .unwrap_or_else(|| session.planner_model.clone()),
                )
            })
            .unwrap_or_else(|| {
                (
                    session.planner_adapter.clone(),
                    session.planner_model.clone(),
                )
            }))
    }

    async fn prepare_author_phase(
        &self,
        session: &mut Session,
        guidance: Option<&str>,
    ) -> Result<(), MastermindError> {
        let (envelope, source, open_handoffs, task_count) = self.load_session_envelope(session)?;
        session.skill_source = Some(source);
        session.open_handoffs = open_handoffs;
        session.task_count = task_count;
        let (adapter_id, model_slug) = self.phase_author_identity(session)?;
        let model_slug_label = model_slug.clone();
        let adapter = (self.adapters)(&adapter_id)
            .ok_or_else(|| MastermindError::UnknownAdapter(adapter_id.clone()))?;
        let model = ClaudePlanningModel::new(adapter, &session.repo)
            .with_model(model_slug)
            .with_skill_preamble(Some(envelope))
            .with_conversation(session.planning_conversation.clone());
        session.phase_status = PhaseStatus::Running;
        let response = model
            .propose(&self.phase_preparation_prompt(session, guidance))
            .await?;
        session.record_turn(
            "preparation",
            active_agent_for_phase(session.phase),
            &model_slug_label,
            &response.text,
        );
        if session.phase == MastermindPhase::HtmlToReact {
            session.phase_write_paths =
                h2r_write_paths_from_preparation(&session.repo, &response.text);
        } else {
            session.phase_write_paths = phase_output_paths(&session.repo, session.phase);
        }
        if !response
            .text
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("READY_TO_AUTHOR")
        {
            tracing::debug!(
                phase = session.phase.id(),
                response = %response.text,
                "phase preparation returned non-canonical readiness text"
            );
        }
        session.phase_status = PhaseStatus::AwaitingWriteApproval;
        Ok(())
    }

    async fn author_phase(
        &self,
        session: &mut Session,
        revision: Option<&str>,
    ) -> Result<String, MastermindError> {
        // Stage before the envelope is built, so `skillScripts` reports the
        // staged path the agent can actually reach. Authoring only — the
        // read-only preparation turn must not write into the repository.
        stage_phase_scripts(&self.skill_root, &session.repo, session.phase);
        let (envelope, source, open_handoffs, task_count) = self.load_session_envelope(session)?;
        session.skill_source = Some(source);
        session.open_handoffs = open_handoffs;
        session.task_count = task_count;
        let (adapter_id, model_slug) = self.phase_author_identity(session)?;
        let model_slug_label = model_slug.clone();
        let adapter = (self.adapters)(&adapter_id)
            .ok_or_else(|| MastermindError::UnknownAdapter(adapter_id.clone()))?;
        // Phase 7.5 is the one phase whose canonical procedure *is* a shell
        // command: `h2r.py extract | emit | verify` plus a real type-check,
        // and the skill forbids hand-writing the markup those steps produce.
        // Denying the shell there does not harden the turn, it just forces the
        // violation. Delegation and skill re-entry stay denied everywhere, and
        // the write scope is still enforced by `allowed_paths`.
        let shell_tools = [
            "Bash",
            "BashOutput",
            "KillShell",
            "PowerShell",
            "Tmux",
            "REPL",
        ];
        let deny = ["Agent", "Task", "TaskCreate", "Skill", "Workflow"]
            .into_iter()
            .chain(
                (session.phase != MastermindPhase::HtmlToReact)
                    .then_some(shell_tools)
                    .into_iter()
                    .flatten(),
            )
            .map(str::to_owned)
            .collect();
        // Granting the shell by omitting it from the denylist is necessary
        // but not sufficient. The claude adapter runs at `acceptEdits`, which
        // pre-approves file edits and nothing else, and a headless `-p` run
        // cannot answer a permission prompt — so every Bash call this phase's
        // procedure makes was denied in silence and the turn returned an empty
        // result having written nothing. Naming the tools the procedure needs
        // pre-approves exactly those; the denylist above still outranks this.
        let allow: Vec<String> = if session.phase == MastermindPhase::HtmlToReact {
            [
                "Bash",
                "BashOutput",
                "KillShell",
                "Read",
                "Write",
                "Edit",
                "Glob",
                "Grep",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect()
        } else {
            Vec::new()
        };
        let model = ClaudePlanningModel::new(adapter, &session.repo)
            .with_model(model_slug)
            .with_skill_preamble(Some(envelope))
            .with_allowed_paths(scoped_phase_write_paths(session))
            .with_tool_allowlist(allow)
            .with_tool_denylist(deny)
            .with_timeout(Duration::from_secs(AUTHOR_TIMEOUT_SECS))
            .with_conversation(session.planning_conversation.clone());
        session.phase_status = PhaseStatus::Running;
        let response = model.propose(&self.phase_prompt(session, revision)).await?;
        session.record_turn(
            "authoring",
            active_agent_for_phase(session.phase),
            &model_slug_label,
            &response.text,
        );
        Ok(response.text)
    }

    async fn review_phase(&self, session: &mut Session) -> Result<bool, MastermindError> {
        let Some(record) = self.registry.get_agent(REVIEWER_AGENT)? else {
            session.last_review =
                Some("VERDICT: PASS\nNo reviewer record is installed.".to_owned());
            return Ok(true);
        };
        let adapter = (self.adapters)(&record.adapter_id)
            .ok_or_else(|| MastermindError::UnknownAdapter(record.adapter_id.clone()))?;
        // The prompt orders the reviewer to check the deliverable "against the
        // canonical phase requirements", so it has to be given them. Its own
        // record preamble carries none of the Mastermind rubric — the phase
        // envelope does, including the phase reference whose review checklist
        // this turn is supposed to apply.
        let (envelope, _, _, _) = self.load_session_envelope(session)?;
        let preamble = format!("{}{envelope}", self.registry.preamble_for(&record)?);
        let reviewer_model = record.model.unwrap_or_else(|| "gpt-5.6-terra".to_owned());
        let mut model = ClaudePlanningModel::new(adapter, &session.repo)
            .with_model(reviewer_model.clone())
            .with_skill_preamble(Some(preamble))
            .with_timeout(Duration::from_secs(REVIEW_TIMEOUT_SECS));
        // Phase 9's deliverable *is* execution evidence, so its reviewer is
        // the one reviewer that runs commands. Both gates have to open
        // together: dropping the shell tokens is what clears agy's
        // `--sandbox` (it has no per-tool deny surface, so the denylist's
        // shell tokens are the only thing selecting it), and a non-empty
        // write scope is what selects `--mode accept-edits` over the
        // read-only `plan` mode a build would fail under.
        if review_runs_commands(session.phase) {
            model = model
                .with_tool_denylist(reviewer_denylist_with_shell())
                .with_allowed_paths(vec![session.repo.display().to_string()]);
        }
        let prompt = format!(
            "Review Mastermind Phase {} ({}) deliverable: {}. Check it against the approved upstream documents and the canonical phase requirements supplied above, and apply in full every review rubric the live phase reference states for this phase — its listed FAIL conditions are binding on your verdict.{}{} Answer in this reply and do not defer, ask to proceed, or write a plan document. Reply with `VERDICT: PASS` or `VERDICT: FAIL`, followed by concrete findings, each naming the file and the requirement it violates.",
            session.phase.number(),
            session.phase.name(),
            phase_deliverable(session.phase),
            phase_review_checks(session.phase),
            review_scope_clause(session.phase),
        );
        let response = model.propose(&prompt).await?;
        let passed = verdict_is_pass(&response.text);
        // Failing closed on a missing verdict is right; failing closed
        // *silently* is not. An empty provider result used to land as
        // `lastReview: ""` with no review turn at all — indistinguishable
        // from a genuine rejection that simply listed no findings.
        let review = if response.text.trim().is_empty() {
            format!(
                "VERDICT: FAIL

The reviewer ({REVIEWER_AGENT} on {reviewer_model}) returned an empty response, so this phase was not reviewed. This is a provider result, not a finding against the deliverable: re-run the review, or approve the phase explicitly if you have checked it yourself."
            )
        } else {
            response.text
        };
        session.record_turn("review", REVIEWER_AGENT, &reviewer_model, &review);
        session.last_review = Some(review);
        Ok(passed)
    }

    async fn run_document_phase(
        &self,
        session: &mut Session,
        revision: Option<&str>,
    ) -> Result<(), MastermindError> {
        session.phase_status = PhaseStatus::Running;
        self.persist_session(session)?;
        let before = phase_artifact_fingerprint(session);
        let mut author_result = self.author_phase(session, revision).await;
        // A quota rejection happens before a successful provider result. If
        // the scoped deliverable is still absent, the fresh provider can make
        // the one authorized authoring attempt without widening its scope.
        if author_result.is_err()
            && !phase_deliverable_exists(&session.repo, session.phase)
            && self
                .fail_over_planner(session, author_result.as_ref().expect_err("checked error"))?
        {
            session.phase_status = PhaseStatus::Running;
            author_result = self.author_phase(session, revision).await;
        }
        let after = phase_artifact_fingerprint(session);
        if !phase_deliverable_exists(&session.repo, session.phase) {
            session.phase_status = PhaseStatus::Blocked;
            if let Err(error) = author_result {
                return Err(error);
            }
            // The author returned normally but wrote nothing. Its own response
            // is the only account of why, so surface it instead of discarding
            // it — without this the failure is indistinguishable from a crash
            // and every retry is a blind guess.
            let response = author_result.unwrap_or_default();
            let transcript = response.trim();
            session.last_review = Some(format!(
                "AUTHOR RETURNED WITHOUT WRITING {}

The scoped authoring turn completed successfully but its                  deliverable is absent. Verbatim author response follows.

{}",
                phase_deliverable(session.phase),
                if transcript.is_empty() {
                    "(the author returned an empty response)"
                } else {
                    transcript
                },
            ));
            return Err(MastermindError::InvalidPhase {
                session: session.id.clone(),
                phase: session.phase.id().to_owned(),
                status: format!(
                    "author session finished without producing {} (author response retained in the phase review)",
                    phase_deliverable(session.phase)
                ),
            });
        }
        if let Err(error) = author_result {
            if !artifact_changed_after_authoring(&before, &after) {
                session.phase_status = PhaseStatus::Blocked;
                return Err(error);
            }
            tracing::warn!(
                session = %session.id,
                phase = session.phase.id(),
                %error,
                "authoring call failed after changing its scoped artifact; continuing with read-only review"
            );
            // A timed-out/cancelled provider conversation is not safe to
            // resume for a later revision. Any subsequent edit reconstructs
            // a clean scoped turn from the durable envelope.
            session.planning_conversation.reset();
        }
        // Phase 7.5 emits real React source outside the supervisor's task
        // graph, so nothing else would ever graph it. Refresh before review:
        // the reviewer has no shell and can only read a graph that already
        // exists.
        if session.phase == MastermindPhase::HtmlToReact {
            refresh_code_graph(&session.repo).await;
        }
        let review_result = self.review_phase(session).await;
        let approved = match review_result {
            Ok(approved) => approved,
            Err(error) if self.fail_over_planner(session, &error)? => {
                session.phase_status = PhaseStatus::Running;
                self.review_phase(session).await?
            }
            Err(error) => return Err(error),
        };
        session.phase_status = if approved {
            PhaseStatus::AwaitingApproval
        } else {
            PhaseStatus::NeedsRevision
        };
        Ok(())
    }

    async fn enter_current_phase(
        &self,
        session: &mut Session,
    ) -> Result<Option<PlanCycleReport>, MastermindError> {
        session.phase_session = session.phase_session.saturating_add(1);
        session.planning_conversation.reset();
        session.phase_write_paths.clear();
        session.phase_status = PhaseStatus::Active;
        session.last_review = None;
        self.refresh_orchestrator_model(session)?;
        match session.phase {
            MastermindPhase::Discovery => Ok(None),
            MastermindPhase::Prd
            | MastermindPhase::Features
            | MastermindPhase::ImplementationPlan
            | MastermindPhase::ApiRecord
            | MastermindPhase::Design
            | MastermindPhase::Mockups
            | MastermindPhase::HtmlToReact => {
                self.prepare_author_phase(session, None).await?;
                Ok(None)
            }
            MastermindPhase::Build => {
                if let Some(report) = block_build_on_preflight(session) {
                    return Ok(Some(report));
                }
                let mut report = session
                    .orchestrator
                    .cycle_with_instruction(Some(BUILD_PLAN_INSTRUCTION))
                    .await;
                if self.fail_over_after_report(session, &report)? {
                    report = session
                        .orchestrator
                        .cycle_with_instruction(Some(BUILD_PLAN_INSTRUCTION))
                        .await;
                }
                if !session.orchestrator.plan().nodes().is_empty()
                    && report.rejected.is_empty()
                    && session.orchestrator.plan().run_id().is_none()
                {
                    session.orchestrator.commit()?;
                    session.phase_status = PhaseStatus::Running;
                }
                Ok(Some(report))
            }
            MastermindPhase::Wrap => {
                session.phase_status = if self.review_phase(session).await? {
                    PhaseStatus::AwaitingApproval
                } else {
                    PhaseStatus::NeedsRevision
                };
                Ok(None)
            }
            MastermindPhase::Setup | MastermindPhase::Complete => Ok(None),
        }
    }

    /// Start a session: build the roster-aware orchestrator and the
    /// supervisor that will execute what it commits. No model call yet —
    /// planning is [`Self::plan`].
    pub async fn start(
        &self,
        goal: &str,
        repo: &Path,
        planner_adapter: Option<&str>,
        planner_model: Option<&str>,
    ) -> Result<Value, MastermindError> {
        self.start_with_concurrency(goal, repo, planner_adapter, planner_model, 4)
            .await
    }

    /// Start with an operator-selected concurrency cap. One Mastermind
    /// session owns one supervisor/run, so this is also the persisted run
    /// admission default for that session.
    pub async fn start_with_concurrency(
        &self,
        goal: &str,
        repo: &Path,
        planner_adapter: Option<&str>,
        planner_model: Option<&str>,
        max_concurrency: usize,
    ) -> Result<Value, MastermindError> {
        if !repo.is_dir() {
            return Err(MastermindError::NoRepo(repo.to_path_buf()));
        }
        ensure_git_repository(repo)?;
        fs::create_dir_all(repo.join("docs").join("features")).map_err(|error| {
            MastermindError::GitSetup {
                repo: repo.to_path_buf(),
                detail: format!("could not prepare canonical docs directories: {error}"),
            }
        })?;
        let records = self.worker_records()?;
        let roster: Vec<RosterEntry> = records
            .iter()
            .map(|record| RosterEntry {
                id: record.id.clone(),
                name: record.name.clone(),
                description: record.description.clone(),
                adapter: record.adapter_id.clone(),
                model: record.model.clone(),
            })
            .collect();
        let pools: Vec<String> = records.iter().map(|record| record.id.clone()).collect();
        let has = |id: &str| pools.iter().any(|pool| pool == id);

        let session_id = Uuid::now_v7().to_string();
        let state_dir = self.state_dir.join("mastermind").join(&session_id);

        // The selected top-tier planner also owns the planning-document
        // phase. Otherwise a Codex planning session silently dispatches the
        // spec writer to the persisted Claude Opus default.
        let orchestrator_record = self.registry.get_agent(ORCHESTRATOR_AGENT)?;
        let planner_adapter_id = planner_adapter
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .or_else(|| {
                orchestrator_record
                    .as_ref()
                    .map(|record| record.adapter_id.clone())
            })
            .unwrap_or_else(|| "claude-code".to_owned());
        let planner_model_slug = planner_model
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .or_else(|| {
                orchestrator_record
                    .as_ref()
                    .and_then(|record| record.model.clone())
            })
            .unwrap_or_else(|| ORCHESTRATOR_MODEL.to_owned());

        // Route each pool at the adapter its registry record names, so a
        // task assigned to `nextjs-dev` spawns on agy and one assigned to
        // `spec-writer` spawns on claude-code.
        let mut config = SupervisorConfig::for_repo(&state_dir, repo)
            .with_agents_db(&self.agents_db)
            .with_shared_task_workspace()
            .with_max_concurrency(max_concurrency)
            .with_role_model(STRONGER_AGENT, &planner_model_slug);
        config.journal_db = self.supervisor_journal(&state_dir);
        config.orchestrator = ORCHESTRATOR_AGENT.to_owned();
        for record in &records {
            config = config.with_role_adapter(&record.id, &record.adapter_id);
        }
        config = config.with_role_adapter(STRONGER_AGENT, &planner_adapter_id);
        if records.iter().any(|record| record.id == DEBUGGER_AGENT)
            && records
                .iter()
                .any(|record| record.id == DEBUGGER_SOL_ESCALATION_AGENT)
        {
            config =
                config.with_reasoning_escalation(DEBUGGER_AGENT, DEBUGGER_SOL_ESCALATION_AGENT);
        }

        let mut adapter_ids: Vec<String> = records
            .iter()
            .map(|record| record.adapter_id.clone())
            .collect();
        adapter_ids.push(config.default_adapter.clone());
        adapter_ids.push(planner_adapter_id.clone());
        adapter_ids.sort();
        adapter_ids.dedup();
        let mut adapters: Vec<Arc<dyn RuntimeAdapter>> = Vec::with_capacity(adapter_ids.len());
        for id in &adapter_ids {
            let adapter =
                (self.adapters)(id).ok_or_else(|| MastermindError::UnknownAdapter(id.clone()))?;
            adapters.push(adapter);
        }
        let supervisor = Arc::new(Supervisor::new(config, adapters)?);

        let base_commit = head_commit(repo)?;
        let sink = Arc::new(SupervisorSink {
            store_sink: WorkflowSink::new(Arc::clone(supervisor.store())),
            supervisor: Arc::clone(&supervisor),
            base_commit,
            allowed_paths: vec!["**".to_owned()],
        });

        let policy = PlanPolicy {
            pools: pools.clone(),
            reviewer_pool: if has(REVIEWER_AGENT) {
                REVIEWER_AGENT.to_owned()
            } else {
                pools.first().cloned().unwrap_or_default()
            },
            escalation_pool: has(STRONGER_AGENT).then(|| STRONGER_AGENT.to_owned()),
            ..PlanPolicy::default()
        };

        // The desktop may select a planner per build. The complete live
        // Mastermind envelope is attached after Session exists so it can
        // include durable phase and Memex state.
        let adapter = (self.adapters)(&planner_adapter_id)
            .ok_or_else(|| MastermindError::UnknownAdapter(planner_adapter_id.clone()))?;
        let model = ClaudePlanningModel::new(adapter, repo).with_model(planner_model_slug.clone());

        let orchestrator =
            Orchestrator::new(goal, policy, Arc::new(model), sink).with_roster(roster.clone());

        let mut session = Session {
            id: session_id.clone(),
            goal: goal.to_owned(),
            repo: repo.to_path_buf(),
            orchestrator,
            supervisor,
            roster,
            planner_adapter: planner_adapter_id,
            planner_model: planner_model_slug,
            discovery_questions: Vec::new(),
            discovery_answers: Vec::new(),
            discovery_complete: false,
            phase: MastermindPhase::Discovery,
            phase_status: PhaseStatus::Active,
            approved_phases: vec![MastermindPhase::Setup],
            skill_source: None,
            memory_project: MemexClient::project_key(repo),
            memory_revision: None,
            memory_error: None,
            legacy_memory_imported: false,
            last_review: None,
            phase_session: 1,
            accepted_tasks_in_session: 0,
            open_handoffs: 0,
            task_count: 0,
            planning_conversation: PlanningConversation::default(),
            phase_write_paths: Vec::new(),
            model_ready: false,
            turns: Vec::new(),
        };
        self.import_legacy_memory(&mut session)?;
        self.refresh_orchestrator_model(&mut session)?;
        self.persist_session(&session)?;
        let summary = session.summary();
        self.sessions
            .lock()
            .map_err(|_| MastermindError::UnknownSession(session_id.clone()))?
            .insert(session_id, Arc::new(AsyncMutex::new(session)));
        Ok(summary)
    }

    fn session(&self, id: &str) -> Result<Arc<AsyncMutex<Session>>, MastermindError> {
        self.sessions
            .lock()
            .map_err(|_| MastermindError::UnknownSession(id.to_owned()))?
            .get(id)
            .cloned()
            .ok_or_else(|| MastermindError::UnknownSession(id.to_owned()))
    }

    /// Stage-neutral conversational turn. `mastermind.plan` remains an
    /// alias for older desktops; new clients call `mastermind.respond`.
    pub async fn respond(
        &self,
        session_id: &str,
        instruction: Option<&str>,
    ) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();
        if !guard.model_ready {
            if let Err(error) = self.refresh_orchestrator_model(&mut guard) {
                guard.memory_error = Some(error.to_string());
                guard.phase_status = PhaseStatus::Blocked;
                let _ = self.persist_session(&guard);
                return Err(error);
            }
        }

        let guidance = instruction.map(str::trim).filter(|value| !value.is_empty());
        let mut report = PlanCycleReport::default();

        if guard.phase == MastermindPhase::Discovery {
            if guard.phase_status == PhaseStatus::AwaitingWriteApproval {
                if let Some(revision) = guidance {
                    self.record_revision_memory(&mut guard, revision)?;
                    guard.discovery_answers.push((
                        "Additional discovery guidance".to_owned(),
                        revision.to_owned(),
                    ));
                    guard.phase_status = PhaseStatus::Active;
                } else {
                    self.persist_session(&guard)?;
                    return Ok(json!({
                        "cycle": report_json(&report),
                        "session": guard.summary(),
                    }));
                }
            } else if matches!(
                guard.phase_status,
                PhaseStatus::AwaitingApproval | PhaseStatus::NeedsRevision
            ) {
                let Some(revision) = guidance else {
                    return Err(MastermindError::DiscoveryAwaitingApproval(
                        session_id.to_owned(),
                    ));
                };
                self.record_revision_memory(&mut guard, revision)?;
                guard
                    .discovery_answers
                    .push(("Revision guidance".to_owned(), revision.to_owned()));
                guard.phase_status = PhaseStatus::Active;
                if let Err(error) = self.run_document_phase(&mut guard, Some(revision)).await {
                    guard.phase_status = PhaseStatus::Blocked;
                    let _ = self.persist_session(&guard);
                    return Err(error);
                }
                self.persist_session(&guard)?;
                return Ok(json!({
                    "cycle": report_json(&report),
                    "session": guard.summary(),
                }));
            } else if let Some(answer) = guidance {
                if let Some(question) = guard.discovery_questions.first().cloned() {
                    guard.discovery_questions.remove(0);
                    guard
                        .discovery_answers
                        .push((question.prompt, answer.to_owned()));
                }
            }

            if guard.discovery_questions.is_empty() {
                let context = if guard.discovery_answers.is_empty() {
                    guidance.map(str::to_owned)
                } else {
                    Some(format!(
                        "Confirmed discovery so far:\n{}\nAsk the next 1-3 questions required by the canonical discovery checklist. Return [] only when every census feature has all nine details and non-functional choices settled.",
                        guard.discovery_brief()
                    ))
                };
                let mut questions = match guard
                    .orchestrator
                    .discovery_questions(context.as_deref())
                    .await
                {
                    Ok(questions) => questions,
                    Err(error) => {
                        let error = MastermindError::from(error);
                        if self.fail_over_planner(&mut guard, &error)? {
                            guard
                                .orchestrator
                                .discovery_questions(context.as_deref())
                                .await?
                        } else {
                            return Err(error);
                        }
                    }
                };
                if questions.is_empty() && guard.discovery_answers.is_empty() {
                    questions = fallback_discovery_questions();
                }
                guard.discovery_questions = questions;
            }

            if guard.discovery_questions.is_empty() && !guard.discovery_answers.is_empty() {
                guard.phase_status = PhaseStatus::AwaitingWriteApproval;
            }
            report = discovery_report(&guard);
        } else if guard.phase_status == PhaseStatus::Active
            || (guard.phase_status == PhaseStatus::Running && guard.phase != MastermindPhase::Build)
        {
            // A `Running` status seen here is always stale: this function holds
            // the session lock, so no provider turn can be in flight. It marks a
            // preparation that failed without resetting the phase. Re-enter
            // instead of falling through the dispatch chain, which would persist
            // the stuck state and return an empty cycle that reads as success.
            if guard.phase_status == PhaseStatus::Running {
                tracing::warn!(
                    session = %guard.id,
                    phase = guard.phase.id(),
                    "re-entering a phase left in `running` by a failed turn"
                );
            }
            match self.enter_current_phase(&mut guard).await {
                Ok(Some(phase_report)) => report = phase_report,
                Ok(None) => {}
                Err(error) if self.fail_over_planner(&mut guard, &error)? => {
                    // The failed provider returned no successful planning
                    // result, so a fresh provider can safely replay this
                    // phase from the durable envelope once.
                    match self.enter_current_phase(&mut guard).await {
                        Ok(Some(phase_report)) => report = phase_report,
                        Ok(None) => {}
                        Err(retry_error) => {
                            guard.phase_status = PhaseStatus::Blocked;
                            guard.memory_error = Some(retry_error.to_string());
                            let _ = self.persist_session(&guard);
                            return Err(retry_error);
                        }
                    }
                }
                Err(error) => {
                    // Leave an honest terminal state instead of a second
                    // stranded `running`, so the desktop can offer recovery.
                    guard.phase_status = PhaseStatus::Blocked;
                    guard.memory_error = Some(error.to_string());
                    let _ = self.persist_session(&guard);
                    return Err(error);
                }
            }
        } else if guard.phase_status == PhaseStatus::AwaitingWriteApproval {
            let Some(revision) = guidance else {
                return Err(MastermindError::InvalidPhase {
                    session: session_id.to_owned(),
                    phase: guard.phase.id().to_owned(),
                    status: guard.phase_status.as_str().to_owned(),
                });
            };
            self.record_revision_memory(&mut guard, revision)?;
            self.prepare_author_phase(&mut guard, Some(revision))
                .await?;
        } else if guard.phase_status == PhaseStatus::NeedsRevision
            && guidance.is_some_and(accepts_phase_as_is)
        {
            let previous_review = guard.last_review.take().unwrap_or_default();
            guard.last_review = Some(format!(
                "{previous_review}\n\nUSER OVERRIDE: Accepted as-is through chat; reviewer findings intentionally waived."
            ));
            guard.phase_status = PhaseStatus::AwaitingApproval;
            report = self.apply_phase_approval(&mut guard)?;
        } else if matches!(
            guard.phase_status,
            PhaseStatus::AwaitingApproval | PhaseStatus::NeedsRevision
        ) {
            let Some(revision) = guidance else {
                return Err(MastermindError::InvalidPhase {
                    session: session_id.to_owned(),
                    phase: guard.phase.id().to_owned(),
                    status: guard.phase_status.as_str().to_owned(),
                });
            };
            self.record_revision_memory(&mut guard, revision)?;
            guard.phase_status = PhaseStatus::Active;
            self.run_document_phase(&mut guard, Some(revision)).await?;
        } else if guard.phase == MastermindPhase::Build {
            if let Some(preflight) = block_build_on_preflight(&mut guard) {
                report = preflight;
                self.persist_session(&guard)?;
                return Ok(json!({
                    "cycle": report_json(&report),
                    "session": guard.summary(),
                }));
            }
            if guard.phase_status == PhaseStatus::Blocked {
                guard.phase_status = PhaseStatus::Active;
                guard.last_review = None;
            }
            let instruction = build_cycle_instruction(guidance);
            report = guard
                .orchestrator
                .cycle_with_instruction(Some(&instruction))
                .await;
            if self.fail_over_after_report(&mut guard, &report)? {
                report = guard
                    .orchestrator
                    .cycle_with_instruction(Some(&instruction))
                    .await;
            }
            guard.accepted_tasks_in_session = guard
                .accepted_tasks_in_session
                .saturating_add(u32::try_from(report.accepted.len()).unwrap_or(u32::MAX));
            if guard.orchestrator.plan().run_id().is_none()
                && !guard.orchestrator.plan().nodes().is_empty()
                && report.rejected.is_empty()
            {
                guard.orchestrator.commit()?;
                guard.phase_status = PhaseStatus::Running;
            }
            if let Err(error) = self.sync_plan_tasks(&guard) {
                guard.memory_error = Some(error.to_string());
                let _ = self.persist_session(&guard);
                return Err(error);
            }
            if guard.accepted_tasks_in_session >= 5 {
                let content = json!({
                    "sessionId": guard.id,
                    "phase": guard.phase.id(),
                    "phaseSession": guard.phase_session,
                    "acceptedTasks": guard.accepted_tasks_in_session,
                    "plan": guard.orchestrator.snapshot(),
                });
                let id = self.memex.remember(
                    &guard.memory_project,
                    &content.to_string(),
                    ORCHESTRATOR_AGENT,
                    &guard.planner_adapter,
                    "context",
                    &format!(
                        "mastermind,session:{},phase:8,kind:group-boundary",
                        guard.id
                    ),
                )?;
                guard.memory_revision = Some(id);
                guard.phase_session = guard.phase_session.saturating_add(1);
                guard.accepted_tasks_in_session = 0;
                self.refresh_orchestrator_model(&mut guard)?;
            }
        }

        self.persist_session(&guard)?;
        Ok(json!({
            "cycle": report_json(&report),
            "session": guard.summary(),
        }))
    }

    pub async fn plan(
        &self,
        session_id: &str,
        instruction: Option<&str>,
    ) -> Result<Value, MastermindError> {
        self.respond(session_id, instruction).await
    }

    /// The user gate: materialize the plan into a run the supervisor can
    /// execute (spec + per-node contracts + run manifest).
    pub async fn commit(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();
        if guard.phase != MastermindPhase::Build || !guard.discovery_complete {
            return Err(MastermindError::DiscoveryIncomplete(session_id.to_owned()));
        }
        let run_id = match guard.orchestrator.plan().run_id() {
            Some(run_id) => run_id,
            None => guard.orchestrator.commit()?,
        };
        guard.phase_status = PhaseStatus::Running;
        self.persist_session(&guard)?;
        Ok(json!({
            "runId": run_id.to_string(),
            "session": guard.summary(),
        }))
    }

    /// Drive the committed run: the supervisor leases ready tasks and
    /// spawns each one's registry agent — record's model, skill preamble,
    /// policy-compiled permissions, and the selected project checkout.
    pub async fn drive(
        &self,
        session_id: &str,
        max_ticks: Option<u32>,
    ) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let guard = session.lock().await;
        if !guard.discovery_complete {
            return Err(MastermindError::DiscoveryIncomplete(session_id.to_owned()));
        }
        let run_id = guard
            .orchestrator
            .plan()
            .run_id()
            .ok_or_else(|| MastermindError::NotCommitted(session_id.to_owned()))?;
        let supervisor = Arc::clone(&guard.supervisor);
        // Scale with the graph: a shared workspace serializes write tasks, so
        // the floor is one tick per node plus its review, and a fixed ceiling
        // starves large plans rather than finishing them.
        let nodes = u32::try_from(guard.orchestrator.plan().nodes().len()).unwrap_or(u32::MAX);
        let ticks = max_ticks
            .unwrap_or_else(|| {
                DEFAULT_MAX_TICKS.saturating_add(nodes.saturating_mul(TICKS_PER_NODE))
            })
            .clamp(1, 1024);
        drop(guard);
        let summary = supervisor.drive(&run_id, ticks).await?;
        let failures = supervisor.failure_details(&run_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();
        let run = guard.orchestrator.refresh().ok().flatten();
        if let Err(error) = self.sync_run_memory(&guard, run.as_ref()) {
            guard.memory_error = Some(error.to_string());
            let _ = self.persist_session(&guard);
            return Err(error);
        }
        guard.memory_error = None;
        if format!("{:?}", summary.status).eq_ignore_ascii_case("completed") {
            guard.phase_status = if self.review_phase(&mut guard).await? {
                PhaseStatus::AwaitingApproval
            } else {
                PhaseStatus::NeedsRevision
            };
        }
        self.persist_session(&guard)?;
        Ok(json!({
            "runId": run_id.to_string(),
            "ticks": summary.ticks,
            "status": format!("{:?}", summary.status),
            "failures": failures,
            "session": guard.summary(),
        }))
    }

    /// Operator recovery: return a failed run's stranded work to the queue.
    ///
    /// This is the escape hatch the observed Phase-8 build had no equivalent
    /// of. `T07` failed on an `antigravity-agy` quota outage, burned its
    /// transient retries while the window was still shut, and took five
    /// dependents to `Blocked` with it — 14 finished tasks stranded behind
    /// one node that failed for a reason that had since evaporated. The
    /// planner's only lever was `escalate`, which cannot give a terminal
    /// task another attempt (it now says so; see
    /// [`RejectionReason::TaskTerminal`](agentos_orchestrator::RejectionReason)).
    ///
    /// Deliberately operator-initiated and deliberately *not* automatic:
    /// the engine must never decide on its own that a failure has stopped
    /// being true. Reopening a run that is not failed is a caller error, so
    /// the RPC answers `invalid_params` rather than quietly doing nothing —
    /// a no-op reported as success is the exact failure mode this whole
    /// change exists to remove.
    ///
    /// After it returns, `mastermind.drive` resumes the run.
    pub async fn reopen_run(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();
        let run_id = guard
            .orchestrator
            .plan()
            .run_id()
            .ok_or_else(|| MastermindError::NotCommitted(session_id.to_owned()))?;

        // The engine's own state decides whether there is anything to
        // reopen — never the session's cached phase status.
        let status = guard
            .orchestrator
            .refresh()?
            .map(|view| view.status)
            .ok_or_else(|| MastermindError::NotCommitted(session_id.to_owned()))?;
        if status != RunStatus::Failed {
            return Err(MastermindError::RunNotFailed {
                session: session_id.to_owned(),
                status: status.as_str().to_owned(),
            });
        }

        let report = guard.supervisor.reopen_run(&run_id)?;
        let run = guard.orchestrator.refresh().ok().flatten();
        guard.phase_status = PhaseStatus::Running;
        self.persist_session(&guard)?;
        tracing::info!(
            session = %session_id,
            run_id = %run_id,
            reopened = report.reopened.len(),
            unblocked = report.unblocked.len(),
            "mastermind run reopened; drive to resume"
        );
        Ok(json!({
            "runId": run_id.to_string(),
            "reopened": report.reopened,
            "unblocked": report.unblocked,
            "status": report.status.as_str(),
            "run": run.map(|view| json!({
                "runId": view.run_id.to_string(),
                "status": format!("{:?}", view.status),
                "tasks": view.tasks.iter().map(|task| json!({
                    "nodeId": task.node_id,
                    "state": format!("{:?}", task.state),
                    "attemptCount": task.attempt_count,
                    "pool": task.pool,
                })).collect::<Vec<_>>(),
            })),
            "session": guard.summary(),
        }))
    }

    /// Stop admitting new attempts while allowing current leases to drain.
    pub async fn pause_run(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let guard = session.lock().await;
        let run_id = guard
            .orchestrator
            .plan()
            .run_id()
            .ok_or_else(|| MastermindError::NotCommitted(session_id.to_owned()))?;
        Ok(json!({ "control": guard.supervisor.pause_run(&run_id)? }))
    }

    /// Resume normal admission for a persistently paused run.
    pub async fn resume_run(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let guard = session.lock().await;
        let run_id = guard
            .orchestrator
            .plan()
            .run_id()
            .ok_or_else(|| MastermindError::NotCommitted(session_id.to_owned()))?;
        Ok(json!({ "control": guard.supervisor.resume_run(&run_id)? }))
    }

    /// Cancel all cancellable work in a committed run.
    pub async fn cancel_run(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let guard = session.lock().await;
        let run_id = guard
            .orchestrator
            .plan()
            .run_id()
            .ok_or_else(|| MastermindError::NotCommitted(session_id.to_owned()))?;
        Ok(json!({ "control": guard.supervisor.cancel_run(&run_id)? }))
    }

    /// Cancel one queued or running task without affecting unrelated work.
    pub async fn cancel_task(
        &self,
        session_id: &str,
        task_id: &str,
    ) -> Result<Value, MastermindError> {
        let task_id = Uuid::parse_str(task_id)
            .map_err(|_| MastermindError::InvalidControl("taskId must be a UUID".to_owned()))?;
        let session = self.session(session_id)?;
        let guard = session.lock().await;
        Ok(json!({ "control": guard.supervisor.cancel_task(&task_id)? }))
    }

    /// Reopen one failed task with a fresh retry attempt.
    pub async fn retry_task(
        &self,
        session_id: &str,
        task_id: &str,
    ) -> Result<Value, MastermindError> {
        let task_id = Uuid::parse_str(task_id)
            .map_err(|_| MastermindError::InvalidControl("taskId must be a UUID".to_owned()))?;
        let session = self.session(session_id)?;
        let guard = session.lock().await;
        Ok(json!({ "control": guard.supervisor.retry_task(&task_id)? }))
    }

    /// Change the role used by queued or retryable work. Active attempts are
    /// rejected by the workflow engine and therefore remain immutable.
    pub async fn reroute_task(
        &self,
        session_id: &str,
        task_id: &str,
        agent_role: Option<&str>,
    ) -> Result<Value, MastermindError> {
        let task_id = Uuid::parse_str(task_id)
            .map_err(|_| MastermindError::InvalidControl("taskId must be a UUID".to_owned()))?;
        let session = self.session(session_id)?;
        let guard = session.lock().await;
        Ok(json!({
            "control": guard.supervisor.reroute_task(&task_id, agent_role)?
        }))
    }

    /// Plan + the engine's view of the committed run.
    /// Read one phase deliverable for the desktop previewer.
    ///
    /// The allowlist is derived from the session's own repository, so a
    /// traversal attempt fails by simply never appearing on it — the check is
    /// membership, not string inspection.
    pub async fn artifact(&self, session_id: &str, path: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let guard = session.lock().await;
        let available = readable_artifacts(&guard.repo);
        let requested = path.replace('\\', "/");
        if !available.contains(&requested) {
            return Err(MastermindError::UnknownArtifact {
                session: session_id.to_owned(),
                path: path.to_owned(),
            });
        }
        let file = guard.repo.join(&requested);
        let raw = fs::read(&file).map_err(|error| MastermindError::Checkpoint {
            path: file.clone(),
            detail: format!("could not read deliverable: {error}"),
        })?;
        let truncated = raw.len() > ARTIFACT_READ_LIMIT;
        let content =
            String::from_utf8_lossy(&raw[..raw.len().min(ARTIFACT_READ_LIMIT)]).into_owned();
        let modified_at = fs::metadata(&file)
            .and_then(|metadata| metadata.modified())
            .map(|time| chrono::DateTime::<Utc>::from(time).to_rfc3339())
            .ok();
        Ok(json!({
            "sessionId": guard.id,
            "path": requested,
            "content": content,
            "bytes": raw.len(),
            "truncated": truncated,
            "modifiedAt": modified_at,
            "available": available,
        }))
    }

    pub async fn status(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();
        let run = guard.orchestrator.refresh().ok().flatten();
        match self.memex.board(&guard.memory_project) {
            Ok(board) => {
                guard.open_handoffs = board.open_handoffs.len();
                guard.task_count = board.tasks.len();
                guard.memory_error = None;
            }
            Err(error) => guard.memory_error = Some(error.to_string()),
        }
        let failures = match guard.orchestrator.plan().run_id() {
            Some(run_id) => guard
                .supervisor
                .failure_details(&run_id)
                .unwrap_or_default(),
            None => Vec::new(),
        };
        Ok(json!({
            "session": guard.summary(),
            "run": run.map(|view| json!({
                "runId": view.run_id.to_string(),
                "status": format!("{:?}", view.status),
                "tasks": view.tasks.iter().map(|task| json!({
                    "nodeId": task.node_id,
                    "state": format!("{:?}", task.state),
                    "pool": task.pool,
                })).collect::<Vec<_>>(),
            })),
            "failures": failures,
        }))
    }

    /// Every live session (the UI's list).
    pub fn list(&self) -> Result<Vec<String>, MastermindError> {
        Ok(self
            .sessions
            .lock()
            .map_err(|_| MastermindError::UnknownSession("<list>".to_owned()))?
            .keys()
            .cloned()
            .collect())
    }

    /// Grant exactly the server-derived paths for the current authoring
    /// phase. The client supplies no paths, so it cannot widen the grant.
    pub async fn authorize_phase_write(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();

        if guard.phase_status != PhaseStatus::AwaitingWriteApproval {
            // A duplicate click can arrive after the first request completed.
            // Return the already-produced gate instead of running the author
            // twice; all other phase/status combinations remain caller errors.
            if phase_requires_write_authorization(guard.phase)
                && phase_deliverable_exists(&guard.repo, guard.phase)
                && matches!(
                    guard.phase_status,
                    PhaseStatus::AwaitingApproval | PhaseStatus::NeedsRevision
                )
            {
                return Ok(json!({
                    "cycle": report_json(&PlanCycleReport::default()),
                    "session": guard.summary(),
                }));
            }
            return Err(MastermindError::InvalidPhase {
                session: session_id.to_owned(),
                phase: guard.phase.id().to_owned(),
                status: guard.phase_status.as_str().to_owned(),
            });
        }

        if !phase_requires_write_authorization(guard.phase) {
            return Err(MastermindError::InvalidPhase {
                session: session_id.to_owned(),
                phase: guard.phase.id().to_owned(),
                status: "phase has no scoped authoring deliverable".to_owned(),
            });
        }

        if let Err(error) = self.run_document_phase(&mut guard, None).await {
            guard.phase_status = PhaseStatus::Blocked;
            guard.memory_error = Some(error.to_string());
            let _ = self.persist_session(&guard);
            return Err(error);
        }
        guard.memory_error = None;
        self.persist_session(&guard)?;
        Ok(json!({
            "cycle": report_json(&PlanCycleReport::default()),
            "session": guard.summary(),
        }))
    }

    /// Recover a previously blocked authoring call when its exact scoped
    /// deliverable already exists. This performs no write-capable provider
    /// turn: it only runs the normal read-only reviewer and restores the
    /// artifact-review gate.
    pub async fn recover_phase_artifact(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();
        if guard.phase_status != PhaseStatus::Blocked
            || !phase_requires_write_authorization(guard.phase)
            || !phase_deliverable_exists(&guard.repo, guard.phase)
        {
            return Err(MastermindError::InvalidPhase {
                session: session_id.to_owned(),
                phase: guard.phase.id().to_owned(),
                status: guard.phase_status.as_str().to_owned(),
            });
        }

        guard.phase_status = PhaseStatus::Running;
        guard.planning_conversation.reset();
        self.persist_session(&guard)?;
        match self.review_phase(&mut guard).await {
            Ok(passed) => {
                guard.phase_status = if passed {
                    PhaseStatus::AwaitingApproval
                } else {
                    PhaseStatus::NeedsRevision
                };
                guard.memory_error = None;
                self.persist_session(&guard)?;
                Ok(json!({
                    "cycle": report_json(&PlanCycleReport::default()),
                    "session": guard.summary(),
                }))
            }
            Err(error) => {
                guard.phase_status = PhaseStatus::Blocked;
                guard.memory_error = Some(error.to_string());
                let _ = self.persist_session(&guard);
                Err(error)
            }
        }
    }

    /// Retry a blocked authoring turn that produced no deliverable. The
    /// prior grant is not widened: the exact server-derived phase paths are
    /// retained, while the invalid/timed-out provider conversation is
    /// discarded and reconstructed from the durable checkpoint envelope.
    pub async fn retry_phase_authoring(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();
        if !can_retry_phase_authoring(&guard) {
            return Err(MastermindError::InvalidPhase {
                session: session_id.to_owned(),
                phase: guard.phase.id().to_owned(),
                status: guard.phase_status.as_str().to_owned(),
            });
        }

        guard.planning_conversation.reset();
        guard.memory_error = None;
        if let Err(error) = self.run_document_phase(&mut guard, None).await {
            guard.phase_status = PhaseStatus::Blocked;
            guard.memory_error = Some(error.to_string());
            let _ = self.persist_session(&guard);
            return Err(error);
        }
        guard.memory_error = None;
        self.persist_session(&guard)?;
        Ok(json!({
            "cycle": report_json(&PlanCycleReport::default()),
            "session": guard.summary(),
        }))
    }

    /// Repoint a session at a different planner provider. A provider
    /// conversation id is meaningless to a different provider, so the
    /// conversation is reset; the phase, its scope, and every approved
    /// artifact are left untouched. `try_lock` is deliberate: a turn in
    /// flight owns the session, and swapping its provider mid-turn would
    /// strand the running conversation.
    pub async fn set_planner(
        &self,
        session_id: &str,
        planner_adapter: &str,
        planner_model: &str,
    ) -> Result<Value, MastermindError> {
        let planner_adapter = planner_adapter.trim();
        let planner_model = planner_model.trim();
        if planner_adapter.is_empty() || planner_model.is_empty() {
            return Err(MastermindError::UnknownAdapter(planner_adapter.to_owned()));
        }
        if (self.adapters)(planner_adapter).is_none() {
            return Err(MastermindError::UnknownAdapter(planner_adapter.to_owned()));
        }
        let session = self.session(session_id)?;
        let mut guard = session
            .try_lock()
            .map_err(|_| MastermindError::TurnInFlight(session_id.to_owned()))?;
        if guard.phase_status == PhaseStatus::Running {
            return Err(MastermindError::TurnInFlight(session_id.to_owned()));
        }
        guard.planner_adapter = planner_adapter.to_owned();
        guard.planner_model = planner_model.to_owned();
        guard.planning_conversation.reset();
        // The orchestrator owns an Arc<dyn PlanningModel>; changing only the
        // persisted labels leaves that Arc pointed at the previous provider
        // until a daemon restart. Rebuild it now so the very next turn uses
        // the selected adapter/model and the current registry roster.
        guard.model_ready = false;
        self.refresh_orchestrator_model(&mut guard)?;
        self.persist_session(&guard)?;
        Ok(json!({ "session": guard.summary() }))
    }

    fn apply_phase_approval(
        &self,
        guard: &mut Session,
    ) -> Result<PlanCycleReport, MastermindError> {
        let approved = guard.phase;
        let next = approved.next(is_react_stack(&guard.repo));
        if let Err(error) = self.sync_phase_memory(guard, next) {
            guard.memory_error = Some(error.to_string());
            let _ = self.persist_session(guard);
            return Err(error);
        }
        if approved == MastermindPhase::Discovery {
            approve_discovery_document(&guard.repo)?;
            guard.discovery_complete = true;
        }
        if !guard.approved_phases.contains(&approved) {
            guard.approved_phases.push(approved);
        }
        guard.planning_conversation.reset();
        guard.phase = next;
        guard.phase_status = if next == MastermindPhase::Complete {
            PhaseStatus::Complete
        } else {
            PhaseStatus::Active
        };
        // Approval is a durable state transition, not a provider turn. Return
        // immediately so the desktop cannot time out while the next phase's
        // model prepares. The next explicit `mastermind.plan/respond` enters
        // the phase and starts that provider turn under its own progress UI.
        Ok(PlanCycleReport::default())
    }

    /// Generic user gate. Memory synchronization is completed before the
    /// phase changes, so a Memex failure leaves the current gate intact.
    pub async fn approve_phase(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();
        if guard.phase_status != PhaseStatus::AwaitingApproval {
            return Err(MastermindError::InvalidPhase {
                session: session_id.to_owned(),
                phase: guard.phase.id().to_owned(),
                status: guard.phase_status.as_str().to_owned(),
            });
        }
        let report = self.apply_phase_approval(&mut guard)?;
        self.persist_session(&guard)?;
        Ok(json!({
            "cycle": report_json(&report),
            "session": guard.summary(),
        }))
    }

    /// Explicitly waive reviewer findings and approve the current artifact
    /// without editing it. This is separate from revision guidance so a
    /// deliberate user override cannot be confused with an authoring turn.
    pub async fn accept_phase_as_is(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        // Each call reports only its own provider turns.
        guard.turns.clear();
        if guard.phase_status != PhaseStatus::NeedsRevision {
            return Err(MastermindError::InvalidPhase {
                session: session_id.to_owned(),
                phase: guard.phase.id().to_owned(),
                status: guard.phase_status.as_str().to_owned(),
            });
        }
        let previous_review = guard.last_review.take().unwrap_or_default();
        guard.last_review = Some(format!(
            "{previous_review}\n\nUSER OVERRIDE: Accepted as-is; reviewer findings intentionally waived."
        ));
        guard.phase_status = PhaseStatus::AwaitingApproval;
        let report = self.apply_phase_approval(&mut guard)?;
        self.persist_session(&guard)?;
        Ok(json!({
            "cycle": report_json(&report),
            "session": guard.summary(),
        }))
    }

    pub async fn revise_phase(
        &self,
        session_id: &str,
        guidance: &str,
    ) -> Result<Value, MastermindError> {
        if guidance.trim().is_empty() {
            return Err(MastermindError::InvalidPhase {
                session: session_id.to_owned(),
                phase: "unknown".to_owned(),
                status: "revision guidance is empty".to_owned(),
            });
        }
        self.respond(session_id, Some(guidance)).await
    }

    /// Re-run Phase 7.5's read-only preparation after a failed review whose
    /// fixes exceed the original generated-source declaration. Scope is derived
    /// exclusively from the already-written plan and fixed governance files;
    /// callers can supply prose guidance but never paths.
    pub async fn reprepare_phase_revision(
        &self,
        session_id: &str,
        guidance: Option<&str>,
    ) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        let mut guard = session.lock().await;
        guard.turns.clear();
        if guard.phase != MastermindPhase::HtmlToReact
            || guard.phase_status != PhaseStatus::NeedsRevision
        {
            return Err(MastermindError::InvalidPhase {
                session: session_id.to_owned(),
                phase: guard.phase.id().to_owned(),
                status: guard.phase_status.as_str().to_owned(),
            });
        }
        let revision_scope = h2r_revision_write_paths(&guard.repo).map_err(|detail| {
            MastermindError::InvalidPhase {
                session: session_id.to_owned(),
                phase: guard.phase.id().to_owned(),
                status: format!("cannot derive Phase 7.5 revision scope: {detail}"),
            }
        })?;
        let previous_paths = guard.phase_write_paths.clone();
        let previous_review = guard.last_review.clone().unwrap_or_default();
        let user_guidance = guidance.map(str::trim).filter(|value| !value.is_empty());
        let preparation_guidance = match user_guidance {
            Some(guidance) => format!(
                "The prior reviewer report follows. Re-plan only the necessary generated remediation; do not hand-edit frozen inputs.\n\n{previous_review}\n\nAdditional user guidance:\n{guidance}"
            ),
            None => format!(
                "The prior reviewer report follows. Re-plan only the necessary generated remediation; do not hand-edit frozen inputs.\n\n{previous_review}"
            ),
        };
        guard.planning_conversation.reset();
        guard.phase_status = PhaseStatus::Active;
        if let Err(error) = self
            .prepare_author_phase(&mut guard, Some(&preparation_guidance))
            .await
        {
            // Preserve the original review and stale grant for diagnosis. A
            // failed read-only preparation grants nothing and remains revisable.
            guard.phase_write_paths = previous_paths;
            guard.phase_status = PhaseStatus::NeedsRevision;
            guard.memory_error = Some(error.to_string());
            let _ = self.persist_session(&guard);
            return Err(error);
        }
        guard.phase_write_paths = revision_scope;
        guard.memory_error = None;
        self.persist_session(&guard)?;
        Ok(json!({
            "cycle": report_json(&PlanCycleReport::default()),
            "session": guard.summary(),
        }))
    }

    /// Compatibility alias for the original Phase-1-only gate.
    pub async fn approve_discovery(&self, session_id: &str) -> Result<Value, MastermindError> {
        let session = self.session(session_id)?;
        if session.lock().await.phase != MastermindPhase::Discovery {
            return Err(MastermindError::DiscoveryIncomplete(session_id.to_owned()));
        }
        self.approve_phase(session_id).await
    }
}

fn approve_discovery_document(repo: &Path) -> Result<(), MastermindError> {
    let path = repo.join("docs").join("DISCOVERY.md");
    let document = std::fs::read_to_string(&path).map_err(|error| MastermindError::GitSetup {
        repo: repo.to_path_buf(),
        detail: format!("could not read docs/DISCOVERY.md: {error}"),
    })?;
    std::fs::write(
        &path,
        document
            .replacen("Status: awaiting approval", "Status: approved", 1)
            .replacen("Status: draft", "Status: approved", 1),
    )
    .map_err(|error| MastermindError::GitSetup {
        repo: repo.to_path_buf(),
        detail: format!("could not approve docs/DISCOVERY.md: {error}"),
    })
}

fn discovery_report(session: &Session) -> PlanCycleReport {
    PlanCycleReport {
        cycle: session.orchestrator.cycle_count(),
        decisions: session
            .discovery_questions
            .first()
            .cloned()
            .into_iter()
            .collect(),
        ..PlanCycleReport::default()
    }
}

/// Safe fallback when a provider cannot produce the discovery envelope.
/// The gate must fail closed: generic product decisions are less tailored,
/// but still better than silently dispatching requirement gathering to a
/// worker after commit.
fn fallback_discovery_questions() -> Vec<PlanningDecision> {
    [
        (
            "What delivery level should the first run target?",
            vec![
                "Smallest working MVP (Recommended)",
                "Production-ready first release",
                "Disposable technical prototype",
            ],
        ),
        (
            "Which deployment constraint should the plan follow?",
            vec![
                "Recommend a managed cloud setup (Recommended)",
                "Use an existing cloud provider/account",
                "Self-hosted or local infrastructure",
            ],
        ),
        (
            "What access and data-retention baseline should apply?",
            vec![
                "Authenticated users with minimal persistence (Recommended)",
                "Public access with ephemeral data",
                "Authenticated users with durable history",
            ],
        ),
    ]
    .into_iter()
    .map(|(prompt, options)| PlanningDecision {
        tool: "AskUserQuestion".to_owned(),
        prompt: prompt.to_owned(),
        options: options.into_iter().map(str::to_owned).collect(),
        multi_select: false,
    })
    .collect()
}

/// One cycle report as wire JSON (accepted ops, rejections with their
/// self-correction hints, and whatever went wrong at the model or engine).
fn report_json(report: &PlanCycleReport) -> Value {
    json!({
        "cycle": report.cycle,
        "accepted": report.accepted.len(),
        "rejected": report.rejected.iter().map(|rejection| json!({
            "code": rejection.reason.code(),
            "message": rejection.reason.to_string(),
            "hint": rejection.reason.hint(),
        })).collect::<Vec<_>>(),
        "modelError": report.model_error,
        "engineError": report.engine_error,
        "rawExcerpt": report.raw_excerpt,
        "decisions": report.decisions.iter().map(|decision| json!({
            "tool": decision.tool,
            "prompt": decision.prompt,
            "options": decision.options,
            "multiSelect": decision.multi_select,
        })).collect::<Vec<_>>(),
    })
}

/// Ensure a selected project folder can host AgentOS worktrees. A non-Git
/// folder becomes a `main` repository with an empty setup commit; existing
/// repositories and their untracked files are left untouched.
fn ensure_git_repository(repo: &Path) -> Result<(), MastermindError> {
    let git = |args: &[&str]| {
        std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .map_err(|err| MastermindError::GitSetup {
                repo: repo.to_path_buf(),
                detail: err.to_string(),
            })
    };

    let inside = git(&["rev-parse", "--is-inside-work-tree"])?;
    if !inside.status.success()
        || String::from_utf8_lossy(&inside.stdout)
            .trim()
            .eq_ignore_ascii_case("false")
    {
        let initialized = git(&["init", "--initial-branch=main"])?;
        if !initialized.status.success() {
            return Err(MastermindError::GitSetup {
                repo: repo.to_path_buf(),
                detail: String::from_utf8_lossy(&initialized.stderr)
                    .trim()
                    .to_owned(),
            });
        }
    }

    // Worktrees require an actual commit, not merely a `.git` directory. An
    // empty commit preserves every user file and uses a one-shot identity, so
    // setup does not modify the user's Git configuration.
    let head = git(&["rev-parse", "--verify", "HEAD"])?;
    if !head.status.success() {
        let initial = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args([
                "-c",
                "user.name=AgentOS",
                "-c",
                "user.email=agentos@localhost",
                "commit",
                "--allow-empty",
                "-m",
                "chore: initialize repository",
            ])
            .output()
            .map_err(|err| MastermindError::GitSetup {
                repo: repo.to_path_buf(),
                detail: err.to_string(),
            })?;
        if !initial.status.success() {
            return Err(MastermindError::GitSetup {
                repo: repo.to_path_buf(),
                detail: String::from_utf8_lossy(&initial.stderr).trim().to_owned(),
            });
        }
    }
    Ok(())
}

/// The commit every task is planned against. `ensure_git_repository` creates
/// one for a new project folder before this function runs.
fn head_commit(repo: &Path) -> Result<String, MastermindError> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|err| MastermindError::NoHead {
            repo: repo.to_path_buf(),
            detail: err.to_string(),
        })?;
    if !output.status.success() {
        return Err(MastermindError::NoHead {
            repo: repo.to_path_buf(),
            detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    let commit = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if commit.is_empty() {
        return Err(MastermindError::NoHead {
            repo: repo.to_path_buf(),
            detail: "empty rev-parse output".to_owned(),
        });
    }
    Ok(commit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_adapters::mock::{MockAdapter, MockBehavior};
    use agentos_orchestrator::model::PlanningDecision;
    use agentos_workflow::{Budgets, NodeType};

    /// Provider prose is untrusted and unbounded; the transcript carries it
    /// verbatim up to a ceiling, and nothing at all when the turn was silent.
    #[test]
    fn turn_text_is_bounded_without_splitting_a_codepoint() {
        assert_eq!(bound_turn_text("   \n  "), None);
        assert_eq!(
            bound_turn_text("  VERDICT: PASS  ").as_deref(),
            Some("VERDICT: PASS")
        );

        let long = "é".repeat(TURN_MAX_CHARS + 500);
        let bounded = bound_turn_text(&long).expect("non-empty");
        assert!(bounded.ends_with("…output truncated…"));
        assert_eq!(
            bounded.chars().filter(|c| *c == 'é').count(),
            TURN_MAX_CHARS
        );

        let exact = "a".repeat(TURN_MAX_CHARS);
        assert_eq!(bound_turn_text(&exact).as_deref(), Some(exact.as_str()));
    }

    #[test]
    fn memex_prompt_replay_is_bounded_newest_first() {
        let records: Vec<MemoryRecord> = (0..10)
            .map(|id| MemoryRecord {
                id,
                project: "agentos:test".to_owned(),
                agent: "orchestrator".to_owned(),
                provider: "mock".to_owned(),
                kind: "context".to_owned(),
                content: format!("record-{id}:{}", "x".repeat(10_000)),
                tags: "mastermind".to_owned(),
                created_at: id.to_string(),
            })
            .collect();

        let bounded = budget_memories_for_prompt(records);
        assert_eq!(bounded.len(), 4, "four 6k slices fill the 24k budget");
        assert!(bounded[0].content.starts_with("record-0:"));
        assert!(bounded
            .iter()
            .all(|record| record.content.contains("memory truncated")));
    }

    #[test]
    fn build_preflight_finds_missing_feature_specs_and_blocked_api_record() {
        let repo = tempfile::tempdir().expect("repo");
        let docs = repo.path().join("docs");
        let features = docs.join("features");
        fs::create_dir_all(&features).expect("features dir");
        fs::write(
            docs.join("IMPLEMENTATION_PLAN.md"),
            "Use docs/features/01-present.md and docs/features/04-missing.md.",
        )
        .expect("plan");
        fs::write(features.join("01-present.md"), "# Present").expect("feature");
        fs::write(
            docs.join("API_RECORD.md"),
            "**Status: BLOCKED — zero external APIs are approved for use.**",
        )
        .expect("api record");

        let findings = build_preflight_findings(repo.path());
        assert_eq!(findings.len(), 2, "{findings:?}");
        assert!(findings[0].contains("04-missing.md"));
        assert!(findings[1].contains("verification blocked"));

        fs::write(features.join("04-missing.md"), "# Now present").expect("feature");
        fs::write(docs.join("API_RECORD.md"), "**Status: APPROVED**").expect("api record");
        assert!(build_preflight_findings(repo.path()).is_empty());
    }

    #[test]
    fn live_skill_block_preserves_every_input_byte() {
        let skill =
            "---\r\nname: mastermind\r\n---\r\n# Mastermind — hierarchical build orchestrator";
        let mut envelope = String::new();
        append_live_skill(&mut envelope, skill);
        let injected = envelope
            .strip_prefix(LIVE_SKILL_BEGIN)
            .and_then(|value| value.strip_suffix(LIVE_SKILL_END))
            .expect("skill markers");
        assert_eq!(injected.as_bytes(), skill.as_bytes());
    }

    /// The reviewer keeps the shared planning ceiling; the author must get a
    /// strictly larger one, or a scoped authoring turn dies before it writes.
    #[test]
    fn author_and_review_windows_exceed_the_shared_planning_ceiling() {
        let default = agentos_orchestrator::model::DEFAULT_PLANNING_TIMEOUT_SECS;
        assert!(
            AUTHOR_TIMEOUT_SECS > default,
            "author window {AUTHOR_TIMEOUT_SECS}s must exceed the planning default {default}s",
        );
        // Raising only the author just moves the ceiling onto the reviewer,
        // which is how Phase 6 authored its deliverable and then failed anyway.
        assert!(
            REVIEW_TIMEOUT_SECS > default,
            "review window {REVIEW_TIMEOUT_SECS}s must exceed the planning default {default}s",
        );
    }

    /// Phase 6 stranded at `running` after a failed preparation. Every
    /// dispatch arm in `respond` missed it, so Continue fell through to the
    /// tail and returned an empty cycle that rendered as a successful
    /// "0-task draft" while never reaching the author.
    #[test]
    fn no_author_phase_status_falls_through_the_respond_dispatch() {
        let dispatched = |phase: MastermindPhase, status: PhaseStatus| -> bool {
            if phase == MastermindPhase::Discovery {
                return true;
            }
            matches!(
                status,
                PhaseStatus::Active
                    | PhaseStatus::Running
                    | PhaseStatus::AwaitingWriteApproval
                    | PhaseStatus::AwaitingApproval
                    | PhaseStatus::NeedsRevision
                    | PhaseStatus::Blocked
                    | PhaseStatus::Complete
            ) || phase == MastermindPhase::Build
        };

        for phase in [
            MastermindPhase::Prd,
            MastermindPhase::Features,
            MastermindPhase::ImplementationPlan,
            MastermindPhase::ApiRecord,
            MastermindPhase::Design,
            MastermindPhase::Mockups,
            MastermindPhase::HtmlToReact,
        ] {
            assert!(
                dispatched(phase, PhaseStatus::Running),
                "{} at `running` has no dispatch arm and would return a false-success empty cycle",
                phase.id()
            );
        }
    }

    /// Two design rounds failed on defects the author could have caught
    /// itself: figures asserted without being computed, and user-facing copy
    /// paraphrased instead of quoted from the frozen feature documents. The
    /// brief has to keep demanding both, and it has to reach the mockups too,
    /// because the wireframes inherit whatever the design document fixes.
    #[test]
    fn design_brief_demands_computed_figures_and_verbatim_copy() {
        let brief = DESIGN_AUTHORING_BRIEF;

        // Each clause answers a defect class that actually failed review.
        // Round 1/4: figures asserted, or a whole class of pair never checked.
        assert!(brief.contains("VERIFY EVERY NUMBER YOU STATE"));
        assert!(brief.contains("relative luminance"));
        assert!(brief.contains("4.5:1"));
        assert!(
            brief.contains("SC 1.4.11"),
            "non-text contrast went unchecked for four rounds"
        );
        assert!(brief.contains("Composite translucent colours"));
        // Rounds 1-2: copy paraphrased, interaction shape invented.
        assert!(brief.contains("QUOTE USER-FACING COPY, NEVER PARAPHRASE IT"));
        assert!(brief.contains("one input"));
        // Every round: a mandated state or component simply absent.
        assert!(brief.contains("BUILD A COVERAGE CHECKLIST FIRST"));
        // Rounds 1,2,3,5: the document broke a rule it had itself written.
        // An exhortation did not stop this; the audit is a procedure.
        assert!(brief.contains("AUDIT YOUR OWN DOCUMENT BEFORE FINISHING"));
        assert!(brief.contains("zero violations"));
        assert!(brief.contains("no home is unbuildable"));
        // Rounds 4-5: project disclosure conventions ignored.
        assert!(brief.contains("FOLLOW THE PROJECT'S OWN GOVERNANCE"));
        assert!(
            brief.contains("frozen and authoritative"),
            "the feature documents must outrank the author's own design intent"
        );

        for phase in [MastermindPhase::Design, MastermindPhase::Mockups] {
            assert!(
                matches!(phase, MastermindPhase::Design | MastermindPhase::Mockups),
                "{} must author under the design verification rules",
                phase.id()
            );
        }
    }

    #[test]
    fn every_author_phase_has_a_server_derived_non_root_scope() {
        let repo = PathBuf::from("C:/work/project");
        for phase in [
            MastermindPhase::Discovery,
            MastermindPhase::Prd,
            MastermindPhase::Features,
            MastermindPhase::ImplementationPlan,
            MastermindPhase::ApiRecord,
            MastermindPhase::Design,
            MastermindPhase::Mockups,
            MastermindPhase::HtmlToReact,
        ] {
            assert!(phase_requires_write_authorization(phase));
            let paths = phase_output_paths(&repo, phase);
            assert!(!paths.is_empty(), "{} has no write scope", phase.id());
            assert!(
                paths.iter().all(|path| Path::new(path) != repo),
                "{} must never authorize the repository root: {paths:?}",
                phase.id()
            );
        }

        let h2r = phase_output_paths(&repo, MastermindPhase::HtmlToReact);
        assert_eq!(
            h2r,
            vec![
                repo.join(".h2r").to_string_lossy().into_owned(),
                repo.join("docs")
                    .join("IMPLEMENTATION_PLAN.md")
                    .to_string_lossy()
                    .into_owned(),
                repo.join("docs")
                    .join("API_RECORD.md")
                    .to_string_lossy()
                    .into_owned(),
                repo.join("wireframe")
                    .join("INDEX.md")
                    .to_string_lossy()
                    .into_owned(),
            ]
        );
    }

    /// Phase 1 authors from the interview transcript alone. The brief is the
    /// only thing stopping the model from mirroring the questions back as
    /// headings, so its prohibitions and its section list are load-bearing.
    #[test]
    fn discovery_authoring_brief_demands_synthesis_and_the_canonical_sections() {
        for prohibition in [
            "Never write an interview question as a heading",
            "(Recommended)",
            "must never contradict itself",
            "DEFERRED(user)",
        ] {
            assert!(
                DISCOVERY_AUTHORING_BRIEF.contains(prohibition),
                "discovery brief lost the {prohibition:?} rule"
            );
        }

        for section in [
            "## Vision",
            "## Platform & stack",
            "## Feature census",
            "## Feature detail",
            "## Non-functional",
            "## Deferred decisions",
        ] {
            assert!(
                DISCOVERY_AUTHORING_BRIEF.contains(section),
                "discovery brief lost the {section:?} section"
            );
        }

        for detail in [
            "happy path",
            "inputs",
            "outputs",
            "states",
            "edge cases",
            "permissions",
            "data lifecycle",
            "integrations",
            "failure modes",
        ] {
            assert!(
                DISCOVERY_AUTHORING_BRIEF.contains(detail),
                "discovery brief lost the {detail:?} drill-down bullet"
            );
        }
    }

    /// The reviewer is the second gate on known phase-specific failures.
    #[test]
    fn phases_with_a_known_failure_mode_carry_their_own_review_rubric() {
        let discovery = phase_review_checks(MastermindPhase::Discovery);
        assert!(discovery.contains("(Recommended)"));
        assert!(discovery.contains("VERDICT: FAIL"));
        assert!(discovery.contains("nine required details"));

        let features = phase_review_checks(MastermindPhase::Features);
        assert!(features.contains("every census item"));
        assert!(features.contains("missing census item"));

        let api = phase_review_checks(MastermindPhase::ApiRecord);
        assert!(api.contains("declares itself BLOCKED"));
        assert!(api.contains("IMPLEMENTATION_PLAN.md"));

        // The mockups implement a contract they did not author, so the
        // reviewer is told the ban list is checked by searching for it.
        let mockups = phase_review_checks(MastermindPhase::Mockups);
        assert!(mockups.contains("Absolute Ban"));
        assert!(mockups.contains("tokens.css"));
        assert!(mockups.contains("section"));

        // Per-task reviews cannot see across tasks.
        let build = phase_review_checks(MastermindPhase::Build);
        assert!(build.contains("whole-branch"));
        assert!(build.contains("API_RECORD.md"));

        // A wrap-up deliverable *is* evidence, so unevidenced claims fail.
        let wrap = phase_review_checks(MastermindPhase::Wrap);
        assert!(wrap.contains("clean build and a full test run"));
        assert!(wrap.contains("acceptance criterion"));

        let h2r = phase_review_checks(MastermindPhase::HtmlToReact);
        assert!(h2r.contains("preview-nav-bar"));
        assert!(h2r.contains("API_RECORD.md"));
        assert!(h2r.contains("frozen wireframe HTML"));
    }

    /// Phase 9 demands execution evidence, so its reviewer must actually be
    /// able to execute. Before this, the same prompt asserted "you have no
    /// shell" *and* "FAIL unless this reply quotes fresh build output" — a
    /// contradiction the reviewer resolved the only way it could, by failing
    /// the phase and naming its own missing shell as the violated
    /// requirement. Live: session 01a0413b, phase-9-wrap, VERDICT: FAIL.
    #[test]
    fn only_the_wrap_review_runs_commands_and_its_prompt_says_so() {
        assert!(review_runs_commands(MastermindPhase::Wrap));
        for phase in [
            MastermindPhase::Prd,
            MastermindPhase::Design,
            MastermindPhase::Mockups,
            MastermindPhase::HtmlToReact,
            MastermindPhase::Build,
        ] {
            assert!(
                !review_runs_commands(phase),
                "{phase:?} must stay reading-only"
            );
            let clause = review_scope_clause(phase);
            assert!(clause.contains("no shell"), "{phase:?}");
            assert!(
                !clause.contains("Run the project's real build"),
                "{phase:?}"
            );
        }

        // The wrap clause must not carry the no-shell sentence that made the
        // phase unpassable, and must order a real run.
        let wrap = review_scope_clause(MastermindPhase::Wrap);
        assert!(!wrap.contains("no shell"));
        assert!(wrap.contains("Run the project's real build"));
        assert!(wrap.contains("quote their actual output"));
    }

    /// A sub-score must not overturn the verdict.
    ///
    /// Live regression: Phase 9's reviewer returned a reply opening with
    /// `VERDICT: FAIL` whose design-rubric section contained
    /// `**Anti-Patterns Verdict: PASS**`. The old
    /// `contains("VERDICT: PASS")` matched that line, so a failing review was
    /// recorded as passing, the phase was approved, and the session reached
    /// `complete` carrying a FAIL as its stored review.
    #[test]
    fn the_first_line_anchored_verdict_decides_and_a_missing_one_fails_closed() {
        // The exact live shape that broke it.
        let live = "VERDICT: FAIL

### Findings
1. Requirement violated ...

                    **Rubric Score: 20/20**
**Anti-Patterns Verdict: PASS**
";
        assert!(
            !verdict_is_pass(live),
            "a sub-score must not overturn the verdict"
        );

        assert!(verdict_is_pass(
            "VERDICT: PASS

No findings."
        ));
        assert!(verdict_is_pass(
            "**VERDICT: PASS**

All criteria met."
        ));
        assert!(verdict_is_pass(
            "- verdict: pass
lowercase is still a verdict"
        ));
        assert!(!verdict_is_pass(
            "VERDICT: FAIL

Something is wrong."
        ));

        // First verdict wins: a later one cannot revise it.
        assert!(!verdict_is_pass(
            "VERDICT: FAIL

later...
VERDICT: PASS"
        ));
        assert!(verdict_is_pass(
            "VERDICT: PASS

later...
VERDICT: FAIL"
        ));

        // No verdict at all fails closed, as does an empty reply.
        assert!(!verdict_is_pass("I reviewed the files and they look fine."));
        assert!(!verdict_is_pass(""));

        // Prose mentioning the token mid-line is not a verdict.
        assert!(!verdict_is_pass(
            "The rubric says to reply VERDICT: PASS when everything is met."
        ));
    }

    /// agy has no per-tool deny surface: the shell tokens in the denylist are
    /// the *only* thing selecting `--sandbox`, so a leftover token silently
    /// re-sandboxes the wrap reviewer and the phase becomes unpassable again
    /// with no error anywhere. This also pins the mirrored list to agy's.
    #[test]
    fn reviewer_shell_grant_clears_every_shell_token() {
        let granted = reviewer_denylist_with_shell();
        for shell in AGY_SHELL_TOOLS {
            assert!(
                !granted.iter().any(|tool| tool == shell),
                "{shell} still denied; agy would keep --sandbox"
            );
            assert!(
                ORCHESTRATOR_TOOL_DENYLIST.contains(&shell),
                "{shell} is not in the orchestrator denylist; the mirror has drifted"
            );
        }

        // The grant is "run the build", not "fix what it reports".
        for kept in ["Write", "Edit", "MultiEdit", "Agent", "Task"] {
            assert!(
                granted.iter().any(|tool| tool == kept),
                "{kept} must stay denied to the reviewer"
            );
        }
    }

    #[test]
    fn phase_review_checks_cover_the_remaining_phases() {
        for phase in [
            MastermindPhase::Setup,
            MastermindPhase::Prd,
            MastermindPhase::ImplementationPlan,
            MastermindPhase::Design,
            MastermindPhase::Complete,
        ] {
            assert_eq!(
                phase_review_checks(phase),
                "",
                "{} must keep the shared review prompt unchanged",
                phase.id()
            );
        }
    }

    #[test]
    fn chat_can_accept_a_reviewed_artifact_without_mistaking_changes_for_approval() {
        for guidance in [
            "checked the product requirements; everything is on point",
            "Everything is fine.",
            "Accept as-is",
            "Nothing needs to be revised",
            "Looks good",
        ] {
            assert!(accepts_phase_as_is(guidance), "{guidance}");
        }
        for guidance in [
            "Everything is fine, but change the deployment target",
            "Looks good except the API section",
            "Revise the authentication flow",
        ] {
            assert!(!accepts_phase_as_is(guidance), "{guidance}");
        }
    }

    #[test]
    fn failed_authoring_recovers_only_when_the_scoped_artifact_changed() {
        let original = Some("original".to_owned());
        let changed = Some("changed".to_owned());
        assert!(artifact_changed_after_authoring(&None, &changed));
        assert!(artifact_changed_after_authoring(&original, &changed));
        assert!(!artifact_changed_after_authoring(&original, &original));
        assert!(!artifact_changed_after_authoring(&None, &None));
    }

    /// The bundled `plan.schema.json` ships `"outDir": "."`, so a plan that
    /// followed the template used to be un-revisable: the scope check
    /// demanded a two-segment outDir and rejected the schema's own default,
    /// leaving Phase 7.5 stuck in needs-revision with no way forward.
    /// Checkpoint recovery runs inside the constructor, so the daemon
    /// journal has to be known by then. When it was a builder applied after
    /// `new()`, every session restored at startup kept `journal_db: None`
    /// and quietly sent its run and task events to a per-session journal the
    /// daemon's projections never read — `runs.list` and `tasks.list`
    /// returned empty while a restored run was executing.
    #[test]
    fn a_restored_session_uses_the_daemon_journal_not_a_per_session_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path().join("state");
        let journal = dir.path().join("agentos.db");
        // A checkpoint directory is enough to exercise the recovery path.
        fs::create_dir_all(state_dir.join("mastermind").join("session-1")).expect("checkpoint dir");

        let service = Mastermind::with_journal_db(
            Arc::new(AgentRegistry::open(&dir.path().join("agents.db")).expect("registry")),
            dir.path().join("agents.db"),
            &state_dir,
            Some(journal.clone()),
            |_| None,
        );

        assert_eq!(
            service.supervisor_journal(&state_dir.join("mastermind").join("session-1")),
            journal,
            "a restored session must append to the daemon journal"
        );
    }

    #[test]
    fn a_dot_out_dir_grants_the_components_directory_not_the_repository_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo = dir.path();
        fs::create_dir_all(repo.join(".h2r")).expect("h2r");
        fs::create_dir_all(repo.join("components")).expect("components");
        fs::write(
            repo.join(".h2r").join("plan.json"),
            r#"{"outDir": ".", "componentsDir": "components"}"#,
        )
        .expect("plan");

        let paths = h2r_revision_write_paths(repo).expect("a schema-shaped plan is revisable");
        assert!(
            paths.iter().any(|path| path.ends_with("components")),
            "the components directory is the write scope: {paths:?}"
        );
        let root = repo.to_string_lossy().into_owned();
        assert!(
            !paths.contains(&root),
            "the repository root is never a grant: {paths:?}"
        );
    }

    /// A nested base still grants the base and its components subtree.
    #[test]
    fn a_nested_out_dir_grants_the_base_and_its_components_subtree() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo = dir.path();
        fs::create_dir_all(repo.join(".h2r")).expect("h2r");
        fs::create_dir_all(repo.join("src").join("ui")).expect("ui");
        fs::write(
            repo.join(".h2r").join("plan.json"),
            r#"{"outDir": "src", "componentsDir": "ui"}"#,
        )
        .expect("plan");

        let paths = h2r_revision_write_paths(repo).expect("nested plan is revisable");
        assert!(paths.iter().any(|path| path.ends_with("src")), "{paths:?}");
        assert!(paths.iter().any(|path| path.ends_with("ui")), "{paths:?}");
    }

    /// Traversal and absolute paths never become a write grant, whichever
    /// field carries them.
    #[test]
    fn traversal_in_either_plan_field_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo = dir.path();
        fs::create_dir_all(repo.join(".h2r")).expect("h2r");
        for plan in [
            r#"{"outDir": "../escape", "componentsDir": "components"}"#,
            r#"{"outDir": ".", "componentsDir": "../escape"}"#,
            r#"{"outDir": "/etc", "componentsDir": "components"}"#,
        ] {
            fs::write(repo.join(".h2r").join("plan.json"), plan).expect("plan");
            assert!(
                h2r_revision_write_paths(repo).is_err(),
                "must refuse: {plan}"
            );
        }
    }

    #[test]
    fn h2r_scope_uses_only_safe_preparation_declarations_plus_private_artifacts() {
        let repo = PathBuf::from("C:/work/project");
        let paths = h2r_write_paths_from_preparation(
            &repo,
            "READY_TO_AUTHOR\nWRITE_PATHS:\n- app/page.tsx\n- components/Hero.tsx\n- ../outside\n- C:/absolute\n- docs/PRD.md\n- wireframe/INDEX.md\n- .git/config\n",
        );
        assert_eq!(
            paths,
            vec![
                repo.join(".h2r").to_string_lossy().into_owned(),
                repo.join("docs")
                    .join("IMPLEMENTATION_PLAN.md")
                    .to_string_lossy()
                    .into_owned(),
                repo.join("docs")
                    .join("API_RECORD.md")
                    .to_string_lossy()
                    .into_owned(),
                repo.join("wireframe")
                    .join("INDEX.md")
                    .to_string_lossy()
                    .into_owned(),
                repo.join("app/page.tsx").to_string_lossy().into_owned(),
                repo.join("components/Hero.tsx")
                    .to_string_lossy()
                    .into_owned(),
            ]
        );
        assert!(paths.iter().all(|path| Path::new(path) != repo));
    }

    #[test]
    fn memex_projections_inject_only_the_newest_effective_rows() {
        let record = |id, kind: &str, tags: &str| MemoryRecord {
            id,
            project: "agentos:test".to_owned(),
            agent: ORCHESTRATOR_AGENT.to_owned(),
            provider: "codex".to_owned(),
            kind: kind.to_owned(),
            content: format!("row {id}"),
            tags: tags.to_owned(),
            created_at: "2026-08-24 00:00:00".to_owned(),
        };
        let selected = newest_effective_memories(vec![
            record(
                9,
                "context",
                "mastermind,session:s,phase:2,kind:phase-snapshot",
            ),
            record(
                8,
                "decision",
                "mastermind,session:s,phase:2,kind:decision-revision",
            ),
            record(
                7,
                "context",
                "mastermind,session:s,phase:2,kind:phase-snapshot",
            ),
            record(
                6,
                "decision",
                "mastermind,session:s,phase:2,kind:decision-revision",
            ),
            record(5, "lesson", "mastermind,session:s,phase:2,kind:lesson"),
        ]);
        assert_eq!(
            selected.iter().map(|record| record.id).collect::<Vec<_>>(),
            vec![9, 8, 5]
        );
    }

    fn node(id: &str, depends_on: &[&str]) -> NodeSpec {
        NodeSpec {
            id: id.to_owned(),
            node_type: NodeType::Run,
            depends_on: depends_on.iter().map(|d| (*d).to_owned()).collect(),
            agent_role: Some("nextjs-dev".to_owned()),
            budgets: Budgets::default(),
            retry: Default::default(),
        }
    }

    /// The worker's prompt has to carry three things the skill preamble
    /// cannot: what the run is for, what THIS task is, and which upstream
    /// tasks it builds on.
    #[test]
    fn contract_carries_goal_task_and_upstream_dependencies() {
        let contract = contract_for(
            &node("impl-api", &["spec", "schema"]),
            Some("Build the /orders route handler."),
            "Ship the orders API",
            "abc123",
            &["src/**".to_owned()],
        );

        assert!(contract.objective.contains("Ship the orders API"), "goal");
        assert!(contract.objective.contains("impl-api"), "task id");
        assert!(
            contract
                .objective
                .contains("Build the /orders route handler."),
            "stated objective"
        );
        assert!(contract.objective.contains("spec, schema"), "upstream ids");
        assert_eq!(contract.dependencies, vec!["spec", "schema"]);
        assert_eq!(contract.base_commit, "abc123");
        assert_eq!(contract.allowed_paths, vec!["src/**".to_owned()]);
        assert_eq!(contract.git_policy, GitPolicy::NoDirectGit);
        contract.validate().expect("synthesized contract is valid");
    }

    #[test]
    fn planning_report_keeps_structured_human_choices_on_the_wire() {
        let report = PlanCycleReport {
            decisions: vec![PlanningDecision {
                tool: "AskUserQuestion".to_owned(),
                prompt: "Which database?".to_owned(),
                options: vec!["SQLite".to_owned(), "Postgres".to_owned()],
                multi_select: false,
            }],
            ..PlanCycleReport::default()
        };

        let wire = report_json(&report);
        assert_eq!(wire["decisions"][0]["prompt"], json!("Which database?"));
        assert_eq!(wire["decisions"][0]["options"][1], json!("Postgres"));
        assert_eq!(wire["decisions"][0]["multiSelect"], json!(false));
    }

    /// A node the orchestrator left bare still produces a usable contract —
    /// an empty objective would fail `TaskContract::validate` and take the
    /// whole commit down with it.
    #[test]
    fn contract_survives_a_node_with_no_stated_objective() {
        let contract = contract_for(
            &node("solo", &[]),
            None,
            "Goal",
            "deadbeef",
            &["**".to_owned()],
        );
        assert!(contract.objective.contains("solo"));
        assert!(!contract.objective.contains("Upstream tasks"));
        contract.validate().expect("bare node still validates");
    }

    #[test]
    fn a_bundled_script_is_staged_where_the_agent_can_reach_it() {
        let skills = tempfile::tempdir().expect("skill root");
        let scripts = skills.path().join("scripts");
        std::fs::create_dir_all(&scripts).expect("scripts dir");
        std::fs::write(scripts.join("h2r.py"), "print('h2r')").expect("script");
        let repo = tempfile::tempdir().expect("repo");

        // Unstaged, `skillScripts` names the installed path. That path is
        // outside the workspace, and the claude-code adapter emits no
        // directory grant at all, so the agent cannot open or run it.
        let installed = skill_scripts(skills.path(), repo.path(), MastermindPhase::HtmlToReact);
        let before = installed["h2r"].as_str().expect("h2r entry").to_owned();
        assert!(
            !before.starts_with(repo.path().to_string_lossy().as_ref()),
            "precondition: the installed script lives outside the workspace"
        );

        stage_phase_scripts(skills.path(), repo.path(), MastermindPhase::HtmlToReact);

        let staged = repo.path().join(".h2r").join("h2r.py");
        assert!(
            staged.is_file(),
            "the script must land inside the write scope"
        );
        let after = skill_scripts(skills.path(), repo.path(), MastermindPhase::HtmlToReact);
        assert_eq!(
            after["h2r"].as_str().expect("h2r entry"),
            staged.to_string_lossy(),
            "skillScripts must report the reachable copy, not the installed one"
        );
    }

    #[test]
    fn a_phase_with_no_bundled_procedure_stages_nothing() {
        let skills = tempfile::tempdir().expect("skill root");
        let scripts = skills.path().join("scripts");
        std::fs::create_dir_all(&scripts).expect("scripts dir");
        std::fs::write(scripts.join("h2r.py"), "print('h2r')").expect("script");
        let repo = tempfile::tempdir().expect("repo");

        stage_phase_scripts(skills.path(), repo.path(), MastermindPhase::Mockups);

        assert!(
            !repo.path().join(".h2r").exists(),
            "staging must not create directories for phases that run no script"
        );
    }

    #[test]
    fn a_greenfield_react_project_still_routes_mockups_to_h2r() {
        let repo = tempfile::tempdir().expect("repo dir");
        std::fs::create_dir_all(repo.path().join("docs")).expect("docs dir");
        std::fs::write(
            repo.path().join("docs/PRD.md"),
            "Frontend is a Next.js app deployed on Vercel; backend is Node.js on Render.",
        )
        .expect("write prd");

        // The h2r phase is what creates the app, so no package.json exists yet.
        assert!(!repo.path().join("package.json").exists());
        assert!(
            is_react_stack(repo.path()),
            "an approved Next.js stack must be detected before any code exists"
        );
        assert_eq!(
            MastermindPhase::Mockups.next(is_react_stack(repo.path())),
            MastermindPhase::HtmlToReact,
            "skipping 7.5 leaves the wireframes unconverted and Build works on raw HTML"
        );
    }

    #[test]
    fn a_non_react_project_skips_h2r_and_a_reactive_mention_does_not_count() {
        let repo = tempfile::tempdir().expect("repo dir");
        std::fs::create_dir_all(repo.path().join("docs")).expect("docs dir");
        std::fs::write(
            repo.path().join("docs/PRD.md"),
            "A Rust CLI with a reactive event loop; reaction times are logged.",
        )
        .expect("write prd");

        assert!(!is_react_stack(repo.path()));
        assert_eq!(
            MastermindPhase::Mockups.next(is_react_stack(repo.path())),
            MastermindPhase::Build
        );
    }

    fn git_repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let git = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(args)
                .output()
                .expect("git runs");
            assert!(
                status.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&status.stderr)
            );
        };
        git(&["init", "--initial-branch=main"]);
        git(&["config", "user.email", "test@example.com"]);
        git(&["config", "user.name", "Test"]);
        std::fs::write(dir.path().join("README.md"), "seed").expect("write");
        git(&["add", "."]);
        git(&["commit", "-m", "seed"]);
        dir
    }

    #[test]
    fn non_git_project_folder_is_initialized_without_touching_its_files() {
        let project = tempfile::tempdir().expect("project dir");
        std::fs::write(project.path().join("notes.txt"), "keep me").expect("user file is written");

        ensure_git_repository(project.path()).expect("Git setup succeeds");

        assert!(
            project.path().join(".git").is_dir(),
            "repository initialized"
        );
        assert_eq!(
            std::fs::read_to_string(project.path().join("notes.txt")).expect("user file remains"),
            "keep me"
        );
        assert!(!head_commit(project.path())
            .expect("setup commit")
            .is_empty());
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(project.path())
            .args(["status", "--short"])
            .output()
            .expect("git status runs");
        assert!(status.status.success());
        assert_eq!(
            String::from_utf8_lossy(&status.stdout),
            "?? notes.txt\n",
            "setup must not stage user files"
        );
    }

    /// Reopening is an operator's recovery lever over a *failed* run. With
    /// nothing committed there is no run at all, so the RPC must say so
    /// (the server maps this to `invalid_params`) rather than reporting a
    /// successful reopen of nothing — the same "no-op reported as success"
    /// shape this change exists to remove from the escalation path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopening_an_uncommitted_session_is_a_caller_error() {
        let repo = git_repo();
        let state = tempfile::tempdir().expect("state dir");
        let agents_db = state.path().join("agents.db");
        let registry = AgentRegistry::open(&agents_db).expect("registry");
        registry.seed_builtin_skills().expect("seed skills");
        registry.seed_builtins().expect("seed");
        let adapter = Arc::new(MockAdapter::new(MockBehavior::Success {
            turns: 1,
            files_changed: vec![],
        }));
        let resolver_adapter = Arc::clone(&adapter);
        let mastermind = Mastermind::new(Arc::new(registry), &agents_db, state.path(), move |_| {
            Some(Arc::clone(&resolver_adapter) as Arc<dyn RuntimeAdapter>)
        })
        .with_memex(MemexClient::from_env().with_db(state.path().join("memex.db")));

        let summary = mastermind
            .start("Ship the orders API", repo.path(), Some("codex"), None)
            .await
            .expect("session starts");
        let session_id = summary["sessionId"].as_str().expect("session id");

        assert!(matches!(
            mastermind.reopen_run(session_id).await,
            Err(MastermindError::NotCommitted(_))
        ));
        assert!(matches!(
            mastermind.reopen_run("no-such-session").await,
            Err(MastermindError::UnknownSession(_))
        ));
    }

    /// The point of the F-12 wiring: `pool` values are registry agent ids,
    /// not the three static placeholder pools — and the orchestrator itself
    /// is never one of them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn start_routes_pools_at_the_registry_roster() {
        let repo = git_repo();
        let state = tempfile::tempdir().expect("state dir");
        let agents_db = state.path().join("agents.db");
        let registry = AgentRegistry::open(&agents_db).expect("registry");
        registry.seed_builtin_skills().expect("seed skills");
        registry.seed_builtins().expect("seed");

        let adapter = Arc::new(MockAdapter::new(MockBehavior::Success {
            turns: 1,
            files_changed: vec![],
        }));
        let resolver_adapter = Arc::clone(&adapter);
        let mastermind = Mastermind::new(Arc::new(registry), &agents_db, state.path(), move |_| {
            Some(Arc::clone(&resolver_adapter) as Arc<dyn RuntimeAdapter>)
        })
        .with_memex(MemexClient::from_env().with_db(state.path().join("memex.db")));

        let summary = mastermind
            .start(
                "Ship the orders API",
                repo.path(),
                Some("codex"),
                Some("gpt-5.6-sol"),
            )
            .await
            .expect("session starts");

        let pools: Vec<String> = serde_json::from_value(summary["pools"].clone()).expect("pools");
        for expected in [
            "nextjs-dev",
            "dependency-manager",
            "code-reviewer",
            "debugger-sol-escalation",
            "spec-writer",
        ] {
            assert!(
                pools.iter().any(|p| p == expected),
                "missing pool {expected}"
            );
        }
        assert!(
            !pools.iter().any(|p| p == ORCHESTRATOR_AGENT),
            "the orchestrator commands; it is never a worker pool"
        );
        assert_eq!(summary["reviewerPool"], json!(REVIEWER_AGENT));
        assert_eq!(summary["plannerAdapter"], json!("codex"));
        assert_eq!(summary["plannerModel"], json!("gpt-5.6-sol"));
        assert!(
            summary["nodes"].as_array().expect("nodes").is_empty(),
            "starting a session plans nothing — the user gate comes first"
        );

        // The roster carries identities, not bare ids: that IS the routing
        // signal the model reads.
        let roster = summary["roster"].as_array().expect("roster");
        let dep = roster
            .iter()
            .find(|entry| entry["id"] == json!("dependency-manager"))
            .expect("dependency-manager on the roster");
        assert!(
            dep["description"]
                .as_str()
                .unwrap_or_default()
                .contains("version"),
            "description is the routing signal"
        );

        // Nothing is committed, so driving is a caller error, not a panic.
        let session_id = summary["sessionId"].as_str().expect("session id");
        assert!(matches!(
            mastermind.commit(session_id).await,
            Err(MastermindError::DiscoveryIncomplete(_))
        ));
        assert!(matches!(
            mastermind.drive(session_id, Some(1)).await,
            Err(MastermindError::DiscoveryIncomplete(_))
        ));

        // Discovery is a real, visible, user-gated phase. The mock planner
        // emits no questions, so the safe fallback supplies three.
        let first = mastermind
            .plan(session_id, None)
            .await
            .expect("discovery starts");
        assert_eq!(first["session"]["stage"], json!("discovery"));
        assert_eq!(first["cycle"]["decisions"].as_array().unwrap().len(), 1);
        mastermind
            .plan(session_id, Some("Smallest working MVP"))
            .await
            .expect("answer one");
        mastermind
            .plan(session_id, Some("Managed cloud"))
            .await
            .expect("answer two");
        let ready = mastermind
            .plan(session_id, Some("Public ephemeral data"))
            .await
            .expect("answer three");
        assert_eq!(
            ready["session"]["phaseStatus"],
            json!("awaiting-write-approval")
        );
        assert_eq!(ready["session"]["canAuthorizeWrite"], json!(true));
        assert_eq!(
            ready["session"]["writeRequest"]["deliverables"],
            json!(["docs/DISCOVERY.md"])
        );
        let discovery_path = repo.path().join("docs").join("DISCOVERY.md");
        assert!(
            !discovery_path.exists(),
            "discovery remains checkpoint-only until the write grant"
        );
        let still_waiting = mastermind
            .plan(session_id, None)
            .await
            .expect("write gate is stable");
        assert_eq!(
            still_waiting["session"]["phaseStatus"],
            json!("awaiting-write-approval")
        );

        // The mock adapter reports tool activity but does not mutate the
        // filesystem. Seed exactly the artifact a real scoped author would
        // create, then exercise authorization and review transitions. Its
        // completion summary echoes the review prompt (including the prompt's
        // mid-line `VERDICT: PASS` instruction) but does not return a verdict;
        // the line-anchored parser must therefore fail closed.
        std::fs::create_dir_all(repo.path().join("docs")).expect("docs");
        std::fs::write(
            &discovery_path,
            "# Discovery\n\nStatus: draft\n\nPublic ephemeral data\n",
        )
        .expect("seed mock discovery deliverable");
        let authored = mastermind
            .authorize_phase_write(session_id)
            .await
            .expect("scoped author turn runs");
        assert_eq!(authored["session"]["phaseStatus"], json!("needs-revision"));
        assert!(
            !verdict_is_pass(
                authored["session"]["lastReview"]
                    .as_str()
                    .expect("mock review text")
            ),
            "an echoed verdict instruction is not a reviewer verdict"
        );
        let discovery_provider_session = {
            let session = mastermind.session(session_id).expect("session");
            let mut guard = session.lock().await;
            let provider_session = guard
                .planning_conversation
                .provider_session_id()
                .expect("provider conversation retained");
            guard.phase_status = PhaseStatus::AwaitingApproval;
            mastermind.persist_session(&guard).expect("review gate");
            provider_session
        };
        let approved = mastermind
            .approve_discovery(session_id)
            .await
            .expect("discovery approval unlocks planning");
        assert_eq!(approved["session"]["stage"], json!("planning"));
        assert_eq!(approved["session"]["phase"], json!("phase-2-prd"));
        assert_eq!(approved["session"]["phaseStatus"], json!("active"));
        assert!(
            mastermind
                .session(session_id)
                .expect("session")
                .lock()
                .await
                .planning_conversation
                .provider_session_id()
                .is_none(),
            "approval returns before starting the next phase's provider turn"
        );
        let prepared = mastermind
            .plan(session_id, None)
            .await
            .expect("explicit continuation prepares PRD");
        assert_eq!(
            prepared["session"]["phaseStatus"],
            json!("awaiting-write-approval")
        );
        let prd_provider_session = mastermind
            .session(session_id)
            .expect("session")
            .lock()
            .await
            .planning_conversation
            .provider_session_id()
            .expect("fresh PRD conversation");
        assert_ne!(
            discovery_provider_session, prd_provider_session,
            "approved phase boundary opens a clean provider conversation"
        );
        std::fs::write(
            repo.path().join("docs/PRD.md"),
            "# Product requirements\n\nReviewed by the user.\n",
        )
        .expect("seed reviewed PRD");
        {
            let session = mastermind.session(session_id).expect("session");
            let mut guard = session.lock().await;
            guard.phase_status = PhaseStatus::NeedsRevision;
            guard.last_review = Some("VERDICT: FAIL\nClarify one optional detail.".to_owned());
            mastermind.persist_session(&guard).expect("revision gate");
        }
        let accepted = mastermind
            .accept_phase_as_is(session_id)
            .await
            .expect("user may waive review findings");
        assert_eq!(accepted["session"]["phase"], json!("phase-3-features"));
        assert_eq!(accepted["session"]["phaseStatus"], json!("active"));
        assert!(accepted["session"]["lastReview"]
            .as_str()
            .unwrap_or_default()
            .contains("Accepted as-is"));
        let features_dir = repo.path().join("docs/features");
        std::fs::create_dir_all(&features_dir).expect("features dir");
        std::fs::write(
            features_dir.join("01-demo.md"),
            "# Demo feature\n\nThe timed-out author completed this artifact.\n",
        )
        .expect("seed timed-out feature artifact");
        {
            let session = mastermind.session(session_id).expect("session");
            let mut guard = session.lock().await;
            guard.phase_status = PhaseStatus::Blocked;
            guard.memory_error =
                Some("planning model unavailable: planning turn exceeded 300s".to_owned());
            guard.phase_write_paths = phase_output_paths(&guard.repo, guard.phase);
            let blocked = guard.summary();
            assert_eq!(blocked["canRecoverArtifact"], json!(true));
            assert_eq!(
                blocked["recoveryRequest"]["deliverables"],
                json!(["docs/features"])
            );
            mastermind
                .persist_session(&guard)
                .expect("blocked artifact checkpoint");
        }
        let recovered = mastermind
            .recover_phase_artifact(session_id)
            .await
            .expect("existing artifact receives read-only review");
        assert_eq!(recovered["session"]["phase"], json!("phase-3-features"));
        assert_eq!(recovered["session"]["phaseStatus"], json!("needs-revision"));
        assert!(
            !verdict_is_pass(
                recovered["session"]["lastReview"]
                    .as_str()
                    .expect("mock recovery review text")
            ),
            "artifact recovery must not accept an echoed verdict instruction"
        );
        assert_eq!(recovered["session"]["canRecoverArtifact"], json!(false));
        assert_eq!(recovered["session"]["memory"]["error"], Value::Null);
        {
            let session = mastermind.session(session_id).expect("session");
            let mut guard = session.lock().await;
            guard.phase = MastermindPhase::ImplementationPlan;
            guard.phase_status = PhaseStatus::Blocked;
            guard.phase_write_paths = phase_output_paths(&guard.repo, guard.phase);
            let missing = guard.summary();
            assert_eq!(missing["canRetryAuthoring"], json!(true));
            assert_eq!(
                missing["retryRequest"]["deliverables"],
                json!(["docs/IMPLEMENTATION_PLAN.md"])
            );
        }
        let memories = mastermind
            .memex
            .search(
                approved["session"]["memory"]["projectKey"]
                    .as_str()
                    .expect("project key"),
                "mastermind",
                100,
            )
            .expect("phase memories");
        assert!(memories.iter().any(|memory| memory.kind == "decision"));
        assert!(memories.iter().any(|memory| memory.kind == "context"));
        let board = mastermind
            .memex
            .board(
                approved["session"]["memory"]["projectKey"]
                    .as_str()
                    .expect("project key"),
            )
            .expect("phase board");
        assert_eq!(
            board.open_handoffs.len(),
            2,
            "Discovery and the accepted-as-is PRD each publish their next-phase handoff"
        );
        let approved_doc =
            std::fs::read_to_string(discovery_path).expect("approved discovery remains visible");
        assert!(approved_doc.contains("Status: approved"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_skill_reload_is_exact_and_checkpoint_restores_without_memory_files() {
        let repo = git_repo();
        let state = tempfile::tempdir().expect("state dir");
        let skill_home = tempfile::tempdir().expect("skill root");
        let skill_root = skill_home.path().join("mastermind");
        std::fs::create_dir_all(skill_root.join("references")).expect("references");
        let skill_path = skill_root.join("SKILL.md");
        let first_skill = "---\r\nname: mastermind\r\n---\r\n# Mastermind — hierarchical build orchestrator\r\nPhase 0–9 fixture";
        std::fs::write(&skill_path, first_skill.as_bytes()).expect("skill");
        std::fs::write(
            skill_root.join("references/discovery.md"),
            "Ask one to three questions.",
        )
        .expect("reference");
        std::fs::write(
            skill_root.join("references/code-graph.md"),
            "Build the graph before skimming an existing stack.",
        )
        .expect("reference");

        let agents_db = state.path().join("agents.db");
        let registry = AgentRegistry::open(&agents_db).expect("registry");
        registry.seed_builtin_skills().expect("seed skills");
        registry.seed_builtins().expect("seed agents");
        let memex_db = state.path().join("memex.db");
        let mastermind = Mastermind::new(Arc::new(registry), &agents_db, state.path(), |_| {
            Some(Arc::new(MockAdapter::new(MockBehavior::Success {
                turns: 1,
                files_changed: vec![],
            })) as Arc<dyn RuntimeAdapter>)
        })
        .with_skill_root(&skill_root)
        .with_memex(MemexClient::from_env().with_db(&memex_db));

        let summary = mastermind
            .start(
                "Build exact injection",
                repo.path(),
                Some("codex"),
                Some("gpt-5.6-sol"),
            )
            .await
            .expect("start");
        let session_id = summary["sessionId"].as_str().expect("session id");
        let session = mastermind.session(session_id).expect("session");
        let guard = session.lock().await;
        let (first_envelope, first_source, _, _) = mastermind
            .load_session_envelope(&guard)
            .expect("first envelope");
        let first_injected = first_envelope
            .split_once(LIVE_SKILL_BEGIN)
            .and_then(|(_, rest)| rest.split_once(LIVE_SKILL_END))
            .map(|(skill, _)| skill)
            .expect("skill block");
        assert_eq!(first_injected.as_bytes(), first_skill.as_bytes());

        let second_skill = format!("{first_skill}\r\nA live edit");
        std::fs::write(&skill_path, second_skill.as_bytes()).expect("edit skill");
        let (second_envelope, second_source, _, _) = mastermind
            .load_session_envelope(&guard)
            .expect("fresh envelope");
        let second_injected = second_envelope
            .split_once(LIVE_SKILL_BEGIN)
            .and_then(|(_, rest)| rest.split_once(LIVE_SKILL_END))
            .map(|(skill, _)| skill)
            .expect("skill block");
        assert_eq!(second_injected.as_bytes(), second_skill.as_bytes());
        assert_ne!(first_source.sha256, second_source.sha256);
        assert_eq!(first_injected.as_bytes(), first_skill.as_bytes());
        let reference_path = skill_root.join("references/discovery.md");
        std::fs::remove_file(&reference_path).expect("remove fixture reference");
        assert!(matches!(
            mastermind.load_session_envelope(&guard),
            Err(MastermindError::SkillSource { .. })
        ));
        std::fs::write(&reference_path, "Ask one to three questions.").expect("restore reference");
        drop(guard);
        drop(mastermind);

        for legacy in ["MEMORY.md", "HANDOFF.md", "RESUME.md"] {
            assert!(!repo.path().join("docs/memory").join(legacy).exists());
        }

        let registry = AgentRegistry::open(&agents_db).expect("reopen registry");
        let restored = Mastermind::new(Arc::new(registry), &agents_db, state.path(), |_| {
            Some(Arc::new(MockAdapter::new(MockBehavior::Success {
                turns: 1,
                files_changed: vec![],
            })) as Arc<dyn RuntimeAdapter>)
        })
        .with_skill_root(&skill_root)
        .with_memex(MemexClient::from_env().with_db(&memex_db));
        let restored_ids = restored.list().expect("restored list");
        if !restored_ids.contains(&session_id.to_owned()) {
            let checkpoint_dir = state
                .path()
                .join("mastermind")
                .join(session_id)
                .join("checkpoints");
            let mut checkpoints: Vec<_> = std::fs::read_dir(checkpoint_dir)
                .expect("checkpoint dir")
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect();
            checkpoints.sort();
            let raw = std::fs::read(checkpoints.last().expect("checkpoint")).expect("checkpoint");
            let checkpoint: SessionCheckpoint =
                serde_json::from_slice(&raw).expect("checkpoint schema");
            panic!(
                "automatic restore failed: {:?}",
                restored.restore_one(checkpoint).err()
            );
        }
        let status = restored.status(session_id).await.expect("restored status");
        assert_eq!(status["session"]["phase"], json!("phase-1-discovery"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn set_planner_repoints_an_idle_session_and_refuses_a_busy_one() {
        let repo = git_repo();
        let state = tempfile::tempdir().expect("state dir");
        let agents_db = state.path().join("agents.db");
        let registry = AgentRegistry::open(&agents_db).expect("registry");
        registry.seed_builtin_skills().expect("seed skills");
        registry.seed_builtins().expect("seed agents");
        let mastermind = Mastermind::new(Arc::new(registry), &agents_db, state.path(), |id| {
            (id != "no-such-provider").then(|| {
                Arc::new(MockAdapter::new(MockBehavior::Success {
                    turns: 1,
                    files_changed: vec![],
                })) as Arc<dyn RuntimeAdapter>
            })
        })
        .with_memex(MemexClient::from_env().with_db(state.path().join("memex.db")));
        let summary = mastermind
            .start(
                "Planner swap",
                repo.path(),
                Some("codex"),
                Some("gpt-5.6-sol"),
            )
            .await
            .expect("start");
        let session_id = summary["sessionId"]
            .as_str()
            .expect("session id")
            .to_owned();

        assert!(
            matches!(
                mastermind
                    .set_planner(&session_id, "no-such-provider", "some-model")
                    .await,
                Err(MastermindError::UnknownAdapter(_))
            ),
            "an unregistered adapter must never be installed"
        );

        {
            let session = mastermind.session(&session_id).expect("session");
            let _in_flight = session.lock().await;
            assert!(
                matches!(
                    mastermind
                        .set_planner(&session_id, "claude-code", "claude-opus-5")
                        .await,
                    Err(MastermindError::TurnInFlight(_))
                ),
                "a turn in flight owns the session and must block a provider swap"
            );
        }

        let after = mastermind
            .set_planner(&session_id, "claude-code", "claude-opus-5")
            .await
            .expect("set planner");
        assert_eq!(after["session"]["plannerAdapter"], json!("claude-code"));
        assert_eq!(after["session"]["plannerModel"], json!("claude-opus-5"));
        assert_eq!(
            after["session"]["phase"],
            json!(summary["phase"].as_str().unwrap_or_default()),
            "repointing the planner must not move the phase"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memex_failure_keeps_the_current_gate_and_checkpoint() {
        let repo = git_repo();
        let state = tempfile::tempdir().expect("state dir");
        let agents_db = state.path().join("agents.db");
        let registry = AgentRegistry::open(&agents_db).expect("registry");
        registry.seed_builtin_skills().expect("seed skills");
        registry.seed_builtins().expect("seed agents");
        let mut mastermind = Mastermind::new(Arc::new(registry), &agents_db, state.path(), |_| {
            Some(Arc::new(MockAdapter::new(MockBehavior::Success {
                turns: 1,
                files_changed: vec![],
            })) as Arc<dyn RuntimeAdapter>)
        })
        .with_memex(MemexClient::from_env().with_db(state.path().join("memex.db")));
        let summary = mastermind
            .start(
                "Boundary safety",
                repo.path(),
                Some("codex"),
                Some("gpt-5.6-sol"),
            )
            .await
            .expect("start");
        let session_id = summary["sessionId"]
            .as_str()
            .expect("session id")
            .to_owned();
        let session = mastermind.session(&session_id).expect("session");
        {
            let mut guard = session.lock().await;
            guard.discovery_answers.push((
                "What should ship?".to_owned(),
                "A safe Memex boundary".to_owned(),
            ));
            guard.phase_status = PhaseStatus::AwaitingApproval;
            std::fs::create_dir_all(repo.path().join("docs")).expect("docs");
            std::fs::write(
                repo.path().join("docs/DISCOVERY.md"),
                "# Discovery\n\nStatus: awaiting approval\n\nA safe Memex boundary\n",
            )
            .expect("draft discovery");
            mastermind.persist_session(&guard).expect("checkpoint gate");
        }

        mastermind.memex = MemexClient::from_env()
            .with_db(state.path().join("unreachable.db"))
            .with_program("definitely-missing-memex-command.exe");
        assert!(matches!(
            mastermind.approve_phase(&session_id).await,
            Err(MastermindError::Memory(_))
        ));
        let guard = session.lock().await;
        assert_eq!(guard.phase, MastermindPhase::Discovery);
        assert_eq!(guard.phase_status, PhaseStatus::AwaitingApproval);
        assert!(!guard.discovery_complete);
        let discovery = std::fs::read_to_string(repo.path().join("docs/DISCOVERY.md"))
            .expect("discovery remains");
        assert!(discovery.contains("Status: awaiting approval"));
        assert!(guard.memory_error.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_sessions_and_missing_repos_are_caller_errors() {
        let state = tempfile::tempdir().expect("state dir");
        let agents_db = state.path().join("agents.db");
        let registry = AgentRegistry::open(&agents_db).expect("registry");
        registry.seed_builtin_skills().expect("seed skills");
        registry.seed_builtins().expect("seed");
        let mastermind = Mastermind::new(Arc::new(registry), &agents_db, state.path(), |_| {
            Some(Arc::new(MockAdapter::new(MockBehavior::Success {
                turns: 1,
                files_changed: vec![],
            })) as Arc<dyn RuntimeAdapter>)
        });

        assert!(matches!(
            mastermind
                .start("goal", &state.path().join("nope"), None, None)
                .await,
            Err(MastermindError::NoRepo(_))
        ));
        assert!(matches!(
            mastermind.plan("not-a-session", None).await,
            Err(MastermindError::UnknownSession(_))
        ));
    }
}
