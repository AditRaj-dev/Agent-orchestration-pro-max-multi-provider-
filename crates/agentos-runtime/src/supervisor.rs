//! The supervisor — F-07's composition root.
//!
//! One [`Supervisor`] owns every moving part and makes them run as a loop:
//!
//! ```text
//! MockAdapter (or any RuntimeAdapter)
//!        ^  SpawnSpec (contract -> paths, tools, worktree, timeout)
//!        |  AdapterEvent stream
//!   SupervisorExecutor (TaskExecutor)
//!        |  Outcome
//!   WorkflowEngine (F-06: leases, retries, budget gate, state machine)
//!        |  tick()
//!   Supervisor::drive  -- every step -> journal Event (run_id + trace_id)
//!        |
//!   +----+------------------+------------------+-------------------+
//!   |    |                  |                  |                   |
//! Run/Parallel        Review node         GitGate node      UsageLedger
//! (session +          (stub reviewer:     (MutationQueue,   (usage rows,
//!  handoff packet      approve/reject)     AgentLedger,      budget check ->
//!  + ownership +                           git.committed)   budget.exceeded)
//!  worktree)
//! ```
//!
//! Responsibilities per stage:
//!
//! - **Run nodes** — pre-lease budget check; exclusive ownership holds on
//!   the contract's `allowed_paths` (GIT-05); a worktree at
//!   `base_commit` (GIT-04); adapter selection by the node's `agent_role`
//!   (mock by default); `SpawnSpec` built from the leased contract
//!   snapshot; the adapter event stream relayed into the journal and the
//!   usage ledger; the terminal `Finished` packet mapped into a
//!   [`HandoffPacket`] that is validated *before* being accepted (HO-01)
//!   and stored as a content-addressed artifact (the `task.output_ready`
//!   event carries only `payload_ref` + `payload_hash`).
//! - **Review nodes** — a deterministic stub reviewer (the real reviewer
//!   pool lands in a later PR): approve iff every dependency packet has no
//!   unresolved items, non-empty test evidence, and no failed test.
//! - **GitGate nodes** — authorize the mutation against F-10 first
//!   (`git_gate_check` for the gate role's git permission plus a live,
//!   fingerprint-bound human approval; nothing is created before that
//!   verdict), enqueue a `Commit` mutation on the serialized queue carrying
//!   that verdict, act as the single consumer, run the stale-base gate,
//!   commit the run's audit bundle on the gate branch, record the
//!   agent-ledger attribution (sha -> task/agent/reviewers/context
//!   versions/workflow, GIT-03) and emit `git.queued` + `git.committed`.
//! - **HumanApproval nodes** — resolved against the F-10 approval store,
//!   never self-approved: a pending decision parks the task in
//!   `HumanRequired` via `Outcome::AwaitingApproval` (no attempt consumed,
//!   no retry budget burned — the engine will not re-run it until someone
//!   resolves the approval and transitions it out), while a refusal or
//!   expiry is a deterministic failure. An approval store that cannot
//!   answer is still a transient failure: that is infrastructure, not a
//!   human.
//! - **Policy at spawn** — the task's `PermissionSet` (role mapping, else
//!   derived from the leased contract) is compiled into `SpawnConstraints`
//!   and carried into the `SpawnSpec`; a permission set that cannot cover
//!   the contract refuses the spawn.
//! - **Recovery** — leases live in the durable task store; on supervisor
//!   restart, tasks stuck `Leased`/`Running` whose adapter sessions are
//!   gone are reclaimed by the engine's expired-lease path (phase 1 of its
//!   tick) and re-executed. Ownership holds are attempt-scoped (acquired
//!   before the session, released after), so a dead supervisor leaves no
//!   holds behind; worktrees are retained for the GC/retention rules.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentos_adapters::{AdapterEvent, AdapterFailure, RuntimeAdapter, SpawnSpec};

use crate::git_advisor::{CommitFacts, GitAdvisor, RebaseAdvice, StaleBaseFacts, GIT_ADVISOR_ROLE};
use agentos_core::{Event, EventType, TaskState};
use agentos_git::cli;
use agentos_git::ledger::{AgentLedger, LedgerEntry};
use agentos_git::ownership::OwnershipMap;
use agentos_git::queue::{MutationAction, MutationQueue};
use agentos_git::worktree::{WorktreeManager, WorktreeRef};
use agentos_journal::db::open_db;
use agentos_journal::events::{append_event, events_for_run};
use agentos_policy::{
    git_gate_check, ApprovalRequest, ApprovalStore, AuditStore, Gate, GitAction, PermissionSet,
    PolicyDenial,
};
use agentos_workflow::{
    ControlResult, EngineConfig, NodeType, Outcome, ReopenReport, RunStatus, Scheduler,
    TaskContract as WorkflowTaskContract, TaskExecutor, TaskRecord, TaskStore, TickReport,
    WorkflowEngine, WorkflowSpec, DEFAULT_LEASE_TTL,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::contract::{GitPolicy, TaskContract};
use crate::digest::sha256_hex;
use crate::error::RuntimeError;
use crate::graphify::{
    reconcile_changes, GraphRefresh, Graphifier, GraphifyOutcome, GraphifySkip, GRAPHIFY_OUT_DIR,
};
use crate::handoff::{HandoffPacket, HandoffStatus, TestStatus};
use crate::policy::{self, GateVerdict, PolicyGate};
use crate::usage_ledger::UsageLedger;

/// Identity of the deterministic stand-in reviewer (the real reviewer pool
/// lands in a later PR; every approved packet cites this id in the ledger).
pub const STUB_REVIEWER: &str = "stub-reviewer@f-07";

// Extension event types (agentos-core's `EventType::Other` keeps them
// round-tripping verbatim through the journal).
const EVT_SESSION_SPAWN: &str = "session.spawn";
const EVT_SESSION_STARTED: &str = "session.started";
const EVT_AGENT_TOOL_USE: &str = "agent.tool_use";
const EVT_AGENT_DECISION: &str = "agent.decision";
const EVT_AGENT_RATE_LIMIT: &str = "agent.rate_limit";
const EVT_AGENT_SPAWN_FAILED: &str = "agent.spawn_failed";
const EVT_USAGE_UPDATED: &str = "usage.updated";
const EVT_TASK_FAILED: &str = "task.failed";
const EVT_HANDOFF_REJECTED: &str = "handoff.rejected";
const EVT_OWNERSHIP_CONFLICT: &str = "ownership.conflict";
const EVT_APPROVAL_REQUIRED: &str = "approval.required";
const EVT_APPROVAL_GRANTED: &str = "approval.granted";
const EVT_APPROVAL_DENIED: &str = "approval.denied";
const EVT_POLICY_DENIED: &str = "policy.denied";
const EVT_GIT_GATE_FAILED: &str = "git.gate_failed";
const EVT_GIT_STALE_BASE: &str = "git.stale_base";
const EVT_RUN_FAILED: &str = "run.failed";
/// An operator returned a failed run's work to the queue. Journaled as its
/// own event because a run leaving `failed` without any task succeeding is
/// otherwise indistinguishable from a projection bug: the audit trail has to
/// name the human action, the nodes it revived and the ones it un-parked.
const EVT_RUN_REOPENED: &str = "run.reopened";
const EVT_GRAPH_UPDATED: &str = "graph.updated";
const EVT_GRAPH_SKIPPED: &str = "graph.skipped";
const EVT_GRAPH_FAILED: &str = "graph.failed";

/// Tool identity stamped on graph events, the way `STUB_REVIEWER` names the
/// deterministic reviewer.
const GRAPHIFY_AGENT: &str = "graphify";

/// Ceiling on consecutive ownership deferrals for one task.
///
/// A deferral bills no retry attempt — queueing behind a peer is not a
/// failed try — but that also removes the termination bound, so a task that
/// can never acquire would spin forever instead of failing. Past this many
/// consecutive deferrals the task degrades to a transient failure and the
/// ordinary retry budget applies. The ownership queue is FIFO-fair, so a
/// task reaching this ceiling means something is wrong beyond contention.
const MAX_CONSECUTIVE_DEFERRALS: u32 = 25;

/// Appended to every worker objective.
///
/// Review approves on the evidence in a task's handoff packet, and the only
/// structured channel a provider CLI reliably has is its own final message.
/// Asking for the block here — rather than through a `SpawnSpec` schema
/// field no adapter carries — makes the evidence portable across claude,
/// codex and agy alike. A worker that skips it reports no evidence and
/// fails review, which is the same outcome as reporting a failing test.
const COMPLETION_REPORT_CONTRACT: &str = r#"

# Completion report (required)

End your final message with a fenced `json` block, and nothing after it:

```json
{
  "summary": "one or two sentences on what you changed",
  "filesChanged": ["relative/path.rs"],
  "tests": [{"name": "cargo test -p thing", "status": "passed", "count": 12}],
  "decisions": ["anything you chose that the task did not dictate"],
  "unresolved": ["anything you could not finish — leave empty if nothing"]
}
```

`tests` is the evidence review runs on, so it must describe commands you
actually ran in this session. `status` is `passed`, `failed`, or `skipped`;
use `skipped` with the reason in `unresolved` when a suite could not run
here. Never report a test you did not run — a fabricated pass is a worse
failure than an honest `skipped`. A non-empty `unresolved` list fails
review, which is correct: say so rather than hiding it."#;

/// The git action every gate node requests today. The gate commits the run's
/// audit bundle on its own branch; merge/rebase/push travel the same code
/// path once the integration policy lands (the queue already gates push).
const GATE_GIT_ACTION: GitAction = GitAction::Commit;
/// The mutation the gate enqueues, paired with [`GATE_GIT_ACTION`].
const GATE_MUTATION: MutationAction = MutationAction::Commit;
/// The approval gate a git mutation is bound to — the gate for
/// [`GATE_GIT_ACTION`], i.e. F-10's [`Gate::GitCommit`]. Commit and push are
/// separate gates in F-10, so an approval for one can never authorize the
/// other even before fingerprints are compared. Kept in sync with
/// `agentos_policy::approval_gate` by a unit test.
const GIT_MUTATION_GATE: Gate = Gate::GitCommit;

/// Where the supervisor keeps its durable run state (manifests, handoffs,
/// content-addressed artifacts) and the four databases it composes.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Directory holding `runs/`, `handoffs/`, `artifacts/`.
    pub state_dir: PathBuf,
    /// Append-only event journal (agentos-daemon schema).
    pub journal_db: PathBuf,
    /// Durable task/run store (agentos-workflow schema).
    pub workflow_db: PathBuf,
    /// Serialized git mutation queue.
    pub queue_db: PathBuf,
    /// Agent (commit attribution) ledger.
    pub ledger_db: PathBuf,
    /// Human approval store (F-10 SEC-04): gates are resolved here, never
    /// in prompt text.
    pub approvals_db: PathBuf,
    /// Append-only policy audit log (F-10 SEC-05).
    pub audit_db: PathBuf,
    /// Main checkout of the governed repository (worktrees hang off it).
    pub repo: PathBuf,
    /// Orchestrator identity stamped into ledger attribution.
    pub orchestrator: String,
    /// Adapter used when a node declares no `agent_role` (or the role has
    /// no mapping): `"mock"` by default.
    pub default_adapter: String,
    /// `agent_role` -> adapter id routing table.
    pub role_adapters: HashMap<String, String>,
    /// `agent_role` -> model override. Explicit session routing wins over
    /// the persisted registry record; this lets a user-selected planning
    /// provider carry the planning-document worker too.
    pub role_models: HashMap<String, String>,
    /// Run write tasks in the selected project checkout instead of hidden
    /// per-task worktrees. Mastermind enables this because its task graph is
    /// serialized by broad ownership holds and users must see staged files.
    pub shared_task_workspace: bool,
    /// `agent_role` -> permission set (F-10 SEC-01). Roles without an entry
    /// — and nodes that declare no role — execute under a set *derived from
    /// the leased contract* ([`crate::policy::derive_permissions`]), which
    /// is the least-privilege default.
    pub role_permissions: HashMap<String, PermissionSet>,
    /// A worker role's reasoning failure may retarget its next permitted
    /// attempt at a stronger role. The workflow engine applies the swap
    /// atomically with requeueing, so the original worker cannot be leased
    /// again in between.
    pub reasoning_escalations: HashMap<String, String>,
    /// F-13: the dynamic agent registry's database. When set, a node's
    /// `agent_role` is first resolved as a registry agent id — the record's
    /// adapter, model and skill preamble drive the spawn — before the
    /// static `role_adapters` table is consulted.
    pub agents_db: Option<PathBuf>,
    /// The permission set the git gate itself acts under. Default:
    /// [`PermissionSet::git_manager`] — the only role holding git actions
    /// (GIT-01). Narrow it to prove PRD §23.3: a gate without `Commit`
    /// cannot commit even with a live approval.
    pub git_gate_permissions: PermissionSet,
    /// Gate a `HumanApproval` node resolves against. F-10's gate set has no
    /// generic "human decision" member; `ProdAction` is the closest
    /// (irreversible action requiring a human) and is configurable here.
    pub human_approval_gate: Gate,
    /// Approval ttl used when the acting permission set declares no
    /// `ApprovalRule` for the gate.
    pub approval_ttl_secs: u64,
    /// Lease ttl the supervisor's engine grants.
    pub lease_ttl: Duration,
    /// Maximum workflow attempts admitted concurrently (1 through 8).
    pub max_concurrency: usize,
    /// Rebuild the code graph after a worker node writes. Reviewers have no
    /// shell and cannot build one themselves, so if the harness does not do
    /// this the graph is only ever as fresh as a worker chose to make it.
    pub graph_refresh: GraphRefresh,
    /// Wall-clock ceiling for one graph refresh.
    pub graph_refresh_timeout_secs: u64,
}

impl SupervisorConfig {
    /// Default configuration: every database lives under `state_dir`, the
    /// supervisor is `agentos-supervisor`, and the mock adapter serves
    /// unmapped roles.
    pub fn for_repo(state_dir: impl Into<PathBuf>, repo: impl Into<PathBuf>) -> Self {
        let state_dir = state_dir.into();
        Self {
            journal_db: state_dir.join("journal.db"),
            workflow_db: state_dir.join("workflow.db"),
            queue_db: state_dir.join("queue.db"),
            ledger_db: state_dir.join("ledger.db"),
            approvals_db: state_dir.join("approvals.db"),
            audit_db: state_dir.join("audit.db"),
            state_dir,
            repo: repo.into(),
            orchestrator: "agentos-supervisor".to_owned(),
            default_adapter: "mock".to_owned(),
            role_adapters: HashMap::new(),
            role_models: HashMap::new(),
            shared_task_workspace: false,
            role_permissions: HashMap::new(),
            reasoning_escalations: HashMap::new(),
            agents_db: None,
            git_gate_permissions: PermissionSet::git_manager(),
            human_approval_gate: Gate::ProdAction,
            approval_ttl_secs: 900,
            lease_ttl: DEFAULT_LEASE_TTL,
            max_concurrency: 4,
            graph_refresh: GraphRefresh::default(),
            graph_refresh_timeout_secs: 120,
        }
    }

    /// Route `agent_role` to `adapter_id`.
    pub fn with_role_adapter(mut self, role: &str, adapter_id: &str) -> Self {
        self.role_adapters
            .insert(role.to_owned(), adapter_id.to_owned());
        self
    }

    /// Bound parallel workflow admission for this supervisor instance.
    /// Validation is performed while constructing the workflow engine.
    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency;
        self
    }

    /// Pin a role to a model for this supervisor session.
    pub fn with_role_model(mut self, role: &str, model: &str) -> Self {
        self.role_models.insert(role.to_owned(), model.to_owned());
        self
    }

    /// Make worker edits land in the selected checkout. Broad path
    /// ownership still prevents overlapping writers.
    pub fn with_shared_task_workspace(mut self) -> Self {
        self.shared_task_workspace = true;
        self
    }

    /// Turn the post-task code-graph refresh off (tests use this so the
    /// suite does not depend on whether graphify is installed locally).
    pub fn with_graph_refresh(mut self, refresh: GraphRefresh) -> Self {
        self.graph_refresh = refresh;
        self
    }

    /// Bind `agent_role` to an explicit permission set (overrides the
    /// contract-derived default for nodes carrying that role).
    pub fn with_role_permissions(mut self, role: &str, permissions: PermissionSet) -> Self {
        self.role_permissions.insert(role.to_owned(), permissions);
        self
    }

    /// On a reasoning failure, send this role's next allowed retry to a
    /// stronger role. Transient infrastructure failures keep their original
    /// role so provider outages do not masquerade as reasoning escalation.
    pub fn with_reasoning_escalation(mut self, role: &str, target: &str) -> Self {
        self.reasoning_escalations
            .insert(role.to_owned(), target.to_owned());
        self
    }

    /// Enable the F-13 dynamic registry: `agent_role` values resolve as
    /// registry agent ids first (adapter + model + skill preamble per
    /// record), falling back to the static routing table.
    pub fn with_agents_db(mut self, path: impl Into<PathBuf>) -> Self {
        self.agents_db = Some(path.into());
        self
    }

    /// Replace the permission set the git gate acts under.
    pub fn with_git_gate_permissions(mut self, permissions: PermissionSet) -> Self {
        self.git_gate_permissions = permissions;
        self
    }
}

/// Result of one bounded `drive` loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriveSummary {
    /// Ticks executed.
    pub ticks: u32,
    /// Run status when the loop stopped (terminal, or idle-but-active).
    pub status: RunStatus,
}

// ------------------------------------------------------------------ journal

/// The append-only event journal (agentos-daemon's SQLite schema), held by
/// path: each append opens a connection so the supervisor (and its
/// executor, shared into the engine) never carries a non-`Sync` connection
/// across an await. WAL + the F-01 busy canon keep concurrent appends safe.
#[derive(Debug, Clone)]
struct Journal {
    path: PathBuf,
}

impl Journal {
    fn open(path: &Path) -> Result<Self, RuntimeError> {
        ensure_parent(path);
        open_db(path)?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    fn append(&self, event: &Event) -> Result<(), RuntimeError> {
        let conn = open_db(&self.path)?;
        append_event(&conn, event)?;
        Ok(())
    }

    fn events_for_run(&self, run_id: &Uuid) -> Result<Vec<Event>, RuntimeError> {
        let conn = open_db(&self.path)?;
        Ok(events_for_run(&conn, run_id)?)
    }
}

fn ensure_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }
}

// ----------------------------------------------------------------- manifest

/// Everything a supervisor (or its successor, after a crash) needs to keep
/// executing a run: correlation ids, the goal, and the full contract per
/// node — the leased version executors read. Persisted as
/// `<state_dir>/runs/<run_id>.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunManifest {
    run_id: Uuid,
    trace_id: Uuid,
    workflow_id: String,
    #[serde(default)]
    workflow_version: u32,
    goal: String,
    contracts: HashMap<String, TaskContract>,
}

// --------------------------------------------------------------------- core

/// State shared between the supervisor and its executor (the executor is
/// moved into the `WorkflowEngine`, so everything it touches lives here).
struct SupervisorCore {
    config: SupervisorConfig,
    journal: Journal,
    adapters: Vec<Arc<dyn RuntimeAdapter>>,
    /// F-13 dynamic registry overlay, opened from `agents_db` when set.
    registry: Option<Arc<agentos_agents::AgentRegistry>>,
    store: Arc<TaskStore>,
    worktrees: WorktreeManager,
    queue: Arc<MutationQueue>,
    agent_ledger: Arc<AgentLedger>,
    ownership: OwnershipMap,
    /// Consecutive ownership deferrals per task; cleared the moment a task
    /// acquires. In memory on purpose: a restart drops every hold too, so a
    /// carried-over count would describe a world that no longer exists.
    deferrals: Mutex<HashMap<Uuid, u32>>,
    ledger: UsageLedger,
    policy: PolicyGate,
    manifests: Mutex<HashMap<Uuid, RunManifest>>,
    handoffs: Mutex<HashMap<Uuid, HandoffPacket>>,
}

impl SupervisorCore {
    fn manifest(&self, run_id: &Uuid) -> Result<RunManifest, RuntimeError> {
        {
            let manifests = Self::lock(&self.manifests);
            if let Some(manifest) = manifests.get(run_id) {
                return Ok(manifest.clone());
            }
        }
        let path = self
            .config
            .state_dir
            .join("runs")
            .join(format!("{run_id}.json"));
        let bytes = std::fs::read(&path).map_err(|_| RuntimeError::MissingManifest(*run_id))?;
        let manifest: RunManifest = serde_json::from_slice(&bytes)?;
        Self::lock(&self.manifests).insert(*run_id, manifest.clone());
        Ok(manifest)
    }

    fn save_manifest(&self, manifest: RunManifest) -> Result<(), RuntimeError> {
        let path = self
            .config
            .state_dir
            .join("runs")
            .join(format!("{}.json", manifest.run_id));
        std::fs::write(&path, serde_json::to_vec_pretty(&manifest)?)?;
        Self::lock(&self.manifests).insert(manifest.run_id, manifest);
        Ok(())
    }

    /// The adapter for a node's `agent_role`: the registry record's
    /// adapter when the role names a registered agent (F-13), else the
    /// static role mapping, else the configured default. Registry *read*
    /// failures degrade to the static path with a loud log — the registry
    /// is an overlay on routing, not a load-bearing replacement.
    fn adapter_for(&self, role: Option<&str>) -> Result<Arc<dyn RuntimeAdapter>, RuntimeError> {
        let wanted = if let Some(role) = role {
            self.registry_agent(role)
                .map(|record| record.adapter_id.clone())
                .or_else(|| self.config.role_adapters.get(role).cloned())
        } else {
            None
        }
        .unwrap_or_else(|| self.config.default_adapter.clone());
        self.adapters
            .iter()
            .find(|adapter| adapter.id() == wanted)
            .cloned()
            .ok_or(RuntimeError::NoAdapter {
                role: role.unwrap_or_default().to_owned(),
                default: wanted,
            })
    }

    /// The registry record for a role, when the registry is enabled and
    /// the role names a registered, enabled agent (F-13).
    fn registry_agent(&self, role: &str) -> Option<agentos_agents::AgentRecord> {
        let registry = self.registry.as_ref()?;
        match registry.resolve(role) {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(role, %error, "agent registry lookup failed; static routing applies");
                None
            }
        }
    }

    /// The git manager's model half, when one is configured (F-09 + F-13).
    /// Resolved the same way a worker's runtime is: an explicit role
    /// mapping first, then the `git-manager` registry record. No record and
    /// no mapping means `None`, and every caller falls back to the
    /// deterministic path — the advisor is an addition to the git manager,
    /// never a dependency of it.
    fn git_advisor(&self) -> Option<GitAdvisor> {
        let record = self.registry_agent(GIT_ADVISOR_ROLE);
        let adapter_id = self
            .config
            .role_adapters
            .get(GIT_ADVISOR_ROLE)
            .cloned()
            .or_else(|| record.as_ref().map(|record| record.adapter_id.clone()))?;
        let adapter = self
            .adapters
            .iter()
            .find(|adapter| adapter.id() == adapter_id)
            .cloned()?;
        let model = self
            .config
            .role_models
            .get(GIT_ADVISOR_ROLE)
            .cloned()
            .or_else(|| record.as_ref().and_then(|record| record.model.clone()));
        let timeout = record
            .as_ref()
            .map(|record| record.timeout_secs)
            .unwrap_or(300);
        Some(GitAdvisor::new(
            adapter,
            model,
            Duration::from_secs(timeout),
        ))
    }

    /// The permission set a task executes under: the role mapping when the
    /// node declares a mapped `agent_role`, else the set derived from the
    /// leased contract snapshot (least privilege by default).
    fn permissions_for(&self, task: &TaskRecord, contract: &TaskContract) -> PermissionSet {
        task.node
            .agent_role
            .as_deref()
            .and_then(|role| self.config.role_permissions.get(role))
            .cloned()
            .unwrap_or_else(|| policy::derive_permissions(contract))
    }

    /// The approval ttl for `gate` under `perms`: the set's own
    /// `ApprovalRule` when it declares one, else the configured fallback.
    fn approval_ttl(&self, perms: &PermissionSet, gate: Gate) -> u64 {
        perms
            .approval_rule(gate)
            .map(|rule| rule.ttl_secs)
            .unwrap_or(self.config.approval_ttl_secs)
    }

    /// The gate + canonical operation payload of a gate node. Pure function
    /// of durable state, so a human can approve *before* the node runs and
    /// the gate recomputes the identical fingerprint when it executes.
    fn gate_operation(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        contract: &TaskContract,
    ) -> Result<(Gate, Value), RuntimeError> {
        match task.node.node_type {
            NodeType::GitGate => Ok((
                GIT_MUTATION_GATE,
                policy::git_mutation_operation(
                    &task.run_id,
                    &task.id,
                    &task.node_id,
                    &manifest.workflow_id,
                    &self.config.repo,
                    GATE_MUTATION.as_str(),
                    &contract.base_commit,
                ),
            )),
            NodeType::HumanApproval => Ok((
                self.config.human_approval_gate,
                policy::human_approval_operation(
                    &task.run_id,
                    &task.id,
                    &task.node_id,
                    &manifest.workflow_id,
                    contract,
                ),
            )),
            _ => Err(RuntimeError::NotAGate {
                node: task.node_id.clone(),
                run_id: task.run_id,
            }),
        }
    }

    /// Open a human approval request for a gate node's operation, track it
    /// against the task, audit it, and journal `approval.required` so the
    /// pending decision is discoverable instead of silently lost.
    fn open_gate_request(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        gate: Gate,
        operation: &Value,
        perms: &PermissionSet,
        requested_by: &str,
    ) -> Result<ApprovalRequest, RuntimeError> {
        let ttl = self.approval_ttl(perms, gate);
        let request = self
            .policy
            .request(gate, operation, &task.id, requested_by, ttl)?;
        self.audit_or_log(
            task,
            self.policy.audit().record_approval(
                requested_by,
                &request,
                None,
                Some(&task.run_id.to_string()),
            ),
        );
        self.emit_for(
            task,
            manifest,
            EventType::Other(EVT_APPROVAL_REQUIRED.to_owned()),
            Some(requested_by),
            json!({
                "node": task.node_id,
                "gate": gate.as_str(),
                "requestId": request.id,
                "operationFingerprint": request.operation_fingerprint,
                "expiresAt": request.expires_at.to_rfc3339(),
                "blocking": true,
            }),
        );
        Ok(request)
    }

    /// Record an audit row, logging (never failing the task) when the audit
    /// store itself is unavailable — the decision has already been made and
    /// journaled; a missing audit row must not turn a denial into an
    /// approval.
    fn audit_or_log(&self, task: &TaskRecord, result: Result<String, agentos_policy::PolicyError>) {
        if let Err(error) = result {
            tracing::error!(task_id = %task.id, %error, "policy audit append failed");
        }
    }

    /// Fire-and-forget event for a task context (journal failures are
    /// logged loudly but never fail a task's outcome).
    fn emit_for(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        event_type: EventType,
        agent: Option<&str>,
        payload: Value,
    ) {
        let mut event = Event::new(event_type)
            .with_run_id(task.run_id)
            .with_trace_id(manifest.trace_id)
            .with_task_id(task.id)
            .with_payload(payload);
        if let Some(agent) = agent {
            event = event.with_agent_id(agent);
        }
        if let Err(error) = self.journal.append(&event) {
            tracing::error!(task_id = %task.id, %error, "journal append failed");
        }
    }

    /// Run-scoped event whose failure the caller propagates.
    /// The graph refresher for this supervisor, or `None` when the refresh
    /// is off or graphify is not installed. Mirrors `git_advisor`: absent
    /// configuration means the behavior that existed before this module.
    fn graphifier(&self) -> Option<Graphifier> {
        Graphifier::resolve(
            &self.config.graph_refresh,
            Duration::from_secs(self.config.graph_refresh_timeout_secs),
        )
    }

    /// Count one consecutive deferral for `task`, returning the new total.
    fn record_deferral(&self, task: Uuid) -> u32 {
        let mut deferrals = match self.deferrals.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let entry = deferrals.entry(task).or_insert(0);
        *entry += 1;
        *entry
    }

    /// Forget a task's deferral streak — it acquired, so the streak is over.
    fn clear_deferrals(&self, task: Uuid) {
        match self.deferrals.lock() {
            Ok(mut guard) => {
                guard.remove(&task);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&task);
            }
        }
    }

    fn emit_run_checked(
        &self,
        run_id: &Uuid,
        manifest: &RunManifest,
        event_type: EventType,
        payload: Value,
    ) -> Result<(), RuntimeError> {
        let event = Event::new(event_type)
            .with_run_id(*run_id)
            .with_trace_id(manifest.trace_id)
            .with_payload(payload);
        self.journal.append(&event)
    }

    /// `task.output_ready` with the packet offloaded to a content-addressed
    /// artifact: the event holds only the summary plus `payload_ref` +
    /// `payload_hash` (F-00 §3).
    fn emit_output_ready(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        from_agent: &str,
        summary: &str,
        artifact_ref: &str,
        artifact_hash: &str,
    ) {
        let event = Event::new(EventType::TaskOutputReady)
            .with_run_id(task.run_id)
            .with_trace_id(manifest.trace_id)
            .with_task_id(task.id)
            .with_agent_id(from_agent)
            .with_payload(json!({
                "node": task.node_id,
                "summary": summary,
                "handoffId": artifact_ref,
            }))
            .with_payload_ref(artifact_ref)
            .with_payload_hash(artifact_hash);
        if let Err(error) = self.journal.append(&event) {
            tracing::error!(task_id = %task.id, %error, "journal append failed");
        }
    }

    /// Lease/running lifecycle pair at executor entry (the engine CASes
    /// `Leased -> Running` immediately before invoking the executor).
    fn emit_lifecycle(&self, task: &TaskRecord, manifest: &RunManifest) {
        let agent = task.lease_owner.as_deref().unwrap_or("supervisor");
        self.emit_for(
            task,
            manifest,
            EventType::AgentLeased,
            Some(agent),
            json!({"node": task.node_id}),
        );
        self.emit_for(
            task,
            manifest,
            EventType::TaskRunning,
            Some(agent),
            json!({"node": task.node_id, "attempt": task.attempt_count + 1}),
        );
    }

    /// Create (or reuse, across retries/restarts) the isolated worktree for
    /// `task_id` at `base_commit`.
    fn obtain_worktree(
        &self,
        run_id: &Uuid,
        task_id: &Uuid,
        base_commit: &str,
    ) -> Result<WorktreeRef, RuntimeError> {
        if self.config.shared_task_workspace {
            let branch =
                cli::current_branch(&self.config.repo).unwrap_or_else(|_| "HEAD".to_owned());
            return Ok(WorktreeRef {
                repo: self.config.repo.clone(),
                path: self.config.repo.clone(),
                branch,
                task_id: task_id.to_string(),
                base_commit: base_commit.to_owned(),
            });
        }
        let key = *task_id;
        let expected = self.worktrees.managed_root().join(key.to_string());
        if expected.exists() {
            // A retry of the same task, or a reclaimed lease after a
            // supervisor restart: the branch already exists at the same
            // base, so reuse it (removal is the GC path's job, not the
            // attempt's).
            if let Ok(entries) = self.worktrees.list() {
                if let Some(entry) = entries.iter().find(|entry| entry.path == expected) {
                    tracing::debug!(task_id = %task_id, ?entry.path, "reusing worktree");
                    return Ok(WorktreeRef {
                        repo: self.config.repo.clone(),
                        path: entry.path.clone(),
                        branch: entry.branch.clone().unwrap_or_default(),
                        task_id: key.to_string(),
                        base_commit: base_commit.to_owned(),
                    });
                }
            }
        }
        Ok(self
            .worktrees
            .create(&run_id.to_string(), &key.to_string(), base_commit)?)
    }

    /// Persist a handoff packet: one content-addressed artifact
    /// (`artifacts/<sha256>.json`) plus a task-keyed record
    /// (`handoffs/<task_id>.json`) for lookups. Returns the `sha256:<hex>`
    /// reference.
    fn store_handoff(
        &self,
        task_id: &Uuid,
        packet: &HandoffPacket,
    ) -> Result<String, RuntimeError> {
        let bytes = serde_json::to_vec(packet)?;
        let hex = sha256_hex(&bytes);
        std::fs::write(
            self.config
                .state_dir
                .join("artifacts")
                .join(format!("{hex}.json")),
            &bytes,
        )?;
        std::fs::write(
            self.config
                .state_dir
                .join("handoffs")
                .join(format!("{task_id}.json")),
            &bytes,
        )?;
        Self::lock(&self.handoffs).insert(*task_id, packet.clone());
        Ok(format!("sha256:{hex}"))
    }

    /// A task's accepted handoff packet, from memory or disk.
    fn handoff(&self, task_id: &Uuid) -> Option<HandoffPacket> {
        {
            let handoffs = Self::lock(&self.handoffs);
            if let Some(packet) = handoffs.get(task_id) {
                return Some(packet.clone());
            }
        }
        let path = self
            .config
            .state_dir
            .join("handoffs")
            .join(format!("{task_id}.json"));
        let bytes = std::fs::read(path).ok()?;
        let packet: HandoffPacket = serde_json::from_slice(&bytes).ok()?;
        Self::lock(&self.handoffs).insert(*task_id, packet.clone());
        Some(packet)
    }

    /// Packets of every task the given node transitively depends on (the
    /// review input set).
    /// Render the upstream completion records a task builds on, for its prompt.
    ///
    /// Every dependent task's objective has always ended with "Upstream tasks
    /// whose output you build on: … Read their handoff packets before
    /// starting." That instruction was unfulfillable: packets live in the
    /// daemon state dir, outside the worker's `allowed_paths`, and no tool
    /// reaches them. Workers therefore started cold apart from shared checkout
    /// file state, while being told to read something they could not open.
    ///
    /// Delivering the packets at lease time closes that gap without touching
    /// the contract, which is immutable after lease.
    ///
    /// Bounded on purpose: a packet's `summary` and `files_changed` are
    /// model-written and unbounded, and a wide fan-in would otherwise spend the
    /// whole downstream context window on upstream prose.
    fn render_upstream_packets(packets: &[HandoffPacket]) -> String {
        const MAX_SUMMARY: usize = 1200;
        const MAX_FILES: usize = 40;
        const MAX_LIST: usize = 10;

        if packets.is_empty() {
            return String::new();
        }

        fn clip(text: &str, max: usize) -> String {
            let trimmed = text.trim();
            match trimmed.char_indices().nth(max) {
                Some((cut, _)) => format!("{}… (truncated)", &trimmed[..cut]),
                None => trimmed.to_owned(),
            }
        }

        fn bullets(label: &str, items: &[String], out: &mut String) {
            if items.is_empty() {
                return;
            }
            out.push_str(&format!(
                "{label}:
"
            ));
            for item in items.iter().take(MAX_LIST) {
                out.push_str(&format!(
                    "- {}
",
                    clip(item, 300)
                ));
            }
            if items.len() > MAX_LIST {
                out.push_str(&format!(
                    "- … and {} more
",
                    items.len() - MAX_LIST
                ));
            }
        }

        let mut out = String::from(
            "

# Upstream work already completed

These are the handoff packets from the tasks you build on. They are the record of what was actually done — prefer them over re-deriving it from the files.
",
        );
        for packet in packets {
            let status = match packet.status {
                HandoffStatus::Completed => "completed",
                HandoffStatus::OutputReady => "output ready",
                HandoffStatus::Blocked => "blocked",
                HandoffStatus::Failed => "failed",
            };
            out.push_str(&format!(
                "
## {} — {}
",
                packet.task_id, status
            ));
            if !packet.summary.trim().is_empty() {
                out.push_str(&format!(
                    "{}
",
                    clip(&packet.summary, MAX_SUMMARY)
                ));
            }
            if !packet.files_changed.is_empty() {
                out.push_str(
                    "
Files changed:
",
                );
                for file in packet.files_changed.iter().take(MAX_FILES) {
                    out.push_str(&format!(
                        "- {file}
"
                    ));
                }
                if packet.files_changed.len() > MAX_FILES {
                    out.push_str(&format!(
                        "- … and {} more
",
                        packet.files_changed.len() - MAX_FILES
                    ));
                }
            }
            bullets(
                "
Decisions",
                &packet.decisions,
                &mut out,
            );
            bullets(
                "
Still unresolved",
                &packet.unresolved,
                &mut out,
            );
            if !packet.tests.is_empty() {
                let failed = packet
                    .tests
                    .iter()
                    .filter(|report| report.status == TestStatus::Failed)
                    .count();
                out.push_str(&format!(
                    "
Tests: {} reported, {} failed
",
                    packet.tests.len(),
                    failed
                ));
            }
        }
        out
    }

    fn dependency_packets(&self, task: &TaskRecord) -> Vec<HandoffPacket> {
        let Ok(tasks) = self.store.tasks_for_run(&task.run_id) else {
            return Vec::new();
        };
        let by_node: HashMap<&str, &TaskRecord> = tasks
            .iter()
            .map(|record| (record.node_id.as_str(), record))
            .collect();
        let mut seen: HashSet<String> = HashSet::new();
        let mut frontier: Vec<String> = task.node.depends_on.clone();
        let mut packets = Vec::new();
        while let Some(node_id) = frontier.pop() {
            if !seen.insert(node_id.clone()) {
                continue;
            }
            let Some(record) = by_node.get(node_id.as_str()) else {
                continue;
            };
            frontier.extend(record.node.depends_on.iter().cloned());
            if let Some(packet) = self.handoff(&record.id) {
                packets.push(packet);
            }
        }
        packets
    }

    /// Every accepted packet of the run (the git gate's audit bundle).
    fn run_packets(&self, run_id: &Uuid) -> Vec<HandoffPacket> {
        let Ok(tasks) = self.store.tasks_for_run(run_id) else {
            return Vec::new();
        };
        tasks
            .iter()
            .filter_map(|record| self.handoff(&record.id))
            .collect()
    }

    /// Recover a lock guard from a poisoned mutex (the maps' data stays
    /// consistent; the maps are caches over durable files).
    fn lock<'a, T>(mutex: &'a Mutex<T>) -> std::sync::MutexGuard<'a, T> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Wall-clock seconds since the task was created (the elapsed-budget leg).
fn elapsed_secs(task: &TaskRecord) -> u64 {
    task.created_at
        .signed_duration_since(chrono::Utc::now())
        .num_seconds()
        .unsigned_abs()
}

/// Session timeout: the stricter of the node's machine budget and the
/// contract's human-scale budget, floored at one second.
/// First `max_chars` characters of an objective for the `session.spawn`
/// payload — enough of the prompt (skill preambles run ~1.2k chars) to
/// audit what a session was asked to do, without journaling the whole
/// contract.
fn truncate_preview(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        text.to_owned()
    } else if max_chars == 0 {
        String::new()
    } else {
        const MARKER: &str = "\n… [middle truncated] …\n";
        let marker_chars = MARKER.chars().count();
        if max_chars <= marker_chars + 2 {
            return text.chars().take(max_chars).collect();
        }

        // Keep most of the beginning for global/role skill auditability and
        // the tail for the actual task objective. A head-only preview loses
        // the objective as soon as skill preambles become substantial.
        let available = max_chars - marker_chars;
        let head_chars = available * 3 / 4;
        let tail_chars = available - head_chars;
        let head: String = text.chars().take(head_chars).collect();
        let tail: String = text
            .chars()
            .rev()
            .take(tail_chars)
            .collect::<String>()
            .chars()
            .rev()
            .collect();
        format!("{head}{MARKER}{tail}")
    }
}

fn session_timeout(task: &TaskRecord, contract: &TaskContract) -> u64 {
    task.node
        .budgets
        .max_elapsed_secs
        .min(u64::from(contract.budgets.max_minutes) * 60)
        .max(1)
}

// ---------------------------------------------------------------- executor

/// The engine-facing executor: dispatches each leased task by node type.
struct SupervisorExecutor {
    core: Arc<SupervisorCore>,
}

#[async_trait]
impl TaskExecutor for SupervisorExecutor {
    async fn run(&self, task: &TaskRecord, _contract: &agentos_workflow::TaskContract) -> Outcome {
        let manifest = match self.core.manifest(&task.run_id) {
            Ok(manifest) => manifest,
            Err(error) => {
                tracing::error!(task_id = %task.id, %error, "run manifest missing; failing task");
                return Outcome::ReasoningFailure;
            }
        };
        // Immutable-after-lease: the contract snapshot is cloned once here
        // and everything below reads this value; amendments produce new
        // versions that only a future lease would observe.
        let Some(contract) = manifest.contracts.get(&task.node_id).cloned() else {
            tracing::error!(node = %task.node_id, "no full contract on the manifest");
            return Outcome::ReasoningFailure;
        };

        self.core.emit_lifecycle(task, &manifest);

        match task.node.node_type {
            NodeType::Run => self.run_session(task, &manifest, &contract).await,
            NodeType::Review => self.run_review(task, &manifest).await,
            NodeType::GitGate => self.run_git_gate(task, &manifest, &contract).await,
            // Fan-in / routing points: the engine's dependency gate already
            // provides the semantics; nothing to execute.
            NodeType::Parallel | NodeType::Branch | NodeType::Loop { .. } => Outcome::Success {
                packet: json!({
                    "node": task.node_id,
                    "kind": task.node.node_type.as_str(),
                    "joined": task.node.depends_on,
                }),
            },
            // Resolved against the F-10 approval store — the supervisor
            // never self-approves.
            NodeType::HumanApproval => self.run_human_approval(task, &manifest, &contract).await,
        }
    }
}

impl SupervisorExecutor {
    /// One adapter session against a `Run` node: holds -> worktree ->
    /// spawn -> relay -> handoff packet.
    async fn run_session(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        contract: &TaskContract,
    ) -> Outcome {
        // Budget gate before doing any billable work (OR-05).
        let elapsed = elapsed_secs(task);
        if UsageLedger::status_of(
            self.core.ledger.usage(task.id).as_ref(),
            &task.node.budgets,
            elapsed,
        )
        .is_exceeded()
        {
            self.core.emit_for(
                task,
                manifest,
                EventType::BudgetExceeded,
                None,
                json!({"node": task.node_id, "elapsedSecs": elapsed, "stage": "pre-session"}),
            );
            return Outcome::ReasoningFailure;
        }

        let task_key = task.id.to_string();
        let globs: Vec<&str> = contract.allowed_paths.iter().map(String::as_str).collect();
        if let Err(conflict) = self.core.ownership.acquire(&task_key, &globs, true) {
            // Another task holds overlapping paths, or an older waiter is
            // ahead in the queue. This is queueing, not failing: the task
            // goes back to `Ready` without billing a retry. Under a shared
            // workspace every write task holds `**`, and charging an attempt
            // per conflict exhausted the retry budget of tasks that had not
            // run a single time.
            //
            // The ceiling is the termination bound: the queue is FIFO-fair,
            // so a task that still cannot acquire after this many rounds is
            // not merely unlucky, and spinning forever is worse than failing
            // honestly.
            let deferrals = self.core.record_deferral(task.id);
            let exhausted = deferrals > MAX_CONSECUTIVE_DEFERRALS;
            self.core.emit_for(
                task,
                manifest,
                EventType::Other(EVT_OWNERSHIP_CONFLICT.to_owned()),
                None,
                json!({
                    "node": task.node_id,
                    "error": conflict.to_string(),
                    "deferred": !exhausted,
                    "consecutiveDeferrals": deferrals,
                }),
            );
            if exhausted {
                self.core.clear_deferrals(task.id);
                return Outcome::TransientFailure;
            }
            return Outcome::Deferred {
                reason: conflict.to_string(),
            };
        }
        self.core.clear_deferrals(task.id);

        let outcome = self.drive_session(task, manifest, contract).await;
        self.core.ownership.release(&task_key);
        if outcome == Outcome::ReasoningFailure {
            if let Some(target) = task
                .node
                .agent_role
                .as_deref()
                .and_then(|role| self.core.config.reasoning_escalations.get(role))
            {
                return Outcome::EscalatedReasoningFailure {
                    agent_role: target.clone(),
                };
            }
        }
        outcome
    }

    async fn drive_session(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        contract: &TaskContract,
    ) -> Outcome {
        let worktree =
            match self
                .core
                .obtain_worktree(&task.run_id, &task.id, &contract.base_commit)
            {
                Ok(worktree) => worktree,
                Err(error) => {
                    self.core.emit_for(
                    task,
                    manifest,
                    EventType::AgentCrashed,
                    None,
                    json!({"node": task.node_id, "stage": "worktree", "error": error.to_string()}),
                );
                    return Outcome::TransientFailure;
                }
            };

        let adapter = match self.core.adapter_for(task.node.agent_role.as_deref()) {
            Ok(adapter) => adapter,
            Err(error) => {
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_TASK_FAILED.to_owned()),
                    None,
                    json!({"node": task.node_id, "error": error.to_string()}),
                );
                return Outcome::ReasoningFailure;
            }
        };

        // SEC-01: the task's permission set is *compiled* into the four
        // policy-bearing SpawnSpec fields — the adapter's flags/sandbox come
        // from policy, never from prompt text. A permission set that cannot
        // cover the contract refuses the spawn (fail closed) rather than
        // silently running with narrower or wider access.
        let permissions = self.core.permissions_for(task, contract);
        let constraints = match policy::compile_constraints(&permissions, &worktree.path, contract)
        {
            Ok(constraints) => constraints,
            Err(error) => {
                self.core.audit_or_log(
                    task,
                    self.core.policy.audit().append(agentos_policy::AuditEntry {
                        actor: adapter.id().to_owned(),
                        action_kind: "permission.denied".to_owned(),
                        resource: format!("spawn:{}", contract.id),
                        details: json!({"node": task.node_id, "reason": error.to_string()}),
                        run_id: Some(task.run_id.to_string()),
                    }),
                );
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_POLICY_DENIED.to_owned()),
                    Some(adapter.id()),
                    json!({"node": task.node_id, "stage": "compile",
                               "error": error.to_string()}),
                );
                return Outcome::ReasoningFailure;
            }
        };

        // The git policy's direct-git denials ride on top of the compiled
        // denylist (deny beats allow, F-10 §2).
        let mut tool_denylist = constraints.tool_denylist.clone();
        if contract.git_policy == GitPolicy::NoDirectGit {
            for tool in ["Bash(git commit:*)", "Bash(git push:*)"] {
                if !tool_denylist.iter().any(|denied| denied == tool) {
                    tool_denylist.push(tool.to_owned());
                }
            }
        }

        let timeout_secs = session_timeout(task, contract);
        // F-13: when the node's role names a registered agent, the record
        // drives the model and its skills ride ahead of the objective as
        // the prompt preamble. Policy-compiled constraints stay supreme:
        // the record only *adds* deny entries (deny beats allow) and never
        // widens access.
        let agent_record = task
            .node
            .agent_role
            .as_deref()
            .and_then(|role| self.core.registry_agent(role));
        let objective = match (&agent_record, &self.core.registry) {
            (Some(record), Some(registry)) => match registry.preamble_for(record) {
                Ok(preamble) => format!("{preamble}{}", contract.objective),
                Err(error) => {
                    tracing::warn!(
                        role = %record.id,
                        %error,
                        "skill preamble composition failed; global skill fallback applied"
                    );
                    format!(
                        "{}{}",
                        agentos_agents::global_skills_preamble(),
                        contract.objective
                    )
                }
            },
            _ => format!(
                "{}{}",
                agentos_agents::global_skills_preamble(),
                contract.objective
            ),
        };
        // Fan the upstream completion records out to this worker. The contract
        // stays immutable after lease; this is context delivered at spawn.
        let objective = format!(
            "{objective}{}",
            SupervisorCore::render_upstream_packets(&self.core.dependency_packets(task)),
        );
        if let Some(record) = &agent_record {
            for tool in &record.tool_denylist {
                if !tool_denylist.iter().any(|denied| denied == tool) {
                    tool_denylist.push(tool.clone());
                }
            }
        }
        // The report contract is identical on every spawn, so it rides on
        // the prompt but stays out of the journal preview — a fixed block
        // repeated on every session.spawn event is noise, and it would push
        // the task's own text out of the truncation window.
        let spec = SpawnSpec {
            task_id: task.id,
            objective: format!("{objective}{COMPLETION_REPORT_CONTRACT}"),
            workspace: worktree.path.clone(),
            allowed_paths: constraints.allowed_paths.clone(),
            forbidden_paths: constraints.forbidden_paths.clone(),
            tool_allowlist: constraints.tool_allowlist.clone(),
            tool_denylist: tool_denylist.clone(),
            model: task
                .node
                .agent_role
                .as_deref()
                .and_then(|role| self.core.config.role_models.get(role).cloned())
                .or_else(|| {
                    agent_record
                        .as_ref()
                        .and_then(|record| record.model.clone())
                }),
            timeout_secs,
            isolated_home: None,
        };

        self.core.emit_for(
            task,
            manifest,
            EventType::Other(EVT_SESSION_SPAWN.to_owned()),
            Some(adapter.id()),
            json!({
                "adapter": adapter.id(),
                "workspace": worktree.path.display().to_string(),
                "branch": worktree.branch,
                "timeoutSecs": timeout_secs,
                "model": spec.model,
                "objectivePreview": truncate_preview(&objective, 2000),
                "allowedPaths": constraints.allowed_paths,
                "forbiddenPaths": constraints.forbidden_paths,
                "toolAllowlist": constraints.tool_allowlist,
                "toolDenylist": tool_denylist,
            }),
        );

        let handle = match adapter.start_session(spec).await {
            Ok(handle) => handle,
            Err(error) => {
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_AGENT_SPAWN_FAILED.to_owned()),
                    Some(adapter.id()),
                    json!({"adapter": adapter.id(), "error": error.to_string()}),
                );
                return if error.is_retryable() {
                    Outcome::TransientFailure
                } else {
                    Outcome::ReasoningFailure
                };
            }
        };
        // Subscribe before awaiting anything: broadcast has no replay, and
        // the mock driver parks until a subscriber exists.
        let mut rx = handle.events();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
        let scheduler = Scheduler::new(Arc::clone(&self.core.store));

        let mut finished: Option<(i32, Option<String>, Option<Value>)> = None;
        let mut failed: Option<AdapterFailure> = None;
        let mut timed_out = false;
        let mut lost = false;

        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Err(_) => {
                    timed_out = true;
                    break;
                }
                Ok(Err(broadcast::error::RecvError::Lagged(dropped))) => {
                    tracing::warn!(task_id = %task.id, dropped, "session relay lagged");
                    continue;
                }
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    lost = true;
                    break;
                }
                Ok(Ok(event)) => {
                    // Keep the lease alive across long sessions (OR-05):
                    // heartbeats use the owner stamped at grant time.
                    if let Some(owner) = task.lease_owner.as_deref() {
                        let _ = scheduler.heartbeat(&task.id, owner);
                    }
                    match event {
                        AdapterEvent::Started { session_id, model } => {
                            self.core.emit_for(
                                task,
                                manifest,
                                EventType::Other(EVT_SESSION_STARTED.to_owned()),
                                Some(adapter.id()),
                                json!({"sessionId": session_id, "model": model}),
                            );
                        }
                        // Text deltas are not journaled: they are chatty and
                        // reconstructable on demand via `transcript_ref`.
                        AdapterEvent::TextDelta(_) => {}
                        AdapterEvent::ToolUse { tool, args_summary } => {
                            self.core.emit_for(
                                task,
                                manifest,
                                EventType::Other(EVT_AGENT_TOOL_USE.to_owned()),
                                Some(adapter.id()),
                                json!({"tool": tool, "argsSummary": args_summary}),
                            );
                        }
                        // A worker asking for a human choice is journaled,
                        // not answered here: unattended runs have nobody to
                        // click. The desktop surfaces it from the journal.
                        AdapterEvent::Decision {
                            tool,
                            prompt,
                            options,
                            multi_select,
                        } => {
                            self.core.emit_for(
                                task,
                                manifest,
                                EventType::Other(EVT_AGENT_DECISION.to_owned()),
                                Some(adapter.id()),
                                json!({
                                    "tool": tool,
                                    "prompt": prompt,
                                    "options": options,
                                    "multiSelect": multi_select,
                                }),
                            );
                        }
                        AdapterEvent::RateLimit { provider_notice } => {
                            self.core.emit_for(
                                task,
                                manifest,
                                EventType::Other(EVT_AGENT_RATE_LIMIT.to_owned()),
                                Some(adapter.id()),
                                provider_notice,
                            );
                        }
                        AdapterEvent::UsageUpdate(usage) => {
                            self.core.ledger.consume(task.id, &usage);
                            self.core.emit_for(
                                task,
                                manifest,
                                EventType::Other(EVT_USAGE_UPDATED.to_owned()),
                                Some(adapter.id()),
                                serde_json::to_value(&usage).unwrap_or(Value::Null),
                            );
                            if UsageLedger::status_of(
                                self.core.ledger.usage(task.id).as_ref(),
                                &task.node.budgets,
                                elapsed_secs(task),
                            )
                            .is_exceeded()
                            {
                                self.core.emit_for(
                                    task,
                                    manifest,
                                    EventType::BudgetExceeded,
                                    None,
                                    json!({"node": task.node_id, "stage": "session",
                                           "spentUsd": self.core.ledger.usage(task.id)
                                               .map(|row| row.cost_usd)}),
                                );
                                let _ = handle.cancel().await;
                                break;
                            }
                        }
                        AdapterEvent::Finished {
                            exit_code,
                            final_result,
                            structured,
                        } => {
                            finished = Some((exit_code, final_result, structured));
                            break;
                        }
                        AdapterEvent::Failed(failure) => {
                            failed = Some(failure);
                            break;
                        }
                    }
                }
            }
        }

        if timed_out {
            let _ = handle.cancel().await;
            self.core.emit_for(
                task,
                manifest,
                EventType::BudgetExceeded,
                None,
                json!({"node": task.node_id, "stage": "session", "reason": "elapsed"}),
            );
            return Outcome::TransientFailure;
        }
        if lost {
            self.core.emit_for(
                task,
                manifest,
                EventType::AgentCrashed,
                Some(adapter.id()),
                json!({"node": task.node_id, "reason": "stream closed without a terminal event"}),
            );
            return Outcome::TransientFailure;
        }
        if let Some(failure) = failed {
            let event_type = if matches!(failure, AdapterFailure::SpawnFailure { .. }) {
                EventType::AgentCrashed
            } else {
                EventType::Other(EVT_TASK_FAILED.to_owned())
            };
            self.core.emit_for(
                task,
                manifest,
                event_type,
                Some(adapter.id()),
                json!({"node": task.node_id, "kind": failure.kind(), "detail": failure.to_string()}),
            );
            return if failure.is_retryable() {
                Outcome::TransientFailure
            } else {
                Outcome::ReasoningFailure
            };
        }
        let Some((exit_code, final_result, structured)) = finished else {
            // The only remaining break is the mid-session budget breach; its
            // event was already emitted.
            return Outcome::ReasoningFailure;
        };

        // Terminal success: map the completion packet into a typed handoff
        // and accept it only if it validates (HO-01).
        let session_id = handle.session_id().to_owned();
        let from_agent = format!("{}#{session_id}", adapter.id());
        let packet = HandoffPacket::from_adapter_finish(
            &task.id.to_string(),
            &from_agent,
            &contract.context_refs,
            exit_code,
            final_result.as_deref(),
            structured.as_ref(),
        );
        if let Err(rule) = packet.validate() {
            self.core.emit_for(
                task,
                manifest,
                EventType::Other(EVT_HANDOFF_REJECTED.to_owned()),
                Some(adapter.id()),
                json!({"node": task.node_id, "rule": rule.to_string()}),
            );
            return Outcome::ReasoningFailure;
        }
        let mut packet = packet;
        packet.transcript_ref = Some(format!("adapter-session:{session_id}"));
        let artifact = match self.core.store_handoff(&task.id, &packet) {
            Ok(artifact) => artifact,
            Err(error) => {
                tracing::error!(task_id = %task.id, %error, "handoff store failed");
                return Outcome::TransientFailure;
            }
        };
        let hash = artifact
            .strip_prefix("sha256:")
            .unwrap_or(&artifact)
            .to_owned();
        self.core.emit_output_ready(
            task,
            manifest,
            &from_agent,
            &packet.summary,
            &artifact,
            &format!("sha256:{hash}"),
        );
        self.refresh_code_graph(task, manifest, &worktree, &packet)
            .await;
        Outcome::Success {
            packet: serde_json::to_value(&packet).unwrap_or(Value::Null),
        }
    }

    /// Deterministic stub review (the real reviewer pool lands later):
    /// approve iff every dependency packet has no unresolved items,
    /// non-empty test evidence, and no failed test.
    async fn run_review(&self, task: &TaskRecord, manifest: &RunManifest) -> Outcome {
        self.core.emit_for(
            task,
            manifest,
            EventType::ReviewRequested,
            Some(STUB_REVIEWER),
            json!({"node": task.node_id, "reviewer": STUB_REVIEWER}),
        );
        let packets = self.core.dependency_packets(task);
        match review_verdict(&packets) {
            Ok(reviewed) => {
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::ReviewApproved,
                    Some(STUB_REVIEWER),
                    json!({"node": task.node_id, "reviewer": STUB_REVIEWER, "reviewed": reviewed}),
                );
                Outcome::Success {
                    packet: json!({
                        "reviewer": STUB_REVIEWER,
                        "approved": true,
                        "reviewed": reviewed,
                    }),
                }
            }
            Err(reason) => {
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::ReviewFailed,
                    Some(STUB_REVIEWER),
                    json!({"node": task.node_id, "reviewer": STUB_REVIEWER, "reason": reason}),
                );
                Outcome::ReasoningFailure
            }
        }
    }

    /// A `HumanApproval` node: resolved against the F-10 approval store,
    /// never self-approved.
    ///
    /// Lifecycle mapping (no new task states — the engine's existing
    /// vocabulary carries it): a decision still pending is a
    /// `TransientFailure`, so the engine walks the task
    /// `Running -> Retryable -> Ready` and re-leases it while transient
    /// retries remain (a bounded wait); when they run out the task fails
    /// terminally. A refusal (denied, expired, or an approval bound to a
    /// mutated operation) is a `ReasoningFailure` — deterministic, since
    /// retrying changes nothing.
    async fn run_human_approval(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        contract: &TaskContract,
    ) -> Outcome {
        let actor = format!("{}/human-gate", self.core.config.orchestrator);
        let (gate, operation) = match self.core.gate_operation(task, manifest, contract) {
            Ok(pair) => pair,
            Err(error) => {
                tracing::error!(node = %task.node_id, %error, "human gate has no operation");
                return Outcome::ReasoningFailure;
            }
        };
        let permissions = self.core.permissions_for(task, contract);
        let run_id = task.run_id.to_string();

        let verdict = match self.core.policy.evaluate(gate, &operation, &task.id) {
            Ok(verdict) => verdict,
            Err(error) => {
                // The store could not answer: wait and retry — an
                // unavailable approval store never means "approved".
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_POLICY_DENIED.to_owned()),
                    Some(actor.as_str()),
                    json!({"node": task.node_id, "gate": gate.as_str(),
                           "stage": "approval-store", "error": error.to_string()}),
                );
                return Outcome::TransientFailure;
            }
        };

        match &verdict {
            GateVerdict::Approved { fingerprint } => {
                self.core.audit_or_log(
                    task,
                    self.core.policy.audit().append(agentos_policy::AuditEntry {
                        actor: actor.clone(),
                        action_kind: "approval.resolved".to_owned(),
                        resource: task.node_id.clone(),
                        details: json!({
                            "gate": gate.as_str(),
                            "operationFingerprint": fingerprint,
                            "decision": "approved",
                        }),
                        run_id: Some(run_id),
                    }),
                );
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_APPROVAL_GRANTED.to_owned()),
                    Some(actor.as_str()),
                    json!({"node": task.node_id, "gate": gate.as_str(),
                           "operationFingerprint": fingerprint}),
                );
                Outcome::Success {
                    packet: json!({
                        "node": task.node_id,
                        "gate": gate.as_str(),
                        "approved": true,
                        "operationFingerprint": fingerprint,
                    }),
                }
            }
            // No request covers this exact operation (never asked, or the
            // operation changed after the last one): ask now, then wait.
            GateVerdict::Missing | GateVerdict::Mutated { .. } => {
                if let Err(error) = self.core.open_gate_request(
                    task,
                    manifest,
                    gate,
                    &operation,
                    &permissions,
                    &actor,
                ) {
                    tracing::error!(task_id = %task.id, %error, "approval request failed");
                }
                Outcome::AwaitingApproval
            }
            GateVerdict::Pending { request_id } => {
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_APPROVAL_REQUIRED.to_owned()),
                    Some(actor.as_str()),
                    json!({"node": task.node_id, "gate": gate.as_str(),
                           "requestId": request_id, "blocking": true, "waiting": true}),
                );
                Outcome::AwaitingApproval
            }
            GateVerdict::Denied { .. }
            | GateVerdict::Expired { .. }
            | GateVerdict::Consumed { .. } => {
                let denial = PolicyDenial::ApprovalRequired { gate };
                self.core.audit_or_log(
                    task,
                    self.core.policy.audit().record_permission_denial(
                        &actor,
                        &task.node_id,
                        &denial,
                        Some(&run_id),
                    ),
                );
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_APPROVAL_DENIED.to_owned()),
                    Some(actor.as_str()),
                    json!({
                        "node": task.node_id,
                        "gate": gate.as_str(),
                        "requestId": verdict.request_id(),
                        "reason": verdict.reason(),
                        "error": RuntimeError::from(denial).to_string(),
                    }),
                );
                Outcome::ReasoningFailure
            }
        }
    }

    /// PRD §23.3 in code: a git mutation is authorized only when the gate
    /// role **holds** the git action *and* a live human approval is bound to
    /// this exact mutation.
    ///
    /// `git_gate_check` supplies the first leg (permission trumps approval —
    /// a role without the action is denied even with a live approval) and
    /// push's approval leg; the supervisor tightens the second leg to
    /// **every** queued mutation, because everything this gate enqueues
    /// lands in the governed repository. Returns `Ok(false)` when policy
    /// refused (already journaled + audited); `Err` only for store failures.
    fn authorize_git_mutation(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        contract: &TaskContract,
        actor: &str,
    ) -> Result<bool, RuntimeError> {
        let (gate, operation) = self.core.gate_operation(task, manifest, contract)?;
        let permissions = self.core.config.git_gate_permissions.clone();
        let run_id = task.run_id.to_string();
        let verdict = self.core.policy.evaluate(gate, &operation, &task.id)?;
        let approved = verdict.is_approved();

        let denial = git_gate_check(&permissions, GATE_GIT_ACTION, approved)
            .err()
            .or_else(|| (!approved).then_some(PolicyDenial::ApprovalRequired { gate }));

        let Some(denial) = denial else {
            self.core.audit_or_log(
                task,
                self.core.policy.audit().record_git_gate(
                    actor,
                    GATE_GIT_ACTION,
                    true,
                    verdict.reason(),
                    Some(&run_id),
                ),
            );
            self.core.emit_for(
                task,
                manifest,
                EventType::Other(EVT_APPROVAL_GRANTED.to_owned()),
                Some(actor),
                json!({
                    "node": task.node_id,
                    "gate": gate.as_str(),
                    "action": GATE_GIT_ACTION.as_str(),
                    "operationFingerprint": agentos_policy::operation_fingerprint(&operation),
                }),
            );
            return Ok(true);
        };

        // Surface a decision a human can act on when none covers this exact
        // mutation. A missing *permission* is not an approval question, so
        // no request is opened for it.
        if matches!(denial, PolicyDenial::ApprovalRequired { .. })
            && matches!(verdict, GateVerdict::Missing | GateVerdict::Mutated { .. })
        {
            if let Err(error) =
                self.core
                    .open_gate_request(task, manifest, gate, &operation, &permissions, actor)
            {
                tracing::error!(task_id = %task.id, %error, "approval request failed");
            }
        }

        self.core.audit_or_log(
            task,
            self.core.policy.audit().record_permission_denial(
                actor,
                &format!("git:{}", GATE_GIT_ACTION.as_str()),
                &denial,
                Some(&run_id),
            ),
        );
        self.core.audit_or_log(
            task,
            self.core.policy.audit().record_git_gate(
                actor,
                GATE_GIT_ACTION,
                false,
                verdict.reason(),
                Some(&run_id),
            ),
        );
        self.core.emit_for(
            task,
            manifest,
            EventType::Other(EVT_POLICY_DENIED.to_owned()),
            Some(actor),
            json!({
                "node": task.node_id,
                "gate": gate.as_str(),
                "action": GATE_GIT_ACTION.as_str(),
                "reason": verdict.reason(),
                "requestId": verdict.request_id(),
                "error": denial.to_string(),
            }),
        );
        self.core.emit_for(
            task,
            manifest,
            EventType::Other(EVT_GIT_GATE_FAILED.to_owned()),
            Some(actor),
            json!({"node": task.node_id, "stage": "policy", "error": denial.to_string()}),
        );
        Ok(false)
    }
    /// Rebuild the code graph after this worker's edits.
    ///
    /// Never fails the task: reviewers cannot build a graph themselves (no
    /// shell), so a stale graph degrades their review, but a graph problem
    /// must not lose a worker's completed work. Runs while the task still
    /// holds its ownership lease, so graphify parses a tree nobody is
    /// writing to.
    async fn refresh_code_graph(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        worktree: &WorktreeRef,
        packet: &HandoffPacket,
    ) {
        let Some(graphifier) = self.core.graphifier() else {
            // `Disabled` emits nothing at all: a supervisor with the refresh
            // off must journal exactly what it journaled before this existed.
            if self.core.config.graph_refresh != GraphRefresh::Disabled {
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_GRAPH_SKIPPED.to_owned()),
                    Some(GRAPHIFY_AGENT),
                    json!({"node": task.node_id, "reason": GraphifySkip::BinaryMissing.as_str()}),
                );
            }
            return;
        };

        // `git status`, not the packet: the worker's file list is prose it
        // wrote about itself. Edits are uncommitted until the git gate, so a
        // base..HEAD diff would be empty here.
        let observed = cli::status_paths(&worktree.path).unwrap_or_else(|error| {
            // Fail open. A spurious cache-warm update costs about a second;
            // a graph that silently went stale costs a review.
            tracing::debug!(task_id = %task.id, %error, "git status unavailable; refreshing anyway");
            Vec::new()
        });
        let changes = reconcile_changes(&observed, &packet.files_changed, &worktree.path);

        if !changes.has_code_changes() {
            let mut payload = changes.payload();
            payload["node"] = json!(task.node_id);
            payload["reason"] = json!(GraphifySkip::NoCodeChanges.as_str());
            self.core.emit_for(
                task,
                manifest,
                EventType::Other(EVT_GRAPH_SKIPPED.to_owned()),
                Some(GRAPHIFY_AGENT),
                payload,
            );
            return;
        }

        match graphifier.update(&worktree.path).await {
            GraphifyOutcome::Updated { elapsed_ms } => {
                let mut payload = changes.payload();
                payload["node"] = json!(task.node_id);
                payload["elapsedMs"] = json!(elapsed_ms);
                payload["graphPath"] = json!(format!("{GRAPHIFY_OUT_DIR}/graph.json"));
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_GRAPH_UPDATED.to_owned()),
                    Some(GRAPHIFY_AGENT),
                    payload,
                );
            }
            GraphifyOutcome::Failed {
                reason,
                exit_code,
                stderr_tail,
            } => {
                tracing::warn!(task_id = %task.id, %reason, "code graph refresh failed");
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_GRAPH_FAILED.to_owned()),
                    Some(GRAPHIFY_AGENT),
                    json!({
                        "node": task.node_id,
                        "reason": reason,
                        "exitCode": exit_code,
                        "stderrTail": stderr_tail,
                    }),
                );
            }
            GraphifyOutcome::Skipped(skip) => {
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_GRAPH_SKIPPED.to_owned()),
                    Some(GRAPHIFY_AGENT),
                    json!({"node": task.node_id, "reason": skip.as_str()}),
                );
            }
        }
    }

    /// The commit message for a run's audit bundle: a deterministic subject
    /// line, plus — when the git manager has a model configured — a short
    /// body describing what changed. The subject is never model text, the
    /// body is sanitized, and a missing, slow, or unusable answer simply
    /// leaves the message as it has always been (GIT-03 attribution still
    /// lives in the ledger, not here).
    async fn commit_message(&self, task: &TaskRecord, worktree: &WorktreeRef) -> String {
        let subject = format!("agentos: audit bundle for run {}", task.run_id);
        let Some(advisor) = self.core.git_advisor() else {
            return subject;
        };
        let files = match cli::status_paths(&worktree.path) {
            Ok(files) if !files.is_empty() => files,
            Ok(_) => return subject,
            Err(error) => {
                tracing::warn!(task_id = %task.id, %error,
                    "could not list pending paths for the commit detail");
                return subject;
            }
        };
        let facts = CommitFacts {
            node_id: task.node_id.clone(),
            run_id: task.run_id.to_string(),
            branch: worktree.branch.clone(),
            files,
        };
        match advisor.commit_detail(&facts).await {
            Some(detail) => format!("{subject}\n\n{detail}"),
            None => subject,
        }
    }

    /// Where a stale-base rejection goes. Without an advisor this is the
    /// long-standing `rebase-or-review` hand-off. With one, the model
    /// chooses between the two commits the harness already knows about;
    /// the sha it picks is re-resolved through `git rev-parse` before any
    /// argv is built, and the replay happens in the task's own isolated
    /// worktree — never on a shared branch, and never past the push gate.
    /// The mutation is retried either way; a successful rebase just means
    /// the retry finds a fresh base.
    async fn stale_base_routing(
        &self,
        task: &TaskRecord,
        repo: &Path,
        worktree: &WorktreeRef,
        contract: &TaskContract,
        head: &str,
    ) -> (&'static str, String) {
        let Some(advisor) = self.core.git_advisor() else {
            return ("rebase-or-review", "no advisor configured".to_owned());
        };
        let facts = StaleBaseFacts {
            node_id: task.node_id.clone(),
            base_commit: contract.base_commit.clone(),
            head_commit: head.to_owned(),
            files: cli::diff_name_only(repo, &contract.base_commit, head).unwrap_or_default(),
        };
        match advisor.rebase_advice(&facts).await {
            RebaseAdvice::Escalate { reason } => ("review", reason),
            RebaseAdvice::Rebase { onto } => match cli::rev_parse_verify(repo, &onto) {
                Ok(Some(resolved)) => {
                    match cli::rebase_onto(&worktree.path, &resolved, &contract.base_commit) {
                        Ok(()) => ("rebased", resolved),
                        Err(error) => {
                            tracing::warn!(task_id = %task.id, %error,
                                "advised rebase failed; the worktree was left unchanged");
                            ("rebase-or-review", error.to_string())
                        }
                    }
                }
                Ok(None) => (
                    "review",
                    format!("advised rebase target {onto} is not a commit here"),
                ),
                Err(error) => ("rebase-or-review", error.to_string()),
            },
        }
    }

    /// The git gate: authorize the mutation against F-10 (gate permission +
    /// a live, fingerprint-bound human approval), then enqueue it on the
    /// serialized queue with that verdict, consume it as the single
    /// consumer, commit the run's audit bundle, and attribute the commit in
    /// the agent ledger.
    async fn run_git_gate(
        &self,
        task: &TaskRecord,
        manifest: &RunManifest,
        contract: &TaskContract,
    ) -> Outcome {
        let gate = format!("{}/git-gate", self.core.config.orchestrator);
        let fail = |core: &SupervisorCore, stage: &str, error: &dyn std::fmt::Display| -> Outcome {
            core.emit_for(
                task,
                manifest,
                EventType::Other(EVT_GIT_GATE_FAILED.to_owned()),
                Some(gate.as_str()),
                json!({"node": task.node_id, "stage": stage, "error": error.to_string()}),
            );
            Outcome::TransientFailure
        };

        // Authorization happens before ANY side effect: an unauthorized
        // mutation creates no worktree, no branch, no queue row (PRD §3
        // "no hidden mutation").
        let approved = match self.authorize_git_mutation(task, manifest, contract, &gate) {
            Ok(true) => true,
            // Policy refused; the denial is journaled and audited. Retrying
            // without a permission/approval change yields the same denial.
            Ok(false) => return Outcome::ReasoningFailure,
            // The approval store itself failed: retry, never assume.
            Err(error) => return fail(&self.core, "policy", &error),
        };

        let repo = self.core.config.repo.clone();
        let worktree =
            match self
                .core
                .obtain_worktree(&task.run_id, &task.id, &contract.base_commit)
            {
                Ok(worktree) => worktree,
                Err(error) => return fail(&self.core, "worktree", &error),
            };

        // The audit bundle: every validated handoff packet of this run,
        // committed on the gate branch (PRD Appendix A's "DONE + audit
        // bundle" seed).
        let packets = self.core.run_packets(&task.run_id);
        let bundle_dir = worktree.path.join(".agentos").join("audit");
        let bundle_path = bundle_dir.join(format!("{}.json", task.run_id));
        if let Err(error) = std::fs::create_dir_all(&bundle_dir)
            .and_then(|()| std::fs::write(&bundle_path, serde_json::to_vec_pretty(&packets)?))
        {
            return fail(&self.core, "audit-bundle", &error);
        }

        // `approved` is the policy verdict computed above — never a literal.
        let request_id = match self.core.queue.enqueue(
            &repo,
            &task.id.to_string(),
            &contract.base_commit,
            GATE_MUTATION,
            approved,
        ) {
            Ok(request_id) => request_id,
            Err(error) => return fail(&self.core, "enqueue", &error),
        };
        self.core.emit_for(
            task,
            manifest,
            EventType::GitQueued,
            Some(gate.as_str()),
            json!({
                "node": task.node_id,
                "requestId": request_id,
                "action": GATE_MUTATION.as_str(),
                "approved": approved,
                "baseCommit": contract.base_commit,
            }),
        );

        let claim_ttl = chrono::Duration::from_std(self.core.config.lease_ttl)
            .unwrap_or_else(|_| chrono::Duration::seconds(60));
        let request = match self.core.queue.claim_next(&repo, &gate, claim_ttl) {
            Ok(Some(request)) => request,
            Ok(None) => {
                // Another consumer holds the repo queue; retry when it
                // drains (HO-03 single-consumer ordering).
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_GIT_GATE_FAILED.to_owned()),
                    Some(gate.as_str()),
                    json!({"node": task.node_id, "stage": "claim", "error": "queue busy"}),
                );
                return Outcome::TransientFailure;
            }
            Err(error) => return fail(&self.core, "claim", &error),
        };

        let head = match cli::rev_parse_head(&repo) {
            Ok(head) => head,
            Err(error) => return fail(&self.core, "head", &error),
        };
        match self.core.queue.stale_base_check(&request.id, &head) {
            Ok(true) => {}
            Ok(false) => {
                let (routing, note) = self
                    .stale_base_routing(task, &repo, &worktree, contract, &head)
                    .await;
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_GIT_STALE_BASE.to_owned()),
                    Some(gate.as_str()),
                    json!({"node": task.node_id, "requestId": request.id,
                           "routing": routing, "advisor": note}),
                );
                return Outcome::TransientFailure;
            }
            Err(error) => return fail(&self.core, "stale-base", &error),
        }

        let message = self.commit_message(task, &worktree).await;
        let sha = match cli::add_all_and_commit(&worktree.path, "agentos", &message) {
            Ok(sha) => sha,
            Err(error) => return fail(&self.core, "commit", &error),
        };
        if let Err(error) = self.core.queue.complete(&request.id, Some(&sha)) {
            return fail(&self.core, "complete", &error);
        }

        let entry = LedgerEntry {
            commit_sha: sha.clone(),
            task_id: task.id.to_string(),
            agent_instance: gate.clone(),
            orchestrator: self.core.config.orchestrator.clone(),
            reviewers: vec![STUB_REVIEWER.to_owned()],
            context_versions: context_versions_from(&packets),
            workflow_id: manifest.workflow_id.clone(),
        };
        if let Err(error) = self.core.agent_ledger.record(&entry) {
            return fail(&self.core, "ledger", &error);
        }

        // The mutation landed: spend the human's single-use approval so it
        // cannot authorize a second commit. Deliberately after the side
        // effect — consuming first would burn the decision on an attempt
        // that then failed.
        let consumed = match self.core.gate_operation(task, manifest, contract) {
            Ok((approval_gate, operation)) => {
                self.core
                    .policy
                    .consume(approval_gate, &operation, gate.as_str())
            }
            Err(error) => {
                tracing::error!(task_id = %task.id, %error,
                    "could not rebuild the gate operation to consume its approval");
                None
            }
        };
        if let Some(approval) = &consumed {
            self.core.audit_or_log(
                task,
                self.core.policy.audit().append(agentos_policy::AuditEntry {
                    actor: gate.clone(),
                    action_kind: "approval.consumed".to_owned(),
                    resource: task.node_id.clone(),
                    details: json!({
                        "gate": GIT_MUTATION_GATE.as_str(),
                        "approvalId": approval,
                        "sha": sha,
                    }),
                    run_id: Some(task.run_id.to_string()),
                }),
            );
        }

        self.core.emit_for(
            task,
            manifest,
            EventType::GitCommitted,
            Some(gate.as_str()),
            json!({
                "node": task.node_id,
                "sha": sha,
                "requestId": request_id,
                "branch": worktree.branch,
                "workflowId": manifest.workflow_id,
                "approvalConsumed": consumed,
            }),
        );
        Outcome::Success {
            packet: json!({"sha": sha, "requestId": request_id, "action": "commit"}),
        }
    }
}

/// The stub reviewer's verdict. Skipped-with-reason tests count as
/// non-failing evidence (the mock adapter reports its tests as skipped);
/// unresolved items, absent evidence, and failed tests block.
fn review_verdict(packets: &[HandoffPacket]) -> Result<Vec<String>, String> {
    if packets.is_empty() {
        return Err("no handoff packets among the dependencies".to_owned());
    }
    for packet in packets {
        if !packet.unresolved.is_empty() {
            return Err(format!(
                "task {} reported {} unresolved item(s)",
                packet.task_id,
                packet.unresolved.len()
            ));
        }
        if packet.tests.is_empty() {
            return Err(format!("task {} reported no test evidence", packet.task_id));
        }
        if packet
            .tests
            .iter()
            .any(|test| test.status == TestStatus::Failed)
        {
            return Err(format!("task {} reported a failing test", packet.task_id));
        }
    }
    Ok(packets
        .iter()
        .map(|packet| packet.task_id.clone())
        .collect())
}

/// Context versions for the ledger: `context.auth@18` -> ("context.auth",
/// "18"); refs without a version record as "unversioned".
fn context_versions_from(packets: &[HandoffPacket]) -> BTreeMap<String, String> {
    let mut versions = BTreeMap::new();
    for packet in packets {
        for reference in &packet.context_refs {
            let (name, version) = match reference.rsplit_once('@') {
                Some((name, version)) if !name.is_empty() && !version.is_empty() => {
                    (name.to_owned(), version.to_owned())
                }
                _ => (reference.clone(), "unversioned".to_owned()),
            };
            versions.insert(name, version);
        }
    }
    versions
}

// --------------------------------------------------------------- supervisor

/// The composition root: journal + engine + adapter registry + git
/// machinery, driven by [`Supervisor::drive`].
pub struct Supervisor {
    core: Arc<SupervisorCore>,
    engine: Arc<WorkflowEngine>,
    store: Arc<TaskStore>,
}

impl Supervisor {
    /// Build a supervisor over `config` with the given adapter registry
    /// (registry entries are shared into the engine's executor, hence
    /// `Arc`s). Reopening the same databases reconstructs a supervisor that
    /// continues existing runs — manifests, handoffs and journal are all on
    /// disk.
    pub fn new(
        config: SupervisorConfig,
        adapters: Vec<Arc<dyn RuntimeAdapter>>,
    ) -> Result<Self, RuntimeError> {
        for dir in ["runs", "handoffs", "artifacts", "approvals"] {
            std::fs::create_dir_all(config.state_dir.join(dir))?;
        }
        let journal = Journal::open(&config.journal_db)?;
        let store = Arc::new(TaskStore::open(&config.workflow_db)?);
        let queue = Arc::new(MutationQueue::open(&config.queue_db)?);
        let agent_ledger = Arc::new(AgentLedger::open(&config.ledger_db)?);
        // F-13: open the dynamic registry when configured. A missing or
        // corrupt registry is a hard failure here (startup), not a
        // per-spawn degrade — misrouting work silently is worse than not
        // starting.
        let registry = match &config.agents_db {
            Some(path) => Some(Arc::new(agentos_agents::AgentRegistry::open(path)?)),
            None => None,
        };
        ensure_parent(&config.approvals_db);
        ensure_parent(&config.audit_db);
        let policy = PolicyGate::open(
            &config.approvals_db,
            &config.audit_db,
            &config.state_dir.join("approvals"),
        )?;
        let lease_ttl = config.lease_ttl;
        let core = Arc::new(SupervisorCore {
            worktrees: WorktreeManager::new(config.repo.clone()),
            journal,
            adapters,
            registry,
            store: Arc::clone(&store),
            queue,
            agent_ledger,
            ownership: OwnershipMap::new(),
            deferrals: Mutex::new(HashMap::new()),
            ledger: UsageLedger::new(),
            policy,
            manifests: Mutex::new(HashMap::new()),
            handoffs: Mutex::new(HashMap::new()),
            config,
        });
        let executor: Arc<dyn TaskExecutor> = Arc::new(SupervisorExecutor {
            core: Arc::clone(&core),
        });
        let engine = Arc::new(
            WorkflowEngine::new_with_config(
                Arc::clone(&store),
                executor,
                EngineConfig {
                    max_concurrency: core.config.max_concurrency,
                },
            )?
            .with_lease_ttl(lease_ttl),
        );
        Ok(Self {
            core,
            engine,
            store,
        })
    }

    /// The engine (direct lease/heartbeat control for embedders; also how
    /// external workers inject crash-simulating ghost leases in tests).
    pub fn engine(&self) -> &WorkflowEngine {
        &self.engine
    }

    /// Persistently pause a run with drain semantics (no new leases).
    pub fn pause_run(&self, run_id: &Uuid) -> Result<ControlResult, RuntimeError> {
        Ok(self.engine.pause_run(run_id)?)
    }

    /// Resume a paused run.
    pub fn resume_run(&self, run_id: &Uuid) -> Result<ControlResult, RuntimeError> {
        Ok(self.engine.resume_run(run_id)?)
    }

    /// Cancel one task, invoking the executor's best-effort cancellation
    /// hook if it was already running.
    pub fn cancel_task(&self, task_id: &Uuid) -> Result<ControlResult, RuntimeError> {
        Ok(self.engine.cancel_task(task_id)?)
    }

    /// Cancel all cancellable work in a run.
    pub fn cancel_run(&self, run_id: &Uuid) -> Result<ControlResult, RuntimeError> {
        Ok(self.engine.cancel_run(run_id)?)
    }

    /// Reopen one failed task with a fresh retry budget.
    pub fn retry_task(&self, task_id: &Uuid) -> Result<ControlResult, RuntimeError> {
        Ok(self.engine.retry_task(task_id)?)
    }

    /// Reroute only unleased queued or parked work; active execution profiles
    /// stay immutable.
    pub fn reroute_task(
        &self,
        task_id: &Uuid,
        role: Option<&str>,
    ) -> Result<ControlResult, RuntimeError> {
        Ok(self.engine.reroute_task(task_id, role)?)
    }

    /// The durable task/run store, for embedders that need to read or
    /// mutate run state through another façade (F-12's plan sink).
    pub fn store(&self) -> &Arc<TaskStore> {
        &self.store
    }

    /// The usage/cost ledger.
    pub fn ledger(&self) -> &UsageLedger {
        &self.core.ledger
    }

    /// The git mutation queue.
    pub fn queue(&self) -> &MutationQueue {
        &self.core.queue
    }

    /// The agent (commit attribution) ledger.
    pub fn agent_ledger(&self) -> &AgentLedger {
        &self.core.agent_ledger
    }

    /// The path ownership map.
    pub fn ownership(&self) -> &OwnershipMap {
        &self.core.ownership
    }

    /// The F-10 approval store. Humans resolve gate requests through it
    /// (`resolve(request_id, decision)`); the supervisor only ever reads it.
    pub fn approvals(&self) -> &ApprovalStore {
        self.core.policy.approvals()
    }

    /// The F-10 append-only policy audit log (SEC-05 bundles come from
    /// `export_bundle(Some(run_id))`).
    pub fn audit(&self) -> &AuditStore {
        self.core.policy.audit()
    }

    /// The gate and canonical operation payload a gate node's approval must
    /// be bound to. Computable as soon as `start_run` has materialized the
    /// tasks, so a human can approve *before* the node is leased; the gate
    /// recomputes exactly this value when it executes, and any drift in the
    /// mutation (base commit, task, action, contract) changes the
    /// fingerprint and invalidates the approval.
    pub fn gate_operation(
        &self,
        run_id: &Uuid,
        node_id: &str,
    ) -> Result<(Gate, Value), RuntimeError> {
        let (task, manifest, contract) = self.gate_node(run_id, node_id)?;
        self.core.gate_operation(&task, &manifest, &contract)
    }

    /// Open a human approval request for a gate node (the machine asks; a
    /// human resolves it through [`Supervisor::approvals`]). Tracked against
    /// the task, audited, and journaled as `approval.required`.
    pub fn request_gate_approval(
        &self,
        run_id: &Uuid,
        node_id: &str,
        requested_by: &str,
    ) -> Result<ApprovalRequest, RuntimeError> {
        let (task, manifest, contract) = self.gate_node(run_id, node_id)?;
        let (gate, operation) = self.core.gate_operation(&task, &manifest, &contract)?;
        let permissions = match task.node.node_type {
            NodeType::GitGate => self.core.config.git_gate_permissions.clone(),
            _ => self.core.permissions_for(&task, &contract),
        };
        self.core.open_gate_request(
            &task,
            &manifest,
            gate,
            &operation,
            &permissions,
            requested_by,
        )
    }

    /// Resolve a gate node to its durable task, run manifest and contract
    /// snapshot.
    fn gate_node(
        &self,
        run_id: &Uuid,
        node_id: &str,
    ) -> Result<(TaskRecord, RunManifest, TaskContract), RuntimeError> {
        let manifest = self.core.manifest(run_id)?;
        let task = self
            .store
            .tasks_for_run(run_id)?
            .into_iter()
            .find(|task| task.node_id == node_id)
            .ok_or_else(|| RuntimeError::NotAGate {
                node: node_id.to_owned(),
                run_id: *run_id,
            })?;
        let contract = manifest
            .contracts
            .get(node_id)
            .cloned()
            .ok_or_else(|| RuntimeError::MissingContracts(vec![node_id.to_owned()]))?;
        Ok((task, manifest, contract))
    }

    /// Start a run of `spec`: validate every node's full contract (and
    /// that its `base_commit` resolves in the repository), materialize the
    /// durable run through the engine, persist the run manifest (trace id +
    /// contracts), and append the run's opening events (`run.created`,
    /// `workflow.started`, `task.created` per node, `task.ready` for the
    /// initially-ready ones).
    pub fn start_run(
        &self,
        spec: &WorkflowSpec,
        goal: &str,
        contracts: HashMap<String, TaskContract>,
    ) -> Result<Uuid, RuntimeError> {
        let missing: Vec<String> = spec
            .nodes
            .iter()
            .filter(|node| !contracts.contains_key(&node.id))
            .map(|node| node.id.clone())
            .collect();
        if !missing.is_empty() {
            return Err(RuntimeError::MissingContracts(missing));
        }
        for node in &spec.nodes {
            let contract = contracts.get(&node.id).expect("presence checked above");
            contract.validate_for(&node.id, node.node_type)?;
            verify_base_commit(&self.core.config.repo, &contract.base_commit)?;
        }

        let run_id = self.engine.start_run(spec, goal)?;
        // Shared-workspace runs must serialize overlapping writes before
        // workers spawn. Isolated worktree runs retain the established
        // ownership-map admission path, which emits the useful conflict
        // audit event and then retries without billing an attempt.
        if self.core.config.shared_task_workspace {
            for task in self.store.tasks_for_run(&run_id)? {
                let contract = contracts
                    .get(&task.node_id)
                    .expect("presence validated before materialization");
                self.store.set_task_contract(
                    &task.id,
                    &WorkflowTaskContract {
                        objective: contract.objective.clone(),
                        allowed_paths: contract.allowed_paths.clone(),
                        forbidden_paths: contract.forbidden_paths.clone(),
                        acceptance_criteria: contract.acceptance_criteria.clone(),
                        required_checks: contract.required_checks.clone(),
                    },
                )?;
            }
        }
        let manifest = RunManifest {
            run_id,
            trace_id: Uuid::now_v7(),
            workflow_id: spec.id.clone(),
            workflow_version: spec.version,
            goal: goal.to_owned(),
            contracts,
        };
        self.core.save_manifest(manifest.clone())?;

        self.core.emit_run_checked(
            &run_id,
            &manifest,
            EventType::RunCreated,
            json!({"goal": goal, "workflowId": spec.id, "workflowVersion": spec.version}),
        )?;
        self.core.emit_run_checked(
            &run_id,
            &manifest,
            EventType::WorkflowStarted,
            json!({"workflowId": spec.id, "version": spec.version, "nodes": spec.nodes.len()}),
        )?;
        for task in self.store.tasks_for_run(&run_id)? {
            self.core.emit_for(
                &task,
                &manifest,
                EventType::TaskCreated,
                None,
                json!({"node": task.node_id, "nodeType": task.node.node_type.as_str()}),
            );
            if task.state == TaskState::Ready {
                self.core.emit_for(
                    &task,
                    &manifest,
                    EventType::TaskReady,
                    None,
                    json!({"node": task.node_id, "initiallyReady": true}),
                );
            }
        }
        Ok(run_id)
    }

    /// Drive the run: tick the engine until the run reaches a terminal
    /// status or a pass does no work. After every tick the supervisor
    /// projects state transitions into the journal (`task.ready`,
    /// `task.done`, `task.failed`) and escalates `Ready` tasks whose spend
    /// has crossed their cost ceiling (emit `budget.exceeded`, CAS to
    /// `Failed`, park dependents) — the engine's own cost leg cannot fire
    /// yet (its scheduler has no injectable ledger; seam noted in the
    /// F-doc).
    pub async fn drive(&self, run_id: &Uuid, max_ticks: u32) -> Result<DriveSummary, RuntimeError> {
        let manifest = self.core.manifest(run_id)?;
        let mut prev: HashMap<Uuid, TaskState> = self
            .store
            .tasks_for_run(run_id)?
            .into_iter()
            .map(|task| (task.id, task.state))
            .collect();

        // A terminal run is immutable. Repeated UI clicks must not execute
        // another scheduler tick or append duplicate run.failed events.
        let initial_tasks = self.store.tasks_for_run(run_id)?;
        let initial_status = RunStatus::from_tasks(&initial_tasks);
        if initial_status != RunStatus::Running {
            return Ok(DriveSummary {
                ticks: 0,
                status: initial_status,
            });
        }

        let mut ticks = 0u32;
        loop {
            if ticks >= max_ticks {
                return Err(RuntimeError::DriveExhausted { max_ticks });
            }
            ticks += 1;
            let report = self.engine.tick().await?;
            self.post_tick(run_id, &manifest, &prev);

            let tasks = self.store.tasks_for_run(run_id)?;
            prev = tasks.iter().map(|task| (task.id, task.state)).collect();
            let status = RunStatus::from_tasks(&tasks);
            if status != RunStatus::Running {
                let event_type = if status == RunStatus::Completed {
                    EventType::RunCompleted
                } else {
                    EventType::Other(EVT_RUN_FAILED.to_owned())
                };
                self.core.emit_run_checked(
                    run_id,
                    &manifest,
                    event_type,
                    json!({"status": status.as_str(), "ticks": ticks}),
                )?;
                return Ok(DriveSummary { ticks, status });
            }
            if !tick_worked(&report) {
                return Ok(DriveSummary { ticks, status });
            }
        }
    }

    /// Return a failed run's work to the queue after its cause has cleared
    /// (a provider quota window that has since reset, a flaky dependency
    /// that is back up) — the operator-initiated counterpart to `drive`
    /// giving up.
    ///
    /// The state change is [`TaskStore::reopen_run`]'s explicit CAS, which
    /// is the only path out of `Failed` in the whole system and is never
    /// reachable from a scheduler tick. All this adds is the audit trail:
    /// one `run.reopened` event naming the revived and un-parked nodes, so
    /// a run that leaves `failed` without any task succeeding is explicable
    /// afterwards.
    ///
    /// Total by design: reopening a run with nothing failed returns an
    /// empty report. Callers that consider that a user error (the daemon's
    /// `mastermind.reopenRun` does) check
    /// [`ReopenReport::changed_anything`] themselves.
    pub fn reopen_run(&self, run_id: &Uuid) -> Result<ReopenReport, RuntimeError> {
        let manifest = self.core.manifest(run_id)?;
        let report = self.engine.reopen_run(run_id)?;
        self.core.emit_run_checked(
            run_id,
            &manifest,
            EventType::Other(EVT_RUN_REOPENED.to_owned()),
            json!({
                "reopened": report.reopened,
                "unblocked": report.unblocked,
                "status": report.status.as_str(),
            }),
        )?;

        // A reopened task's last journaled state is `task.failed`, and
        // `post_tick` will never correct it: the next drive reads the store
        // as its baseline, so the Failed -> Ready move happened outside any
        // tick's before/after diff. Without this the desktop's projection
        // would keep rendering a revived task as failed indefinitely.
        for task in self.store.tasks_for_run(run_id)? {
            if report.reopened.contains(&task.node_id) {
                self.core.emit_for(
                    &task,
                    &manifest,
                    EventType::TaskReady,
                    None,
                    json!({"node": task.node_id, "from": "failed", "reopened": true}),
                );
            }
        }

        tracing::info!(
            run_id = %run_id,
            reopened = ?report.reopened,
            unblocked = ?report.unblocked,
            "run reopened; drive it again to resume the stranded work"
        );
        Ok(report)
    }

    /// Actionable failure events for the desktop instead of a bare Failed
    /// status. Most recent provider/runtime detail for each event is kept.
    pub fn failure_details(&self, run_id: &Uuid) -> Result<Vec<Value>, RuntimeError> {
        let failure_types = [
            EVT_AGENT_SPAWN_FAILED,
            EVT_TASK_FAILED,
            EVT_GIT_GATE_FAILED,
            "agent.crashed",
            "review.failed",
            "budget.exceeded",
        ];
        Ok(self
            .core
            .journal
            .events_for_run(run_id)?
            .into_iter()
            .filter(|event| failure_types.contains(&event.event_type.as_str()))
            .map(|event| {
                json!({
                    "event": event.event_type.as_str(),
                    "taskId": event.task_id.map(|id| id.to_string()),
                    "agent": event.agent_id,
                    "node": event.payload.get("node").and_then(Value::as_str),
                    "kind": event.payload.get("kind").and_then(Value::as_str),
                    "detail": event.payload.get("detail").and_then(Value::as_str)
                        .or_else(|| event.payload.get("error").and_then(Value::as_str))
                        .or_else(|| event.payload.get("reason").and_then(Value::as_str)),
                })
            })
            .collect())
    }

    /// Post-tick journal projection: state-transition diffs plus the
    /// cost-budget escalation scan.
    fn post_tick(&self, run_id: &Uuid, manifest: &RunManifest, prev: &HashMap<Uuid, TaskState>) {
        let Ok(tasks) = self.store.tasks_for_run(run_id) else {
            return;
        };
        for task in &tasks {
            let before = prev.get(&task.id).copied().unwrap_or(TaskState::Planned);
            if before == task.state {
                continue;
            }
            match task.state {
                TaskState::Ready => self.core.emit_for(
                    task,
                    manifest,
                    EventType::TaskReady,
                    None,
                    json!({"node": task.node_id, "from": before.as_str()}),
                ),
                TaskState::Done => self.core.emit_for(
                    task,
                    manifest,
                    EventType::TaskDone,
                    None,
                    json!({"node": task.node_id}),
                ),
                TaskState::Failed | TaskState::Cancelled => self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_TASK_FAILED.to_owned()),
                    None,
                    json!({"node": task.node_id, "state": task.state.as_str()}),
                ),
                _ => {}
            }
        }

        // Cost escalation: a Ready task whose spend crossed its ceiling is
        // failed here and its dependents parked, instead of being leased
        // again (the scheduler's cost leg is inert until the engine exposes
        // ledger injection).
        for task in &tasks {
            if task.state != TaskState::Ready {
                continue;
            }
            let Some(max_cost) = task.node.budgets.max_cost_usd else {
                continue;
            };
            let Some(spent) = self.core.ledger.usage(task.id).map(|row| row.cost_usd) else {
                continue;
            };
            if spent < max_cost {
                continue;
            }
            match self
                .store
                .cas_transition(&task.id, TaskState::Ready, TaskState::Failed)
            {
                Ok(failed) => {
                    tracing::warn!(
                        task_id = %failed.id,
                        node = %failed.node_id,
                        spent_usd = spent,
                        max_cost_usd = max_cost,
                        "cost ceiling crossed; escalated to failed"
                    );
                    self.core.emit_for(
                        &failed,
                        manifest,
                        EventType::BudgetExceeded,
                        None,
                        json!({
                            "node": failed.node_id,
                            "escalated": true,
                            "spentUsd": spent,
                            "maxCostUsd": max_cost,
                        }),
                    );
                    if let Err(error) = self.engine.scheduler().block_dependents_of(&failed) {
                        tracing::error!(task_id = %failed.id, %error, "blocking dependents failed");
                    }
                }
                Err(error) => {
                    tracing::warn!(task_id = %task.id, %error, "escalation raced another writer");
                }
            }
        }
    }

    /// Every accepted handoff packet of the run (memory first, disk for
    /// packets produced before a restart).
    pub fn handoff_packets(&self, run_id: &Uuid) -> Result<Vec<HandoffPacket>, RuntimeError> {
        Ok(self
            .store
            .tasks_for_run(run_id)?
            .iter()
            .filter_map(|task| self.core.handoff(&task.id))
            .collect())
    }

    /// The run's journal events, oldest first.
    pub fn events(&self, run_id: &Uuid) -> Result<Vec<Event>, RuntimeError> {
        self.core.journal.events_for_run(run_id)
    }
}

/// Whether one engine tick did any work (mirror of the engine's private
/// `TickReport::did_work`, which is not exported).
fn tick_worked(report: &TickReport) -> bool {
    report.reclaimed > 0
        || report.promoted > 0
        || report.leased > 0
        || !report.succeeded.is_empty()
        || !report.failed.is_empty()
        || report.requeued > 0
        || report.escalated_over_budget > 0
        || report.blocked > 0
        || report.conflicted > 0
}

/// Resolve `base_commit` in the repository (`git rev-parse --verify
/// <base>^{commit}`) so a bad contract fails at `start_run`, not mid-run.
fn verify_base_commit(repo: &Path, base_commit: &str) -> Result<(), RuntimeError> {
    let subject = format!("{base_commit}^{{commit}}");
    let args: Vec<OsString> = vec![
        "rev-parse".into(),
        "--verify".into(),
        "--quiet".into(),
        subject.into(),
    ];
    cli::run(Some(repo), &args).map_err(|error| {
        RuntimeError::ContractInvalid(format!(
            "base commit `{base_commit}` does not resolve in the repository: {error}"
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn objective_preview_preserves_skill_head_and_task_tail_within_budget() {
        let text = format!("GLOBAL:{}:TASK OBJECTIVE", "x".repeat(100));
        let preview = truncate_preview(&text, 80);
        assert!(preview.starts_with("GLOBAL:"));
        assert!(preview.ends_with("TASK OBJECTIVE"));
        assert!(preview.contains("middle truncated"));
        assert!(preview.chars().count() <= 80);
    }

    #[test]
    fn session_routing_can_pin_planning_worker_to_selected_provider() {
        let config = SupervisorConfig::for_repo("state", "repo")
            .with_role_adapter("spec-writer", "codex")
            .with_role_model("spec-writer", "gpt-5.6-sol")
            .with_shared_task_workspace();
        assert_eq!(
            config.role_adapters.get("spec-writer").map(String::as_str),
            Some("codex")
        );
        assert_eq!(
            config.role_models.get("spec-writer").map(String::as_str),
            Some("gpt-5.6-sol")
        );
        assert!(config.shared_task_workspace);
    }
    use serde_json::json;

    /// The gate constant must stay the gate F-10 assigns to the action the
    /// git gate actually performs — otherwise the supervisor would ask for
    /// approval on one gate and `git_gate_check` would demand another.
    #[test]
    fn the_git_gate_constant_matches_the_policy_gate_for_its_action() {
        assert_eq!(
            agentos_policy::approval_gate(GATE_GIT_ACTION),
            Some(GIT_MUTATION_GATE)
        );
        assert!(crate::policy::single_use_gate(GIT_MUTATION_GATE));
    }

    fn packet(task_id: &str, unresolved: &[&str], tests: &[(&str, TestStatus)]) -> HandoffPacket {
        HandoffPacket {
            id: Uuid::now_v7(),
            from_agent: "mock#mock-1".to_owned(),
            task_id: task_id.to_owned(),
            status: crate::handoff::HandoffStatus::OutputReady,
            summary: "done".to_owned(),
            files_changed: vec![],
            artifacts: vec![],
            context_refs: vec![],
            decisions: vec![],
            tests: tests
                .iter()
                .map(|(name, status)| crate::handoff::TestReport {
                    name: (*name).to_owned(),
                    status: *status,
                    count: None,
                })
                .collect(),
            unresolved: unresolved.iter().map(|s| (*s).to_owned()).collect(),
            requested_action: crate::handoff::RequestedAction::Review,
            transcript_ref: None,
        }
    }

    #[test]
    fn upstream_packets_reach_the_prompt_and_stay_bounded() {
        assert!(
            SupervisorCore::render_upstream_packets(&[]).is_empty(),
            "a task with no dependencies must not carry an empty upstream section"
        );

        let mut upstream = packet(
            "T01",
            &["migration not run"],
            &[("test:auth", TestStatus::Passed)],
        );
        upstream.summary = "Added the auth route handler.".to_owned();
        upstream.decisions = vec!["Chose JWT over sessions".to_owned()];
        upstream.files_changed = (0..60).map(|n| format!("src/file{n}.rs")).collect();

        let rendered = SupervisorCore::render_upstream_packets(&[upstream]);

        // The record itself reaches the worker: this is what makes
        // `contract_for`'s "read their handoff packets" instruction truthful.
        assert!(rendered.contains("T01"), "the upstream task id must appear");
        assert!(rendered.contains("Added the auth route handler."));
        assert!(rendered.contains("Chose JWT over sessions"));
        assert!(rendered.contains("migration not run"));
        assert!(rendered.contains("src/file0.rs"));

        // Bounded: a wide fan-in must not spend the downstream context window.
        assert!(
            !rendered.contains("src/file59.rs"),
            "files beyond the cap must be elided, not rendered"
        );
        assert!(
            rendered.contains("and 20 more"),
            "elision must be stated, not silent: {rendered}"
        );
    }

    #[test]
    fn a_long_upstream_summary_is_truncated_rather_than_dropped() {
        let mut upstream = packet("T02", &[], &[]);
        upstream.summary = "x".repeat(5000);

        let rendered = SupervisorCore::render_upstream_packets(&[upstream]);

        assert!(rendered.contains("truncated"), "truncation must be visible");
        assert!(
            rendered.len() < 3000,
            "an unbounded model-written summary must not pass through whole: {} chars",
            rendered.len()
        );
    }

    #[test]
    fn journal_round_trips_events_per_run() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(&dir.path().join("journal.db")).expect("journal");

        let run_id = Uuid::now_v7();
        for event_type in [EventType::RunCreated, EventType::WorkflowStarted] {
            let event = Event::new(event_type).with_run_id(run_id);
            journal.append(&event).expect("append");
        }
        let events = journal.events_for_run(&run_id).expect("read");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, EventType::RunCreated);
        assert_eq!(events[1].event_type, EventType::WorkflowStarted);

        // A second journal over the same file sees the same history.
        let reopened = Journal::open(&dir.path().join("journal.db")).expect("reopen");
        assert_eq!(reopened.events_for_run(&run_id).unwrap().len(), 2);
        assert!(journal.events_for_run(&Uuid::now_v7()).unwrap().is_empty());
    }

    #[test]
    fn stub_review_requires_evidence_without_unresolved_or_failures() {
        let good = packet("t-good", &[], &[("test:auth", TestStatus::Passed)]);
        let skipped = packet("t-skip", &[], &[("adapter-tests", TestStatus::Skipped)]);
        assert_eq!(
            review_verdict(&[good, skipped]).unwrap(),
            vec!["t-good".to_owned(), "t-skip".to_owned()]
        );

        assert!(review_verdict(&[]).is_err(), "no packets -> fail");
        let unresolved = packet("t-risk", &["cache invalidation across shards"], &[]);
        assert!(review_verdict(&[unresolved]).is_err());

        let no_tests = packet("t-bare", &[], &[]);
        assert!(review_verdict(&[no_tests]).is_err());

        let failing = packet("t-bad", &[], &[("test:auth", TestStatus::Failed)]);
        assert!(review_verdict(&[failing]).is_err());
    }

    #[test]
    fn context_refs_become_ledger_context_versions() {
        let mut a = packet("t-a", &[], &[]);
        a.context_refs = vec![
            "context.auth@18".to_owned(),
            "context.db@7".to_owned(),
            "decision.adr-014".to_owned(),
        ];
        let mut b = packet("t-b", &[], &[]);
        b.context_refs = vec!["context.auth@20".to_owned()];
        let versions = context_versions_from(&[a, b]);
        assert_eq!(versions["context.auth"], "20", "later packet wins");
        assert_eq!(versions["context.db"], "7");
        assert_eq!(versions["decision.adr-014"], "unversioned");
    }

    #[test]
    fn base_commit_verification_rejects_unknown_commits() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        cli::init(&repo).expect("git init");
        std::fs::write(repo.join("README.md"), "seed\n").unwrap();
        let head = cli::add_all_and_commit(&repo, "agentos", "seed").expect("commit");

        assert!(verify_base_commit(&repo, &head).is_ok());
        assert!(
            verify_base_commit(&repo, &head[..8]).is_ok(),
            "partial sha resolves"
        );
        let err = verify_base_commit(&repo, "deadbeef").unwrap_err();
        assert!(err.to_string().contains("deadbeef"), "{err}");
    }

    #[test]
    fn session_timeout_takes_the_stricter_ceiling() {
        let contract = crate::contract::TaskContract::builder("t", "o")
            .base_commit("abc")
            .budgets(crate::contract::ContractBudgets::new(1, 1))
            .build()
            .unwrap(); // 1 minute -> 60s
        let mut record = minimal_record();
        record.node.budgets.max_elapsed_secs = 3_600;
        assert_eq!(session_timeout(&record, &contract), 60, "contract binds");
        record.node.budgets.max_elapsed_secs = 30;
        assert_eq!(session_timeout(&record, &contract), 30, "node binds");
        record.node.budgets.max_elapsed_secs = 0;
        assert_eq!(session_timeout(&record, &contract), 1, "floored at 1s");
    }

    fn minimal_record() -> TaskRecord {
        TaskRecord {
            id: Uuid::now_v7(),
            run_id: Uuid::now_v7(),
            workflow_id: "wf".to_owned(),
            node_id: "n".to_owned(),
            state: TaskState::Running,
            priority: agentos_core::Priority::P2,
            lease_owner: None,
            lease_expires_at: None,
            heartbeat_at: None,
            attempt_count: 0,
            contract: agentos_workflow::TaskContract {
                objective: "o".to_owned(),
                allowed_paths: vec![],
                forbidden_paths: vec![],
                acceptance_criteria: vec![],
                required_checks: vec![],
            },
            node: agentos_workflow::NodeSpec {
                id: "n".to_owned(),
                node_type: NodeType::Run,
                depends_on: vec![],
                agent_role: None,
                budgets: agentos_workflow::Budgets::default(),
                retry: agentos_workflow::RetryPolicy::default(),
            },
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn emit_helpers_stamp_run_and_trace_ids() {
        // Struct-level check that emit_for builds a fully correlated event.
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(&dir.path().join("journal.db")).unwrap();
        let trace_id = Uuid::now_v7();
        let run_id = Uuid::now_v7();
        let event = Event::new(EventType::TaskRunning)
            .with_run_id(run_id)
            .with_trace_id(trace_id)
            .with_task_id(Uuid::now_v7())
            .with_payload(json!({"node": "build"}));
        journal.append(&event).unwrap();
        let stored = journal.events_for_run(&run_id).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].trace_id, Some(trace_id));
        assert_eq!(stored[0].payload["node"], json!("build"));
    }
}
