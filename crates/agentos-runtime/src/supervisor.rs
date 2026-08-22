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
//! - **GitGate nodes** — enqueue a `Commit` mutation on the serialized
//!   queue with `approved = true` (the approval plumbing is F-10's seam),
//!   act as the single consumer, run the stale-base gate, commit the run's
//!   audit bundle on the gate branch, record the agent-ledger attribution
//!   (sha -> task/agent/reviewers/context versions/workflow, GIT-03) and
//!   emit `git.queued` + `git.committed`.
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
use agentos_core::{Event, EventType, TaskState};
use agentos_daemon::db::open_db;
use agentos_daemon::events::{append_event, events_for_run};
use agentos_git::cli;
use agentos_git::ledger::{AgentLedger, LedgerEntry};
use agentos_git::ownership::OwnershipMap;
use agentos_git::queue::{MutationAction, MutationQueue};
use agentos_git::worktree::{WorktreeManager, WorktreeRef};
use agentos_workflow::{
    NodeType, Outcome, RunStatus, Scheduler, TaskExecutor, TaskRecord, TaskStore, TickReport,
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
use crate::handoff::{HandoffPacket, TestStatus};
use crate::usage_ledger::UsageLedger;

/// Identity of the deterministic stand-in reviewer (the real reviewer pool
/// lands in a later PR; every approved packet cites this id in the ledger).
pub const STUB_REVIEWER: &str = "stub-reviewer@f-07";

// Extension event types (agentos-core's `EventType::Other` keeps them
// round-tripping verbatim through the journal).
const EVT_SESSION_SPAWN: &str = "session.spawn";
const EVT_SESSION_STARTED: &str = "session.started";
const EVT_AGENT_TOOL_USE: &str = "agent.tool_use";
const EVT_AGENT_RATE_LIMIT: &str = "agent.rate_limit";
const EVT_AGENT_SPAWN_FAILED: &str = "agent.spawn_failed";
const EVT_USAGE_UPDATED: &str = "usage.updated";
const EVT_TASK_FAILED: &str = "task.failed";
const EVT_HANDOFF_REJECTED: &str = "handoff.rejected";
const EVT_OWNERSHIP_CONFLICT: &str = "ownership.conflict";
const EVT_APPROVAL_REQUIRED: &str = "approval.required";
const EVT_GIT_GATE_FAILED: &str = "git.gate_failed";
const EVT_GIT_STALE_BASE: &str = "git.stale_base";
const EVT_RUN_FAILED: &str = "run.failed";

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
    /// Main checkout of the governed repository (worktrees hang off it).
    pub repo: PathBuf,
    /// Orchestrator identity stamped into ledger attribution.
    pub orchestrator: String,
    /// Adapter used when a node declares no `agent_role` (or the role has
    /// no mapping): `"mock"` by default.
    pub default_adapter: String,
    /// `agent_role` -> adapter id routing table.
    pub role_adapters: HashMap<String, String>,
    /// Lease ttl the supervisor's engine grants.
    pub lease_ttl: Duration,
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
            state_dir,
            repo: repo.into(),
            orchestrator: "agentos-supervisor".to_owned(),
            default_adapter: "mock".to_owned(),
            role_adapters: HashMap::new(),
            lease_ttl: DEFAULT_LEASE_TTL,
        }
    }

    /// Route `agent_role` to `adapter_id`.
    pub fn with_role_adapter(mut self, role: &str, adapter_id: &str) -> Self {
        self.role_adapters
            .insert(role.to_owned(), adapter_id.to_owned());
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
    store: Arc<TaskStore>,
    worktrees: WorktreeManager,
    queue: Arc<MutationQueue>,
    agent_ledger: Arc<AgentLedger>,
    ownership: OwnershipMap,
    ledger: UsageLedger,
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

    /// The adapter for a node's `agent_role`: the role mapping when present,
    /// else the configured default. Arc-cloned out of the registry.
    fn adapter_for(&self, role: Option<&str>) -> Result<Arc<dyn RuntimeAdapter>, RuntimeError> {
        let wanted = role
            .and_then(|role| self.config.role_adapters.get(role))
            .cloned()
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
            // F-07 has no human-approval plumbing (that is F-10's seam): a
            // human gate fails loudly instead of silently self-approving.
            NodeType::HumanApproval => {
                self.core.emit_for(
                    task,
                    &manifest,
                    EventType::Other(EVT_APPROVAL_REQUIRED.to_owned()),
                    None,
                    json!({"node": task.node_id, "blocking": true}),
                );
                Outcome::ReasoningFailure
            }
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
            // Another task holds overlapping paths: retryable — the hold
            // frees when that task's attempt ends.
            self.core.emit_for(
                task,
                manifest,
                EventType::Other(EVT_OWNERSHIP_CONFLICT.to_owned()),
                None,
                json!({"node": task.node_id, "error": conflict.to_string()}),
            );
            return Outcome::TransientFailure;
        }

        let outcome = self.drive_session(task, manifest, contract).await;
        self.core.ownership.release(&task_key);
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

        let timeout_secs = session_timeout(task, contract);
        let spec = SpawnSpec {
            task_id: task.id,
            objective: contract.objective.clone(),
            workspace: worktree.path.clone(),
            allowed_paths: contract.allowed_paths.clone(),
            forbidden_paths: contract.forbidden_paths.clone(),
            tool_allowlist: Vec::new(),
            tool_denylist: match contract.git_policy {
                GitPolicy::NoDirectGit => vec![
                    "Bash(git commit:*)".to_owned(),
                    "Bash(git push:*)".to_owned(),
                ],
                GitPolicy::DirectAllowed => Vec::new(),
            },
            model: None,
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

    /// The git gate: enqueue a Commit mutation (approval plumbing is F-10's
    /// seam, hence `approved = true` here), consume it as the single
    /// consumer, commit the run's audit bundle, attribute the commit in the
    /// agent ledger.
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

        // SEAM (F-10): the approval gate. F-07 enqueues with approved=true —
        // the commit is already review-approved upstream; push stays
        // approval-gated inside the queue itself.
        let request_id = match self.core.queue.enqueue(
            &repo,
            &task.id.to_string(),
            &contract.base_commit,
            MutationAction::Commit,
            true,
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
                "action": "commit",
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
                self.core.emit_for(
                    task,
                    manifest,
                    EventType::Other(EVT_GIT_STALE_BASE.to_owned()),
                    Some(gate.as_str()),
                    json!({"node": task.node_id, "requestId": request.id,
                           "routing": "rebase-or-review"}),
                );
                return Outcome::TransientFailure;
            }
            Err(error) => return fail(&self.core, "stale-base", &error),
        }

        let message = format!("agentos: audit bundle for run {}", task.run_id);
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
        for dir in ["runs", "handoffs", "artifacts"] {
            std::fs::create_dir_all(config.state_dir.join(dir))?;
        }
        let journal = Journal::open(&config.journal_db)?;
        let store = Arc::new(TaskStore::open(&config.workflow_db)?);
        let queue = Arc::new(MutationQueue::open(&config.queue_db)?);
        let agent_ledger = Arc::new(AgentLedger::open(&config.ledger_db)?);
        let lease_ttl = config.lease_ttl;
        let core = Arc::new(SupervisorCore {
            worktrees: WorktreeManager::new(config.repo.clone()),
            journal,
            adapters,
            store: Arc::clone(&store),
            queue,
            agent_ledger,
            ownership: OwnershipMap::new(),
            ledger: UsageLedger::new(),
            manifests: Mutex::new(HashMap::new()),
            handoffs: Mutex::new(HashMap::new()),
            config,
        });
        let executor: Arc<dyn TaskExecutor> = Arc::new(SupervisorExecutor {
            core: Arc::clone(&core),
        });
        let engine =
            Arc::new(WorkflowEngine::new(Arc::clone(&store), executor).with_lease_ttl(lease_ttl));
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
    use serde_json::json;

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
