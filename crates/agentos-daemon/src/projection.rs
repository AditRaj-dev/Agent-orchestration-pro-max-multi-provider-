//! F-11a: journal → UI projections (runs / tasks / agents).
//!
//! CONTRACT: `docs/F-11-desktop.md` §3.3 — the fold table is binding. UI
//! state is a projection, never a second source of truth (F-00 §3): these
//! summaries are re-derived from the append-only journal on demand; nothing
//! here ever writes. Journal sizes are thousands of rows, not millions, so a
//! full re-fold per `runs.list`/`tasks.list`/`agents.list` call is cheaper
//! than an incremental cache and its invalidation bugs (the cache itself is
//! the F-11 §6.5 seam).
//!
//! The fold is **total** by construction: any event type not listed in the
//! fold table only bumps `lastEventType`/`eventCount`/`lastSeq` counters and
//! leaves states untouched, so journals written by newer producers fold
//! without error — the same forward-compatibility rule `agentos-core`
//! applies to parsing itself (`EventType::Other`).
//!
//! Deliberate interpretation choices (fold-table wording vs wire shape):
//!
//! - `budget.exceeded` sets `budgetExceeded` on the **task**; the §3.3
//!   `RunSummary` wire shape carries no such field, and §3.3 (exact serde
//!   shape) outranks the fold-table prose.
//! - `agent.leased`/`task.running` map the agent to `running` — the
//!   `AgentStatus` vocabulary has no `leased` member; "leased/running" in
//!   the table names the two events, not two states.
//! - `run.completed` resolves through the F-06 derivation
//!   (`RunStatus::from_tasks`): `failed` when any task of the run failed,
//!   else `completed`.

use std::collections::HashMap;

use agentos_core::{Event, TaskState};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::events::SequencedEvent;

/// Run status wire strings (§3.3): `running | completed | failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Created, not yet terminal.
    Running,
    /// Terminal success (F-06 derivation: no failed task).
    Completed,
    /// Terminal failure (or a completed run with a failed task).
    Failed,
}

/// Agent status wire vocabulary (§3.3):
/// `idle | planning | running | waiting | reviewing | blocked | failed | complete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    /// Seen in the journal but never spawned/parked, or its session was
    /// cancelled (also the default).
    Idle,
    /// `session.spawn` observed, session not yet started.
    Planning,
    /// Session live (`session.started`, `agent.tool_use`, lease held).
    Running,
    /// `agent.rate_limit` — provider backoff.
    Waiting,
    /// Reviewer evaluating a quality gate.
    Reviewing,
    /// Reserved vocabulary; no event in the v1 table emits it.
    Blocked,
    /// `agent.spawn_failed` / `agent.session_failed`.
    Failed,
    /// Run/task terminal, or `session.finished` — no pending work. A chat
    /// session stays instructable here; the next turn flips it back to
    /// `Running`.
    Complete,
}

/// Per-run task counters (§3.3 `taskCounts`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCounts {
    /// Tasks belonging to the run.
    pub total: u32,
    /// Tasks in `done`.
    pub done: u32,
    /// Tasks in `failed`.
    pub failed: u32,
    /// Everything not done/failed (in flight or parked).
    pub active: u32,
}

/// Cost/token rollup (§3.3 `usage`): `usage.updated` payloads folded per
/// agent. `costUsd` sums the provider-reported cost when there is one
/// (claude canon `total_cost_usd`; codex/agy report tokens only — F-00 §4);
/// `tokensEstimate` takes `totalTokens` when present, else
/// `inputTokens + outputTokens`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageRollup {
    /// Summed provider-reported cost, 0.0 when never reported.
    pub cost_usd: f64,
    /// Summed token estimate.
    pub tokens_estimate: u64,
}

/// §3.3 `RunSummary` — one row per `run.created`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSummary {
    /// Owning run id.
    pub run_id: String,
    /// Derived run status.
    pub status: RunStatus,
    /// From the `workflow.started` payload, when present.
    pub workflow_id: Option<String>,
    /// Task counters, computed after the fold.
    pub task_counts: TaskCounts,
    /// `occurredAt` of `run.created`.
    pub started_at: String,
    /// `occurredAt` of the terminal run event, if any.
    pub ended_at: Option<String>,
    /// Seq of the run's first journaled event.
    pub first_seq: i64,
    /// Seq of the run's latest journaled event.
    pub last_seq: i64,
    /// Events journaled with this `runId`.
    pub event_count: u64,
}

/// §3.3 `TaskSummary` — one row per `task.created`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSummary {
    /// Task id (event `taskId` column, payload `taskId` fallback).
    pub task_id: String,
    /// Owning run id (empty string only for malformed producers that omit
    /// `runId` on `task.created`; the fold never invents one).
    pub run_id: String,
    /// Workflow node id, from the `task.created` payload.
    pub node_id: Option<String>,
    /// Folded lifecycle state (canonical snake_case wire strings).
    pub state: TaskState,
    /// Retry count; only `agent.crashed` increments it (fold table).
    pub attempts: u32,
    /// Last agent seen leasing/running the task, when any.
    pub agent_id: Option<String>,
    /// Node dependencies from the `workflow.started` node list, when the
    /// payload carries one; empty otherwise (the UI renders blocked tasks
    /// from state alone when absent).
    pub depends_on: Vec<String>,
    /// Wire string of the last event for this task.
    pub last_event_type: String,
    /// RFC 3339 timestamp of that event.
    pub last_event_at: String,
    /// From the `git.committed` payload (`sha`; `commitSha` accepted).
    pub commit_sha: Option<String>,
    /// Set by `budget.exceeded` on this task.
    pub budget_exceeded: bool,
}

/// §3.3 `AgentSummary` — one row per distinct `agentId` seen.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSummary {
    /// Agent (instance) identifier, exactly as journaled.
    pub agent_id: String,
    /// From `session.spawn` payloads, when the producer carries it.
    pub provider: Option<String>,
    /// From `session.started` payloads, when present.
    pub model: Option<String>,
    /// Folded status vocabulary.
    pub status: AgentStatus,
    /// Latest run the agent was seen working in.
    pub run_id: Option<String>,
    /// Latest task the agent was seen working on.
    pub task_id: Option<String>,
    /// Wire string of the last event carrying this agent id.
    pub last_event_type: String,
    /// RFC 3339 timestamp of that event.
    pub last_event_at: String,
    /// Events journaled with this agent id.
    pub event_count: u64,
    /// Rolled-up `usage.updated` payloads.
    pub usage: UsageRollup,
}

/// The folded projection: runs/tasks/agents in first-seen (journal) order.
///
/// Accessors rather than pub fields so the ordering/filtering promises
/// (`runs.list` is newest-run-first) live in one place.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Projection {
    runs: Vec<RunSummary>,
    tasks: Vec<TaskSummary>,
    agents: Vec<AgentSummary>,
}

impl Projection {
    /// Runs, newest-run-first (descending `firstSeq` — §3.2 `runs.list`).
    pub fn runs(&self) -> Vec<&RunSummary> {
        let mut runs: Vec<&RunSummary> = self.runs.iter().collect();
        runs.sort_by(|a, b| b.first_seq.cmp(&a.first_seq));
        runs
    }

    /// Tasks in creation order; `run_id` `Some` filters, `None` = all.
    pub fn tasks(&self, run_id: Option<&str>) -> Vec<&TaskSummary> {
        self.tasks
            .iter()
            .filter(|task| run_id.is_none_or(|id| task.run_id == id))
            .collect()
    }

    /// Agents in first-seen order; `run_id` `Some` keeps only agents whose
    /// latest run association matches, `None` = all.
    pub fn agents(&self, run_id: Option<&str>) -> Vec<&AgentSummary> {
        self.agents
            .iter()
            .filter(|agent| run_id.is_none_or(|id| agent.run_id.as_deref() == Some(id)))
            .collect()
    }
}

/// Fold a journal (oldest-first; seqs monotonic) into a [`Projection`].
///
/// Total: unknown event types only bump counters/`lastEventType`. Events
/// that name a task nobody created still count wherever their ids resolve
/// (a state transition for an unknown task is ignored, not fatal).
pub fn fold(journal: &[SequencedEvent]) -> Projection {
    let mut fold = FoldState::default();
    for sequenced in journal {
        fold.apply(sequenced);
    }
    fold.finish()
}

/// Mutable fold accumulator; [`fold`] drives it event by event.
#[derive(Default)]
struct FoldState {
    runs: Vec<RunSummary>,
    run_ix: HashMap<String, usize>,
    tasks: Vec<TaskSummary>,
    task_ix: HashMap<String, usize>,
    agents: Vec<AgentSummary>,
    agent_ix: HashMap<String, usize>,
    /// `nodeId -> dependsOn` from `workflow.started` node lists — kept
    /// because the supervisor journals `workflow.started` *before*
    /// `task.created`, so deps usually apply to tasks that do not exist yet.
    node_deps: HashMap<String, Vec<String>>,
}

impl FoldState {
    fn apply(&mut self, sequenced: &SequencedEvent) {
        let event = &sequenced.event;
        let seq = sequenced.seq;
        let type_str = event.event_type.as_str();
        let run_key = event.run_id.map(|id| id.to_string());
        let task_key = event
            .task_id
            .map(|id| id.to_string())
            .or_else(|| payload_str(&event.payload, "taskId").map(str::to_owned));
        let agent_key = event.agent_id.clone();
        let stamp = rfc3339(&event.occurred_at);

        // Run lifecycle first: run.created registers the run so the generic
        // bookkeeping below finds it; workflow.started carries the deps.
        match type_str {
            "run.created" => {
                if let Some(run_id) = &run_key {
                    self.run_ix.entry(run_id.clone()).or_insert_with(|| {
                        self.runs.push(RunSummary {
                            run_id: run_id.clone(),
                            status: RunStatus::Running,
                            workflow_id: None,
                            task_counts: TaskCounts {
                                total: 0,
                                done: 0,
                                failed: 0,
                                active: 0,
                            },
                            started_at: stamp.clone(),
                            ended_at: None,
                            first_seq: seq,
                            last_seq: seq,
                            event_count: 0,
                        });
                        self.runs.len() - 1
                    });
                }
            }
            "workflow.started" => {
                if let Some(ix) = run_key
                    .as_deref()
                    .and_then(|id| self.run_ix.get(id).copied())
                {
                    if let Some(workflow_id) = payload_str(&event.payload, "workflowId") {
                        self.runs[ix].workflow_id = Some(workflow_id.to_owned());
                    }
                    self.absorb_node_deps(&event.payload);
                }
            }
            "run.completed" | "run.failed" => {
                if let Some(ix) = run_key
                    .as_deref()
                    .and_then(|id| self.run_ix.get(id).copied())
                {
                    self.runs[ix].status = match type_str {
                        // F-06 derivation: any failed task fails the run.
                        "run.completed" => {
                            let any_failed = self.tasks.iter().any(|task| {
                                task.run_id == self.runs[ix].run_id
                                    && task.state == TaskState::Failed
                            });
                            if any_failed {
                                RunStatus::Failed
                            } else {
                                RunStatus::Completed
                            }
                        }
                        _ => RunStatus::Failed,
                    };
                    self.runs[ix].ended_at = Some(stamp.clone());
                }
                // Terminal run → every agent of that run is complete.
                if let Some(run_id) = &run_key {
                    for agent in &mut self.agents {
                        if agent.run_id.as_deref() == Some(run_id) {
                            agent.status = AgentStatus::Complete;
                        }
                    }
                }
            }
            // An operator returned a failed run's work to the queue
            // (`mastermind.reopenRun`). The run is live again, so the
            // terminal marks come off — otherwise the run list would keep
            // showing `failed` with an `endedAt` for a run that is once
            // more executing. The per-task correction rides the
            // `task.ready` events the supervisor emits alongside this one;
            // `taskCounts` then re-derives in `finish`.
            "run.reopened" => {
                if let Some(ix) = run_key
                    .as_deref()
                    .and_then(|id| self.run_ix.get(id).copied())
                {
                    self.runs[ix].status = RunStatus::Running;
                    self.runs[ix].ended_at = None;
                }
            }
            _ => {}
        }

        // Generic run bookkeeping: every event with a resolvable runId
        // counts toward that run (unknown types included — the total-fold
        // rule).
        if let Some(ix) = run_key
            .as_deref()
            .and_then(|id| self.run_ix.get(id).copied())
        {
            self.runs[ix].last_seq = seq;
            self.runs[ix].event_count += 1;
        }

        // Task effects.
        self.apply_task_effects(event, type_str, &task_key, &stamp, &agent_key);

        // Agent effects (create-on-first-sight: agent ids arrive unannounced
        // on session events and lease rows).
        if let Some(agent_id) = &agent_key {
            let ix = self.agent_ix.get(agent_id).copied().unwrap_or_else(|| {
                self.agents.push(AgentSummary {
                    agent_id: agent_id.clone(),
                    provider: None,
                    model: None,
                    status: AgentStatus::Idle,
                    run_id: None,
                    task_id: None,
                    last_event_type: type_str.to_owned(),
                    last_event_at: stamp.clone(),
                    event_count: 0,
                    usage: UsageRollup {
                        cost_usd: 0.0,
                        tokens_estimate: 0,
                    },
                });
                let ix = self.agents.len() - 1;
                self.agent_ix.insert(agent_id.clone(), ix);
                ix
            });
            let agent = &mut self.agents[ix];
            agent.last_event_type = type_str.to_owned();
            agent.last_event_at = stamp.clone();
            agent.event_count += 1;
            if let Some(run_id) = &run_key {
                agent.run_id = Some(run_id.clone());
            }
            if let Some(task_id) = &task_key {
                agent.task_id = Some(task_id.clone());
            }
            match type_str {
                "session.spawn" => {
                    agent.status = AgentStatus::Planning;
                    if let Some(provider) = payload_str(&event.payload, "provider") {
                        agent.provider = Some(provider.to_owned());
                    }
                }
                "session.started" => {
                    agent.status = AgentStatus::Running;
                    if let Some(model) = payload_str(&event.payload, "model") {
                        agent.model = Some(model.to_owned());
                    }
                }
                "session.instruction" => agent.status = AgentStatus::Running,
                "agent.tool_use" => agent.status = AgentStatus::Running,
                "agent.rate_limit" => agent.status = AgentStatus::Waiting,
                // Terminal arms. Without these an agent that ever reached
                // `Running` stayed Running for the life of the journal, so
                // finished and cancelled sessions still read as live.
                "session.finished" => agent.status = AgentStatus::Complete,
                "session.cancelled" => agent.status = AgentStatus::Idle,
                "agent.session_failed" => agent.status = AgentStatus::Failed,
                "agent.spawn_failed" => agent.status = AgentStatus::Failed,
                "agent.leased" | "task.running" => agent.status = AgentStatus::Running,
                "review.requested" => agent.status = AgentStatus::Reviewing,
                "usage.updated" => {
                    let cost = event.payload.get("costUsd").and_then(Value::as_f64);
                    agent.usage.cost_usd += cost.unwrap_or(0.0);
                    agent.usage.tokens_estimate += payload_u64(&event.payload, "totalTokens")
                        .unwrap_or_else(|| {
                            payload_u64(&event.payload, "inputTokens").unwrap_or(0)
                                + payload_u64(&event.payload, "outputTokens").unwrap_or(0)
                        });
                }
                "task.done" | "task.failed" => agent.status = AgentStatus::Complete,
                _ => {}
            }
        }
    }

    /// The §3.3 task columns of the fold table.
    fn apply_task_effects(
        &mut self,
        event: &Event,
        type_str: &str,
        task_key: &Option<String>,
        stamp: &str,
        agent_key: &Option<String>,
    ) {
        let Some(task_id) = task_key else { return };
        let run_id = event.run_id.map(|id| id.to_string()).unwrap_or_default();

        match type_str {
            "task.created" => {
                let node_id = payload_str(&event.payload, "node")
                    .or_else(|| payload_str(&event.payload, "nodeId"))
                    .map(str::to_owned);
                let depends_on = node_id
                    .as_deref()
                    .and_then(|node| self.node_deps.get(node))
                    .cloned()
                    .unwrap_or_default();
                self.task_ix.entry(task_id.clone()).or_insert_with(|| {
                    self.tasks.push(TaskSummary {
                        task_id: task_id.clone(),
                        run_id,
                        node_id,
                        state: TaskState::Created,
                        attempts: 0,
                        agent_id: None,
                        depends_on,
                        last_event_type: type_str.to_owned(),
                        last_event_at: stamp.to_owned(),
                        commit_sha: None,
                        budget_exceeded: false,
                    });
                    self.tasks.len() - 1
                });
            }
            "agent.leased" | "task.running" => {
                if let Some(ix) = self.task_ix.get(task_id).copied() {
                    if let Some(agent) = agent_key {
                        self.tasks[ix].agent_id = Some(agent.clone());
                    }
                }
            }
            _ => {}
        }

        let Some(ix) = self.task_ix.get(task_id).copied() else {
            return;
        };
        let task = &mut self.tasks[ix];
        task.last_event_type = type_str.to_owned();
        task.last_event_at = stamp.to_owned();

        // State machine exactly as named (§3.3 fold table).
        if let Some(state) = folded_task_state(type_str) {
            task.state = state;
        }
        match type_str {
            "git.committed" => {
                task.commit_sha = payload_str(&event.payload, "sha")
                    .or_else(|| payload_str(&event.payload, "commitSha"))
                    .map(str::to_owned);
            }
            "approval.required" => {
                if event.payload.get("blocking").and_then(Value::as_bool) == Some(true) {
                    // BUILD-3 semantics: parked for a human; resumption
                    // arrives as a later state event, never inferred.
                    task.state = TaskState::HumanRequired;
                }
            }
            "agent.crashed" => {
                task.attempts += 1;
            }
            "budget.exceeded" => {
                task.budget_exceeded = true;
            }
            _ => {}
        }
    }

    /// Absorb a `workflow.started` node list (`{nodes: [{id, dependsOn}]}`)
    /// when the payload carries one. Applied retroactively to already-folded
    /// tasks and stashed for tasks created later; a non-array `nodes` value
    /// (today's supervisor journals a count) leaves the map untouched.
    fn absorb_node_deps(&mut self, payload: &Value) {
        let Some(nodes) = payload.get("nodes").and_then(Value::as_array) else {
            return;
        };
        for node in nodes {
            let Some(id) = payload_str(node, "id") else {
                continue;
            };
            let deps = node
                .get("dependsOn")
                .and_then(Value::as_array)
                .map(|list| {
                    list.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            self.node_deps.insert(id.to_owned(), deps.clone());
            for task in &mut self.tasks {
                if task.node_id.as_deref() == Some(id) {
                    task.depends_on = deps.clone();
                }
            }
        }
    }

    /// Final pass: derive each run's `taskCounts` from its folded tasks.
    fn finish(mut self) -> Projection {
        for run in &mut self.runs {
            let own: Vec<&TaskSummary> = self
                .tasks
                .iter()
                .filter(|task| task.run_id == run.run_id)
                .collect();
            let done = own
                .iter()
                .filter(|task| task.state == TaskState::Done)
                .count() as u32;
            let failed = own
                .iter()
                .filter(|task| task.state == TaskState::Failed)
                .count() as u32;
            run.task_counts = TaskCounts {
                total: own.len() as u32,
                done,
                failed,
                active: own.len() as u32 - done - failed,
            };
        }
        Projection {
            runs: self.runs,
            tasks: self.tasks,
            agents: self.agents,
        }
    }
}

/// The exactly-as-named task state transitions of the fold table.
fn folded_task_state(type_str: &str) -> Option<TaskState> {
    match type_str {
        "task.created" => Some(TaskState::Created),
        "task.ready" => Some(TaskState::Ready),
        "agent.leased" => Some(TaskState::Leased),
        "task.running" => Some(TaskState::Running),
        "task.output_ready" => Some(TaskState::OutputReady),
        "review.requested" => Some(TaskState::ReviewPending),
        "review.approved" => Some(TaskState::Approved),
        "git.queued" => Some(TaskState::GitQueued),
        "git.committed" => Some(TaskState::Committed),
        "task.done" => Some(TaskState::Done),
        "task.failed" => Some(TaskState::Failed),
        _ => None,
    }
}

/// RFC 3339 with a `Z` suffix and automatic sub-second precision, matching
/// how `agentos-core` serializes `occurredAt` onto the wire.
fn rfc3339(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// String field of a JSON object payload, when present and a string.
fn payload_str<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload.get(key).and_then(Value::as_str)
}

/// Unsigned integer field of a JSON object payload, when present and a
/// non-negative integer (floats are estimates, not counters — ignored).
fn payload_u64(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(Value::as_u64)
}
