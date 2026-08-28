//! The executor boundary and the [`WorkflowEngine`] that ties store,
//! scheduler and executors into one deterministic machine (PRD §9 OR-03/
//! OR-05).
//!
//! [`TaskExecutor`] is deliberately provider-free: tests (and later the
//! F-02 adapter layer) plug in any async closure, mock or real runtime
//! adapter without touching the engine. The engine is composable
//! primitives plus a tokio driver method — **not** a daemon loop:
//!
//! - [`WorkflowEngine::start_run`] validates the spec (OR-03: acyclicity,
//!   dependency references and bounded loops checked BEFORE any state is
//!   created) and materializes it into durable records in one transaction.
//! - [`WorkflowEngine::tick`] is one scheduling pass: reclaim expired
//!   leases -> promote dependency-satisfied tasks -> budget gate -> grant
//!   leases -> spawn executor futures -> await and record every outcome via
//!   CAS. It returns when the pass's work is done.
//! - [`WorkflowEngine::run_until_idle`] is the bounded driver: `tick` until
//!   a pass does nothing, with a hard tick ceiling so a pathological
//!   retrying workflow errors out instead of spinning forever.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use agentos_core::{CoreError, TaskState};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::error::WorkflowError;
use crate::scheduler::{Scheduler, DEFAULT_LEASE_TTL};
use crate::spec::{NodeSpec, TaskContract, WorkflowSpec};
use crate::store::{RunStatus, TaskRecord, TaskStore};
use crate::validate;

/// The result of one executor run (OR-08's failure taxonomy: transient
/// infrastructure errors are retried more generously than reasoning
/// failures).
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// The bounded task completed; `packet` is the structured completion
    /// packet against the contract (OR-04). F-06 returns packets in the
    /// [`TickReport`]; durable artifact storage is the content-addressed
    /// store's job (F-07/F-10), not the task table's.
    Success { packet: Value },
    /// The agent failed by reasoning (bad output, wrong path, contract
    /// violation) — governed by `retry.reasoning_retries`.
    ReasoningFailure,
    /// A reasoning failure that should route the next allowed attempt to a
    /// different agent role. The engine consumes the failed attempt and
    /// retargets atomically before the task becomes leaseable again.
    EscalatedReasoningFailure { agent_role: String },
    /// The failure is transient infrastructure (worker crash, IO, provider
    /// outage) — governed by `retry.transient_retries`.
    TransientFailure,
    /// The task cannot proceed without a human decision (an approval gate
    /// with no live approval). The task parks in `HumanRequired` and
    /// **consumes no attempt** — waiting on a person is not a failed try.
    ///
    /// Nothing in the engine un-parks it: resuming is an explicit
    /// `HumanRequired -> Ready` (rework/resume) or `-> Approved` (sign-off)
    /// CAS by whoever resolves the approval, and `-> Failed`/`-> Cancelled`
    /// stay available so a parked task is never un-cancellable.
    AwaitingApproval,
    /// The task could not start for a reason that is nobody's failure and
    /// will clear on its own — today: another task holds overlapping path
    /// ownership. The task returns to `Ready` and **consumes no attempt**:
    /// waiting your turn is not a failed try, and a serialized graph used
    /// to exhaust its retry budget queueing rather than working.
    Deferred { reason: String },
}

/// Executes one leased task against its contract (the F-02 adapter layer
/// implements this over `RuntimeAdapter`s; tests implement it with mocks).
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Run `task` under `contract`. Implementations must be idempotent-ish:
    /// the same task may be leased again after a crash-induced lease expiry,
    /// so side effects must tolerate at-least-once execution.
    async fn run(&self, task: &TaskRecord, contract: &TaskContract) -> Outcome;

    /// Best-effort cancellation hook invoked only after the durable task row
    /// has entered `Cancelled`. Implementations that own a child process or
    /// provider request can stop it here; the default preserves compatibility
    /// for simple executors and makes cancellation safe to adopt incrementally.
    fn cancel(&self, _task: &TaskRecord) {}
}

/// In-memory engine admission configuration. Run pause state is persisted in
/// [`TaskStore`], while this cap deliberately remains a deployment setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineConfig {
    /// Maximum attempts this engine may admit in one scheduling pass.
    pub max_concurrency: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self { max_concurrency: 4 }
    }
}

impl EngineConfig {
    /// Validate the bounded operator-facing concurrency range.
    pub fn validate(self) -> Result<Self, WorkflowError> {
        if !(1..=8).contains(&self.max_concurrency) {
            return Err(WorkflowError::InvalidMaxConcurrency {
                value: self.max_concurrency,
            });
        }
        Ok(self)
    }
}

/// Why a task has not been admitted in the current tick. These are
/// observational queue reasons, never state changes: clearing a reason lets
/// normal scheduler admission proceed without a special recovery path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueReason {
    Dependency,
    Concurrency,
    PathConflict,
    Approval,
    Paused,
    ProviderUnavailable,
    GitUnavailable,
}

/// One queued task and the reasons currently visible to the engine.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueBlock {
    pub task_id: Uuid,
    pub run_id: Uuid,
    pub node_id: String,
    pub reasons: Vec<QueueReason>,
}

/// Explicit operator commands exposed by the engine service layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlAction {
    Pause,
    Resume,
    Cancel,
    Retry,
    Reopen,
    Reroute,
}

/// Result of an idempotent control request. `changed` contains durable task
/// ids; `skipped` names rows that were already terminal or immutable.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlResult {
    pub action: ControlAction,
    pub run_id: Option<Uuid>,
    pub task_id: Option<Uuid>,
    pub changed: Vec<Uuid>,
    pub skipped: Vec<Uuid>,
}

/// Lightweight hook intended for daemon event-journal integration. The
/// workflow crate deliberately does not own a journal, so callers can append
/// this event to their existing durable stream without coupling the engine to
/// the daemon crate.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowControlEvent {
    pub action: ControlAction,
    pub run_id: Option<Uuid>,
    pub task_id: Option<Uuid>,
    pub changed: Vec<Uuid>,
}

/// Sink for control-plane events. Hooks must be non-blocking and idempotent;
/// durable control state is written before this notification fires.
pub trait WorkflowControlHook: Send + Sync {
    fn on_control(&self, event: &WorkflowControlEvent);
}

/// How a task terminally failed, for [`TickReport`] consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// Escalated by the budget gate (attempts / elapsed / cost).
    BudgetExceeded,
    /// Transient failures exhausted `transient_retries`.
    TransientExhausted,
    /// Reasoning failures exhausted `reasoning_retries`.
    ReasoningExhausted,
    /// The executor future panicked; the lease expires and reclaim decides
    /// the retry (the designed crash-recovery path).
    ExecutorCrashed,
}

/// A successful completion surfaced by one tick.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskSuccess {
    pub task_id: Uuid,
    pub node_id: String,
    pub packet: Value,
}

/// A terminal failure surfaced by one tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFailure {
    pub task_id: Uuid,
    pub node_id: String,
    pub kind: FailureKind,
}

/// What one scheduling pass did. Every counter is per-pass; a default
/// `TickReport` means the pass found nothing to do (the driver's idle
/// signal).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TickReport {
    /// Expired leases reclaimed (crash recovery).
    pub reclaimed: usize,
    /// `Planned` tasks promoted to `Ready` by dependency satisfaction.
    pub promoted: usize,
    /// Leases granted to this engine's executor.
    pub leased: usize,
    /// Successful completions (with their packets).
    pub succeeded: Vec<TaskSuccess>,
    /// Terminal failures, with the failure kind.
    pub failed: Vec<TaskFailure>,
    /// Failures re-queued for another attempt.
    pub requeued: usize,
    /// Over-budget tasks escalated `Ready -> Failed` by a gate.
    pub escalated_over_budget: usize,
    /// `Planned` dependents parked as `Blocked` after a failure.
    pub blocked: usize,
    /// Outcomes dropped because another writer moved the task first (e.g. a
    /// second engine reclaimed the lease mid-run); the durable row wins.
    pub conflicted: usize,
    /// Tasks parked in `HumanRequired` awaiting a human decision. No
    /// attempt was consumed and the engine will not pick them up again
    /// until someone transitions them out.
    pub awaiting_approval: usize,
    /// Tasks returned to `Ready` without consuming an attempt because they
    /// could not start yet (path ownership held by a peer).
    pub deferred: usize,
    /// Ready work left queued by a transient admission rule. This is useful
    /// for a daemon status surface but deliberately does not keep the bounded
    /// driver spinning.
    pub queued: Vec<QueueBlock>,
}

impl TickReport {
    fn did_work(&self) -> bool {
        self.reclaimed > 0
            || self.promoted > 0
            || self.leased > 0
            || !self.succeeded.is_empty()
            || !self.failed.is_empty()
            || self.requeued > 0
            || self.deferred > 0
            || self.escalated_over_budget > 0
            || self.blocked > 0
            || self.conflicted > 0
            || self.awaiting_approval > 0
    }
}

/// Aggregate of a bounded driver run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DriverReport {
    /// Ticks executed (including the idle final pass).
    pub ticks: u32,
    pub succeeded: Vec<TaskSuccess>,
    pub failed: Vec<TaskFailure>,
}

/// The deterministic workflow engine: durable store + scheduler + executor.
pub struct WorkflowEngine {
    store: Arc<TaskStore>,
    scheduler: Scheduler,
    executor: Arc<dyn TaskExecutor>,
    /// Lease-owner identity of this engine instance (OR-05 ownership).
    owner: String,
    lease_ttl: Duration,
    config: EngineConfig,
    control_hook: Option<Arc<dyn WorkflowControlHook>>,
}

impl WorkflowEngine {
    /// Build an engine over `store` whose tasks are executed by `executor`.
    pub fn new(store: Arc<TaskStore>, executor: Arc<dyn TaskExecutor>) -> Self {
        Self {
            scheduler: Scheduler::new(Arc::clone(&store)),
            store,
            executor,
            owner: format!("engine-{}", Uuid::now_v7()),
            lease_ttl: DEFAULT_LEASE_TTL,
            config: EngineConfig::default(),
            control_hook: None,
        }
    }

    /// Build with an explicit, validated admission configuration.
    pub fn new_with_config(
        store: Arc<TaskStore>,
        executor: Arc<dyn TaskExecutor>,
        config: EngineConfig,
    ) -> Result<Self, WorkflowError> {
        Ok(Self::new(store, executor).with_config(config)?)
    }

    /// Replace the admission configuration after validating it.
    pub fn with_config(mut self, config: EngineConfig) -> Result<Self, WorkflowError> {
        self.config = config.validate()?;
        Ok(self)
    }

    /// Attach a best-effort observer for daemon/service integration.
    pub fn with_control_hook(mut self, hook: Arc<dyn WorkflowControlHook>) -> Self {
        self.control_hook = Some(hook);
        self
    }

    /// Override the lease ttl this engine grants (default
    /// [`DEFAULT_LEASE_TTL`]).
    pub fn with_lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// The durable store (for direct reads/projections).
    pub fn store(&self) -> &TaskStore {
        &self.store
    }

    /// The scheduler (for direct lease/heartbeat control by embedders).
    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// This engine's lease-owner identity.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Current in-memory admission configuration.
    pub fn config(&self) -> EngineConfig {
        self.config
    }

    /// Persistently pause a run. This has drain semantics: it does not touch
    /// existing leases, and only prevents subsequent lease grants.
    pub fn pause_run(&self, run_id: &Uuid) -> Result<ControlResult, WorkflowError> {
        self.store.set_run_paused(run_id, true)?;
        Ok(self.emit_control(
            ControlAction::Pause,
            Some(*run_id),
            None,
            Vec::new(),
            Vec::new(),
        ))
    }

    /// Resume a persistently paused run.
    pub fn resume_run(&self, run_id: &Uuid) -> Result<ControlResult, WorkflowError> {
        self.store.set_run_paused(run_id, false)?;
        Ok(self.emit_control(
            ControlAction::Resume,
            Some(*run_id),
            None,
            Vec::new(),
            Vec::new(),
        ))
    }

    /// Cancel one queued, parked or running task. A running executor is told
    /// after durable cancellation; a late outcome then loses its CAS race.
    pub fn cancel_task(&self, task_id: &Uuid) -> Result<ControlResult, WorkflowError> {
        let before = self.store.task(task_id)?;
        let cancelled = self.store.cancel_task(task_id)?;
        if cancelled.is_some() && matches!(before.state, TaskState::Leased | TaskState::Running) {
            self.executor.cancel(&before);
        }
        if let Some(task) = &cancelled {
            // A dependent cannot become ready behind an explicitly abandoned
            // prerequisite; park it with the same deterministic propagation
            // used for exhausted retries.
            let _ = self.scheduler.block_dependents_of(task)?;
        }
        self.refresh_run_statuses()?;
        Ok(self.emit_control(
            ControlAction::Cancel,
            Some(before.run_id),
            Some(*task_id),
            cancelled.iter().map(|task| task.id).collect(),
            if cancelled.is_none() {
                vec![*task_id]
            } else {
                Vec::new()
            },
        ))
    }

    /// Cancel every cancellable task in a run, using the same best-effort
    /// hook for attempts that were already leased or running.
    pub fn cancel_run(&self, run_id: &Uuid) -> Result<ControlResult, WorkflowError> {
        let before = self.store.tasks_for_run(run_id)?;
        let cancelled = self.store.cancel_run(run_id)?;
        let changed: HashSet<Uuid> = cancelled.iter().map(|task| task.id).collect();
        for task in before.iter().filter(|task| {
            changed.contains(&task.id)
                && matches!(task.state, TaskState::Leased | TaskState::Running)
        }) {
            self.executor.cancel(task);
        }
        self.refresh_run_statuses()?;
        let skipped = before
            .iter()
            .filter(|task| !changed.contains(&task.id))
            .map(|task| task.id)
            .collect();
        Ok(self.emit_control(
            ControlAction::Cancel,
            Some(*run_id),
            None,
            cancelled.into_iter().map(|task| task.id).collect(),
            skipped,
        ))
    }

    /// Reopen one explicitly failed task with a fresh retry budget.
    pub fn retry_task(&self, task_id: &Uuid) -> Result<ControlResult, WorkflowError> {
        let before = self.store.task(task_id)?;
        let reopened = self.store.reopen_task(task_id)?;
        let mut changed: Vec<Uuid> = reopened.iter().map(|task| task.id).collect();
        if reopened.is_some() {
            changed.extend(
                self.store
                    .unpark_blocked_dependents(task_id)?
                    .into_iter()
                    .map(|task| task.id),
            );
        }
        self.refresh_run_statuses()?;
        Ok(self.emit_control(
            ControlAction::Retry,
            Some(before.run_id),
            Some(*task_id),
            changed,
            if reopened.is_none() {
                vec![*task_id]
            } else {
                Vec::new()
            },
        ))
    }

    /// Reopen every failed task in a run and unpark its blocked dependents.
    pub fn reopen_run(&self, run_id: &Uuid) -> Result<crate::store::ReopenReport, WorkflowError> {
        let report = self.store.reopen_run(run_id)?;
        let changed = self
            .store
            .tasks_for_run(run_id)?
            .into_iter()
            .filter(|task| {
                report.reopened.contains(&task.node_id) || report.unblocked.contains(&task.node_id)
            })
            .map(|task| task.id)
            .collect();
        let _ = self.emit_control(
            ControlAction::Reopen,
            Some(*run_id),
            None,
            changed,
            Vec::new(),
        );
        Ok(report)
    }

    /// Reroute only work that has not acquired an execution profile. The
    /// store refuses leased/running and terminal rows atomically.
    pub fn reroute_task(
        &self,
        task_id: &Uuid,
        role: Option<&str>,
    ) -> Result<ControlResult, WorkflowError> {
        let before = self.store.task(task_id)?;
        let rerouted = self.store.set_agent_role(task_id, role)?;
        Ok(self.emit_control(
            ControlAction::Reroute,
            Some(before.run_id),
            Some(*task_id),
            rerouted.iter().map(|task| task.id).collect(),
            if rerouted.is_none() {
                vec![*task_id]
            } else {
                Vec::new()
            },
        ))
    }

    /// Start a run of `spec` under `goal`: validate first (OR-03 — nothing
    /// is created for a spec that could not run), then materialize the run
    /// and one durable task per node in a single transaction. Returns the
    /// run id.
    pub fn start_run(&self, spec: &WorkflowSpec, goal: &str) -> Result<Uuid, WorkflowError> {
        validate::validate(spec)?;
        let run_id = self.store.create_run(spec, goal)?;
        tracing::info!(
            run_id = %run_id,
            workflow = %spec.id,
            version = spec.version,
            nodes = spec.nodes.len(),
            "run started"
        );
        Ok(run_id)
    }

    /// Add a node to a live run (F-12 re-planning): the store validates the
    /// tentative DAG and inserts the task in one transaction; the next tick
    /// picks it up. Rejections carry the same machine-readable
    /// [`ValidationError`](crate::ValidationError) codes as `start_run`.
    pub fn add_task(&self, run_id: &Uuid, node: &NodeSpec) -> Result<Uuid, WorkflowError> {
        let task_id = self.store.add_task(run_id, node)?;
        tracing::info!(run_id = %run_id, node = %node.id, task_id = %task_id,
            "node added to live run");
        Ok(task_id)
    }

    /// Live status of a run, computed from its durable tasks (the stored
    /// column is only a projection).
    pub fn run_status(&self, run_id: &Uuid) -> Result<RunStatus, WorkflowError> {
        let tasks = self.store.tasks_for_run(run_id)?;
        Ok(RunStatus::from_tasks(&tasks))
    }

    /// One deterministic scheduling pass. See the [module docs](self) for
    /// the exact phase order.
    pub async fn tick(&self) -> Result<TickReport, WorkflowError> {
        let now = Utc::now();
        let mut report = TickReport::default();

        // Phase 1: crash recovery — expired leases requeue (or escalate).
        for reclaimed in self.scheduler.reclaim_expired_leases(now)? {
            report.reclaimed += 1;
            if reclaimed.state == TaskState::Failed {
                report.failed.push(TaskFailure {
                    task_id: reclaimed.id,
                    node_id: reclaimed.node_id.clone(),
                    kind: FailureKind::BudgetExceeded,
                });
                report.blocked += self.scheduler.block_dependents_of(&reclaimed)?;
            }
        }

        // Phase 2: dependency gate — promote Planned tasks whose deps are
        // all Done.
        report.promoted = self.scheduler.promote_unblocked()?;

        // Phase 3: budget gate + bounded, conflict-aware lease grants (OR-05: "scheduler checks
        // dependencies, ownership conflicts, budget ... before lease").
        let runs: BTreeMap<Uuid, _> = self
            .store
            .runs()?
            .into_iter()
            .map(|run| (run.id, run))
            .collect();
        let mut reserved = self.store.tasks_holding_leases()?;
        let mut available = self.config.max_concurrency.saturating_sub(reserved.len());
        for task in self.scheduler.ready_tasks()? {
            if runs.get(&task.run_id).is_some_and(|run| run.paused) {
                report.queued.push(queue_block(&task, QueueReason::Paused));
                continue;
            }
            if available == 0 {
                report
                    .queued
                    .push(queue_block(&task, QueueReason::Concurrency));
                continue;
            }
            if reserved
                .iter()
                .any(|active| write_scopes_overlap(&task, active))
            {
                report
                    .queued
                    .push(queue_block(&task, QueueReason::PathConflict));
                continue;
            }
            if let Some(failed) = self.scheduler.escalate_over_budget(&task, now)? {
                report.escalated_over_budget += 1;
                report.failed.push(TaskFailure {
                    task_id: failed.id,
                    node_id: failed.node_id.clone(),
                    kind: FailureKind::BudgetExceeded,
                });
                report.blocked += self.scheduler.block_dependents_of(&failed)?;
                continue;
            }
            let leased = self
                .scheduler
                .grant_lease(&task.id, &self.owner, self.lease_ttl)?;
            reserved.push(leased);
            available -= 1;
            report.leased += 1;
        }

        // Phase 4: run our leased tasks. Leased -> Running is a CAS; a miss
        // (another engine raced us) is reported, not fatal.
        let mut joins: JoinSet<(Uuid, String, Outcome)> = JoinSet::new();
        for task in self.store.leased_by(&self.owner)? {
            match self
                .store
                .cas_transition(&task.id, TaskState::Leased, TaskState::Running)
            {
                Ok(_) => {
                    let executor = Arc::clone(&self.executor);
                    joins.spawn(async move {
                        let outcome = executor.run(&task, &task.contract).await;
                        (task.id, task.node_id.clone(), outcome)
                    });
                }
                Err(CoreError::IllegalTransition { .. }) => {
                    tracing::warn!(task_id = %task.id, "lease lost before execution start");
                    report.conflicted += 1;
                }
                Err(err) => return Err(err.into()),
            }
        }

        // Phase 5: record outcomes via CAS. A panicked executor is
        // reconciled below; its lease expires and reclaim (phase 1 of a
        // later tick) decides the retry — the designed crash-recovery path.
        while let Some(joined) = joins.join_next().await {
            match joined {
                Ok((task_id, node_id, outcome)) => {
                    self.record_outcome(task_id, node_id, outcome, &mut report)?;
                }
                Err(join_err) => {
                    tracing::error!(error = %join_err, "executor future panicked");
                }
            }
        }
        // Panic reconciliation: anything of ours still `Running` had an
        // executor that never returned an outcome. The durable row (with
        // its lease) is left as-is — lease expiry drives recovery.
        for orphan in self.store.tasks()? {
            if orphan.state == TaskState::Running
                && orphan.lease_owner.as_deref() == Some(&self.owner)
            {
                tracing::error!(
                    task_id = %orphan.id,
                    node = %orphan.node_id,
                    "executor crashed; awaiting lease expiry for recovery"
                );
                report.failed.push(TaskFailure {
                    task_id: orphan.id,
                    node_id: orphan.node_id.clone(),
                    kind: FailureKind::ExecutorCrashed,
                });
            }
        }

        // Phase 6: refresh the cached run-status projections.
        self.refresh_run_statuses()?;

        Ok(report)
    }

    /// The bounded driver: `tick` until a pass does no work, at most
    /// `max_ticks` passes. Hitting the ceiling with work pending is an
    /// error (a runaway loop guard), not silent truncation.
    pub async fn run_until_idle(&self, max_ticks: u32) -> Result<DriverReport, WorkflowError> {
        let mut driver = DriverReport::default();
        for tick_no in 1..=max_ticks {
            let report = self.tick().await?;
            let worked = report.did_work();
            driver.ticks = tick_no;
            driver.succeeded.extend(report.succeeded);
            driver.failed.extend(report.failed);
            if !worked {
                return Ok(driver);
            }
        }
        Err(WorkflowError::MaxTicksExceeded { max_ticks })
    }

    /// Record one executor outcome through the store's CAS paths.
    fn record_outcome(
        &self,
        task_id: Uuid,
        node_id: String,
        outcome: Outcome,
        report: &mut TickReport,
    ) -> Result<(), WorkflowError> {
        match outcome {
            Outcome::Success { packet } => match self.store.record_success(&task_id) {
                Ok(_) => {
                    tracing::info!(task_id = %task_id, node = %node_id, "task done");
                    report.succeeded.push(TaskSuccess {
                        task_id,
                        node_id,
                        packet,
                    });
                }
                // The durable row moved first (e.g. reclaimed by another
                // engine); its state wins, our packet is dropped.
                Err(CoreError::IllegalTransition { .. }) => {
                    tracing::warn!(task_id = %task_id, "outcome dropped; task moved on");
                    report.conflicted += 1;
                }
                Err(err) => return Err(err.into()),
            },
            Outcome::AwaitingApproval => {
                match self.store.cas_transition(
                    &task_id,
                    TaskState::Running,
                    TaskState::HumanRequired,
                ) {
                    Ok(_) => {
                        tracing::info!(task_id = %task_id, node = %node_id,
                            "task parked awaiting a human decision");
                        report.awaiting_approval += 1;
                    }
                    Err(CoreError::IllegalTransition { .. }) => {
                        tracing::warn!(task_id = %task_id, "outcome dropped; task moved on");
                        report.conflicted += 1;
                    }
                    Err(err) => return Err(err.into()),
                }
            }
            Outcome::Deferred { reason } => {
                // Running -> Retryable -> Ready: the lifecycle graph has no
                // direct `Running -> Ready` arc, and `record_failure` is the
                // wrong door — it would bill an attempt.
                match self
                    .store
                    .cas_transition(&task_id, TaskState::Running, TaskState::Retryable)
                    .and_then(|_| {
                        self.store
                            .cas_transition(&task_id, TaskState::Retryable, TaskState::Ready)
                    }) {
                    Ok(_) => {
                        tracing::info!(task_id = %task_id, node = %node_id, %reason,
                            "task deferred; requeued without consuming an attempt");
                        report.deferred += 1;
                    }
                    Err(CoreError::IllegalTransition { .. }) => {
                        tracing::warn!(task_id = %task_id, "outcome dropped; task moved on");
                        report.conflicted += 1;
                    }
                    Err(err) => return Err(err.into()),
                }
            }
            Outcome::TransientFailure
            | Outcome::ReasoningFailure
            | Outcome::EscalatedReasoningFailure { .. } => {
                let (reasoning, retarget_role) = match outcome {
                    Outcome::TransientFailure => (false, None),
                    Outcome::ReasoningFailure => (true, None),
                    Outcome::EscalatedReasoningFailure { agent_role } => (true, Some(agent_role)),
                    _ => unreachable!("success, approval and deferral matched above"),
                };
                let task = self.store.task(&task_id)?;
                let failures_after = task.attempt_count + 1;
                let requeue = self.scheduler.may_requeue(&task, reasoning, failures_after);
                match self.store.record_failure_with_retarget(
                    &task_id,
                    requeue,
                    retarget_role.as_deref(),
                ) {
                    Ok(record) => {
                        if record.state == TaskState::Ready {
                            report.requeued += 1;
                        } else {
                            let kind = if reasoning {
                                FailureKind::ReasoningExhausted
                            } else {
                                FailureKind::TransientExhausted
                            };
                            tracing::warn!(task_id = %task_id, node = %node_id, kind = ?kind,
                                "task failed terminally");
                            report.failed.push(TaskFailure {
                                task_id,
                                node_id,
                                kind,
                            });
                            report.blocked += self.scheduler.block_dependents_of(&record)?;
                        }
                    }
                    Err(CoreError::IllegalTransition { .. }) => {
                        tracing::warn!(task_id = %task_id, "outcome dropped; task moved on");
                        report.conflicted += 1;
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }
        Ok(())
    }

    /// Recompute every run's stored status from its durable tasks.
    fn refresh_run_statuses(&self) -> Result<(), WorkflowError> {
        for run in self.store.runs()? {
            let tasks = self.store.tasks_for_run(&run.id)?;
            let status = RunStatus::from_tasks(&tasks);
            if run.status != status {
                self.store.set_run_status(&run.id, status)?;
            }
        }
        Ok(())
    }

    fn emit_control(
        &self,
        action: ControlAction,
        run_id: Option<Uuid>,
        task_id: Option<Uuid>,
        changed: Vec<Uuid>,
        skipped: Vec<Uuid>,
    ) -> ControlResult {
        let result = ControlResult {
            action,
            run_id,
            task_id,
            changed,
            skipped,
        };
        if let Some(hook) = &self.control_hook {
            hook.on_control(&WorkflowControlEvent {
                action,
                run_id,
                task_id,
                changed: result.changed.clone(),
            });
        }
        result
    }
}

fn queue_block(task: &TaskRecord, reason: QueueReason) -> QueueBlock {
    QueueBlock {
        task_id: task.id,
        run_id: task.run_id,
        node_id: task.node_id.clone(),
        reasons: vec![reason],
    }
}

/// Empty scopes mean "not declared", not "the entire repository". Runtime
/// composition should persist concrete allowed paths before admission; once
/// present, same-or-ancestor paths serialize their writes.
fn write_scopes_overlap(left: &TaskRecord, right: &TaskRecord) -> bool {
    left.contract.allowed_paths.iter().any(|a| {
        right
            .contract
            .allowed_paths
            .iter()
            .any(|b| path_overlaps(a, b))
    })
}

fn path_overlaps(left: &str, right: &str) -> bool {
    let left = scope_root(left);
    let right = scope_root(right);
    if left.is_empty() || right.is_empty() {
        return true;
    }
    left == right
        || left
            .strip_prefix(&right)
            .is_some_and(|tail| tail.starts_with('/'))
        || right
            .strip_prefix(&left)
            .is_some_and(|tail| tail.starts_with('/'))
}

/// Collapse common repository glob declarations into the directory they
/// reserve. A wildcard at the root reserves the whole project; precise file
/// paths retain their filename and only conflict with that exact path.
fn scope_root(path: &str) -> String {
    let path = path.trim().trim_matches('/').replace('\\', "/");
    path.trim_end_matches("/**")
        .trim_end_matches("/*")
        .trim_end_matches('/')
        .to_owned()
}
