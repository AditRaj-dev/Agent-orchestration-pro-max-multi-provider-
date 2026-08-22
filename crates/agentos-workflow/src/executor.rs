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
}

/// Executes one leased task against its contract (the F-02 adapter layer
/// implements this over `RuntimeAdapter`s; tests implement it with mocks).
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Run `task` under `contract`. Implementations must be idempotent-ish:
    /// the same task may be leased again after a crash-induced lease expiry,
    /// so side effects must tolerate at-least-once execution.
    async fn run(&self, task: &TaskRecord, contract: &TaskContract) -> Outcome;
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
}

impl TickReport {
    fn did_work(&self) -> bool {
        self.reclaimed > 0
            || self.promoted > 0
            || self.leased > 0
            || !self.succeeded.is_empty()
            || !self.failed.is_empty()
            || self.requeued > 0
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
        }
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

        // Phase 3: budget gate + lease grants (OR-05: "scheduler checks
        // dependencies, ownership conflicts, budget ... before lease").
        for task in self.scheduler.ready_tasks()? {
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
            self.scheduler
                .grant_lease(&task.id, &self.owner, self.lease_ttl)?;
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
            Outcome::TransientFailure | Outcome::ReasoningFailure => {
                let reasoning = outcome == Outcome::ReasoningFailure;
                let task = self.store.task(&task_id)?;
                let failures_after = task.attempt_count + 1;
                let requeue = self.scheduler.may_requeue(&task, reasoning, failures_after);
                match self.store.record_failure(&task_id, requeue) {
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
}
