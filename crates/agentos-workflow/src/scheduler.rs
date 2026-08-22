//! Dependency-aware scheduler with durable leases (PRD §9 OR-05) and the
//! budget/retry controller (OR-08).
//!
//! The scheduler is a thin, deterministic policy layer over the durable
//! [`TaskStore`]: it decides *what is ready*, *who may lease*, *when leases
//! expire*, and *when budgets escalate a task to `Failed`* — all state
//! changes go through the store's CAS operations, so every decision is
//! crash-safe and multi-engine safe.
//!
//! Semantics (the F-06 contract):
//!
//! - **ready**: task state is `Ready` AND every dependency of its node is in
//!   terminal success (`Done`). Ordered by [`Priority`] (P0 first), then age
//!   (oldest first).
//! - **lease**: `grant_lease` CAS `Ready -> Leased` with owner, expiry
//!   (`now + ttl`) and heartbeat stamped. `heartbeat` renews the expiry by
//!   the granted ttl; `reclaim_expired_leases` moves expired leases back to
//!   `Ready` with `attempt_count + 1` (the crashed attempt is consumed).
//! - **budget gate** before every lease: `attempt_count < max_attempts`,
//!   elapsed-since-creation `< max_elapsed_secs`, and (via the
//!   [`CostLedger`] hook) spend `< max_cost_usd`. Over-budget tasks are
//!   escalated `Ready -> Failed` — never silently re-run. Lease expiry is an
//!   infrastructure (transient) failure, so requeue after reclaim also
//!   honors `retry.transient_retries`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use agentos_core::{CoreError, TaskState};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::WorkflowError;
use crate::store::{TaskRecord, TaskStore};

/// Lease ttl used by the engine when it does not override one.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(60);

/// Cost-source hook for the `max_cost_usd` budget check (OR-05: "scheduler
/// checks ... budget ... before lease"; OR-08: `maxCost`).
///
/// F-06 ships [`NoCostLedger`] (no cost data yet — the F-07 budget ledger
/// will provide a real implementation); the hook is already wired through
/// every budget gate so the integration is a drop-in.
pub trait CostLedger: Send + Sync {
    /// Dollars already spent on this task, if known. `None` means "no cost
    /// data", which never fails the gate.
    fn spent_usd(&self, task: &TaskRecord) -> Option<f64>;
}

/// The default cost source: no cost data (the gate's cost leg is inert).
pub struct NoCostLedger;

impl CostLedger for NoCostLedger {
    fn spent_usd(&self, _task: &TaskRecord) -> Option<f64> {
        None
    }
}

/// Deterministic scheduler over the durable task store.
pub struct Scheduler {
    store: Arc<TaskStore>,
    cost: Arc<dyn CostLedger>,
}

impl Scheduler {
    /// A scheduler with no cost data ([`NoCostLedger`]).
    pub fn new(store: Arc<TaskStore>) -> Self {
        Self {
            store,
            cost: Arc::new(NoCostLedger),
        }
    }

    /// A scheduler with a cost source for the `max_cost_usd` gate.
    pub fn with_cost_ledger(store: Arc<TaskStore>, cost: Arc<dyn CostLedger>) -> Self {
        Self { store, cost }
    }

    /// The underlying store (for engine composition).
    pub fn store(&self) -> &TaskStore {
        &self.store
    }

    /// Tasks that are `Ready` with every dependency in terminal success,
    /// ordered by priority (P0 first) then age (oldest first) — the durable
    /// priority queue head (OR-05).
    pub fn ready_tasks(&self) -> Result<Vec<TaskRecord>, WorkflowError> {
        let all = self.store.tasks()?;
        let states_by_run: HashMap<Uuid, HashMap<String, TaskState>> =
            all.iter().fold(HashMap::new(), |mut runs, task| {
                runs.entry(task.run_id)
                    .or_default()
                    .insert(task.node_id.clone(), task.state);
                runs
            });
        let mut ready: Vec<TaskRecord> = all
            .into_iter()
            .filter(|task| task.state == TaskState::Ready)
            .filter(|task| {
                // The task's own run is keyed above by construction.
                task.dependencies_satisfied(
                    states_by_run
                        .get(&task.run_id)
                        .expect("the task's own run was keyed from the same task list"),
                )
            })
            .collect();
        ready.sort_by(|a, b| {
            (a.priority, a.created_at, a.id).cmp(&(b.priority, b.created_at, b.id))
        });
        Ok(ready)
    }

    /// Grant a lease on a ready task: CAS `Ready -> Leased`, stamping owner,
    /// `now + ttl` expiry and the initial heartbeat (OR-05). The budget gate
    /// is the caller's duty — [`Scheduler::within_budget`] /
    /// [`Scheduler::escalate_over_budget`] — because only the caller knows
    /// the tick's clock.
    pub fn grant_lease(
        &self,
        task_id: &Uuid,
        owner: &str,
        ttl: Duration,
    ) -> Result<TaskRecord, WorkflowError> {
        Ok(self.store.grant_lease(task_id, owner, ttl)?)
    }

    /// Renew a lease held by `owner` (from `Leased` or `Running` — the
    /// worker holds the lease throughout execution): stamps the heartbeat
    /// and extends the expiry by the granted ttl. Errors with
    /// [`WorkflowError::LeaseOwnerMismatch`] if someone else holds the
    /// lease.
    pub fn heartbeat(&self, task_id: &Uuid, owner: &str) -> Result<TaskRecord, WorkflowError> {
        let task = self.store.task(task_id)?;
        let holds = matches!(
            (&task.lease_owner, task.state),
            (Some(holder), TaskState::Leased | TaskState::Running) if holder == owner
        );
        if !holds {
            if let (Some(holder), TaskState::Leased | TaskState::Running) =
                (&task.lease_owner, task.state)
            {
                return Err(WorkflowError::LeaseOwnerMismatch {
                    task: task_id.to_string(),
                    holder: holder.clone(),
                    owner: owner.to_owned(),
                });
            }
            return Err(WorkflowError::Storage(CoreError::IllegalTransition {
                from: task.state,
                to: TaskState::Leased,
            }));
        }
        Ok(self.store.heartbeat(task_id, owner)?)
    }

    /// Crash recovery (OR-05: "lease expiry enables crash recovery without
    /// duplicate ownership"): every lease past its expiry goes back to
    /// `Ready` with `attempt_count + 1` (a `Leased` task directly; a
    /// `Running` task through the legal `Running -> Retryable -> Ready`
    /// walk), then the shared budget gate escalates over-budget tasks to
    /// `Failed`.
    ///
    /// Returns the reclaimed tasks in their post-decision state (`Ready` or
    /// `Failed`).
    pub fn reclaim_expired_leases(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskRecord>, WorkflowError> {
        let mut reclaimed = Vec::new();
        for leased in self.store.tasks_holding_leases()? {
            let Some(expires) = leased.lease_expires_at else {
                continue;
            };
            if expires > now {
                continue;
            }
            let requeued = match leased.state {
                TaskState::Leased => self.store.reclaim_expired(&leased.id)?,
                // The executor died mid-run: park, then requeue.
                TaskState::Running => self.store.record_failure(&leased.id, true)?,
                other => {
                    return Err(WorkflowError::Storage(CoreError::IllegalTransition {
                        from: other,
                        to: TaskState::Ready,
                    }))
                }
            };
            match self.escalate_over_budget(&requeued, now)? {
                Some(failed) => {
                    tracing::warn!(
                        task_id = %failed.id,
                        node = %failed.node_id,
                        attempt_count = failed.attempt_count,
                        "lease expired and budget exhausted; escalated to failed"
                    );
                    reclaimed.push(failed);
                }
                None => reclaimed.push(requeued),
            }
        }
        Ok(reclaimed)
    }

    /// Promote `Planned` tasks whose dependencies are all `Done` to `Ready`
    /// (the dependency gate of the scheduler). Returns the number promoted.
    pub fn promote_unblocked(&self) -> Result<usize, WorkflowError> {
        let mut promoted = 0;
        for run in self.store.runs()? {
            let tasks = self.store.tasks_for_run(&run.id)?;
            let states = states_by_run_node(&tasks);
            for task in &tasks {
                if task.state == TaskState::Planned && task.dependencies_satisfied(&states) {
                    self.store
                        .cas_transition(&task.id, TaskState::Planned, TaskState::Ready)?;
                    promoted += 1;
                }
            }
        }
        Ok(promoted)
    }

    /// After a task fails, park every transitive dependent that is still
    /// `Planned` as `Blocked` (PRD §6.2 failure branch). Returns the number
    /// blocked.
    pub fn block_dependents_of(&self, failed: &TaskRecord) -> Result<usize, WorkflowError> {
        let tasks = self.store.tasks_for_run(&failed.run_id)?;
        let mut blocked = 0;
        // Worklist over reverse edges: tasks (Planned) depending on the
        // failed node, transitively.
        let mut frontier = vec![failed.node_id.clone()];
        let mut seen = std::collections::HashSet::new();
        while let Some(node_id) = frontier.pop() {
            for task in &tasks {
                if task.state == TaskState::Planned
                    && task.node.depends_on.contains(&node_id)
                    && seen.insert(task.id)
                {
                    self.store
                        .cas_transition(&task.id, TaskState::Planned, TaskState::Blocked)?;
                    blocked += 1;
                    frontier.push(task.node_id.clone());
                }
            }
        }
        Ok(blocked)
    }

    /// The budget gate (OR-08), checked before every lease: attempts,
    /// elapsed time, and cost via the [`CostLedger`] hook.
    pub fn within_budget(&self, task: &TaskRecord, now: DateTime<Utc>) -> bool {
        let budgets = &task.node.budgets;
        if task.attempt_count >= budgets.max_attempts {
            return false;
        }
        if now.signed_duration_since(task.created_at).num_seconds()
            >= budgets.max_elapsed_secs as i64
        {
            return false;
        }
        if let (Some(max_cost), Some(spent)) = (budgets.max_cost_usd, self.cost.spent_usd(task)) {
            if spent >= max_cost {
                return false;
            }
        }
        true
    }

    /// Escalate an over-budget task `Ready -> Failed` (OR-08 escalation
    /// after bounded retries). Returns `Some(failed_record)` when the
    /// escalation fired, `None` when the task is within budget.
    pub fn escalate_over_budget(
        &self,
        task: &TaskRecord,
        now: DateTime<Utc>,
    ) -> Result<Option<TaskRecord>, WorkflowError> {
        if self.within_budget(task, now) {
            return Ok(None);
        }
        let failed = self
            .store
            .cas_transition(&task.id, TaskState::Ready, TaskState::Failed)?;
        tracing::warn!(
            task_id = %failed.id,
            node = %failed.node_id,
            attempt_count = failed.attempt_count,
            "budget exhausted; task escalated to failed"
        );
        Ok(Some(failed))
    }

    /// Whether a reported failure may requeue under the node's retry policy
    /// (OR-08: transient infrastructure errors vs reasoning failures).
    ///
    /// `failures_after` is the task's `attempt_count` including the failure
    /// being decided. A failure requeues while `failures_after <= allowance`;
    /// the budget gate independently bounds total leases via `max_attempts`.
    pub fn may_requeue(&self, task: &TaskRecord, reasoning: bool, failures_after: u32) -> bool {
        let allowance = if reasoning {
            task.node.retry.reasoning_retries
        } else {
            task.node.retry.transient_retries
        };
        failures_after <= allowance
    }
}

/// Key a run's task states by node id.
fn states_by_run_node(tasks: &[TaskRecord]) -> HashMap<String, TaskState> {
    tasks
        .iter()
        .map(|task| (task.node_id.clone(), task.state))
        .collect()
}
