//! The boundary where a proposal becomes durable engine state.
//!
//! [`PlanSink`] is deliberately narrow — three methods, all of them
//! delegating to `agentos-workflow`:
//!
//! - [`PlanSink::commit`] hands a compiled [`WorkflowSpec`] to the engine,
//!   which validates it *again* and materializes durable task records.
//! - [`PlanSink::run_view`] reads the engine's own state back (the
//!   orchestrator's only view of reality; it keeps no shadow copy).
//! - [`PlanSink::escalate`] retunes priority through the engine's
//!   [`TaskStore::set_priority`] — the lever F-06 explicitly left for
//!   OR-01.
//!
//! There is no method for "mutate a live task graph", by design. The
//! orchestrator proposes; the engine disposes.
//!
//! Two implementations ship:
//!
//! | Impl | Use |
//! |---|---|
//! | [`WorkflowSink`] | store-only embedders (a planning session with no executor attached) |
//! | `WorkflowEngine` | the full engine, so commits go through [`WorkflowEngine::start_run`] verbatim |
//!
//! **Liveness contract.** Nothing in this module is required for a run to
//! progress. Once [`PlanSink::commit`] returns, the engine ticks on its own;
//! an orchestrator that crashes, hangs or loses its model simply stops
//! proposing (PRD §9 OR-01 acceptance criterion 2).

use std::sync::Arc;

use agentos_core::{Priority, TaskState};
use agentos_workflow::{NodeSpec, RunStatus, TaskStore, WorkflowEngine};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{OrchestratorError, RejectionReason};
use crate::plan::Plan;

/// One durable task, as the orchestrator sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskView {
    /// Durable task id.
    pub task_id: Uuid,
    /// The workflow node it materializes.
    pub node_id: String,
    /// Lifecycle state (PRD §6.2).
    pub state: TaskState,
    /// Scheduling priority.
    pub priority: Priority,
    /// Attempts consumed so far.
    pub attempt_count: u32,
    /// Pool the node is routed to, when assigned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
}

/// The engine's view of a run — the summarized worker outcomes OR-01 feeds
/// back into the orchestrator's state snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunView {
    /// The run.
    pub run_id: Uuid,
    /// Status computed from the durable tasks.
    pub status: RunStatus,
    /// One entry per materialized task.
    pub tasks: Vec<TaskView>,
}

/// Where a validated plan is committed and from which engine state is read.
pub trait PlanSink: Send + Sync {
    /// Validate and materialize `plan` into a run, returning the run id.
    ///
    /// Implementations MUST route validation through `agentos-workflow`
    /// rather than trusting the plan's own pre-check.
    fn commit(&self, plan: &Plan) -> Result<Uuid, OrchestratorError>;

    /// Read the engine's current view of a run.
    fn run_view(&self, run_id: &Uuid) -> Result<RunView, OrchestratorError>;

    /// Append one node to a live run, returning the new durable task id.
    ///
    /// The engine validates the tentative DAG inside its insert
    /// transaction, so a cycle, a dangling dependency or a duplicate node
    /// id is refused here rather than being trusted from the draft.
    fn add_node(
        &self,
        run_id: &Uuid,
        node: &NodeSpec,
        priority: Option<Priority>,
    ) -> Result<Uuid, OrchestratorError>;

    /// Apply an escalation to a live run: raise the named node (or every
    /// non-terminal task when `node_id` is `None`) to `P0` so the engine's
    /// ready-queue serves it first. Returns how many tasks were retuned.
    ///
    /// This is the whole of the engine-side effect. Routing an escalation
    /// to a stronger model, a supervisor or a human approval gate needs the
    /// reviewer/supervisor pools (F-13) and the F-10 approval store; until
    /// those land the escalation is recorded in the plan ledger and
    /// surfaced to the user, never silently swallowed.
    ///
    /// A node-scoped escalation naming a terminal task, or a node the run
    /// carries no task for, MUST be refused with
    /// [`RejectionReason::TaskTerminal`] / [`RejectionReason::TaskNotInRun`]
    /// rather than answered `Ok(_)`. Both levers escalation owns act on a
    /// task's *next attempt*; reporting success for a task that will never
    /// have one is what let a planner burn three cycles climbing its
    /// escalation ladder against a dead node.
    fn escalate(&self, run_id: &Uuid, node_id: Option<&str>) -> Result<usize, OrchestratorError>;

    /// Route a live run's task(s) at another agent pool — the engine-side
    /// effect of escalating to a stronger agent or a supervisor.
    ///
    /// Only tasks that are queued or parked are retargeted: the engine
    /// refuses a swap under a `Leased`/`Running` task (its attempt is
    /// already contracted against the old role). Returns how many tasks
    /// actually moved, so an escalation that changed nothing is reported
    /// rather than claimed — and a node-scoped retarget against a terminal
    /// or absent task is refused outright, exactly as
    /// [`PlanSink::escalate`] refuses it.
    fn retarget(
        &self,
        run_id: &Uuid,
        node_id: Option<&str>,
        pool: &str,
    ) -> Result<usize, OrchestratorError>;
}

/// A [`PlanSink`] over the durable task store alone.
pub struct WorkflowSink {
    store: Arc<TaskStore>,
}

impl WorkflowSink {
    /// Wrap a task store.
    pub fn new(store: Arc<TaskStore>) -> Self {
        Self { store }
    }

    /// The underlying store.
    pub fn store(&self) -> &Arc<TaskStore> {
        &self.store
    }
}

impl PlanSink for WorkflowSink {
    fn commit(&self, plan: &Plan) -> Result<Uuid, OrchestratorError> {
        let spec = plan.to_spec();
        // The engine's validator, not ours (OR-03: validated before any
        // state is created).
        agentos_workflow::validate(&spec)?;
        let run_id = self.store.create_run(&spec, &plan.goal)?;
        apply_priorities(&self.store, &run_id, plan)?;
        tracing::info!(run_id = %run_id, nodes = spec.nodes.len(),
            "orchestrator plan committed to the workflow store");
        Ok(run_id)
    }

    fn add_node(
        &self,
        run_id: &Uuid,
        node: &NodeSpec,
        priority: Option<Priority>,
    ) -> Result<Uuid, OrchestratorError> {
        add_node(&self.store, run_id, node, priority)
    }

    fn run_view(&self, run_id: &Uuid) -> Result<RunView, OrchestratorError> {
        run_view(&self.store, run_id)
    }

    fn escalate(&self, run_id: &Uuid, node_id: Option<&str>) -> Result<usize, OrchestratorError> {
        escalate(&self.store, run_id, node_id)
    }

    fn retarget(
        &self,
        run_id: &Uuid,
        node_id: Option<&str>,
        pool: &str,
    ) -> Result<usize, OrchestratorError> {
        retarget(&self.store, run_id, node_id, pool)
    }
}

/// The full engine is itself a sink: commits go through
/// [`WorkflowEngine::start_run`], so the orchestrator uses exactly the same
/// entry point any other embedder does.
impl PlanSink for WorkflowEngine {
    fn commit(&self, plan: &Plan) -> Result<Uuid, OrchestratorError> {
        let spec = plan.to_spec();
        let run_id = self.start_run(&spec, &plan.goal)?;
        apply_priorities(self.store(), &run_id, plan)?;
        Ok(run_id)
    }

    fn add_node(
        &self,
        run_id: &Uuid,
        node: &NodeSpec,
        priority: Option<Priority>,
    ) -> Result<Uuid, OrchestratorError> {
        let task_id = self.add_task(run_id, node)?;
        if let Some(priority) = priority {
            self.store().set_priority(&task_id, priority)?;
        }
        Ok(task_id)
    }

    fn run_view(&self, run_id: &Uuid) -> Result<RunView, OrchestratorError> {
        run_view(self.store(), run_id)
    }

    fn escalate(&self, run_id: &Uuid, node_id: Option<&str>) -> Result<usize, OrchestratorError> {
        escalate(self.store(), run_id, node_id)
    }

    fn retarget(
        &self,
        run_id: &Uuid,
        node_id: Option<&str>,
        pool: &str,
    ) -> Result<usize, OrchestratorError> {
        retarget(self.store(), run_id, node_id, pool)
    }
}

// ---------------------------------------------------------------------------
// Shared implementations (Arc<TaskStore> and &TaskStore flavours)
// ---------------------------------------------------------------------------

/// Push the plan's per-node priorities onto the materialized tasks.
///
/// `TaskStore::create_run` stamps every task `P2` (NodeSpec carries no
/// priority field, F-06 §7 note 8). This is the documented follow-up the
/// workflow crate left for OR-01.
fn apply_priorities(
    store: &TaskStore,
    run_id: &Uuid,
    plan: &Plan,
) -> Result<(), OrchestratorError> {
    let wanted: Vec<(&str, Priority)> = plan
        .nodes()
        .iter()
        .filter_map(|node| {
            node.priority
                .map(|priority| (node.spec.id.as_str(), priority))
        })
        .collect();
    if wanted.is_empty() {
        return Ok(());
    }
    let tasks = store.tasks_for_run(run_id)?;
    for (node_id, priority) in wanted {
        if let Some(task) = tasks.iter().find(|task| task.node_id == node_id) {
            store.set_priority(&task.id, priority)?;
        }
    }
    Ok(())
}

/// Append one node to a live run through the store's transactional
/// validate-then-insert, then stamp its priority.
fn add_node(
    store: &TaskStore,
    run_id: &Uuid,
    node: &NodeSpec,
    priority: Option<Priority>,
) -> Result<Uuid, OrchestratorError> {
    let task_id = store.add_task(run_id, node)?;
    if let Some(priority) = priority {
        store.set_priority(&task_id, priority)?;
    }
    tracing::info!(run_id = %run_id, node = %node.id, task_id = %task_id,
        "node appended to a live run");
    Ok(task_id)
}

/// Project the engine's durable tasks into a [`RunView`].
fn run_view(store: &TaskStore, run_id: &Uuid) -> Result<RunView, OrchestratorError> {
    let tasks = store.tasks_for_run(run_id)?;
    let status = RunStatus::from_tasks(&tasks);
    Ok(RunView {
        run_id: *run_id,
        status,
        tasks: tasks
            .into_iter()
            .map(|task| TaskView {
                task_id: task.id,
                node_id: task.node_id,
                state: task.state,
                priority: task.priority,
                attempt_count: task.attempt_count,
                pool: task.node.agent_role,
            })
            .collect(),
    })
}

/// Refuse a node-scoped escalation the engine cannot act on.
///
/// Escalation's entire engine-side surface is "retune the priority of the
/// next attempt" and "route the next attempt at another pool". A terminal
/// task has no next attempt, so both levers are inert against it — and a
/// sink that answers `Ok(0)` (or worse, `Ok(1)` for the priority write that
/// nothing will ever read) tells the planner its escalation landed.
///
/// It did not, and the planner cannot tell: the observed Phase-8 repair
/// spent three model cycles walking `supervisor` -> `stronger_agent` ->
/// `human` against a terminally `Failed` `T07`, each answered `accepted: 1`,
/// while the task and its five `Blocked` dependents never moved. Refusing
/// here turns those wasted cycles into one machine-readable rejection that
/// the next prompt carries (see [`RejectionReason::TaskTerminal`]).
///
/// Run-wide escalations (`node_id == None`) are unaffected: they are a
/// blanket "hurry up" over whatever is still live, and skipping finished
/// tasks there is correct, not a silent no-op.
fn refuse_unescalatable(
    store: &TaskStore,
    run_id: &Uuid,
    node_id: Option<&str>,
    op: &str,
) -> Result<(), OrchestratorError> {
    let Some(wanted) = node_id else {
        return Ok(());
    };
    let tasks = store.tasks_for_run(run_id)?;
    let Some(task) = tasks.iter().find(|task| task.node_id == wanted) else {
        return Err(RejectionReason::TaskNotInRun {
            op: op.to_owned(),
            node: wanted.to_owned(),
            run_id: run_id.to_string(),
        }
        .into());
    };
    if task.state.is_terminal() {
        tracing::warn!(run_id = %run_id, node = %wanted, state = %task.state, op,
            "refused an escalation against a terminal task; it has no next attempt to route");
        return Err(RejectionReason::TaskTerminal {
            op: op.to_owned(),
            node: wanted.to_owned(),
            state: task.state.as_str().to_owned(),
        }
        .into());
    }
    Ok(())
}

/// Raise the escalated task(s) to `P0` through the engine's own API.
fn escalate(
    store: &TaskStore,
    run_id: &Uuid,
    node_id: Option<&str>,
) -> Result<usize, OrchestratorError> {
    refuse_unescalatable(store, run_id, node_id, "escalate")?;
    let tasks = store.tasks_for_run(run_id)?;
    let mut retuned = 0;
    for task in tasks {
        if let Some(wanted) = node_id {
            if task.node_id != wanted {
                continue;
            }
        } else if task.state.is_terminal() {
            // A run-wide escalation is a blanket "hurry up" over whatever
            // is still live. Skipping terminal tasks is correct here, and
            // includes `Failed`: raising the priority of a task that will
            // never be leased again is bookkeeping nobody reads.
            continue;
        }
        if task.priority == Priority::P0 {
            continue;
        }
        store.set_priority(&task.id, Priority::P0)?;
        retuned += 1;
    }
    tracing::info!(run_id = %run_id, node = node_id.unwrap_or("<run>"), retuned,
        "escalation raised task priority to P0");
    Ok(retuned)
}

/// Retarget queued/parked tasks at `pool` through the engine's guarded
/// `set_agent_role`; tasks it refuses (executing, terminal) are skipped and
/// simply not counted.
fn retarget(
    store: &TaskStore,
    run_id: &Uuid,
    node_id: Option<&str>,
    pool: &str,
) -> Result<usize, OrchestratorError> {
    refuse_unescalatable(store, run_id, node_id, "escalate")?;
    let mut moved = 0;
    for task in store.tasks_for_run(run_id)? {
        if let Some(wanted) = node_id {
            if task.node_id != wanted {
                continue;
            }
        }
        if task.node.agent_role.as_deref() == Some(pool) {
            continue;
        }
        if store.set_agent_role(&task.id, Some(pool))?.is_some() {
            moved += 1;
        }
    }
    tracing::info!(run_id = %run_id, node = node_id.unwrap_or("<run>"), pool, moved,
        "escalation retargeted tasks at another pool");
    Ok(moved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{CreateTask, PlanOperation};
    use crate::plan::PlanPolicy;
    use agentos_workflow::NodeType;

    fn committed_plan() -> (WorkflowSink, Plan, Uuid) {
        let store = Arc::new(TaskStore::open_in_memory().unwrap());
        let sink = WorkflowSink::new(store);
        let mut plan = Plan::new("ship it", PlanPolicy::default());
        plan.apply(&PlanOperation::CreateTask(CreateTask {
            node_id: "spec".to_owned(),
            node_type: NodeType::Run,
            depends_on: vec![],
            pool: Some("backend".to_owned()),
            objective: None,
            priority: Some(Priority::P1),
            budgets: None,
            retry: None,
        }))
        .unwrap();
        plan.apply(&PlanOperation::CreateTask(CreateTask {
            node_id: "build".to_owned(),
            node_type: NodeType::Run,
            depends_on: vec!["spec".to_owned()],
            pool: Some("backend".to_owned()),
            objective: None,
            priority: None,
            budgets: None,
            retry: None,
        }))
        .unwrap();
        let run_id = sink.commit(&plan).unwrap();
        plan.mark_committed(run_id);
        (sink, plan, run_id)
    }

    #[test]
    fn commit_materializes_the_plan_and_applies_priorities() {
        let (sink, _plan, run_id) = committed_plan();
        let view = sink.run_view(&run_id).unwrap();
        assert_eq!(view.status, RunStatus::Running);
        assert_eq!(view.tasks.len(), 2);

        let spec = view.tasks.iter().find(|t| t.node_id == "spec").unwrap();
        assert_eq!(spec.priority, Priority::P1, "explicit priority applied");
        assert_eq!(spec.state, TaskState::Ready, "no dependencies -> ready");
        assert_eq!(spec.pool.as_deref(), Some("backend"));

        let build = view.tasks.iter().find(|t| t.node_id == "build").unwrap();
        assert_eq!(build.priority, Priority::P2, "store default preserved");
        assert_eq!(build.state, TaskState::Planned, "gated on its dependency");
    }

    #[test]
    fn commit_refuses_an_invalid_plan_through_the_engines_validator() {
        let store = Arc::new(TaskStore::open_in_memory().unwrap());
        let sink = WorkflowSink::new(store);
        // An empty plan never reaches the store: the engine's validator
        // rejects it first (OR-03).
        let plan = Plan::new("nothing", PlanPolicy::default());
        let err = sink.commit(&plan).unwrap_err();
        assert!(matches!(err, OrchestratorError::Workflow(_)), "{err:?}");
        assert_eq!(
            sink.store().runs().unwrap().len(),
            0,
            "no durable side effects"
        );
    }

    #[test]
    fn escalate_raises_one_node_or_the_whole_run_to_p0() {
        let (sink, _plan, run_id) = committed_plan();

        assert_eq!(sink.escalate(&run_id, Some("build")).unwrap(), 1);
        let view = sink.run_view(&run_id).unwrap();
        assert_eq!(
            view.tasks
                .iter()
                .find(|t| t.node_id == "build")
                .unwrap()
                .priority,
            Priority::P0
        );
        // Idempotent: already P0.
        assert_eq!(sink.escalate(&run_id, Some("build")).unwrap(), 0);
        // A node the run carries no task for is refused, not answered
        // `Ok(0)`: "nothing happened" and "nothing could ever happen" are
        // different answers and the planner acts on them differently.
        let err = sink.escalate(&run_id, Some("ghost")).unwrap_err();
        assert!(
            matches!(
                err,
                OrchestratorError::Rejected(RejectionReason::TaskNotInRun { .. })
            ),
            "{err:?}"
        );
        // Run-wide.
        assert_eq!(sink.escalate(&run_id, None).unwrap(), 1);
    }

    #[test]
    fn escalating_a_terminal_task_is_rejected_rather_than_reported_as_accepted() {
        let (sink, _plan, run_id) = committed_plan();
        let spec = sink
            .store()
            .tasks_for_run(&run_id)
            .unwrap()
            .into_iter()
            .find(|task| task.node_id == "spec")
            .unwrap();
        // Drive `spec` to the terminal failure the observed quota outage
        // produced: Ready -> Failed, no attempts left, no next attempt.
        sink.store()
            .cas_transition(&spec.id, TaskState::Ready, TaskState::Failed)
            .unwrap();

        for error in [
            sink.escalate(&run_id, Some("spec")).unwrap_err(),
            sink.retarget(&run_id, Some("spec"), "reviewers")
                .unwrap_err(),
        ] {
            match error {
                OrchestratorError::Rejected(RejectionReason::TaskTerminal {
                    ref node,
                    ref state,
                    ..
                }) => {
                    assert_eq!(node, "spec");
                    assert_eq!(state, "failed");
                    assert!(error.to_string().contains("reopen the run"), "{error}");
                }
                other => panic!("expected a task_terminal rejection, got {other:?}"),
            }
        }

        // The refusal is a read-only verdict: nothing was retuned or routed.
        let view = sink.run_view(&run_id).unwrap();
        let spec_view = view.tasks.iter().find(|t| t.node_id == "spec").unwrap();
        assert_eq!(spec_view.priority, Priority::P1, "priority untouched");
        assert_eq!(spec_view.pool.as_deref(), Some("backend"), "role untouched");

        // A run-wide escalation still works: it is a blanket "hurry up"
        // over whatever is live, not a claim about one dead node.
        assert_eq!(sink.escalate(&run_id, None).unwrap(), 1);
    }
}
