//! End-to-end: a scripted orchestrator model drives a plan through the real
//! F-06 [`WorkflowEngine`] to a completed run.
//!
//! Every test here uses the durable SQLite store on disk and the engine's
//! own scheduler; only two things are doubles:
//!
//! - the planning model ([`ScriptedPlanningModel`]) — **no billable calls
//!   anywhere in this suite**, mirroring the F-03 fixture-test rule;
//! - the task executor (a counting mock) — F-06's provider-free
//!   [`TaskExecutor`] boundary, exactly as the workflow crate's own engine
//!   tests use it.
//!
//! The properties under test are PRD §9 OR-01's two acceptance criteria:
//! invalid commands are rejected with machine-readable reasons, and a run
//! continues deterministically when the orchestrator is unavailable.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agentos_core::{Priority, TaskState};
use agentos_orchestrator::{Orchestrator, PlanPolicy, PlanSink, RunView, ScriptedPlanningModel};
use agentos_workflow::{
    Outcome, RunStatus, TaskContract, TaskExecutor, TaskRecord, TaskStore, WorkflowEngine,
};
use async_trait::async_trait;
use serde_json::json;

/// A provider-free executor that records the order nodes ran in.
struct RecordingExecutor {
    order: Mutex<Vec<String>>,
    runs: AtomicUsize,
}

impl RecordingExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            order: Mutex::new(Vec::new()),
            runs: AtomicUsize::new(0),
        })
    }

    fn order(&self) -> Vec<String> {
        self.order.lock().expect("order mutex").clone()
    }

    fn runs(&self) -> usize {
        self.runs.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl TaskExecutor for RecordingExecutor {
    async fn run(&self, task: &TaskRecord, _contract: &TaskContract) -> Outcome {
        self.order
            .lock()
            .expect("order mutex")
            .push(task.node_id.clone());
        self.runs.fetch_add(1, Ordering::Relaxed);
        Outcome::Success {
            packet: json!({ "node": task.node_id, "filesChanged": [] }),
        }
    }
}

/// Store on disk (durability is part of the contract), engine over it.
fn engine(executor: Arc<RecordingExecutor>) -> (tempfile::TempDir, Arc<WorkflowEngine>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(TaskStore::open(&dir.path().join("workflow.db")).expect("store"));
    let engine = Arc::new(WorkflowEngine::new(store, executor));
    (dir, engine)
}

const FEATURE_PLAN: &str = r#"Here is the plan.

```json
[
  {"op":"create_task","nodeId":"spec","pool":"backend",
   "objective":"write the API contract","priority":"p1"},
  {"op":"create_task","nodeId":"impl-api","dependsOn":["spec"],"pool":"backend"},
  {"op":"create_task","nodeId":"impl-ui","dependsOn":["spec"],"pool":"frontend"},
  {"op":"request_review","nodeId":"impl-api"},
  {"op":"add_dependency","nodeId":"review-impl-api","dependsOn":"impl-ui"}
]
```
"#;

#[tokio::test]
async fn a_planned_goal_runs_to_completion_through_the_workflow_engine() {
    let executor = RecordingExecutor::new();
    let (_dir, engine) = engine(Arc::clone(&executor));

    let model = Arc::new(ScriptedPlanningModel::new([
        FEATURE_PLAN,
        "[]", // nothing more to propose
        r#"[{"op":"close_goal","summary":"feature shipped"}]"#,
    ]));
    let mut orchestrator = Orchestrator::new(
        "ship the feature",
        PlanPolicy::default(),
        model,
        Arc::clone(&engine) as Arc<dyn PlanSink>,
    );

    // --- planning phase (proposals only; nothing durable yet) -----------
    let reports = orchestrator.run_planning().await;
    assert_eq!(reports.len(), 2, "planning stops when nothing is proposed");
    assert!(reports[0].rejected.is_empty(), "{:?}", reports[0].rejected);
    assert_eq!(reports[0].accepted.len(), 5);
    assert_eq!(
        orchestrator.plan().node_ids(),
        vec!["spec", "impl-api", "impl-ui", "review-impl-api"]
    );
    assert!(
        engine.store().runs().unwrap().is_empty(),
        "commit is user-gated"
    );

    // --- the user gate: commit into the engine --------------------------
    let run_id = orchestrator.commit().expect("engine accepts the plan");
    let view: RunView = engine.run_view(&run_id).unwrap();
    assert_eq!(view.tasks.len(), 4);
    assert_eq!(view.status, RunStatus::Running);
    // Priority from `create_task` was applied through the engine's own API.
    assert_eq!(
        view.tasks
            .iter()
            .find(|t| t.node_id == "spec")
            .unwrap()
            .priority,
        Priority::P1
    );
    // Routing survived into the durable node spec.
    assert_eq!(
        view.tasks
            .iter()
            .find(|t| t.node_id == "impl-ui")
            .unwrap()
            .pool
            .as_deref(),
        Some("frontend")
    );

    // --- the engine drives the run on its own ---------------------------
    let driver = engine
        .run_until_idle(50)
        .await
        .expect("engine drives the run");
    assert_eq!(driver.succeeded.len(), 4);
    assert!(driver.failed.is_empty());
    assert_eq!(executor.runs(), 4);

    let order = executor.order();
    let position = |node: &str| order.iter().position(|id| id == node).expect(node);
    assert!(position("spec") < position("impl-api"), "{order:?}");
    assert!(position("spec") < position("impl-ui"), "{order:?}");
    // The review gate the orchestrator inserted ran after BOTH of its
    // dependencies — the added edge took effect in the engine.
    assert!(
        position("impl-api") < position("review-impl-api"),
        "{order:?}"
    );
    assert!(
        position("impl-ui") < position("review-impl-api"),
        "{order:?}"
    );

    let final_view = engine.run_view(&run_id).unwrap();
    assert_eq!(final_view.status, RunStatus::Completed);
    assert!(final_view.tasks.iter().all(|t| t.state == TaskState::Done));

    // --- supervising phase: close the goal ------------------------------
    let report = orchestrator.cycle().await;
    assert!(report.rejected.is_empty(), "{:?}", report.rejected);
    assert_eq!(report.accepted.len(), 1);
    assert!(orchestrator.plan().is_closed());
    assert_eq!(
        orchestrator.plan().closing_summary(),
        Some("feature shipped")
    );
}

#[tokio::test]
async fn the_run_completes_with_no_orchestrator_attached_at_all() {
    // PRD §9 OR-01 acceptance criterion 2: "a run can continue
    // deterministically if orchestrator is temporarily unavailable for
    // already-planned tasks."
    let executor = RecordingExecutor::new();
    let (_dir, engine) = engine(Arc::clone(&executor));

    let run_id = {
        let model = Arc::new(ScriptedPlanningModel::new([FEATURE_PLAN]));
        let mut orchestrator = Orchestrator::new(
            "ship the feature",
            PlanPolicy::default(),
            model,
            Arc::clone(&engine) as Arc<dyn PlanSink>,
        );
        orchestrator.cycle().await;
        let run_id = orchestrator.commit().unwrap();
        // The orchestrator (and its model) go away entirely here.
        drop(orchestrator);
        run_id
    };

    let driver = engine.run_until_idle(50).await.unwrap();
    assert_eq!(driver.succeeded.len(), 4);
    assert_eq!(
        engine.run_view(&run_id).unwrap().status,
        RunStatus::Completed
    );
}

#[tokio::test]
async fn a_model_outage_mid_supervision_does_not_stall_the_run() {
    let executor = RecordingExecutor::new();
    let (_dir, engine) = engine(Arc::clone(&executor));

    let model = Arc::new(ScriptedPlanningModel::with_failures(vec![
        Ok(FEATURE_PLAN.to_owned()),
        Err("claude: spawn failed (ENOENT)".to_owned()),
    ]));
    let mut orchestrator = Orchestrator::new(
        "ship the feature",
        PlanPolicy::default(),
        model,
        Arc::clone(&engine) as Arc<dyn PlanSink>,
    );
    orchestrator.cycle().await;
    let run_id = orchestrator.commit().unwrap();

    // The proposer is down: reported, never an error, never a stall.
    let report = orchestrator.cycle().await;
    assert!(report.model_error.is_some());
    assert!(report.accepted.is_empty() && report.rejected.is_empty());

    let driver = engine.run_until_idle(50).await.unwrap();
    assert_eq!(driver.succeeded.len(), 4);
    assert_eq!(
        engine.run_view(&run_id).unwrap().status,
        RunStatus::Completed
    );
}

#[tokio::test]
async fn the_engine_vetoes_closing_a_goal_whose_run_is_still_working() {
    let executor = RecordingExecutor::new();
    let (_dir, engine) = engine(Arc::clone(&executor));

    let model = Arc::new(ScriptedPlanningModel::new([
        FEATURE_PLAN,
        r#"[{"op":"close_goal","summary":"done (it is not)"}]"#,
    ]));
    let mut orchestrator = Orchestrator::new(
        "ship the feature",
        PlanPolicy::default(),
        model,
        Arc::clone(&engine) as Arc<dyn PlanSink>,
    );
    orchestrator.cycle().await;
    orchestrator.commit().unwrap();

    // Engine says "running"; the model's close_goal is refused with a
    // machine-readable reason.
    let report = orchestrator.cycle().await;
    assert!(report.accepted.is_empty());
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(report.rejected[0].reason.code(), "run_not_terminal");
    assert!(!orchestrator.plan().is_closed());

    // Once the engine finishes, the very same proposal is accepted.
    engine.run_until_idle(50).await.unwrap();
    let report = orchestrator.cycle().await;
    assert!(report.rejected.is_empty(), "{:?}", report.rejected);
    assert!(orchestrator.plan().is_closed());
}

#[tokio::test]
async fn a_rejected_cycle_feeds_the_correction_into_the_next_proposal() {
    let executor = RecordingExecutor::new();
    let (_dir, engine) = engine(Arc::clone(&executor));

    // Cycle 1 proposes a cycle in the DAG plus a bogus pool; cycle 2 fixes
    // both — the self-correction path.
    let model = Arc::new(ScriptedPlanningModel::new([
        r#"[
            {"op":"create_task","nodeId":"a","pool":"backend"},
            {"op":"create_task","nodeId":"b","dependsOn":["a"],"pool":"quantum"},
            {"op":"add_dependency","nodeId":"a","dependsOn":"b"}
        ]"#,
        r#"[
            {"op":"create_task","nodeId":"b","dependsOn":["a"],"pool":"backend"},
            {"op":"request_review","nodeId":"b"}
        ]"#,
        "[]",
    ]));
    let mut orchestrator = Orchestrator::new(
        "ship the feature",
        PlanPolicy::default(),
        model,
        Arc::clone(&engine) as Arc<dyn PlanSink>,
    );

    let first = orchestrator.cycle().await;
    assert_eq!(first.accepted.len(), 1, "only `a` survived");
    let codes: Vec<&str> = first
        .rejected
        .iter()
        .map(|rejection| rejection.reason.code())
        .collect();
    assert_eq!(codes, vec!["unknown_pool", "unknown_node"]);
    // `b` never existed, so the edge referenced an unknown node; the plan
    // is still a valid DAG.
    orchestrator.plan().validate().unwrap();

    // The corrections travel into the next prompt.
    let prompt = orchestrator.snapshot().render_prompt();
    assert!(prompt.contains("unknown_pool"), "{prompt}");
    assert!(prompt.contains("configured pools"), "{prompt}");

    let second = orchestrator.cycle().await;
    assert!(second.rejected.is_empty(), "{:?}", second.rejected);
    assert_eq!(orchestrator.plan().node_ids(), vec!["a", "b", "review-b"]);

    let run_id = orchestrator.commit().unwrap();
    engine.run_until_idle(50).await.unwrap();
    assert_eq!(
        engine.run_view(&run_id).unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(executor.runs(), 3);
}

#[tokio::test]
async fn an_escalation_on_a_live_run_retunes_priority_through_the_engine() {
    let executor = RecordingExecutor::new();
    let (_dir, engine) = engine(Arc::clone(&executor));

    let model = Arc::new(ScriptedPlanningModel::new([
        FEATURE_PLAN,
        r#"[{"op":"escalate","nodeId":"impl-ui","target":"stronger_agent",
             "reason":"the UI contract is ambiguous"}]"#,
    ]));
    let mut orchestrator = Orchestrator::new(
        "ship the feature",
        PlanPolicy::default(),
        model,
        Arc::clone(&engine) as Arc<dyn PlanSink>,
    );
    orchestrator.cycle().await;
    let run_id = orchestrator.commit().unwrap();

    let report = orchestrator.cycle().await;
    assert!(report.rejected.is_empty(), "{:?}", report.rejected);
    assert!(report.engine_error.is_none());
    assert_eq!(orchestrator.plan().escalations().len(), 1);

    // The engine's durable row carries the new priority.
    let view = engine.run_view(&run_id).unwrap();
    let ui = view.tasks.iter().find(|t| t.node_id == "impl-ui").unwrap();
    assert_eq!(ui.priority, Priority::P0);
    // Escalation is a scheduling hint, not a lifecycle jump: the task is
    // still gated on its dependency.
    assert_eq!(ui.state, TaskState::Planned);
}

#[tokio::test]
async fn a_committed_plan_survives_a_restart_of_everything() {
    // The orchestrator holds no durable state of its own; the engine's
    // SQLite store is the single source of truth (PRD OR-03 "durable task
    // records so application restarts do not lose run state").
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("workflow.db");

    let run_id = {
        let executor = RecordingExecutor::new();
        let store = Arc::new(TaskStore::open(&db).unwrap());
        let engine = Arc::new(WorkflowEngine::new(store, executor));
        let model = Arc::new(ScriptedPlanningModel::new([FEATURE_PLAN]));
        let mut orchestrator = Orchestrator::new(
            "ship the feature",
            PlanPolicy::default(),
            model,
            Arc::clone(&engine) as Arc<dyn PlanSink>,
        );
        orchestrator.cycle().await;
        orchestrator.commit().unwrap()
        // engine, store, orchestrator and model all dropped here
    };

    let executor = RecordingExecutor::new();
    let store = Arc::new(TaskStore::open(&db).unwrap());
    let engine = Arc::new(WorkflowEngine::new(
        store,
        Arc::clone(&executor) as Arc<dyn TaskExecutor>,
    ));
    let driver = engine.run_until_idle(50).await.unwrap();
    assert_eq!(driver.succeeded.len(), 4);
    assert_eq!(
        engine.run_view(&run_id).unwrap().status,
        RunStatus::Completed
    );
}
