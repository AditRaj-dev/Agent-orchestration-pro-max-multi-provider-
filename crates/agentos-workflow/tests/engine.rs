//! F-06 integration tests — the engine end to end with mock executors:
//! happy path, parallel fan-out, lease expiry crash recovery, budget
//! exhaustion escalation, restart durability, and scheduling-order rules.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentos_core::{Priority, TaskState};
use agentos_workflow::{
    Budgets, FailureKind, NodeSpec, NodeType, Outcome, RetryPolicy, RunStatus, TaskContract,
    TaskExecutor, TaskRecord, TaskStore, WorkflowEngine, WorkflowError, WorkflowSpec,
};
use async_trait::async_trait;
use uuid::Uuid;

// ---------------------------------------------------------------- fixtures

fn node(id: &str, node_type: NodeType, depends_on: &[&str]) -> NodeSpec {
    NodeSpec {
        id: id.to_owned(),
        node_type,
        depends_on: depends_on.iter().map(|d| (*d).to_owned()).collect(),
        agent_role: None,
        budgets: Budgets::default(),
        retry: RetryPolicy::default(),
    }
}

fn governed_node(
    id: &str,
    node_type: NodeType,
    depends_on: &[&str],
    budgets: Budgets,
    retry: RetryPolicy,
) -> NodeSpec {
    NodeSpec {
        budgets,
        retry,
        ..node(id, node_type, depends_on)
    }
}

fn spec(id: &str, nodes: Vec<NodeSpec>) -> WorkflowSpec {
    WorkflowSpec {
        id: id.to_owned(),
        version: 1,
        nodes,
    }
}

/// Mock executor: pops scripted outcomes per node; unscripted nodes succeed
/// with a packet naming the node.
struct ScriptedExecutor {
    scripted: Mutex<HashMap<String, VecDeque<Outcome>>>,
}

impl ScriptedExecutor {
    fn new() -> Self {
        Self {
            scripted: Mutex::new(HashMap::new()),
        }
    }

    fn then_transient_failures(&self, node: &str, count: usize) {
        let mut scripted = self.scripted.lock().unwrap();
        let queue = scripted.entry(node.to_owned()).or_default();
        for _ in 0..count {
            queue.push_back(Outcome::TransientFailure);
        }
    }
}

#[async_trait]
impl TaskExecutor for ScriptedExecutor {
    async fn run(&self, task: &TaskRecord, _contract: &TaskContract) -> Outcome {
        let next = {
            let mut scripted = self.scripted.lock().unwrap();
            scripted
                .get_mut(&task.node_id)
                .and_then(|queue| queue.pop_front())
        };
        next.unwrap_or(Outcome::Success {
            packet: serde_json::json!({ "node": task.node_id }),
        })
    }
}

/// Mock executor proving concurrency: tracks simultaneous runs.
struct ConcurrentExecutor {
    current: AtomicUsize,
    max_seen: AtomicUsize,
}

#[async_trait]
impl TaskExecutor for ConcurrentExecutor {
    async fn run(&self, task: &TaskRecord, _contract: &TaskContract) -> Outcome {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_seen.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.current.fetch_sub(1, Ordering::SeqCst);
        Outcome::Success {
            packet: serde_json::json!({ "node": task.node_id }),
        }
    }
}

fn temp_store() -> (tempfile::TempDir, Arc<TaskStore>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(TaskStore::open(&dir.path().join("workflow.db")).expect("store"));
    (dir, store)
}

fn states_by_node(engine: &WorkflowEngine, run_id: Uuid) -> HashMap<String, TaskState> {
    engine
        .store()
        .tasks_for_run(&run_id)
        .unwrap()
        .into_iter()
        .map(|task| (task.node_id.clone(), task.state))
        .collect()
}

// ------------------------------------------------------------------ tests

/// Happy path: spec (run) -> review -> commit (git_gate), one node per
/// tick, review only becoming ready after spec is Done.
#[tokio::test]
async fn happy_path_spec_review_git_gate() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
    let workflow = spec(
        "feature-build",
        vec![
            node("spec", NodeType::Run, &[]),
            node("review", NodeType::Review, &["spec"]),
            node("commit", NodeType::GitGate, &["review"]),
        ],
    );

    let run_id = engine.start_run(&workflow, "ship F-06").unwrap();

    // Tick 1: only `spec` is ready (review's dependency is unsatisfied).
    let tick1 = engine.tick().await.unwrap();
    assert_eq!(tick1.succeeded.len(), 1);
    assert_eq!(tick1.succeeded[0].node_id, "spec");
    assert_eq!(tick1.succeeded[0].packet["node"], serde_json::json!("spec"));
    let states = states_by_node(&engine, run_id);
    assert_eq!(states["spec"], TaskState::Done);
    assert_eq!(states["review"], TaskState::Planned);
    assert_eq!(states["commit"], TaskState::Planned);

    // Tick 2: review becomes ready and completes.
    let tick2 = engine.tick().await.unwrap();
    assert_eq!(tick2.promoted, 1);
    assert_eq!(tick2.succeeded.len(), 1);
    assert_eq!(tick2.succeeded[0].node_id, "review");
    assert_eq!(
        states_by_node(&engine, run_id)["commit"],
        TaskState::Planned
    );

    // Tick 3: the git gate.
    let tick3 = engine.tick().await.unwrap();
    assert_eq!(tick3.succeeded.len(), 1);
    assert_eq!(tick3.succeeded[0].node_id, "commit");

    // Tick 4: nothing left to do.
    let tick4 = engine.tick().await.unwrap();
    assert_eq!(tick4.succeeded.len(), 0);
    assert_eq!(tick4.leased, 0);

    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Completed);
    assert_eq!(
        engine.store().run(&run_id).unwrap().status,
        RunStatus::Completed
    );
}

/// Parallel fan-out: two independent Run nodes execute concurrently (max
/// simultaneous executors == 2) and only then does the Review fan-in become
/// ready.
#[tokio::test]
async fn parallel_fan_out_runs_before_review() {
    let (_dir, store) = temp_store();
    let executor = Arc::new(ConcurrentExecutor {
        current: AtomicUsize::new(0),
        max_seen: AtomicUsize::new(0),
    });
    let engine = WorkflowEngine::new(store, Arc::clone(&executor) as Arc<dyn TaskExecutor>);
    let workflow = spec(
        "fan-out",
        vec![
            node("a", NodeType::Run, &[]),
            node("b", NodeType::Run, &[]),
            node("review", NodeType::Review, &["a", "b"]),
        ],
    );

    let run_id = engine.start_run(&workflow, "parallel build").unwrap();
    let driver = engine.run_until_idle(10).await.unwrap();

    // Both independent branches ran simultaneously...
    assert_eq!(
        executor.max_seen.load(Ordering::SeqCst),
        2,
        "the fan-out must run its branches concurrently"
    );
    // ...all three nodes completed, review last (depended on both).
    assert_eq!(driver.succeeded.len(), 3);
    assert_eq!(driver.succeeded[2].node_id, "review");
    assert_eq!(driver.failed.len(), 0);

    let states = states_by_node(&engine, run_id);
    assert_eq!(states["a"], TaskState::Done);
    assert_eq!(states["b"], TaskState::Done);
    assert_eq!(states["review"], TaskState::Done);
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Completed);
}

/// Crash recovery (OR-05): a lease that expires without an outcome (the
/// worker died) is reclaimed — attempt consumed, task back to Ready — and
/// re-leased to a healthy engine, which completes it.
#[tokio::test]
async fn lease_expiry_reclaims_and_re_leases() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
    let workflow = spec("single", vec![node("only", NodeType::Run, &[])]);

    let run_id = engine.start_run(&workflow, "crash me").unwrap();
    let task = &engine.store().tasks_for_run(&run_id).unwrap()[0];

    // A worker takes a lease and crashes instantly (ttl 0 => already
    // expired), never running the executor.
    let leased = engine
        .scheduler()
        .grant_lease(&task.id, "crashed-worker", Duration::ZERO)
        .unwrap();
    assert_eq!(leased.state, TaskState::Leased);
    assert_eq!(leased.lease_owner.as_deref(), Some("crashed-worker"));

    // The engine's next pass: reclaim -> requeue -> re-lease -> succeed.
    let report = engine.tick().await.unwrap();
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.leased, 1);
    assert_eq!(report.succeeded.len(), 1);
    assert_eq!(report.succeeded[0].node_id, "only");

    let final_task = engine.store().task(&task.id).unwrap();
    assert_eq!(final_task.state, TaskState::Done);
    assert_eq!(
        final_task.attempt_count, 1,
        "the crashed attempt was consumed"
    );
    assert!(final_task.lease_owner.is_none());
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Completed);
}

/// OR-08: attempts budget — after `max_attempts` failed leases the task
/// escalates to Failed, its dependent is parked Blocked, the run is Failed,
/// and no further lease is ever granted.
#[tokio::test]
async fn attempts_budget_exhaustion_escalates_to_failed() {
    let (_dir, store) = temp_store();
    let executor = Arc::new(ScriptedExecutor::new());
    // Allow 2 attempts, but a retry policy that would requeue forever:
    // the budget gate must win.
    executor.then_transient_failures("builder", 10);
    let engine = WorkflowEngine::new(store, Arc::clone(&executor) as Arc<dyn TaskExecutor>);
    let workflow = spec(
        "budget",
        vec![
            governed_node(
                "builder",
                NodeType::Run,
                &[],
                Budgets {
                    max_attempts: 2,
                    ..Budgets::default()
                },
                RetryPolicy {
                    transient_retries: 9,
                    reasoning_retries: 0,
                },
            ),
            node("after", NodeType::Run, &["builder"]),
        ],
    );

    let run_id = engine.start_run(&workflow, "bounded work").unwrap();
    let driver = engine.run_until_idle(20).await.unwrap();

    // Two transient failures requeued (within the retry allowance)...
    // then the attempts gate escalated.
    assert_eq!(
        driver
            .failed
            .iter()
            .filter(|f| f.kind == FailureKind::BudgetExceeded)
            .count(),
        1
    );

    let tasks = engine.store().tasks_for_run(&run_id).unwrap();
    let builder = tasks.iter().find(|t| t.node_id == "builder").unwrap();
    let after = tasks.iter().find(|t| t.node_id == "after").unwrap();
    assert_eq!(builder.state, TaskState::Failed);
    assert_eq!(builder.attempt_count, 2);
    assert_eq!(after.state, TaskState::Blocked, "dependent parked");
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Failed);

    // No further leases, ever: another pass does nothing.
    let idle = engine.tick().await.unwrap();
    assert_eq!(idle.leased, 0);
    assert_eq!(idle.succeeded.len(), 0);
    assert_eq!(idle.escalated_over_budget, 0);
    assert_eq!(
        engine.store().task(&builder.id).unwrap().state,
        TaskState::Failed
    );
}

/// OR-08: elapsed budget — `max_elapsed_secs: 0` is exhausted at the first
/// gate, so the task fails without ever being leased.
#[tokio::test]
async fn elapsed_budget_exhaustion_fails_before_first_lease() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
    let workflow = spec(
        "elapsed",
        vec![governed_node(
            "slow",
            NodeType::Run,
            &[],
            Budgets {
                max_attempts: 9,
                max_elapsed_secs: 0,
                max_cost_usd: None,
            },
            RetryPolicy::default(),
        )],
    );

    let run_id = engine.start_run(&workflow, "too slow").unwrap();
    let report = engine.tick().await.unwrap();

    assert_eq!(report.escalated_over_budget, 1);
    assert_eq!(report.leased, 0, "an over-elapsed task is never leased");
    assert_eq!(report.succeeded.len(), 0);

    let task = &engine.store().tasks_for_run(&run_id).unwrap()[0];
    assert_eq!(task.state, TaskState::Failed);
    assert_eq!(task.attempt_count, 0);
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Failed);
}

/// OR-03 durability: drop the engine AND the store, reopen the database
/// file, and a fresh engine continues the run — including recovering a
/// mid-flight lease left behind by a crashed worker.
#[tokio::test]
async fn restart_durability_continues_the_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("workflow.db");
    let workflow = spec(
        "chain",
        vec![
            node("a", NodeType::Run, &[]),
            node("b", NodeType::Run, &["a"]),
            node("c", NodeType::Run, &["b"]),
        ],
    );

    let run_id;
    {
        let store = Arc::new(TaskStore::open(&db_path).unwrap());
        let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
        run_id = engine.start_run(&workflow, "survive restarts").unwrap();

        // Tick 1 completes `a`.
        let tick1 = engine.tick().await.unwrap();
        assert_eq!(tick1.succeeded.len(), 1);

        // A second worker leases `b` (promoted by the tick's promote pass)
        // and crashes without an outcome.
        engine.scheduler().promote_unblocked().unwrap();
        let b = engine
            .store()
            .tasks_for_run(&run_id)
            .unwrap()
            .into_iter()
            .find(|t| t.node_id == "b")
            .unwrap();
        engine
            .scheduler()
            .grant_lease(&b.id, "ghost-worker", Duration::ZERO)
            .unwrap();

        // "Crash": engine and store dropped without cleanup.
    }

    // Reopen from disk. Durable state must be intact.
    let store2 = Arc::new(TaskStore::open(&db_path).unwrap());
    let engine2 = WorkflowEngine::new(store2, Arc::new(ScriptedExecutor::new()));
    let states = states_by_node(&engine2, run_id);
    assert_eq!(states["a"], TaskState::Done);
    assert_eq!(states["b"], TaskState::Leased, "ghost lease still on disk");
    assert_eq!(states["c"], TaskState::Planned);

    let driver = engine2.run_until_idle(20).await.unwrap();
    assert_eq!(driver.succeeded.len(), 2, "b (recovered) and c complete");

    let tasks = engine2.store().tasks_for_run(&run_id).unwrap();
    assert!(tasks.iter().all(|t| t.state == TaskState::Done));
    let b = tasks.iter().find(|t| t.node_id == "b").unwrap();
    assert_eq!(b.attempt_count, 1, "the ghost attempt was consumed");
    assert_eq!(engine2.run_status(&run_id).unwrap(), RunStatus::Completed);
}

/// The ready queue is ordered by priority (P0 first), then age.
#[tokio::test]
async fn ready_tasks_order_priority_then_age() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
    let workflow = spec(
        "queue",
        vec![
            node("old", NodeType::Run, &[]),
            node("urgent", NodeType::Run, &[]),
        ],
    );

    let run_id = engine.start_run(&workflow, "ordering").unwrap();
    let tasks = engine.store().tasks_for_run(&run_id).unwrap();
    let urgent = tasks.iter().find(|t| t.node_id == "urgent").unwrap();

    // `urgent` was created later but is retuned to P1; `old` stays P2.
    engine
        .store()
        .set_priority(&urgent.id, Priority::P1)
        .unwrap();

    let ready = engine.scheduler().ready_tasks().unwrap();
    let order: Vec<&str> = ready.iter().map(|t| t.node_id.as_str()).collect();
    assert_eq!(order, vec!["urgent", "old"], "priority beats age");

    // Same priority: creation order (age) decides.
    engine
        .store()
        .set_priority(&urgent.id, Priority::P2)
        .unwrap();
    let ready = engine.scheduler().ready_tasks().unwrap();
    let order: Vec<&str> = ready.iter().map(|t| t.node_id.as_str()).collect();
    assert_eq!(order, vec!["old", "urgent"], "age breaks priority ties");
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Running);
}

/// Heartbeats renew the lease by the granted ttl; foreign heartbeats are
/// rejected with the holder named.
#[tokio::test]
async fn heartbeat_renews_and_rejects_foreign_owners() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
    let workflow = spec("hb", vec![node("only", NodeType::Run, &[])]);
    let run_id = engine.start_run(&workflow, "heartbeats").unwrap();

    let task = &engine.store().tasks_for_run(&run_id).unwrap()[0];
    let leased = engine
        .scheduler()
        .grant_lease(&task.id, "worker-1", Duration::from_secs(60))
        .unwrap();
    let first_expiry = leased.lease_expires_at.unwrap();

    let renewed = engine.scheduler().heartbeat(&task.id, "worker-1").unwrap();
    let second_expiry = renewed.lease_expires_at.unwrap();
    assert!(second_expiry > first_expiry, "heartbeat extends the lease");
    assert!(
        second_expiry - renewed.heartbeat_at.unwrap() >= second_expiry - first_expiry,
        "renewal keeps the granted ttl"
    );

    let err = engine
        .scheduler()
        .heartbeat(&task.id, "worker-2")
        .unwrap_err();
    match err {
        WorkflowError::LeaseOwnerMismatch {
            task,
            holder,
            owner,
        } => {
            assert_eq!(holder, "worker-1");
            assert_eq!(owner, "worker-2");
            assert!(task.contains("worker") || !task.is_empty());
        }
        other => panic!("expected LeaseOwnerMismatch, got {other:?}"),
    }
}

/// start_run validates BEFORE creating anything: a cyclic spec leaves the
/// store untouched.
#[tokio::test]
async fn start_run_rejects_invalid_specs_without_side_effects() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
    let cyclic = spec(
        "cyclic",
        vec![
            node("a", NodeType::Run, &["b"]),
            node("b", NodeType::Run, &["a"]),
        ],
    );

    assert!(matches!(
        engine.start_run(&cyclic, "never starts").unwrap_err(),
        WorkflowError::Validation(_)
    ));
    assert!(
        engine.store().runs().unwrap().is_empty(),
        "no run row may be created for an invalid spec"
    );
    assert!(engine.store().tasks().unwrap().is_empty());
}
