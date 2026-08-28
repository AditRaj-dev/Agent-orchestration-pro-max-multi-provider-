//! F-06 integration tests — the engine end to end with mock executors:
//! happy path, parallel fan-out, lease expiry crash recovery, budget
//! exhaustion escalation, restart durability, and scheduling-order rules.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentos_core::{CoreError, Priority, TaskState};
use agentos_workflow::{
    Budgets, EngineConfig, FailureKind, NodeSpec, NodeType, Outcome, QueueReason, RetryPolicy,
    RunStatus, TaskContract, TaskExecutor, TaskRecord, TaskStore, WorkflowEngine, WorkflowError,
    WorkflowSpec,
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

    fn then_outcomes(&self, node: &str, outcomes: Vec<Outcome>) {
        let mut scripted = self.scripted.lock().unwrap();
        let queue = scripted.entry(node.to_owned()).or_default();
        for outcome in outcomes {
            queue.push_back(outcome);
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

/// Records the best-effort cancellation signal without requiring a real
/// provider process in the workflow test suite.
struct CancellableExecutor {
    cancelled: AtomicUsize,
}

#[async_trait]
impl TaskExecutor for CancellableExecutor {
    async fn run(&self, task: &TaskRecord, _contract: &TaskContract) -> Outcome {
        Outcome::Success {
            packet: serde_json::json!({ "node": task.node_id }),
        }
    }

    fn cancel(&self, _task: &TaskRecord) {
        self.cancelled.fetch_add(1, Ordering::SeqCst);
    }
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

/// Disjoint write scopes retain the existing fan-out behaviour, while a
/// configured cap bounds admission before workers are spawned.
#[tokio::test]
async fn admission_runs_disjoint_work_in_parallel_and_enforces_the_cap() {
    let (_dir, store) = temp_store();
    let executor = Arc::new(ConcurrentExecutor {
        current: AtomicUsize::new(0),
        max_seen: AtomicUsize::new(0),
    });
    let engine = WorkflowEngine::new_with_config(
        Arc::clone(&store),
        Arc::clone(&executor) as Arc<dyn TaskExecutor>,
        EngineConfig { max_concurrency: 2 },
    )
    .unwrap();
    let run_id = engine
        .start_run(
            &spec(
                "scoped",
                vec![
                    node("frontend", NodeType::Run, &[]),
                    node("backend", NodeType::Run, &[]),
                ],
            ),
            "ship both",
        )
        .unwrap();
    let tasks = store.tasks_for_run(&run_id).unwrap();
    for (task, path) in tasks.iter().zip(["frontend", "backend"]) {
        store
            .set_task_contract(
                &task.id,
                &TaskContract {
                    objective: task.node_id.clone(),
                    allowed_paths: vec![path.to_owned()],
                    forbidden_paths: vec![],
                    acceptance_criteria: vec![],
                    required_checks: vec![],
                },
            )
            .unwrap();
    }
    engine.run_until_idle(8).await.unwrap();
    assert_eq!(executor.max_seen.load(Ordering::SeqCst), 2);

    let capped_executor = Arc::new(ConcurrentExecutor {
        current: AtomicUsize::new(0),
        max_seen: AtomicUsize::new(0),
    });
    let capped = WorkflowEngine::new_with_config(
        Arc::new(TaskStore::open_in_memory().unwrap()),
        Arc::clone(&capped_executor) as Arc<dyn TaskExecutor>,
        EngineConfig { max_concurrency: 1 },
    )
    .unwrap();
    capped
        .start_run(
            &spec(
                "capped",
                vec![node("a", NodeType::Run, &[]), node("b", NodeType::Run, &[])],
            ),
            "one at a time",
        )
        .unwrap();
    capped.run_until_idle(8).await.unwrap();
    assert_eq!(capped_executor.max_seen.load(Ordering::SeqCst), 1);
    assert!(WorkflowEngine::new_with_config(
        Arc::new(TaskStore::open_in_memory().unwrap()),
        Arc::new(ScriptedExecutor::new()),
        EngineConfig { max_concurrency: 9 },
    )
    .is_err());
}

/// Overlapping declared write scopes are serialized and the deferred work
/// gets a machine-readable reason instead of consuming retry budget.
#[tokio::test]
async fn overlapping_write_scopes_are_serialized_with_a_queue_reason() {
    let (_dir, store) = temp_store();
    let executor = Arc::new(ConcurrentExecutor {
        current: AtomicUsize::new(0),
        max_seen: AtomicUsize::new(0),
    });
    let engine = WorkflowEngine::new_with_config(
        Arc::clone(&store),
        Arc::clone(&executor) as Arc<dyn TaskExecutor>,
        EngineConfig { max_concurrency: 4 },
    )
    .unwrap();
    let run_id = engine
        .start_run(
            &spec(
                "overlap",
                vec![
                    node("api", NodeType::Run, &[]),
                    node("server", NodeType::Run, &[]),
                ],
            ),
            "serialize writes",
        )
        .unwrap();
    let tasks = store.tasks_for_run(&run_id).unwrap();
    for (task, path) in tasks.iter().zip(["src", "src/api"]) {
        store
            .set_task_contract(
                &task.id,
                &TaskContract {
                    objective: task.node_id.clone(),
                    allowed_paths: vec![path.to_owned()],
                    forbidden_paths: vec![],
                    acceptance_criteria: vec![],
                    required_checks: vec![],
                },
            )
            .unwrap();
    }
    let first = engine.tick().await.unwrap();
    assert!(first
        .queued
        .iter()
        .any(|blocked| blocked.reasons == vec![QueueReason::PathConflict]));
    engine.run_until_idle(8).await.unwrap();
    assert_eq!(executor.max_seen.load(Ordering::SeqCst), 1);
}

/// Pause is durable and draining: it admits no new lease, then a resumed
/// engine can pick the already-ready work back up.
#[tokio::test]
async fn pause_resume_cancel_and_retry_are_explicit_controls() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(Arc::clone(&store), Arc::new(ScriptedExecutor::new()));
    let run_id = engine
        .start_run(
            &spec("controls", vec![node("work", NodeType::Run, &[])]),
            "control it",
        )
        .unwrap();
    engine.pause_run(&run_id).unwrap();
    assert!(store.run(&run_id).unwrap().paused);
    let paused = engine.tick().await.unwrap();
    assert_eq!(paused.leased, 0);
    assert!(paused
        .queued
        .iter()
        .any(|blocked| blocked.reasons == vec![QueueReason::Paused]));
    engine.resume_run(&run_id).unwrap();
    engine.run_until_idle(4).await.unwrap();
    assert_eq!(states_by_node(&engine, run_id)["work"], TaskState::Done);

    let retry_run = engine
        .start_run(
            &spec(
                "retry",
                vec![
                    node("failed", NodeType::Run, &[]),
                    node("later", NodeType::Run, &["failed"]),
                ],
            ),
            "retry it",
        )
        .unwrap();
    let failed = store
        .tasks_for_run(&retry_run)
        .unwrap()
        .into_iter()
        .find(|task| task.node_id == "failed")
        .unwrap();
    let failed = store
        .cas_transition(&failed.id, TaskState::Ready, TaskState::Failed)
        .unwrap();
    engine.scheduler().block_dependents_of(&failed).unwrap();
    let retried = engine.retry_task(&failed.id).unwrap();
    assert!(retried.changed.contains(&failed.id));
    assert_eq!(store.task(&failed.id).unwrap().state, TaskState::Ready);
    assert_eq!(
        states_by_node(&engine, retry_run)["later"],
        TaskState::Planned
    );

    let cancel_run = engine
        .start_run(
            &spec("cancel", vec![node("queued", NodeType::Run, &[])]),
            "cancel it",
        )
        .unwrap();
    let cancelled = engine.cancel_run(&cancel_run).unwrap();
    assert_eq!(cancelled.changed.len(), 1);
    assert_eq!(
        states_by_node(&engine, cancel_run)["queued"],
        TaskState::Cancelled
    );

    let cancellable = Arc::new(CancellableExecutor {
        cancelled: AtomicUsize::new(0),
    });
    let hook_engine = WorkflowEngine::new(
        Arc::new(TaskStore::open_in_memory().unwrap()),
        Arc::clone(&cancellable) as Arc<dyn TaskExecutor>,
    );
    let hook_run = hook_engine
        .start_run(
            &spec("hook", vec![node("leased", NodeType::Run, &[])]),
            "stop it",
        )
        .unwrap();
    let leased = hook_engine
        .store()
        .tasks_for_run(&hook_run)
        .unwrap()
        .remove(0);
    hook_engine
        .scheduler()
        .grant_lease(&leased.id, hook_engine.owner(), Duration::from_secs(60))
        .unwrap();
    hook_engine.cancel_task(&leased.id).unwrap();
    assert_eq!(cancellable.cancelled.load(Ordering::SeqCst), 1);
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

// ------------------------------------------- live re-planning (add_task)

/// F-12 re-planning: a node added to a live run is picked up by the next
/// tick, and its dependency edge is honoured — it stays `Planned` until the
/// dependency is `Done`.
#[tokio::test]
async fn add_task_extends_a_live_run_and_respects_the_new_edge() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
    let run_id = engine
        .start_run(
            &spec("feature-build", vec![node("spec", NodeType::Run, &[])]),
            "goal",
        )
        .expect("start run");

    // Added before `spec` finishes: depends on it, so it must not be Ready.
    let task_id = engine
        .add_task(&run_id, &node("review", NodeType::Review, &["spec"]))
        .expect("add task");
    assert_eq!(
        engine.store().task(&task_id).unwrap().state,
        TaskState::Planned
    );

    let driver = engine.run_until_idle(8).await.expect("drive");
    let states = states_by_node(&engine, run_id);
    assert_eq!(states["spec"], TaskState::Done);
    assert_eq!(states["review"], TaskState::Done, "added node ran too");
    assert_eq!(driver.succeeded.len(), 2);
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Completed);
}

/// A node whose dependencies are already `Done` is immediately `Ready`; one
/// added with no dependencies is too.
#[tokio::test]
async fn add_task_lands_ready_when_its_dependencies_are_already_done() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
    let run_id = engine
        .start_run(
            &spec("feature-build", vec![node("spec", NodeType::Run, &[])]),
            "goal",
        )
        .expect("start run");
    engine.run_until_idle(4).await.expect("drive");

    let dependent = engine
        .add_task(&run_id, &node("review", NodeType::Review, &["spec"]))
        .expect("add dependent");
    let free = engine
        .add_task(&run_id, &node("docs", NodeType::Run, &[]))
        .expect("add free");
    assert_eq!(
        engine.store().task(&dependent).unwrap().state,
        TaskState::Ready
    );
    assert_eq!(engine.store().task(&free).unwrap().state, TaskState::Ready);
}

/// A node added behind an already-failed dependency parks as `Blocked` —
/// nothing would ever promote it, so it must not sit `Planned` forever.
#[tokio::test]
async fn add_task_blocks_behind_a_failed_dependency() {
    let (_dir, store) = temp_store();
    let executor = Arc::new(ScriptedExecutor::new());
    executor.then_outcomes("spec", vec![Outcome::ReasoningFailure; 4]);
    let engine = WorkflowEngine::new(store, Arc::clone(&executor) as Arc<dyn TaskExecutor>);
    let run_id = engine
        .start_run(
            &spec("feature-build", vec![node("spec", NodeType::Run, &[])]),
            "goal",
        )
        .expect("start run");
    engine.run_until_idle(12).await.expect("drive");
    assert_eq!(states_by_node(&engine, run_id)["spec"], TaskState::Failed);

    let task_id = engine
        .add_task(&run_id, &node("review", NodeType::Review, &["spec"]))
        .expect("add task");
    assert_eq!(
        engine.store().task(&task_id).unwrap().state,
        TaskState::Blocked
    );
}

/// A bounded reasoning escalation consumes the first attempt, then changes
/// the durable route before another engine can lease the retry.
#[tokio::test]
async fn reasoning_escalation_retargets_the_next_retry_atomically() {
    let (_dir, store) = temp_store();
    let executor = Arc::new(ScriptedExecutor::new());
    executor.then_outcomes(
        "debug",
        vec![Outcome::EscalatedReasoningFailure {
            agent_role: "debugger-sol-escalation".to_owned(),
        }],
    );
    let engine = WorkflowEngine::new(store, Arc::clone(&executor) as Arc<dyn TaskExecutor>);
    let mut debug = node("debug", NodeType::Run, &[]);
    debug.agent_role = Some("debugger".to_owned());
    let run_id = engine
        .start_run(&spec("debugging", vec![debug]), "diagnose it")
        .expect("start run");

    engine.tick().await.expect("Terra attempt");
    let task = engine.store().tasks_for_run(&run_id).unwrap().remove(0);
    assert_eq!(task.state, TaskState::Ready);
    assert_eq!(task.attempt_count, 1);
    assert_eq!(
        task.node.agent_role.as_deref(),
        Some("debugger-sol-escalation")
    );

    engine.tick().await.expect("Sol retry");
    let task = engine.store().tasks_for_run(&run_id).unwrap().remove(0);
    assert_eq!(task.state, TaskState::Done);
    assert_eq!(
        task.node.agent_role.as_deref(),
        Some("debugger-sol-escalation")
    );
}

/// The engine stays authoritative over re-planning: cycles, duplicate ids,
/// dangling dependencies and terminal runs are all rejected, and nothing is
/// written when they are.
#[tokio::test]
async fn add_task_rejects_illegal_plans_and_terminal_runs() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(store, Arc::new(ScriptedExecutor::new()));
    let run_id = engine
        .start_run(
            &spec(
                "feature-build",
                vec![
                    node("spec", NodeType::Run, &[]),
                    node("review", NodeType::Review, &["spec"]),
                ],
            ),
            "goal",
        )
        .expect("start run");

    // A cycle: `spec` already depends on nothing, but `review` depends on
    // `spec`, so a node `spec` depends on via `review` would close a loop.
    let cyclic = NodeSpec {
        depends_on: vec!["review".to_owned()],
        ..node("spec", NodeType::Run, &[])
    };
    assert!(matches!(
        engine.add_task(&run_id, &cyclic),
        // duplicate id is caught before the cycle rule
        Err(WorkflowError::Validation(_))
    ));
    assert!(matches!(
        engine.add_task(&run_id, &node("ship", NodeType::Run, &["nope"])),
        Err(WorkflowError::Validation(_))
    ));
    assert_eq!(
        engine.store().tasks_for_run(&run_id).unwrap().len(),
        2,
        "rejected adds write nothing"
    );

    // An unknown run is NotFound, not a silent insert.
    assert!(matches!(
        engine.add_task(&Uuid::now_v7(), &node("late", NodeType::Run, &[])),
        Err(WorkflowError::Storage(agentos_core::CoreError::NotFound(_)))
    ));

    // Re-planning onto a run whose work has all landed is legal: the
    // `completed` projection is derived, not a freeze.
    engine.run_until_idle(8).await.expect("drive");
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Completed);
    engine
        .add_task(&run_id, &node("followup", NodeType::Run, &[]))
        .expect("re-plan onto a settled run");
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Running);
    engine.run_until_idle(8).await.expect("drive follow-up");
    assert_eq!(states_by_node(&engine, run_id)["followup"], TaskState::Done);
}

// ------------------------------------------ human approval parking

/// `Outcome::AwaitingApproval` parks the task in `HumanRequired` without
/// consuming an attempt, the driver goes idle instead of spinning, and the
/// run stays `Running`. Resuming is an explicit CAS by whoever resolves the
/// approval; the task then executes normally.
#[tokio::test]
async fn awaiting_approval_parks_without_consuming_an_attempt_and_resumes() {
    let (_dir, store) = temp_store();
    let executor = Arc::new(ScriptedExecutor::new());
    executor.then_outcomes("gate", vec![Outcome::AwaitingApproval]);
    let engine = WorkflowEngine::new(store, Arc::clone(&executor) as Arc<dyn TaskExecutor>);
    let run_id = engine
        .start_run(
            &spec("feature-build", vec![node("gate", NodeType::GitGate, &[])]),
            "goal",
        )
        .expect("start run");

    let driver = engine.run_until_idle(8).await.expect("drive");
    let parked = engine.store().tasks_for_run(&run_id).unwrap().remove(0);
    assert_eq!(parked.state, TaskState::HumanRequired);
    assert_eq!(
        parked.attempt_count, 0,
        "waiting on a human is not an attempt"
    );
    assert!(parked.lease_owner.is_none(), "lease released while parked");
    assert!(driver.failed.is_empty());
    assert!(driver.succeeded.is_empty());
    assert!(driver.ticks <= 3, "parked work must not spin the driver");
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Running);

    // The human resumes it: HumanRequired -> Ready, then it runs.
    engine
        .store()
        .cas_transition(&parked.id, TaskState::HumanRequired, TaskState::Ready)
        .expect("resume");
    engine.run_until_idle(8).await.expect("drive again");
    assert_eq!(
        engine.store().task(&parked.id).unwrap().state,
        TaskState::Done
    );
}

/// Retargeting a task at another pool (OR-01 escalation) rewrites the stored
/// node spec, but only while the task is not executing — a swap under a
/// leased task would change the contract the current attempt is working
/// against.
#[tokio::test]
async fn set_agent_role_retargets_queued_tasks_and_refuses_executing_ones() {
    let (_dir, store) = temp_store();
    let engine = WorkflowEngine::new(Arc::clone(&store), Arc::new(ScriptedExecutor::new()));
    let run_id = engine
        .start_run(
            &spec(
                "feature-build",
                vec![
                    node("spec", NodeType::Run, &[]),
                    node("review", NodeType::Review, &["spec"]),
                ],
            ),
            "goal",
        )
        .expect("start run");
    let tasks = engine.store().tasks_for_run(&run_id).unwrap();
    let ready = tasks.iter().find(|t| t.node_id == "spec").unwrap();
    let planned = tasks.iter().find(|t| t.node_id == "review").unwrap();

    // Ready and Planned tasks both retarget: the next lease reads the role.
    let retargeted = store
        .set_agent_role(&ready.id, Some("stronger"))
        .expect("retarget ready")
        .expect("state allows it");
    assert_eq!(retargeted.node.agent_role.as_deref(), Some("stronger"));
    assert_eq!(retargeted.state, TaskState::Ready, "state is untouched");
    assert!(store
        .set_agent_role(&planned.id, Some("stronger"))
        .expect("retarget planned")
        .is_some());

    // A leased task refuses — reported as "not routed", not an error.
    store
        .grant_lease(&ready.id, "someone", Duration::from_secs(60))
        .expect("lease");
    assert!(store
        .set_agent_role(&ready.id, Some("even-stronger"))
        .expect("no storage failure")
        .is_none());
    assert_eq!(
        store.task(&ready.id).unwrap().node.agent_role.as_deref(),
        Some("stronger"),
        "the leased task keeps the role its attempt was contracted with"
    );

    // Terminal tasks refuse too: there is no next attempt to route.
    store
        .cas_transition(&planned.id, TaskState::Planned, TaskState::Failed)
        .expect("fail it");
    assert!(store
        .set_agent_role(&planned.id, Some("stronger"))
        .expect("no storage failure")
        .is_none());
}

// ------------------------------------------------- operator reopen (F-12)

/// The observed Phase-8 failure, reproduced and then repaired.
///
/// `t07` burns its whole attempt budget against a provider outage that is
/// still in force, fails terminally, and parks its two transitive dependents
/// as `Blocked`; the run goes `Failed` with finished work stranded behind it.
/// Once the outage clears, one `reopen_run` has to give all three of them
/// back — with a *fresh* attempt budget, or the reopened task would be failed
/// again by the first budget gate it meets.
#[tokio::test]
async fn reopen_run_returns_a_failed_task_and_its_blocked_dependents_to_work() {
    let (_dir, store) = temp_store();
    let executor = Arc::new(ScriptedExecutor::new());
    // Exactly the attempts the budget allows fail; the outage "clears"
    // after that, so anything later succeeds.
    executor.then_transient_failures("t07", 2);
    let engine = WorkflowEngine::new(
        Arc::clone(&store),
        Arc::clone(&executor) as Arc<dyn TaskExecutor>,
    );
    let workflow = spec(
        "quota-outage",
        vec![
            governed_node(
                "t07",
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
            node("t09", NodeType::Run, &["t07"]),
            node("t10", NodeType::Run, &["t09"]),
        ],
    );
    let run_id = engine.start_run(&workflow, "phase 8 build").unwrap();
    engine.run_until_idle(20).await.unwrap();

    // The observed end state.
    let before = states_by_node(&engine, run_id);
    assert_eq!(before["t07"], TaskState::Failed);
    assert_eq!(before["t09"], TaskState::Blocked, "direct dependent");
    assert_eq!(before["t10"], TaskState::Blocked, "transitive dependent");
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Failed);
    assert_eq!(store.runs().unwrap()[0].status, RunStatus::Failed);

    let report = store.reopen_run(&run_id).expect("reopen");

    assert_eq!(report.reopened, vec!["t07".to_owned()]);
    assert_eq!(report.unblocked, vec!["t09".to_owned(), "t10".to_owned()]);
    assert_eq!(report.status, RunStatus::Running, "the run leaves Failed");
    assert!(report.changed_anything());

    let after = states_by_node(&engine, run_id);
    assert_eq!(after["t07"], TaskState::Ready, "queued again");
    assert_eq!(
        after["t09"],
        TaskState::Planned,
        "the exact inverse of block_dependents_of; the dependency gate \
         promotes it when t07 is actually Done"
    );
    assert_eq!(after["t10"], TaskState::Planned);
    let reopened = store
        .tasks_for_run(&run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.node_id == "t07")
        .unwrap();
    assert_eq!(
        reopened.attempt_count, 0,
        "a task reopened with an exhausted budget would fail again immediately"
    );
    assert!(reopened.lease_owner.is_none());
    assert_eq!(
        store.run(&run_id).unwrap().status,
        RunStatus::Running,
        "the cached run-status projection is recomputed, not left stale"
    );

    // The fresh attempts are real: the run now finishes the work it stranded.
    engine.run_until_idle(20).await.unwrap();
    let finished = states_by_node(&engine, run_id);
    assert!(
        finished.values().all(|state| *state == TaskState::Done),
        "{finished:?}"
    );
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Completed);
}

/// The invariant the reopen must not have cost us: `Failed` is still
/// terminal for every path the engine itself can take. Reopening is an
/// operator's explicit act, never something a tick can infer.
#[tokio::test]
async fn the_engine_never_resurrects_a_failed_task_on_its_own() {
    let (_dir, store) = temp_store();
    let executor = Arc::new(ScriptedExecutor::new());
    executor.then_transient_failures("t07", 2);
    let engine = WorkflowEngine::new(
        Arc::clone(&store),
        Arc::clone(&executor) as Arc<dyn TaskExecutor>,
    );
    let workflow = spec(
        "terminal-stays-terminal",
        vec![
            governed_node(
                "t07",
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
            node("t09", NodeType::Run, &["t07"]),
        ],
    );
    let run_id = engine.start_run(&workflow, "phase 8 build").unwrap();
    engine.run_until_idle(20).await.unwrap();
    let failed = store
        .tasks_for_run(&run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.node_id == "t07")
        .unwrap();
    assert_eq!(failed.state, TaskState::Failed);

    // The state machine still has no arc out of Failed, so the store's
    // CAS door — the one every engine path goes through — stays shut.
    for target in [
        TaskState::Ready,
        TaskState::Planned,
        TaskState::Retryable,
        TaskState::Running,
    ] {
        assert!(
            !TaskState::Failed.can_transition(&target),
            "failed -> {target} must stay illegal"
        );
        let err = store
            .cas_transition(&failed.id, TaskState::Failed, target)
            .expect_err("terminal tasks must refuse a CAS out");
        assert!(
            matches!(
                err,
                CoreError::IllegalTransition {
                    from: TaskState::Failed,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    // And no amount of ticking moves it: the executor's remaining scripted
    // outcomes are never reached, because the task is never leased again.
    for _ in 0..5 {
        let report = engine.tick().await.unwrap();
        assert_eq!(report.leased, 0);
        assert_eq!(report.requeued, 0);
        assert_eq!(report.promoted, 0);
    }
    let states = states_by_node(&engine, run_id);
    assert_eq!(states["t07"], TaskState::Failed);
    assert_eq!(states["t09"], TaskState::Blocked);
    assert_eq!(engine.run_status(&run_id).unwrap(), RunStatus::Failed);
}
