//! F-07 end-to-end tests: the full composition (MockAdapter → workflow
//! engine → git queue → agent ledger → event journal) driven as one loop
//! against a real temporary git repository, plus failure/retry, budget
//! escalation, and crash recovery.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use agentos_adapters::{MockAdapter, MockBehavior, RuntimeAdapter};
use agentos_core::{EventType, TaskState};
use agentos_git::cli;
use agentos_git::queue::RequestStatus;
use agentos_runtime::{
    ContractBudgets, DriveSummary, HandoffPacket, Supervisor, SupervisorConfig, TaskContract,
};
use agentos_workflow::{Budgets, NodeSpec, NodeType, RetryPolicy, RunStatus, WorkflowSpec};
use tempfile::TempDir;
use uuid::Uuid;

// ---------------------------------------------------------------- fixtures

/// A temporary repository with one seed commit; returns (dir, repo, head).
fn temp_repo() -> (TempDir, std::path::PathBuf, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    cli::init(&repo).expect("git init");
    std::fs::write(repo.join("README.md"), "seed\n").expect("seed file");
    let head = cli::add_all_and_commit(&repo, "agentos", "seed commit").expect("seed commit");
    (dir, repo, head)
}

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

/// spec (run) -> a, b (parallel runs) -> build (fan-in) -> review -> commit.
fn feature_build_spec() -> WorkflowSpec {
    WorkflowSpec {
        id: "feature-build".to_owned(),
        version: 1,
        nodes: vec![
            node("spec", NodeType::Run, &[]),
            node("a", NodeType::Run, &["spec"]),
            node("b", NodeType::Run, &["spec"]),
            node("build", NodeType::Parallel, &["a", "b"]),
            node("review", NodeType::Review, &["build"]),
            node("commit", NodeType::GitGate, &["review"]),
        ],
    }
}

/// Full contracts for every node of [`feature_build_spec``], keyed by node
/// id. Write tasks get disjoint path scopes so the parallel branches never
/// conflict on ownership.
fn contracts_for(spec: &WorkflowSpec, head: &str) -> HashMap<String, TaskContract> {
    let allowed_by_node: HashMap<&str, Vec<&str>> = HashMap::from([
        ("spec", vec!["docs/**"]),
        ("a", vec!["src/a/**"]),
        ("b", vec!["src/b/**"]),
        ("build", vec![]),
        ("review", vec![]),
        ("commit", vec![]),
    ]);
    spec.nodes
        .iter()
        .map(|node| {
            (
                node.id.clone(),
                TaskContract::builder(
                    format!("TASK-{}", node.id.to_uppercase()),
                    format!("execute `{}` for the F-07 e2e run", node.id),
                )
                .allowed_paths(
                    allowed_by_node
                        .get(node.id.as_str())
                        .map(|paths| {
                            paths
                                .iter()
                                .map(|p| (*p).to_owned())
                                .collect::<Vec<String>>()
                        })
                        .unwrap_or_default(),
                )
                .forbidden_paths(vec!["infra/prod/**".to_owned()])
                .context_refs(vec!["context.repo@1".to_owned()])
                .acceptance_criteria(vec!["change lands through the gate".to_owned()])
                .required_checks(vec!["cargo test".to_owned()])
                .budgets(ContractBudgets::new(10, 3))
                .base_commit(head.to_owned())
                .build()
                .expect("contract"),
            )
        })
        .collect()
}

fn success_adapter() -> Arc<MockAdapter> {
    Arc::new(MockAdapter::new(MockBehavior::Success {
        turns: 1,
        files_changed: vec!["src/a/mod.rs".to_owned()],
    }))
}

fn supervisor_for(repo: &Path, state_dir: &Path, mock: Arc<MockAdapter>) -> Supervisor {
    let config = SupervisorConfig::for_repo(state_dir, repo);
    Supervisor::new(config, vec![mock as Arc<dyn RuntimeAdapter>]).expect("supervisor")
}

fn tasks_by_node(
    supervisor: &Supervisor,
    run_id: Uuid,
) -> HashMap<String, agentos_workflow::TaskRecord> {
    supervisor
        .engine()
        .store()
        .tasks_for_run(&run_id)
        .expect("tasks")
        .into_iter()
        .map(|task| (task.node_id.clone(), task))
        .collect()
}

fn event_types(supervisor: &Supervisor, run_id: Uuid) -> Vec<String> {
    supervisor
        .events(&run_id)
        .expect("events")
        .iter()
        .map(|event| event.event_type.to_string())
        .collect()
}

/// Assert `expected` appears in `types` as an ordered subsequence.
fn assert_ordered_subsequence(types: &[String], expected: &[&str]) {
    let mut cursor = 0usize;
    for want in expected {
        match types[cursor..].iter().position(|t| t == want) {
            Some(offset) => cursor += offset + 1,
            None => panic!("expected `{want}` at or after index {cursor}; journal was {types:#?}"),
        }
    }
}

// ------------------------------------------------------------------- tests

/// E2E happy path: the six-node feature-build workflow runs to completion
/// through the real composition — validated handoff packets, a git request
/// that went through the queue, a ledger entry, and the Appendix E event
/// sequence in order.
#[tokio::test]
async fn e2e_happy_path_spec_parallel_review_gitgate() {
    let (dir, repo, head) = temp_repo();
    let spec = feature_build_spec();
    let supervisor = supervisor_for(&repo, &dir.path().join("state"), success_adapter());

    let run_id = supervisor
        .start_run(&spec, "ship F-07", contracts_for(&spec, &head))
        .expect("start run");
    let summary = supervisor.drive(&run_id, 20).await.expect("drive");

    assert_eq!(summary.status, RunStatus::Completed);
    let tasks = tasks_by_node(&supervisor, run_id);
    assert!(
        tasks.values().all(|t| t.state == TaskState::Done),
        "every task done: {:?}",
        tasks
            .iter()
            .map(|(k, v)| (k.as_str(), v.state))
            .collect::<Vec<_>>()
    );

    // Handoff packets: spec + both parallel branches (3 adapter sessions).
    // The two build-branch packets are exactly the "2 validated handoff
    // packets" of the parallel fan-out.
    let packets = supervisor.handoff_packets(&run_id).expect("packets");
    assert_eq!(packets.len(), 3, "spec + a + b produce packets");
    for node in ["a", "b"] {
        let task_id = tasks[node].id.to_string();
        let packet = packets
            .iter()
            .find(|p| p.task_id == task_id)
            .unwrap_or_else(|| panic!("packet for `{node}` missing"));
        packet.validate().expect("build packet validates (HO-01)");
        assert!(packet
            .transcript_ref
            .as_deref()
            .is_some_and(|r| r.starts_with("adapter-session:")));
    }

    // The git request went through the queue and is Done with the sha.
    let events = supervisor.events(&run_id).expect("events");
    let committed = events
        .iter()
        .find(|event| event.event_type == EventType::GitCommitted)
        .expect("git.committed event");
    let sha = committed.payload["sha"].as_str().expect("sha").to_owned();
    let request_id = committed.payload["requestId"]
        .as_str()
        .expect("requestId")
        .to_owned();
    let request = supervisor
        .queue()
        .get(&request_id)
        .expect("queue read")
        .expect("request exists");
    assert_eq!(request.status, RequestStatus::Done);
    assert_eq!(request.result_sha.as_deref(), Some(sha.as_str()));
    assert!(request.approved, "F-07 enqueues approved=true (F-10 seam)");

    // The ledger maps the sha to task/agent/reviewers/context/workflow.
    let record = supervisor
        .agent_ledger()
        .by_commit(&sha)
        .expect("ledger read")
        .expect("ledger entry");
    assert_eq!(record.entry.workflow_id, "feature-build");
    assert_eq!(record.entry.task_id, tasks["commit"].id.to_string());
    assert_eq!(
        record.entry.reviewers,
        vec!["stub-reviewer@f-07".to_owned()]
    );
    assert_eq!(record.entry.context_versions["context.repo"], "1");

    // Appendix E subset, in order, for this run.
    let types = event_types(&supervisor, run_id);
    assert_ordered_subsequence(
        &types,
        &[
            "run.created",
            "workflow.started",
            "task.created",
            "task.ready",
            "agent.leased",
            "task.running",
            "task.output_ready",
            "review.requested",
            "review.approved",
            "git.queued",
            "git.committed",
            "task.done",
            "run.completed",
        ],
    );

    // Usage flowed into the journal and the ledger priced the preamble.
    assert!(
        types.iter().filter(|t| *t == "usage.updated").count() >= 3,
        "one usage event per session"
    );
    let usage = supervisor
        .ledger()
        .run_total(&tasks.values().map(|t| t.id).collect::<Vec<_>>());
    assert!(usage.cost_usd > 0.0);
    assert!(usage.session_overhead_tokens >= 3 * 22_000);
    assert_eq!(usage.per_model.len(), 1, "the mock reports one model");

    // Every event of the run carries run_id and trace_id (F-00 §3).
    let trace_ids: std::collections::HashSet<Option<Uuid>> =
        events.iter().map(|event| event.trace_id).collect();
    assert_eq!(trace_ids.len(), 1, "one trace id for the whole run");
    assert!(events.iter().all(|event| event.trace_id.is_some()));
    assert!(events.iter().all(|event| event.run_id == Some(run_id)));

    // task.output_ready offloaded its payload: ref + hash, no inline packet.
    let output_ready = events
        .iter()
        .find(|event| event.event_type == EventType::TaskOutputReady)
        .expect("task.output_ready");
    let reference = output_ready.payload_ref.as_deref().expect("payload_ref");
    assert!(reference.starts_with("sha256:"));
    assert_eq!(output_ready.payload_hash.as_deref(), Some(reference));
    let digest = reference.trim_start_matches("sha256:");
    let artifact = repo
        .parent()
        .unwrap()
        .join("state")
        .join("artifacts")
        .join(format!("{digest}.json"));
    let stored: HandoffPacket =
        serde_json::from_slice(&std::fs::read(artifact).expect("artifact file"))
            .expect("artifact parses as a packet");
    stored.validate().expect("stored packet validates");

    // Worktrees are retained (GC/retention rules govern removal, not runs).
    let worktrees = std::fs::read_dir(repo.join(".agentos-worktrees"))
        .expect("managed worktree dir")
        .count();
    assert!(
        worktrees >= 4,
        "one worktree per adapter/gate task: {worktrees}"
    );
}

/// E2E failure + retry: a transient provider failure (rate-limit class) is
/// retried within budget and the run still completes.
#[tokio::test]
async fn e2e_flaky_transient_failure_retries_to_completion() {
    let (dir, repo, head) = temp_repo();
    let spec = feature_build_spec();
    // The first session of the whole supervisor flakes (mock counts per
    // adapter instance), then every later session succeeds.
    let mock = Arc::new(MockAdapter::new(MockBehavior::FlakyThenSuccess {
        failures_before_success: 1,
    }));
    let supervisor = supervisor_for(&repo, &dir.path().join("state"), mock);

    let run_id = supervisor
        .start_run(&spec, "retry within budget", contracts_for(&spec, &head))
        .expect("start run");
    let summary: DriveSummary = supervisor.drive(&run_id, 30).await.expect("drive");

    assert_eq!(summary.status, RunStatus::Completed);
    let tasks = tasks_by_node(&supervisor, run_id);
    assert!(
        tasks.values().all(|t| t.state == TaskState::Done),
        "run completes after the retry"
    );
    assert_eq!(
        tasks["spec"].attempt_count, 1,
        "the flaky attempt was consumed once, then the retry succeeded"
    );
    assert_eq!(
        tasks["a"].attempt_count, 0,
        "later sessions succeed first try"
    );

    // The failure path journaled its evidence: rate-limit notice + failure.
    let types = event_types(&supervisor, run_id);
    assert!(types.contains(&"agent.rate_limit".to_owned()));
    assert!(types.contains(&"task.failed".to_owned()));
    assert!(types.iter().filter(|t| *t == "task.running").count() >= 7);
}

/// E2E budget escalation: a cost ceiling below one session's spend makes
/// the supervisor emit `budget.exceeded` and fail the task — never
/// silently continue.
#[tokio::test]
async fn e2e_cost_budget_exceeded_escalates_to_failed() {
    let (dir, repo, head) = temp_repo();
    // One write node, one read-gate chain; the write node's cost ceiling is
    // below the mock's per-session $0.02.
    let spec = WorkflowSpec {
        id: "budget-run".to_owned(),
        version: 1,
        nodes: vec![
            NodeSpec {
                budgets: Budgets {
                    max_cost_usd: Some(0.01),
                    ..Budgets::default()
                },
                ..node("build", NodeType::Run, &[])
            },
            node("review", NodeType::Review, &["build"]),
        ],
    };
    let mut contracts = contracts_for(&spec, &head);
    contracts.insert(
        "build".to_owned(),
        TaskContract::builder("TASK-BUILD", "build the thing")
            .allowed_paths(vec!["src/**".to_owned()])
            .base_commit(head.to_owned())
            .budgets(ContractBudgets::new(5, 3))
            .build()
            .unwrap(),
    );

    let supervisor = supervisor_for(&repo, &dir.path().join("state"), success_adapter());
    let run_id = supervisor
        .start_run(&spec, "spend carefully", contracts)
        .expect("start run");
    let summary = supervisor.drive(&run_id, 20).await.expect("drive");

    assert_eq!(summary.status, RunStatus::Failed);
    let tasks = tasks_by_node(&supervisor, run_id);
    assert_eq!(tasks["build"].state, TaskState::Failed);
    assert_eq!(
        tasks["review"].state,
        TaskState::Blocked,
        "dependent parked"
    );
    assert!(
        supervisor
            .ledger()
            .usage(tasks["build"].id)
            .expect("usage recorded")
            .cost_usd
            >= 0.02
    );

    let events = supervisor.events(&run_id).expect("events");
    let exceeded: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == EventType::BudgetExceeded)
        .collect();
    assert!(
        !exceeded.is_empty(),
        "budget.exceeded must be journaled: {:?}",
        events
            .iter()
            .map(|e| e.event_type.to_string())
            .collect::<Vec<_>>()
    );
    assert!(exceeded.iter().any(|e| e.payload["stage"] == "session"));
}

/// E2E crash recovery: drive halfway, drop the supervisor with a task
/// ghost-leased, reconstruct over the same databases — the engine's
/// expired-lease path reclaims and the run completes.
#[tokio::test]
async fn e2e_crash_recovery_reclaims_expired_lease_and_completes() {
    let (dir, repo, head) = temp_repo();
    let state = dir.path().join("state");
    let spec = feature_build_spec();
    let contracts = contracts_for(&spec, &head);
    let run_id;

    {
        let supervisor = supervisor_for(&repo, &state, success_adapter());
        run_id = supervisor
            .start_run(&spec, "survive the crash", contracts.clone())
            .expect("start run");

        // One tick completes `spec`; the parallel branches are still
        // Planned.
        let report = supervisor.engine().tick().await.expect("tick");
        assert_eq!(report.succeeded.len(), 1);
        assert_eq!(report.succeeded[0].node_id, "spec");

        // A worker leases branch `a` and "crashes": lease already expired,
        // adapter session gone. Then the supervisor itself is dropped.
        supervisor
            .engine()
            .scheduler()
            .promote_unblocked()
            .expect("promote");
        let a = tasks_by_node(&supervisor, run_id)["a"].id;
        supervisor
            .engine()
            .scheduler()
            .grant_lease(&a, "ghost-worker", Duration::ZERO)
            .expect("ghost lease");
    } // supervisor dropped without cleanup

    // Reconstruct over the same databases: manifest, handoffs and journal
    // are all read back from disk.
    let successor = supervisor_for(&repo, &state, success_adapter());
    let ghosted = tasks_by_node(&successor, run_id);
    assert_eq!(ghosted["spec"].state, TaskState::Done, "progress survived");
    assert_eq!(
        ghosted["a"].state,
        TaskState::Leased,
        "ghost lease still on disk"
    );

    let summary = successor.drive(&run_id, 30).await.expect("drive");
    assert_eq!(summary.status, RunStatus::Completed);

    let tasks = tasks_by_node(&successor, run_id);
    assert!(tasks.values().all(|t| t.state == TaskState::Done));
    assert_eq!(
        tasks["a"].attempt_count, 1,
        "the reclaimed attempt was consumed exactly once"
    );
    assert_eq!(tasks["b"].attempt_count, 0);

    // The journal spans both supervisor generations and still ends in
    // run.completed, in Appendix E order.
    let types = event_types(&successor, run_id);
    assert_ordered_subsequence(
        &types,
        &[
            "workflow.started",
            "task.running",
            "review.approved",
            "git.queued",
            "git.committed",
            "run.completed",
        ],
    );
    let packets = successor.handoff_packets(&run_id).expect("packets");
    assert_eq!(
        packets.len(),
        3,
        "packets written before the crash are readable after it"
    );
}

/// Ownership holds are exclusive across concurrent branches: two Run nodes
/// with overlapping scopes cannot both hold them (the second attempt is
/// journaled as a conflict and retried after the first releases).
#[tokio::test]
async fn e2e_ownership_conflict_is_journaled_and_retried() {
    let (dir, repo, head) = temp_repo();
    // Two concurrent Run nodes claiming the SAME scope: one wins the
    // exclusive hold; the other's first attempt conflicts, is journaled,
    // and is retried as a transient failure once the winner has released
    // (its attempt ended), after which the hold is free.
    let spec = WorkflowSpec {
        id: "conflict".to_owned(),
        version: 1,
        nodes: vec![
            node("a", NodeType::Run, &[]),
            node("b", NodeType::Run, &[]),
            node("join", NodeType::Review, &["a", "b"]),
        ],
    };
    let mut contracts = HashMap::new();
    for node_id in ["a", "b"] {
        contracts.insert(
            node_id.to_owned(),
            TaskContract::builder(format!("TASK-{node_id}"), "contend for the same scope")
                .allowed_paths(vec!["src/shared/**".to_owned()])
                .base_commit(head.to_owned())
                .build()
                .unwrap(),
        );
    }
    contracts.insert(
        "join".to_owned(),
        TaskContract::builder("TASK-JOIN", "review the contention")
            .base_commit(head.to_owned())
            .build()
            .unwrap(),
    );

    let supervisor = supervisor_for(&repo, &dir.path().join("state"), success_adapter());
    let run_id = supervisor
        .start_run(&spec, "exclusive holds", contracts)
        .expect("start run");
    let summary = supervisor.drive(&run_id, 30).await.expect("drive");

    // The run still completes: the conflicted attempt is retried after the
    // winner released its hold.
    assert_eq!(summary.status, RunStatus::Completed);
    let types = event_types(&supervisor, run_id);
    assert!(
        types.contains(&"ownership.conflict".to_owned()),
        "the conflict is journaled: {types:?}"
    );
    let tasks = tasks_by_node(&supervisor, run_id);
    let conflicted_attempts: u32 = tasks.values().map(|t| t.attempt_count).sum();
    assert!(
        conflicted_attempts >= 1,
        "the conflicting attempt was consumed and retried"
    );
    assert!(supervisor.ownership().holds_for_task("").is_empty());
}
