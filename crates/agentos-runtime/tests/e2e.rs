//! F-07 end-to-end tests: the full composition (MockAdapter → workflow
//! engine → policy gate → git queue → agent ledger → event journal) driven
//! as one loop against a real temporary git repository, plus failure/retry,
//! budget escalation, crash recovery, and the F-10 policy boundary (PRD
//! §23.3) exercised through the real mutation queue.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use agentos_adapters::{MockAdapter, MockBehavior, RuntimeAdapter};
use agentos_core::{EventType, TaskState};
use agentos_git::cli;
use agentos_git::queue::RequestStatus;
use agentos_runtime::{
    ApprovalDecision, ContractBudgets, DriveSummary, HandoffPacket, PermissionSet, Supervisor,
    SupervisorConfig, TaskContract,
};
use agentos_workflow::{Budgets, NodeSpec, NodeType, RetryPolicy, RunStatus, WorkflowSpec};
use serde_json::{json, Value};
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

/// The human half of the F-10 gate: open the request the node's operation is
/// bound to and approve it. Returns the request id.
fn approve_gate(supervisor: &Supervisor, run_id: Uuid, node: &str) -> String {
    let request = supervisor
        .request_gate_approval(&run_id, node, "user@e2e")
        .expect("approval request");
    assert!(
        supervisor
            .approvals()
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve"),
        "a pending request must transition exactly once"
    );
    request.id
}

/// Approve some *other* operation on the same gate — the fingerprint-binding
/// probe. `mutate` receives the node's real operation payload.
fn approve_mutated_gate(
    supervisor: &Supervisor,
    run_id: Uuid,
    node: &str,
    ttl_secs: i64,
    mutate: impl FnOnce(&mut Value),
) {
    let (gate, mut operation) = supervisor
        .gate_operation(&run_id, node)
        .expect("gate operation");
    mutate(&mut operation);
    let request = supervisor
        .approvals()
        .request(
            gate,
            &operation,
            "user@e2e",
            chrono::Duration::seconds(ttl_secs),
        )
        .expect("request");
    assert!(supervisor
        .approvals()
        .resolve(&request.id, ApprovalDecision::Approved)
        .expect("resolve"));
}

/// Payload of the first event of `event_type` in the run's journal.
fn first_payload(supervisor: &Supervisor, run_id: Uuid, event_type: &str) -> Value {
    supervisor
        .events(&run_id)
        .expect("events")
        .into_iter()
        .find(|event| event.event_type.to_string() == event_type)
        .unwrap_or_else(|| panic!("no `{event_type}` event in the journal"))
        .payload
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
    // The git gate is fingerprint-bound: a human approves the exact
    // mutation before the gate node is ever leased (F-10 SEC-04).
    let approval_id = approve_gate(&supervisor, run_id, "commit");
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
    assert!(
        request.approved,
        "the queued row carries the policy verdict, not a literal"
    );
    assert_eq!(
        first_payload(&supervisor, run_id, "git.queued")["approved"],
        json!(true)
    );
    // The policy decision is in the append-only audit bundle (SEC-05).
    let bundle = supervisor
        .audit()
        .export_bundle(Some(&run_id.to_string()))
        .expect("audit bundle");
    let kinds: Vec<String> = bundle["events"]
        .as_array()
        .expect("events array")
        .iter()
        .map(|event| event["actionKind"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert!(
        kinds.contains(&"approval.requested".to_owned()),
        "the request is audited: {kinds:?}"
    );
    assert!(
        kinds.contains(&"git.gate".to_owned()),
        "the gate decision is audited: {kinds:?}"
    );
    assert!(
        kinds.contains(&"approval.consumed".to_owned()),
        "spending the approval is audited: {kinds:?}"
    );
    // The commit landed, so its single-use approval is spent: it can never
    // authorize a second mutation, ttl or no ttl.
    assert_eq!(
        supervisor
            .approvals()
            .status(&approval_id)
            .expect("approval status"),
        Some(agentos_runtime::ApprovalStatus::Consumed),
        "a git approval authorizes exactly one mutation"
    );
    let (gate, operation) = supervisor
        .gate_operation(&run_id, "commit")
        .expect("rebuild the gate operation");
    assert_eq!(gate.as_str(), "git_commit");
    assert!(
        !supervisor
            .approvals()
            .is_approved(gate, &operation)
            .expect("re-check"),
        "the spent approval must not authorize the same operation again"
    );

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
            "approval.granted",
            "git.queued",
            "git.committed",
            "task.done",
            "run.completed",
        ],
    );

    // SEC-01 at the adapter seam: the spawn spec's policy fields are the
    // compiled permission set, not the contract's raw globs.
    let spawn = first_payload(&supervisor, run_id, "session.spawn");
    let denied: Vec<&str> = spawn["toolDenylist"]
        .as_array()
        .expect("toolDenylist")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        denied.contains(&"WebFetch") && denied.contains(&"WebSearch"),
        "an offline worker's network tools are denied: {denied:?}"
    );
    assert!(
        denied.contains(&"Bash(git commit:*)"),
        "no-direct-git rides on top of the compiled denylist: {denied:?}"
    );
    let allowed: Vec<&str> = spawn["toolAllowlist"]
        .as_array()
        .expect("toolAllowlist")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(allowed.contains(&"Edit"), "write worker keeps Edit");
    assert!(
        spawn["allowedPaths"]
            .as_array()
            .expect("allowedPaths")
            .iter()
            .any(|path| path.as_str().is_some_and(|p| p.ends_with("/docs"))),
        "the contract's write root is granted, workspace-joined: {spawn:#?}"
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
    approve_gate(&supervisor, run_id, "commit");
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
        // Approved before the crash: the approval lives in SQLite and its
        // task-keyed record on disk, so the successor supervisor inherits it.
        approve_gate(&supervisor, run_id, "commit");

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

// ------------------------------------------------- F-10 policy boundary

/// Assert the run never mutated the repository: no queue row reached the
/// git manager, no commit was attributed, and the integration head is
/// exactly where it started.
fn assert_nothing_was_committed(supervisor: &Supervisor, run_id: Uuid, repo: &Path, head: &str) {
    let types = event_types(supervisor, run_id);
    assert!(
        !types.contains(&"git.queued".to_owned()),
        "a blocked mutation never reaches the queue: {types:?}"
    );
    assert!(
        !types.contains(&"git.committed".to_owned()),
        "a blocked mutation never commits: {types:?}"
    );
    assert!(
        types.contains(&"policy.denied".to_owned()),
        "the denial is journaled: {types:?}"
    );
    assert_eq!(
        cli::rev_parse_head(repo).expect("head"),
        head,
        "the integration head did not move"
    );
}

/// The policy audit rows for a run, as `actionKind` strings.
fn audit_kinds(supervisor: &Supervisor, run_id: Uuid) -> Vec<String> {
    supervisor
        .audit()
        .export_bundle(Some(&run_id.to_string()))
        .expect("audit bundle")["events"]
        .as_array()
        .expect("events array")
        .iter()
        .map(|event| event["actionKind"].as_str().unwrap_or_default().to_owned())
        .collect()
}

/// PRD §23.3, first leg: with **no** approval the git gate fails closed —
/// nothing is queued, nothing is committed, and the run fails. The gate
/// still opens a request a human could act on.
#[tokio::test]
async fn e2e_git_gate_blocks_without_an_approval() {
    let (dir, repo, head) = temp_repo();
    let spec = feature_build_spec();
    let supervisor = supervisor_for(&repo, &dir.path().join("state"), success_adapter());

    let run_id = supervisor
        .start_run(&spec, "commit without asking", contracts_for(&spec, &head))
        .expect("start run");
    let summary = supervisor.drive(&run_id, 30).await.expect("drive");

    assert_eq!(summary.status, RunStatus::Failed);
    let tasks = tasks_by_node(&supervisor, run_id);
    assert_eq!(tasks["commit"].state, TaskState::Failed);
    assert_nothing_was_committed(&supervisor, run_id, &repo, &head);

    let denial = first_payload(&supervisor, run_id, "policy.denied");
    assert_eq!(denial["reason"], json!("no_approval_requested"));
    assert!(
        denial["error"]
            .as_str()
            .expect("error")
            .contains("unexpired human approval"),
        "{denial:#?}"
    );
    // The gate surfaced a decision instead of losing it.
    let required = first_payload(&supervisor, run_id, "approval.required");
    assert_eq!(required["gate"], json!("git_commit"));
    assert!(required["requestId"].is_string());
    assert!(supervisor
        .agent_ledger()
        .by_task(&tasks["commit"].id.to_string())
        .expect("ledger read")
        .is_empty());
    let kinds = audit_kinds(&supervisor, run_id);
    assert!(kinds.contains(&"permission.denied".to_owned()), "{kinds:?}");
    assert!(kinds.contains(&"git.gate".to_owned()), "{kinds:?}");
}

/// SEC-04 fingerprint binding through the real queue: a **live, approved**
/// row for a mutation that differs by one field does not authorize this
/// mutation.
#[tokio::test]
async fn e2e_git_gate_blocks_when_the_approval_covers_a_mutated_operation() {
    let (dir, repo, head) = temp_repo();
    let spec = feature_build_spec();
    let supervisor = supervisor_for(&repo, &dir.path().join("state"), success_adapter());

    let run_id = supervisor
        .start_run(&spec, "approve something else", contracts_for(&spec, &head))
        .expect("start run");
    // Same gate, same task — a different base commit. One field is enough.
    approve_mutated_gate(&supervisor, run_id, "commit", 600, |operation| {
        operation["baseCommit"] = json!("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    });

    let summary = supervisor.drive(&run_id, 30).await.expect("drive");
    assert_eq!(summary.status, RunStatus::Failed);
    assert_eq!(
        tasks_by_node(&supervisor, run_id)["commit"].state,
        TaskState::Failed
    );
    assert_nothing_was_committed(&supervisor, run_id, &repo, &head);
}

/// SEC-04 expiry is enforced at check time: an approval whose ttl elapsed
/// authorizes nothing, even though its row still reads `approved`.
#[tokio::test]
async fn e2e_git_gate_blocks_when_the_approval_has_expired() {
    let (dir, repo, head) = temp_repo();
    let spec = feature_build_spec();
    let supervisor = supervisor_for(&repo, &dir.path().join("state"), success_adapter());

    let run_id = supervisor
        .start_run(&spec, "approve too late", contracts_for(&spec, &head))
        .expect("start run");
    // The exact operation, approved with a zero ttl (instantly expired).
    approve_mutated_gate(&supervisor, run_id, "commit", 0, |_| {});

    let summary = supervisor.drive(&run_id, 30).await.expect("drive");
    assert_eq!(summary.status, RunStatus::Failed);
    assert_nothing_was_committed(&supervisor, run_id, &repo, &head);
}

/// PRD §23.3, second leg: **permission trumps approval**. A gate role
/// without the `Commit` git action cannot commit even with a genuinely
/// approved, unexpired, exactly-matching approval.
#[tokio::test]
async fn e2e_git_gate_denies_a_role_without_git_permission_despite_a_live_approval() {
    let (dir, repo, head) = temp_repo();
    let spec = feature_build_spec();
    // A write worker holds no git actions — every mutation belongs to the
    // git manager (GIT-01).
    let config = SupervisorConfig::for_repo(dir.path().join("state"), &repo)
        .with_git_gate_permissions(PermissionSet::worker_write(&["**"]));
    let supervisor = Supervisor::new(config, vec![success_adapter() as Arc<dyn RuntimeAdapter>])
        .expect("supervisor");

    let run_id = supervisor
        .start_run(
            &spec,
            "commit without the role",
            contracts_for(&spec, &head),
        )
        .expect("start run");
    approve_gate(&supervisor, run_id, "commit");

    let summary = supervisor.drive(&run_id, 30).await.expect("drive");
    assert_eq!(summary.status, RunStatus::Failed);
    assert_nothing_was_committed(&supervisor, run_id, &repo, &head);
    let denial = first_payload(&supervisor, run_id, "policy.denied");
    assert_eq!(
        denial["reason"],
        json!("approved"),
        "the approval was live; the permission leg refused it"
    );
    assert!(
        denial["error"]
            .as_str()
            .expect("error")
            .contains("not permitted"),
        "{denial:#?}"
    );
}

/// A `HumanApproval` node with an approval resolves and the run completes;
/// the same workflow without one waits (bounded by the transient retry
/// budget) and then fails; a refusal fails immediately. The supervisor
/// never self-approves.
#[tokio::test]
async fn e2e_human_approval_node_resolves_both_ways() {
    let approval_spec = WorkflowSpec {
        id: "human-gate".to_owned(),
        version: 1,
        nodes: vec![
            node("build", NodeType::Run, &[]),
            node("gate", NodeType::HumanApproval, &["build"]),
            node("review", NodeType::Review, &["gate"]),
        ],
    };
    let contracts_for_gate = |head: &str| -> HashMap<String, TaskContract> {
        let mut contracts = HashMap::new();
        contracts.insert(
            "build".to_owned(),
            TaskContract::builder("TASK-BUILD", "build behind a human gate")
                .allowed_paths(vec!["src/**".to_owned()])
                .base_commit(head.to_owned())
                .build()
                .unwrap(),
        );
        for node_id in ["gate", "review"] {
            contracts.insert(
                node_id.to_owned(),
                TaskContract::builder(format!("TASK-{node_id}"), "human decision")
                    .base_commit(head.to_owned())
                    .build()
                    .unwrap(),
            );
        }
        contracts
    };

    // ---- approved: the node proceeds.
    {
        let (dir, repo, head) = temp_repo();
        let supervisor = supervisor_for(&repo, &dir.path().join("state"), success_adapter());
        let run_id = supervisor
            .start_run(&approval_spec, "ask a human", contracts_for_gate(&head))
            .expect("start run");
        approve_gate(&supervisor, run_id, "gate");

        let summary = supervisor.drive(&run_id, 30).await.expect("drive");
        assert_eq!(summary.status, RunStatus::Completed);
        assert_eq!(
            tasks_by_node(&supervisor, run_id)["gate"].state,
            TaskState::Done
        );
        let granted = first_payload(&supervisor, run_id, "approval.granted");
        assert_eq!(granted["gate"], json!("prod_action"));
        assert!(granted["operationFingerprint"]
            .as_str()
            .expect("fingerprint")
            .starts_with("fnv1a64:"));
    }

    // ---- unresolved: the node parks on the human, burning no retries,
    // and resumes when the approval lands.
    {
        let (dir, repo, head) = temp_repo();
        let supervisor = supervisor_for(&repo, &dir.path().join("state"), success_adapter());
        let run_id = supervisor
            .start_run(
                &approval_spec,
                "nobody answers yet",
                contracts_for_gate(&head),
            )
            .expect("start run");

        let summary = supervisor.drive(&run_id, 30).await.expect("drive");
        assert_eq!(summary.status, RunStatus::Running, "parked, not failed");
        let tasks = tasks_by_node(&supervisor, run_id);
        assert_eq!(tasks["gate"].state, TaskState::HumanRequired);
        assert_eq!(
            tasks["gate"].attempt_count, 0,
            "waiting on a person is not a failed attempt"
        );
        assert_eq!(
            tasks["review"].state,
            TaskState::Planned,
            "the dependent waits behind the gate, never run unapproved"
        );
        let types = event_types(&supervisor, run_id);
        assert!(types.contains(&"approval.required".to_owned()), "{types:?}");
        assert!(
            !types.contains(&"approval.granted".to_owned()),
            "nothing self-approves: {types:?}"
        );

        // The human answers late: approve, un-park, drive again.
        approve_gate(&supervisor, run_id, "gate");
        supervisor
            .engine()
            .store()
            .cas_transition(
                &tasks["gate"].id,
                TaskState::HumanRequired,
                TaskState::Ready,
            )
            .expect("resume the parked gate");
        let summary = supervisor.drive(&run_id, 30).await.expect("drive again");
        assert_eq!(summary.status, RunStatus::Completed);
        let tasks = tasks_by_node(&supervisor, run_id);
        assert_eq!(tasks["gate"].state, TaskState::Done);
        assert_eq!(tasks["review"].state, TaskState::Done);
    }

    // ---- denied: the node fails deterministically with the typed denial.
    {
        let (dir, repo, head) = temp_repo();
        let supervisor = supervisor_for(&repo, &dir.path().join("state"), success_adapter());
        let run_id = supervisor
            .start_run(&approval_spec, "a human says no", contracts_for_gate(&head))
            .expect("start run");
        let request = supervisor
            .request_gate_approval(&run_id, "gate", "user@e2e")
            .expect("request");
        assert!(supervisor
            .approvals()
            .resolve(&request.id, ApprovalDecision::Denied)
            .expect("resolve"));

        let summary = supervisor.drive(&run_id, 30).await.expect("drive");
        assert_eq!(summary.status, RunStatus::Failed);
        let denied = first_payload(&supervisor, run_id, "approval.denied");
        assert_eq!(denied["reason"], json!("denied"));
        assert_eq!(denied["requestId"], json!(request.id));
        assert_eq!(
            tasks_by_node(&supervisor, run_id)["gate"].attempt_count,
            2,
            "a refusal is a reasoning failure: it burns the single reasoning \
             retry, not the (larger) transient wait budget"
        );
    }
}

/// SEC-01 at spawn: a permission set that cannot cover the contract refuses
/// the session before any billable work happens.
#[tokio::test]
async fn e2e_spawn_is_refused_when_policy_cannot_cover_the_contract() {
    let (dir, repo, head) = temp_repo();
    let spec = WorkflowSpec {
        id: "narrow-policy".to_owned(),
        version: 1,
        nodes: vec![NodeSpec {
            agent_role: Some("narrow".to_owned()),
            ..node("build", NodeType::Run, &[])
        }],
    };
    let mut contracts = HashMap::new();
    contracts.insert(
        "build".to_owned(),
        TaskContract::builder("TASK-BUILD", "write outside the granted scope")
            .allowed_paths(vec!["src/wide/**".to_owned()])
            .base_commit(head.to_owned())
            .build()
            .unwrap(),
    );
    let config = SupervisorConfig::for_repo(dir.path().join("state"), &repo)
        .with_role_permissions("narrow", PermissionSet::worker_write(&["src/narrow/**"]));
    let supervisor = Supervisor::new(config, vec![success_adapter() as Arc<dyn RuntimeAdapter>])
        .expect("supervisor");

    let run_id = supervisor
        .start_run(&spec, "over-claim the scope", contracts)
        .expect("start run");
    let summary = supervisor.drive(&run_id, 20).await.expect("drive");

    assert_eq!(summary.status, RunStatus::Failed);
    let types = event_types(&supervisor, run_id);
    assert!(types.contains(&"policy.denied".to_owned()), "{types:?}");
    assert!(
        !types.contains(&"session.started".to_owned()),
        "no session is ever spawned: {types:?}"
    );
    let denial = first_payload(&supervisor, run_id, "policy.denied");
    assert_eq!(denial["stage"], json!("compile"));
    assert!(denial["error"]
        .as_str()
        .expect("error")
        .contains("src/wide/**"));
}
