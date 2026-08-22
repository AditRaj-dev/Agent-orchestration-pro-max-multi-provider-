//! F-11 §3.3 fold obligations against synthetic journals: the full happy
//! path, crash-retry, human parking, unknown event types (totality), the
//! usage rollup, run-status derivation, and the exact wire shapes.

use agentos_core::{Event, EventType, TaskState};
use agentos_daemon::events::SequencedEvent;
use agentos_daemon::projection::{fold, AgentStatus, RunStatus};
use serde_json::{json, Value};
use uuid::Uuid;

/// Assign seqs 1..N in journal order.
fn journal_of(events: Vec<Event>) -> Vec<SequencedEvent> {
    events
        .into_iter()
        .enumerate()
        .map(|(index, event)| SequencedEvent {
            seq: index as i64 + 1,
            event,
        })
        .collect()
}

/// Full happy-path fold: one task walks created → done with a commit sha;
/// deps arrive from the workflow.started node list; the agent ends complete
/// with rolled-up usage; the run derives `completed`.
#[test]
fn happy_path_fold_matches_the_table() {
    let run = Uuid::now_v7();
    let trace = Uuid::now_v7();
    let task = Uuid::now_v7();
    let ev = |event_type: EventType| {
        Event::new(event_type)
            .with_run_id(run)
            .with_trace_id(trace)
            .with_task_id(task)
    };

    let journal = journal_of(vec![
        Event::new(EventType::RunCreated)
            .with_run_id(run)
            .with_trace_id(trace)
            .with_payload(json!({ "goal": "ship" })),
        Event::new(EventType::WorkflowStarted)
            .with_run_id(run)
            .with_trace_id(trace)
            .with_payload(json!({
                "workflowId": "wf-1",
                "nodes": [{ "id": "n1", "dependsOn": [] }]
            })),
        ev(EventType::TaskCreated).with_payload(json!({ "node": "n1" })),
        ev(EventType::TaskReady),
        ev(EventType::AgentLeased).with_agent_id("worker-1"),
        ev(EventType::TaskRunning).with_agent_id("worker-1"),
        Event::new(EventType::Other("session.spawn".to_owned()))
            .with_run_id(run)
            .with_trace_id(trace)
            .with_task_id(task)
            .with_agent_id("worker-1")
            .with_payload(json!({ "provider": "mock-inc" })),
        Event::new(EventType::Other("session.started".to_owned()))
            .with_run_id(run)
            .with_trace_id(trace)
            .with_task_id(task)
            .with_agent_id("worker-1")
            .with_payload(json!({ "model": "mock-model" })),
        Event::new(EventType::Other("usage.updated".to_owned()))
            .with_run_id(run)
            .with_trace_id(trace)
            .with_task_id(task)
            .with_agent_id("worker-1")
            .with_payload(json!({ "costUsd": 0.75, "totalTokens": 1000 })),
        ev(EventType::TaskOutputReady),
        ev(EventType::ReviewRequested),
        ev(EventType::ReviewApproved),
        ev(EventType::GitQueued),
        ev(EventType::GitCommitted).with_payload(json!({ "sha": "abc123" })),
        ev(EventType::TaskDone),
        Event::new(EventType::RunCompleted)
            .with_run_id(run)
            .with_trace_id(trace),
    ]);

    let projection = fold(&journal);

    let runs = projection.runs();
    assert_eq!(runs.len(), 1);
    let summary = runs[0];
    assert_eq!(summary.run_id, run.to_string());
    assert_eq!(summary.status, RunStatus::Completed);
    assert_eq!(summary.workflow_id.as_deref(), Some("wf-1"));
    assert_eq!(summary.task_counts.total, 1);
    assert_eq!(summary.task_counts.done, 1);
    assert_eq!(summary.task_counts.failed, 0);
    assert_eq!(summary.task_counts.active, 0);
    assert_eq!(summary.first_seq, 1);
    assert_eq!(summary.last_seq, 16);
    assert_eq!(summary.event_count, 16);
    assert!(summary.ended_at.is_some());

    let tasks = projection.tasks(None);
    assert_eq!(tasks.len(), 1);
    let task_summary = tasks[0];
    assert_eq!(task_summary.task_id, task.to_string());
    assert_eq!(task_summary.run_id, run.to_string());
    assert_eq!(task_summary.node_id.as_deref(), Some("n1"));
    assert_eq!(task_summary.state, TaskState::Done);
    assert_eq!(task_summary.agent_id.as_deref(), Some("worker-1"));
    assert_eq!(task_summary.depends_on, Vec::<String>::new());
    assert_eq!(task_summary.commit_sha.as_deref(), Some("abc123"));
    assert_eq!(task_summary.attempts, 0);
    assert_eq!(task_summary.last_event_type, "task.done");

    let agents = projection.agents(None);
    assert_eq!(agents.len(), 1);
    let agent = agents[0];
    assert_eq!(agent.agent_id, "worker-1");
    assert_eq!(agent.provider.as_deref(), Some("mock-inc"));
    assert_eq!(agent.model.as_deref(), Some("mock-model"));
    assert_eq!(
        agent.status,
        AgentStatus::Complete,
        "run terminal → complete"
    );
    assert_eq!(agent.usage.cost_usd, 0.75);
    assert_eq!(agent.usage.tokens_estimate, 1000);
    assert_eq!(agent.event_count, 5);
}

/// `agent.crashed` bumps attempts; the re-lease continues the machine
/// without resurrecting the counter.
#[test]
fn crash_retry_increments_attempts_once() {
    let run = Uuid::now_v7();
    let task = Uuid::now_v7();
    let ev = |event_type: EventType| {
        Event::new(event_type)
            .with_run_id(run)
            .with_task_id(task)
            .with_agent_id("worker-1")
    };

    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        ev(EventType::TaskCreated),
        ev(EventType::TaskReady),
        ev(EventType::AgentLeased),
        ev(EventType::TaskRunning),
        ev(EventType::AgentCrashed),
        ev(EventType::TaskReady),
        ev(EventType::AgentLeased),
        ev(EventType::TaskRunning),
    ]);

    let projection = fold(&journal);
    let task_summary = &projection.tasks(None)[0];
    assert_eq!(task_summary.state, TaskState::Running);
    assert_eq!(task_summary.attempts, 1, "exactly one crash, one increment");
    assert_eq!(task_summary.last_event_type, "task.running");
}

/// `approval.required` with `blocking: true` parks the task in
/// `human_required`; a non-blocking request never parks; resume is a later
/// state event.
#[test]
fn blocking_approval_parks_task_in_human_required() {
    let run = Uuid::now_v7();
    let task = Uuid::now_v7();
    let ev = |event_type: EventType, payload: Value| {
        Event::new(event_type)
            .with_run_id(run)
            .with_task_id(task)
            .with_payload(payload)
    };

    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        ev(EventType::TaskCreated, json!({})),
        ev(EventType::TaskReady, json!({})),
        ev(EventType::AgentLeased, json!({})),
        ev(EventType::TaskRunning, json!({})),
        ev(
            EventType::Other("approval.required".to_owned()),
            json!({ "gate": "prod_action", "requestId": "r1", "blocking": true }),
        ),
    ]);
    let projection = fold(&journal);
    assert_eq!(projection.tasks(None)[0].state, TaskState::HumanRequired);

    // Non-blocking (git-gate informational request): state unchanged.
    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        ev(EventType::TaskCreated, json!({})),
        ev(EventType::TaskReady, json!({})),
        ev(EventType::AgentLeased, json!({})),
        ev(EventType::TaskRunning, json!({})),
        ev(
            EventType::Other("approval.required".to_owned()),
            json!({ "gate": "git_push", "requestId": "r2" }),
        ),
    ]);
    let projection = fold(&journal);
    assert_eq!(
        projection.tasks(None)[0].state,
        TaskState::Running,
        "non-blocking approval.required must not park"
    );

    // Resume: a later task.ready walks the task out of the park.
    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        ev(EventType::TaskCreated, json!({})),
        ev(EventType::TaskReady, json!({})),
        ev(EventType::TaskRunning, json!({})),
        ev(
            EventType::Other("approval.required".to_owned()),
            json!({ "blocking": true }),
        ),
        ev(EventType::TaskReady, json!({})),
    ]);
    let projection = fold(&journal);
    assert_eq!(projection.tasks(None)[0].state, TaskState::Ready);
}

/// Totality: unknown event types fold without error and only bump
/// counters/lastEventType — including unknown strings this build has never
/// seen and events for tasks that were never created.
#[test]
fn unknown_event_types_fold_totally() {
    let run = Uuid::now_v7();
    let task = Uuid::now_v7();
    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        Event::new(EventType::TaskCreated)
            .with_run_id(run)
            .with_task_id(task),
        Event::new(EventType::TaskReady)
            .with_run_id(run)
            .with_task_id(task),
        Event::new(EventType::Other("quantum.entangled".to_owned()))
            .with_run_id(run)
            .with_task_id(task),
        Event::new(EventType::Other("future.v2.thing".to_owned())).with_run_id(run),
    ]);

    let projection = fold(&journal);
    let task_summary = &projection.tasks(None)[0];
    assert_eq!(task_summary.state, TaskState::Ready, "state untouched");
    assert_eq!(task_summary.last_event_type, "quantum.entangled");

    let run_summary = &projection.runs()[0];
    assert_eq!(run_summary.event_count, 5, "unknown types still count");
    assert_eq!(run_summary.last_seq, 5);
    assert_eq!(run_summary.status, RunStatus::Running);
}

/// The usage rollup sums cost when reported (null cost is claude-canon
/// "provider reports tokens only") and falls back to input+output tokens
/// when totalTokens is absent.
#[test]
fn usage_rollup_sums_costs_and_tokens() {
    let run = Uuid::now_v7();
    let task = Uuid::now_v7();
    let usage = |payload: Value| {
        Event::new(EventType::Other("usage.updated".to_owned()))
            .with_run_id(run)
            .with_task_id(task)
            .with_agent_id("claude-code")
            .with_payload(payload)
    };

    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        usage(json!({ "costUsd": 0.4132, "totalTokens": 140270 })),
        usage(json!({ "costUsd": null, "inputTokens": 10, "outputTokens": 5 })),
    ]);

    let projection = fold(&journal);
    let agent = &projection.agents(None)[0];
    assert_eq!(agent.usage.cost_usd, 0.4132);
    assert_eq!(agent.usage.tokens_estimate, 140270 + 10 + 5);
}

/// F-06 derivation on run.completed: a failed task fails the run;
/// run.failed is failed outright; taskCounts reflect the mix.
#[test]
fn run_status_derivation_on_completion() {
    let run = Uuid::now_v7();
    let ok_task = Uuid::now_v7();
    let bad_task = Uuid::now_v7();
    let mut journal = vec![Event::new(EventType::RunCreated).with_run_id(run)];
    for task in [ok_task, bad_task] {
        journal.push(
            Event::new(EventType::TaskCreated)
                .with_run_id(run)
                .with_task_id(task),
        );
    }
    journal.push(
        Event::new(EventType::TaskDone)
            .with_run_id(run)
            .with_task_id(ok_task),
    );
    journal.push(
        Event::new(EventType::Other("task.failed".to_owned()))
            .with_run_id(run)
            .with_task_id(bad_task),
    );

    let completed = fold(&journal_of({
        let mut journal = journal.clone();
        journal.push(Event::new(EventType::RunCompleted).with_run_id(run));
        journal
    }));
    let summary = &completed.runs()[0];
    assert_eq!(
        summary.status,
        RunStatus::Failed,
        "any failed task fails the run"
    );
    assert_eq!(summary.task_counts.total, 2);
    assert_eq!(summary.task_counts.done, 1);
    assert_eq!(summary.task_counts.failed, 1);
    assert_eq!(summary.task_counts.active, 0);

    let failed = fold(&journal_of({
        let mut journal = journal;
        journal.push(Event::new(EventType::Other("run.failed".to_owned())).with_run_id(run));
        journal
    }));
    assert_eq!(failed.runs()[0].status, RunStatus::Failed);
    assert_eq!(failed.tasks(None)[1].state, TaskState::Failed);
}

/// `runs.list` is newest-run-first; task/agent filters follow runId.
#[test]
fn runs_are_newest_first_and_filters_work() {
    let run_a = Uuid::now_v7();
    let run_b = Uuid::now_v7();
    let task_a = Uuid::now_v7();
    let task_b = Uuid::now_v7();
    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run_a),
        Event::new(EventType::TaskCreated)
            .with_run_id(run_a)
            .with_task_id(task_a),
        Event::new(EventType::AgentLeased)
            .with_run_id(run_a)
            .with_task_id(task_a)
            .with_agent_id("shared-agent"),
        Event::new(EventType::RunCreated).with_run_id(run_b),
        Event::new(EventType::TaskCreated)
            .with_run_id(run_b)
            .with_task_id(task_b),
    ]);

    let projection = fold(&journal);
    let runs = projection.runs();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].run_id, run_b.to_string(), "newest run first");
    assert_eq!(runs[1].run_id, run_a.to_string());

    assert_eq!(projection.tasks(None).len(), 2);
    assert_eq!(projection.tasks(Some(run_a.to_string().as_str())).len(), 1);
    assert_eq!(projection.tasks(Some(run_b.to_string().as_str())).len(), 1);

    // The agent's latest run association is run_b? No — it only ever worked
    // run_a; filtering by run_b finds nothing, by run_a finds it.
    assert_eq!(projection.agents(Some(run_a.to_string().as_str())).len(), 1);
    assert!(projection
        .agents(Some(run_b.to_string().as_str()))
        .is_empty());
}

/// The agent status vocabulary transitions (§3.3): planning → running →
/// waiting → reviewing → failed, and run terminal → complete; `agent.leased`
/// and `task.running` both read as `running`.
#[test]
fn agent_status_ladder() {
    let run = Uuid::now_v7();
    let task = Uuid::now_v7();
    let agent_event = |type_str: &str| {
        Event::new(EventType::Other(type_str.to_owned()))
            .with_run_id(run)
            .with_task_id(task)
            .with_agent_id("a1")
    };

    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        Event::new(EventType::TaskCreated)
            .with_run_id(run)
            .with_task_id(task),
        agent_event("session.spawn"),
    ]);
    assert_eq!(fold(&journal).agents(None)[0].status, AgentStatus::Planning);

    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        Event::new(EventType::TaskCreated)
            .with_run_id(run)
            .with_task_id(task),
        agent_event("session.spawn"),
        agent_event("session.started"),
    ]);
    assert_eq!(fold(&journal).agents(None)[0].status, AgentStatus::Running);

    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        agent_event("agent.rate_limit"),
    ]);
    assert_eq!(fold(&journal).agents(None)[0].status, AgentStatus::Waiting);

    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        agent_event("review.requested"),
    ]);
    assert_eq!(
        fold(&journal).agents(None)[0].status,
        AgentStatus::Reviewing
    );

    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        agent_event("agent.spawn_failed"),
    ]);
    assert_eq!(fold(&journal).agents(None)[0].status, AgentStatus::Failed);

    // agent.leased / task.running map to running (the table's
    // "leased/running" pair names two events, one state).
    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        Event::new(EventType::TaskCreated)
            .with_run_id(run)
            .with_task_id(task),
        Event::new(EventType::AgentLeased)
            .with_run_id(run)
            .with_task_id(task)
            .with_agent_id("a1"),
    ]);
    assert_eq!(fold(&journal).agents(None)[0].status, AgentStatus::Running);

    // Budget flag on the task only — the RunSummary shape has no such field.
    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        Event::new(EventType::TaskCreated)
            .with_run_id(run)
            .with_task_id(task),
        Event::new(EventType::BudgetExceeded)
            .with_run_id(run)
            .with_task_id(task),
    ]);
    let projection = fold(&journal);
    assert!(projection.tasks(None)[0].budget_exceeded);
    let wire = serde_json::to_value(projection.runs()[0]).unwrap();
    assert!(
        wire.get("budgetExceeded").is_none(),
        "§3.3 RunSummary carries no budgetExceeded field"
    );
}

/// §3.3 exact wire shapes: the serde key sets match the contract exactly
/// (no extra fields, none missing) and states are the canonical
/// snake_case strings.
#[test]
fn summary_wire_shapes_match_the_contract_exactly() {
    let run = Uuid::now_v7();
    let task = Uuid::now_v7();
    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        Event::new(EventType::WorkflowStarted).with_run_id(run),
        Event::new(EventType::TaskCreated)
            .with_run_id(run)
            .with_task_id(task),
        Event::new(EventType::AgentLeased)
            .with_run_id(run)
            .with_task_id(task)
            .with_agent_id("w1"),
        Event::new(EventType::Other("usage.updated".to_owned()))
            .with_run_id(run)
            .with_task_id(task)
            .with_agent_id("w1")
            .with_payload(json!({ "costUsd": 0.0, "totalTokens": 0 })),
    ]);
    let projection = fold(&journal);

    let keys = |value: &Value| -> Vec<String> {
        let mut keys: Vec<String> = value.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    };

    let run_wire = serde_json::to_value(projection.runs()[0]).unwrap();
    assert_eq!(
        keys(&run_wire),
        [
            "endedAt",
            "eventCount",
            "firstSeq",
            "lastSeq",
            "runId",
            "startedAt",
            "status",
            "taskCounts",
            "workflowId"
        ],
        "RunSummary keys (§3.3)"
    );
    assert_eq!(
        keys(&run_wire["taskCounts"]),
        ["active", "done", "failed", "total"]
    );

    let task_wire = serde_json::to_value(projection.tasks(None)[0]).unwrap();
    assert_eq!(
        keys(&task_wire),
        [
            "agentId",
            "attempts",
            "budgetExceeded",
            "commitSha",
            "dependsOn",
            "lastEventAt",
            "lastEventType",
            "nodeId",
            "runId",
            "state",
            "taskId"
        ],
        "TaskSummary keys (§3.3)"
    );
    assert_eq!(task_wire["state"], json!("leased"));

    let agent_wire = serde_json::to_value(projection.agents(None)[0]).unwrap();
    assert_eq!(
        keys(&agent_wire),
        [
            "agentId",
            "eventCount",
            "lastEventAt",
            "lastEventType",
            "model",
            "provider",
            "runId",
            "status",
            "taskId",
            "usage"
        ],
        "AgentSummary keys (§3.3)"
    );
    assert_eq!(keys(&agent_wire["usage"]), ["costUsd", "tokensEstimate"]);

    // Every TaskState lands as its canonical snake_case wire string.
    let states = [
        (TaskState::Created, "created"),
        (TaskState::Ready, "ready"),
        (TaskState::Leased, "leased"),
        (TaskState::Running, "running"),
        (TaskState::OutputReady, "output_ready"),
        (TaskState::ReviewPending, "review_pending"),
        (TaskState::Approved, "approved"),
        (TaskState::GitQueued, "git_queued"),
        (TaskState::Committed, "committed"),
        (TaskState::Done, "done"),
        (TaskState::HumanRequired, "human_required"),
        (TaskState::Failed, "failed"),
    ];
    for (state, wire) in states {
        assert_eq!(serde_json::to_value(state).unwrap(), json!(wire));
    }
}

/// The `dependsOn` retro-apply: today's supervisor journals workflow.started
/// before task.created, and the node list must reach tasks created later —
/// plus tolerate the current payload shape where `nodes` is just a count.
#[test]
fn depends_on_applies_retroactively_and_tolerates_count_nodes() {
    let run = Uuid::now_v7();
    let task = Uuid::now_v7();

    // Node list present, tasks created after.
    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        Event::new(EventType::WorkflowStarted)
            .with_run_id(run)
            .with_payload(json!({
                "workflowId": "wf",
                "nodes": [{ "id": "n1", "dependsOn": ["n0"] }, { "id": "n2", "dependsOn": ["n1"] }]
            })),
        Event::new(EventType::TaskCreated)
            .with_run_id(run)
            .with_task_id(task)
            .with_payload(json!({ "node": "n2" })),
    ]);
    let projection = fold(&journal);
    assert_eq!(projection.tasks(None)[0].depends_on, vec!["n1".to_owned()]);

    // Current supervisor shape: nodes is a count — no deps, no error.
    let journal = journal_of(vec![
        Event::new(EventType::RunCreated).with_run_id(run),
        Event::new(EventType::WorkflowStarted)
            .with_run_id(run)
            .with_payload(json!({ "workflowId": "wf", "version": 1, "nodes": 5 })),
        Event::new(EventType::TaskCreated)
            .with_run_id(run)
            .with_task_id(task)
            .with_payload(json!({ "node": "n9" })),
    ]);
    let projection = fold(&journal);
    assert!(projection.tasks(None)[0].depends_on.is_empty());
}
