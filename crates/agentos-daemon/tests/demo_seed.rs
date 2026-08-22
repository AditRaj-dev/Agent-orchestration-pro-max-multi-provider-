//! F-11 §3.4 demo-seed obligations: refuse the default journal path, refuse
//! a non-empty journal, refuse broken fixtures — and seed the *real* frozen
//! fixture (`fixtures/demo-run.json`) into a temp journal, then fold it as
//! the desktop app would.

use std::path::{Path, PathBuf};

use agentos_core::{Event, EventType};
use agentos_daemon::{db, events, projection, seed, seed::SeedError};
use serde_json::json;

/// Path of the frozen F-07 happy-path fixture this repo ships.
fn demo_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("demo-run.json")
}

/// A two-event fixture written to a temp file (the happy path is exercised
/// against the real fixture below).
fn mini_fixture(dir: &tempfile::TempDir) -> PathBuf {
    let path = dir.path().join("mini.json");
    let fixture = json!([
        {
            "id": "019250ab-1e70-7c9a-9a1e-4f2b6c8d0001",
            "eventType": "run.created",
            "occurredAt": "2026-08-22T09:15:00Z",
            "runId": "019250ab-1e50-7c9a-9a1e-4f2b6c8d00a1",
            "payload": { "goal": "mini demo", "demo": true }
        },
        {
            "id": "019250ab-1e70-7c9a-9a1e-4f2b6c8d0002",
            "eventType": "task.created",
            "occurredAt": "2026-08-22T09:15:01Z",
            "runId": "019250ab-1e50-7c9a-9a1e-4f2b6c8d00a1",
            "taskId": "019250ab-1e60-7c9a-9a1e-4f2b6c8d0101",
            "payload": { "node": "n1", "nodeType": "run" }
        }
    ]);
    std::fs::write(&path, fixture.to_string()).expect("write mini fixture");
    path
}

#[test]
fn refuses_the_default_journal_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fixture = mini_fixture(&dir);

    // Direct hit...
    let err = seed::seed_demo(&db::default_journal_path(), &fixture).unwrap_err();
    assert!(matches!(err, SeedError::DefaultJournalPath { .. }));
    // ...and the Windows-mixture spelling of the same path.
    let twisted = db::default_journal_path().to_string_lossy().to_lowercase();
    let err = seed::seed_demo(Path::new(&twisted), &fixture).unwrap_err();
    assert!(matches!(err, SeedError::DefaultJournalPath { .. }));
}

#[test]
fn refuses_a_journal_that_already_has_events() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("seeded.db");
    let conn = db::open_db(&db_path).expect("open");
    events::append_event(&conn, &Event::new(EventType::RunCreated)).expect("one real event");

    let err = seed::seed_demo(&db_path, &mini_fixture(&dir)).unwrap_err();
    match err {
        SeedError::JournalNotEmpty { event_count, .. } => assert_eq!(event_count, 1),
        other => panic!("expected JournalNotEmpty, got {other:?}"),
    }
}

#[test]
fn refuses_broken_fixtures() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("fresh.db");

    let not_json = dir.path().join("not-json.json");
    std::fs::write(&not_json, "{ nope").unwrap();
    assert!(matches!(
        seed::seed_demo(&db_path, &not_json).unwrap_err(),
        SeedError::FixtureInvalid { .. }
    ));

    let not_array = dir.path().join("not-array.json");
    std::fs::write(&not_array, r#"{ "events": [] }"#).unwrap();
    assert!(matches!(
        seed::seed_demo(&db_path, &not_array).unwrap_err(),
        SeedError::FixtureInvalid { .. }
    ));

    assert!(matches!(
        seed::seed_demo(&db_path, &dir.path().join("missing.json")).unwrap_err(),
        SeedError::Io(_)
    ));
}

#[test]
fn seeds_a_fresh_journal_and_refuses_a_reseed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("demo.db");

    let report = seed::seed_demo(&db_path, &mini_fixture(&dir)).expect("seed");
    assert_eq!(report.appended, 2);

    let conn = db::open_db(&db_path).expect("reopen");
    let journal = events::tail_with_seq(&conn, 0, u32::MAX).expect("read back");
    assert_eq!(journal.len(), 2);
    assert_eq!(journal[0].seq, 1);
    // The second fixture payload forgot "demo" — the seeder stamps it.
    assert_eq!(journal[1].event.payload["demo"], json!(true));
    assert_eq!(journal[1].event.payload["node"], json!("n1"));

    // Second seed into the now-non-empty journal is refused.
    assert!(matches!(
        seed::seed_demo(&db_path, &mini_fixture(&dir)).unwrap_err(),
        SeedError::JournalNotEmpty { .. }
    ));
}

/// The real frozen corpus: seeds 51 events and folds exactly what the
/// desktop app should render for the F-07 happy path.
#[test]
fn the_frozen_fixture_seeds_and_folds_like_a_real_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("demo-run.db");

    let report = seed::seed_demo(&db_path, &demo_fixture()).expect("seed the frozen fixture");
    assert_eq!(report.appended, 51);

    let conn = db::open_db(&db_path).expect("reopen");
    let journal = events::tail_with_seq(&conn, 0, u32::MAX).expect("read journal");
    assert_eq!(journal.len(), 51);
    assert_eq!(events::journal_stats(&conn).unwrap().last_seq, 51);
    // External-writer canon: every event belongs to exactly one run.
    assert!(journal
        .iter()
        .all(|sequenced| sequenced.event.run_id.is_some()));
    assert!(journal
        .iter()
        .all(|sequenced| sequenced.event.payload.get("demo") == Some(&json!(true))));

    let projection = projection::fold(&journal);

    // Run: completed with all five tasks done.
    let runs = projection.runs();
    assert_eq!(runs.len(), 1);
    let run = runs[0];
    assert_eq!(run.status, projection::RunStatus::Completed);
    assert_eq!(run.workflow_id.as_deref(), Some("demo-e2e-happy-path"));
    assert_eq!(run.task_counts.total, 5);
    assert_eq!(run.task_counts.done, 5);
    assert_eq!(run.task_counts.failed, 0);
    assert_eq!(run.task_counts.active, 0);
    assert_eq!(run.event_count, 51);
    assert_eq!(run.first_seq, 1);
    assert_eq!(run.last_seq, 51);

    // Tasks: commit carries the sha; deps mirror the workflow node list.
    let tasks = projection.tasks(None);
    assert_eq!(tasks.len(), 5);
    let commit = tasks
        .iter()
        .find(|task| task.node_id.as_deref() == Some("commit"))
        .expect("commit task");
    assert_eq!(commit.state, agentos_core::TaskState::Done);
    assert_eq!(
        commit.commit_sha.as_deref(),
        Some("e4a9c31b7f2d5806a91c4e7bd20f86a3d5c1b942")
    );
    assert_eq!(commit.depends_on, vec!["review".to_owned()]);
    let build_a = tasks
        .iter()
        .find(|task| task.node_id.as_deref() == Some("build-a"))
        .expect("build-a task");
    assert_eq!(build_a.depends_on, vec!["spec".to_owned()]);
    assert_eq!(build_a.state, agentos_core::TaskState::Done);

    // Agents: the realistic adapter ids, claude cost canon, codex/agy
    // tokens-only, everything complete after run.completed.
    let agents = projection.agents(None);
    let by_id = |id: &str| {
        agents
            .iter()
            .find(|agent| agent.agent_id == id)
            .unwrap_or_else(|| panic!("missing agent {id}"))
    };
    assert_eq!(agents.len(), 5);
    let claude = by_id("claude-code");
    assert_eq!(claude.provider.as_deref(), Some("anthropic"));
    assert_eq!(claude.model.as_deref(), Some("claude-opus-5"));
    assert_eq!(claude.status, projection::AgentStatus::Complete);
    assert_eq!(claude.usage.cost_usd, 0.4132);
    assert_eq!(claude.usage.tokens_estimate, 140270);
    let codex = by_id("codex");
    assert_eq!(
        codex.usage.cost_usd, 0.0,
        "codex reports tokens only (F-00 §4)"
    );
    assert_eq!(codex.usage.tokens_estimate, 43940 + 10440);
    let agy = by_id("agy");
    assert_eq!(agy.usage.tokens_estimate, 56410);
    let reviewer = by_id("stub-reviewer@f-07");
    assert_eq!(reviewer.status, projection::AgentStatus::Complete);
    assert_eq!(reviewer.event_count, 4);
    assert!(by_id("git_push").event_count >= 5);
}
