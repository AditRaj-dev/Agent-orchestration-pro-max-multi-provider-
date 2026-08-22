//! GIT-03 integration: agent ledger round-trip in both query directions
//! (commit → entry, task → entries).

mod common;

use std::collections::BTreeMap;

use agentos_git::ledger::{AgentLedger, LedgerEntry};

fn entry(commit_sha: &str, task_id: &str) -> LedgerEntry {
    let mut context_versions = BTreeMap::new();
    context_versions.insert("prompts".to_string(), "v3".to_string());
    context_versions.insert("repo_snapshot".to_string(), "sha-abc".to_string());
    LedgerEntry {
        commit_sha: commit_sha.to_string(),
        task_id: task_id.to_string(),
        agent_instance: "claude-code/opus-5#s1".to_string(),
        orchestrator: "mastermind".to_string(),
        reviewers: vec!["sonnet-review-1".to_string(), "human-reviewer".to_string()],
        context_versions,
        workflow_id: "wf-42".to_string(),
    }
}

#[test]
fn ledger_round_trip_by_commit_and_by_task() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ledger = AgentLedger::open(&dir.path().join("ledger.sqlite3")).expect("open ledger");

    let first = ledger.record(&entry("aa1111", "task-1")).expect("record");
    let second = ledger.record(&entry("bb2222", "task-1")).expect("record");
    ledger.record(&entry("cc3333", "task-2")).expect("record");

    // commit -> entry, all attribution fields intact
    let by_commit = ledger.by_commit("aa1111").expect("query").expect("present");
    assert_eq!(by_commit, first);
    assert_eq!(by_commit.entry.task_id, "task-1");
    assert_eq!(
        by_commit.entry.reviewers,
        vec!["sonnet-review-1".to_string(), "human-reviewer".to_string()]
    );
    assert_eq!(
        by_commit.entry.context_versions.get("repo_snapshot"),
        Some(&"sha-abc".to_string())
    );
    assert_eq!(by_commit.entry.workflow_id, "wf-42");
    assert!(!by_commit.recorded_at.is_empty());

    // task -> entries, oldest first, task-scoped
    let by_task = ledger.by_task("task-1").expect("query");
    assert_eq!(by_task.len(), 2);
    assert_eq!(by_task[0].entry.commit_sha, "aa1111");
    assert_eq!(by_task[1].entry.commit_sha, "bb2222");
    assert_eq!(by_task[1], second);
    assert_eq!(ledger.by_task("task-2").expect("query").len(), 1);

    // misses are clean
    assert!(ledger.by_commit("dead0000").expect("query").is_none());
    assert!(ledger.by_task("no-such-task").expect("query").is_empty());
}

#[test]
fn ledger_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("ledger.sqlite3");

    let ledger = AgentLedger::open(&db_path).expect("open ledger");
    let recorded = ledger.record(&entry("aa1111", "task-1")).expect("record");
    drop(ledger);

    let reopened = AgentLedger::open(&db_path).expect("reopen ledger");
    assert_eq!(
        reopened
            .by_commit("aa1111")
            .expect("query")
            .expect("present"),
        recorded
    );
}
