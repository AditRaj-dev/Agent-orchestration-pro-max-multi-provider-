//! F-01 integration smoke test: append events across two runs, reopen the
//! database file with a fresh connection (exercising the read-first
//! journal-mode canon on an existing WAL database), and verify ordering,
//! full round-trip field equality, and that the append-only triggers reject
//! UPDATE and DELETE.

use std::time::Duration;

use agentos_core::{CoreError, Event, EventType};
use agentos_daemon::{db, events};
use serde_json::json;
use uuid::Uuid;

/// Three events across two runs: named types plus one `Other("custom.x")`
/// carrying a nested payload, and one with an offloaded (ref + hash) payload.
fn sample_events() -> Vec<Event> {
    let run_a = Uuid::now_v7();
    let run_b = Uuid::now_v7();
    vec![
        Event::new(EventType::RunCreated)
            .with_run_id(run_a)
            .with_trace_id(Uuid::now_v7())
            .with_payload(json!({ "goal": "ship F-01" })),
        Event::new(EventType::Other("custom.x".to_owned()))
            .with_run_id(run_a)
            .with_trace_id(Uuid::now_v7())
            .with_task_id(Uuid::now_v7())
            .with_agent_id("worker-07")
            .with_payload(json!({ "nested": { "items": [1, 2, 3], "ok": true } })),
        Event::new(EventType::TaskDone)
            .with_run_id(run_b)
            .with_trace_id(Uuid::now_v7())
            .with_agent_id("mock-adapter")
            .with_payload_ref("sha256:9f2cfe1a")
            .with_payload_hash("sha256:9f2cfe1a"),
    ]
}

#[test]
fn events_round_trip_across_reopened_database() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("daemon.db");

    let originals = sample_events();
    {
        let conn = db::open_db(&db_path).expect("first open_db");
        let seqs: Vec<i64> = originals
            .iter()
            .map(|event| events::append_event(&conn, event).expect("append"))
            .collect();
        // Append-only journal: seqs are strictly increasing; on a fresh
        // database AUTOINCREMENT starts at 1.
        assert_eq!(seqs, vec![1, 2, 3]);
    } // drop the first connection

    // Reopen the same file with a new connection: proves the journal-mode
    // canon path (busy_timeout, then READ journal_mode -> already wal, no
    // unconditional pragma re-issue) and that migrations are idempotent.
    let conn = db::open_db(&db_path).expect("reopen open_db");
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("read journal_mode");
    assert_eq!(mode, "wal");
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read user_version");
    assert_eq!(version, 1);

    // Per-run reads: only that run's events, oldest first, every field equal.
    let run_a = originals[0].run_id.expect("run_a set");
    let from_run_a = events::events_for_run(&conn, &run_a).expect("events_for_run a");
    assert_eq!(from_run_a.len(), 2);
    assert_eq!(from_run_a[0], originals[0]);
    assert_eq!(from_run_a[1], originals[1]);

    let run_b = originals[2].run_id.expect("run_b set");
    let from_run_b = events::events_for_run(&conn, &run_b).expect("events_for_run b");
    assert_eq!(from_run_b, vec![originals[2].clone()]);

    // Tail feed: full journal in seq order (full round-trip equality of
    // every field, including the Other("custom.x") type and the offloaded
    // payload ref/hash), plus windowed paging.
    let all = events::tail(&conn, 0, 100).expect("tail all");
    assert_eq!(all, originals);
    let page = events::tail(&conn, 1, 1).expect("tail page");
    assert_eq!(page, vec![originals[1].clone()]);

    // Append-only enforcement at the storage layer: UPDATE and DELETE are
    // aborted by the triggers, and the journal is left untouched.
    let update = conn.execute("UPDATE events SET payload = '{}' WHERE seq = 1", []);
    let update_err = update.expect_err("UPDATE must be rejected");
    assert!(
        update_err.to_string().contains("append-only"),
        "unexpected UPDATE error: {update_err}"
    );

    let delete = conn.execute("DELETE FROM events WHERE seq = 1", []);
    let delete_err = delete.expect_err("DELETE must be rejected");
    assert!(
        delete_err.to_string().contains("append-only"),
        "unexpected DELETE error: {delete_err}"
    );

    let after = events::tail(&conn, 0, 100).expect("tail after mutations");
    assert_eq!(after, originals);
}

#[test]
fn busy_maps_to_retryable_core_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("daemon.db");

    // Hold the WAL write lock on one connection...
    let writer = db::open_db(&db_path).expect("writer open_db");
    writer
        .execute("BEGIN IMMEDIATE", [])
        .expect("writer BEGIN IMMEDIATE");

    // ...and shorten the contender's busy timeout so the test fails fast
    // instead of waiting out the default 5000ms.
    let contender = db::open_db(&db_path).expect("contender open_db");
    contender
        .busy_timeout(Duration::from_millis(100))
        .expect("short busy_timeout");

    let err = events::append_event(&contender, &Event::new(EventType::TaskCreated))
        .expect_err("append under held write lock must surface busy");
    assert_eq!(err, CoreError::SqliteBusy);
    assert!(err.is_retryable());

    // Release the lock; the retry of the identical append now succeeds —
    // SQLITE_BUSY is transient by construction.
    writer.execute("COMMIT", []).expect("writer COMMIT");
    let seq = events::append_event(&contender, &Event::new(EventType::TaskCreated))
        .expect("retry after lock release");
    assert_eq!(seq, 1);
}
