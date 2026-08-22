//! F-11a e2e (§5 obligations): in-process server on an ephemeral loopback
//! port + tokio-tungstenite client, against a temp SQLite journal. The
//! "external writer" is a second direct connection to the same journal
//! file — the supported write path until the supervisor moves in-process
//! (F-11 §3.2) — proving WAL readers see its appends live.
//!
//! Multi-thread runtimes throughout: the slow-consumer test appends
//! thousands of events synchronously and must not starve the server task.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentos_agents::AgentRegistry;
use agentos_core::{Event, EventType};
use agentos_daemon::agent_sessions::{AdapterSet, AgentSessions};
use agentos_daemon::{db, events, server};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

/// Every read has a deadline so a silent bug fails the test instead of
/// hanging the suite.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Server fixture: temp journal (seeded), agent registry (built-ins
/// seeded), mock-backed chat sessions, bound WS server, shutdown switch.
struct TestServer {
    _dir: tempfile::TempDir,
    journal_path: PathBuf,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), server::ServerError>>,
    addr: SocketAddr,
}

async fn spawn_server(seeded: Vec<Event>) -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_path = dir.path().join("journal.db");
    let conn = db::open_db(&journal_path).expect("open journal");
    for event in &seeded {
        events::append_event(&conn, event).expect("seed append");
    }
    let registry = Arc::new(AgentRegistry::open(&dir.path().join("agents.db")).expect("registry"));
    registry.seed_builtins().expect("seed built-ins");
    let sessions = Arc::new(AgentSessions::new(
        Arc::clone(&registry),
        AdapterSet::wired(),
        journal_path.clone(),
        dir.path().to_path_buf(),
    ));
    let bound = server::WsServer::new(
        Arc::new(Mutex::new(conn)),
        journal_path.clone(),
        registry,
        sessions,
    )
    .bind("127.0.0.1:0")
    .expect("bind ephemeral loopback port");
    let addr = bound.local_addr();
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(bound.serve(shutdown_rx));
    TestServer {
        _dir: dir,
        journal_path,
        shutdown,
        task,
        addr,
    }
}

/// An unnamed marker event — the fold treats it as unknown, which is all
/// the subscribe/list paths need from it.
fn marker_event(run: Uuid, n: u64) -> Event {
    Event::new(EventType::Other(format!("demo.marker.{n}")))
        .with_run_id(run)
        .with_trace_id(Uuid::now_v7())
        .with_payload(json!({ "n": n }))
}

struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

async fn connect(addr: SocketAddr) -> Client {
    let (ws, _response) = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(format!("ws://{addr}/")),
    )
    .await
    .expect("connect timeout")
    .expect("connect_async");
    Client { ws }
}

impl Client {
    async fn send(&mut self, value: &Value) {
        self.ws
            .send(Message::Text(value.to_string().into()))
            .await
            .expect("send frame");
    }

    async fn send_raw(&mut self, text: &str) {
        self.ws
            .send(Message::Text(text.to_string().into()))
            .await
            .expect("send raw frame");
    }

    /// Next JSON frame; panics on close (use [`Client::next_message`] when
    /// a close is expected).
    async fn next_frame(&mut self) -> Value {
        match self.next_message().await {
            Message::Text(text) => serde_json::from_str(text.as_str()).expect("frame is JSON"),
            other => panic!("expected a text frame, got {other:?}"),
        }
    }

    /// Next raw WS message (deadline-bounded).
    async fn next_message(&mut self) -> Message {
        tokio::time::timeout(READ_TIMEOUT, self.ws.next())
            .await
            .expect("read timeout")
            .expect("stream ended")
            .expect("read error")
    }

    /// Send a request and wait for the response with the matching id,
    /// skipping any notifications that interleave (§2 concurrency).
    async fn request(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.send(&json!({ "id": id, "method": method, "params": params }))
            .await;
        loop {
            let frame = self.next_frame().await;
            if frame.get("id").and_then(Value::as_u64) == Some(id) {
                return frame;
            }
        }
    }

    /// Read frames until the matching `subscription.closed` arrives.
    async fn wait_closed(&mut self, subscription_id: &str) -> Value {
        loop {
            let frame = self.next_frame().await;
            if frame["notification"] == json!("subscription.closed")
                && frame["subscriptionId"] == json!(subscription_id)
            {
                return frame;
            }
        }
    }
}

/// `ping` and `daemon.info` round-trips (§3.2), with the identity fields
/// checked against the actual process and journal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_and_daemon_info_roundtrip() {
    let run = Uuid::now_v7();
    let srv = spawn_server(vec![marker_event(run, 1), marker_event(run, 2)]).await;
    let mut client = connect(srv.addr).await;

    let pong = client.request(1, "ping", json!({})).await;
    assert_eq!(pong["id"], json!(1));
    assert_eq!(pong["ok"], json!(true));
    assert_eq!(pong["result"]["pong"], json!(true));
    let server_time = pong["result"]["serverTime"].as_str().expect("serverTime");
    assert!(
        server_time.ends_with('Z'),
        "rfc3339 Z expected: {server_time}"
    );

    let info = client.request(2, "daemon.info", json!({})).await;
    assert_eq!(info["ok"], json!(true));
    assert_eq!(info["result"]["version"], json!(env!("CARGO_PKG_VERSION")));
    assert_eq!(info["result"]["pid"], json!(std::process::id()));
    assert_eq!(
        info["result"]["journalPath"],
        json!(srv.journal_path.display().to_string())
    );
    assert_eq!(info["result"]["eventCount"], json!(2));
    assert_eq!(info["result"]["lastSeq"], json!(2));
    assert!(info["result"]["startedAt"].as_str().unwrap().ends_with('Z'));

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// `events.list` pagination with exact `truncated` semantics (§3.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_list_pagination_and_truncated() {
    let run = Uuid::now_v7();
    let seeded: Vec<Event> = (1..=5).map(|n| marker_event(run, n)).collect();
    let srv = spawn_server(seeded).await;
    let mut client = connect(srv.addr).await;

    let first = list_page_helper(&mut client, 1, 0, 2).await;
    assert_eq!(first["ok"], json!(true));
    let events = first["result"]["events"].as_array().expect("events");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["seq"], json!(1));
    assert_eq!(events[0]["eventType"], json!("demo.marker.1"));
    assert_eq!(events[1]["seq"], json!(2));
    assert_eq!(first["result"]["lastSeq"], json!(5));
    assert_eq!(first["result"]["truncated"], json!(true));

    let middle = list_page_helper(&mut client, 2, 2, 2).await;
    assert_eq!(middle["result"]["events"].as_array().unwrap().len(), 2);
    assert_eq!(middle["result"]["truncated"], json!(true));

    let last = list_page_helper(&mut client, 3, 4, 2).await;
    let events = last["result"]["events"].as_array().expect("events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["seq"], json!(5));
    assert_eq!(last["result"]["truncated"], json!(false));

    // §3.1 shape on every event: core serde fields plus seq.
    for key in [
        "id",
        "eventType",
        "occurredAt",
        "runId",
        "traceId",
        "payload",
        "schemaVersion",
    ] {
        assert!(events[0].get(key).is_some(), "missing {key}");
    }

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// The error ladder (§2.2): unknown method, malformed frames (`id: null`),
/// the `git.diff` seam, and `events.list` parameter validation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_frames_match_the_contract() {
    let run = Uuid::now_v7();
    let srv = spawn_server(vec![marker_event(run, 1)]).await;
    let mut client = connect(srv.addr).await;

    // Unknown method echoes the id with method_not_found.
    let resp = client.request(1, "brain.transplant", json!({})).await;
    assert_eq!(resp["id"], json!(1));
    assert_eq!(resp["ok"], json!(false));
    assert_eq!(resp["error"]["code"], json!("method_not_found"));

    // Unparseable frame → invalid_request with id null (§2.2).
    client.send_raw("this is not json {").await;
    let resp = client.next_frame().await;
    assert_eq!(resp["id"], json!(null));
    assert_eq!(resp["error"]["code"], json!("invalid_request"));

    // Frame with no method → invalid_request, id null.
    client.send_raw(r#"{"id": 9}"#).await;
    let resp = client.next_frame().await;
    assert_eq!(resp["id"], json!(null));
    assert_eq!(resp["error"]["code"], json!("invalid_request"));

    // Params that is not an object → invalid_request.
    client
        .send_raw(r#"{"id": 10, "method": "ping", "params": 5}"#)
        .await;
    let resp = client.next_frame().await;
    assert_eq!(resp["error"]["code"], json!("invalid_request"));

    // git.diff is the documented v1 seam.
    let resp = client
        .request(
            2,
            "git.diff",
            json!({ "repo": ".", "base": "a", "head": "b" }),
        )
        .await;
    assert_eq!(resp["error"]["code"], json!("not_supported"));

    // limit > 1000 → invalid_params (the §2.2 example, verbatim ceiling).
    let resp = client
        .request(3, "events.list", json!({ "limit": 1001 }))
        .await;
    assert_eq!(resp["id"], json!(3));
    assert_eq!(resp["error"]["code"], json!("invalid_params"));
    assert!(resp["error"]["message"]
        .as_str()
        .expect("message")
        .contains("limit must be <= 1000"));

    // limit 0 and negative afterSeq are parameter errors too.
    let resp = client
        .request(4, "events.list", json!({ "limit": 0 }))
        .await;
    assert_eq!(resp["error"]["code"], json!("invalid_params"));
    let resp = client
        .request(5, "events.list", json!({ "afterSeq": -1 }))
        .await;
    assert_eq!(resp["error"]["code"], json!("invalid_params"));
    // Floats are not integers.
    let resp = client
        .request(6, "events.list", json!({ "afterSeq": 1.5 }))
        .await;
    assert_eq!(resp["error"]["code"], json!("invalid_params"));

    // The ceiling itself (1000) is legal; unknown params are ignored
    // (forward compatibility).
    let resp = client
        .request(
            7,
            "events.list",
            json!({ "limit": 1000, "futureParam": true }),
        )
        .await;
    assert_eq!(resp["ok"], json!(true));
    assert_eq!(resp["result"]["truncated"], json!(false));

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// The heart of §3.2: subscribe → replay in seq order → live tail fed by an
/// external second connection → resubscribe (at-least-once redelivery) →
/// unsubscribe (`stopped` + `subscription.closed` reason `unsubscribed`,
/// and `stopped:false` for an unknown id).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_replays_then_tails_external_writes() {
    let run = Uuid::now_v7();
    let seeded: Vec<Event> = (1..=3).map(|n| marker_event(run, n)).collect();
    let srv = spawn_server(seeded).await;
    let mut client = connect(srv.addr).await;

    // Subscribe: reply first, then replay of seq 1..=3 in order.
    let resp = client
        .request(1, "events.subscribe", json!({ "afterSeq": 0 }))
        .await;
    let sub = resp["result"]["subscriptionId"]
        .as_str()
        .expect("sub id")
        .to_owned();
    assert!(sub.starts_with("sub-"), "subscription id shape: {sub}");

    for seq in 1..=3i64 {
        let frame = client.next_frame().await;
        assert_eq!(frame["notification"], json!("event"), "frame: {frame}");
        assert_eq!(frame["subscriptionId"], json!(sub));
        assert_eq!(frame["seq"], json!(seq));
        assert_eq!(frame["event"]["seq"], json!(seq), "§3.1 event carries seq");
        assert_eq!(
            frame["event"]["eventType"],
            json!(format!("demo.marker.{seq}"))
        );
        assert_eq!(frame["event"]["runId"], json!(run.to_string()));
    }

    // External writer: a SECOND direct connection to the same journal —
    // appends must appear on the live tail within a few poll intervals.
    let external = db::open_db(&srv.journal_path).expect("external writer connection");
    for n in 4..=5 {
        events::append_event(&external, &marker_event(run, n)).expect("external append");
    }
    for seq in 4..=5i64 {
        let frame = client.next_frame().await;
        assert_eq!(frame["notification"], json!("event"), "frame: {frame}");
        assert_eq!(frame["seq"], json!(seq), "gapless: no seq skipped");
        assert_eq!(
            frame["event"]["eventType"],
            json!(format!("demo.marker.{seq}"))
        );
    }

    // Resubscribe from mid-journal: replay is at-least-once per seq (3..=5
    // again) and still strictly ordered.
    let resp = client
        .request(2, "events.subscribe", json!({ "afterSeq": 2 }))
        .await;
    let sub2 = resp["result"]["subscriptionId"]
        .as_str()
        .expect("sub2 id")
        .to_owned();
    assert_ne!(sub, sub2);
    for seq in 3..=5i64 {
        let frame = client.next_frame().await;
        assert_eq!(frame["subscriptionId"], json!(sub2));
        assert_eq!(frame["seq"], json!(seq));
    }

    // Unsubscribe the first: {stopped: true} then the closed notification.
    let resp = client
        .request(3, "events.unsubscribe", json!({ "subscriptionId": sub }))
        .await;
    assert_eq!(resp["result"]["stopped"], json!(true));
    let closed = client.wait_closed(&sub).await;
    assert_eq!(closed["reason"], json!("unsubscribed"));

    // Unknown id (and the just-unsubscribed one) report stopped: false.
    let resp = client
        .request(4, "events.unsubscribe", json!({ "subscriptionId": sub }))
        .await;
    assert_eq!(resp["result"]["stopped"], json!(false));
    let resp = client
        .request(
            5,
            "events.unsubscribe",
            json!({ "subscriptionId": "sub-999" }),
        )
        .await;
    assert_eq!(resp["result"]["stopped"], json!(false));

    // Missing subscriptionId → invalid_params.
    let resp = client.request(6, "events.unsubscribe", json!({})).await;
    assert_eq!(resp["error"]["code"], json!("invalid_params"));

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// `runs.list` / `tasks.list` / `agents.list` over the WS surface, checked
/// against a synthetic mid-flight run (one task done, one still parked at
/// created; agent running; no run.completed yet).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projections_served_over_ws() {
    let (run, journal) = mini_run();
    let srv = spawn_server(journal).await;
    let mut client = connect(srv.addr).await;

    let resp = client.request(1, "runs.list", json!({})).await;
    let runs = resp["result"]["runs"].as_array().expect("runs");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["runId"], json!(run.to_string()));
    assert_eq!(runs[0]["status"], json!("running"));
    assert_eq!(runs[0]["workflowId"], json!("wf-mini"));
    assert_eq!(runs[0]["taskCounts"]["total"], json!(2));
    assert_eq!(runs[0]["taskCounts"]["done"], json!(1));
    assert_eq!(runs[0]["taskCounts"]["active"], json!(1));

    let resp = client
        .request(2, "tasks.list", json!({ "runId": run.to_string() }))
        .await;
    let tasks = resp["result"]["tasks"].as_array().expect("tasks");
    assert_eq!(tasks.len(), 2);
    let done_task = tasks
        .iter()
        .find(|task| task["state"] == json!("done"))
        .expect("one done task");
    assert_eq!(
        done_task["commitSha"],
        json!("fedcba9876543210fedcba9876543210fedcba98")
    );

    // tasks.list with a bogus runId is a parameter error, not an empty list.
    let resp = client
        .request(3, "tasks.list", json!({ "runId": "not-a-uuid" }))
        .await;
    assert_eq!(resp["error"]["code"], json!("invalid_params"));

    let resp = client
        .request(4, "agents.list", json!({ "runId": run.to_string() }))
        .await;
    let agents = resp["result"]["agents"].as_array().expect("agents");
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["agentId"], json!("mock"));
    assert_eq!(agents[0]["status"], json!("running"));
    assert_eq!(agents[0]["model"], json!("mock-model"));
    assert_eq!(agents[0]["usage"]["costUsd"], json!(0.25));
    assert_eq!(agents[0]["usage"]["tokensEstimate"], json!(120));

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// §2.3 shutdown: `daemon.stopping` broadcast, then close 1001, and the
/// serve task drains and returns cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_broadcasts_stopping_and_closes_1001() {
    let run = Uuid::now_v7();
    let srv = spawn_server(vec![marker_event(run, 1)]).await;
    let mut client = connect(srv.addr).await;
    let pong = client.request(1, "ping", json!({})).await;
    assert_eq!(pong["ok"], json!(true));

    srv.shutdown.send(true).ok();

    let stopping = client.next_frame().await;
    assert_eq!(stopping["notification"], json!("daemon.stopping"));

    match client.next_message().await {
        Message::Close(Some(frame)) => {
            assert_eq!(u16::from(frame.code), 1001, "close code 1001 (Going Away)");
        }
        other => panic!("expected close frame, got {other:?}"),
    }

    srv.task.await.expect("serve task").expect("serve clean");
}

/// §2 slow-consumer path: a subscriber that stops reading while thousands
/// of events land gets `subscription.closed` reason `slow_consumer` — never
/// unbounded buffering, never a silent stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_subscriber_is_closed_not_stalled() {
    let run = Uuid::now_v7();
    let srv = spawn_server(vec![]).await;
    let mut client = connect(srv.addr).await;

    let resp = client
        .request(1, "events.subscribe", json!({ "afterSeq": 0 }))
        .await;
    let sub = resp["result"]["subscriptionId"]
        .as_str()
        .expect("sub id")
        .to_owned();

    // External writer floods the journal while the client reads nothing.
    let external = db::open_db(&srv.journal_path).expect("external writer");
    for n in 1..=3000u64 {
        events::append_event(&external, &marker_event(run, n)).expect("flood append");
    }

    // Now drain: a healthy burst arrives, terminated by the slow-consumer
    // close (the bounded outbound buffer plus socket backpressure is what
    // trips it — 3000 small events far exceed both).
    let mut seen_events = 0u64;
    let closed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let frame = client.next_frame().await;
            if frame["notification"] == json!("subscription.closed")
                && frame["subscriptionId"] == json!(sub)
            {
                return frame;
            }
            if frame["notification"] == json!("event") {
                seen_events += 1;
            }
        }
    })
    .await
    .expect("slow-consumer close within deadline");

    assert_eq!(closed["reason"], json!("slow_consumer"));
    assert!(
        seen_events >= 64,
        "expected a healthy burst before the close, saw {seen_events}"
    );

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// F-13 live e2e (opt-in, BILLABLE): a real chat with the seeded
/// researcher (agy → gemini-3.1-pro-high). Doubly gated: `#[ignore]` plus
/// `AGENTOS_AGY_E2E=1` — mirroring the adapter crates' live-probe gates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live agy session (billable): set AGENTOS_AGY_E2E=1 to run"]
async fn live_researcher_chat_e2e() {
    if std::env::var("AGENTOS_AGY_E2E").ok().as_deref() != Some("1") {
        return;
    }
    let srv = spawn_server(vec![]).await;
    let mut client = connect(srv.addr).await;

    let resp = client
        .request(
            1,
            "agent.session.start",
            json!({
                "agentId": "researcher",
                "message": "In one sentence: what is the Antigravity CLI? Reply in a single sentence."
            }),
        )
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");
    let session_id = resp["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    let finished = tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            let frame = client.next_frame().await;
            if frame["notification"] == json!("event")
                && frame["event"]["eventType"] == json!("session.finished")
                && frame["event"]["payload"]["sessionId"] == json!(session_id)
            {
                return frame;
            }
        }
    })
    .await
    .expect("researcher session.finished within 3 minutes");
    let reply = finished["event"]["payload"]["finalResult"]
        .as_str()
        .unwrap_or_default();
    assert!(!reply.trim().is_empty(), "researcher produced a reply");

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// `events.list` request with the pagination params filled in.
async fn list_page_helper(client: &mut Client, id: u64, after: i64, limit: i64) -> Value {
    client
        .request(
            id,
            "events.list",
            json!({ "afterSeq": after, "limit": limit }),
        )
        .await
}

/// F-13: registry CRUD over the WS surface — seeded built-ins visible,
/// create/update/delete round-trips, builtin delete refused, mutations
/// journaled as `agent.*` events, skills listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_crud_roundtrips_over_ws() {
    let srv = spawn_server(vec![]).await;
    let mut client = connect(srv.addr).await;

    // Built-ins are seeded and listed.
    let resp = client.request(1, "registry.agents.list", json!({})).await;
    assert_eq!(resp["ok"], json!(true));
    let agents = resp["result"]["agents"].as_array().expect("agents");
    let ids: Vec<&str> = agents
        .iter()
        .map(|a| a["id"].as_str().expect("id"))
        .collect();
    for expected in ["agent-creator", "orchestrator", "researcher"] {
        assert!(
            ids.contains(&expected),
            "built-in {expected} seeded: {ids:?}"
        );
    }

    // Skills list carries the built-in skills.
    let resp = client.request(2, "registry.skills.list", json!({})).await;
    let skills = resp["result"]["skills"].as_array().expect("skills");
    assert!(skills
        .iter()
        .any(|s| s["id"] == json!("mastermind-commands")));

    // Create with the minimal wire shape (server-owned fields default).
    let resp = client
        .request(
            3,
            "registry.agents.create",
            json!({ "agent": {
                "id": "sql-reviewer", "name": "SQL Reviewer",
                "description": "reviews migrations",
                "adapterId": "claude-code", "model": "claude-sonnet-5",
                "skills": ["tech-research"]
            }}),
        )
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");
    assert_eq!(
        resp["result"]["agent"]["mode"],
        json!("plan"),
        "safe default"
    );
    assert_eq!(resp["result"]["agent"]["builtin"], json!(false));

    // Wire cannot forge builtin.
    let resp = client
        .request(
            4,
            "registry.agents.create",
            json!({ "agent": {
                "id": "fake-builtin", "name": "Fake", "description": "x",
                "adapterId": "mock", "builtin": true
            }}),
        )
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");
    assert_eq!(resp["result"]["agent"]["builtin"], json!(false));

    // Update: rename the created agent.
    let resp = client
        .request(
            5,
            "registry.agents.update",
            json!({ "agent": {
                "id": "sql-reviewer", "name": "SQL Reviewer (senior)",
                "description": "reviews migrations", "adapterId": "claude-code"
            }}),
        )
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");
    assert_eq!(
        resp["result"]["agent"]["name"],
        json!("SQL Reviewer (senior)")
    );

    // Builtin delete is refused; user delete works and both show up in the
    // journal as `agent.*` events.
    let resp = client
        .request(6, "registry.agents.delete", json!({ "id": "researcher" }))
        .await;
    assert_eq!(resp["ok"], json!(false));
    assert_eq!(resp["error"]["code"], json!("invalid_params"));

    let resp = client
        .request(7, "registry.agents.delete", json!({ "id": "fake-builtin" }))
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");

    let conn = db::open_db(&srv.journal_path).expect("journal");
    let batch = events::tail_with_seq(&conn, 0, u32::MAX).expect("tail");
    let mut types = std::collections::BTreeSet::new();
    for sequenced in &batch {
        types.insert(sequenced.event.event_type.to_string());
    }
    assert!(types.contains("agent.created"), "{types:?}");
    assert!(types.contains("agent.updated"), "{types:?}");
    assert!(types.contains("agent.deleted"), "{types:?}");

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// F-13: `registry.catalog` shape — three providers, free probes only,
/// claude/mock static model lists, no auth requirement to enumerate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_catalog_lists_providers_and_models() {
    let srv = spawn_server(vec![]).await;
    let mut client = connect(srv.addr).await;

    let resp = client.request(1, "registry.catalog", json!({})).await;
    assert_eq!(resp["ok"], json!(true), "{resp}");
    let providers = resp["result"]["providers"].as_array().expect("providers");
    let ids: Vec<&str> = providers
        .iter()
        .map(|p| p["id"].as_str().expect("provider id"))
        .collect();
    assert!(ids.contains(&"claude-code"), "{ids:?}");
    assert!(ids.contains(&"antigravity-agy"), "{ids:?}");
    assert!(ids.contains(&"mock"), "{ids:?}");

    let claude = providers
        .iter()
        .find(|p| p["id"] == json!("claude-code"))
        .expect("claude entry");
    let models = claude["models"].as_array().expect("claude models");
    assert!(
        models.iter().any(|m| m["id"] == json!("claude-opus-5")),
        "{models:?}"
    );

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// F-13: a mock-adapter chat session over the WS API — start against a
/// mock-backed agent, watch `session.*` events arrive through the normal
/// subscription path, and confirm the terminal cleanup (sends refuse).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_session_over_ws_journals_and_terminates() {
    let srv = spawn_server(vec![]).await;
    let mut client = connect(srv.addr).await;

    // A mock-backed agent to chat with (credential-free).
    let resp = client
        .request(
            1,
            "registry.agents.create",
            json!({ "agent": {
                "id": "chatty", "name": "Chatty", "description": "mock chat",
                "adapterId": "mock", "model": "mock-model-1", "skills": ["tech-research"]
            }}),
        )
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");

    // Subscribe first so the chat events stream to us live.
    let resp = client
        .request(2, "events.subscribe", json!({ "afterSeq": 0 }))
        .await;
    let _sub = resp["result"]["subscriptionId"]
        .as_str()
        .expect("sub")
        .to_owned();

    let resp = client
        .request(
            3,
            "agent.session.start",
            json!({ "agentId": "chatty", "message": "hello there" }),
        )
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");
    let session_id = resp["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();
    assert!(session_id.starts_with("chat-"), "{session_id}");

    // Drain notifications until session.finished for our id (bounded).
    let finished = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let frame = client.next_frame().await;
            if frame["notification"] == json!("event")
                && frame["event"]["eventType"] == json!("session.finished")
                && frame["event"]["payload"]["sessionId"] == json!(session_id)
            {
                return frame;
            }
        }
    })
    .await
    .expect("session.finished within deadline");
    assert_eq!(
        finished["event"]["agentId"],
        json!("chatty"),
        "events carry the registry agent id"
    );

    // Finished sessions refuse follow-ups with invalid_params.
    let resp = client
        .request(
            4,
            "agent.session.send",
            json!({ "sessionId": session_id, "message": "again" }),
        )
        .await;
    assert_eq!(resp["ok"], json!(false));
    assert_eq!(resp["error"]["code"], json!("invalid_params"));

    // Unknown agents / empty messages are parameter errors.
    let resp = client
        .request(
            5,
            "agent.session.start",
            json!({ "agentId": "ghost", "message": "hi" }),
        )
        .await;
    assert_eq!(resp["error"]["code"], json!("invalid_params"));
    let resp = client
        .request(
            6,
            "agent.session.start",
            json!({ "agentId": "chatty", "message": "" }),
        )
        .await;
    assert_eq!(resp["error"]["code"], json!("invalid_params"));

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// A small synthetic journal: task `a` walks to `done` (with a commit sha),
/// task `b` never leaves `created`, agent `mock` is mid-run.
fn mini_run() -> (Uuid, Vec<Event>) {
    let run = Uuid::now_v7();
    let trace = Uuid::now_v7();
    let task_a = Uuid::now_v7();
    let task_b = Uuid::now_v7();
    let ev = |event_type: EventType| Event::new(event_type).with_run_id(run).with_trace_id(trace);
    (
        run,
        vec![
            ev(EventType::RunCreated).with_payload(json!({ "goal": "mini" })),
            ev(EventType::WorkflowStarted).with_payload(json!({
                "workflowId": "wf-mini",
                "nodes": [
                    { "id": "a", "dependsOn": [] },
                    { "id": "b", "dependsOn": ["a"] }
                ]
            })),
            ev(EventType::TaskCreated)
                .with_task_id(task_a)
                .with_payload(json!({ "node": "a" })),
            ev(EventType::TaskCreated)
                .with_task_id(task_b)
                .with_payload(json!({ "node": "b" })),
            ev(EventType::TaskReady).with_task_id(task_a),
            ev(EventType::AgentLeased)
                .with_task_id(task_a)
                .with_agent_id("mock"),
            ev(EventType::TaskRunning)
                .with_task_id(task_a)
                .with_agent_id("mock"),
            ev(EventType::Other("session.spawn".to_owned()))
                .with_task_id(task_a)
                .with_agent_id("mock")
                .with_payload(json!({ "provider": "mock-inc" })),
            ev(EventType::Other("session.started".to_owned()))
                .with_task_id(task_a)
                .with_agent_id("mock")
                .with_payload(json!({ "model": "mock-model" })),
            ev(EventType::Other("usage.updated".to_owned()))
                .with_task_id(task_a)
                .with_agent_id("mock")
                .with_payload(json!({ "costUsd": 0.25, "totalTokens": 120 })),
            ev(EventType::TaskOutputReady).with_task_id(task_a),
            ev(EventType::ReviewRequested).with_task_id(task_a),
            ev(EventType::ReviewApproved).with_task_id(task_a),
            ev(EventType::GitQueued).with_task_id(task_a),
            ev(EventType::GitCommitted)
                .with_task_id(task_a)
                .with_payload(json!({ "sha": "fedcba9876543210fedcba9876543210fedcba98" })),
            ev(EventType::TaskDone).with_task_id(task_a),
        ],
    )
}
