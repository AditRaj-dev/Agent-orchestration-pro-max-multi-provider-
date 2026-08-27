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

use agentos_adapters::mock::{MockAdapter, MockBehavior};
use agentos_adapters::RuntimeAdapter;
use agentos_agents::AgentRegistry;
use agentos_core::{Event, EventType};
use agentos_daemon::agent_sessions::{AdapterSet, AgentSessions};
use agentos_daemon::mastermind::Mastermind;
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
    dir: tempfile::TempDir,
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
    registry.seed_builtin_skills().expect("seed skills");
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
        dir,
        journal_path,
        shutdown,
        task,
        addr,
    }
}

/// A server whose Mastermind planner deliberately keeps one RPC in flight
/// long enough to prove the same socket remains responsive to heartbeats.
async fn spawn_slow_mastermind_server() -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_path = dir.path().join("journal.db");
    let conn = db::open_db(&journal_path).expect("open journal");
    let agents_db = dir.path().join("agents.db");
    let registry = Arc::new(AgentRegistry::open(&agents_db).expect("registry"));
    registry.seed_builtin_skills().expect("seed skills");
    registry.seed_builtins().expect("seed built-ins");
    let sessions = Arc::new(AgentSessions::new(
        Arc::clone(&registry),
        AdapterSet::wired(),
        journal_path.clone(),
        dir.path().to_path_buf(),
    ));
    let slow_adapter = Arc::new(MockAdapter::new(MockBehavior::Interview { questions: 1 }));
    let mastermind_adapter = Arc::clone(&slow_adapter);
    let mastermind = Arc::new(
        Mastermind::new(Arc::clone(&registry), &agents_db, dir.path(), move |_| {
            Some(Arc::clone(&mastermind_adapter) as Arc<dyn RuntimeAdapter>)
        })
        .with_memex_db(dir.path().join("memex.db")),
    );
    let bound = server::WsServer::new(
        Arc::new(Mutex::new(conn)),
        journal_path.clone(),
        registry,
        sessions,
    )
    .with_mastermind(mastermind)
    .bind("127.0.0.1:0")
    .expect("bind ephemeral loopback port");
    let addr = bound.local_addr();
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(bound.serve(shutdown_rx));
    TestServer {
        dir,
        journal_path,
        shutdown,
        task,
        addr,
    }
}

/// A credential-free Mastermind server used to exercise the phase write
/// authorization RPC and its persistent gate over the real WebSocket API.
async fn spawn_mastermind_write_gate_server() -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal_path = dir.path().join("journal.db");
    let conn = db::open_db(&journal_path).expect("open journal");
    let agents_db = dir.path().join("agents.db");
    let registry = Arc::new(AgentRegistry::open(&agents_db).expect("registry"));
    registry.seed_builtin_skills().expect("seed skills");
    registry.seed_builtins().expect("seed built-ins");
    let sessions = Arc::new(AgentSessions::new(
        Arc::clone(&registry),
        AdapterSet::wired(),
        journal_path.clone(),
        dir.path().to_path_buf(),
    ));
    let adapter = Arc::new(MockAdapter::new(MockBehavior::Success {
        turns: 1,
        files_changed: vec![],
    }));
    let mastermind_adapter = Arc::clone(&adapter);
    let mastermind = Arc::new(
        Mastermind::new(Arc::clone(&registry), &agents_db, dir.path(), move |_| {
            Some(Arc::clone(&mastermind_adapter) as Arc<dyn RuntimeAdapter>)
        })
        .with_memex_db(dir.path().join("memex.db")),
    );
    let bound = server::WsServer::new(
        Arc::new(Mutex::new(conn)),
        journal_path.clone(),
        registry,
        sessions,
    )
    .with_mastermind(mastermind)
    .bind("127.0.0.1:0")
    .expect("bind ephemeral loopback port");
    let addr = bound.local_addr();
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(bound.serve(shutdown_rx));
    TestServer {
        dir,
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
    /// Per-frame read deadline; see [`Client::with_read_timeout`].
    read_timeout: Duration,
}

async fn connect(addr: SocketAddr) -> Client {
    let (ws, _response) = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(format!("ws://{addr}/")),
    )
    .await
    .expect("connect timeout")
    .expect("connect_async");
    Client {
        ws,
        read_timeout: READ_TIMEOUT,
    }
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

    /// Raise the per-frame read deadline. Live sessions are silent for
    /// minutes while the provider thinks, so the default 10s deadline —
    /// right for local frames — times out on any real model turn.
    fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    /// Next raw WS message (deadline-bounded).
    async fn next_message(&mut self) -> Message {
        tokio::time::timeout(self.read_timeout, self.ws.next())
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

/// Regression: a long provider turn must not monopolize the connection's
/// reader loop. The desktop heartbeat is an ordinary `ping` RPC on the same
/// socket; if it cannot overtake `mastermind.plan`, the client closes the
/// socket and the human sees WebSocket 1006/4000 instead of a plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_stays_responsive_while_mastermind_plan_is_in_flight() {
    let srv = spawn_slow_mastermind_server().await;
    let repo = srv.dir.path().join("project");
    std::fs::create_dir_all(&repo).expect("project dir");
    let mut client = connect(srv.addr).await;

    let started = client
        .request(
            1,
            "mastermind.start",
            json!({
                "goal": "Plan a tiny demo",
                "repo": repo,
                "plannerAdapter": "mock",
                "plannerModel": "mock-model-1",
            }),
        )
        .await;
    assert_eq!(started["ok"], json!(true), "{started}");
    let session_id = started["result"]["sessionId"]
        .as_str()
        .expect("session id")
        .to_owned();

    client
        .send(&json!({
            "id": 2,
            "method": "mastermind.plan",
            "params": { "sessionId": session_id },
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    client
        .send(&json!({ "id": 3, "method": "ping", "params": {} }))
        .await;

    let heartbeat = tokio::time::timeout(Duration::from_millis(150), client.next_frame())
        .await
        .expect("heartbeat was blocked behind the planner");
    assert_eq!(
        heartbeat["id"],
        json!(3),
        "ping must overtake plan: {heartbeat}"
    );
    assert_eq!(heartbeat["result"]["pong"], json!(true));

    let planned = client.next_frame().await;
    assert_eq!(planned["id"], json!(2), "{planned}");
    assert_eq!(planned["ok"], json!(true), "{planned}");

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// The phase authoring grant is a first-class RPC gate: discovery remains
/// read-only through preparation, an early grant is rejected, and the
/// authorized turn advances to the existing artifact-review gate. Repeating
/// the click returns that same gate instead of authoring a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mastermind_scoped_write_authorization_roundtrips_over_ws() {
    let srv = spawn_mastermind_write_gate_server().await;
    let repo = srv.dir.path().join("project");
    std::fs::create_dir_all(&repo).expect("project dir");
    let discovery = repo.join("docs").join("DISCOVERY.md");
    let mut client = connect(srv.addr).await;

    let started = client
        .request(
            1,
            "mastermind.start",
            json!({
                "goal": "Plan a tiny demo",
                "repo": repo,
                "plannerAdapter": "mock",
                "plannerModel": "mock-model-1",
            }),
        )
        .await;
    assert_eq!(started["ok"], json!(true), "{started}");
    let session_id = started["result"]["sessionId"]
        .as_str()
        .expect("session id")
        .to_owned();

    let early = client
        .request(
            2,
            "mastermind.authorizePhaseWrite",
            json!({ "sessionId": session_id }),
        )
        .await;
    assert_eq!(early["ok"], json!(false), "{early}");
    assert_eq!(early["error"]["code"], json!("invalid_params"));

    let early_accept = client
        .request(
            20,
            "mastermind.acceptPhaseAsIs",
            json!({ "sessionId": session_id }),
        )
        .await;
    assert_eq!(early_accept["ok"], json!(false), "{early_accept}");
    assert_eq!(early_accept["error"]["code"], json!("invalid_params"));

    let early_recovery = client
        .request(
            21,
            "mastermind.recoverPhaseArtifact",
            json!({ "sessionId": session_id }),
        )
        .await;
    assert_eq!(early_recovery["ok"], json!(false), "{early_recovery}");
    assert_eq!(early_recovery["error"]["code"], json!("invalid_params"));

    let early_retry = client
        .request(
            22,
            "mastermind.retryPhaseAuthoring",
            json!({ "sessionId": session_id }),
        )
        .await;
    assert_eq!(early_retry["ok"], json!(false), "{early_retry}");
    assert_eq!(early_retry["error"]["code"], json!("invalid_params"));

    let first = client
        .request(3, "mastermind.plan", json!({ "sessionId": session_id }))
        .await;
    assert_eq!(first["ok"], json!(true), "{first}");
    for (id, answer) in [
        (4, "Smallest working MVP"),
        (5, "Managed cloud"),
        (6, "Public ephemeral data"),
    ] {
        let response = client
            .request(
                id,
                "mastermind.respond",
                json!({ "sessionId": session_id, "instruction": answer }),
            )
            .await;
        assert_eq!(response["ok"], json!(true), "{response}");
        if id == 6 {
            assert_eq!(
                response["result"]["session"]["phaseStatus"],
                json!("awaiting-write-approval")
            );
            assert_eq!(
                response["result"]["session"]["writeRequest"]["deliverables"],
                json!(["docs/DISCOVERY.md"])
            );
            assert_eq!(
                response["result"]["session"]["canAuthorizeWrite"],
                json!(true)
            );
        }
    }
    assert!(
        !discovery.exists(),
        "read-only discovery must not create its deliverable"
    );

    // The mock reports a successful tool turn but intentionally never
    // mutates disk. Seed the exact server-authorized artifact so the RPC can
    // exercise verification and review without provider credentials. Its
    // completion summary only echoes the prompt's mid-line verdict
    // instruction, so the real line-anchored parser must fail closed.
    std::fs::create_dir_all(discovery.parent().expect("docs parent")).expect("docs");
    std::fs::write(&discovery, "# Discovery\n\nPublic ephemeral data\n")
        .expect("mock author artifact");
    let authorized = client
        .request(
            7,
            "mastermind.authorizePhaseWrite",
            json!({ "sessionId": session_id }),
        )
        .await;
    assert_eq!(authorized["ok"], json!(true), "{authorized}");
    assert_eq!(
        authorized["result"]["session"]["phaseStatus"],
        json!("needs-revision")
    );

    let duplicate = client
        .request(
            8,
            "mastermind.authorizePhaseWrite",
            json!({ "sessionId": session_id }),
        )
        .await;
    assert_eq!(duplicate["ok"], json!(true), "{duplicate}");
    assert_eq!(
        duplicate["result"]["session"]["phaseStatus"],
        json!("needs-revision")
    );

    // The fixture cannot author an independent verdict, so use the explicit
    // operator override rather than teaching the test to accept echoed prose.
    let accepted = client
        .request(
            9,
            "mastermind.acceptPhaseAsIs",
            json!({ "sessionId": session_id }),
        )
        .await;
    assert_eq!(accepted["ok"], json!(true), "{accepted}");
    assert_eq!(accepted["result"]["session"]["phase"], json!("phase-2-prd"));
    assert_eq!(
        accepted["result"]["session"]["phaseStatus"],
        json!("active"),
        "approval must return before the next provider preparation turn"
    );

    let prepared = client
        .request(10, "mastermind.plan", json!({ "sessionId": session_id }))
        .await;
    assert_eq!(prepared["ok"], json!(true), "{prepared}");
    assert_eq!(
        prepared["result"]["session"]["phaseStatus"],
        json!("awaiting-write-approval")
    );

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// The desktop previewer reads phase deliverables through one narrow RPC.
/// It serves what the session already advertises, expands the features glob
/// to real files, and refuses anything outside that allowlist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mastermind_artifact_serves_only_session_deliverables() {
    let srv = spawn_mastermind_write_gate_server().await;
    let repo = srv.dir.path().join("project");
    std::fs::create_dir_all(repo.join("docs").join("features")).expect("features dir");
    std::fs::write(
        repo.join("docs").join("DISCOVERY.md"),
        "# Discovery\n\nProse.\n",
    )
    .expect("discovery");
    std::fs::write(
        repo.join("docs").join("features").join("01-relay.md"),
        "# Relay\n",
    )
    .expect("feature doc");
    std::fs::write(srv.dir.path().join("secret.md"), "not a deliverable").expect("secret");
    let mut client = connect(srv.addr).await;

    let started = client
        .request(
            1,
            "mastermind.start",
            json!({
                "goal": "Preview the deliverables",
                "repo": repo,
                "plannerAdapter": "mock",
                "plannerModel": "mock-model-1",
            }),
        )
        .await;
    assert_eq!(started["ok"], json!(true), "{started}");
    let session_id = started["result"]["sessionId"]
        .as_str()
        .expect("session id")
        .to_owned();

    let read = client
        .request(
            2,
            "mastermind.artifact",
            json!({ "sessionId": session_id, "path": "docs/DISCOVERY.md" }),
        )
        .await;
    assert_eq!(read["ok"], json!(true), "{read}");
    assert_eq!(read["result"]["content"], json!("# Discovery\n\nProse.\n"));
    assert_eq!(read["result"]["truncated"], json!(false));

    // The glob the deliverable list shows is expanded to real files here, so
    // the previewer can offer each feature doc individually.
    let available = read["result"]["available"]
        .as_array()
        .expect("available list");
    assert!(
        available.contains(&json!("docs/features/01-relay.md")),
        "{available:?}"
    );
    assert!(
        !available.contains(&json!("docs/features/*.md")),
        "{available:?}"
    );

    for escape in [
        "../secret.md",
        "docs/../../secret.md",
        "docs/features/*.md",
        "Cargo.toml",
    ] {
        let refused = client
            .request(
                3,
                "mastermind.artifact",
                json!({ "sessionId": session_id, "path": escape }),
            )
            .await;
        assert_eq!(
            refused["ok"],
            json!(false),
            "{escape} must be refused: {refused}"
        );
    }

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
    let mut client = connect(srv.addr)
        .await
        .with_read_timeout(Duration::from_secs(300));

    // Events reach a connection only after it subscribes (§3.2).
    let sub = client
        .request(0, "events.subscribe", json!({ "afterSeq": 0 }))
        .await;
    assert_eq!(sub["ok"], json!(true), "{sub}");

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

    // Wait for either terminal event: a failed session must report the
    // provider's reason (quota, auth) rather than hang until the deadline.
    let terminal = tokio::time::timeout(Duration::from_secs(240), async {
        loop {
            let frame = client.next_frame().await;
            if frame["notification"] != json!("event") {
                continue;
            }
            let event_type = frame["event"]["eventType"].clone();
            let mine = frame["event"]["payload"]["sessionId"] == json!(session_id);
            if mine
                && (event_type == json!("session.finished")
                    || event_type == json!("agent.session_failed"))
            {
                return frame;
            }
        }
    })
    .await
    .expect("researcher session reached a terminal event within 4 minutes");

    assert_eq!(
        terminal["event"]["eventType"],
        json!("session.finished"),
        "session failed: {}",
        terminal["event"]["payload"]["error"]
    );
    let reply = terminal["event"]["payload"]["finalResult"]
        .as_str()
        .unwrap_or_default();
    assert!(!reply.trim().is_empty(), "researcher produced a reply");

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// F-13 live e2e (opt-in, BILLABLE): the **agent builder**, end to end —
/// a real chat with the seeded `agent-creator` (agy → claude-sonnet-4-6),
/// whose fenced-JSON draft the daemon must parse, validate and journal as
/// `agent.proposal`; then the human half, registering that exact draft
/// through `registry.agents.create` and cleaning it up again.
///
/// This is the only test that proves the creator's *skill body* actually
/// produces a registry-valid contract — the offline tests only prove the
/// parser handles a well-formed one. Doubly gated: `#[ignore]` plus
/// `AGENTOS_AGY_E2E=1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live agy session (billable): set AGENTOS_AGY_E2E=1 to run"]
async fn live_agent_creator_proposal_e2e() {
    if std::env::var("AGENTOS_AGY_E2E").ok().as_deref() != Some("1") {
        return;
    }
    let srv = spawn_server(vec![]).await;
    let mut client = connect(srv.addr)
        .await
        .with_read_timeout(Duration::from_secs(300));

    // Everything the interview would ask for, supplied up front: the skill
    // tells the creator to interview first, so a one-shot test must remove
    // the reason to ask.
    let brief = "Skip the interview - here is everything. I need an agent that reads \
                 SQL migration files and reports risky statements. It only reads files, \
                 never edits. Runs on antigravity-agy with model gpt-oss-120b-medium. \
                 Sessions are short, about 10 minutes. Use skill code-graph-discipline. \
                 Give it the id sql-migration-auditor. Output the JSON proposal now.";

    // Events reach a connection only after it subscribes (§3.2).
    let sub = client
        .request(0, "events.subscribe", json!({ "afterSeq": 0 }))
        .await;
    assert_eq!(sub["ok"], json!(true), "{sub}");

    let resp = client
        .request(
            1,
            "agent.session.start",
            json!({ "agentId": "agent-creator", "message": brief }),
        )
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");
    let session_id = resp["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    // The creator may propose, or may emit an invalid draft — both are
    // journaled, and an invalid one is a real finding, not a flake.
    //
    // Ordering matters: the daemon journals `session.finished` FIRST and
    // parses the proposal out of that same final text afterwards, so a
    // reader that stops at the terminal event always misses the proposal.
    // Keep draining for a grace period after the terminal event.
    let outcome = tokio::time::timeout(Duration::from_secs(300), async {
        let mut terminal: Option<Value> = None;
        loop {
            let frame = match terminal {
                // After the session ended the proposal is milliseconds
                // away or never coming.
                Some(_) => {
                    match tokio::time::timeout(Duration::from_secs(20), client.next_frame()).await {
                        Ok(frame) => frame,
                        Err(_) => return terminal.expect("terminal seen"),
                    }
                }
                None => client.next_frame().await,
            };
            if frame["notification"] != json!("event") {
                continue;
            }
            let event_type = frame["event"]["eventType"].clone();
            if event_type == json!("agent.proposal")
                || event_type == json!("agent.proposal_invalid")
            {
                return frame;
            }
            // A finished session with no proposal event means the model
            // answered in prose; a failed one means the adapter chain
            // broke. Both are findings, not flakes — surface them.
            let is_terminal = event_type == json!("session.finished")
                || event_type == json!("agent.session_failed");
            if is_terminal && frame["event"]["payload"]["sessionId"] == json!(session_id) {
                terminal = Some(frame);
            }
        }
    })
    .await
    .expect("creator produced a proposal outcome within 5 minutes");

    assert_eq!(
        outcome["event"]["eventType"],
        json!("agent.proposal"),
        "creator did not emit a registry-valid draft: {outcome}"
    );
    let draft = outcome["event"]["payload"]["agent"].clone();
    assert!(
        draft["id"].as_str().is_some_and(|id| !id.is_empty()),
        "draft carries an id: {draft}"
    );
    assert_eq!(
        draft["mode"],
        json!("plan"),
        "a read-only agent must be proposed in plan mode: {draft}"
    );
    assert_eq!(draft["adapterId"], json!("antigravity-agy"), "{draft}");

    // The human half: register the proposed draft verbatim.
    let created = client
        .request(2, "registry.agents.create", json!({ "agent": draft }))
        .await;
    assert_eq!(
        created["ok"],
        json!(true),
        "draft must register as-is: {created}"
    );
    let created_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("created id")
        .to_owned();

    let listed = client.request(3, "registry.agents.list", json!({})).await;
    assert!(
        listed["result"]["agents"]
            .as_array()
            .expect("agents array")
            .iter()
            .any(|agent| agent["id"] == json!(created_id)),
        "the registered agent is in the roster"
    );

    // Clean up: this is a real registry on disk.
    let deleted = client
        .request(4, "registry.agents.delete", json!({ "id": created_id }))
        .await;
    assert_eq!(deleted["ok"], json!(true), "{deleted}");

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// Register a throwaway agent for a live test and return its id.
///
/// Live tests pin *paths*, not providers. The daemon's chat path (spawn
/// spec → adapter → streamed events → journal) is provider-independent, so
/// these run on whichever runtime has capacity: as written, the entire
/// Antigravity account was quota-exhausted (~61h, every model), so the
/// write/resume/decision paths are exercised through codex. The agy half
/// of each path stays covered by the adapter's frozen-transcript tests.
async fn create_scratch_agent(
    client: &mut Client,
    id: &str,
    request_id: u64,
    adapter_id: &str,
    model: &str,
    mode: &str,
    skills: Value,
) -> String {
    let created = client
        .request(
            request_id,
            "registry.agents.create",
            json!({ "agent": {
                "id": id,
                "name": "E2E Scratch Agent",
                "description": "Throwaway agent created by a live e2e test.",
                "adapterId": adapter_id,
                "model": model,
                "mode": mode,
                "skills": skills,
                "timeoutSecs": 600,
            }}),
        )
        .await;
    assert_eq!(
        created["ok"],
        json!(true),
        "scratch agent must register: {created}"
    );
    created["result"]["agent"]["id"]
        .as_str()
        .expect("created id")
        .to_owned()
}

/// Wait for a chat session's terminal event (either kind), draining a
/// grace period afterwards so events journaled *after* the terminal one
/// (proposals, late decisions) are not missed.
async fn await_session_terminal(
    client: &mut Client,
    session_id: &str,
    budget: Duration,
) -> (Value, Vec<Value>) {
    tokio::time::timeout(budget, async {
        let mut trailing = Vec::new();
        let mut terminal: Option<Value> = None;
        loop {
            let frame = match terminal {
                Some(_) => {
                    match tokio::time::timeout(Duration::from_secs(15), client.next_frame()).await {
                        Ok(frame) => frame,
                        Err(_) => return (terminal.expect("terminal seen"), trailing),
                    }
                }
                None => client.next_frame().await,
            };
            if frame["notification"] != json!("event") {
                continue;
            }
            let event_type = frame["event"]["eventType"].clone();
            let mine = frame["event"]["payload"]["sessionId"] == json!(session_id);
            if terminal.is_some() {
                trailing.push(frame);
                continue;
            }
            if mine
                && (event_type == json!("session.finished")
                    || event_type == json!("agent.session_failed"))
            {
                terminal = Some(frame);
            } else {
                trailing.push(frame);
            }
        }
    })
    .await
    .expect("session reached a terminal event within its budget")
}

/// F-13 live e2e (opt-in, BILLABLE): the **write path and the resume
/// path** through the daemon — the two behaviors nothing had ever
/// exercised end to end. An `accept_edits` agent writes a real file into
/// the chat workspace, then a follow-up instruction must resume the same
/// provider session rather than start a fresh one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live session (billable): set AGENTOS_CODEX_E2E=1 to run"]
async fn live_chat_writes_and_resumes_e2e() {
    if std::env::var("AGENTOS_CODEX_E2E").ok().as_deref() != Some("1") {
        return;
    }
    let srv = spawn_server(vec![]).await;
    let workspace = srv.dir.path().to_path_buf();
    let mut client = connect(srv.addr)
        .await
        .with_read_timeout(Duration::from_secs(300));
    let sub = client
        .request(0, "events.subscribe", json!({ "afterSeq": 0 }))
        .await;
    assert_eq!(sub["ok"], json!(true), "{sub}");

    let agent_id = create_scratch_agent(
        &mut client,
        "e2e-writer",
        1,
        "codex",
        "gpt-5.6-terra",
        "accept_edits",
        json!([]),
    )
    .await;

    let resp = client
        .request(
            2,
            "agent.session.start",
            json!({
                "agentId": agent_id,
                "message": "Create a file named probe_written.txt in the current working \
                            directory containing exactly the word WRITTEN. Then reply DONE. \
                            Do not ask questions."
            }),
        )
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");
    let session_id = resp["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    let (terminal, _) =
        await_session_terminal(&mut client, &session_id, Duration::from_secs(420)).await;
    assert_eq!(
        terminal["event"]["eventType"],
        json!("session.finished"),
        "coder session failed: {}",
        terminal["event"]["payload"]["error"]
    );
    let written = workspace.join("probe_written.txt");
    assert!(
        written.is_file(),
        "an accept_edits agent must write into the real workspace, not a \
         virtualized brain dir; {} missing",
        written.display()
    );

    // Resume: the follow-up must reach the SAME conversation.
    let follow_up = client
        .request(
            3,
            "agent.session.send",
            json!({ "sessionId": session_id, "message":
                "What exact word did you just write into that file? Reply with only that word." }),
        )
        .await;
    assert_eq!(follow_up["ok"], json!(true), "{follow_up}");

    let (resumed, _) =
        await_session_terminal(&mut client, &session_id, Duration::from_secs(420)).await;
    assert_eq!(
        resumed["event"]["eventType"],
        json!("session.finished"),
        "resumed turn failed: {}",
        resumed["event"]["payload"]["error"]
    );
    let reply = resumed["event"]["payload"]["finalResult"]
        .as_str()
        .unwrap_or_default();
    assert!(
        reply.contains("WRITTEN"),
        "the resumed turn must carry the first turn's context; got {reply:?}"
    );

    let deleted = client
        .request(4, "registry.agents.delete", json!({ "id": agent_id }))
        .await;
    assert_eq!(deleted["ok"], json!(true), "{deleted}");

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// F-02/F-13 live e2e (opt-in, BILLABLE): the **decision protocol** end to
/// end. Neither agy nor codex has a usable asking tool headless, so a
/// question must arrive as a fenced ask block in the answer text, be
/// lifted by `decision::from_text`, and reach the desktop as an
/// `agent.decision` event carrying real options.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live session (billable): set AGENTOS_CODEX_E2E=1 to run"]
async fn live_decision_protocol_e2e() {
    if std::env::var("AGENTOS_CODEX_E2E").ok().as_deref() != Some("1") {
        return;
    }
    let srv = spawn_server(vec![]).await;
    let mut client = connect(srv.addr)
        .await
        .with_read_timeout(Duration::from_secs(300));
    let sub = client
        .request(0, "events.subscribe", json!({ "afterSeq": 0 }))
        .await;
    assert_eq!(sub["ok"], json!(true), "{sub}");

    let agent_id = create_scratch_agent(
        &mut client,
        "e2e-asker",
        1,
        "codex",
        "gpt-5.6-terra",
        "plan",
        json!(["decision-protocol"]),
    )
    .await;

    let resp = client
        .request(
            2,
            "agent.session.start",
            json!({
                "agentId": agent_id,
                "message": "Before writing any code for a notes app, ask me which database \
                            to use - Postgres or SQLite. Ask that one question and stop."
            }),
        )
        .await;
    assert_eq!(resp["ok"], json!(true), "{resp}");
    let session_id = resp["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    let (terminal, trailing) =
        await_session_terminal(&mut client, &session_id, Duration::from_secs(420)).await;
    assert_eq!(
        terminal["event"]["eventType"],
        json!("session.finished"),
        "session failed: {}",
        terminal["event"]["payload"]["error"]
    );

    let decision = trailing
        .iter()
        .find(|frame| frame["event"]["eventType"] == json!("agent.decision"))
        .unwrap_or_else(|| {
            panic!(
                "the decision-protocol skill must produce an agent.decision; \
                 final text was {:?}",
                terminal["event"]["payload"]["finalResult"]
            )
        });
    let options = decision["event"]["payload"]["options"]
        .as_array()
        .expect("options array");
    assert!(
        options.len() >= 2,
        "a decision needs real choices: {decision}"
    );
    assert!(
        decision["event"]["payload"]["prompt"]
            .as_str()
            .is_some_and(|prompt| !prompt.trim().is_empty()),
        "{decision}"
    );

    let deleted = client
        .request(3, "registry.agents.delete", json!({ "id": agent_id }))
        .await;
    assert_eq!(deleted["ok"], json!(true), "{deleted}");

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// F-13b live e2e (opt-in, BILLABLE): **reopening a closed chat actually
/// restores the agent's memory.** A conversation stores a token and is then
/// cancelled — killing the daemon-side session entirely. A later
/// `agent.session.reopen` must bind the journaled `providerSessionId` to a
/// new adapter session and recall the token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live session (billable): set AGENTOS_CODEX_E2E=1 to run"]
async fn live_reopen_restores_the_conversation_e2e() {
    if std::env::var("AGENTOS_CODEX_E2E").ok().as_deref() != Some("1") {
        return;
    }
    let srv = spawn_server(vec![]).await;
    let mut client = connect(srv.addr)
        .await
        .with_read_timeout(Duration::from_secs(300));
    let sub = client
        .request(0, "events.subscribe", json!({ "afterSeq": 0 }))
        .await;
    assert_eq!(sub["ok"], json!(true), "{sub}");

    let agent_id = create_scratch_agent(
        &mut client,
        "e2e-rememberer",
        1,
        "codex",
        "gpt-5.6-terra",
        "plan",
        json!([]),
    )
    .await;

    let started = client
        .request(
            2,
            "agent.session.start",
            json!({ "agentId": agent_id,
                    "message": "Remember this token: FIG9. Reply with exactly: STORED" }),
        )
        .await;
    assert_eq!(started["ok"], json!(true), "{started}");
    let first_session = started["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    let (terminal, _) =
        await_session_terminal(&mut client, &first_session, Duration::from_secs(420)).await;
    assert_eq!(
        terminal["event"]["eventType"],
        json!("session.finished"),
        "first turn failed: {}",
        terminal["event"]["payload"]["error"]
    );

    // End the session for good: nothing live remains to talk to.
    let cancelled = client
        .request(
            3,
            "agent.session.cancel",
            json!({ "sessionId": first_session }),
        )
        .await;
    assert_eq!(cancelled["ok"], json!(true), "{cancelled}");
    let refused = client
        .request(
            4,
            "agent.session.send",
            json!({ "sessionId": first_session, "message": "still there?" }),
        )
        .await;
    assert_eq!(refused["ok"], json!(false), "the session really is closed");

    // History knows the conversation and how to continue it.
    let listed = client
        .request(5, "chat.sessions", json!({ "agentId": agent_id }))
        .await;
    let summary = listed["result"]["sessions"][0].clone();
    assert_eq!(summary["sessionId"], json!(first_session), "{listed}");
    assert!(
        summary["providerSessionId"].as_str().is_some(),
        "no resume handle was journaled: {summary}"
    );

    // Reopen: a NEW daemon session bound to the SAME provider conversation.
    let reopened = client
        .request(
            6,
            "agent.session.reopen",
            json!({ "sessionId": first_session,
                    "message": "What token did I ask you to remember? Reply with only the token." }),
        )
        .await;
    assert_eq!(reopened["ok"], json!(true), "{reopened}");
    let resumed_session = reopened["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();
    assert_ne!(
        resumed_session, first_session,
        "a reopen opens a new session"
    );
    assert_eq!(reopened["result"]["resumedFrom"], json!(first_session));

    let (resumed_terminal, _) =
        await_session_terminal(&mut client, &resumed_session, Duration::from_secs(420)).await;
    assert_eq!(
        resumed_terminal["event"]["eventType"],
        json!("session.finished"),
        "resumed turn failed: {}",
        resumed_terminal["event"]["payload"]["error"]
    );
    let reply = resumed_terminal["event"]["payload"]["finalResult"]
        .as_str()
        .unwrap_or_default();
    assert!(
        reply.contains("FIG9"),
        "reopening must restore the provider's memory; got {reply:?}"
    );

    let deleted = client
        .request(7, "registry.agents.delete", json!({ "id": agent_id }))
        .await;
    assert_eq!(deleted["ok"], json!(true), "{deleted}");

    srv.shutdown.send(true).ok();
    srv.task.await.expect("serve task").expect("serve clean");
}

/// F-13b: chat history survives the browser. A mock-backed conversation is
/// journaled, then read back through `chat.sessions` / `chat.transcript` by
/// a client that never saw it happen — which is exactly what a reloaded
/// desktop is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_history_is_readable_by_a_client_that_missed_it() {
    let srv = spawn_server(vec![]).await;

    // Conversation happens on one connection.
    {
        let mut client = connect(srv.addr).await;
        let created = client
            .request(
                1,
                "registry.agents.create",
                json!({ "agent": {
                    "id": "chatty-history",
                    "name": "Chatty",
                    "description": "mock agent for the history test",
                    "adapterId": "mock",
                    "mode": "plan",
                    "timeoutSecs": 60,
                }}),
            )
            .await;
        assert_eq!(created["ok"], json!(true), "{created}");

        let started = client
            .request(
                2,
                "agent.session.start",
                json!({ "agentId": "chatty-history", "message": "first question" }),
            )
            .await;
        assert_eq!(started["ok"], json!(true), "{started}");

        // Let the mock's scripted run reach its terminal event.
        for _ in 0..50 {
            let sessions = client.request(3, "chat.sessions", json!({})).await;
            let rows = sessions["result"]["sessions"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if rows
                .first()
                .is_some_and(|row| row["turns"].as_u64().unwrap_or(0) > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    // A FRESH connection — the reloaded desktop — reads the history.
    let mut reader = connect(srv.addr).await;
    let listed = reader
        .request(10, "chat.sessions", json!({ "agentId": "chatty-history" }))
        .await;
    assert_eq!(listed["ok"], json!(true), "{listed}");
    let sessions = listed["result"]["sessions"]
        .as_array()
        .expect("sessions array");
    assert_eq!(sessions.len(), 1, "one conversation happened: {listed}");
    let summary = &sessions[0];
    assert_eq!(summary["agentId"], json!("chatty-history"));
    assert_eq!(
        summary["title"],
        json!("first question"),
        "title is the opening ask"
    );
    assert_eq!(summary["provider"], json!("mock"));
    assert!(
        summary["providerSessionId"].as_str().is_some(),
        "the resume handle is part of history: {summary}"
    );

    let session_id = summary["sessionId"].as_str().expect("sessionId").to_owned();
    let transcript = reader
        .request(11, "chat.transcript", json!({ "sessionId": session_id }))
        .await;
    assert_eq!(transcript["ok"], json!(true), "{transcript}");
    let messages = transcript["result"]["messages"]
        .as_array()
        .expect("messages array");
    assert!(
        messages
            .iter()
            .any(|m| m["role"] == json!("user") && m["text"] == json!("first question")),
        "the user's own words are in the transcript: {transcript}"
    );
    assert!(
        messages.iter().any(|m| m["role"] == json!("agent")),
        "so is the reply: {transcript}"
    );
    assert_eq!(
        transcript["result"]["session"]["sessionId"],
        json!(session_id),
        "the transcript carries its summary"
    );

    // Unknown sessions are parameter errors, not empty successes.
    let missing = reader
        .request(12, "chat.transcript", json!({ "sessionId": "chat-nope" }))
        .await;
    assert_eq!(missing["ok"], json!(true), "an unknown id folds to nothing");
    assert!(missing["result"]["messages"]
        .as_array()
        .is_some_and(|m| m.is_empty()));

    // Reopening needs a resumable provider session. The mock now preserves
    // the supplied provider id so credential-free tests exercise that same
    // continuity contract instead of silently opening a memoryless chat.
    let reopened = reader
        .request(
            13,
            "agent.session.reopen",
            json!({ "sessionId": session_id, "message": "and now?" }),
        )
        .await;
    assert_eq!(reopened["ok"], json!(true), "{reopened}");
    assert_eq!(
        reopened["result"]["resumedFrom"],
        json!(session_id),
        "the daemon must retain the original conversation link: {reopened}"
    );

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

    let codex = providers
        .iter()
        .find(|p| p["id"] == json!("codex"))
        .expect("codex entry");
    let codex_models: Vec<&str> = codex["models"]
        .as_array()
        .expect("codex models")
        .iter()
        .map(|model| model["id"].as_str().expect("codex model id"))
        .collect();
    assert_eq!(
        codex_models,
        vec![
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-5.5",
            "gpt-5.4",
        ]
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

    // A completed turn leaves the session instructable: the follow-up
    // reaches the adapter instead of being refused by the daemon. (The
    // mock adapter then declines it — its run is a fixed script — so the
    // reply is an error, but not the daemon's "cannot be resumed".)
    let resp = client
        .request(
            4,
            "agent.session.send",
            json!({ "sessionId": session_id, "message": "again" }),
        )
        .await;
    assert!(
        !resp["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("cannot be resumed"),
        "the daemon must not evict a session on a completed turn: {resp}"
    );

    // Cancelling is what ends it; afterwards sends are refused.
    let cancelled = client
        .request(
            5,
            "agent.session.cancel",
            json!({ "sessionId": session_id }),
        )
        .await;
    assert_eq!(cancelled["ok"], json!(true), "{cancelled}");
    let resp = client
        .request(
            6,
            "agent.session.send",
            json!({ "sessionId": session_id, "message": "again" }),
        )
        .await;
    assert_eq!(resp["ok"], json!(false));
    assert_eq!(resp["error"]["code"], json!("invalid_params"));

    // Unknown agents / empty messages are parameter errors.
    let resp = client
        .request(
            7,
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
