//! agentos-daemon binary (F-01 + F-11a): open the journal, mark startup,
//! serve the loopback WebSocket API until ctrl-c, then broadcast
//! `daemon.stopping` and close every peer with 1001.
//!
//! Subcommands (hand-parsed argv — the daemon deliberately has no clap):
//!
//! - *(none)* / `serve` — the default: F-01 journal open + startup marker,
//!   WS server bound per `AGENTOS_WS_ADDR` (default `127.0.0.1:8741`).
//! - `demo-seed --db <path> --fixture <file.json>` — append the frozen demo
//!   fixture into an explicit, still-empty journal (F-11 §3.4); refuses the
//!   default journal path and any journal that already has events.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use agentos_agents::AgentRegistry;
use agentos_core::{Event, EventType};
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use agentos_daemon::{
    agent_sessions::{AdapterSet, AgentSessions},
    db, events, seed, server,
};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        None | Some("serve") => serve(&args[2..]).await,
        Some("demo-seed") => demo_seed(&args[2..]),
        Some(other) => {
            error!(
                subcommand = other,
                "unknown subcommand (expected `serve` or `demo-seed`)"
            );
            ExitCode::FAILURE
        }
    }
}

/// Default mode: F-01 startup sequence, then the F-11a WS API until ctrl-c.
///
/// F-13 additions: the agent registry (`agents.db` beside the journal,
/// built-ins seeded idempotently) and the chat-session service over the
/// three adapters (claude-code, antigravity-agy, mock — mock so the UI and
/// tests can exercise the full surface without billing).
async fn serve(flags: &[String]) -> ExitCode {
    let mut project: Option<PathBuf> = None;
    let mut rest = flags.iter();
    while let Some(flag) = rest.next() {
        let Some(value) = rest.next() else {
            error!(flag, "flag requires a value");
            return ExitCode::FAILURE;
        };
        match flag.as_str() {
            "--project" => project = Some(PathBuf::from(value)),
            other => {
                error!(flag = other, "unknown serve flag (expected --project)");
                return ExitCode::FAILURE;
            }
        }
    }

    let db_path = journal_path();
    if let Some(parent) = db_path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(err) = std::fs::create_dir_all(parent) {
                error!(path = %parent.display(), %err, "failed to create journal parent directory");
                return ExitCode::FAILURE;
            }
        }
    }

    let conn = match db::open_db(&db_path) {
        Ok(conn) => conn,
        Err(err) => {
            error!(path = %db_path.display(), %err, "failed to open journal database");
            return ExitCode::FAILURE;
        }
    };
    info!(journal = %db_path.display(), "journal open (SQLite, WAL, busy_timeout=5000ms)");

    // F-13: the agent registry lives beside the journal and seeds the
    // mastermind trio idempotently (edited built-ins survive).
    let registry_path = match db_path.parent() {
        Some(parent) => parent.join("agents.db"),
        None => PathBuf::from("agents.db"),
    };
    let registry = match AgentRegistry::open(&registry_path)
        .and_then(|r| r.seed_builtins().map(|inserted| (r, inserted)))
    {
        Ok((registry, inserted)) => {
            info!(
                registry = %registry_path.display(),
                inserted,
                "agent registry open; built-ins seeded"
            );
            Arc::new(registry)
        }
        Err(err) => {
            error!(path = %registry_path.display(), %err, "failed to open agent registry");
            return ExitCode::FAILURE;
        }
    };

    // Chat sessions run in the project root: `--project`, else
    // `AGENTOS_PROJECT`, else the daemon's cwd.
    let workspace = project
        .or_else(|| std::env::var_os("AGENTOS_PROJECT").map(PathBuf::from))
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    if let Err(err) = std::fs::create_dir_all(&workspace) {
        error!(path = %workspace.display(), %err, "failed to create the chat workspace");
        return ExitCode::FAILURE;
    }

    let sessions = Arc::new(AgentSessions::new(
        Arc::clone(&registry),
        AdapterSet::wired(),
        db_path.clone(),
        workspace.clone(),
    ));
    info!(workspace = %workspace.display(), "agent chat sessions enabled (claude-code, antigravity-agy, mock)");

    // Startup marker. Daemon lifecycle is not run-scoped, so `run_id`
    // stays `None` ("where applicable", F-00 §3); a fresh trace id ties
    // this daemon session's events together once more of them exist.
    let started = Event::new(EventType::Other("daemon.started".to_owned()))
        .with_trace_id(Uuid::now_v7())
        .with_payload(serde_json::json!({
            "pid": std::process::id(),
            "journal": db_path.display().to_string(),
        }));
    match events::append_event(&conn, &started) {
        Ok(seq) => info!(seq, event_type = %started.event_type, "startup marker appended"),
        Err(err) => {
            error!(%err, "failed to append startup marker");
            return ExitCode::FAILURE;
        }
    }

    // F-11a: the journal is served read-only from one shared connection;
    // external writers append through their own connections (WAL), which
    // the subscription tail polls observe (F-11 §3.2).
    let addr = server::ws_addr_from_env();
    let bound = match server::WsServer::new(
        Arc::new(Mutex::new(conn)),
        db_path.clone(),
        registry,
        sessions,
    )
    .bind(&addr)
    {
        Ok(bound) => bound,
        Err(err) => {
            error!(addr = %addr, %err, "failed to bind the websocket api");
            return ExitCode::FAILURE;
        }
    };
    let bound_addr = bound.local_addr();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let serve_task = tokio::spawn(bound.serve(shutdown_rx));
    info!(addr = %bound_addr, "agentos-daemon ready; websocket api live; waiting for ctrl-c");

    if let Err(err) = tokio::signal::ctrl_c().await {
        error!(%err, "failed to wait for ctrl-c");
        return ExitCode::FAILURE;
    }
    info!("ctrl-c received; shutting down gracefully");

    // Flipping the watch makes every connection broadcast
    // `daemon.stopping` and close with 1001 before `serve` returns.
    shutdown_tx.send(true).ok();
    match serve_task.await {
        Ok(Ok(())) => {
            info!("websocket api drained; bye");
            ExitCode::SUCCESS
        }
        Ok(Err(err)) => {
            error!(%err, "websocket server failed during shutdown");
            ExitCode::FAILURE
        }
        Err(err) => {
            error!(%err, "websocket server task panicked");
            ExitCode::FAILURE
        }
    }
}

/// `demo-seed --db <path> --fixture <file.json>` (F-11 §3.4). Hand-parsed:
/// both flags are required; unknown flags are an error, not a guess.
fn demo_seed(args: &[String]) -> ExitCode {
    let mut db_path: Option<PathBuf> = None;
    let mut fixture: Option<PathBuf> = None;
    let mut rest = args.iter();
    while let Some(flag) = rest.next() {
        let Some(value) = rest.next() else {
            error!(flag, "flag requires a value");
            return ExitCode::FAILURE;
        };
        match flag.as_str() {
            "--db" => db_path = Some(PathBuf::from(value)),
            "--fixture" => fixture = Some(PathBuf::from(value)),
            other => {
                error!(
                    flag = other,
                    "unknown demo-seed flag (expected --db/--fixture)"
                );
                return ExitCode::FAILURE;
            }
        }
    }
    let (Some(db_path), Some(fixture)) = (db_path, fixture) else {
        error!("demo-seed requires both --db <path> and --fixture <file.json>");
        return ExitCode::FAILURE;
    };

    match seed::seed_demo(&db_path, &fixture) {
        Ok(report) => {
            info!(
                journal = %db_path.display(),
                fixture = %fixture.display(),
                appended = report.appended,
                "demo journal seeded"
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            error!(journal = %db_path.display(), %err, "demo-seed refused");
            ExitCode::FAILURE
        }
    }
}

/// Journal location: `AGENTOS_DB` overrides; otherwise the per-OS default
/// ([`db::default_journal_path`], shared with the seeder's refusal check).
fn journal_path() -> PathBuf {
    match std::env::var_os("AGENTOS_DB") {
        Some(path) => PathBuf::from(path),
        None => db::default_journal_path(),
    }
}
