//! agentos-daemon binary (F-01): open the journal, mark startup, wait for
//! ctrl-c, shut down. Minimal by design — no IPC/WebSocket yet; those land
//! with F-02+.

use std::path::PathBuf;
use std::process::ExitCode;

use agentos_core::{Event, EventType};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use agentos_daemon::{db, events};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let db_path = db_path();
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

    info!("agentos-daemon ready; waiting for ctrl-c");
    if let Err(err) = tokio::signal::ctrl_c().await {
        error!(%err, "failed to wait for ctrl-c");
        return ExitCode::FAILURE;
    }
    info!("ctrl-c received; shutting down gracefully");
    ExitCode::SUCCESS
}

/// Journal location: `AGENTOS_DB` overrides; otherwise the per-OS default.
fn db_path() -> PathBuf {
    match std::env::var_os("AGENTOS_DB") {
        Some(path) => PathBuf::from(path),
        None => default_db_path(),
    }
}

/// `%LOCALAPPDATA%/agentos/daemon.db` on Windows (reference platform),
/// `$HOME/.local/state/agentos/daemon.db` elsewhere (XDG state dir shape).
fn default_db_path() -> PathBuf {
    if cfg!(windows) {
        let base = match std::env::var_os("LOCALAPPDATA") {
            Some(dir) => PathBuf::from(dir),
            None => home_dir().join("AppData").join("Local"),
        };
        base.join("agentos").join("daemon.db")
    } else {
        home_dir()
            .join(".local")
            .join("state")
            .join("agentos")
            .join("daemon.db")
    }
}

/// `$HOME` with a conservative fallback for environments that unset it.
fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}
