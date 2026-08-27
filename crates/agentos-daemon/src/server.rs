//! F-11a: the daemon's read-only WebSocket JSON API over the journal.
//!
//! CONTRACT: `docs/F-11-desktop.md` §2–§3 (framing, methods, projections).
//! The desktop app is a pure client of this surface; drift between this
//! module and the F-doc is a bug, not an evolution.
//!
//! Shape of the implementation:
//!
//! - **Framing** — one JSON object per TEXT frame. Requests
//!   `{"id","method","params"?}` get `{"id","ok":true,"result"}` or
//!   `{"id","ok":false,"error":{"code","message"}}`; anything unparseable
//!   (including a missing `method`) is answered `invalid_request` with
//!   `id: null` (§2.2). Frames with no `id` are notifications and are a
//!   server → client-only shape, so a client frame without one is malformed.
//! - **Journal access** — one shared `rusqlite::Connection` behind a
//!   `std::sync::Mutex`. The API's journal reads are synchronous and
//!   desktop-scale (a handful of peers, queries in the microsecond
//!   range), so the mutex is cheaper than a connection pool and its
//!   lifecycle. Guards are never held across an `.await`: every journal
//!   read is a synchronous block whose result crosses the async boundary
//!   as owned data. (F-13 made `dispatch` async for the adapter-facing
//!   registry arms — chat-session spawns and the free catalog probes —
//!   but the guard rule stands: async arms never touch the journal
//!   mutex.) WAL means readers never block writers — and external
//!   processes appending to the same journal file are the *supported*
//!   write path until the supervisor moves in-process (§3.2), which
//!   per-subscription polling observes because every autocommit
//!   `SELECT` takes a fresh snapshot.
//! - **Backpressure** — each connection has a bounded outbound frame
//!   channel ([`OUTBOUND_CAPACITY`]) drained by a writer task. Request
//!   responses are client-paced and use a blocking `send` (the client that
//!   asked is expected to read); subscription notifications use
//!   `try_send`, and a subscriber that fills the buffer is closed with
//!   `subscription.closed` reason `slow_consumer` (§2) — the server never
//!   blocks a handler on a slow consumer and never drops frames silently
//!   for healthy ones.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener as StdTcpListener, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use agentos_agents::{AgentRecord, SkillRecord};
use agentos_core::{CoreError, Event, EventType};
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rusqlite::Connection;
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::agent_sessions::{AgentSessions, SessionError};
use crate::events::{self, SequencedEvent};
use crate::projection;

/// Registry-mutation journal events (F-13 audit trail: the tables are
/// mutable, the journal records who changed what when).
const EVT_AGENT_CREATED: &str = "agent.created";
const EVT_AGENT_UPDATED: &str = "agent.updated";
const EVT_AGENT_DELETED: &str = "agent.deleted";
const EVT_SKILL_CREATED: &str = "skill.created";
const EVT_SKILL_UPDATED: &str = "skill.updated";
const EVT_SKILL_DELETED: &str = "skill.deleted";

/// Default bind address (loopback only — F-11 §2).
pub const DEFAULT_WS_ADDR: &str = "127.0.0.1:8741";

/// Environment variable overriding the bind address.
pub const WS_ADDR_ENV: &str = "AGENTOS_WS_ADDR";

/// Subscription tail poll interval (F-11 §3.2 contract). The interval is
/// the contract; a `PRAGMA data_version` short-circuit would be allowed but
/// polling a WAL `SELECT seq > ?` is already the cheap path.
pub const TAIL_POLL_INTERVAL_MS: u64 = 250;

/// Rows fetched per subscription poll (`events::tail(last_seq, batch)`).
/// 500 keeps replay bursty-but-bounded: a 10k-event journal replays in 20
/// polls back-to-back while live tails stay one small query per tick.
const TAIL_BATCH: u32 = 500;

/// Per-connection outbound frame buffer. Frames beyond this while the peer
/// is not reading mark the peer a `slow_consumer` (bounded memory, §2).
const OUTBOUND_CAPACITY: usize = 128;

/// Error code: unparseable frame / no method (§2.2).
pub const CODE_INVALID_REQUEST: &str = "invalid_request";
/// Error code: the method exists for a future slice, not this one.
pub const CODE_NOT_SUPPORTED: &str = "not_supported";
/// Error code: known method, rejected parameter values.
pub const CODE_INVALID_PARAMS: &str = "invalid_params";
/// Error code: journal/serialization failure under a valid request.
pub const CODE_INTERNAL_ERROR: &str = "internal_error";
/// Error code: unknown method name.
pub const CODE_METHOD_NOT_FOUND: &str = "method_not_found";

/// Hard ceiling for `events.list` `limit` (§3.2).
const MAX_LIST_LIMIT: i64 = 1000;

/// Bind/serve failures outside the frame protocol (the frame protocol has
/// its own error envelope via [`ApiError`]).
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// F-11 binds loopback only; anything else is refused at bind time.
    #[error(
        "bind address {addr:?} is not loopback; the F-11 API is loopback-only \
             (token handshake is the documented seam before any non-loopback bind)"
    )]
    NotLoopback {
        /// The rejected address string.
        addr: String,
    },
    /// The address could not be resolved to a socket at all.
    #[error("bind address {addr:?} does not resolve to a socket address")]
    Unresolvable {
        /// The rejected address string.
        addr: String,
    },
    /// OS-level socket failure (bind, accept, …).
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A frame-level error: `{"code","message"}` inside the §2.2 envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// One of the §2.2 codes (`invalid_request`, …).
    pub code: &'static str,
    /// Human-readable detail; the desktop app surfaces it verbatim.
    pub message: String,
}

impl ApiError {
    /// `invalid_request` — unparseable frame or no method (id echoes null).
    fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: CODE_INVALID_REQUEST,
            message: message.into(),
        }
    }

    /// `method_not_found`.
    fn method_not_found(message: impl Into<String>) -> Self {
        Self {
            code: CODE_METHOD_NOT_FOUND,
            message: message.into(),
        }
    }

    /// `invalid_params`.
    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: CODE_INVALID_PARAMS,
            message: message.into(),
        }
    }

    /// `internal_error`.
    fn internal(message: impl Into<String>) -> Self {
        Self {
            code: CODE_INTERNAL_ERROR,
            message: message.into(),
        }
    }

    /// `not_supported` — specified but built in a later slice.
    fn not_supported(message: impl Into<String>) -> Self {
        Self {
            code: CODE_NOT_SUPPORTED,
            message: message.into(),
        }
    }
}

/// The WS server: shared journal plus daemon identity for `daemon.info`,
/// the F-13 agent registry, and the chat-session service.
#[derive(Debug)]
pub struct WsServer {
    journal: Arc<StdMutex<Connection>>,
    journal_path: PathBuf,
    registry: Arc<agentos_agents::AgentRegistry>,
    sessions: Arc<AgentSessions>,
    /// F-12: the mastermind service, when the daemon was built with one.
    /// `None` in fixtures and UI-only daemons — the `mastermind.*` methods
    /// then answer `not_supported` instead of pretending to plan.
    mastermind: Option<Arc<crate::mastermind::Mastermind>>,
    started_at: DateTime<Utc>,
    next_subscription: std::sync::atomic::AtomicU64,
}

impl WsServer {
    /// Build a server over a shared journal connection, the agent registry
    /// (F-13), and the chat-session service. The journal connection is
    /// shared, not pooled: see the [module docs](self). `started_at`
    /// (surfaced by `daemon.info`) is taken here, at server construction,
    /// which `main` does once at startup.
    pub fn new(
        journal: Arc<StdMutex<Connection>>,
        journal_path: impl Into<PathBuf>,
        registry: Arc<agentos_agents::AgentRegistry>,
        sessions: Arc<AgentSessions>,
    ) -> Self {
        Self {
            journal,
            journal_path: journal_path.into(),
            registry,
            sessions,
            mastermind: None,
            started_at: Utc::now(),
            next_subscription: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Attach the F-12 mastermind service (`main` does; fixtures do not).
    pub fn with_mastermind(mut self, mastermind: Arc<crate::mastermind::Mastermind>) -> Self {
        self.mastermind = Some(mastermind);
        self
    }

    /// The mastermind service or a plain `not_supported`.
    fn mastermind(&self) -> Result<&Arc<crate::mastermind::Mastermind>, ApiError> {
        self.mastermind.as_ref().ok_or_else(|| {
            ApiError::not_supported(
                "this daemon was built without the mastermind service (no repo/state dir configured)",
            )
        })
    }

    /// Validate `addr` as loopback and bind it, returning the bound server
    /// (call [`BoundServer::local_addr`] — tests bind `127.0.0.1:0` for an
    /// ephemeral port).
    pub fn bind(self, addr: &str) -> Result<BoundServer, ServerError> {
        let socket = parse_loopback_addr(addr)?;
        let listener = StdTcpListener::bind(socket)?;
        Ok(BoundServer {
            listener,
            server: self,
        })
    }

    /// Run one synchronous journal read under the shared mutex and map any
    /// failure to `internal_error`. The closure must stay synchronous: a
    /// guard crossing an `.await` would pin the journal against every other
    /// peer (see module docs).
    fn with_journal<T>(
        &self,
        read: impl FnOnce(&Connection) -> Result<T, CoreError>,
    ) -> Result<T, ApiError> {
        let conn = self
            .journal
            .lock()
            .map_err(|_| ApiError::internal("journal mutex poisoned"))?;
        read(&conn).map_err(|err| ApiError::internal(format!("journal read failed: {err}")))
    }

    /// Whole-journal fold for `runs.list`/`tasks.list`/`agents.list`.
    /// Re-folding per call is the documented v1 choice (§3.3 — thousands of
    /// rows, no incremental cache required).
    fn fold_all(&self) -> Result<projection::Projection, ApiError> {
        self.with_journal(|conn| events::tail_with_seq(conn, 0, u32::MAX))
            .map(|journal| projection::fold(&journal))
    }

    /// §2.2 `{"id","ok","result"}` envelope.
    ///
    /// Async since F-13: the registry-session arms await adapter spawns
    /// and the free provider-catalog probes. The F-11 invariant that
    /// matters survives — journal guards never cross an await
    /// ([`Self::with_journal`] stays synchronous); only the adapter-facing
    /// arms hold async state.
    async fn dispatch(
        &self,
        method: &str,
        params: &Value,
        out: &mpsc::Sender<Value>,
        subs: &ConnSubs,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<Value, ApiError> {
        match method {
            "ping" => Ok(json!({ "pong": true, "serverTime": now_rfc3339() })),
            "daemon.info" => self.daemon_info(),
            "events.list" => self.events_list(params),
            "events.subscribe" => self.events_subscribe(params, out, subs, shutdown),
            "events.unsubscribe" => self.events_unsubscribe(params, subs),
            "runs.list" => {
                let projection = self.fold_all()?;
                let runs = projection.runs();
                Ok(json!({ "runs": runs }))
            }
            "tasks.list" => {
                let run_id = optional_run_id(params)?;
                let projection = self.fold_all()?;
                let tasks = projection.tasks(run_id.as_deref());
                Ok(json!({ "tasks": tasks }))
            }
            // Provider rate-limit meters for the top bar. A cheap file
            // read, so the desktop polls it rather than subscribing.
            "usage.limits" => Ok(json!({ "meters": crate::usage_meters::meters() })),
            "agents.list" => {
                let run_id = optional_run_id(params)?;
                let projection = self.fold_all()?;
                let agents = projection.agents(run_id.as_deref());
                Ok(json!({ "agents": agents }))
            }
            // ----- F-13: the dynamic agent registry ---------------------
            "registry.agents.list" => {
                let agents = self.registry.list_agents().map_err(registry_error)?;
                Ok(json!({ "agents": agents }))
            }
            "registry.agents.create" => self.registry_create_agent(params),
            "registry.agents.update" => self.registry_update_agent(params),
            "registry.agents.delete" => self.registry_delete_agent(params),
            "registry.agents.set-enabled" => self.registry_set_enabled(params),
            "registry.skills.list" => {
                let skills = self.registry.list_skills().map_err(registry_error)?;
                Ok(json!({ "skills": skills }))
            }
            "registry.skills.available" => self.registry_available_skills(),
            "registry.skills.import" => self.registry_import_skills(params),
            "registry.skills.create" => self.registry_create_skill(params),
            "registry.skills.update" => self.registry_update_skill(params),
            "registry.skills.delete" => self.registry_delete_skill(params),
            "registry.catalog" => {
                let providers = self.sessions.provider_catalog().await;
                Ok(json!({ "providers": providers }))
            }
            "agent.session.start" => {
                let agent_id = required_string(params, "agentId")?;
                let message = required_string(params, "message")?;
                let session_id = self
                    .sessions
                    .start(agent_id, message)
                    .await
                    .map_err(session_error)?;
                Ok(json!({ "sessionId": session_id }))
            }
            // F-13b chat history: past conversations folded out of the
            // journal, and reopening one on the provider session it left
            // behind.
            "chat.sessions" => {
                let agent_id = optional_string(params, "agentId")?;
                let sessions = self
                    .sessions
                    .chat_sessions(agent_id)
                    .map_err(session_error)?;
                Ok(json!({ "sessions": sessions }))
            }
            "chat.transcript" => {
                let session_id = required_string(params, "sessionId")?;
                let messages = self
                    .sessions
                    .chat_transcript(session_id)
                    .map_err(session_error)?;
                let summary = self
                    .sessions
                    .chat_session(session_id)
                    .map_err(session_error)?;
                Ok(json!({ "messages": messages, "session": summary }))
            }
            "agent.session.reopen" => {
                let session_id = required_string(params, "sessionId")?;
                let message = required_string(params, "message")?;
                let new_session = self
                    .sessions
                    .reopen(session_id, message)
                    .await
                    .map_err(session_error)?;
                Ok(json!({ "sessionId": new_session, "resumedFrom": session_id }))
            }
            "agent.session.send" => {
                let session_id = required_string(params, "sessionId")?;
                let message = required_string(params, "message")?;
                self.sessions
                    .send(session_id, message)
                    .await
                    .map_err(session_error)?;
                Ok(json!({ "sent": true }))
            }
            "agent.session.cancel" => {
                let session_id = required_string(params, "sessionId")?;
                self.sessions
                    .cancel(session_id)
                    .await
                    .map_err(session_error)?;
                Ok(json!({ "cancelled": true }))
            }
            // ----- F-12: the mastermind flow -----------------------------
            "mastermind.start" => {
                let goal = required_string(params, "goal")?;
                let repo = required_string(params, "repo")?;
                let planner_adapter = params
                    .get("plannerAdapter")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty());
                let planner_model = params
                    .get("plannerModel")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty());
                self.mastermind()?
                    .start(
                        goal,
                        std::path::Path::new(repo),
                        planner_adapter,
                        planner_model,
                    )
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.plan" | "mastermind.respond" => {
                let session_id = required_string(params, "sessionId")?;
                let instruction = params
                    .get("instruction")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty());
                self.mastermind()?
                    .plan(session_id, instruction)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.commit" => {
                let session_id = required_string(params, "sessionId")?;
                self.mastermind()?
                    .commit(session_id)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.approveDiscovery" => {
                let session_id = required_string(params, "sessionId")?;
                self.mastermind()?
                    .approve_discovery(session_id)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.approvePhase" => {
                let session_id = required_string(params, "sessionId")?;
                self.mastermind()?
                    .approve_phase(session_id)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.acceptPhaseAsIs" => {
                let session_id = required_string(params, "sessionId")?;
                self.mastermind()?
                    .accept_phase_as_is(session_id)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.authorizePhaseWrite" => {
                let session_id = required_string(params, "sessionId")?;
                self.mastermind()?
                    .authorize_phase_write(session_id)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.repreparePhaseRevision" => {
                let session_id = required_string(params, "sessionId")?;
                let guidance = params
                    .get("guidance")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty());
                self.mastermind()?
                    .reprepare_phase_revision(session_id, guidance)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.recoverPhaseArtifact" => {
                let session_id = required_string(params, "sessionId")?;
                self.mastermind()?
                    .recover_phase_artifact(session_id)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.setPlanner" => {
                let session_id = required_string(params, "sessionId")?;
                let planner_adapter = required_string(params, "plannerAdapter")?;
                let planner_model = required_string(params, "plannerModel")?;
                self.mastermind()?
                    .set_planner(session_id, planner_adapter, planner_model)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.retryPhaseAuthoring" => {
                let session_id = required_string(params, "sessionId")?;
                self.mastermind()?
                    .retry_phase_authoring(session_id)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.revisePhase" => {
                let session_id = required_string(params, "sessionId")?;
                let guidance = required_string(params, "guidance")?;
                self.mastermind()?
                    .revise_phase(session_id, guidance)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.drive" => {
                let session_id = required_string(params, "sessionId")?;
                let max_ticks = params
                    .get("maxTicks")
                    .and_then(Value::as_u64)
                    .map(|ticks| ticks.min(u64::from(u32::MAX)) as u32);
                self.mastermind()?
                    .drive(session_id, max_ticks)
                    .await
                    .map_err(mastermind_error)
            }
            // Operator recovery after a cleared transient outage. Separate
            // from `drive` on purpose: resuming a failed run is a decision
            // a person makes, never something a tick infers.
            "mastermind.reopenRun" => {
                let session_id = required_string(params, "sessionId")?;
                self.mastermind()?
                    .reopen_run(session_id)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.status" => {
                let session_id = required_string(params, "sessionId")?;
                self.mastermind()?
                    .status(session_id)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.artifact" => {
                let session_id = required_string(params, "sessionId")?;
                let path = required_string(params, "path")?;
                self.mastermind()?
                    .artifact(session_id, path)
                    .await
                    .map_err(mastermind_error)
            }
            "mastermind.list" => {
                let sessions = self.mastermind()?.list().map_err(mastermind_error)?;
                Ok(json!({ "sessions": sessions }))
            }
            // ----- Git push identity / remote selection -----------------
            "git.push-targets" => git_push_targets(params),
            "git.push-target.set" => git_push_target_set(params),
            "git.diff" => Err(ApiError::not_supported(
                "git.diff is a v1 seam (F-11 §6.1): the daemon does not embed agentos-git yet; \
                 UX-05 renders event-carried attribution",
            )),
            other => Err(ApiError::method_not_found(format!(
                "unknown method {other:?}"
            ))),
        }
    }

    /// `registry.agents.create` (F-13 §3): deserialize the `agent` param,
    /// force the wire record to non-builtin, insert, journal `agent.created`.
    fn registry_create_agent(&self, params: &Value) -> Result<Value, ApiError> {
        let mut record: AgentRecord = agent_param(params)?;
        record.builtin = false; // wire records are never builtin (API-edge policy)
        report_unresolved(self.resolve_skills(&record.skills))?;
        let record = self.registry.create_agent(record).map_err(registry_error)?;
        self.journal_agent_mutation(EVT_AGENT_CREATED, &record, None);
        Ok(json!({ "agent": record }))
    }

    /// Make sure every named skill exists, pulling it out of the local skill
    /// library when the registry has not seen it yet.
    ///
    /// The library (`~/.claude/skills`) holds well-written method documents
    /// this daemon used to be blind to, so an agent naming `react-patterns`
    /// was rejected while the file sat on disk. Naming a skill is now enough
    /// to install it. Returns the ids that could not be resolved at all,
    /// each with its reason.
    pub(crate) fn resolve_skills(&self, skills: &[String]) -> Vec<(String, String)> {
        let mut unresolved = Vec::new();
        let dir = crate::skill_import::source_dir();

        for id in skills {
            match self.registry.get_skill(id) {
                Ok(Some(_)) => continue,
                Ok(None) => {}
                Err(err) => {
                    unresolved.push((id.clone(), err.to_string()));
                    continue;
                }
            }
            match crate::skill_import::load(&dir, id) {
                Ok(record) => match self.registry.create_skill(record) {
                    Ok(record) => {
                        tracing::info!(skill = %record.id, "imported skill from the local library");
                        self.journal_skill_mutation(EVT_SKILL_CREATED, &record);
                    }
                    Err(err) => unresolved.push((id.clone(), err.to_string())),
                },
                Err(reason) => unresolved.push((id.clone(), reason)),
            }
        }
        unresolved
    }

    /// `registry.skills.available`: what is importable from disk. Cheap by
    /// design — frontmatter only, no bodies — so a directory of a thousand
    /// skills lists without reading a thousand files end to end.
    fn registry_available_skills(&self) -> Result<Value, ApiError> {
        let dir = crate::skill_import::source_dir();
        let installed: Vec<String> = self
            .registry
            .list_skills()
            .map_err(registry_error)?
            .into_iter()
            .map(|skill| skill.id)
            .collect();
        let skills = crate::skill_import::discover(&dir, &installed);
        Ok(json!({
            "sourceDir": dir.display().to_string(),
            "skills": skills,
        }))
    }

    /// `registry.skills.import`: install the named skills from disk.
    ///
    /// Partial success is the contract: one unreadable or oversized file
    /// must not lose the other forty-nine imports, so every id reports its
    /// own outcome and the call itself succeeds.
    fn registry_import_skills(&self, params: &Value) -> Result<Value, ApiError> {
        let ids = params
            .get("ids")
            .and_then(Value::as_array)
            .ok_or_else(|| ApiError::invalid_params("ids must be an array of skill ids"))?;
        if ids.is_empty() {
            return Err(ApiError::invalid_params("ids must name at least one skill"));
        }

        let dir = crate::skill_import::source_dir();
        let mut imported = Vec::new();
        let mut skipped = Vec::new();

        for id in ids {
            let Some(id) = id.as_str() else {
                skipped.push(json!({ "id": id, "reason": "not a string" }));
                continue;
            };
            match crate::skill_import::load(&dir, id) {
                Ok(record) => match self.registry.create_skill(record) {
                    Ok(record) => {
                        self.journal_skill_mutation(EVT_SKILL_CREATED, &record);
                        imported.push(record.id);
                    }
                    Err(err) => skipped.push(json!({ "id": id, "reason": err.to_string() })),
                },
                Err(reason) => skipped.push(json!({ "id": id, "reason": reason })),
            }
        }

        Ok(json!({ "imported": imported, "skipped": skipped }))
    }

    /// `registry.skills.create`: install a new skill. Wire records are never
    /// builtin — a seeded skill is the daemon's own, not something an API
    /// caller can mint.
    fn registry_create_skill(&self, params: &Value) -> Result<Value, ApiError> {
        let mut record: SkillRecord = skill_param(params)?;
        record.builtin = false;
        let record = self.registry.create_skill(record).map_err(registry_error)?;
        self.write_through(&record);
        self.journal_skill_mutation(EVT_SKILL_CREATED, &record);
        Ok(json!({ "skill": record }))
    }

    /// `registry.skills.update`: edit an existing skill's body or metadata.
    fn registry_update_skill(&self, params: &Value) -> Result<Value, ApiError> {
        let mut record: SkillRecord = skill_param(params)?;
        let previous = self
            .registry
            .get_skill(&record.id)
            .map_err(registry_error)?
            .ok_or_else(|| {
                ApiError::invalid_params(format!("skill {:?} does not exist", record.id))
            })?;
        record.builtin = previous.builtin; // preserved by the registry
        let record = self.registry.update_skill(record).map_err(registry_error)?;
        self.write_through(&record);
        self.journal_skill_mutation(EVT_SKILL_UPDATED, &record);
        Ok(json!({ "skill": record }))
    }

    /// `registry.skills.delete`: the registry refuses while an agent still
    /// holds the skill, so a delete cannot strand a roster entry.
    fn registry_delete_skill(&self, params: &Value) -> Result<Value, ApiError> {
        let id = required_string(params, "id")?;
        let record = self
            .registry
            .get_skill(id)
            .map_err(registry_error)?
            .ok_or_else(|| ApiError::invalid_params(format!("skill {id:?} does not exist")))?;
        self.registry.delete_skill(id).map_err(registry_error)?;
        self.remove_write_through(id);
        self.journal_skill_mutation(EVT_SKILL_DELETED, &record);
        Ok(json!({ "deleted": id }))
    }

    /// Mirror a skill into the library so the two never diverge.
    ///
    /// A write that fails is logged, not fatal: the registry row is already
    /// committed, and a read-only library should not fail the call.
    fn write_through(&self, record: &SkillRecord) {
        let dir = crate::skill_import::source_dir();
        if let Err(err) = crate::skill_import::write_one(&dir, record) {
            tracing::warn!(skill = %record.id, %err, "could not mirror skill into the library");
        }
    }

    /// Drop a deleted skill's mirror so the library does not keep listing
    /// it. Same failure posture as `write_through`: the registry row is
    /// already gone, and a read-only library must not fail the call.
    fn remove_write_through(&self, id: &str) {
        let dir = crate::skill_import::source_dir();
        if let Err(err) = crate::skill_import::remove_one(&dir, id) {
            tracing::warn!(skill = %id, %err, "could not remove the skill from the library");
        }
    }

    /// Journal one skill mutation. Skills are not agent-scoped, so the
    /// event carries no `agent_id`; the payload is the whole record.
    fn journal_skill_mutation(&self, event_type: &str, record: &SkillRecord) {
        let event = Event::new(EventType::Other(event_type.to_owned()))
            .with_payload(json!({ "skill": record }));
        match crate::db::open_db(&self.journal_path)
            .and_then(|conn| events::append_event(&conn, &event))
        {
            Ok(_seq) => {}
            Err(err) => tracing::error!(%err, "skill mutation journal append failed"),
        }
    }

    /// `registry.agents.update`: same shape, `agent.updated` journal event.
    fn registry_update_agent(&self, params: &Value) -> Result<Value, ApiError> {
        let mut record: AgentRecord = agent_param(params)?;
        let previous = self
            .registry
            .get_agent(&record.id)
            .map_err(registry_error)?
            .ok_or_else(|| {
                ApiError::invalid_params(format!("agent {:?} does not exist", record.id))
            })?;
        record.builtin = previous.builtin; // preserved by the registry; keep the wire honest
        report_unresolved(self.resolve_skills(&record.skills))?;
        let record = self.registry.update_agent(record).map_err(registry_error)?;
        self.journal_agent_mutation(EVT_AGENT_UPDATED, &record, None);
        Ok(json!({ "agent": record }))
    }

    /// `registry.agents.delete`: `agent.deleted` journal event carries the
    /// full record (the table row is gone; the journal keeps the history).
    fn registry_delete_agent(&self, params: &Value) -> Result<Value, ApiError> {
        let id = required_string(params, "id")?;
        let record = self
            .registry
            .get_agent(id)
            .map_err(registry_error)?
            .ok_or_else(|| ApiError::invalid_params(format!("agent {id:?} does not exist")))?;
        self.registry.delete_agent(id).map_err(registry_error)?;
        self.journal_agent_mutation(EVT_AGENT_DELETED, &record, None);
        Ok(json!({ "deleted": true, "id": id }))
    }

    /// `registry.agents.set-enabled`: routing opt-in/out, journaled as an
    /// update (the enabled flag is part of the record).
    fn registry_set_enabled(&self, params: &Value) -> Result<Value, ApiError> {
        let id = required_string(params, "id")?;
        let enabled = match params.get("enabled") {
            Some(Value::Bool(value)) => *value,
            _ => return Err(ApiError::invalid_params("enabled must be a boolean")),
        };
        self.registry
            .set_agent_enabled(id, enabled)
            .map_err(registry_error)?;
        let record = self
            .registry
            .get_agent(id)
            .map_err(registry_error)?
            .unwrap_or_else(|| AgentRecord {
                id: id.to_owned(),
                name: String::new(),
                description: String::new(),
                adapter_id: String::new(),
                model: None,
                effort: None,
                mode: agentos_agents::AgentMode::Plan,
                skills: vec![],
                tool_allowlist: vec![],
                tool_denylist: vec![],
                timeout_secs: 0,
                builtin: false,
                enabled,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            });
        self.journal_agent_mutation(EVT_AGENT_UPDATED, &record, None);
        Ok(json!({ "agent": record }))
    }

    /// Append one registry-mutation audit event (fresh connection per
    /// append — the supervisor's WAL pattern; failures are logged loudly
    /// but never fail the mutation they describe).
    fn journal_agent_mutation(&self, event_type: &str, record: &AgentRecord, extra: Option<Value>) {
        let mut payload = json!({ "agent": record });
        if let Some(extra) = extra {
            payload["details"] = extra;
        }
        let event = Event::new(EventType::Other(event_type.to_owned()))
            .with_agent_id(record.id.clone())
            .with_payload(payload);
        match crate::db::open_db(&self.journal_path)
            .and_then(|conn| events::append_event(&conn, &event))
        {
            Ok(_seq) => {}
            Err(err) => tracing::error!(%err, "registry mutation journal append failed"),
        }
    }

    /// `daemon.info` (§3.2): identity plus a one-query journal snapshot.
    fn daemon_info(&self) -> Result<Value, ApiError> {
        let stats = self.with_journal(events::journal_stats)?;
        Ok(json!({
            "version": env!("CARGO_PKG_VERSION"),
            "pid": std::process::id(),
            "journalPath": self.journal_path.display().to_string(),
            "eventCount": stats.event_count,
            "lastSeq": stats.last_seq,
            "startedAt": rfc3339(&self.started_at),
        }))
    }

    /// `events.list` (§3.2): `afterSeq`/`limit` validation, then one
    /// `limit + 1` fetch so `truncated` is exact without a second query.
    fn events_list(&self, params: &Value) -> Result<Value, ApiError> {
        let after_seq = optional_non_negative_i64(params, "afterSeq")?.unwrap_or(0);
        let limit = match optional_non_negative_i64(params, "limit")? {
            Some(0) => return Err(ApiError::invalid_params("limit must be >= 1")),
            Some(limit) if limit > MAX_LIST_LIMIT => {
                return Err(ApiError::invalid_params(format!(
                    "limit must be <= {MAX_LIST_LIMIT}"
                )))
            }
            limit => limit.unwrap_or(200),
        };

        let (stats, mut fetched) = self.with_journal(|conn| -> Result<_, CoreError> {
            let stats = events::journal_stats(conn)?;
            // Fetch one extra row to compute `truncated` exactly.
            let fetched = events::tail_with_seq(conn, after_seq, limit as u32 + 1)?;
            Ok((stats, fetched))
        })?;
        let truncated = fetched.len() as i64 > limit;
        if truncated {
            fetched.truncate(limit as usize);
        }
        Ok(json!({
            "events": fetched.iter().map(sequenced_to_wire).collect::<Vec<_>>(),
            "lastSeq": stats.last_seq,
            "truncated": truncated,
        }))
    }

    /// `events.subscribe` (§3.2): register the subscription and spawn its
    /// replay-then-tail task; the reply carries only the id, replay frames
    /// arrive as §2.3 notifications.
    fn events_subscribe(
        &self,
        params: &Value,
        out: &mpsc::Sender<Value>,
        subs: &ConnSubs,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<Value, ApiError> {
        let after_seq = optional_non_negative_i64(params, "afterSeq")?.unwrap_or(0);
        let id = format!(
            "sub-{}",
            self.next_subscription
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let (stop_tx, stop_rx) = watch::channel(false);
        subs.lock()
            .map_err(|_| ApiError::internal("subscription registry poisoned"))?
            .insert(id.clone(), stop_tx);
        tokio::spawn(subscription_task(
            Arc::clone(&self.journal),
            out.clone(),
            stop_rx,
            shutdown.clone(),
            id.clone(),
            after_seq,
        ));
        Ok(json!({ "subscriptionId": id }))
    }

    /// `events.unsubscribe` (§3.2): `{"stopped": bool}` — true iff an
    /// active subscription was signalled to stop (it then emits
    /// `subscription.closed` reason `unsubscribed`).
    fn events_unsubscribe(&self, params: &Value, subs: &ConnSubs) -> Result<Value, ApiError> {
        let id = required_string(params, "subscriptionId")?;
        let stopped = subs
            .lock()
            .map_err(|_| ApiError::internal("subscription registry poisoned"))?
            .remove(id)
            .map(|stop_tx| stop_tx.send(true).is_ok())
            .unwrap_or(false);
        Ok(json!({ "stopped": stopped }))
    }

    /// One inbound TEXT frame → one response frame. Async since F-13 (the
    /// registry-session arms await adapters); the invariant that matters is
    /// unchanged: journal guards live inside synchronous closures and never
    /// cross an await point.
    async fn handle_text_frame(
        &self,
        out: &mpsc::Sender<Value>,
        subs: &ConnSubs,
        shutdown: &watch::Receiver<bool>,
        text: &str,
    ) -> Value {
        let (id, method, params) = match parse_request(text) {
            Ok(parsed) => parsed,
            // §2.2: malformed frames (unparseable, no method, bad id/params
            // shape) are answered with `id: null`.
            Err(err) => return error_response(Value::Null, err),
        };
        let params = params.unwrap_or_else(|| json!({}));
        match self.dispatch(&method, &params, out, subs, shutdown).await {
            Ok(result) => ok_response(id, result),
            Err(err) => error_response(id, err),
        }
    }

    /// Accept loop body: WS handshake, writer task, then the reader loop
    /// until the peer closes, the channel dies, or the daemon shuts down.
    async fn handle_connection(
        self: Arc<Self>,
        tcp: TcpStream,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let ws = match tokio_tungstenite::accept_async(tcp).await {
            Ok(ws) => ws,
            Err(err) => {
                debug!(%err, "websocket handshake failed");
                return;
            }
        };
        let (out_tx, out_rx) = mpsc::channel::<Value>(OUTBOUND_CAPACITY);
        let (sink, mut stream) = ws.split();
        tokio::spawn(writer_task(sink, out_rx, shutdown.clone()));

        let subs: ConnSubs = Arc::new(StdMutex::new(HashMap::new()));
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                maybe = stream.next() => match maybe {
                    Some(Ok(Message::Text(text))) => {
                        // Requests on one socket are independent and carry
                        // correlation ids, so dispatch them independently.
                        // A provider-backed call (notably mastermind.plan)
                        // can take minutes. Awaiting it in this reader loop
                        // prevented the same socket from answering the UI's
                        // application-level heartbeat, which then closed a
                        // healthy connection after ten seconds.
                        let server = Arc::clone(&self);
                        let out = out_tx.clone();
                        let request_subs = Arc::clone(&subs);
                        let request_shutdown = shutdown.clone();
                        tokio::spawn(async move {
                            let response = server
                                .handle_text_frame(
                                    &out,
                                    &request_subs,
                                    &request_shutdown,
                                    text.as_str(),
                                )
                                .await;
                            // A failed send only means this connection ended;
                            // the reader owns teardown and no task may close
                            // a replacement connection.
                            let _ = out.send(response).await;
                        });
                    }
                    Some(Ok(Message::Binary(_))) => {
                        // §2: one JSON object per TEXT frame, no binary.
                        let response = error_response(
                            Value::Null,
                            ApiError::invalid_request(
                                "binary frames are not part of the F-11 framing",
                            ),
                        );
                        if out_tx.send(response).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {} // Ping/Pong: tungstenite answers pings
                    Some(Err(err)) => {
                        debug!(%err, "websocket read failed");
                        break;
                    }
                    None => break,
                },
            }
        }

        // Teardown: signal every live subscription so its task observes the
        // stop watch and exits instead of polling a dead channel. The
        // writer task drains once all senders (this one and the
        // subscriptions') are dropped.
        let Ok(mut registry) = subs.lock() else {
            return;
        };
        for (_, stop_tx) in registry.drain() {
            let _ = stop_tx.send(true);
        }
    }
}

/// A [`WsServer`] with its loopback socket bound, ready to [`serve`].
#[derive(Debug)]
pub struct BoundServer {
    listener: StdTcpListener,
    server: WsServer,
}

impl BoundServer {
    /// The actually-bound address (the ephemeral port when bound to `:0`).
    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound listener always has a local addr")
    }

    /// Accept connections until `shutdown` flips true, then wait for every
    /// live connection to observe it (each broadcasts `daemon.stopping` and
    /// closes 1001) before returning — callers can await the handle and
    /// know the socket is quiet.
    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
        self.listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(self.listener)?;
        let server = Arc::new(self.server);
        let mut connections = JoinSet::new();

        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((stream, _peer)) => {
                        let server = Arc::clone(&server);
                        let shutdown = shutdown.clone();
                        connections.spawn(async move {
                            server.handle_connection(stream, shutdown).await
                        });
                    }
                    Err(err) => warn!(%err, "accept failed"),
                },
                _ = shutdown.changed() => break,
            }
        }

        // handle_connection and every subscription select on the same
        // shutdown watch, so the JoinSet drains promptly.
        while connections.join_next().await.is_some() {}
        Ok(())
    }
}

/// Resolve `addr` (`host:port`) and refuse anything non-loopback (§2).
fn parse_loopback_addr(addr: &str) -> Result<SocketAddr, ServerError> {
    let socket = match addr.parse::<SocketAddr>() {
        Ok(socket) => Some(socket),
        Err(_) => match addr.to_socket_addrs() {
            Ok(mut resolved) => resolved.next(),
            Err(err) => {
                warn!(addr, %err, "bind address does not resolve");
                None
            }
        },
    };
    let Some(socket) = socket else {
        return Err(ServerError::Unresolvable {
            addr: addr.to_owned(),
        });
    };
    if socket.ip().is_loopback() {
        Ok(socket)
    } else {
        warn!(addr, %socket, "refusing non-loopback bind");
        Err(ServerError::NotLoopback {
            addr: addr.to_owned(),
        })
    }
}

/// The bind address for this process: `AGENTOS_WS_ADDR` override, else the
/// default (loopback).
pub fn ws_addr_from_env() -> String {
    std::env::var(WS_ADDR_ENV).unwrap_or_else(|_| DEFAULT_WS_ADDR.to_owned())
}

/// Per-connection subscription registry: id → stop switch. Shared with the
/// subscription tasks only through `events_unsubscribe` (same connection
/// task), so a plain std mutex is contention-free.
type ConnSubs = Arc<StdMutex<HashMap<String, watch::Sender<bool>>>>;

/// Replay-then-tail loop for one subscription (§3.2).
///
/// Gapless and at-least-once per seq: `last_seq` starts at the request's
/// `afterSeq`, every frame advances it only after the frame was queued, and
/// the loop never skips — replay (`seq > afterSeq`) and live tail are the
/// same query. Idle polls sleep [`TAIL_POLL_INTERVAL_MS`]; a non-empty
/// batch is drained before sleeping so replay runs at full speed.
///
/// Delivery is `try_send`: a full bounded buffer means the peer cannot keep
/// up, and the subscription is closed with reason `slow_consumer` rather
/// than blocking handlers or growing memory (§2).
async fn subscription_task(
    journal: Arc<StdMutex<Connection>>,
    out: mpsc::Sender<Value>,
    mut stop: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
    subscription_id: String,
    mut last_seq: i64,
) {
    loop {
        if *stop.borrow() {
            let _ = out
                .send(closed_notification(&subscription_id, "unsubscribed"))
                .await;
            break;
        }
        if *shutdown.borrow() || out.is_closed() {
            break; // daemon.stopping / connection teardown already announce it
        }

        let batch = match journal
            .lock()
            .map_err(|_| CoreError::Serialization("journal mutex poisoned".to_owned()))
            .and_then(|conn| events::tail_with_seq(&conn, last_seq, TAIL_BATCH))
        {
            Ok(batch) => batch,
            Err(err) => {
                warn!(subscription = %subscription_id, %err, "subscription tail failed");
                let _ = out
                    .send(closed_notification(&subscription_id, "journal_error"))
                    .await;
                break;
            }
        };

        if batch.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(TAIL_POLL_INTERVAL_MS)) => {}
                _ = stop.changed() => {
                    let _ = out
                        .send(closed_notification(&subscription_id, "unsubscribed"))
                        .await;
                    break;
                }
                _ = shutdown.changed() => break,
            }
            continue;
        }

        let mut overflowed = false;
        for sequenced in &batch {
            last_seq = sequenced.seq;
            if out
                .try_send(event_notification(&subscription_id, sequenced))
                .is_err()
            {
                overflowed = true;
                break;
            }
        }
        if overflowed {
            // Bounded buffer full: the peer is a slow consumer. The close
            // frame uses a blocking send — the writer is still draining, it
            // is only behind — so the reason reliably reaches the client.
            warn!(subscription = %subscription_id, "slow consumer; closing subscription");
            let _ = out
                .send(closed_notification(&subscription_id, "slow_consumer"))
                .await;
            break;
        }
    }
}

/// One frame → one TEXT message on the sink (§2 framing).
async fn send_json<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    frame: &Value,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let text: String = frame.to_string();
    sink.send(Message::Text(text.into())).await
}

/// Drain the outbound channel to the socket; on daemon shutdown, flush what
/// is queued, broadcast `daemon.stopping`, then close with 1001 (§2.3).
async fn writer_task<S>(
    mut sink: SplitSink<WebSocketStream<S>, Message>,
    mut rx: mpsc::Receiver<Value>,
    mut shutdown: watch::Receiver<bool>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut stopping = *shutdown.borrow();
    loop {
        if stopping {
            break;
        }
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(frame) => {
                    if send_json(&mut sink, &frame).await.is_err() {
                        return; // socket dead; nothing to announce
                    }
                }
                None => return, // all senders dropped (connection teardown)
            },
            _ = shutdown.changed() => stopping = true,
        }
    }

    // Best-effort drain of already-queued frames so late responses are not
    // lost behind the announcement, then stopping + close 1001.
    while let Ok(frame) = rx.try_recv() {
        if send_json(&mut sink, &frame).await.is_err() {
            return;
        }
    }
    let _ = send_json(&mut sink, &json!({ "notification": "daemon.stopping" })).await;
    let _ = sink
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Away, // 1001 (Going Away)
            reason: "".into(),
        })))
        .await;
}

/// Parse one inbound TEXT frame into `(id, method, params?)`.
fn parse_request(text: &str) -> Result<(Value, String, Option<Value>), ApiError> {
    let root: Value = serde_json::from_str(text)
        .map_err(|err| ApiError::invalid_request(format!("frame is not valid JSON: {err}")))?;
    let object = root
        .as_object()
        .ok_or_else(|| ApiError::invalid_request("frame must be a JSON object"))?;
    // §2: requests carry a client-chosen id (u64 or string); frames with
    // no id are notifications, and those are a server → client-only shape,
    // so a client frame without one is malformed.
    match object.get("id") {
        Some(Value::Number(_)) | Some(Value::String(_)) => {}
        _ => {
            return Err(ApiError::invalid_request(
                "id must be present and a number or a string",
            ))
        }
    }
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .filter(|method| !method.is_empty())
        .ok_or_else(|| ApiError::invalid_request("frame must carry a non-empty string method"))?;
    let params = match object.get("params") {
        None | Some(Value::Null) => None,
        Some(value @ Value::Object(_)) => Some(value.clone()),
        Some(_) => {
            return Err(ApiError::invalid_request(
                "params must be an object when present",
            ))
        }
    };
    // The match above guarantees presence; the fallback is unreachable.
    Ok((
        object.get("id").cloned().unwrap_or(Value::Null),
        method.to_owned(),
        params,
    ))
}

/// §2.2 success envelope.
fn ok_response(id: Value, result: Value) -> Value {
    json!({ "id": id, "ok": true, "result": result })
}

/// §2.2 error envelope.
fn error_response(id: Value, error: ApiError) -> Value {
    json!({ "id": id, "ok": false, "error": { "code": error.code, "message": error.message } })
}

/// §3.1 wire shape: the core event serde plus the journal `seq`.
fn sequenced_to_wire(sequenced: &SequencedEvent) -> Value {
    let mut wire = serde_json::to_value(&sequenced.event)
        .unwrap_or_else(|_| json!({ "id": null, "eventType": "serialization.failed" }));
    if let Value::Object(map) = &mut wire {
        map.insert("seq".to_owned(), json!(sequenced.seq));
    }
    wire
}

/// §2.3 `event` notification.
fn event_notification(subscription_id: &str, sequenced: &SequencedEvent) -> Value {
    json!({
        "notification": "event",
        "subscriptionId": subscription_id,
        "seq": sequenced.seq,
        "event": sequenced_to_wire(sequenced),
    })
}

/// §2.3 `subscription.closed` notification.
fn closed_notification(subscription_id: &str, reason: &str) -> Value {
    json!({
        "notification": "subscription.closed",
        "subscriptionId": subscription_id,
        "reason": reason,
    })
}

/// Optional non-negative integer parameter (§3.2 `afterSeq`/`limit`).
/// Floats and strings are rejected: seqs are i64, not estimates.
fn optional_non_negative_i64(params: &Value, key: &str) -> Result<Option<i64>, ApiError> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_i64()
            .filter(|value| *value >= 0)
            .map(Some)
            .ok_or_else(|| {
                ApiError::invalid_params(format!("{key} must be a non-negative integer"))
            }),
        Some(_) => Err(ApiError::invalid_params(format!(
            "{key} must be a non-negative integer"
        ))),
    }
}

/// Required non-empty string parameter.
/// An optional non-empty string parameter: absent and empty both read as
/// "no filter".
fn optional_string<'a>(params: &'a Value, key: &str) -> Result<Option<&'a str>, ApiError> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if !text.is_empty() => Ok(Some(text.as_str())),
        Some(Value::String(_)) => Ok(None),
        Some(_) => Err(ApiError::invalid_params(format!("{key} must be a string"))),
    }
}

fn required_string<'a>(params: &'a Value, key: &str) -> Result<&'a str, ApiError> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::invalid_params(format!("{key} must be a non-empty string")))
}

/// Read the named Git remote that plain `git push` will use. Git
/// authentication itself stays with the OS credential manager or SSH agent;
/// this API neither reads nor stores credentials.
fn git_push_targets(params: &Value) -> Result<Value, ApiError> {
    let repo = PathBuf::from(required_string(params, "repo")?);
    git_push_profile(&repo)
}

/// Set Git's repository-local `remote.pushDefault`. The remote name must
/// already exist in the selected repository, so an RPC caller cannot inject
/// an arbitrary destination.
fn git_push_target_set(params: &Value) -> Result<Value, ApiError> {
    let repo = PathBuf::from(required_string(params, "repo")?);
    let remote = required_string(params, "remote")?;
    let profile = git_push_profile(&repo)?;
    let known = profile["remotes"]
        .as_array()
        .is_some_and(|remotes| remotes.iter().any(|entry| entry["name"] == remote));
    if !known {
        return Err(ApiError::invalid_params(format!(
            "remote {remote:?} is not configured for {}",
            repo.display()
        )));
    }
    let output = git_output(&repo, &["config", "--local", "remote.pushDefault", remote])?;
    if !output.status.success() {
        return Err(git_command_error(&repo, "set push default", &output));
    }
    git_push_profile(&repo)
}

/// Build the credential-safe Git push profile surfaced in Settings. A remote
/// URL identifies the authentication context (for example `github-work` in
/// an SSH URL); the credential manager/SSH agent remains the sole secret
/// holder.
fn git_push_profile(repo: &Path) -> Result<Value, ApiError> {
    if !repo.is_dir() {
        return Err(ApiError::invalid_params(format!(
            "repo {} does not exist",
            repo.display()
        )));
    }
    let remote_output = git_output(repo, &["remote"])?;
    if !remote_output.status.success() {
        return Err(git_command_error(repo, "list remotes", &remote_output));
    }
    let selected = git_config_value(repo, "remote.pushDefault")?;
    let remotes = String::from_utf8_lossy(&remote_output.stdout)
        .lines()
        .filter(|name| !name.trim().is_empty())
        .map(|name| {
            let name = name.trim();
            let url = git_output(repo, &["remote", "get-url", "--push", name])?;
            if !url.status.success() {
                return Err(git_command_error(repo, "read push remote", &url));
            }
            Ok(json!({
                "name": name,
                "pushUrl": String::from_utf8_lossy(&url.stdout).trim(),
                "selected": selected.as_deref() == Some(name),
            }))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(json!({
        "repo": repo.display().to_string(),
        "pushDefault": selected,
        "identity": {
            "name": git_config_value(repo, "user.name")?,
            "email": git_config_value(repo, "user.email")?,
        },
        "remotes": remotes,
    }))
}

fn git_config_value(repo: &Path, key: &str) -> Result<Option<String>, ApiError> {
    let output = git_output(repo, &["config", "--get", key])?;
    if output.status.success() {
        return Ok(Some(
            String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        ));
    }
    if output.status.code() == Some(1) {
        return Ok(None);
    }
    Err(git_command_error(repo, "read Git config", &output))
}

fn git_output(repo: &Path, args: &[&str]) -> Result<Output, ApiError> {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|error| {
            ApiError::internal(format!("could not run git in {}: {error}", repo.display()))
        })
}

fn git_command_error(repo: &Path, action: &str, output: &Output) -> ApiError {
    let detail = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    ApiError::invalid_params(format!(
        "Git could not {action} in {}: {detail}",
        repo.display()
    ))
}

/// The `agent` parameter of the registry mutation methods: an object
/// deserializing to an [`AgentRecord`] (wire-friendly — server-owned
/// fields default; `builtin` is forced at the API edge).
fn agent_param(params: &Value) -> Result<AgentRecord, ApiError> {
    let agent = params.get("agent").ok_or_else(|| {
        ApiError::invalid_params("agent must be an object (the full agent record)")
    })?;
    serde_json::from_value(agent.clone())
        .map_err(|err| ApiError::invalid_params(format!("agent record is not valid: {err}")))
}

/// Turn unresolved skill ids into one actionable error naming all of them.
fn report_unresolved(unresolved: Vec<(String, String)>) -> Result<(), ApiError> {
    if unresolved.is_empty() {
        return Ok(());
    }
    let detail = unresolved
        .iter()
        .map(|(id, reason)| format!("{id} ({reason})"))
        .collect::<Vec<_>>()
        .join(", ");
    Err(ApiError::invalid_params(format!(
        "these skills are neither registered nor in the local skill library: {detail}"
    )))
}

fn skill_param(params: &Value) -> Result<SkillRecord, ApiError> {
    let skill = params.get("skill").ok_or_else(|| {
        ApiError::invalid_params("skill must be an object (the full skill record)")
    })?;
    serde_json::from_value(skill.clone())
        .map_err(|err| ApiError::invalid_params(format!("skill record is not valid: {err}")))
}

/// Registry failures → §2.2 codes: domain rejections are `invalid_params`
/// (the client can fix them); storage failures are `internal_error`.
fn registry_error(err: agentos_agents::AgentsError) -> ApiError {
    match err {
        agentos_agents::AgentsError::Core(core) => {
            ApiError::internal(format!("registry storage failure: {core}"))
        }
        other => ApiError::invalid_params(other.to_string()),
    }
}

/// Chat-session failures → §2.2 codes.
fn session_error(err: SessionError) -> ApiError {
    match err {
        SessionError::InvalidParams(detail) => ApiError::invalid_params(detail),
        SessionError::NotFound(detail) => ApiError::invalid_params(detail),
        SessionError::Registry(registry) => registry_error(registry),
        // A caller mistake stays a caller mistake even when the adapter is
        // the one that spots it: sending to a finished session, sending
        // mid-turn, or asking for a capability the runtime has not got are
        // all `invalid_params` everywhere else on this API.
        SessionError::Adapter(
            adapter @ (agentos_adapters::AdapterError::SessionNotActive(_)
            | agentos_adapters::AdapterError::Busy(_)
            | agentos_adapters::AdapterError::Unsupported(_)),
        ) => ApiError::invalid_params(adapter.to_string()),
        SessionError::Adapter(adapter) => ApiError::internal(format!("adapter failure: {adapter}")),
        SessionError::Core(core) => ApiError::internal(format!("journal failure: {core}")),
        SessionError::Internal(detail) => ApiError::internal(detail),
    }
}

/// F-12 failures: a caller mistake (bad session id, missing repo, no commit
/// yet) is `invalid_params`; anything else is the machinery failing.
fn mastermind_error(err: crate::mastermind::MastermindError) -> ApiError {
    use crate::mastermind::MastermindError as E;
    match err {
        E::UnknownSession(detail) => ApiError::invalid_params(format!("unknown session {detail}")),
        E::NotCommitted(detail) => {
            ApiError::invalid_params(format!("session {detail} has no committed run yet"))
        }
        E::NoRepo(path) => {
            ApiError::invalid_params(format!("repo {} does not exist", path.display()))
        }
        E::NoHead { .. } => ApiError::invalid_params(err.to_string()),
        // Reopening a run that is not failed is a caller mistake, not a
        // daemon fault: the engine's own status says there is nothing
        // stranded to recover.
        E::RunNotFailed { .. } => ApiError::invalid_params(err.to_string()),
        E::DiscoveryIncomplete(_)
        | E::DiscoveryAwaitingApproval(_)
        | E::InvalidPhase { .. }
        | E::UnknownArtifact { .. } => ApiError::invalid_params(err.to_string()),
        E::Registry(registry) => registry_error(registry),
        other => ApiError::internal(other.to_string()),
    }
}

/// Optional `runId` parameter, validated as a UUID and normalized to the
/// canonical hyphenated string the projections key on.
fn optional_run_id(params: &Value) -> Result<Option<String>, ApiError> {
    match params.get("runId") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => raw
            .parse::<Uuid>()
            .map(|id| Some(id.to_string()))
            .map_err(|_| ApiError::invalid_params("runId must be a UUID")),
        Some(_) => Err(ApiError::invalid_params("runId must be a UUID string")),
    }
}

/// RFC 3339 with `Z` suffix — the timestamp convention of every §2/§3 field.
fn now_rfc3339() -> String {
    rfc3339(&Utc::now())
}

fn rfc3339(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §2.2 framing: parse accepts the canonical request shape and rejects
    /// every malformed variant with `invalid_request`.
    #[test]
    fn request_parsing_follows_the_framing_contract() {
        let (id, method, params) =
            parse_request(r#"{"id": 7, "method": "events.list", "params": {"afterSeq": 0}}"#)
                .expect("canonical frame parses");
        assert_eq!(id, json!(7));
        assert_eq!(method, "events.list");
        assert_eq!(params.expect("params present")["afterSeq"], json!(0));

        // No params / null params are both "absent".
        let (_, method, params) =
            parse_request(r#"{"id": "abc", "method": "ping"}"#).expect("params-less frame");
        assert_eq!(method, "ping");
        assert!(params.is_none());
        let (_, _, params) =
            parse_request(r#"{"id": 1, "method": "ping", "params": null}"#).unwrap();
        assert!(params.is_none());

        for malformed in [
            "not json",
            "[]",
            r#"{"id": 3}"#,
            r#"{"id": 3, "method": ""}"#,
            r#"{"method": "ping"}"#, // no id is a notification shape → malformed
            r#"{"id": true, "method": "ping"}"#,
            r#"{"id": 1, "method": "ping", "params": 5}"#,
        ] {
            let err = parse_request(malformed)
                .expect_err("frame must be rejected")
                .code;
            assert_eq!(err, CODE_INVALID_REQUEST, "frame: {malformed}");
        }
    }

    /// Loopback enforcement (§2): anything binding off-loopback is refused
    /// before a socket exists.
    #[test]
    fn non_loopback_addresses_are_refused_at_parse() {
        assert!(parse_loopback_addr("127.0.0.1:8741").is_ok());
        assert!(parse_loopback_addr("127.0.0.1:0").is_ok());
        assert!(parse_loopback_addr("[::1]:8741").is_ok());
        assert!(matches!(
            parse_loopback_addr("0.0.0.0:8741"),
            Err(ServerError::NotLoopback { .. })
        ));
        assert!(matches!(
            parse_loopback_addr("192.168.1.5:8741"),
            Err(ServerError::NotLoopback { .. })
        ));
        assert!(matches!(
            parse_loopback_addr("192.168.1.5"),
            Err(ServerError::Unresolvable { .. }) // no port: unresolvable
        ));
    }

    /// `mastermind.reopenRun` refusals are the caller's fault, not the
    /// daemon's: asking to reopen a run that is not failed, or naming a
    /// session that does not exist, must answer `invalid_params` so the
    /// desktop can show the reason instead of an opaque internal error.
    #[test]
    fn reopening_a_run_that_is_not_failed_is_a_caller_error() {
        use crate::mastermind::MastermindError as E;

        let not_failed = mastermind_error(E::RunNotFailed {
            session: "sess-1".to_owned(),
            status: "completed".to_owned(),
        });
        assert_eq!(not_failed.code, CODE_INVALID_PARAMS);
        assert!(not_failed.message.contains("not failed"), "{not_failed:?}");

        assert_eq!(
            mastermind_error(E::NotCommitted("sess-1".to_owned())).code,
            CODE_INVALID_PARAMS
        );
        assert_eq!(
            mastermind_error(E::UnknownSession("ghost".to_owned())).code,
            CODE_INVALID_PARAMS
        );
    }

    /// §3.1: the wire event is the core serde shape plus `seq`.
    #[test]
    fn sequenced_events_carry_seq_on_the_wire() {
        let event = agentos_core::Event::new(agentos_core::EventType::TaskDone)
            .with_run_id(Uuid::now_v7())
            .with_task_id(Uuid::now_v7());
        let wire = sequenced_to_wire(&SequencedEvent { seq: 41, event });
        assert_eq!(wire["seq"], json!(41));
        assert_eq!(wire["eventType"], json!("task.done"));
        for key in [
            "id",
            "occurredAt",
            "runId",
            "taskId",
            "payload",
            "schemaVersion",
        ] {
            assert!(wire.get(key).is_some(), "missing {key} in {wire}");
        }
    }

    #[test]
    fn push_target_profile_selects_only_existing_remotes() {
        let repo = tempfile::tempdir().expect("repo dir");
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .arg("-C")
                .arg(repo.path())
                .args(args)
                .output()
                .expect("git runs");
            assert!(output.status.success(), "git {args:?} failed");
        };
        git(&["init", "--initial-branch=main"]);
        git(&[
            "remote",
            "add",
            "personal",
            "git@github-personal:me/demo.git",
        ]);
        git(&["remote", "add", "work", "git@github-work:org/demo.git"]);

        let before = git_push_targets(&json!({ "repo": repo.path() })).expect("profile");
        assert_eq!(before["pushDefault"], Value::Null);
        assert_eq!(before["remotes"].as_array().unwrap().len(), 2);

        let after = git_push_target_set(&json!({
            "repo": repo.path(),
            "remote": "work",
        }))
        .expect("set selected remote");
        assert_eq!(after["pushDefault"], json!("work"));
        assert!(after["remotes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|remote| remote["name"] == "work" && remote["selected"] == true));
        assert!(matches!(
            git_push_target_set(&json!({ "repo": repo.path(), "remote": "unknown" })),
            Err(ApiError {
                code: CODE_INVALID_PARAMS,
                ..
            })
        ));
    }
}
