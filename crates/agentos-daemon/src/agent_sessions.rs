//! F-13: live agent chat sessions over the registry.
//!
//! The "talk to the agent-creator / researcher" path: resolve an agent
//! record, compose its skill preamble ahead of the user message, spawn the
//! record's adapter session, and translate the adapter event stream into
//! journal events (`session.spawn` / `session.started` / `agent.tool_use`
//! / `usage.updated` / `session.finished` / `agent.spawn_failed`) carrying
//! the registry agent id — the same vocabulary the supervisor's executor
//! uses, so the existing projections and `events.subscribe` UI stream pick
//! chat sessions up unchanged.
//!
//! ## Session shape
//!
//! - Chat sessions are **not** run- or task-scoped: `run_id`/`task_id`
//!   stay `None` (stamping them would fabricate run/task projections) and
//!   one fresh `trace_id` correlates every event of one conversation.
//! - Follow-ups reuse the adapter's own resume path
//!   ([`SessionHandle::send_instruction`] — claude `--resume`, agy
//!   `--conversation`), verified shapes both.
//! - The wall-clock ceiling is the record's `timeoutSecs`, enforced by a
//!   driver-task deadline (and redundantly by the adapter watchdog).
//!
//! ## Proposal parsing
//!
//! When the session's agent holds the `agent-creation` skill, the driver
//! treats the final text as a potential agent proposal: fenced JSON blocks
//! are extracted and validated into an [`AgentRecord`] draft (see
//! [`parse_agent_proposal`]). A valid draft journals `agent.proposal`;
//! an invalid one journals `agent.proposal_invalid` with the reason.
//! Either way nothing is written to the registry — registration is an
//! explicit `registry.agents.create` from the human (the mastermind gate).

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use agentos_adapters::agy::AgyAdapter;
use agentos_adapters::claude::ClaudeAdapter;
use agentos_adapters::mock::{MockAdapter, MockBehavior};
use agentos_adapters::{AdapterError, AdapterEvent, RuntimeAdapter, SpawnSpec};
use agentos_agents::{AgentMode, AgentRecord, AgentRegistry, AgentsError};
use agentos_core::Event;
use chrono::Utc;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

use crate::events;

/// Skill that marks an agent as a proposer of agent definitions.
pub const AGENT_CREATION_SKILL: &str = "agent-creation";

/// Write tools denied to plan-mode agents on adapters that pin an
/// edit-permission mode (claude `acceptEdits`): read-only agents get their
/// writes denied at the tool layer instead. Mirrors the F-12 orchestrator
/// guard minus the delegation/network tokens — chat agents keep web reach.
const PLAN_MODE_WRITE_DENYLIST: [&str; 4] = ["Write", "Edit", "MultiEdit", "NotebookEdit"];

// Chat-session event types ( EventType::Other keeps them round-tripping).
const EVT_SESSION_SPAWN: &str = "session.spawn";
const EVT_SESSION_STARTED: &str = "session.started";
const EVT_SESSION_INSTRUCTION: &str = "session.instruction";
const EVT_SESSION_FINISHED: &str = "session.finished";
const EVT_SESSION_FAILED: &str = "agent.session_failed";
const EVT_SESSION_CANCELLED: &str = "session.cancelled";
const EVT_AGENT_TOOL_USE: &str = "agent.tool_use";
const EVT_AGENT_RATE_LIMIT: &str = "agent.rate_limit";
const EVT_USAGE_UPDATED: &str = "usage.updated";
const EVT_AGENT_SPAWN_FAILED: &str = "agent.spawn_failed";
const EVT_AGENT_PROPOSAL: &str = "agent.proposal";
const EVT_AGENT_PROPOSAL_INVALID: &str = "agent.proposal_invalid";

/// A live or finished chat session's bookkeeping.
struct ChatSession {
    agent_id: String,
    handle: agentos_adapters::SessionHandle,
}

/// The daemon's adapter set: the three wired runtimes. Concrete (not
/// `dyn`) so the free `agy models` catalog probe stays reachable, and so
/// `registry.catalog` reports exactly what a chat session can spawn.
/// Construction is side-effect free — no probe runs until asked.
pub struct AdapterSet {
    claude: Arc<ClaudeAdapter>,
    agy: Arc<AgyAdapter>,
    mock: Arc<MockAdapter>,
}

impl AdapterSet {
    /// The wired trio. The mock's canned behavior is the success script
    /// (one turn, no file changes) — enough surface for UI and tests.
    pub fn wired() -> Self {
        Self {
            claude: Arc::new(ClaudeAdapter::new()),
            agy: Arc::new(AgyAdapter::new()),
            mock: Arc::new(MockAdapter::new(MockBehavior::Success {
                turns: 1,
                files_changed: vec![],
            })),
        }
    }

    /// Resolve by adapter id (registry `adapterId` values).
    fn by_id(&self, id: &str) -> Option<Arc<dyn RuntimeAdapter>> {
        match id {
            "claude-code" => Some(Arc::clone(&self.claude) as Arc<dyn RuntimeAdapter>),
            "antigravity-agy" => Some(Arc::clone(&self.agy) as Arc<dyn RuntimeAdapter>),
            "mock" => Some(Arc::clone(&self.mock) as Arc<dyn RuntimeAdapter>),
            _ => None,
        }
    }

    /// Adapter ids in roster order.
    fn ids(&self) -> [&'static str; 3] {
        ["claude-code", "antigravity-agy", "mock"]
    }
}

impl Default for AdapterSet {
    fn default() -> Self {
        Self::wired()
    }
}

impl fmt::Debug for AdapterSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdapterSet")
            .field("wired", &self.ids())
            .finish()
    }
}

/// Chat sessions over the registry + adapters.
pub struct AgentSessions {
    registry: Arc<AgentRegistry>,
    adapters: AdapterSet,
    /// Journal db path: each append opens its own connection (WAL), the
    /// supervisor's proven pattern — no connection crosses an await.
    journal: PathBuf,
    /// Working directory for chat sessions (the daemon's project root).
    workspace: PathBuf,
    next: AtomicU64,
    active: Arc<StdMutex<HashMap<String, ChatSession>>>,
}

impl fmt::Debug for AgentSessions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentSessions")
            .field("journal", &self.journal)
            .field("workspace", &self.workspace)
            .field("adapters", &self.adapters)
            .field("active", &self.active.lock().map(|m| m.len()))
            .finish()
    }
}

impl AgentSessions {
    /// Build the service over a registry, the adapter set, the journal
    /// path, and the chat workspace root (must exist).
    pub fn new(
        registry: Arc<AgentRegistry>,
        adapters: AdapterSet,
        journal: PathBuf,
        workspace: PathBuf,
    ) -> Self {
        Self {
            registry,
            adapters,
            journal,
            workspace,
            next: AtomicU64::new(0),
            active: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    fn adapter(&self, id: &str) -> Option<Arc<dyn RuntimeAdapter>> {
        self.adapters.by_id(id)
    }

    /// Start a chat session with an agent: resolve, compose, spawn, and
    /// hand the event pump to a driver task. Returns the chat session id.
    pub async fn start(&self, agent_id: &str, message: &str) -> Result<String, SessionError> {
        if message.trim().is_empty() {
            return Err(SessionError::InvalidParams(
                "message must be non-empty".to_owned(),
            ));
        }
        let record = self
            .registry
            .resolve(agent_id)?
            .ok_or_else(|| SessionError::NotFound(format!("agent {agent_id} (or disabled)")))?;
        let adapter = self.adapter(&record.adapter_id).ok_or_else(|| {
            SessionError::InvalidParams(format!(
                "no adapter {:?} is wired into this daemon",
                record.adapter_id
            ))
        })?;

        let preamble = self.registry.preamble_for(&record)?;
        let spec = chat_spawn_spec(&record, &preamble, message, self.workspace.clone());

        // Journal before spawn so the conversation's first frame is
        // ordered ahead of anything the session emits.
        let session_no = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let session_id = format!("chat-{session_no}-{}", Uuid::now_v7().simple());
        let trace_id = Uuid::now_v7();
        self.journal(
            Event::new(agentos_core::EventType::Other(EVT_SESSION_SPAWN.to_owned()))
                .with_trace_id(trace_id)
                .with_agent_id(record.id.clone())
                .with_payload(json!({
                    "sessionId": session_id,
                    "provider": adapter.id(),
                    "model": record.model,
                    "workspace": self.workspace.display().to_string(),
                    "chat": true,
                })),
        )?;

        let handle = adapter.start_session(spec).await.map_err(|error| {
            self.journal(
                Event::new(agentos_core::EventType::Other(
                    EVT_AGENT_SPAWN_FAILED.to_owned(),
                ))
                .with_trace_id(trace_id)
                .with_agent_id(record.id.clone())
                .with_payload(json!({
                    "sessionId": session_id,
                    "adapter": adapter.id(),
                    "error": error.to_string(),
                    "chat": true,
                })),
            )
            .ok();
            SessionError::from(error)
        })?;

        self.active
            .lock()
            .map_err(|_| SessionError::Internal("session registry poisoned".to_owned()))?
            .insert(
                session_id.clone(),
                ChatSession {
                    agent_id: record.id.clone(),
                    handle: handle.clone(),
                },
            );

        tokio::spawn(drive_session(
            Arc::clone(&self.registry),
            self.journal.clone(),
            self.active.clone(),
            session_id.clone(),
            trace_id,
            record,
            handle,
        ));
        Ok(session_id)
    }

    /// Send a follow-up message to a live session (adapter resume path).
    pub async fn send(&self, session_id: &str, message: &str) -> Result<(), SessionError> {
        if message.trim().is_empty() {
            return Err(SessionError::InvalidParams(
                "message must be non-empty".to_owned(),
            ));
        }
        let (agent_id, handle) = {
            let active = self
                .active
                .lock()
                .map_err(|_| SessionError::Internal("session registry poisoned".to_owned()))?;
            active
                .get(session_id)
                .map(|session| (session.agent_id.clone(), session.handle.clone()))
                .ok_or_else(|| {
                    SessionError::NotFound(format!(
                        "session {session_id} (finished sessions cannot be resumed; start a new one)"
                    ))
                })?
        };
        self.journal(
            Event::new(agentos_core::EventType::Other(
                EVT_SESSION_INSTRUCTION.to_owned(),
            ))
            .with_agent_id(agent_id)
            .with_payload(json!({ "sessionId": session_id, "message": message })),
        )?;
        handle.send_instruction(message.to_owned()).await?;
        Ok(())
    }

    /// Cancel a live session (process-kill semantics; no terminal event).
    pub async fn cancel(&self, session_id: &str) -> Result<(), SessionError> {
        let handle = {
            let active = self
                .active
                .lock()
                .map_err(|_| SessionError::Internal("session registry poisoned".to_owned()))?;
            active
                .get(session_id)
                .map(|session| session.handle.clone())
                .ok_or_else(|| SessionError::NotFound(format!("session {session_id}")))?
        };
        handle.cancel().await?;
        if let Some(agent_id) = self
            .active
            .lock()
            .ok()
            .and_then(|active| active.get(session_id).map(|s| s.agent_id.clone()))
        {
            self.journal(
                Event::new(agentos_core::EventType::Other(
                    EVT_SESSION_CANCELLED.to_owned(),
                ))
                .with_agent_id(agent_id)
                .with_payload(json!({ "sessionId": session_id })),
            )?;
        }
        self.active
            .lock()
            .ok()
            .and_then(|mut active| active.remove(session_id).map(|_| ()));
        Ok(())
    }

    /// Whether a session id is still live.
    pub fn is_active(&self, session_id: &str) -> bool {
        self.active
            .lock()
            .map(|active| active.contains_key(session_id))
            .unwrap_or(false)
    }

    /// Append one event to the journal (fresh connection per append, WAL).
    fn journal(&self, event: Event) -> Result<(), SessionError> {
        let conn = crate::db::open_db(&self.journal)?;
        events::append_event(&conn, &event)?;
        Ok(())
    }

    /// The provider catalog for `registry.catalog`: free probes only —
    /// adapter detection/auth plus model lists (the agy list is the free
    /// `agy models` call; claude's is the observed slug set; mock is
    /// static). A failing provider degrades to `models: []`, never an
    /// error for the whole catalog.
    pub async fn provider_catalog(&self) -> Vec<Value> {
        let agy_models: Vec<Value> = self
            .adapters
            .agy
            .list_models()
            .await
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| json!({ "id": entry.id, "label": entry.label }))
                    .collect()
            })
            .unwrap_or_default();
        let mut providers = Vec::with_capacity(3);
        for id in self.adapters.ids() {
            let adapter = self.adapter(id).expect("ids() covers the wired set");
            let info = adapter.detect().await;
            let auth = adapter.auth_status().await;
            let models: Vec<Value> = if id == "antigravity-agy" {
                agy_models.clone()
            } else if id == "claude-code" {
                // Observed slugs (handoff §3.1: sonnet-5 default, opus-5
                // pinned orchestrator, haiku-4-5 auxiliary) — the claude
                // CLI has no free catalog call.
                ["claude-opus-5", "claude-sonnet-5", "claude-haiku-4-5"]
                    .iter()
                    .map(|slug| json!({ "id": slug, "label": slug }))
                    .collect()
            } else {
                vec![json!({ "id": "mock-model-1", "label": "Mock Model 1" })]
            };
            providers.push(json!({
                "id": id,
                "version": info.version,
                "path": info.path.as_ref().map(|p| p.display().to_string()),
                "auth": serde_json::to_value(auth).unwrap_or(Value::Null),
                "models": models,
            }));
        }
        providers
    }
}

/// Build the chat SpawnSpec from a record: skill preamble ahead of the
/// message, record model/tools/timeout, mode mapped through the adapters'
/// contract (agy derives plan-mode from empty `allowed_paths`; claude pins
/// `acceptEdits`, so plan-mode gets write tools denied).
fn chat_spawn_spec(
    record: &AgentRecord,
    preamble: &str,
    message: &str,
    workspace: PathBuf,
) -> SpawnSpec {
    let mut tool_denylist = record.tool_denylist.clone();
    if record.mode == AgentMode::Plan && record.adapter_id == "claude-code" {
        for tool in PLAN_MODE_WRITE_DENYLIST {
            if !tool_denylist.iter().any(|denied| denied == tool) {
                tool_denylist.push(tool.to_owned());
            }
        }
    }
    SpawnSpec {
        task_id: Uuid::now_v7(),
        objective: format!("{preamble}{message}"),
        workspace: workspace.clone(),
        // Mode mapping rides the adapters' own contract (F-05): the agy
        // invocation derives `--mode plan` from an empty allowed_paths
        // list and `accept-edits` from a non-empty one (extra dirs are
        // deduped against the workspace `--add-dir`). Plan agents stay
        // read-only; accept-edits agents name their workspace.
        allowed_paths: match record.mode {
            AgentMode::Plan => Vec::new(),
            AgentMode::AcceptEdits => vec![workspace.display().to_string()],
        },
        forbidden_paths: Vec::new(),
        tool_allowlist: record.tool_allowlist.clone(),
        tool_denylist,
        model: record.model.clone(),
        timeout_secs: record.timeout_secs,
        isolated_home: None,
    }
}

/// The per-session event pump: adapter events → journal events, with the
/// record's timeout as the wall-clock ceiling. On any terminal outcome the
/// session leaves the active map (finished sessions are not resumable
/// through this API).
async fn drive_session(
    registry: Arc<AgentRegistry>,
    journal: PathBuf,
    active: Arc<StdMutex<HashMap<String, ChatSession>>>,
    session_id: String,
    trace_id: Uuid,
    record: AgentRecord,
    handle: agentos_adapters::SessionHandle,
) {
    let mut rx = handle.events();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(record.timeout_secs.max(1));
    let appender = JournalAppender { path: journal };
    let proposes = record.skills.iter().any(|s| s == AGENT_CREATION_SKILL);

    let emit = |event_type: &str, payload: Value| {
        appender.append(
            Event::new(agentos_core::EventType::Other(event_type.to_owned()))
                .with_trace_id(trace_id)
                .with_agent_id(record.id.clone())
                .with_payload(payload),
        )
    };

    loop {
        let received = tokio::time::timeout_at(deadline, rx.recv()).await;
        match received {
            Err(_elapsed) => {
                let _ = handle.cancel().await;
                emit(
                    EVT_SESSION_FAILED,
                    json!({
                    "sessionId": session_id, "error": "chat session timed out", "chat": true }),
                );
                break;
            }
            Ok(Err(RecvError::Lagged(dropped))) => {
                tracing::warn!(session = %session_id, dropped, "chat event stream lagged");
                continue;
            }
            // Quiet stream (cancelled sessions go silent by contract).
            Ok(Err(RecvError::Closed)) => {
                tracing::debug!(session = %session_id, "chat event stream closed");
                break;
            }
            Ok(Ok(event)) => match event {
                AdapterEvent::Started {
                    session_id: provider_session,
                    model,
                } => {
                    emit(
                        EVT_SESSION_STARTED,
                        json!({
                            "sessionId": session_id,
                            "providerSessionId": provider_session,
                            "model": model,
                            "chat": true,
                        }),
                    );
                }
                AdapterEvent::TextDelta(_) => {} // final text carries the reply
                AdapterEvent::ToolUse { tool, args_summary } => {
                    emit(
                        EVT_AGENT_TOOL_USE,
                        json!({
                            "sessionId": session_id, "tool": tool, "argsSummary": args_summary,
                        }),
                    );
                }
                AdapterEvent::UsageUpdate(usage) => {
                    emit(
                        EVT_USAGE_UPDATED,
                        json!({
                            "sessionId": session_id,
                            "costUsd": usage.cost_usd,
                            "totalTokens": usage.total_tokens,
                            "inputTokens": usage.input_tokens,
                            "outputTokens": usage.output_tokens,
                            "cacheReadTokens": usage.cache_read_tokens,
                            "sessionOverheadTokens": usage.session_overhead_tokens,
                        }),
                    );
                }
                AdapterEvent::RateLimit { provider_notice } => {
                    emit(
                        EVT_AGENT_RATE_LIMIT,
                        json!({
                            "sessionId": session_id, "providerNotice": provider_notice,
                        }),
                    );
                }
                AdapterEvent::Finished { final_result, .. } => {
                    emit(
                        EVT_SESSION_FINISHED,
                        json!({
                            "sessionId": session_id,
                            "finalResult": final_result.clone().unwrap_or_default(),
                            "chat": true,
                        }),
                    );
                    if proposes {
                        if let Some(final_text) = final_result.as_deref() {
                            journal_proposal(
                                &registry,
                                &appender,
                                &session_id,
                                &record,
                                final_text,
                            );
                        }
                    }
                    break;
                }
                AdapterEvent::Failed(failure) => {
                    emit(
                        EVT_SESSION_FAILED,
                        json!({
                            "sessionId": session_id, "error": failure.to_string(), "chat": true,
                        }),
                    );
                    break;
                }
            },
        }
    }

    if let Ok(mut active) = active.lock() {
        active.remove(&session_id);
    }
}

/// Try to read one agent proposal out of a creator session's final text
/// and journal the outcome (`agent.proposal` or `agent.proposal_invalid`).
/// Registry state is never touched here — registration is the human's call.
fn journal_proposal(
    registry: &AgentRegistry,
    appender: &JournalAppender,
    session_id: &str,
    record: &AgentRecord,
    final_text: &str,
) {
    let parsed = parse_agent_proposal(final_text);
    let draft = match parsed {
        Ok(Some(draft)) => match validate_draft(registry, draft) {
            Ok(valid) => valid,
            Err(reason) => {
                appender.append(
                    Event::new(agentos_core::EventType::Other(
                        EVT_AGENT_PROPOSAL_INVALID.to_owned(),
                    ))
                    .with_agent_id(record.id.clone())
                    .with_payload(json!({
                        "sessionId": session_id, "reason": reason, "chat": true,
                    })),
                );
                return;
            }
        },
        Ok(None) => return, // no fenced JSON object: plain conversation turn
        Err(reason) => {
            appender.append(
                Event::new(agentos_core::EventType::Other(
                    EVT_AGENT_PROPOSAL_INVALID.to_owned(),
                ))
                .with_agent_id(record.id.clone())
                .with_payload(json!({
                    "sessionId": session_id, "reason": reason, "chat": true,
                })),
            );
            return;
        }
    };
    appender.append(
        Event::new(agentos_core::EventType::Other(
            EVT_AGENT_PROPOSAL.to_owned(),
        ))
        .with_agent_id(record.id.clone())
        .with_payload(json!({
            "sessionId": session_id,
            "agent": serde_json::to_value(&draft).unwrap_or(Value::Null),
            "chat": true,
        })),
    );
}

/// A creator proposal's wire shape (the `agent-creation` skill contract).
/// Everything except `id`/`name`/`description`/`adapterId` is optional
/// with sensible defaults so an interview-style draft still validates.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AgentDraft {
    id: String,
    name: String,
    #[serde(default)]
    description: String,
    #[serde(rename = "adapterId", alias = "adapter_id")]
    adapter_id: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    effort: Option<agentos_agents::AgentEffort>,
    #[serde(default)]
    mode: Option<AgentMode>,
    #[serde(default)]
    skills: Vec<String>,
    #[serde(default)]
    tool_allowlist: Vec<String>,
    #[serde(default)]
    tool_denylist: Vec<String>,
    #[serde(default = "default_timeout")]
    timeout_secs: u64,
}

fn default_timeout() -> u64 {
    600
}

/// Turn a draft into a full record and validate it against the registry
/// (shape + skill existence + id collision).
fn validate_draft(registry: &AgentRegistry, draft: AgentDraft) -> Result<AgentRecord, String> {
    let record = AgentRecord {
        id: draft.id,
        name: draft.name,
        description: draft.description,
        adapter_id: draft.adapter_id,
        model: draft.model,
        effort: draft.effort,
        mode: draft.mode.unwrap_or(AgentMode::Plan),
        skills: draft.skills,
        tool_allowlist: draft.tool_allowlist,
        tool_denylist: draft.tool_denylist,
        timeout_secs: draft.timeout_secs,
        builtin: false,
        enabled: true,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    record.validate().map_err(|err| err.to_string())?;
    for skill in &record.skills {
        registry
            .get_skill(skill)
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("skill {skill} does not exist"))?;
    }
    if let Ok(Some(existing)) = registry.get_agent(&record.id) {
        return Err(format!(
            "agent id {:?} already exists (existing: {})",
            existing.id, existing.name
        ));
    }
    Ok(record)
}

/// Extract a fenced-JSON agent proposal from session text. Total:
///
/// - `Ok(None)` — no fenced JSON object found (a plain conversation turn
///   is not an error).
/// - `Err(reason)` — a fenced block *looked like* a proposal (an object
///   with an `id`) but failed to deserialize; the reason goes back to the
///   UI as `agent.proposal_invalid`.
/// - `Ok(Some(draft))` — a well-formed draft.
pub(crate) fn parse_agent_proposal(text: &str) -> Result<Option<AgentDraft>, String> {
    for block in fenced_blocks(text) {
        // Only objects with an `id` are proposal candidates.
        let looks_like_proposal = serde_json::from_str::<Value>(&block)
            .ok()
            .and_then(|value| value.get("id").cloned())
            .is_some();
        if !looks_like_proposal {
            continue;
        }
        return match serde_json::from_str::<AgentDraft>(&block) {
            Ok(draft) => Ok(Some(draft)),
            Err(err) => Err(format!("proposal JSON did not match the contract: {err}")),
        };
    }
    Ok(None)
}

/// Every fenced code block's content (``` or ```json fences).
fn fenced_blocks(text: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut lines = text.lines().peekable();
    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();
        if let Some(info) = trimmed.strip_prefix("```") {
            let _info_tag = info.trim(); // "json", "js", "" — content is what matters
            let mut block = String::new();
            for inner in lines.by_ref() {
                if inner.trim_start().starts_with("```") {
                    blocks.push(block);
                    break;
                }
                block.push_str(inner);
                block.push('\n');
            }
        }
    }
    blocks
}

/// Minimal journal appender (fresh connection per append; see
/// [`AgentSessions`]).
struct JournalAppender {
    path: PathBuf,
}

impl JournalAppender {
    fn append(&self, event: Event) {
        match crate::db::open_db(&self.path).and_then(|conn| events::append_event(&conn, &event)) {
            Ok(_) => {}
            Err(err) => tracing::error!(%err, "chat session journal append failed"),
        }
    }
}

/// Chat-session API failures, mapped to F-11 error codes by the server.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
    #[error("{0}")]
    NotFound(String),
    #[error(transparent)]
    Registry(#[from] AgentsError),
    #[error(transparent)]
    Adapter(#[from] AdapterError),
    #[error(transparent)]
    Core(#[from] agentos_core::CoreError),
    #[error("{0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_sessions() -> (Arc<AgentRegistry>, AgentSessions, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let registry =
            Arc::new(AgentRegistry::open(&dir.path().join("agents.db")).expect("registry"));
        registry.seed_builtins().expect("seeds");
        // A mock-backed stand-in for the creator so tests never bill.
        registry
            .create_agent(AgentRecord {
                id: "mock-creator".to_owned(),
                name: "Mock Creator".to_owned(),
                description: "mock".to_owned(),
                adapter_id: "mock".to_owned(),
                model: Some("mock-model-1".to_owned()),
                effort: None,
                mode: AgentMode::Plan,
                skills: vec![AGENT_CREATION_SKILL.to_owned()],
                tool_allowlist: vec![],
                tool_denylist: vec![],
                timeout_secs: 60,
                builtin: false,
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .expect("create mock creator");
        let sessions = AgentSessions::new(
            Arc::clone(&registry),
            AdapterSet::wired(),
            dir.path().join("journal.db"),
            dir.path().to_path_buf(),
        );
        (registry, sessions, dir)
    }

    #[tokio::test]
    async fn chat_session_lifecycle_journals_the_full_event_set() {
        let (_registry, sessions, dir) = mock_sessions();
        let session_id = sessions
            .start("mock-creator", "hello")
            .await
            .expect("start");

        // The driver finishes quickly (mock); poll the journal briefly.
        let mut saw_spawn = false;
        let mut saw_started = false;
        let mut saw_finished = false;
        for _ in 0..50 {
            let conn = crate::db::open_db(&dir.path().join("journal.db")).expect("journal");
            let batch = events::tail_with_seq(&conn, 0, u32::MAX).expect("tail");
            for sequenced in &batch {
                let name = sequenced.event.event_type.to_string();
                saw_spawn |= name == "session.spawn";
                saw_started |= name == EVT_SESSION_STARTED;
                saw_finished |= name == EVT_SESSION_FINISHED;
            }
            if saw_spawn && saw_started && saw_finished {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(saw_spawn, "session.spawn journaled");
        assert!(saw_started, "session.started journaled");
        assert!(saw_finished, "session.finished journaled");

        // Terminal: the session is no longer active, sends refuse.
        for _ in 0..50 {
            if !sessions.is_active(&session_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!sessions.is_active(&session_id));
        let err = sessions.send(&session_id, "again").await.unwrap_err();
        assert!(matches!(err, SessionError::NotFound(_)), "{err:?}");
    }

    #[tokio::test]
    async fn unknown_and_disabled_agents_refuse_to_start() {
        let (_registry, sessions, _dir) = mock_sessions();
        assert!(matches!(
            sessions.start("ghost", "hi").await,
            Err(SessionError::NotFound(_))
        ));
        sessions
            .registry
            .set_agent_enabled("mock-creator", false)
            .expect("disable");
        assert!(matches!(
            sessions.start("mock-creator", "hi").await,
            Err(SessionError::NotFound(_))
        ));
        assert!(matches!(
            sessions.start("mock-creator", "   ").await,
            Err(SessionError::InvalidParams(_))
        ));
    }

    #[test]
    fn chat_spec_composes_preamble_model_and_mode() {
        let record = AgentRecord {
            id: "x-agent".to_owned(),
            name: "X".to_owned(),
            description: "x".to_owned(),
            adapter_id: "mock".to_owned(),
            model: Some("mock-model-1".to_owned()),
            effort: None,
            mode: AgentMode::Plan,
            skills: vec![],
            tool_allowlist: vec![],
            tool_denylist: vec!["WebFetch".to_owned()],
            timeout_secs: 300,
            builtin: false,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let spec = chat_spawn_spec(&record, "# Preamble\n", "do the thing", PathBuf::from("ws"));
        assert!(spec.objective.starts_with("# Preamble\n"));
        assert!(spec.objective.ends_with("do the thing"));
        assert_eq!(spec.model.as_deref(), Some("mock-model-1"));
        assert!(spec.allowed_paths.is_empty(), "plan mode stays read-only");
        assert_eq!(spec.tool_denylist, vec!["WebFetch".to_owned()]);
        assert_eq!(spec.timeout_secs, 300);
    }

    #[test]
    fn plan_mode_claude_agents_get_write_tools_denied() {
        let mut record = AgentRecord {
            id: "x-agent".to_owned(),
            name: "X".to_owned(),
            description: "x".to_owned(),
            adapter_id: "claude-code".to_owned(),
            model: None,
            effort: None,
            mode: AgentMode::Plan,
            skills: vec![],
            tool_allowlist: vec![],
            tool_denylist: vec![],
            timeout_secs: 300,
            builtin: false,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let spec = chat_spawn_spec(&record, "", "hi", PathBuf::from("ws"));
        for tool in PLAN_MODE_WRITE_DENYLIST {
            assert!(
                spec.tool_denylist.contains(&tool.to_owned()),
                "{tool} denied"
            );
        }
        // Edit mode keeps the record's lists untouched.
        record.mode = AgentMode::AcceptEdits;
        let spec = chat_spawn_spec(&record, "", "hi", PathBuf::from("ws"));
        assert!(spec.tool_denylist.is_empty());
    }

    #[test]
    fn proposal_parser_takes_the_first_well_formed_candidate() {
        let proposal = r#"Sure — here is my draft:

```json
{"id":"sql-reviewer","name":"SQL Reviewer","description":"reviews migrations",
 "adapterId":"claude-code","model":"claude-sonnet-5","mode":"plan",
 "skills":[],"timeoutSecs":600}
```

Let me know if that fits."#;
        let draft = parse_agent_proposal(proposal)
            .expect("no parse error")
            .expect("a draft");
        assert_eq!(draft.id, "sql-reviewer");
        assert_eq!(draft.adapter_id, "claude-code");
        assert_eq!(draft.timeout_secs, 600);
    }

    #[test]
    fn proposal_parser_is_total_over_garbage() {
        // Plain conversation → Ok(None), never an error.
        assert!(parse_agent_proposal("no code blocks here")
            .expect("ok")
            .is_none());
        assert!(parse_agent_proposal("```js\nconsole.log(1)\n```")
            .expect("ok")
            .is_none());
        // An object with an id that breaks the contract → Err(reason).
        let bad = "```json\n{\"id\":42}\n```";
        assert!(parse_agent_proposal(bad).is_err());
    }

    #[test]
    fn fenced_blocks_handles_json_and_plain_fences() {
        let text = "before\n```json\n{\"a\":1}\n```\nmid\n```\nraw\n```\npost";
        let blocks = fenced_blocks(text);
        assert_eq!(blocks.len(), 2, "{blocks:?}");
        assert!(blocks[0].contains("\"a\":1"));
        assert!(blocks[1].contains("raw"));
        // An opener must start its line — inline backticks are not fences.
        assert!(fenced_blocks("pre ```json\n{\"a\":1}\n```").is_empty());
    }
}
