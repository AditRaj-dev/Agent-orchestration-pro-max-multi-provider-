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
use agentos_adapters::codex::CodexAdapter;
use agentos_adapters::mock::{MockAdapter, MockBehavior};
use agentos_adapters::{AdapterError, AdapterEvent, RuntimeAdapter, SpawnSpec};
use agentos_agents::{AgentMode, AgentRecord, AgentRegistry, AgentsError};
use agentos_core::Event;
use chrono::Utc;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

use crate::events;

/// How long `send` waits for a mid-turn adapter to become instructable
/// again. Answering a plan-mode question is the common case: the asking
/// turn ends within a second or two of emitting its decision event.
const INSTRUCTION_WAIT: Duration = Duration::from_secs(120);
/// Poll interval while waiting for the current turn to reach its terminal.
const INSTRUCTION_POLL: Duration = Duration::from_millis(200);

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
/// A plan-mode session asked the human to choose; the desktop renders the
/// options as buttons and answers with `agent.session.send`.
const EVT_AGENT_DECISION: &str = "agent.decision";
const EVT_USAGE_UPDATED: &str = "usage.updated";
const EVT_AGENT_SPAWN_FAILED: &str = "agent.spawn_failed";
const EVT_AGENT_PROPOSAL: &str = "agent.proposal";
const EVT_AGENT_PROPOSAL_INVALID: &str = "agent.proposal_invalid";
/// A creator drafted a new skill to install (`"kind":"skill"`).
const EVT_SKILL_PROPOSAL: &str = "skill.proposal";

/// Current Codex model surface exposed by the desktop. Codex CLI has no
/// credential-free model-list command, so this catalog is maintained here
/// and shared by every `registry.catalog` caller.
const CODEX_MODELS: [(&str, &str); 5] = [
    ("gpt-5.6-sol", "GPT-5.6 Sol"),
    ("gpt-5.6-terra", "GPT-5.6 Terra"),
    ("gpt-5.6-luna", "GPT-5.6 Luna"),
    ("gpt-5.5", "GPT-5.5"),
    ("gpt-5.4", "GPT-5.4"),
];

/// A live or finished chat session's bookkeeping.
struct ChatSession {
    agent_id: String,
    handle: agentos_adapters::SessionHandle,
}

/// What a reopened chat carries over from the conversation it continues.
struct ResumeTarget {
    /// The daemon chat session being continued (journaled as `resumedFrom`).
    from_session: String,
    /// The provider-side handle the adapter resumes.
    provider_session_id: String,
}

/// The daemon's adapter set: the four wired runtimes. Concrete (not
/// `dyn`) so the free `agy models` catalog probe stays reachable, and so
/// `registry.catalog` reports exactly what a chat session can spawn.
/// Construction is side-effect free — no probe runs until asked.
pub struct AdapterSet {
    claude: Arc<ClaudeAdapter>,
    agy: Arc<AgyAdapter>,
    codex: Arc<CodexAdapter>,
    mock: Arc<MockAdapter>,
}

impl AdapterSet {
    /// The wired quartet. The mock's canned behavior is the success
    /// script (one turn, no file changes) — enough surface for UI and
    /// tests.
    pub fn wired() -> Self {
        Self {
            claude: Arc::new(ClaudeAdapter::new()),
            agy: Arc::new(AgyAdapter::new()),
            codex: Arc::new(CodexAdapter::new()),
            mock: Arc::new(MockAdapter::new(MockBehavior::Success {
                turns: 1,
                files_changed: vec![],
            })),
        }
    }

    /// Swap the mock adapter for one with a different script. Test-only:
    /// it lets the daemon's own send path be driven against an agent that
    /// asks questions mid-turn, without touching a billable provider.
    #[cfg(test)]
    pub(crate) fn with_mock(mut self, behavior: MockBehavior) -> Self {
        self.mock = Arc::new(MockAdapter::new(behavior));
        self
    }

    /// Resolve by adapter id (registry `adapterId` values). Public since
    /// F-12: the mastermind service hands the same table to its
    /// supervisors so a task routed at `nextjs-dev` spawns on the adapter
    /// that agent's record names.
    pub fn by_id(&self, id: &str) -> Option<Arc<dyn RuntimeAdapter>> {
        match id {
            "claude-code" => Some(Arc::clone(&self.claude) as Arc<dyn RuntimeAdapter>),
            "antigravity-agy" => Some(Arc::clone(&self.agy) as Arc<dyn RuntimeAdapter>),
            "codex" => Some(Arc::clone(&self.codex) as Arc<dyn RuntimeAdapter>),
            "mock" => Some(Arc::clone(&self.mock) as Arc<dyn RuntimeAdapter>),
            _ => None,
        }
    }

    /// Adapter ids in roster order.
    fn ids(&self) -> [&'static str; 4] {
        ["claude-code", "antigravity-agy", "codex", "mock"]
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

    /// Start a fresh chat session with an agent: resolve, compose, spawn,
    /// and hand the event pump to a driver task. Returns the chat session
    /// id.
    pub async fn start(&self, agent_id: &str, message: &str) -> Result<String, SessionError> {
        self.start_inner(agent_id, message, None).await
    }

    /// Continue an earlier conversation (F-13b): look the old session up in
    /// the journal, take its `providerSessionId`, and open a NEW chat
    /// session bound to it, with `message` as the next turn.
    ///
    /// A new daemon session id is deliberate — the old one is closed
    /// history, and the journal records `resumedFrom` so the two read as
    /// one thread. What carries over is the *provider's* memory, which is
    /// the part the human cares about.
    pub async fn reopen(&self, session_id: &str, message: &str) -> Result<String, SessionError> {
        let summary = self
            .chat_session(session_id)?
            .ok_or_else(|| SessionError::NotFound(format!("chat session {session_id}")))?;
        let provider_session = summary.provider_session_id.clone().ok_or_else(|| {
            SessionError::InvalidParams(format!(
                "chat session {session_id} never reported a provider session id, \
                 so there is nothing to resume (start a new chat instead)"
            ))
        })?;
        let agent_id = summary.agent_id.clone();
        self.start_inner(
            &agent_id,
            message,
            Some(ResumeTarget {
                from_session: session_id.to_owned(),
                provider_session_id: provider_session,
            }),
        )
        .await
    }

    /// One chat session's summary, folded from the journal.
    pub fn chat_session(
        &self,
        session_id: &str,
    ) -> Result<Option<crate::chat_history::ChatSessionSummary>, SessionError> {
        Ok(self
            .chat_sessions(None)?
            .into_iter()
            .find(|summary| summary.session_id == session_id))
    }

    /// Chat sessions folded from the journal, newest activity first.
    pub fn chat_sessions(
        &self,
        agent_id: Option<&str>,
    ) -> Result<Vec<crate::chat_history::ChatSessionSummary>, SessionError> {
        let conn = crate::db::open_db(&self.journal)?;
        let journal = events::tail_with_seq(&conn, 0, u32::MAX)?;
        Ok(crate::chat_history::fold_sessions(&journal, agent_id))
    }

    /// One session's transcript, folded from the journal in order.
    pub fn chat_transcript(
        &self,
        session_id: &str,
    ) -> Result<Vec<crate::chat_history::ChatMessage>, SessionError> {
        let conn = crate::db::open_db(&self.journal)?;
        let journal = events::tail_with_seq(&conn, 0, u32::MAX)?;
        Ok(crate::chat_history::fold_transcript(&journal, session_id))
    }

    async fn start_inner(
        &self,
        agent_id: &str,
        message: &str,
        resume: Option<ResumeTarget>,
    ) -> Result<String, SessionError> {
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

        let mut preamble = self.registry.preamble_for(&record)?;
        // A proposing agent must know which skill ids actually exist. Its
        // method tells it to "use only skill ids the user listed as
        // available", but nothing ever listed them — so it invented plausible
        // names, `validate_draft` rejected every draft on the first unknown
        // skill, and the human never saw a single proposal card.
        if record.skills.iter().any(|s| s == AGENT_CREATION_SKILL) {
            preamble.push_str(&self.registry_catalog_preamble()?);
        }
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
                    "resumedFrom": resume.as_ref().map(|target| target.from_session.clone()),
                    "resumedProviderSession":
                        resume.as_ref().map(|target| target.provider_session_id.clone()),
                })),
        )?;

        // The opening message rides inside the spawn spec, so nothing else
        // would ever journal it — and a transcript rebuilt from the journal
        // would start with the agent answering a question nobody asked.
        self.journal(
            Event::new(agentos_core::EventType::Other(
                EVT_SESSION_INSTRUCTION.to_owned(),
            ))
            .with_trace_id(trace_id)
            .with_agent_id(record.id.clone())
            .with_payload(json!({
                "sessionId": session_id,
                "message": message,
                "chat": true,
            })),
        )?;

        let spawn = match &resume {
            Some(target) => {
                adapter
                    .resume_session(spec, target.provider_session_id.clone())
                    .await
            }
            None => adapter.start_session(spec).await,
        };
        let handle = spawn.map_err(|error| {
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

    /// The authoritative id lists a proposing agent must draft against:
    /// every registered skill, and the adapters this daemon actually wires.
    fn registry_catalog_preamble(&self) -> Result<String, SessionError> {
        let skills = self.registry.list_skills()?;
        let mut out = String::from(
            "# Registry catalog (authoritative)\n\n             These are the ONLY valid ids. A draft naming anything else is              rejected by the registry and never reaches the user.\n\n             ## Skill ids for the `skills` field\n\n",
        );
        if skills.is_empty() {
            out.push_str("(none registered — leave `skills` empty)\n");
        } else {
            for skill in &skills {
                out.push_str(&format!("- `{}` — {}\n", skill.id, skill.name));
            }
        }
        out.push_str("\n## Adapter ids for the `adapterId` field\n\n");
        for id in self.adapters.ids() {
            out.push_str(&format!("- `{id}`\n"));
        }
        out.push_str("\n## Beyond this list\n\n");
        out.push_str(
            "A local skill library also sits on this machine. If you know a skill id \
             from it that fits the need better than anything above, name it in the \
             draft anyway — it installs on use. Only an id that exists in neither \
             place is rejected, so prefer the list above when it covers the need, \
             and never invent an id you have not actually seen.\n",
        );
        out.push_str("\n---\n\n");
        Ok(out)
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
        // A decision arrives while the turn that asked it is still
        // streaming, so the adapter is briefly busy. Wait for the turn to
        // land instead of failing: a failed send used to make the desktop
        // open a NEW session, where the agent had no memory of the
        // conversation and asked its opening questions all over again.
        let deadline = tokio::time::Instant::now() + INSTRUCTION_WAIT;
        loop {
            match handle.send_instruction(message.to_owned()).await {
                Err(AdapterError::Busy(_)) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(INSTRUCTION_POLL).await;
                }
                other => {
                    other?;
                    // Journal only what the adapter actually accepted.
                    // Recording the instruction up front put messages in
                    // the transcript that no agent ever received, and left
                    // the UI showing a turn in flight that never was.
                    self.journal(
                        Event::new(agentos_core::EventType::Other(
                            EVT_SESSION_INSTRUCTION.to_owned(),
                        ))
                        .with_agent_id(agent_id)
                        .with_payload(json!({
                            "sessionId": session_id, "message": message, "chat": true,
                        })),
                    )?;
                    return Ok(());
                }
            }
        }
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
        // Eviction is the daemon's own decision and always succeeds. Whether
        // there was still a live provider run to stop is a different fact,
        // and the journal must not claim an operator interrupted something
        // that had already ended.
        let reason = match handle.cancel().await {
            Ok(()) => "stopped by the operator",
            Err(AdapterError::SessionNotActive(_)) => "session had already finished",
            Err(err) => return Err(err.into()),
        };
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
                .with_payload(json!({
                    "sessionId": session_id, "reason": reason, "chat": true,
                })),
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
    /// `agy models` call; claude's and codex's are the observed slug sets,
    /// neither CLI having a free catalog call; mock is static). A failing
    /// provider degrades to `models: []`, never an error for the whole
    /// catalog.
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
        let mut providers = Vec::with_capacity(self.adapters.ids().len());
        for id in self.adapters.ids() {
            let adapter = self.adapter(id).expect("ids() covers the wired set");
            let info = adapter.detect().await;
            let auth = adapter.auth_status().await;
            let models: Vec<Value> = if id == "antigravity-agy" {
                agy_models.clone()
            } else if id == "codex" {
                // Codex exposes no free catalog call. Keep the desktop's
                // complete supported model surface explicit and let the
                // provider return an account-specific error at spawn time.
                CODEX_MODELS
                    .iter()
                    .map(|(slug, label)| json!({ "id": slug, "label": label }))
                    .collect()
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
    // The budget is per TURN, not per session: every adapter defines
    // `Finished` as "turn complete, session still instructable", so the
    // clock restarts when a turn lands and the next one is awaited.
    let turn_budget = Duration::from_secs(record.timeout_secs.max(1));
    let mut deadline = tokio::time::Instant::now() + turn_budget;
    let mut turns_completed: u64 = 0;
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
                if turns_completed == 0 {
                    emit(
                        EVT_SESSION_FAILED,
                        json!({
                        "sessionId": session_id, "error": "chat session timed out", "chat": true }),
                    );
                } else {
                    // Turns landed and then nobody spoke again: the session
                    // aged out, which is not a failure of the work already
                    // delivered.
                    emit(
                        EVT_SESSION_CANCELLED,
                        json!({
                            "sessionId": session_id,
                            "reason": format!(
                                "idle for {}s after {turns_completed} completed turn(s)",
                                turn_budget.as_secs()
                            ),
                            "chat": true,
                        }),
                    );
                }
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
                            "sessionId": session_id,
                            "tool": tool,
                            "argsSummary": args_summary,
                            "chat": true,
                        }),
                    );
                }
                AdapterEvent::Decision {
                    tool,
                    prompt,
                    options,
                    multi_select,
                } => {
                    emit(
                        EVT_AGENT_DECISION,
                        json!({
                            "sessionId": session_id,
                            "tool": tool,
                            "prompt": prompt,
                            "options": options,
                            "multiSelect": multi_select,
                            "chat": true,
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
                    // The only place a provider's live utilization is ever
                    // observable: it rides the wire and is never persisted
                    // by the CLI itself.
                    crate::usage_meters::record_rate_limit(&record.adapter_id, &provider_notice);
                    emit(
                        EVT_AGENT_RATE_LIMIT,
                        json!({
                            "sessionId": session_id,
                            "providerNotice": provider_notice,
                            "chat": true,
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
                    // `Finished` is the end of a TURN, not of the session:
                    // every adapter (F-03/F-04/F-05) delivers follow-ups as
                    // a resumed run on the same provider session, so the
                    // pump keeps listening and the session stays in the
                    // active map. Ending here made every second message in
                    // a chat fail with "finished sessions cannot be
                    // resumed" — and the desktop silently started a fresh
                    // session, losing the conversation.
                    turns_completed += 1;
                    deadline = tokio::time::Instant::now() + turn_budget;
                    continue;
                }
                AdapterEvent::Failed(failure) => {
                    // agy reports quota exhaustion as a Transient failure
                    // rather than a RateLimit event, so the meter has to
                    // read it off the failure detail.
                    crate::usage_meters::record_failure_detail(
                        &record.adapter_id,
                        &failure.to_string(),
                    );
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
    // A skill draft is checked first: when the creator needs a capability
    // that does not exist, it must install the skill before proposing the
    // agent that holds it — otherwise `validate_draft` rejects the agent on
    // the very skill the creator just wrote.
    match parse_skill_proposal(final_text) {
        Ok(Some(skill)) => {
            appender.append(
                Event::new(agentos_core::EventType::Other(
                    EVT_SKILL_PROPOSAL.to_owned(),
                ))
                .with_agent_id(record.id.clone())
                .with_payload(json!({
                    "sessionId": session_id,
                    "skill": serde_json::to_value(&skill).unwrap_or(Value::Null),
                    "chat": true,
                })),
            );
            return;
        }
        Ok(None) => {}
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
    }

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
        if registry
            .get_skill(skill)
            .map_err(|e| e.to_string())?
            .is_some()
        {
            continue;
        }
        // Not registered — but the local skill library may already hold a
        // method document under that id, in which case naming it is enough
        // to install it. Only a name that exists nowhere rejects the draft.
        let dir = crate::skill_import::source_dir();
        let imported = crate::skill_import::load(&dir, skill)
            .map_err(|reason| format!("skill {skill} does not exist: {reason}"))?;
        registry
            .create_skill(imported)
            .map_err(|err| format!("skill {skill} could not be installed: {err}"))?;
        tracing::info!(skill = %skill, "imported skill from the local library for a proposal");
    }
    if let Ok(Some(existing)) = registry.get_agent(&record.id) {
        return Err(format!(
            "agent id {:?} already exists (existing: {})",
            existing.id, existing.name
        ));
    }
    Ok(record)
}

/// A skill the creator wants installed. Mirrors `SkillRecord`'s writable
/// fields; `builtin` is never wire-settable.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SkillDraft {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub body: String,
}

/// Read one skill draft out of a reply: a fenced JSON object tagged
/// `"kind":"skill"`. The tag is what separates it from an agent draft —
/// both are fenced JSON objects with an `id`.
pub(crate) fn parse_skill_proposal(text: &str) -> Result<Option<SkillDraft>, String> {
    for block in fenced_blocks(text) {
        let Ok(value) = serde_json::from_str::<Value>(&block) else {
            continue;
        };
        if value.get("kind").and_then(Value::as_str) != Some("skill") {
            continue;
        }
        return match serde_json::from_str::<SkillDraft>(&block) {
            Ok(draft) if draft.id.trim().is_empty() => {
                Err("skill draft has an empty id".to_owned())
            }
            Ok(draft) if draft.body.trim().is_empty() => Err(format!(
                "skill {} has an empty body; the body is the directive injected into sessions",
                draft.id
            )),
            Ok(draft) => Ok(Some(draft)),
            Err(err) => Err(format!("skill JSON did not match the contract: {err}")),
        };
    }
    Ok(None)
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
        // Only objects with an `id` are proposal candidates, and a block
        // tagged as a skill belongs to `parse_skill_proposal`.
        let parsed = serde_json::from_str::<Value>(&block).ok();
        let looks_like_proposal = parsed
            .as_ref()
            .and_then(|value| value.get("id").cloned())
            .is_some();
        let is_skill = parsed
            .as_ref()
            .and_then(|value| value.get("kind").and_then(Value::as_str))
            == Some("skill");
        if !looks_like_proposal || is_skill {
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
        registry.seed_builtin_skills().expect("seed skills");
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

        // A finished TURN does not end the session: every adapter delivers
        // follow-ups as a resumed run, so the session stays active and the
        // instruction reaches the adapter. (The mock then refuses it — its
        // run is a fixed script — but that refusal comes from the adapter,
        // not from the daemon evicting the session.)
        assert!(
            sessions.is_active(&session_id),
            "a completed turn must leave the session instructable"
        );
        let err = sessions.send(&session_id, "again").await.unwrap_err();
        assert!(
            !matches!(err, SessionError::NotFound(_)),
            "the daemon must not refuse the follow-up itself: {err:?}"
        );

        // Cancel is what ends a chat session.
        sessions.cancel(&session_id).await.expect("cancel");
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

    /// A creator that needs a capability the catalog lacks installs the
    /// skill instead of inventing an id for it.
    #[test]
    fn a_skill_draft_is_recognised_by_its_kind_tag() {
        let text = "Here is the skill.\n\n```json\n{\"kind\":\"skill\",\
            \"id\":\"react-native-dev\",\"name\":\"React Native Dev\",\
            \"description\":\"Expo and RN method\",\
            \"body\":\"# React Native — Method\\n\\nUse Expo.\"}\n```";
        let draft = parse_skill_proposal(text)
            .expect("parses")
            .expect("a skill draft");
        assert_eq!(draft.id, "react-native-dev");
        assert_eq!(draft.name, "React Native Dev");
        assert!(draft.body.contains("Method"));

        // The same block must NOT be read as an agent draft, or a skill
        // would register an agent.
        assert!(
            parse_agent_proposal(text).expect("total").is_none(),
            "a skill block is not an agent proposal"
        );
    }

    /// An agent draft still parses as one: the tag separates them.
    #[test]
    fn an_agent_draft_is_not_mistaken_for_a_skill() {
        let text = "```json\n{\"id\":\"sql-reviewer\",\"name\":\"SQL Reviewer\",\
            \"description\":\"reviews\",\"adapterId\":\"claude-code\"}\n```";
        assert!(parse_skill_proposal(text).expect("total").is_none());
        assert!(parse_agent_proposal(text).expect("parses").is_some());
    }

    /// A skill whose body is empty is not a skill — the body IS the
    /// directive injected into every session that holds it.
    #[test]
    fn a_bodyless_skill_draft_is_refused_with_a_reason() {
        let text = "```json\n{\"kind\":\"skill\",\"id\":\"empty\",\
            \"name\":\"Empty\",\"body\":\"   \"}\n```";
        let err = parse_skill_proposal(text).expect_err("refused");
        assert!(err.contains("empty body"), "{err}");
    }

    /// A proposing agent must be handed the registry's real skill ids. Its
    /// method says "use only skill ids the user listed as available", and
    /// nothing listed them — so it invented plausible names, every draft
    /// died in `validate_draft` on the first unknown skill, and the human
    /// never saw a proposal card at all.
    #[tokio::test]
    async fn a_proposing_agent_is_told_which_ids_exist() {
        let (_registry, sessions, dir) = mock_sessions();

        let catalog = sessions.registry_catalog_preamble().expect("catalog");
        assert!(catalog.contains("`agent-creation`"), "catalog: {catalog}");
        assert!(catalog.contains("`react-dev`"), "catalog: {catalog}");
        assert!(catalog.contains("`antigravity-agy`"), "catalog: {catalog}");

        // And it must actually reach the session. The mock echoes its
        // objective into the final result, so the journal proves delivery.
        sessions
            .start("mock-creator", "design me an agent")
            .await
            .expect("start");
        let mut saw_catalog = false;
        for _ in 0..50 {
            let conn = crate::db::open_db(&dir.path().join("journal.db")).expect("journal");
            let batch = events::tail_with_seq(&conn, 0, u32::MAX).expect("tail");
            saw_catalog = batch.iter().any(|s| {
                s.event.event_type.to_string() == EVT_SESSION_FINISHED
                    && s.event
                        .payload
                        .get("finalResult")
                        .and_then(|v| v.as_str())
                        .is_some_and(|text| text.contains("Registry catalog"))
            });
            if saw_catalog {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            saw_catalog,
            "the catalog must ride in the creator's preamble"
        );
    }

    /// A non-proposing agent gets no catalog: it is context it cannot use.
    #[test]
    fn a_plain_agent_gets_no_registry_catalog() {
        let (registry, sessions, _dir) = mock_sessions();
        let researcher = registry
            .get_agent("researcher")
            .expect("lookup")
            .expect("seeded researcher");
        assert!(
            !researcher.skills.iter().any(|s| s == AGENT_CREATION_SKILL),
            "the researcher does not propose agents"
        );
        let preamble = sessions
            .registry
            .preamble_for(&researcher)
            .expect("preamble");
        assert!(!preamble.contains("Registry catalog"));
    }

    /// The question loop: a plan-mode agent asks mid-turn, so the adapter
    /// is briefly busy when the answer arrives. `send` must WAIT for the
    /// turn to land and deliver the answer to the SAME session. Opening a
    /// new session instead gives an agent with no memory of the interview,
    /// which asks question 1 again — and every answer spawns another
    /// session, forever.
    #[tokio::test]
    async fn answering_mid_turn_resumes_the_same_session() {
        let dir = tempfile::tempdir().expect("tempdir");
        let registry =
            Arc::new(AgentRegistry::open(&dir.path().join("agents.db")).expect("registry"));
        registry.seed_builtin_skills().expect("seed skills");
        registry.seed_builtins().expect("seeds");
        registry
            .create_agent(AgentRecord {
                id: "mock-interviewer".to_owned(),
                name: "Mock Interviewer".to_owned(),
                description: "asks questions mid-turn".to_owned(),
                adapter_id: "mock".to_owned(),
                model: Some("mock-model-1".to_owned()),
                effort: None,
                mode: AgentMode::Plan,
                skills: vec![],
                tool_allowlist: vec![],
                tool_denylist: vec![],
                timeout_secs: 60,
                builtin: false,
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .expect("create interviewer");

        let sessions = AgentSessions::new(
            Arc::clone(&registry),
            AdapterSet::wired().with_mock(MockBehavior::Interview { questions: 3 }),
            dir.path().join("journal.db"),
            dir.path().to_path_buf(),
        );

        let session_id = sessions
            .start("mock-interviewer", "build me an agent")
            .await
            .expect("start");

        let journal = dir.path().join("journal.db");
        let count_of = |name: &str| {
            let conn = crate::db::open_db(&journal).expect("journal");
            events::tail_with_seq(&conn, 0, u32::MAX)
                .expect("tail")
                .iter()
                .filter(|s| s.event.event_type.to_string() == name)
                .count()
        };

        // Answer each question once it has actually been asked — a human
        // cannot answer before seeing it. The answer still lands while the
        // asking turn is streaming, which is the case that used to fail.
        for question in 1..=3 {
            for _ in 0..200 {
                if count_of("agent.decision") >= question {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let answer = format!("answer {question}a");
            sessions
                .send(&session_id, &answer)
                .await
                .unwrap_or_else(|err| panic!("answering {answer:?} must not fail: {err:?}"));
        }

        // Let the closing turn land.
        for _ in 0..100 {
            if count_of("session.finished") >= 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let of_type = count_of;

        // ONE session for the whole interview. More than one spawn is the
        // bug: it means an answer opened a fresh conversation.
        assert_eq!(
            of_type("session.spawn"),
            1,
            "the whole interview must run in one session; extra spawns are the question loop"
        );
        // The opening message plus one journaled instruction per answer.
        assert_eq!(
            of_type("session.instruction"),
            4,
            "every answer must be journaled against the same session"
        );
        assert_eq!(of_type("agent.decision"), 3, "three questions were asked");
    }

    /// A mid-turn rejection is not a dead session: the two must stay
    /// distinguishable, because only one of them justifies starting over.
    #[tokio::test]
    async fn a_mid_turn_send_reports_busy_not_a_dead_session() {
        use agentos_adapters::mock::{MockAdapter, MockBehavior};
        use agentos_adapters::RuntimeAdapter;

        let adapter = MockAdapter::new(MockBehavior::Interview { questions: 1 });
        let spec = SpawnSpec {
            task_id: Uuid::now_v7(),
            objective: "interview me".to_owned(),
            workspace: std::env::temp_dir(),
            allowed_paths: Vec::new(),
            forbidden_paths: Vec::new(),
            tool_allowlist: Vec::new(),
            tool_denylist: Vec::new(),
            model: None,
            timeout_secs: 60,
            isolated_home: None,
        };
        let handle = adapter.start_session(spec).await.expect("session");
        let mut rx = handle.events();

        // Wait for the question: the turn is streaming from here.
        loop {
            match rx.recv().await.expect("event") {
                AdapterEvent::Decision { .. } => break,
                _ => continue,
            }
        }

        let err = handle
            .send_instruction("answer now".to_owned())
            .await
            .expect_err("a mid-turn instruction is refused");
        assert!(
            matches!(err, AdapterError::Busy(_)),
            "mid-turn must be Busy (wait), not SessionNotActive (start over): {err:?}"
        );
        assert!(err.is_retryable(), "Busy clears when the turn lands");
    }

    /// Every id the set advertises must resolve to a live adapter — a
    /// registry agent pointing at an advertised-but-unwired provider would
    /// fail only at spawn time, in front of the user.
    #[test]
    fn every_advertised_adapter_id_resolves() {
        let adapters = AdapterSet::wired();
        assert!(adapters.ids().contains(&"codex"), "F-04 is wired");
        for id in adapters.ids() {
            assert!(adapters.by_id(id).is_some(), "{id} must resolve");
            assert!(
                agentos_agents::KNOWN_ADAPTERS.contains(&id),
                "{id} must be a registry-valid adapterId"
            );
        }
        assert!(adapters.by_id("nope").is_none());
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
