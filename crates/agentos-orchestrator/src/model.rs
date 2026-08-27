//! The planning-model boundary and its Claude Code implementation.
//!
//! [`PlanningModel`] is one method wide on purpose: *prompt in, text out*.
//! Everything that turns text into engine state lives in [`crate::parse`]
//! and [`crate::plan`], so the model is replaceable (a stub in tests, a
//! different provider later) and can never reach the durable store.
//!
//! ## The substrate (F-00 §4, handoff §"SESSION 4 DECISIONS")
//!
//! The orchestrator model is **`claude-opus-5` through the F-03
//! [`ClaudeAdapter`](agentos_adapters::claude::ClaudeAdapter)**. Observed
//! slug rules: `claude-opus-5` ✅, alias `opus` ✅, `opus-5` → 404. The
//! model row reports a 1M context window and ~$0.02 for a trivial call.
//!
//! ## "Commands, never writes code"
//!
//! The mastermind shape (HANDOFF-BUILD-2 §2) says the orchestrator never
//! writes code. That is enforced outside the model text, per PRD SEC-01, by
//! the only lever the provider-independent [`SpawnSpec`] exposes: the tool
//! denylist, rendered by F-03 as the equals-form
//! `--disallowedTools=a,b` (F-00 §4 canon).
//!
//! Every token in [`ORCHESTRATOR_TOOL_DENYLIST`] is **observed**, not
//! assumed: `Bash`, `WebFetch` and `WebSearch` from probe T2 (zero tool
//! uses, graceful exit 0), and the rest read straight out of the installed
//! CLI's own tool registry (`claude` 2.1.239: the literal array beginning
//! `["Bash","BashOutput","KillShell","PowerShell","Tmux","Monitor","REPL",
//! "Read","Edit","MultiEdit","Write","NotebookEdit",...]`, plus its
//! write-tool set `["Write","Edit","MultiEdit","NotebookEdit"]`). That
//! matters because an unknown token is a silent no-op, not an error — a
//! typo would leave the guard wide open while looking correct.
//!
//! The registry also showed the guard was too *narrow*: denying `Bash`
//! alone leaves `PowerShell`, `Tmux` and `REPL`, and denying the write
//! tools leaves delegation (`Agent`, `Task`, `TaskCreate`, `Skill`,
//! `Workflow`) through which a subagent writes whatever it likes. All are
//! denied now. Re-run [`verify_denylist_tokens`] on adapter/CLI upgrades.
//!
//! One caveat remains: `SpawnSpec` carries no permission-mode field, so
//! F-03 pins `acceptEdits`. The orchestrator cannot request claude's plan
//! mode through the trait; the denylist is the whole guard. Recorded as a
//! seam in `docs/F-12-orchestrator.md`.
//!
//! No test in this crate makes a billable call. The only live invocation is
//! an `#[ignore]`d probe gated behind `AGENTOS_ORCHESTRATOR_E2E=1`, mirroring
//! F-03's `AGENTOS_CLAUDE_E2E=1`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use agentos_adapters::{
    AdapterError, AdapterEvent, AdapterFailure, RuntimeAdapter, SpawnSpec, UsageSnapshot,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;

use crate::error::OrchestratorError;

/// Preserve the adapter's structured quota signal instead of reducing it to
/// display text. Mastermind can then switch providers without treating an
/// ordinary task/model failure as a quota event.
fn adapter_model_error(error: AdapterError) -> OrchestratorError {
    match error {
        AdapterError::SessionFailed(AdapterFailure::Transient { detail }) => {
            OrchestratorError::ProviderLimit { detail }
        }
        other => OrchestratorError::Model {
            detail: other.to_string(),
        },
    }
}

fn provider_model_error(failure: AdapterFailure) -> OrchestratorError {
    match failure {
        AdapterFailure::Transient { detail } => OrchestratorError::ProviderLimit { detail },
        other => OrchestratorError::Model {
            detail: other.to_string(),
        },
    }
}

/// The pinned orchestrator model (F-00 §4; `opus-5` 404s, `opus` is an
/// accepted alias).
pub const ORCHESTRATOR_MODEL: &str = "claude-opus-5";

/// Env var that opts the ignored e2e probe into a live (free-only) run.
pub const ORCHESTRATOR_E2E_ENV: &str = "AGENTOS_ORCHESTRATOR_E2E";

/// Default wall-clock ceiling for one planning turn.
pub const DEFAULT_PLANNING_TIMEOUT_SECS: u64 = 300;

/// Tools denied to the orchestrator session so it commands rather than
/// codes. Every token is verified against the installed CLI's tool registry
/// (see the module docs); an unrecognized token would silently do nothing.
pub const ORCHESTRATOR_TOOL_DENYLIST: [&str; 17] = [
    // Shells — every one of them executes arbitrary code.
    "Bash",
    "BashOutput",
    "KillShell",
    "PowerShell",
    "Tmux",
    "REPL",
    // The CLI's own write-tool set.
    "Write",
    "Edit",
    "MultiEdit",
    "NotebookEdit",
    // Delegation: a subagent inherits none of this denylist, so an
    // orchestrator that can spawn one can write code through it.
    "Agent",
    "Task",
    "TaskCreate",
    "Skill",
    "Workflow",
    // Network reach (T2-verified).
    "WebFetch",
    "WebSearch",
];

/// Tool tokens the orchestrator must never hold, as observed in the CLI
/// registry. Kept separate from the denylist so a token that disappears
/// from a future CLI can be spotted rather than silently no-op'd.
///
/// This is the upgrade smoke test: point it at the installed binary and it
/// reports any denied token the CLI no longer knows about. It reads the
/// file only — no session, no billable call.
pub fn verify_denylist_tokens(cli_path: &std::path::Path) -> std::io::Result<Vec<&'static str>> {
    let bytes = std::fs::read(cli_path)?;
    let haystack = String::from_utf8_lossy(&bytes);
    Ok(ORCHESTRATOR_TOOL_DENYLIST
        .into_iter()
        .filter(|token| !haystack.contains(&format!("\"{token}\"")))
        .collect())
}

/// What one planning turn produced.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ModelResponse {
    /// The model's final text. Parsed by [`crate::parse::parse_operations`],
    /// which treats it as data.
    pub text: String,
    /// Provider session id, when the runtime reported one (enables resume).
    pub session_id: Option<String>,
    /// Usage/cost for the turn, when reported.
    pub usage: Option<UsageSnapshot>,
    /// The provider signalled rate-limit pressure during the turn. Observed
    /// on this account at seven-day utilization 0.86 (handoff §"SESSION 4"):
    /// not a failure, but the caller should back off (OR-08).
    pub rate_limited: bool,
    /// Structured plan-mode questions emitted during this turn. The model
    /// may still finish the turn after asking; callers keep these separate
    /// from the operation text so a desktop can render real choices.
    pub decisions: Vec<PlanningDecision>,
}

/// One provider-neutral question raised while the planner is in plan mode.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanningDecision {
    pub tool: String,
    pub prompt: String,
    pub options: Vec<String>,
    pub multi_select: bool,
}

/// Provider conversation retained for the duration of one Mastermind phase.
///
/// A CLI process may still be one-shot, but Claude, Codex, and agy all expose
/// a provider conversation id that lets the next process resume the same
/// context. Mastermind shares this handle between its read-only preparation
/// model and the scoped author model, then clears it at the approved phase
/// boundary.
#[derive(Debug, Clone, Default)]
pub struct PlanningConversation {
    provider_session_id: Arc<StdMutex<Option<String>>>,
}

impl PlanningConversation {
    pub fn new(provider_session_id: Option<String>) -> Self {
        Self {
            provider_session_id: Arc::new(StdMutex::new(provider_session_id)),
        }
    }

    pub fn provider_session_id(&self) -> Option<String> {
        self.provider_session_id
            .lock()
            .ok()
            .and_then(|id| id.clone())
    }

    pub fn reset(&self) {
        if let Ok(mut id) = self.provider_session_id.lock() {
            *id = None;
        }
    }

    fn remember(&self, provider_session_id: String) {
        if let Ok(mut id) = self.provider_session_id.lock() {
            *id = Some(provider_session_id);
        }
    }
}

/// A model that proposes plan operations.
#[async_trait]
pub trait PlanningModel: Send + Sync {
    /// Run one planning turn. `Err` means the proposer is unavailable —
    /// which the workflow engine must survive, so callers degrade to "no
    /// new proposals" instead of failing the run.
    async fn propose(&self, prompt: &str) -> Result<ModelResponse, OrchestratorError>;
}

/// The orchestrator model over the F-03 Claude Code adapter.
pub struct ClaudePlanningModel {
    adapter: Arc<dyn RuntimeAdapter>,
    workspace: PathBuf,
    model: String,
    timeout: Duration,
    /// F-13: skill text prepended to every planning prompt (the
    /// `mastermind-commands` skill body, loaded from the registry by the
    /// caller). `None` keeps the bare prompt.
    skill_preamble: Option<String>,
    /// Write roots granted to this provider turn. Empty keeps the
    /// orchestrator read-only; phase document authors opt into the one
    /// project subtree they own.
    allowed_paths: Vec<String>,
    tool_allowlist: Vec<String>,
    /// Optional denylist override for delegated document/design sessions.
    /// The top-tier orchestrator retains [`ORCHESTRATOR_TOOL_DENYLIST`].
    tool_denylist: Option<Vec<String>>,
    /// Shared provider conversation. The full skill envelope is sent only
    /// when this is empty; resumed turns send their incremental prompt.
    conversation: PlanningConversation,
}

impl ClaudePlanningModel {
    /// Build the planning model over any [`RuntimeAdapter`] (the F-03
    /// `ClaudeAdapter` in production, the F-02 `MockAdapter` in
    /// credential-free tests).
    ///
    /// `workspace` is the session's cwd; the orchestrator only reads there
    /// — its write tools are denied.
    pub fn new(adapter: Arc<dyn RuntimeAdapter>, workspace: impl Into<PathBuf>) -> Self {
        Self {
            adapter,
            workspace: workspace.into(),
            model: ORCHESTRATOR_MODEL.to_owned(),
            timeout: Duration::from_secs(DEFAULT_PLANNING_TIMEOUT_SECS),
            skill_preamble: None,
            allowed_paths: Vec::new(),
            tool_allowlist: Vec::new(),
            tool_denylist: None,
            conversation: PlanningConversation::default(),
        }
    }

    /// Override the model slug (`opus` is the verified alias).
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Override the per-turn wall-clock ceiling.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attach the skill preamble (F-13): rendered ahead of every planning
    /// prompt, the same composition rule every registry agent's session
    /// uses. Empty text is treated as absent.
    pub fn with_skill_preamble(mut self, preamble: Option<String>) -> Self {
        self.skill_preamble = preamble.filter(|text| !text.trim().is_empty());
        self
    }

    /// Grant this turn edit access to explicit paths. This is used only by
    /// the phase document/design author spawned by Mastermind; the command
    /// orchestrator never calls it.
    /// Tools this turn may use without a permission prompt.
    ///
    /// A headless `-p` run cannot answer a prompt, so anything not
    /// pre-approved is denied in silence. `acceptEdits` covers file edits
    /// only: a phase whose procedure is a shell command gets zero Bash calls
    /// and returns an empty result, which is exactly how Phase 7.5 failed.
    pub fn with_tool_allowlist(mut self, tools: Vec<String>) -> Self {
        self.tool_allowlist = tools;
        self
    }

    pub fn with_allowed_paths(mut self, paths: Vec<String>) -> Self {
        self.allowed_paths = paths;
        self
    }

    /// Override the top-tier denylist for a delegated author/reviewer.
    pub fn with_tool_denylist(mut self, tools: Vec<String>) -> Self {
        self.tool_denylist = Some(tools);
        self
    }

    /// Reuse a provider conversation across read-only and scoped-write turns.
    pub fn with_conversation(mut self, conversation: PlanningConversation) -> Self {
        self.conversation = conversation;
        self
    }

    /// The spawn spec for one planning turn.
    ///
    /// Pure and unit-testable: the denylist, model pin and empty
    /// allowed-paths list are asserted in tests without spawning anything.
    pub fn spawn_spec(&self, prompt: &str) -> SpawnSpec {
        self.spawn_spec_for_turn(prompt, true)
    }

    fn spawn_spec_for_turn(&self, prompt: &str, include_preamble: bool) -> SpawnSpec {
        let objective = match (&self.skill_preamble, include_preamble) {
            (Some(preamble), true) => format!("{preamble}{prompt}"),
            _ => prompt.to_owned(),
        };
        SpawnSpec {
            task_id: Uuid::now_v7(),
            objective,
            workspace: self.workspace.clone(),
            allowed_paths: self.allowed_paths.clone(),
            forbidden_paths: Vec::new(),
            tool_allowlist: self.tool_allowlist.clone(),
            tool_denylist: self.tool_denylist.clone().unwrap_or_else(|| {
                ORCHESTRATOR_TOOL_DENYLIST
                    .iter()
                    .map(|tool| (*tool).to_owned())
                    .collect()
            }),
            model: Some(self.model.clone()),
            timeout_secs: self.timeout.as_secs(),
            isolated_home: None,
        }
    }
}

#[async_trait]
impl PlanningModel for ClaudePlanningModel {
    async fn propose(&self, prompt: &str) -> Result<ModelResponse, OrchestratorError> {
        let resume_id = self.conversation.provider_session_id();
        // Establishing the session spawns a provider process and waits on its
        // handshake, so it shares the turn timeout rather than running before
        // it. The event-loop timeout below cannot cover this await, and a
        // provider that never completes its handshake used to park the caller
        // forever: the phase stayed `running` with no timer, no error and no
        // terminal state, which left every recovery action gated off because
        // each one requires `blocked`. Only a daemon restart cleared it.
        let handle = match tokio::time::timeout(self.timeout, async {
            let handle = if let Some(provider_session_id) = resume_id {
                let resume_spec = self.spawn_spec_for_turn(prompt, false);
                match self
                    .adapter
                    .resume_session(resume_spec, provider_session_id.clone())
                    .await
                {
                    Ok(handle) => handle,
                    Err(error) => {
                        tracing::warn!(
                            provider_session_id,
                            %error,
                            "planning conversation resume failed; rebuilding from the durable envelope"
                        );
                        self.conversation.reset();
                        self.adapter
                            .start_session(self.spawn_spec_for_turn(prompt, true))
                            .await
                            .map_err(adapter_model_error)?
                    }
                }
            } else {
                self.adapter
                    .start_session(self.spawn_spec_for_turn(prompt, true))
                    .await
                    .map_err(adapter_model_error)?
            };
            Ok::<_, OrchestratorError>(handle)
        })
        .await
        {
            Ok(result) => result?,
            Err(_elapsed) => {
                return Err(OrchestratorError::Model {
                    detail: format!(
                        "provider session did not start within {}s",
                        self.timeout.as_secs()
                    ),
                });
            }
        };
        // NOTE (discovered constraint): `SessionHandle::events()` is a
        // broadcast subscription with no replay, and F-02's adapters spawn
        // their driving task inside `start_session`. Early events can
        // therefore be missed by a late subscriber. The terminal
        // `Finished` event carries the final result text and arrives after
        // process exit, so the planning turn does not depend on the
        // fan-out being complete; accumulated `TextDelta`s are only a
        // fallback.
        let mut events = handle.events();
        let mut response = ModelResponse::default();
        let mut deltas = String::new();

        let collected = tokio::time::timeout(self.timeout, async {
            loop {
                match events.recv().await {
                    Ok(AdapterEvent::Started { session_id, .. }) => {
                        self.conversation.remember(session_id.clone());
                        response.session_id = Some(session_id);
                    }
                    Ok(AdapterEvent::TextDelta(text)) => deltas.push_str(&text),
                    Ok(AdapterEvent::UsageUpdate(usage)) => response.usage = Some(usage),
                    Ok(AdapterEvent::RateLimit { provider_notice }) => {
                        // Not a failure (observed in successful runs); the
                        // caller backs off.
                        tracing::warn!(notice = %provider_notice,
                            "orchestrator model reported rate-limit pressure");
                        response.rate_limited = true;
                    }
                    Ok(AdapterEvent::ToolUse { tool, .. }) => {
                        tracing::debug!(tool = %tool, "orchestrator model used a tool");
                    }
                    Ok(AdapterEvent::Decision {
                        tool,
                        prompt,
                        options,
                        multi_select,
                    }) => {
                        tracing::info!(tool = %tool,
                            "orchestrator model asked for a human decision");
                        response.decisions.push(PlanningDecision {
                            tool,
                            prompt,
                            options,
                            multi_select,
                        });
                    }
                    Ok(AdapterEvent::Finished { final_result, .. }) => {
                        if let Some(text) = final_result {
                            response.text = text;
                        }
                        return Ok(());
                    }
                    Ok(AdapterEvent::Failed(failure)) => return Err(provider_model_error(failure)),
                    // A slow consumer dropped events; the terminal event is
                    // still ahead of us, so keep reading.
                    Err(RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "orchestrator model event stream lagged");
                    }
                    // Cancelled sessions go quiet with no terminal event.
                    Err(RecvError::Closed) => {
                        return Err(OrchestratorError::Model {
                            detail: "session stream closed without a terminal event".to_owned(),
                        });
                    }
                }
            }
        })
        .await;

        match collected {
            Ok(Ok(())) => {
                if response.text.trim().is_empty() {
                    response.text = deltas;
                }
                Ok(response)
            }
            Ok(Err(err)) => Err(err),
            Err(_elapsed) => {
                let _ = handle.cancel().await;
                Err(OrchestratorError::Model {
                    detail: format!("planning turn exceeded {}s", self.timeout.as_secs()),
                })
            }
        }
    }
}

/// A scripted [`PlanningModel`] for tests and offline demos.
///
/// It returns canned responses in order and then repeats the last one, so a
/// whole planning session runs with zero provider calls. Exported (not
/// `#[cfg(test)]`) because the crate's integration tests and downstream
/// crates both need a credential-free planner.
pub struct ScriptedPlanningModel {
    responses: Vec<Result<String, String>>,
    cursor: std::sync::atomic::AtomicUsize,
}

impl ScriptedPlanningModel {
    /// Script a sequence of successful responses.
    pub fn new<I, S>(responses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            responses: responses.into_iter().map(|text| Ok(text.into())).collect(),
            cursor: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Script a sequence that may include model outages (`Err(detail)`).
    pub fn with_failures(responses: Vec<Result<String, String>>) -> Self {
        Self {
            responses,
            cursor: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// How many turns have been consumed.
    pub fn turns(&self) -> usize {
        self.cursor.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait]
impl PlanningModel for ScriptedPlanningModel {
    async fn propose(&self, _prompt: &str) -> Result<ModelResponse, OrchestratorError> {
        let index = self
            .cursor
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match self.responses.get(index).or_else(|| self.responses.last()) {
            Some(Ok(text)) => Ok(ModelResponse {
                text: text.clone(),
                session_id: Some("scripted".to_owned()),
                usage: None,
                rate_limited: false,
                decisions: Vec::new(),
            }),
            Some(Err(detail)) => Err(OrchestratorError::Model {
                detail: detail.clone(),
            }),
            None => Ok(ModelResponse::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_adapters::claude::{ClaudeAdapter, ClaudeInvocation};
    use agentos_adapters::mock::{MockAdapter, MockBehavior};
    use agentos_adapters::{
        AdapterError, AuthStatus, Capabilities, RuntimeAdapter, RuntimeInfo, SessionHandle,
        SpawnSpec,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A provider whose session establishment never completes. Before the
    /// turn timeout covered `start_session`, this hung the caller forever and
    /// stranded the phase in `running`.
    struct HangingStartAdapter {
        inner: MockAdapter,
    }

    impl HangingStartAdapter {
        fn new() -> Self {
            Self {
                inner: MockAdapter::new(MockBehavior::Success {
                    turns: 1,
                    files_changed: vec![],
                }),
            }
        }
    }

    #[async_trait::async_trait]
    impl RuntimeAdapter for HangingStartAdapter {
        fn id(&self) -> &str {
            "hanging-start"
        }

        async fn detect(&self) -> RuntimeInfo {
            self.inner.detect().await
        }

        async fn auth_status(&self) -> AuthStatus {
            self.inner.auth_status().await
        }

        async fn capabilities(&self) -> Capabilities {
            self.inner.capabilities().await
        }

        async fn start_session(&self, _spec: SpawnSpec) -> Result<SessionHandle, AdapterError> {
            std::future::pending::<()>().await;
            unreachable!("pending future never resolves")
        }

        async fn resume_session(
            &self,
            _spec: SpawnSpec,
            _provider_session_id: String,
        ) -> Result<SessionHandle, AdapterError> {
            std::future::pending::<()>().await;
            unreachable!("pending future never resolves")
        }

        async fn shutdown(&self) -> Result<(), AdapterError> {
            self.inner.shutdown().await
        }
    }

    #[tokio::test]
    async fn hanging_session_start_fails_within_the_turn_timeout() {
        let model = ClaudePlanningModel::new(
            std::sync::Arc::new(HangingStartAdapter::new()),
            std::path::Path::new("."),
        )
        .with_timeout(Duration::from_secs(1));

        let error = model
            .propose("anything")
            .await
            .expect_err("a provider that never starts must not hang the turn");

        assert!(
            error.to_string().contains("did not start within 1s"),
            "expected a session-start timeout, got: {error}"
        );
    }

    struct ResumeFailsAdapter {
        inner: MockAdapter,
        resume_calls: AtomicUsize,
        started_objectives: StdMutex<Vec<String>>,
    }

    impl ResumeFailsAdapter {
        fn new() -> Self {
            Self {
                inner: MockAdapter::new(MockBehavior::Success {
                    turns: 1,
                    files_changed: vec![],
                }),
                resume_calls: AtomicUsize::new(0),
                started_objectives: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl RuntimeAdapter for ResumeFailsAdapter {
        fn id(&self) -> &str {
            "resume-fails"
        }

        async fn detect(&self) -> RuntimeInfo {
            self.inner.detect().await
        }

        async fn auth_status(&self) -> AuthStatus {
            self.inner.auth_status().await
        }

        async fn capabilities(&self) -> Capabilities {
            self.inner.capabilities().await
        }

        async fn start_session(&self, spec: SpawnSpec) -> Result<SessionHandle, AdapterError> {
            self.started_objectives
                .lock()
                .expect("objectives")
                .push(spec.objective.clone());
            self.inner.start_session(spec).await
        }

        async fn resume_session(
            &self,
            _spec: SpawnSpec,
            _provider_session_id: String,
        ) -> Result<SessionHandle, AdapterError> {
            self.resume_calls.fetch_add(1, Ordering::Relaxed);
            Err(AdapterError::Unsupported(
                "expired mock conversation".to_owned(),
            ))
        }

        async fn shutdown(&self) -> Result<(), AdapterError> {
            self.inner.shutdown().await
        }
    }

    fn planner() -> ClaudePlanningModel {
        ClaudePlanningModel::new(Arc::new(ClaudeAdapter::new()), PathBuf::from("."))
    }

    #[test]
    fn spawn_spec_pins_opus_5_and_denies_the_write_tools() {
        let spec = planner().spawn_spec("plan this");
        assert_eq!(spec.model.as_deref(), Some("claude-opus-5"));
        assert_eq!(spec.objective, "plan this");
        assert!(spec.tool_allowlist.is_empty());
        for tool in [
            "Bash",
            "PowerShell",
            "Tmux",
            "REPL",
            "WebFetch",
            "WebSearch",
            "Write",
            "Edit",
            "MultiEdit",
            "NotebookEdit",
            "Agent",
            "Task",
            "Skill",
        ] {
            assert!(
                spec.tool_denylist.iter().any(|denied| denied == tool),
                "{tool} must be denied to the orchestrator"
            );
        }
        assert_eq!(spec.timeout_secs, DEFAULT_PLANNING_TIMEOUT_SECS);
    }

    #[test]
    fn the_spec_renders_the_observed_equals_form_claude_argv() {
        // The F-00 §4 canon: variadic tool flags travel as ONE argv element
        // in equals form, and the prompt goes over stdin.
        let spec = planner().spawn_spec("plan this");
        let args = ClaudeInvocation::from_spec(&spec).args();
        let denylist = args
            .iter()
            .find(|arg| arg.starts_with("--disallowedTools="))
            .expect("denylist flag rendered in equals form");
        assert!(denylist.contains("Bash,BashOutput,KillShell"), "{denylist}");
        assert!(
            denylist.contains("Write,Edit,MultiEdit,NotebookEdit"),
            "{denylist}"
        );
        assert!(args.contains(&"--model".to_owned()));
        assert!(args.contains(&"claude-opus-5".to_owned()));
        // Prompt is NOT an argv element (stdin delivery, T3 canon).
        assert!(!args.iter().any(|arg| arg == "plan this"));
    }

    /// The upgrade smoke test, run against a real CLI binary when
    /// `AGENTOS_CLAUDE_CLI` points at one. Reads the file; never spawns a
    /// session, never bills. A token the CLI no longer knows about would
    /// silently stop denying anything, so this fails loudly instead.
    #[test]
    fn every_denied_token_exists_in_the_installed_cli_registry() {
        let Ok(path) = std::env::var("AGENTOS_CLAUDE_CLI") else {
            eprintln!("AGENTOS_CLAUDE_CLI unset; skipping CLI registry check");
            return;
        };
        let missing =
            verify_denylist_tokens(std::path::Path::new(&path)).expect("read the cli binary");
        assert!(
            missing.is_empty(),
            "tokens the installed CLI does not know (they would silently no-op): {missing:?}"
        );
    }

    #[test]
    fn the_denylist_covers_every_write_path_the_cli_exposes() {
        // Shells, file writes and delegation: each is a way to author code.
        for token in ["Bash", "PowerShell", "Tmux", "REPL"] {
            assert!(ORCHESTRATOR_TOOL_DENYLIST.contains(&token), "{token}");
        }
        for token in ["Write", "Edit", "MultiEdit", "NotebookEdit"] {
            assert!(ORCHESTRATOR_TOOL_DENYLIST.contains(&token), "{token}");
        }
        for token in ["Agent", "Task", "TaskCreate", "Skill", "Workflow"] {
            assert!(ORCHESTRATOR_TOOL_DENYLIST.contains(&token), "{token}");
        }
    }

    #[test]
    fn model_alias_and_timeout_are_overridable() {
        let planner = planner()
            .with_model("opus")
            .with_timeout(Duration::from_secs(30));
        let spec = planner.spawn_spec("x");
        assert_eq!(spec.model.as_deref(), Some("opus"));
        assert_eq!(spec.timeout_secs, 30);
    }

    #[test]
    fn skill_preamble_rides_ahead_of_the_prompt() {
        let skilled = planner().with_skill_preamble(Some("# Mastermind\n\nOrders.\n\n".to_owned()));
        let spec = skilled.spawn_spec("plan this");
        assert!(
            spec.objective.starts_with("# Mastermind"),
            "preamble first: {}",
            &spec.objective[..30.min(spec.objective.len())]
        );
        assert!(spec.objective.ends_with("plan this"));

        let discovery = skilled.spawn_spec("# DISCOVERY INTERVIEW\nAsk questions.");
        assert!(discovery.objective.starts_with("# Mastermind"));
        assert!(discovery
            .objective
            .ends_with("# DISCOVERY INTERVIEW\nAsk questions."));

        // Absent and empty preambles keep the bare prompt.
        assert_eq!(planner().spawn_spec("plan this").objective, "plan this");
        assert_eq!(
            planner()
                .with_skill_preamble(Some("   ".to_owned()))
                .spawn_spec("plan this")
                .objective,
            "plan this"
        );
    }

    #[tokio::test]
    async fn a_mock_backed_planning_turn_collects_the_final_text_and_never_bills() {
        // MockAdapter is credential-free (F-02); this exercises the whole
        // event-collection path with zero provider calls.
        let adapter = Arc::new(MockAdapter::new(MockBehavior::Success {
            turns: 1,
            files_changed: vec![],
        }));
        let planner = ClaudePlanningModel::new(adapter, PathBuf::from("."))
            .with_timeout(Duration::from_secs(5));
        let response = planner.propose("plan this").await.unwrap();
        assert!(response.session_id.is_some() || !response.text.is_empty());
    }

    #[tokio::test]
    async fn planning_turns_resume_one_provider_conversation_until_reset() {
        let adapter = Arc::new(MockAdapter::new(MockBehavior::Success {
            turns: 1,
            files_changed: vec![],
        }));
        let conversation = PlanningConversation::default();
        let planner = ClaudePlanningModel::new(adapter, PathBuf::from("."))
            .with_skill_preamble(Some("# full envelope\n".to_owned()))
            .with_conversation(conversation.clone())
            .with_timeout(Duration::from_secs(5));

        let first = planner.propose("first turn").await.expect("first turn");
        let second = planner.propose("incremental turn").await.expect("resume");
        assert_eq!(first.session_id, second.session_id);
        assert_eq!(conversation.provider_session_id(), second.session_id);

        conversation.reset();
        let third = planner.propose("new phase").await.expect("new session");
        assert_ne!(second.session_id, third.session_id);
    }

    #[tokio::test]
    async fn failed_resume_retries_once_with_the_full_durable_envelope() {
        let adapter = Arc::new(ResumeFailsAdapter::new());
        let conversation = PlanningConversation::default();
        let planner = ClaudePlanningModel::new(adapter.clone(), PathBuf::from("."))
            .with_skill_preamble(Some("# checkpoint envelope\n".to_owned()))
            .with_conversation(conversation.clone())
            .with_timeout(Duration::from_secs(5));

        let first = planner.propose("prepare").await.expect("initial session");
        let fallback = planner
            .propose("author from checkpoint")
            .await
            .expect("fresh fallback");

        assert_eq!(adapter.resume_calls.load(Ordering::Relaxed), 1);
        assert_ne!(first.session_id, fallback.session_id);
        assert_eq!(conversation.provider_session_id(), fallback.session_id);
        assert_eq!(
            adapter
                .started_objectives
                .lock()
                .expect("objectives")
                .as_slice(),
            [
                "# checkpoint envelope\nprepare",
                "# checkpoint envelope\nauthor from checkpoint",
            ],
            "the one fresh retry reconstructs the full envelope under the same model scope"
        );
    }

    #[tokio::test]
    async fn adapter_failures_surface_as_a_retryable_model_error() {
        let adapter = Arc::new(MockAdapter::new(MockBehavior::FailWith(
            agentos_adapters::AdapterFailure::AuthFailure {
                detail: "logged out".to_owned(),
            },
        )));
        let planner = ClaudePlanningModel::new(adapter, PathBuf::from("."))
            .with_timeout(Duration::from_secs(5));
        let err = planner.propose("plan this").await.unwrap_err();
        assert!(matches!(err, OrchestratorError::Model { .. }), "{err:?}");
        // A down proposer is retryable and must never fail a run.
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn scripted_model_replays_then_repeats_its_last_response() {
        let model = ScriptedPlanningModel::new(["[]", "[{\"op\":\"close_goal\"}]"]);
        assert_eq!(model.propose("p").await.unwrap().text, "[]");
        assert_eq!(
            model.propose("p").await.unwrap().text,
            "[{\"op\":\"close_goal\"}]"
        );
        assert_eq!(
            model.propose("p").await.unwrap().text,
            "[{\"op\":\"close_goal\"}]"
        );
        assert_eq!(model.turns(), 3);
    }

    #[tokio::test]
    async fn scripted_model_can_simulate_an_outage() {
        let model =
            ScriptedPlanningModel::with_failures(vec![Err("claude binary not found".to_owned())]);
        let err = model.propose("p").await.unwrap_err();
        assert!(matches!(err, OrchestratorError::Model { .. }));
    }

    #[test]
    fn structured_transient_provider_failure_is_preserved_for_failover() {
        let error = provider_model_error(AdapterFailure::Transient {
            detail: "provider rate limit (status 429)".to_owned(),
        });
        assert!(matches!(error, OrchestratorError::ProviderLimit { .. }));
    }

    // -----------------------------------------------------------------
    // The ONLY live invocation in this crate: free discovery probes on the
    // real claude binary (detect + auth-file existence). No prompt is sent,
    // so nothing is billable. Skipped by default and doubly gated behind
    // AGENTOS_ORCHESTRATOR_E2E=1, mirroring F-03's AGENTOS_CLAUDE_E2E.
    #[tokio::test]
    #[ignore = "live claude probe (free, non-billable): set AGENTOS_ORCHESTRATOR_E2E=1 to run"]
    async fn e2e_free_probe_orchestrator_substrate_is_present() {
        if std::env::var(ORCHESTRATOR_E2E_ENV).ok().as_deref() != Some("1") {
            return;
        }
        let adapter = ClaudeAdapter::new();
        let health = adapter.health().await;
        assert!(
            health.runtime.path.is_some(),
            "claude binary not found: {health:?}"
        );
        assert!(
            health.runtime.version.is_some(),
            "claude --version produced nothing: {health:?}"
        );
        // Capability check only — the planning turn itself is billable and
        // is never run from the test suite.
        let caps = adapter.capabilities().await;
        assert!(caps.resume, "orchestrator relies on resumable sessions");
    }
}
