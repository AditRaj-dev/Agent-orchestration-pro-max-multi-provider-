//! Codex adapter — F-04.
//!
//! Drives the OpenAI Codex CLI (`codex exec --json`) through the F-02
//! [`RuntimeAdapter`] contract. Every claim below is observed on this
//! machine, 2026-08-22, codex-cli 0.149.0 on a ChatGPT account with model
//! `gpt-5.6-terra`; the transcripts are frozen in
//! `cli-codex-output/<stamp>/fixtures/` and the parser tests run against
//! them (`codex.P1-basic` … `codex.P9-bad-model`).
//!
//! Observed contract this adapter encodes:
//!
//! - **Envelope**: `thread.started{thread_id}` → `turn.started` →
//!   `item.started`/`item.completed{item}`\* → `turn.completed{usage}`,
//!   with `turn.failed{error{message}}` (preceded by a top-level
//!   `error{message}`) on the failure path.
//! - **Item types**: `agent_message{text}` (the answer, possibly several
//!   per turn), `command_execution{command, aggregated_output, exit_code,
//!   status}` (all shell work — *file writes too*: codex writes through
//!   PowerShell `Set-Content`, no separate `file_change` item appeared),
//!   `todo_list{items[{text, completed}]}` (its plan surface — progress
//!   reporting, **not** a human gate), and `error{message}` for non-fatal
//!   notices (hook-timeout clamping, skills-budget overflow).
//! - **No decision surface.** `codex exec` exposes no asking tool and
//!   *rejects* `--ask-for-approval` outright (exit 2, "unexpected
//!   argument" — fixture P4); approvals exist only in the interactive TUI
//!   / app-server. So codex asks the way agy does, through the text
//!   protocol: [`crate::decision::from_text`] over `agent_message` text
//!   (fixture P5 is a real turn whose answer is a fenced ask block).
//! - **Prompt delivery is stdin.** `codex exec … -` reads the prompt from
//!   stdin (verified), which sidesteps argv quoting and prompts that start
//!   with a dash entirely.
//! - **Resume**: `codex exec resume <thread_id> … ` preserves the thread id
//!   and the context (verified: BANANA42 round-trip). The `resume`
//!   subcommand has a *narrower* flag set — no `--sandbox`, `--cd` or
//!   `--add-dir` — so the sandbox rides as `-c sandbox_mode="…"` and the
//!   working root is the process cwd.
//! - **Exit codes**: 0 with `turn.completed` is success; a rejected model
//!   is exit 1 with `error` + `turn.failed` carrying an embedded
//!   `{"status": 400, …}` (the typed code the [`Classifier`] ladder
//!   consumes); an unknown flag is exit 2 with no turn at all.
//! - **Usage** (`turn.completed.usage`): `input_tokens`,
//!   `cached_input_tokens`, `cache_write_input_tokens`, `output_tokens`,
//!   `reasoning_output_tokens`. Token-only — codex reports no cost field.
//!   A trivial prompt billed 21 437 input tokens, which is the fixed
//!   instruction/skills preamble ([`CODEX_SESSION_OVERHEAD_TOKENS`]).
//! - **Structured output**: `--output-schema <file>` constrains the final
//!   `agent_message` to schema-shaped JSON (verified) — there is no
//!   separate structured item, so the adapter parses the last message.
//!
//! Classification follows F-00 §4: exit code + presence of a terminal turn
//! event + typed provider code, never stderr text (codex stderr is
//! wall-to-wall skill-YAML noise; it is drained for debug only).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::broadcast;

use crate::adapter::{RuntimeAdapter, SessionBackend, SessionHandle};
use crate::error::{AdapterError, AdapterFailure, Classifier};
use crate::events::AdapterEvent;
use crate::types::{AuthStatus, Capabilities, RuntimeInfo, SpawnSpec, UsageSnapshot};

/// Stable adapter identifier (F-02 `RuntimeAdapter::id`).
pub const CODEX_ADAPTER_ID: &str = "codex";
/// Fixed per-session preamble codex bills before the user prompt: 21 437
/// input tokens observed on "Reply with exactly: PROBE_OK" (fixture P1).
/// Instructions + skills index, not the user's words.
pub const CODEX_SESSION_OVERHEAD_TOKENS: u64 = 21_000;
/// Env var overriding the codex binary location.
pub const CODEX_BIN_ENV: &str = "AGENTOS_CODEX_BIN";
/// Env var that opts the ignored e2e tests into live (free-only) probes.
pub const CODEX_E2E_ENV: &str = "AGENTOS_CODEX_E2E";
/// Compact `ToolUse` summaries never carry full payloads (F-00 §3).
const TOOL_SUMMARY_MAX: usize = 160;

/// Sandbox policy for model-generated shell commands
/// (`--sandbox <read-only|workspace-write|danger-full-access>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodexSandbox {
    /// Reads only; every write attempt is refused by the sandbox.
    ReadOnly,
    /// Writes inside the workspace (plus `--add-dir` grants).
    WorkspaceWrite,
    /// No sandbox at all. The adapter never selects this on its own.
    DangerFullAccess,
}

impl CodexSandbox {
    /// Flag value as the CLI expects it (also the `sandbox_mode` config
    /// value used on the resume path).
    pub fn as_str(self) -> &'static str {
        match self {
            CodexSandbox::ReadOnly => "read-only",
            CodexSandbox::WorkspaceWrite => "workspace-write",
            CodexSandbox::DangerFullAccess => "danger-full-access",
        }
    }
}

/// One fully-resolved `codex exec` invocation (pure, unit-testable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexInvocation {
    /// The prompt. Delivered on **stdin** (argv carries `-`), so quoting
    /// and leading-dash prompts are non-issues.
    pub objective: String,
    /// Working root: the child's cwd and, on a fresh run, `--cd`.
    pub workspace: PathBuf,
    /// Extra writable grants (`--add-dir`, repeatable). Entries equal to
    /// the workspace are deduplicated. Fresh runs only — `resume` has no
    /// such flag.
    pub extra_dirs: Vec<PathBuf>,
    /// Sandbox policy; see [`CodexSandbox`].
    pub sandbox: CodexSandbox,
    /// `--model` passthrough when set.
    pub model: Option<String>,
    /// `--output-schema <file>` when structured output is requested.
    pub output_schema: Option<PathBuf>,
    /// `codex exec resume <thread_id>` target when set.
    pub resume_thread: Option<String>,
    /// Redirected home for env isolation (F-02 `SpawnSpec::isolated_home`).
    pub isolated_home: Option<PathBuf>,
    /// Harness wall-clock budget. `0` disables the watchdog. codex exec has
    /// no timeout flag of its own, so the harness owns this entirely.
    pub timeout_secs: u64,
}

impl CodexInvocation {
    /// Derive an invocation from a spawn spec.
    ///
    /// Sandbox mapping mirrors the F-05 agy derivation: `SpawnSpec` has no
    /// write-intent field, so an empty `allowed_paths` means a read-only
    /// analysis run (`read-only`) and a non-empty list declares write
    /// targets (`workspace-write`). `danger-full-access` is never derived —
    /// it must be asked for explicitly.
    pub fn from_spec(spec: &SpawnSpec) -> Self {
        let extra_dirs = spec
            .allowed_paths
            .iter()
            .map(PathBuf::from)
            .filter(|path| *path != spec.workspace)
            .collect();
        Self {
            objective: spec.objective.clone(),
            workspace: spec.workspace.clone(),
            extra_dirs,
            sandbox: if spec.allowed_paths.is_empty() {
                CodexSandbox::ReadOnly
            } else {
                CodexSandbox::WorkspaceWrite
            },
            model: spec.model.clone(),
            output_schema: None,
            resume_thread: None,
            isolated_home: spec.isolated_home.clone(),
            timeout_secs: spec.timeout_secs,
        }
    }

    /// Override the model.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Override the sandbox policy.
    pub fn with_sandbox(mut self, sandbox: CodexSandbox) -> Self {
        self.sandbox = sandbox;
        self
    }

    /// Request structured output via an `--output-schema` file.
    pub fn with_output_schema(mut self, schema_file: impl Into<PathBuf>) -> Self {
        self.output_schema = Some(schema_file.into());
        self
    }

    /// Resume the given thread (`codex exec resume <id>`).
    pub fn with_resume(mut self, thread_id: impl Into<String>) -> Self {
        self.resume_thread = Some(thread_id.into());
        self
    }

    /// Render the argv vector (canonical order, stable for tests).
    ///
    /// The final element is always `-`: the prompt goes on stdin.
    pub fn args(&self) -> Vec<String> {
        let mut args = vec!["exec".to_owned()];
        match &self.resume_thread {
            Some(thread) => {
                // `resume` accepts neither --sandbox nor --cd/--add-dir
                // (verified: exit 2 "unexpected argument"), so the policy
                // rides as a config override and the cwd is the root.
                args.push("resume".to_owned());
                args.push(thread.clone());
                args.push("--json".to_owned());
                args.push("--skip-git-repo-check".to_owned());
                args.push("-c".to_owned());
                args.push(format!("sandbox_mode=\"{}\"", self.sandbox.as_str()));
            }
            None => {
                args.push("--json".to_owned());
                args.push("--skip-git-repo-check".to_owned());
                args.push("--sandbox".to_owned());
                args.push(self.sandbox.as_str().to_owned());
                args.push("--cd".to_owned());
                args.push(self.workspace.to_string_lossy().into_owned());
                for dir in &self.extra_dirs {
                    args.push("--add-dir".to_owned());
                    args.push(dir.to_string_lossy().into_owned());
                }
            }
        }
        if let Some(model) = &self.model {
            args.push("--model".to_owned());
            args.push(model.clone());
        }
        if let Some(schema) = &self.output_schema {
            args.push("--output-schema".to_owned());
            args.push(schema.to_string_lossy().into_owned());
        }
        args.push("-".to_owned());
        args
    }

    /// Env-var redirection for home isolation. codex keeps auth and config
    /// under `CODEX_HOME` (default `~/.codex`), so isolation redirects that
    /// alongside the platform home vars; building the isolated home is the
    /// supervisor's job, not the adapter's.
    pub fn env_overrides(&self) -> Vec<(&'static str, PathBuf)> {
        self.isolated_home
            .as_ref()
            .map(|home| {
                vec![
                    ("USERPROFILE", home.clone()),
                    ("HOME", home.clone()),
                    ("CODEX_HOME", home.join(".codex")),
                ]
            })
            .unwrap_or_default()
    }
}

/// `turn.completed.usage` object. All fields optional — provider schemas
/// drift, parsing stays total.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexUsage {
    /// Prompt tokens (the cached portion is a subset of this, not an
    /// addition — observed 21 437 input with 11 008 cached).
    pub input_tokens: Option<u64>,
    /// Cache-hit tokens inside `input_tokens`.
    pub cached_input_tokens: Option<u64>,
    /// Tokens written into the cache this turn.
    pub cache_write_input_tokens: Option<u64>,
    /// Completion tokens.
    pub output_tokens: Option<u64>,
    /// Reasoning tokens (a subset of the completion budget).
    pub reasoning_output_tokens: Option<u64>,
}

impl CodexUsage {
    /// Map onto the F-02 ledger snapshot.
    ///
    /// `cost_usd` is `None` (codex reports no cost field — OBS-04: label
    /// estimates only where they exist) and `per_model` is empty (one exec
    /// run bills one model, with no per-model breakdown). `total_tokens` is
    /// input + output: the cached tokens are already inside `input_tokens`,
    /// so adding them would double-count.
    pub fn to_snapshot(&self) -> UsageSnapshot {
        let input = self.input_tokens.unwrap_or(0);
        let output = self.output_tokens.unwrap_or(0);
        UsageSnapshot {
            input_tokens: input,
            output_tokens: output,
            thinking_tokens: self.reasoning_output_tokens.unwrap_or(0),
            cache_read_tokens: self.cached_input_tokens.unwrap_or(0),
            total_tokens: input + output,
            cost_usd: None,
            per_model: Vec::new(),
            session_overhead_tokens: CODEX_SESSION_OVERHEAD_TOKENS,
        }
    }
}

/// Typed provider code out of a codex failure message.
///
/// The failure text is itself JSON (observed: `{"type":"error","status":
/// 400,"error":{...}}`), so the HTTP status is machine-readable — this is
/// payload content, never stderr scraping. Feeding it to the [`Classifier`]
/// turns 401/403 into `AuthFailure` and 429 into `Transient` automatically.
fn provider_code_from_error(message: &str) -> Option<i64> {
    let value: Value = serde_json::from_str(message.trim()).ok()?;
    value
        .get("status")
        .or_else(|| value.get("error").and_then(|error| error.get("status")))
        .and_then(Value::as_i64)
}

/// Compact summary of a `command_execution` item (never the full output).
fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_owned()
    } else {
        let cut: String = text.chars().take(max).collect();
        format!("{cut}...")
    }
}

/// Incremental reducer over `codex exec --json` NDJSON.
///
/// Total by construction: an unparseable or unknown line yields no events
/// (schema drift must never kill a run).
#[derive(Debug, Default)]
pub struct CodexStreamReducer {
    thread_id: Option<String>,
    last_message: Option<String>,
    failure: Option<String>,
    saw_turn_terminal: bool,
    usage: Option<CodexUsage>,
    structured_requested: bool,
}

impl CodexStreamReducer {
    /// A reducer that will try to parse the final message as structured
    /// output (set when the invocation carried `--output-schema`).
    pub fn expecting_structured_output() -> Self {
        Self {
            structured_requested: true,
            ..Self::default()
        }
    }

    /// Map one NDJSON line to zero or more adapter events.
    pub fn push_line(&mut self, line: &str) -> Vec<AdapterEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            tracing::debug!(line = %trimmed, "codex adapter: skipping non-JSON stream line");
            return Vec::new();
        };
        match value.get("type").and_then(Value::as_str) {
            Some("thread.started") => {
                let thread_id = value
                    .get("thread_id")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned();
                self.thread_id = Some(thread_id.clone());
                vec![AdapterEvent::Started {
                    session_id: thread_id,
                    // codex does not report the resolved model in its
                    // stream; the invocation knows it, the stream does not.
                    model: None,
                }]
            }
            Some("turn.started") => Vec::new(),
            Some("item.started") => self.item_events(&value, true),
            Some("item.completed") => self.item_events(&value, false),
            Some("turn.completed") => {
                self.saw_turn_terminal = true;
                match value
                    .get("usage")
                    .and_then(|usage| serde_json::from_value::<CodexUsage>(usage.clone()).ok())
                {
                    Some(usage) => {
                        self.usage = Some(usage);
                        vec![AdapterEvent::UsageUpdate(usage.to_snapshot())]
                    }
                    None => Vec::new(),
                }
            }
            Some("turn.failed") => {
                self.saw_turn_terminal = true;
                self.failure = value
                    .get("error")
                    .and_then(|error| error.get("message"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| Some("codex reported turn.failed".to_owned()));
                Vec::new()
            }
            // Top-level `error` precedes `turn.failed` on the failure path
            // and also stands alone when the run dies before a turn.
            Some("error") => {
                if self.failure.is_none() {
                    self.failure = value
                        .get("message")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                Vec::new()
            }
            other => {
                tracing::debug!(kind = ?other, "codex adapter: skipping unknown stream event");
                Vec::new()
            }
        }
    }

    /// Events for one `item.started` / `item.completed` envelope.
    fn item_events(&mut self, value: &Value, started: bool) -> Vec<AdapterEvent> {
        let Some(item) = value.get("item") else {
            return Vec::new();
        };
        match item.get("type").and_then(Value::as_str) {
            // Shell work (writes included). The started/completed pair
            // describes one call, so only the start emits a ToolUse.
            Some("command_execution") => {
                if !started {
                    return Vec::new();
                }
                let command = item
                    .get("command")
                    .and_then(Value::as_str)
                    .unwrap_or("(unknown command)");
                vec![AdapterEvent::ToolUse {
                    tool: "command_execution".to_owned(),
                    args_summary: truncate(command, TOOL_SUMMARY_MAX),
                }]
            }
            // codex's plan surface. Progress reporting, NOT a human gate —
            // it must never become a Decision, or every plan update would
            // block the run on a click.
            Some("todo_list") => {
                if started {
                    return Vec::new();
                }
                let items = item
                    .get("items")
                    .and_then(Value::as_array)
                    .map(|items| items.len())
                    .unwrap_or(0);
                vec![AdapterEvent::ToolUse {
                    tool: "todo_list".to_owned(),
                    args_summary: format!("{items} plan items"),
                }]
            }
            Some("agent_message") => {
                if started {
                    return Vec::new();
                }
                let Some(text) = item.get("text").and_then(Value::as_str) else {
                    return Vec::new();
                };
                self.last_message = Some(text.to_owned());
                let mut events = vec![AdapterEvent::TextDelta(text.to_owned())];
                // codex has no asking tool: a question with options rides
                // in the answer as a fenced block (F-02 text protocol).
                events.extend(crate::decision::from_text(text));
                events
            }
            // Non-fatal notices (hook clamping, skills-budget overflow).
            // Diagnostics, not run failures — the turn continues.
            Some("error") => {
                if let Some(message) = item.get("message").and_then(Value::as_str) {
                    tracing::debug!(target: "agentos_adapters::codex::notice", message = %message);
                }
                Vec::new()
            }
            other => {
                tracing::debug!(item_type = ?other, "codex adapter: skipping unconsumed item");
                Vec::new()
            }
        }
    }

    /// The thread id learned from `thread.started` — the resume handle.
    pub fn thread_id(&self) -> Option<&str> {
        self.thread_id.as_deref()
    }

    /// The last `agent_message` text seen (the final answer).
    pub fn last_message(&self) -> Option<&str> {
        self.last_message.as_deref()
    }

    /// Terminal event once the process exit code is known.
    ///
    /// Success is the F-00 §4 conjunction: exit 0, a `turn.completed`, and
    /// no failure message. Anything else goes through the [`Classifier`]
    /// with the typed provider code parsed out of the failure payload.
    pub fn finish(&self, exit_code: i32) -> AdapterEvent {
        let failed = self.failure.is_some();
        if exit_code == 0 && self.saw_turn_terminal && !failed {
            return AdapterEvent::Finished {
                exit_code,
                final_result: self.last_message.clone(),
                structured: self.structured_output(),
            };
        }
        let provider_code = self.failure.as_deref().and_then(provider_code_from_error);
        let failure = Classifier::classify(exit_code, self.saw_turn_terminal, provider_code, false);
        AdapterEvent::Failed(match (failure, self.failure.as_deref()) {
            // Keep the provider's own words on the failure, which the
            // classifier only sees as a code.
            (AdapterFailure::TaskFailure { detail }, Some(message)) => {
                AdapterFailure::TaskFailure {
                    detail: format!("{detail}: {message}"),
                }
            }
            (other, _) => other,
        })
    }

    /// The final message parsed as JSON, when a schema was requested.
    /// codex emits no separate structured item: `--output-schema`
    /// constrains the final `agent_message` itself (verified, fixture P7).
    fn structured_output(&self) -> Option<Value> {
        if !self.structured_requested {
            return None;
        }
        serde_json::from_str(self.last_message.as_deref()?.trim()).ok()
    }
}

/// Harness-side timeout classification: codex exec has no timeout flag, so
/// the watchdog is the only budget. Timeout → `Transient` (retryable).
fn harness_timeout_failure(timeout_secs: u64) -> AdapterFailure {
    AdapterFailure::Transient {
        detail: format!(
            "codex exec run exceeded the {timeout_secs}s harness budget; process killed"
        ),
    }
}

// ---------------------------------------------------------------------------
// Live session plumbing
// ---------------------------------------------------------------------------

/// Shared mutable state of one codex session.
struct CodexSessionState {
    /// Adapter-internal handle id (`codex-<n>`); the provider thread id is
    /// learned at runtime from `thread.started`.
    handle_id: String,
    /// Resolved codex binary (recorded at session start).
    binary: PathBuf,
    /// The first run's invocation; follow-ups clone it and swap the
    /// objective for a `resume` run.
    base_invocation: CodexInvocation,
    /// Provider thread id once learned (enables resume).
    thread_id: Mutex<Option<String>>,
    /// The live child, parked so `cancel` can tree-kill it.
    child: tokio::sync::Mutex<Option<Child>>,
    /// Cancel requested — stream goes quiet, no terminal event.
    cancelled: AtomicBool,
    /// Session permanently over (a `Failed` terminal or cancel).
    dead: AtomicBool,
    /// An exec run is in flight (one at a time).
    run_active: AtomicBool,
}

/// [`SessionBackend`] for one codex session.
struct CodexSessionBackend {
    state: std::sync::Arc<CodexSessionState>,
    events: broadcast::Sender<AdapterEvent>,
}

#[async_trait]
impl SessionBackend for CodexSessionBackend {
    /// Deliver one instruction.
    ///
    /// `codex exec` runs are one-shot, so an instruction is a **new exec
    /// run resuming the thread** (`codex exec resume <thread_id>`, verified
    /// to preserve both the id and the context). Its events, terminal event
    /// included, stream on the same session channel — the same documented
    /// extension F-05 makes for agy: `Finished` means *turn complete*, and
    /// the session stays instructable until it fails or is cancelled.
    async fn send_instruction(&self, text: String) -> Result<(), AdapterError> {
        let state = &self.state;
        if state.cancelled.load(Ordering::Relaxed) || state.dead.load(Ordering::Relaxed) {
            return Err(AdapterError::SessionNotActive(state.handle_id.clone()));
        }
        if state.run_active.swap(true, Ordering::Relaxed) {
            // Alive, just mid-turn. Distinct from a dead session:
            // the caller must wait for the terminal event, never open
            // a second session — a fresh session has none of the
            // conversation and re-asks everything it already asked.
            return Err(AdapterError::Busy(state.handle_id.clone()));
        }
        let thread = state
            .thread_id
            .lock()
            .expect("codex thread state poisoned")
            .clone();
        let Some(thread) = thread else {
            state.run_active.store(false, Ordering::Relaxed);
            return Err(AdapterError::Internal(
                "no codex thread id known yet; wait for the first run's \
                 thread.started event"
                    .to_owned(),
            ));
        };
        let mut invocation = state.base_invocation.clone();
        invocation.objective = text;
        invocation.resume_thread = Some(thread);
        match codex_command(&state.binary, &invocation).spawn() {
            Ok(child) => {
                let state = state.clone();
                let events = self.events.clone();
                tokio::spawn(async move {
                    drive_exec_run(state, invocation, events, child).await;
                });
                Ok(())
            }
            Err(error) => {
                state.run_active.store(false, Ordering::Relaxed);
                state.dead.store(true, Ordering::Relaxed);
                let _ = self
                    .events
                    .send(AdapterEvent::Failed(AdapterFailure::SpawnFailure {
                        detail: format!("failed to spawn resumed codex exec run: {error}"),
                    }));
                Ok(())
            }
        }
    }

    /// Cancel: process-kill semantics. The stream ends without a terminal
    /// event, exactly as a killed CLI behaves.
    async fn cancel(&self) -> Result<(), AdapterError> {
        self.state.cancelled.store(true, Ordering::Relaxed);
        self.state.dead.store(true, Ordering::Relaxed);
        if let Some(mut child) = self.state.child.lock().await.take() {
            kill_child(&mut child).await;
        }
        Ok(())
    }
}

/// The live codex adapter.
///
/// Discovery (`detect`/`auth_status`/`capabilities`/`health`) is free and
/// makes no billable call; auth stays entirely with the codex CLI — the
/// adapter never touches credentials.
pub struct CodexAdapter {
    next_session: AtomicUsize,
    sessions: Mutex<Vec<SessionHandle>>,
}

impl CodexAdapter {
    /// Create the adapter.
    pub fn new() -> Self {
        Self {
            next_session: AtomicUsize::new(0),
            sessions: Mutex::new(Vec::new()),
        }
    }
    /// Spawn one session from a ready invocation. Shared by
    /// `start_session` (fresh) and `resume_session` (`codex exec resume <thread_id>`).
    async fn launch(&self, invocation: CodexInvocation) -> Result<SessionHandle, AdapterError> {
        let binary = resolve_binary().ok_or_else(|| {
            AdapterError::Internal(format!(
                "codex binary not found (checked ${CODEX_BIN_ENV}, PATH)"
            ))
        })?;
        // Spawn synchronously so machinery failures (bad cwd, missing
        // binary) surface as `Result` errors, per the F-02 contract.
        let child = codex_command(&binary, &invocation)
            .spawn()
            .map_err(|error| {
                AdapterError::Internal(format!(
                    "failed to spawn codex in {}: {error}",
                    invocation.workspace.display()
                ))
            })?;

        let session_no = self.next_session.fetch_add(1, Ordering::Relaxed) + 1;
        let handle_id = format!("codex-{session_no}");
        let (events, _) = broadcast::channel(1024);
        let state = std::sync::Arc::new(CodexSessionState {
            handle_id: handle_id.clone(),
            binary,
            base_invocation: invocation.clone(),
            thread_id: Mutex::new(None),
            child: tokio::sync::Mutex::new(None),
            cancelled: AtomicBool::new(false),
            dead: AtomicBool::new(false),
            run_active: AtomicBool::new(true),
        });
        let backend = std::sync::Arc::new(CodexSessionBackend {
            state: state.clone(),
            events: events.clone(),
        });
        let handle = SessionHandle::new(handle_id, events.clone(), backend);
        self.sessions
            .lock()
            .expect("codex session registry poisoned")
            .push(handle.clone());

        let driver_state = state;
        let driver_invocation = invocation;
        let driver_events = events;
        tokio::spawn(async move {
            drive_exec_run(driver_state, driver_invocation, driver_events, child).await;
        });
        Ok(handle)
    }
}

impl Default for CodexAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RuntimeAdapter for CodexAdapter {
    fn id(&self) -> &str {
        CODEX_ADAPTER_ID
    }

    async fn detect(&self) -> RuntimeInfo {
        let path = resolve_binary();
        let version = match &path {
            Some(binary) => probe_version(binary).await,
            None => None,
        };
        RuntimeInfo {
            id: CODEX_ADAPTER_ID.to_owned(),
            version,
            path,
        }
    }

    /// Free tier-0 check only (F-00 §5): binary presence plus the codex
    /// credential file (`$CODEX_HOME/auth.json`, default `~/.codex`). The
    /// authoritative check is `codex login status`, a live invocation, so
    /// it stays out of the free path.
    async fn auth_status(&self) -> AuthStatus {
        if resolve_binary().is_none() {
            return AuthStatus::Unknown;
        }
        match codex_home() {
            Some(home) if home.join("auth.json").is_file() => AuthStatus::Ready,
            Some(_) => AuthStatus::NeedsLogin,
            None => AuthStatus::Unknown,
        }
    }

    async fn capabilities(&self) -> Capabilities {
        codex_capabilities()
    }

    async fn start_session(&self, spec: SpawnSpec) -> Result<SessionHandle, AdapterError> {
        self.launch(CodexInvocation::from_spec(&spec)).await
    }

    /// Continue an earlier provider conversation: same invocation,
    /// plus `codex exec resume <thread_id>`.
    async fn resume_session(
        &self,
        spec: SpawnSpec,
        provider_session_id: String,
    ) -> Result<SessionHandle, AdapterError> {
        self.launch(CodexInvocation::from_spec(&spec).with_resume(provider_session_id))
            .await
    }

    async fn shutdown(&self) -> Result<(), AdapterError> {
        let sessions: Vec<SessionHandle> = self
            .sessions
            .lock()
            .expect("codex session registry poisoned")
            .clone();
        for handle in &sessions {
            let _ = handle.cancel().await;
        }
        Ok(())
    }
}

/// Capability surface, each flag annotated with its observed basis.
///
/// Per-tool allow/deny does not exist on `codex exec` (the sandbox modes
/// and `.rules` execpolicy files are the surface); as on agy, F-10's
/// harness-side gating compensates.
fn codex_capabilities() -> Capabilities {
    Capabilities {
        // `--sandbox workspace-write` write verified (fixture P6).
        filesystem_edit: true,
        // Every tool call observed is `command_execution` (fixture P2).
        shell: true,
        // No web/browser tool appeared in any observed exec run, and
        // `codex exec` exposes no search flag: not claimed.
        network: false,
        // `--output-schema` constrains the final message (fixture P7).
        structured_output: true,
        // `codex exec resume <thread_id>` preserves id + context (P10).
        resume: true,
        // `-i, --image <FILE>` on both exec and resume.
        multimodal: true,
        // gpt-5.x-class context window.
        long_context: true,
        // `codex mcp` manages external MCP servers for the CLI.
        mcp_client: true,
    }
}

/// Resolve the codex binary: `$AGENTOS_CODEX_BIN`, then a PATH scan. Free.
fn resolve_binary() -> Option<PathBuf> {
    if let Some(override_path) = std::env::var_os(CODEX_BIN_ENV) {
        if !override_path.is_empty() {
            return Some(PathBuf::from(override_path));
        }
    }
    find_on_path()
}

#[cfg(windows)]
const EXECUTABLE_NAMES: [&str; 3] = ["codex.exe", "codex.cmd", "codex"];

#[cfg(not(windows))]
const EXECUTABLE_NAMES: [&str; 1] = ["codex"];

fn find_on_path() -> Option<PathBuf> {
    let path_env = std::env::var_os("PATH")?;
    std::env::split_paths(&path_env).find_map(|dir| {
        EXECUTABLE_NAMES
            .iter()
            .map(|name| dir.join(name))
            .find(|candidate| candidate.is_file())
    })
}

/// `$CODEX_HOME`, else `~/.codex` — where `auth.json` and `config.toml`
/// live. The only filesystem auth signal available without a live call.
fn codex_home() -> Option<PathBuf> {
    if let Some(home) = std::env::var_os("CODEX_HOME") {
        if !home.is_empty() {
            return Some(PathBuf::from(home));
        }
    }
    let home = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"))?;
    Some(PathBuf::from(home).join(".codex"))
}

/// Free `--version` probe; returns the first stdout line when available.
async fn probe_version(binary: &Path) -> Option<String> {
    let output = Command::new(binary).arg("--version").output().await.ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().next()?.trim();
    (!line.is_empty()).then(|| line.to_owned())
}

/// Assemble the child command for one exec run. Argv-vector only — no
/// shell, no string quoting; the prompt goes on stdin.
fn codex_command(binary: &Path, invocation: &CodexInvocation) -> Command {
    let mut command = Command::new(binary);
    command
        .args(invocation.args())
        .current_dir(&invocation.workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for (key, value) in invocation.env_overrides() {
        command.env(key, value);
    }
    command
}

/// Kill the codex child process.
///
/// Windows: tree-kill via `taskkill /PID <pid> /T /F` first (handoff §7.7)
/// — codex spawns PowerShell children for every tool call, and a bare
/// `TerminateProcess` on the direct child would orphan them.
async fn kill_child(child: &mut Child) {
    #[cfg(windows)]
    if let Some(pid) = child.id() {
        let tree_kill = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .output()
            .await;
        match &tree_kill {
            Ok(output) if output.status.success() => {}
            other => tracing::warn!(?other, pid, "codex adapter: taskkill tree-kill failed"),
        }
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

enum Watched<T> {
    Completed(T),
    TimedOut,
}

/// Run `fut` under the harness watchdog. `timeout_secs == 0` disables it.
async fn harness_watchdog<F: std::future::Future>(timeout_secs: u64, fut: F) -> Watched<F::Output> {
    if timeout_secs == 0 {
        return Watched::Completed(fut.await);
    }
    match tokio::time::timeout(Duration::from_secs(timeout_secs), fut).await {
        Ok(value) => Watched::Completed(value),
        Err(_) => Watched::TimedOut,
    }
}

async fn kill_stored_child(state: &CodexSessionState) {
    if let Some(mut child) = state.child.lock().await.take() {
        kill_child(&mut child).await;
    }
}

/// Reap the child and return its exit code. `None` child means cancel()
/// already took it.
async fn take_exit_code(state: &CodexSessionState) -> i32 {
    match state.child.lock().await.take() {
        Some(mut child) => match child.wait().await {
            Ok(status) => status.code().unwrap_or(-1),
            Err(error) => {
                tracing::warn!(%error, "codex adapter: failed to reap child");
                -1
            }
        },
        None => -1,
    }
}

/// Drain stderr line by line. Diagnostics only — classification never
/// reads stderr (F-00 §4). codex stderr is wall-to-wall skill-YAML noise.
async fn drain_stderr(stderr: ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!(target: "agentos_adapters::codex::stderr", line = %line);
    }
}

/// Consume NDJSON stdout, forwarding mapped events until EOF or cancel.
async fn read_stream(
    stdout: ChildStdout,
    state: &CodexSessionState,
    events: &broadcast::Sender<AdapterEvent>,
    mut reducer: CodexStreamReducer,
) -> CodexStreamReducer {
    let mut lines = BufReader::new(stdout).lines();
    'lines: loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) | Err(_) => break 'lines,
        };
        if state.cancelled.load(Ordering::Relaxed) {
            break 'lines;
        }
        for event in reducer.push_line(&line) {
            if state.cancelled.load(Ordering::Relaxed) {
                break 'lines;
            }
            let _ = events.send(event);
        }
    }
    reducer
}

/// Drive one exec run to its terminal event: write the prompt to stdin,
/// stream stdout, reap, classify. Cancelled runs end silently.
async fn drive_exec_run(
    state: std::sync::Arc<CodexSessionState>,
    invocation: CodexInvocation,
    events: broadcast::Sender<AdapterEvent>,
    mut child: Child,
) {
    // The prompt is stdin (argv carries `-`): write it and close, or codex
    // waits on EOF forever.
    if let Some(mut stdin) = child.stdin.take() {
        let prompt = invocation.objective.clone();
        tokio::spawn(async move {
            if let Err(error) = stdin.write_all(prompt.as_bytes()).await {
                tracing::warn!(%error, "codex adapter: failed to write prompt to stdin");
            }
            let _ = stdin.shutdown().await;
        });
    }
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    *state.child.lock().await = Some(child);
    if let Some(stderr) = stderr {
        tokio::spawn(drain_stderr(stderr));
    }

    // Broadcast channels do not replay. Do not consume Codex's leading
    // `thread.started` frame until the caller has subscribed, otherwise the
    // resumable provider UUID is lost and the local `codex-<n>` handle can
    // be mistaken for a thread id by higher layers.
    while events.receiver_count() == 0 && !state.cancelled.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    if state.cancelled.load(Ordering::Relaxed) {
        kill_stored_child(&state).await;
        return;
    }

    let run = async {
        let reducer = match invocation.output_schema {
            Some(_) => CodexStreamReducer::expecting_structured_output(),
            None => CodexStreamReducer::default(),
        };
        let reducer = match stdout {
            Some(stdout) => read_stream(stdout, &state, &events, reducer).await,
            None => reducer,
        };
        let exit_code = take_exit_code(&state).await;
        (reducer, exit_code)
    };

    let (reducer, exit_code) = match harness_watchdog(invocation.timeout_secs, run).await {
        Watched::Completed(pair) => pair,
        Watched::TimedOut => {
            kill_stored_child(&state).await;
            state.run_active.store(false, Ordering::Relaxed);
            state.dead.store(true, Ordering::Relaxed);
            let _ = events.send(AdapterEvent::Failed(harness_timeout_failure(
                invocation.timeout_secs,
            )));
            return;
        }
    };

    state.run_active.store(false, Ordering::Relaxed);
    if state.cancelled.load(Ordering::Relaxed) {
        return; // process-kill semantics: the stream just ends
    }
    if let Some(id) = reducer.thread_id().map(str::to_owned) {
        let mut guard = state.thread_id.lock().expect("codex thread state poisoned");
        if guard.is_none() {
            *guard = Some(id);
        }
    }

    let terminal = reducer.finish(exit_code);
    if matches!(terminal, AdapterEvent::Failed(_)) {
        state.dead.store(true, Ordering::Relaxed);
    }
    let _ = events.send(terminal);
}

// ---------------------------------------------------------------------------
// Tests — offline. The stream fixtures are REAL frozen codex transcripts
// (cli-codex-output/<stamp>/fixtures), not synthetic: every parser claim
// below is checked against captured provider output. No test spawns the
// binary or makes a billable call; the live probes are `#[ignore]`d and
// gated behind AGENTOS_CODEX_E2E=1, and only free (`--version`) at that.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn spec(objective: &str, allowed_paths: Vec<String>, timeout_secs: u64) -> SpawnSpec {
        SpawnSpec {
            task_id: Uuid::new_v4(),
            objective: objective.to_owned(),
            workspace: PathBuf::from("worktrees/task-codex"),
            allowed_paths,
            forbidden_paths: vec![],
            tool_allowlist: vec![],
            tool_denylist: vec![],
            model: None,
            timeout_secs,
            isolated_home: None,
        }
    }

    /// The frozen corpus, newest run wins. Absent elsewhere: skip, don't
    /// fail (same contract as the claude fixture tests).
    fn fixture(name: &str) -> Option<String> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../cli-codex-output");
        let mut runs: Vec<_> = std::fs::read_dir(root)
            .ok()?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect();
        runs.sort();
        std::fs::read_to_string(runs.last()?.join("fixtures").join(name)).ok()
    }

    fn reduce(transcript: &str) -> (Vec<AdapterEvent>, CodexStreamReducer) {
        let mut reducer = CodexStreamReducer::default();
        let events = transcript
            .lines()
            .flat_map(|line| reducer.push_line(line))
            .collect();
        (events, reducer)
    }

    // -- arg builder --------------------------------------------------------

    #[test]
    fn a_read_only_run_declares_no_write_targets() {
        let invocation = CodexInvocation::from_spec(&spec("analyze this", vec![], 900));
        assert_eq!(invocation.sandbox, CodexSandbox::ReadOnly);
        assert_eq!(
            invocation.args(),
            vec![
                "exec",
                "--json",
                "--skip-git-repo-check",
                "--sandbox",
                "read-only",
                "--cd",
                "worktrees/task-codex",
                "-",
            ]
        );
    }

    #[test]
    fn declared_write_targets_select_workspace_write_and_add_dirs() {
        let invocation = CodexInvocation::from_spec(&spec(
            "build it",
            vec![
                "worktrees/task-codex".to_owned(), // deduplicated: it IS the workspace
                "shared/design".to_owned(),
            ],
            600,
        ))
        .with_model("gpt-5.6-terra")
        .with_output_schema("schemas/out.json");
        assert_eq!(invocation.sandbox, CodexSandbox::WorkspaceWrite);
        let args = invocation.args();
        assert_eq!(args.iter().filter(|arg| *arg == "--add-dir").count(), 1);
        assert!(args
            .windows(2)
            .any(|pair| pair[0] == "--add-dir" && pair[1] == "shared/design"));
        assert!(args
            .windows(2)
            .any(|pair| pair[0] == "--model" && pair[1] == "gpt-5.6-terra"));
        assert!(args
            .windows(2)
            .any(|pair| pair[0] == "--output-schema" && pair[1] == "schemas/out.json"));
        assert_eq!(args.last().unwrap(), "-", "the prompt goes on stdin");
    }

    /// `codex exec resume` rejects `--sandbox`, `--cd` and `--add-dir`
    /// (verified: exit 2 "unexpected argument"), so the policy must ride as
    /// a config override and the workspace as the process cwd.
    #[test]
    fn a_resume_run_carries_the_sandbox_as_a_config_override() {
        let invocation = CodexInvocation::from_spec(&spec("continue", vec!["w".to_owned()], 600))
            .with_resume("01a02a69-521f-71e1-bcaa-d630e84f07ed");
        let args = invocation.args();
        assert_eq!(
            &args[..3],
            &["exec", "resume", "01a02a69-521f-71e1-bcaa-d630e84f07ed"]
        );
        assert!(args
            .windows(2)
            .any(|pair| { pair[0] == "-c" && pair[1] == "sandbox_mode=\"workspace-write\"" }));
        for rejected in ["--sandbox", "--cd", "--add-dir"] {
            assert!(
                !args.iter().any(|arg| arg == rejected),
                "resume rejects {rejected}"
            );
        }
    }

    #[test]
    fn home_isolation_redirects_codex_home_too() {
        let mut invocation = CodexInvocation::from_spec(&spec("go", vec![], 60));
        assert!(invocation.env_overrides().is_empty());
        invocation.isolated_home = Some(PathBuf::from("temp-homes/worker-1"));
        let overrides = invocation.env_overrides();
        assert_eq!(overrides.len(), 3);
        assert!(overrides
            .iter()
            .any(|(key, value)| *key == "CODEX_HOME" && value.ends_with(".codex")));
    }

    // -- frozen real transcripts -------------------------------------------

    #[test]
    fn the_basic_fixture_maps_to_started_text_usage_and_finished() {
        let Some(transcript) = fixture("codex.P1-basic.stdout.jsonl") else {
            eprintln!("codex fixture corpus unavailable; skipping");
            return;
        };
        let (events, reducer) = reduce(&transcript);
        let kinds: Vec<&str> = events.iter().map(AdapterEvent::kind).collect();
        assert_eq!(kinds, vec!["started", "text_delta", "usage_update"]);
        match &events[0] {
            AdapterEvent::Started { session_id, .. } => {
                assert!(session_id.starts_with("01a0"), "thread id: {session_id}");
            }
            other => panic!("expected Started, got {other:?}"),
        }
        assert_eq!(events[1], AdapterEvent::TextDelta("PROBE_OK".to_owned()));
        match &events[2] {
            AdapterEvent::UsageUpdate(usage) => {
                assert_eq!(usage.input_tokens, 21_437);
                assert_eq!(usage.output_tokens, 7);
                assert_eq!(usage.cache_read_tokens, 11_008);
                assert_eq!(usage.total_tokens, 21_444, "cached is inside input");
                assert_eq!(usage.cost_usd, None, "codex reports tokens only");
                assert_eq!(usage.session_overhead_tokens, CODEX_SESSION_OVERHEAD_TOKENS);
            }
            other => panic!("expected UsageUpdate, got {other:?}"),
        }
        match reducer.finish(0) {
            AdapterEvent::Finished {
                exit_code,
                final_result,
                structured,
            } => {
                assert_eq!(exit_code, 0);
                assert_eq!(final_result.as_deref(), Some("PROBE_OK"));
                assert!(structured.is_none(), "no schema was requested");
            }
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[test]
    fn shell_work_becomes_one_tool_use_per_call() {
        let Some(transcript) = fixture("codex.P2-tools.stdout.jsonl") else {
            eprintln!("codex fixture corpus unavailable; skipping");
            return;
        };
        let (events, _) = reduce(&transcript);
        let tool_uses: Vec<&AdapterEvent> = events
            .iter()
            .filter(|event| matches!(event, AdapterEvent::ToolUse { .. }))
            .collect();
        assert_eq!(
            tool_uses.len(),
            2,
            "two commands ran; started/completed pairs must not double-count"
        );
        match tool_uses[1] {
            AdapterEvent::ToolUse { tool, args_summary } => {
                assert_eq!(tool, "command_execution");
                assert!(
                    args_summary.contains("PROBE_SHELL"),
                    "summary: {args_summary}"
                );
                assert!(args_summary.chars().count() <= TOOL_SUMMARY_MAX + 3);
            }
            other => panic!("expected ToolUse, got {other:?}"),
        }
        assert!(
            events
                .iter()
                .all(|event| !matches!(event, AdapterEvent::Decision { .. })),
            "a shell turn asks nothing"
        );
    }

    /// codex's `todo_list` is progress, not a gate: it maps to a ToolUse
    /// and must never produce buttons.
    #[test]
    fn the_plan_surface_is_progress_not_a_decision() {
        let Some(transcript) = fixture("codex.P3-plan.stdout.jsonl") else {
            eprintln!("codex fixture corpus unavailable; skipping");
            return;
        };
        let (events, _) = reduce(&transcript);
        let plans: Vec<&AdapterEvent> = events
            .iter()
            .filter(
                |event| matches!(event, AdapterEvent::ToolUse { tool, .. } if tool == "todo_list"),
            )
            .collect();
        assert_eq!(plans.len(), 1, "started + completed describe one plan");
        match plans[0] {
            AdapterEvent::ToolUse { args_summary, .. } => {
                assert_eq!(args_summary, "3 plan items");
            }
            other => panic!("expected ToolUse, got {other:?}"),
        }
        assert!(events
            .iter()
            .all(|event| !matches!(event, AdapterEvent::Decision { .. })));
    }

    /// The text protocol, end to end on a real turn: codex has no asking
    /// tool, so the fenced ask block in its answer becomes the Decision the
    /// desktop renders as buttons.
    #[test]
    fn a_fenced_ask_in_the_answer_becomes_a_decision() {
        let Some(transcript) = fixture("codex.P5-ask-protocol.stdout.jsonl") else {
            eprintln!("codex fixture corpus unavailable; skipping");
            return;
        };
        let (events, _) = reduce(&transcript);
        let decision = events
            .iter()
            .find(|event| matches!(event, AdapterEvent::Decision { .. }))
            .expect("the answer carried an ask block");
        match decision {
            AdapterEvent::Decision {
                prompt, options, ..
            } => {
                assert!(prompt.contains("database"), "prompt: {prompt}");
                assert_eq!(options, &["Postgres".to_owned(), "SQLite".to_owned()]);
            }
            other => panic!("expected Decision, got {other:?}"),
        }
    }

    /// A rejected model: exit 1, `error` + `turn.failed` carrying an
    /// embedded HTTP status. A turn terminal WAS seen, so this is a task
    /// failure, not a spawn failure — and the provider's words survive.
    #[test]
    fn a_rejected_model_is_a_task_failure_with_the_provider_message() {
        let Some(transcript) = fixture("codex.P9-bad-model.stdout.jsonl") else {
            eprintln!("codex fixture corpus unavailable; skipping");
            return;
        };
        let (_, reducer) = reduce(&transcript);
        match reducer.finish(1) {
            AdapterEvent::Failed(AdapterFailure::TaskFailure { detail }) => {
                assert!(detail.contains("exit code 1"), "detail: {detail}");
                assert!(detail.contains("not supported"), "provider words: {detail}");
            }
            other => panic!("expected TaskFailure, got {other:?}"),
        }
    }

    /// The bad-flag death (`--ask-for-approval`, exit 2): no turn event at
    /// all, so it classifies as a spawn failure — the F-00 §4 ladder, no
    /// stderr reading involved.
    #[test]
    fn an_unknown_flag_dies_before_any_turn() {
        let Some(transcript) = fixture("codex.P4-approval-flag.stdout.jsonl") else {
            eprintln!("codex fixture corpus unavailable; skipping");
            return;
        };
        let (events, reducer) = reduce(&transcript);
        assert!(events.is_empty(), "the CLI died before emitting anything");
        assert!(matches!(
            reducer.finish(2),
            AdapterEvent::Failed(AdapterFailure::SpawnFailure { .. })
        ));
    }

    #[test]
    fn structured_output_is_parsed_from_the_final_message() {
        let mut reducer = CodexStreamReducer::expecting_structured_output();
        for line in [
            json!({"type": "thread.started", "thread_id": "t-1"}).to_string(),
            json!({"type": "item.completed", "item": {"id": "i0", "type": "agent_message",
                "text": "{\"answer\":\"Paris\"}"}})
            .to_string(),
            json!({"type": "turn.completed", "usage": {"input_tokens": 10, "output_tokens": 2}})
                .to_string(),
        ] {
            reducer.push_line(&line);
        }
        match reducer.finish(0) {
            AdapterEvent::Finished { structured, .. } => {
                assert_eq!(structured.unwrap()["answer"], json!("Paris"));
            }
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[test]
    fn typed_http_codes_are_lifted_out_of_the_failure_payload() {
        assert_eq!(
            provider_code_from_error(r#"{"type":"error","status":429,"error":{"type":"rate"}}"#),
            Some(429)
        );
        assert_eq!(
            provider_code_from_error(r#"{"error":{"status":401,"message":"nope"}}"#),
            Some(401)
        );
        assert_eq!(provider_code_from_error("plain text failure"), None);

        // 429 → Transient (retryable) through the shared ladder.
        let mut reducer = CodexStreamReducer::default();
        reducer.push_line(
            &json!({"type": "turn.failed", "error": {"message":
                "{\"status\":429,\"error\":{\"type\":\"rate_limit\"}}"}})
            .to_string(),
        );
        match reducer.finish(1) {
            AdapterEvent::Failed(failure) => {
                assert!(failure.is_retryable(), "rate limits retry: {failure:?}");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn schema_drift_and_noise_never_kill_the_stream() {
        let mut reducer = CodexStreamReducer::default();
        assert!(reducer.push_line("not json at all").is_empty());
        assert!(reducer.push_line("").is_empty());
        assert!(reducer
            .push_line(&json!({"type": "future_event_v9", "payload": {}}).to_string())
            .is_empty());
        assert!(reducer
            .push_line(
                &json!({"type": "item.completed", "item": {"type": "error",
                "message": "clamping SessionEnd hook timeout"}})
                .to_string()
            )
            .is_empty());
        // Still usable afterwards.
        assert_eq!(
            reducer
                .push_line(&json!({"type": "thread.started", "thread_id": "t-9"}).to_string())
                .len(),
            1
        );
    }

    #[test]
    fn capabilities_claim_only_what_was_observed() {
        let capabilities = codex_capabilities();
        assert!(capabilities.filesystem_edit && capabilities.shell);
        assert!(capabilities.structured_output && capabilities.resume);
        assert!(
            !capabilities.network,
            "no web tool was observed in any exec run"
        );
    }

    // -- live probes (free only, opt-in) -----------------------------------

    /// The only billable test in this file: one live session plus one
    /// resumed turn, proving the whole chain — spawn, stdin prompt
    /// delivery, stream mapping, thread capture, `send_instruction` as a
    /// resume run, and the terminal event. Opt-in and ignored by default.
    #[tokio::test]
    #[ignore = "live BILLABLE session; set AGENTOS_CODEX_E2E=1 to run"]
    async fn e2e_session_streams_and_resumes() {
        if std::env::var(CODEX_E2E_ENV).is_err() {
            eprintln!("{CODEX_E2E_ENV} unset; skipping");
            return;
        }
        let workspace = std::env::temp_dir().join("agentos-codex-e2e");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let adapter = CodexAdapter::new();
        let mut spec = spec(
            "Remember the token PLUM7. Reply with exactly: STORED",
            vec![],
            300,
        );
        spec.workspace = workspace;

        let handle = adapter.start_session(spec).await.expect("session starts");
        let mut events = handle.events();

        let mut answers = Vec::new();
        let mut thread_started = None;
        loop {
            match events.recv().await.expect("stream stays open") {
                AdapterEvent::Started { session_id, .. } => thread_started = Some(session_id),
                AdapterEvent::TextDelta(text) => answers.push(text),
                AdapterEvent::Finished { exit_code, .. } => {
                    assert_eq!(exit_code, 0, "first turn succeeds");
                    break;
                }
                AdapterEvent::Failed(failure) => panic!("first turn failed: {failure:?}"),
                _ => {}
            }
        }
        assert!(thread_started.is_some(), "thread.started was mapped");
        assert!(
            answers.iter().any(|text| text.contains("STORED")),
            "answers: {answers:?}"
        );

        // The resume path: a follow-up instruction must see the context.
        handle
            .send_instruction(
                "What token did I ask you to remember? Reply with just the token.".to_owned(),
            )
            .await
            .expect("instruction accepted");
        let mut recalled = Vec::new();
        loop {
            match events.recv().await.expect("stream stays open") {
                AdapterEvent::TextDelta(text) => recalled.push(text),
                AdapterEvent::Finished { exit_code, .. } => {
                    assert_eq!(exit_code, 0, "resumed turn succeeds");
                    break;
                }
                AdapterEvent::Failed(failure) => panic!("resumed turn failed: {failure:?}"),
                _ => {}
            }
        }
        assert!(
            recalled.iter().any(|text| text.contains("PLUM7")),
            "resume must carry context; got {recalled:?}"
        );
        adapter.shutdown().await.expect("shutdown is idempotent");
    }

    #[tokio::test]
    #[ignore = "live probe; set AGENTOS_CODEX_E2E=1 to run (free: --version only)"]
    async fn e2e_detect_reports_a_version() {
        if std::env::var(CODEX_E2E_ENV).is_err() {
            eprintln!("{CODEX_E2E_ENV} unset; skipping");
            return;
        }
        let info = CodexAdapter::new().detect().await;
        assert_eq!(info.id, CODEX_ADAPTER_ID);
        let version = info.version.expect("codex --version");
        assert!(version.contains("codex"), "version line: {version}");
    }
}
