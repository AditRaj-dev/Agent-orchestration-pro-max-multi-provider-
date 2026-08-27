//! Antigravity (`agy`) adapter — F-05.
//!
//! Drives the Antigravity CLI (observed v1.1.18, handoff addendum "PIVOT")
//! through the F-02 [`RuntimeAdapter`] contract. Canon sources (binding):
//! `F-00-CONVENTIONS.md` §4 agy rows, `handoff.md` ADDENDUM "Antigravity
//! CLI v1.1.18" (all findings observed on this machine, 2026-08-22).
//!
//! Observed contract this adapter encodes:
//!
//! - `--print` is **variadic** → prompt is delivered as one argv element in
//!   equals form, `--print='<objective>'` (the battery's verified form).
//! - `--output-format json` → one result object; `stream-json` → NDJSON
//!   events `init` → `step_update` → `result` (result payload identical to
//!   the json-mode object).
//! - `status: ERROR` → exit code 2, usage **still present**.
//! - Writes in print mode are **virtualized** to
//!   `~/.gemini/antigravity-cli/brain/<conversation_id>/`; the cwd is not
//!   writable by default. Real-directory writes require `--add-dir <dir>`
//!   (repeatable) — so the adapter always grants the workspace.
//! - Resume via `--conversation <id>`; structured output via
//!   `--json-schema <file>`; coarse policy via `--mode plan|accept-edits`.
//!   There are **no per-tool allow/deny flags** (gap vs Claude) — harness-
//!   side gating compensates (F-10).
//! - ~37k fixed input tokens per session (core agent prompt, not user
//!   skills — the `--disable-slash-commands` A/B moved it 37133→37136).
//!
//! Classification follows F-00 §4: exit code + presence of the final
//! result event, never stderr text. stderr is drained for debug logging
//! only.
//!
//! All tests are offline: parsers and the arg builder are pure and run
//! against **synthetic fixtures** built from the documented observed schema
//! (no frozen agy transcript corpus exists on disk). The only live
//! invocations anywhere are free probes (`--version`, `agy models`), and
//! those live in `#[ignore]`d tests gated behind `AGENTOS_AGY_E2E=1`.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::sync::broadcast;

use crate::adapter::{RuntimeAdapter, SessionBackend, SessionHandle};
use crate::error::{AdapterError, AdapterFailure, Classifier};
use crate::events::AdapterEvent;
use crate::types::{AuthStatus, Capabilities, RuntimeInfo, SpawnSpec, UsageSnapshot};

/// Stable adapter identifier (F-02 `RuntimeAdapter::id`).
pub const AGY_ADAPTER_ID: &str = "antigravity-agy";
/// Fixed per-session preamble the agy core agent prompt bills (F-00 §4:
/// ~37k input tokens observed; core prompt, not user skills — the
/// `--disable-slash-commands` A/B changed 37133 → 37136 only).
pub const AGY_SESSION_OVERHEAD_TOKENS: u64 = 37_000;
/// Slack added to the CLI-side `--print-timeout` so the *harness* watchdog
/// always fires first and owns the classification: a harness-side timeout
/// maps to [`AdapterFailure::Transient`] (retryable), while letting the
/// CLI die first would classify as an un-final-evented exit.
pub const AGY_PRINT_TIMEOUT_SLACK_SECS: u64 = 30;
/// Cap for compact tool summaries (F-00 §3: never full payloads).
const AGY_TOOL_SUMMARY_MAX: usize = 160;
/// Env var that opts the ignored e2e tests into live (free-only) agy probes.
pub const AGY_E2E_ENV: &str = "AGENTOS_AGY_E2E";
/// Env var overriding the agy binary location (same pattern as the probe
/// scripts' `PROBE_*_BIN`).
pub const AGY_BIN_ENV: &str = "AGENTOS_AGY_BIN";

/// Tool names that mean "this turn must not run shell commands". agy cannot
/// deny them individually, so their presence selects `--sandbox` instead.
const SHELL_TOOLS: [&str; 6] = [
    "Bash",
    "BashOutput",
    "KillShell",
    "PowerShell",
    "Tmux",
    "REPL",
];

/// Coarse agy permission mode. There is no per-tool allow/deny surface on
/// agy (observed gap vs Claude `--allowedTools`); `plan` is the verified
/// read-only enforcement, `accept-edits` permits edits (and denies shell
/// headless with an observable "user denied permission" error).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AgyMode {
    /// `--mode plan` — VERIFIED read-only (write prompt → plan artifact,
    /// no file written, SUCCESS status).
    Plan,
    /// `--mode accept-edits` — file edits allowed (in `--add-dir` dirs).
    /// The old "shell denied headless" behaviour held only at
    /// `permission_mode=request-review`, which also blocked every file write.
    /// Under `always-proceed` the terminal runs: verified by a shell-only
    /// command creating a file on disk. `--sandbox` is what blocks it now.
    AcceptEdits,
}

impl AgyMode {
    /// Flag value as the CLI expects it.
    pub fn as_str(self) -> &'static str {
        match self {
            AgyMode::Plan => "plan",
            AgyMode::AcceptEdits => "accept-edits",
        }
    }
}

/// Reasoning-effort passthrough (`--effort low|medium|high`, observed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AgyEffort {
    /// `--effort low`.
    Low,
    /// `--effort medium`.
    Medium,
    /// `--effort high`.
    High,
}

impl AgyEffort {
    /// Flag value as the CLI expects it.
    pub fn as_str(self) -> &'static str {
        match self {
            AgyEffort::Low => "low",
            AgyEffort::Medium => "medium",
            AgyEffort::High => "high",
        }
    }
}

/// Output mode for a print run. `stream-json` is preferred (init event
/// carries conversation id + model early); `json` is the single-object
/// fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AgyOutputFormat {
    /// `--output-format stream-json` — NDJSON `init`/`step_update`/`result`.
    StreamJson,
    /// `--output-format json` — one final result object.
    Json,
}

impl AgyOutputFormat {
    /// Flag value as the CLI expects it.
    pub fn as_str(self) -> &'static str {
        match self {
            AgyOutputFormat::StreamJson => "stream-json",
            AgyOutputFormat::Json => "json",
        }
    }
}

/// One fully-resolved agy print-run invocation (pure, unit-testable).
///
/// Built from a [`SpawnSpec`] via [`AgyInvocation::from_spec`] plus
/// passthrough hooks the spec does not carry yet (effort, structured-output
/// schema file, resume id) — an explicit documented gap: F-02's `SpawnSpec`
/// has no fields for them, so callers chain the `with_*` builders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgyInvocation {
    /// Block terminal execution while still permitting file edits.
    ///
    /// agy has no per-tool allow/deny surface, so the daemon's shell denylist
    /// for scoped authoring turns cannot be expressed directly. `--sandbox` is
    /// the coarse equivalent: verified to stop a shell-only command from
    /// creating a file while `write_to_file` still succeeds. Note the failure
    /// is silent — the model reports the command as run — so enable this only
    /// where shell genuinely must not fire.
    pub sandbox: bool,

    /// The prompt. Rendered as the LAST argument in equals form
    /// `--print=<objective>` (variadic-flag canon, F-00 §4).
    pub objective: String,
    /// Working directory / primary access grant. ALWAYS passed via
    /// `--add-dir` (print-mode writes are virtualized to the brain dir
    /// without it) and used as the child process cwd.
    pub workspace: PathBuf,
    /// Extra access grants (from `SpawnSpec::allowed_paths`), each as its
    /// own repeatable `--add-dir`. Entries equal to `workspace` are
    /// deduplicated.
    pub extra_dirs: Vec<PathBuf>,
    /// Coarse permission mode; see [`AgyMode`].
    pub mode: AgyMode,
    /// Output mode; [`AgyOutputFormat::StreamJson`] by default.
    pub output_format: AgyOutputFormat,
    /// `--model` passthrough when set.
    pub model: Option<String>,
    /// `--effort` passthrough hook when set.
    pub effort: Option<AgyEffort>,
    /// `--json-schema <file>` when structured output is requested. agy
    /// accepts inline JSON or a file path; the adapter always uses a file
    /// (schemas exceed comfortable argv lengths).
    pub json_schema: Option<PathBuf>,
    /// `--conversation <id>` resume target when set.
    pub conversation: Option<String>,
    /// Harness wall-clock budget. `0` disables both the harness watchdog
    /// and the `--print-timeout` flag (CLI default 5m then applies).
    /// When non-zero the CLI receives this + [`AGY_PRINT_TIMEOUT_SLACK_SECS`]
    /// so the harness watchdog classifies the timeout as `Transient` first.
    pub print_timeout_secs: u64,
}

impl AgyInvocation {
    /// Derive an invocation from a spawn spec.
    ///
    /// Mode mapping (documented F-05 decision): `SpawnSpec` has no explicit
    /// write-intent field, so F-05 derives it from `allowed_paths`
    /// emptiness — an empty list means the harness declared no additional
    /// access targets, i.e. a read-only analysis run → `--mode plan`
    /// (verified read-only enforcement); a non-empty list declares write
    /// targets → `--mode accept-edits`. The workspace itself is always
    /// granted via `--add-dir` either way: the *mode* is the enforcement,
    /// `--add-dir` is only the access grant, and plan-mode reads of the
    /// workspace are unaffected.
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
            mode: if spec.allowed_paths.is_empty() {
                AgyMode::Plan
            } else {
                AgyMode::AcceptEdits
            },
            sandbox: spec
                .tool_denylist
                .iter()
                .any(|tool| SHELL_TOOLS.contains(&tool.as_str())),
            output_format: AgyOutputFormat::StreamJson,
            model: spec.model.clone(),
            effort: None,
            json_schema: None,
            conversation: None,
            print_timeout_secs: spec.timeout_secs,
        }
    }

    /// Override the model.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Set the `--effort` passthrough.
    pub fn with_effort(mut self, effort: AgyEffort) -> Self {
        self.effort = Some(effort);
        self
    }

    /// Request structured output via a `--json-schema` file.
    pub fn with_json_schema(mut self, schema_file: impl Into<PathBuf>) -> Self {
        self.json_schema = Some(schema_file.into());
        self
    }

    /// Resume the given conversation (`--conversation <id>`).
    pub fn with_conversation(mut self, conversation_id: impl Into<String>) -> Self {
        self.conversation = Some(conversation_id.into());
        self
    }

    /// Render the argv vector (canonical order, stable for tests).
    ///
    /// The prompt is the final element, in equals form — the variadic-safe
    /// delivery the battery verified.
    pub fn args(&self) -> Vec<String> {
        let mut args = vec![
            "--mode".to_owned(),
            self.mode.as_str().to_owned(),
            "--output-format".to_owned(),
            self.output_format.as_str().to_owned(),
        ];
        if self.sandbox {
            args.push("--sandbox".to_owned());
        }
        if self.mode == AgyMode::AcceptEdits {
            // Without this the session runs at `permission_mode=request-review`
            // and every tool call waits for an approval no one can give in
            // print mode: the run still exits SUCCESS, with an empty response
            // and no file written. A scoped authoring turn is silently a no-op.
            // Autonomy is bounded by `--mode` and the `--add-dir` grants, the
            // same way the codex adapter bounds it with `--sandbox`.
            args.push("--dangerously-skip-permissions".to_owned());
        }
        if let Some(model) = &self.model {
            args.push("--model".to_owned());
            args.push(model.clone());
        }
        if let Some(effort) = self.effort {
            args.push("--effort".to_owned());
            args.push(effort.as_str().to_owned());
        }
        if let Some(schema) = &self.json_schema {
            args.push("--json-schema".to_owned());
            args.push(schema.to_string_lossy().into_owned());
        }
        if let Some(conversation) = &self.conversation {
            args.push("--conversation".to_owned());
            args.push(conversation.clone());
        }
        // ALWAYS: without --add-dir the CLI virtualizes writes into the
        // brain dir and the real workspace never changes (observed).
        args.push("--add-dir".to_owned());
        args.push(self.workspace.to_string_lossy().into_owned());
        for dir in &self.extra_dirs {
            args.push("--add-dir".to_owned());
            args.push(dir.to_string_lossy().into_owned());
        }
        if self.print_timeout_secs > 0 {
            args.push("--print-timeout".to_owned());
            // The CLI parses this with Go's time.ParseDuration, which
            // requires a unit — a bare integer is "missing unit in
            // duration" and exits 2 before emitting any event.
            args.push(format!(
                "{}s",
                self.print_timeout_secs + AGY_PRINT_TIMEOUT_SLACK_SECS
            ));
        }
        match self.output_format {
            // The prompt is NDJSON on stdin. Delivering it in argv caps it at
            // the OS command-line limit — a large phase envelope failed to
            // spawn with "The filename or extension is too long" (os error
            // 206) before it ever reached the model.
            AgyOutputFormat::StreamJson => {
                args.push("--input-format".to_owned());
                args.push("stream-json".to_owned());
            }
            AgyOutputFormat::Json => args.push(format!("--print={}", self.objective)),
        }
        args
    }

    /// The single NDJSON frame delivering the prompt, for runs that read stdin.
    /// `None` means the prompt rides in argv instead (`--output-format json`).
    ///
    /// agy keys stream frames on `event`, not `type`, and `message` must be an
    /// object — a bare string is rejected as `cannot unmarshal string into Go
    /// struct field streamInputMessage.message`.
    pub fn stdin_frame(&self) -> Option<String> {
        match self.output_format {
            AgyOutputFormat::Json => None,
            AgyOutputFormat::StreamJson => Some(format!(
                "{}
",
                serde_json::json!({
                    "event": "user",
                    "message": {
                        "role": "user",
                        "content": [{ "type": "text", "text": self.objective }],
                    },
                })
            )),
        }
    }
}

/// `usage` object of the agy result contract (all fields optional for
/// tolerant parsing; agy reports tokens only — no cost field exists).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgyUsage {
    /// Prompt tokens.
    pub input_tokens: Option<u64>,
    /// Completion tokens.
    pub output_tokens: Option<u64>,
    /// Reasoning tokens (agy splits these out).
    pub thinking_tokens: Option<u64>,
    /// Conversation-cache hit tokens (32k–97k observed on resumed turns).
    pub cache_read_tokens: Option<u64>,
    /// Provider-reported total; falls back to the field sum when absent.
    pub total_tokens: Option<u64>,
}

impl AgyUsage {
    /// Map onto the F-02 ledger snapshot.
    ///
    /// `cost_usd` is `None` (no cost field exists in the observed agy
    /// contract — OBS-04: label estimates only where they exist) and
    /// `per_model` is empty: a print run bills exactly one model and agy
    /// exposes no per-model breakdown. `session_overhead_tokens` pins the
    /// observed ~37k core-prompt overhead (F-00 §4).
    pub fn to_snapshot(&self) -> UsageSnapshot {
        let input = self.input_tokens.unwrap_or(0);
        let output = self.output_tokens.unwrap_or(0);
        let thinking = self.thinking_tokens.unwrap_or(0);
        let cache_read = self.cache_read_tokens.unwrap_or(0);
        let total = self
            .total_tokens
            .unwrap_or(input + output + thinking + cache_read);
        UsageSnapshot {
            input_tokens: input,
            output_tokens: output,
            thinking_tokens: thinking,
            cache_read_tokens: cache_read,
            total_tokens: total,
            cost_usd: None,
            per_model: Vec::new(),
            session_overhead_tokens: AGY_SESSION_OVERHEAD_TOKENS,
        }
    }
}

/// The agy result contract — payload of `--output-format json` output and
/// of the `result` stream event. Fields the adapter does not consume
/// (`duration_seconds`, `num_turns`, `json_schema`) are tolerated and
/// ignored rather than pinned.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgyResult {
    /// Conversation id (the resume handle).
    pub conversation_id: Option<String>,
    /// `SUCCESS` | `ERROR` (observed values).
    pub status: Option<String>,
    /// Final response text on success.
    pub response: Option<String>,
    /// Structured error text on `ERROR` (machine-parsed content — the
    /// denial detector and the typed-code hook read this, never stderr).
    pub error: Option<String>,
    /// Token usage; present even on `ERROR` (exit 2) runs.
    pub usage: Option<AgyUsage>,
    /// Schema-constrained output when `--json-schema` was supplied.
    pub structured_output: Option<Value>,
}

impl AgyResult {
    /// Whether the payload itself claims success (`status == SUCCESS`,
    /// case-insensitive-tolerant).
    pub fn status_is_success(&self) -> bool {
        self.status
            .as_deref()
            .is_some_and(|status| status.eq_ignore_ascii_case("SUCCESS"))
    }

    /// Whether the payload explicitly claims failure. Missing or unknown
    /// status values are not failures by themselves: the provider schema is
    /// drift-tolerant, and the process exit plus final result remain the
    /// canonical success signals.
    fn explicitly_failed(&self) -> bool {
        self.status
            .as_deref()
            .is_some_and(|status| status.eq_ignore_ascii_case("ERROR"))
            || self
                .error
                .as_deref()
                .is_some_and(|error| !error.trim().is_empty())
    }
}

/// `init` stream event (conversation_id + model; the 58-tool inventory and
/// `permission_mode` are tolerated and skipped — not consumed by F-02
/// events yet).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgyInit {
    /// Conversation id.
    pub conversation_id: Option<String>,
    /// Model the conversation runs on.
    pub model: Option<String>,
}

/// `step_update` payload (the object nested under the `step_update` key of
/// the envelope — see [`AgyStreamReducer::push_line`]). Observed
/// `step_type` values: `user_input`, `checkpoint`, `agent_response`,
/// `tool`; `state` runs `ACTIVE` → `DONE` | `ERROR`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AgyStepUpdate {
    step_type: Option<String>,
    /// `ACTIVE` | `DONE` | `ERROR` on the live CLI. Typed as a raw value
    /// because older/synthetic transcripts carry an object here — a state
    /// we cannot read must not cost us the whole step.
    state: Option<Value>,
    text_delta: Option<String>,
    usage: Option<AgyUsage>,
    /// Tool identity on `step_type: "tool"` steps.
    tool_name: Option<String>,
    /// Free-form tool detail; summarized, never forwarded whole (F-00 §3).
    tool_info: Option<Value>,
}

impl AgyStepUpdate {
    /// Whether this tool step is the opening edge of a call. Tool steps
    /// run ACTIVE -> DONE|ERROR, so only the opening edge emits and one
    /// call stays one `ToolUse`.
    fn is_tool_call_start(&self) -> bool {
        !matches!(
            self.state.as_ref().and_then(Value::as_str),
            Some("DONE") | Some("ERROR")
        )
    }

    /// Compact `ToolUse` summary from `tool_info`: the first few scalar
    /// fields, truncated. Never the whole payload.
    fn tool_summary(&self) -> String {
        let Some(Value::Object(info)) = &self.tool_info else {
            return self
                .tool_info
                .as_ref()
                .map(|value| truncate(&value.to_string(), AGY_TOOL_SUMMARY_MAX))
                .unwrap_or_else(|| "(no arguments)".to_owned());
        };
        if info.is_empty() {
            return "(no arguments)".to_owned();
        }
        let parts: Vec<String> = info
            .iter()
            .take(3)
            .map(|(key, value)| {
                let text = match value {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                };
                format!("{key}={}", truncate(&text, 60))
            })
            .collect();
        truncate(&parts.join(", "), AGY_TOOL_SUMMARY_MAX)
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_owned()
    } else {
        let cut: String = text.chars().take(max).collect();
        format!("{cut}...")
    }
}

/// Hook for future typed provider codes. agy v1.1.18 surfaced no typed
/// provider business codes in its `error` field (unlike zcode's
/// `ProviderBusinessError [1113]/[3007]`); when one appears, map its
/// textual signature to a code here and the [`Classifier`] ladder turns it
/// into Billing/BotGate/Auth/Transient automatically.
fn provider_code_from_error(_error: &str) -> Option<i64> {
    None
}

/// Whether the structured `error` field reports account quota exhaustion.
///
/// Observed verbatim (2026-08-22, gemini-3.1-pro-high): `"Individual quota
/// reached. Please upgrade your subscription to increase your limits.
/// Resets in 146h41m26s."` — with `status: ERROR` and **exit code 0**.
/// This is a time-boxed capacity limit, i.e. exactly `Transient`: retrying
/// after the stated window is the correct behavior, and classifying it as
/// a task failure would make the supervisor blame the task.
///
/// Machine-parsed payload content, never stderr (F-00 §4).
fn quota_exhausted(error: Option<&str>) -> bool {
    error.is_some_and(|text| {
        let lowered = text.to_ascii_lowercase();
        lowered.contains("quota reached") || lowered.contains("quota exceeded")
    })
}

/// Whether agy says the provider stream ended prematurely and explicitly
/// invites continuation. This is infrastructure loss, not a judgement on
/// the task, so the supervisor may safely retry it.
fn stream_interrupted(error: Option<&str>) -> bool {
    error.is_some_and(|text| {
        text.to_ascii_lowercase()
            .contains("the stream was interrupted")
    })
}

/// Whether the structured `error` field is the observed agy headless
/// denial ("user denied permission" — accept-edits denying shell). This is
/// machine-parsed payload content, NOT stderr scraping; the F-02
/// Classifier treats it as the observable denial event.
fn denial_observed(error: Option<&str>) -> bool {
    error.is_some_and(|text| text.contains("user denied permission"))
}

/// Terminal event for a finished print run from the two machine-reliable
/// signals (exit code + result payload) plus payload-content hooks.
///
/// The payload's own explicit failure signals are authoritative: `ERROR` or
/// a non-empty structured error is never success regardless of exit code.
/// Otherwise the F-00 §4 conjunction decides (exit 0 + final result). This
/// matters because live agy results sometimes omit or drift the optional
/// `status` field while still returning a complete response and exit 0.
fn terminal_event(exit_code: i32, result: Option<&AgyResult>) -> AdapterEvent {
    match result {
        // A quota window outranks the generic ladder: retryable, with the
        // provider's own reset window preserved for the backoff policy.
        Some(result) if quota_exhausted(result.error.as_deref()) => {
            AdapterEvent::Failed(AdapterFailure::Transient {
                detail: format!(
                    "agy account quota exhausted: {}",
                    result.error.as_deref().unwrap_or("quota reached")
                ),
            })
        }
        Some(result) if stream_interrupted(result.error.as_deref()) => {
            AdapterEvent::Failed(AdapterFailure::Transient {
                detail: format!(
                    "agy provider stream interrupted: {}",
                    result
                        .error
                        .as_deref()
                        .unwrap_or("the stream was interrupted")
                ),
            })
        }
        Some(result) if Classifier::is_success(exit_code, true) && !result.explicitly_failed() => {
            AdapterEvent::Finished {
                exit_code,
                final_result: result.response.clone(),
                structured: result.structured_output.clone(),
            }
        }
        // Do not feed explicit provider failure evidence into Classifier's
        // defensive "these inputs succeeded" branch. That branch is correct
        // for the generic two-signal contract; agy's extra payload signal is
        // adapter-specific.
        Some(result) if exit_code == 0 && result.explicitly_failed() => {
            AdapterEvent::Failed(AdapterFailure::TaskFailure {
                detail: format!(
                    "agy result explicitly reported failure despite exit code 0: {}",
                    result
                        .error
                        .as_deref()
                        .filter(|error| !error.trim().is_empty())
                        .unwrap_or("status ERROR")
                ),
            })
        }
        _ => AdapterEvent::Failed(Classifier::classify(
            exit_code,
            result.is_some(),
            result
                .and_then(|r| r.error.as_deref())
                .and_then(provider_code_from_error),
            result.is_some_and(|r| denial_observed(r.error.as_deref())),
        )),
    }
}

/// Incremental stream-json parser (pure). Feed it lines as they arrive;
/// it maps each to zero or more [`AdapterEvent`]s and remembers the
/// conversation id and the final `result` payload for the terminal
/// decision once the exit code is known.
#[derive(Debug, Default)]
pub struct AgyStreamReducer {
    init_conversation_id: Option<String>,
    result: Option<AgyResult>,
}

impl AgyStreamReducer {
    /// Ingest one stdout line. Unknown event kinds, blank lines, and
    /// non-JSON noise are skipped (tolerant parsing, F-00 §4.2: provider
    /// schemas drift) — never an error.
    pub fn push_line(&mut self, line: &str) -> Vec<AdapterEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            tracing::debug!(line = %trimmed, "agy adapter: skipping non-JSON stream line");
            return Vec::new();
        };
        // The envelope field name is not pinned by the observed canon;
        // accept `type` (fixtures, claude/codex convention) and `event`.
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .or_else(|| value.get("event").and_then(Value::as_str));
        // The live CLI NESTS each payload under a key equal to its kind
        // (`{"event":"step_update","step_update":{...}}`), with
        // `conversation_id` hoisted to the envelope on `init`. Flat
        // envelopes are still accepted so older/synthetic shapes keep
        // parsing — hence `payload()`, which prefers the nested object.
        let payload = |kind: &str| -> Value {
            match value.get(kind) {
                Some(Value::Object(inner)) => {
                    let mut merged = inner.clone();
                    // Envelope-level fields the payload does not repeat.
                    for hoisted in ["conversation_id", "model"] {
                        if !merged.contains_key(hoisted) {
                            if let Some(field) = value.get(hoisted) {
                                merged.insert(hoisted.to_owned(), field.clone());
                            }
                        }
                    }
                    Value::Object(merged)
                }
                _ => value.clone(),
            }
        };
        match kind {
            Some("init") => match parse_as::<AgyInit>(&payload("init")) {
                Some(init) => {
                    let event = AdapterEvent::Started {
                        session_id: init
                            .conversation_id
                            .clone()
                            .unwrap_or_else(|| "unknown".to_owned()),
                        model: init.model,
                    };
                    if let Some(id) = init.conversation_id {
                        self.init_conversation_id = Some(id);
                    }
                    vec![event]
                }
                None => Vec::new(),
            },
            Some("step_update") => match parse_as::<AgyStepUpdate>(&payload("step_update")) {
                Some(step) => {
                    let mut events = Vec::new();
                    // Tool steps run ACTIVE -> DONE|ERROR; only the ACTIVE
                    // edge emits, so one call is one ToolUse.
                    if step.step_type.as_deref() == Some("tool") && step.is_tool_call_start() {
                        events.push(AdapterEvent::ToolUse {
                            tool: step
                                .tool_name
                                .clone()
                                .unwrap_or_else(|| "unknown".to_owned()),
                            args_summary: step.tool_summary(),
                        });
                    }
                    if let Some(delta) = &step.text_delta {
                        if !delta.is_empty() {
                            events.push(AdapterEvent::TextDelta(delta.clone()));
                        }
                    }
                    if let Some(usage) = step.usage {
                        events.push(AdapterEvent::UsageUpdate(usage.to_snapshot()));
                    }
                    events
                }
                None => Vec::new(),
            },
            Some("result") => match parse_as::<AgyResult>(&payload("result")) {
                Some(result) => {
                    let mut events = Vec::new();
                    if let Some(usage) = &result.usage {
                        events.push(AdapterEvent::UsageUpdate(usage.to_snapshot()));
                    }
                    // agy has no asking tool: a plan-mode question arrives
                    // as a fenced block in the answer text (F-02 Decision,
                    // taught by the `decision-protocol` skill).
                    if let Some(response) = result.response.as_deref() {
                        events.extend(crate::decision::from_text(response));
                    }
                    self.result = Some(result);
                    events
                }
                None => Vec::new(),
            },
            other => {
                tracing::debug!(kind = ?other, "agy adapter: skipping unknown stream event");
                Vec::new()
            }
        }
    }

    /// The conversation id learned so far (`init` first, else `result`).
    pub fn conversation_id(&self) -> Option<&str> {
        self.init_conversation_id.as_deref().or_else(|| {
            self.result
                .as_ref()
                .and_then(|r| r.conversation_id.as_deref())
        })
    }

    /// The final result payload, when a `result` event was seen.
    pub fn result(&self) -> Option<&AgyResult> {
        self.result.as_ref()
    }

    /// Terminal event once the process exit code is known.
    pub fn finish(&self, exit_code: i32) -> AdapterEvent {
        terminal_event(exit_code, self.result.as_ref())
    }

    /// Build a reducer from json-mode stdout (one result object, possibly
    /// surrounded by whitespace or non-JSON noise). No result → the run
    /// classifies as a pre-model death.
    pub fn from_json_output(text: &str) -> Self {
        Self {
            result: parse_json_result(text),
            ..Self::default()
        }
    }
}

/// Locate the result object in json-mode stdout, tolerating surrounding
/// noise: (1) the whole trimmed text, (2) the first line that parses as a
/// result object (single-line output), (3) the substring from the first
/// `{` to the last `}` (pretty-printed object inside noise).
fn parse_json_result(text: &str) -> Option<AgyResult> {
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        // Nested envelope first (live shape), then flat.
        if let Some(inner) = value.get("result").filter(|inner| looks_like_result(inner)) {
            return serde_json::from_value(inner.clone()).ok();
        }
        if looks_like_result(&value) {
            return serde_json::from_value(value).ok();
        }
    }
    if let Some(line_hit) = trimmed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(looks_like_result)
    {
        return serde_json::from_value(line_hit).ok();
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if end <= start {
        return None;
    }
    let slice = &trimmed[start..=end];
    serde_json::from_str::<Value>(slice)
        .ok()
        .filter(looks_like_result)
        .and_then(|value| serde_json::from_value(value).ok())
}

fn looks_like_result(value: &Value) -> bool {
    value.get("status").is_some() || value.get("conversation_id").is_some()
}

fn parse_as<T: serde::de::DeserializeOwned>(value: &Value) -> Option<T> {
    serde_json::from_value(value.clone()).ok()
}

/// Events for a completed json-mode run. Json mode has no `init` event, so
/// the adapter synthesizes a late `Started` from the result payload
/// (before the terminal event) so downstream consumers still learn the
/// conversation id.
fn json_run_events(result: Option<&AgyResult>, exit_code: i32) -> Vec<AdapterEvent> {
    let Some(result) = result else {
        return vec![AdapterEvent::Failed(Classifier::classify(
            exit_code, false, None, false,
        ))];
    };
    let mut events = vec![AdapterEvent::Started {
        session_id: result
            .conversation_id
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
        model: None,
    }];
    if let Some(usage) = &result.usage {
        events.push(AdapterEvent::UsageUpdate(usage.to_snapshot()));
    }
    // Same text protocol as the streaming path (agy has no asking tool).
    if let Some(response) = result.response.as_deref() {
        events.extend(crate::decision::from_text(response));
    }
    events.push(terminal_event(exit_code, Some(result)));
    events
}

/// Harness-side timeout classification (deliverable: timeout → Transient,
/// the only retryable class).
fn harness_timeout_failure(timeout_secs: u64) -> AdapterFailure {
    AdapterFailure::Transient {
        detail: format!(
            "agy print run exceeded the {timeout_secs}s harness budget; process killed"
        ),
    }
}

// ---------------------------------------------------------------------------
// Live session plumbing
// ---------------------------------------------------------------------------

/// Shared mutable state of one agy conversation session.
struct AgySessionState {
    /// Adapter-internal handle id (`agy-<n>`); the *provider* conversation
    /// id is learned only at runtime (init/result) and is kept separately.
    handle_id: String,
    /// Resolved agy binary (recorded at session start).
    binary: PathBuf,
    /// The invocation template of the first run; follow-up instructions
    /// clone it and swap objective + `--conversation`.
    base_invocation: AgyInvocation,
    /// Provider conversation id once learned (enables resume).
    conversation_id: Mutex<Option<String>>,
    /// The live child, parked here so `cancel` can tree-kill it.
    child: tokio::sync::Mutex<Option<Child>>,
    /// Cancel requested — stream goes quiet, no terminal event
    /// (process-kill semantics).
    cancelled: AtomicBool,
    /// Session permanently over (a `Failed` terminal or cancel).
    dead: AtomicBool,
    /// A print run is in flight (one at a time; instructions are accepted
    /// between runs).
    run_active: AtomicBool,
}

/// [`SessionBackend`] for one agy conversation.
struct AgySessionBackend {
    state: std::sync::Arc<AgySessionState>,
    events: broadcast::Sender<AdapterEvent>,
}

#[async_trait]
impl SessionBackend for AgySessionBackend {
    /// Deliver one instruction to the conversation.
    ///
    /// Semantics (documented F-05 deviation): agy print runs are one-shot,
    /// so an instruction is delivered as a **new print run resuming the
    /// conversation** (`--conversation <id>`, verified resume path). The
    /// run's events — including its own terminal event — stream on the
    /// same session channel after the previous run's terminal event.
    /// This deliberately extends the F-02 "terminal = end of stream"
    /// convention: for agy, `Finished` means *turn complete*, and the
    /// session stays instructable until it `Failed` or is cancelled.
    async fn send_instruction(&self, text: String) -> Result<(), AdapterError> {
        let state = &self.state;
        if state.cancelled.load(Ordering::Relaxed) || state.dead.load(Ordering::Relaxed) {
            return Err(AdapterError::SessionNotActive(state.handle_id.clone()));
        }
        if state.run_active.swap(true, Ordering::Relaxed) {
            // The session is alive, just mid-turn: the caller must wait for
            // the terminal event, never open a second session.
            return Err(AdapterError::Busy(state.handle_id.clone()));
        }
        let conversation = state
            .conversation_id
            .lock()
            .expect("agy conversation state poisoned")
            .clone();
        let Some(conversation) = conversation else {
            state.run_active.store(false, Ordering::Relaxed);
            return Err(AdapterError::Internal(
                "no agy conversation id known yet; wait for the first run's \
                 init/result event"
                    .to_owned(),
            ));
        };
        let mut invocation = state.base_invocation.clone();
        invocation.objective = text;
        invocation.conversation = Some(conversation);
        match spawn_agy_run(&state.binary, &invocation).await {
            Ok(child) => {
                let state = state.clone();
                let events = self.events.clone();
                tokio::spawn(async move {
                    drive_print_run(state, invocation, events, child).await;
                });
                Ok(())
            }
            Err(error) => {
                state.run_active.store(false, Ordering::Relaxed);
                state.dead.store(true, Ordering::Relaxed);
                let _ = self
                    .events
                    .send(AdapterEvent::Failed(AdapterFailure::SpawnFailure {
                        detail: format!("failed to spawn resumed agy print run: {error}"),
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

/// The live Antigravity adapter.
///
/// Discovery (`detect`/`auth_status`/`capabilities`/`health`) is free and
/// makes no billable calls; auth stays entirely with the agy CLI /
/// Antigravity account — the adapter never touches credentials.
pub struct AgyAdapter {
    next_session: AtomicUsize,
    sessions: Mutex<Vec<SessionHandle>>,
    /// Cached `agy models` catalog + when it was fetched (free probe, but
    /// not free of latency — callers open the picker per UI view).
    models_cache: Mutex<Option<(std::time::Instant, Vec<AgyModelEntry>)>>,
}

/// One row of the `agy models` catalog (observed format: `<id>\t<label>`,
/// e.g. `gemini-3.1-pro-high\tGemini 3.1 Pro (High)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgyModelEntry {
    /// The `--model` slug (verbatim from the catalog).
    pub id: String,
    /// Human-facing label.
    pub label: String,
}

/// How long a fetched catalog stays fresh.
const MODELS_CACHE_TTL: Duration = Duration::from_secs(60);

impl AgyAdapter {
    /// Create the adapter.
    pub fn new() -> Self {
        Self {
            next_session: AtomicUsize::new(0),
            sessions: Mutex::new(Vec::new()),
            models_cache: Mutex::new(None),
        }
    }
    /// Spawn one session from a ready invocation. Shared by
    /// `start_session` (fresh) and `resume_session` (`--conversation <id>`).
    async fn launch(&self, invocation: AgyInvocation) -> Result<SessionHandle, AdapterError> {
        let binary = resolve_binary().ok_or_else(|| {
            AdapterError::Internal(format!(
                "agy binary not found (checked ${}, %LOCALAPPDATA%\\agy\\bin\\agy.exe, PATH)",
                AGY_BIN_ENV
            ))
        })?;
        // Spawn synchronously so machinery failures (bad cwd, missing
        // binary) are `Result` errors, per the F-02 contract.
        let child = spawn_agy_run(&binary, &invocation).await.map_err(|error| {
            AdapterError::Internal(format!(
                "failed to spawn agy in {}: {error}",
                invocation.workspace.display()
            ))
        })?;

        let session_no = self.next_session.fetch_add(1, Ordering::Relaxed) + 1;
        let handle_id = format!("agy-{session_no}");
        let (events, _) = broadcast::channel(1024);
        let state = std::sync::Arc::new(AgySessionState {
            handle_id: handle_id.clone(),
            binary,
            base_invocation: invocation.clone(),
            conversation_id: Mutex::new(None),
            child: tokio::sync::Mutex::new(None),
            cancelled: AtomicBool::new(false),
            dead: AtomicBool::new(false),
            run_active: AtomicBool::new(true),
        });
        let backend = std::sync::Arc::new(AgySessionBackend {
            state: state.clone(),
            events: events.clone(),
        });
        let handle = SessionHandle::new(handle_id, events.clone(), backend);
        self.sessions
            .lock()
            .expect("agy session registry poisoned")
            .push(handle.clone());

        let driver_state = state;
        let driver_invocation = invocation;
        let driver_events = events;
        tokio::spawn(async move {
            drive_print_run(driver_state, driver_invocation, driver_events, child).await;
        });
        Ok(handle)
    }

    /// The free `agy models` catalog — the authoritative model list *and*
    /// the authoritative auth check (handoff addendum; never billable).
    ///
    /// Parsing is total: the observed shape is tab-separated `<id>\t<label>`
    /// lines behind a `Fetching available models...` banner; unknown line
    /// shapes are skipped, never fatal. Cached for [`MODELS_CACHE_TTL`].
    pub async fn list_models(&self) -> Result<Vec<AgyModelEntry>, AdapterError> {
        {
            let cache = self.models_cache.lock().expect("agy models cache poisoned");
            if let Some((fetched_at, entries)) = cache.as_ref() {
                if fetched_at.elapsed() < MODELS_CACHE_TTL {
                    return Ok(entries.clone());
                }
            }
        }
        let binary = resolve_binary().ok_or_else(|| {
            AdapterError::Internal(format!(
                "agy binary not found (checked ${}, %LOCALAPPDATA%\\agy\\bin\\agy.exe, PATH)",
                AGY_BIN_ENV
            ))
        })?;
        let output = Command::new(&binary)
            .arg("models")
            .output()
            .await
            .map_err(|error| AdapterError::Internal(format!("spawn `agy models`: {error}")))?;
        if !output.status.success() {
            return Err(AdapterError::Internal(format!(
                "`agy models` failed ({}): {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let entries = parse_models_catalog(&String::from_utf8_lossy(&output.stdout));
        *self.models_cache.lock().expect("agy models cache poisoned") =
            Some((std::time::Instant::now(), entries.clone()));
        Ok(entries)
    }
}

/// Parse the observed `agy models` output into catalog rows. Total: the
/// banner and any line without a `<id>\t<label>` split is skipped.
fn parse_models_catalog(stdout: &str) -> Vec<AgyModelEntry> {
    stdout
        .lines()
        .filter_map(|line| {
            let line = line.trim_end_matches('\r').trim_start();
            let (id, label) = line.split_once('\t')?;
            let id = id.trim();
            if id.is_empty() {
                return None;
            }
            Some(AgyModelEntry {
                id: id.to_owned(),
                label: label.trim().to_owned(),
            })
        })
        .collect()
}

impl Default for AgyAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RuntimeAdapter for AgyAdapter {
    fn id(&self) -> &str {
        AGY_ADAPTER_ID
    }

    async fn detect(&self) -> RuntimeInfo {
        let path = resolve_binary();
        let version = match &path {
            Some(binary) => probe_version(binary).await,
            None => None,
        };
        RuntimeInfo {
            id: AGY_ADAPTER_ID.to_owned(),
            version,
            path,
        }
    }

    /// Free tier-0 check only (F-00 §5): binary presence plus the agy
    /// state dir (`~/.gemini/antigravity-cli`, where the brain/scratch
    /// dirs live). The authoritative check is the *free* `agy models`
    /// call — but that is a live invocation, so it is e2e-gated in tests
    /// and never performed here.
    async fn auth_status(&self) -> AuthStatus {
        if resolve_binary().is_none() {
            return AuthStatus::Unknown;
        }
        match antigravity_state_dir() {
            Some(dir) if dir.is_dir() => AuthStatus::Ready,
            _ => AuthStatus::Unknown,
        }
    }

    async fn capabilities(&self) -> Capabilities {
        agy_capabilities()
    }

    async fn start_session(&self, spec: SpawnSpec) -> Result<SessionHandle, AdapterError> {
        self.launch(AgyInvocation::from_spec(&spec)).await
    }

    /// Continue an earlier provider conversation: same invocation,
    /// plus `--conversation <id>`.
    async fn resume_session(
        &self,
        spec: SpawnSpec,
        provider_session_id: String,
    ) -> Result<SessionHandle, AdapterError> {
        self.launch(AgyInvocation::from_spec(&spec).with_conversation(provider_session_id))
            .await
    }

    async fn shutdown(&self) -> Result<(), AdapterError> {
        let sessions: Vec<SessionHandle> = self
            .sessions
            .lock()
            .expect("agy session registry poisoned")
            .clone();
        for handle in &sessions {
            // Best-effort, idempotent: cancel is process-kill semantics.
            let _ = handle.cancel().await;
        }
        Ok(())
    }
}

/// Capability surface, each flag annotated with its observed basis.
///
/// Tool-level allow/deny does **not** exist on agy (observed gap vs
/// Claude's `--allowedTools`/`--disallowedTools`): `Capabilities` is a
/// boolean surface, so the gap is invisible here by design — the F-doc
/// states it and F-10's harness-side gating (mode + `--add-dir` scoping +
/// forbidden-path enforcement in the supervisor) compensates.
fn agy_capabilities() -> Capabilities {
    Capabilities {
        // Real-dir writes verified with `--add-dir` (virtualized without).
        filesystem_edit: true,
        // `run_command` is in the init tool inventory; headless accept-edits
        // denies shell by policy (observable denial event) — capability
        // present, policy-gated.
        shell: true,
        // Full browser automation suite + web tools in the 58-tool inventory.
        network: true,
        // `--json-schema` → clean `structured_output` (verified).
        structured_output: true,
        // `--conversation <id>` round-trip verified (BANANA42).
        resume: true,
        // gemini-3.x model family accepts image input (model-family
        // inference; not directly exercised by the battery — see F-doc).
        multimodal: true,
        // gemini 1M-token-class catalog entries (gemini-3.1-pro).
        long_context: true,
        // `call_mcp_tool` observed in the init inventory.
        mcp_client: true,
    }
}

/// Resolve the agy binary: `$AGENTOS_AGY_BIN`, then the observed canonical
/// install `%LOCALAPPDATA%\agy\bin\agy.exe`, then a PATH scan. Free.
fn resolve_binary() -> Option<PathBuf> {
    if let Some(override_path) = std::env::var_os(AGY_BIN_ENV) {
        if !override_path.is_empty() {
            return Some(PathBuf::from(override_path));
        }
    }
    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
        let canonical = PathBuf::from(local_app_data)
            .join("agy")
            .join("bin")
            .join("agy.exe");
        if canonical.is_file() {
            return Some(canonical);
        }
    }
    find_on_path()
}

#[cfg(windows)]
const EXECUTABLE_NAMES: [&str; 2] = ["agy.exe", "agy"];

#[cfg(not(windows))]
const EXECUTABLE_NAMES: [&str; 1] = ["agy"];

fn find_on_path() -> Option<PathBuf> {
    let path_env = std::env::var_os("PATH")?;
    std::env::split_paths(&path_env).find_map(|dir| {
        EXECUTABLE_NAMES
            .iter()
            .map(|name| dir.join(name))
            .find(|candidate| candidate.is_file())
    })
}

/// The agy state dir (brain/scratch virtualized-write roots live under
/// it) — the only filesystem auth signal available without a live call.
fn antigravity_state_dir() -> Option<PathBuf> {
    let home = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"))?;
    Some(PathBuf::from(home).join(".gemini").join("antigravity-cli"))
}

/// Free `--version` probe; returns the first stdout line when available.
async fn probe_version(binary: &Path) -> Option<String> {
    let output = Command::new(binary).arg("--version").output().await.ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().next()?.trim();
    if line.is_empty() {
        None
    } else {
        Some(line.to_owned())
    }
}

/// Spawn one print run and deliver the prompt frame on stdin when the run
/// reads stdin. The handle is dropped after the write so the child sees EOF
/// and begins its turn instead of blocking on more input.
async fn spawn_agy_run(binary: &Path, invocation: &AgyInvocation) -> std::io::Result<Child> {
    let mut child = agy_command(binary, invocation).spawn()?;
    if let Some(frame) = invocation.stdin_frame() {
        let mut handle = child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("agy stdin pipe was not created"))?;
        handle.write_all(frame.as_bytes()).await?;
        handle.shutdown().await?;
    }
    Ok(child)
}

/// Assemble the child command for one print run. Argv-vector only — no
/// shell, no string quoting (F-05 hard rule).
fn agy_command(binary: &Path, invocation: &AgyInvocation) -> Command {
    let mut command = Command::new(binary);
    let stdin = if invocation.stdin_frame().is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    };
    command
        .args(invocation.args())
        .current_dir(&invocation.workspace)
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

/// Kill the agy child process.
///
/// Windows: tree-kill via `taskkill /PID <pid> /T /F` first (handoff §7.7)
/// because agy spawns its own children (subagents, browser automation);
/// a bare `TerminateProcess` on the direct child would orphan them. Then
/// the direct kill reaps regardless of taskkill's outcome.
async fn kill_child(child: &mut Child) {
    #[cfg(windows)]
    if let Some(pid) = child.id() {
        let tree_kill = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .output()
            .await;
        match &tree_kill {
            Ok(output) if output.status.success() => {}
            other => tracing::warn!(?other, pid, "agy adapter: taskkill tree-kill failed"),
        }
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

enum Watched<T> {
    Completed(T),
    TimedOut,
}

/// Run `fut` under the harness watchdog. `timeout_secs == 0` disables it
/// (the CLI's own 5m `--print-timeout` default then applies).
async fn harness_watchdog<F: std::future::Future>(timeout_secs: u64, fut: F) -> Watched<F::Output> {
    if timeout_secs == 0 {
        return Watched::Completed(fut.await);
    }
    match tokio::time::timeout(Duration::from_secs(timeout_secs), fut).await {
        Ok(value) => Watched::Completed(value),
        Err(_) => Watched::TimedOut,
    }
}

async fn kill_stored_child(state: &AgySessionState) {
    if let Some(mut child) = state.child.lock().await.take() {
        kill_child(&mut child).await;
    }
}

/// Reap the child and return its exit code. `None` child means cancel()
/// already took it to kill it — callers check the cancelled flag and
/// suppress the terminal event.
async fn take_exit_code(state: &AgySessionState) -> i32 {
    match state.child.lock().await.take() {
        Some(mut child) => match child.wait().await {
            Ok(status) => status.code().unwrap_or(-1),
            Err(error) => {
                tracing::warn!(%error, "agy adapter: failed to reap child");
                -1
            }
        },
        None => -1,
    }
}

/// Drain stderr line by line. Diagnostics only — classification never
/// reads stderr (F-00 §4 binding rule).
async fn drain_stderr(stderr: ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!(target: "agentos_adapters::agy::stderr", line = %line);
    }
}

/// Consume stream-json stdout, forwarding mapped events until EOF or
/// cancellation.
async fn read_stream(
    stdout: ChildStdout,
    state: &AgySessionState,
    events: &broadcast::Sender<AdapterEvent>,
) -> AgyStreamReducer {
    let mut reducer = AgyStreamReducer::default();
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

/// Drive one print run to its terminal event. The caller marks
/// `run_active` before spawning; this clears it and records the
/// conversation id. Cancelled runs end silently (no terminal event).
async fn drive_print_run(
    state: std::sync::Arc<AgySessionState>,
    invocation: AgyInvocation,
    events: broadcast::Sender<AdapterEvent>,
    mut child: Child,
) {
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    *state.child.lock().await = Some(child);
    if let Some(stderr) = stderr {
        tokio::spawn(drain_stderr(stderr));
    }

    let run = async {
        let reducer = match (invocation.output_format, stdout) {
            (AgyOutputFormat::StreamJson, Some(stdout)) => {
                read_stream(stdout, &state, &events).await
            }
            (AgyOutputFormat::Json, Some(mut stdout)) => {
                let mut text = String::new();
                match stdout.read_to_string(&mut text).await {
                    Ok(_) => AgyStreamReducer::from_json_output(&text),
                    Err(_) => AgyStreamReducer::default(),
                }
            }
            (_, None) => AgyStreamReducer::default(),
        };
        let exit_code = take_exit_code(&state).await;
        (reducer, exit_code)
    };

    let (reducer, exit_code) = match harness_watchdog(invocation.print_timeout_secs, run).await {
        Watched::Completed(pair) => pair,
        Watched::TimedOut => {
            kill_stored_child(&state).await;
            state.run_active.store(false, Ordering::Relaxed);
            state.dead.store(true, Ordering::Relaxed);
            let _ = events.send(AdapterEvent::Failed(harness_timeout_failure(
                invocation.print_timeout_secs,
            )));
            return;
        }
    };

    state.run_active.store(false, Ordering::Relaxed);
    if state.cancelled.load(Ordering::Relaxed) {
        // Process-kill semantics: the stream just ends.
        return;
    }
    if let Some(id) = reducer.conversation_id().map(str::to_owned) {
        let mut guard = state
            .conversation_id
            .lock()
            .expect("agy conversation state poisoned");
        if guard.is_none() {
            *guard = Some(id);
        }
    }

    let terminal: Vec<AdapterEvent> = match invocation.output_format {
        AgyOutputFormat::StreamJson => vec![reducer.finish(exit_code)],
        AgyOutputFormat::Json => json_run_events(reducer.result(), exit_code),
    };
    for event in terminal {
        if matches!(event, AdapterEvent::Failed(_)) {
            state.dead.store(true, Ordering::Relaxed);
        }
        let _ = events.send(event);
    }
}

// ---------------------------------------------------------------------------
// Tests — all offline. Fixtures below are SYNTHETIC, built from the
// documented observed schema (handoff ADDENDUM "Antigravity CLI
// v1.1.18"); no frozen agy transcript corpus exists on disk. No test
// spawns the real binary or makes any billable call.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;
    use uuid::Uuid;

    // -- synthetic fixtures (observed-schema shaped) ------------------------

    fn spec(objective: &str, allowed_paths: Vec<String>, timeout_secs: u64) -> SpawnSpec {
        SpawnSpec {
            task_id: Uuid::new_v4(),
            objective: objective.to_owned(),
            workspace: PathBuf::from("worktrees/task-agy"),
            allowed_paths,
            forbidden_paths: vec![],
            tool_allowlist: vec![],
            tool_denylist: vec![],
            model: None,
            timeout_secs,
            isolated_home: None,
        }
    }

    fn synth_result(status: &str, error: Option<&str>) -> AgyResult {
        AgyResult {
            conversation_id: Some("conv-synth-9".to_owned()),
            status: Some(status.to_owned()),
            response: (status == "SUCCESS").then(|| "Implemented the feature.".to_owned()),
            error: error.map(str::to_owned),
            usage: Some(AgyUsage {
                input_tokens: Some(38_455),
                output_tokens: Some(301),
                thinking_tokens: Some(12),
                cache_read_tokens: Some(31_800),
                total_tokens: Some(70_568),
            }),
            structured_output: None,
        }
    }

    /// Synthetic json-mode SUCCESS payload (schema per handoff ADDENDUM).
    const SYNTH_JSON_SUCCESS: &str = r#"{
        "conversation_id": "conv-synth-001",
        "status": "SUCCESS",
        "response": "Implemented the feature.",
        "error": null,
        "duration_seconds": 12.5,
        "num_turns": 3,
        "usage": {
            "input_tokens": 39211,
            "output_tokens": 742,
            "thinking_tokens": 118,
            "cache_read_tokens": 33208,
            "total_tokens": 73279
        },
        "structured_output": {"summary": "done", "files_changed": ["src/lib.rs"]},
        "json_schema": {"type": "object"}
    }"#;

    /// Synthetic json-mode ERROR payload — exit 2, usage still present.
    const SYNTH_JSON_ERROR: &str = r#"{
        "conversation_id": "conv-synth-002",
        "status": "ERROR",
        "response": null,
        "error": "The task could not be completed: tests failed",
        "duration_seconds": 8.0,
        "num_turns": 2,
        "usage": {
            "input_tokens": 38455,
            "output_tokens": 301,
            "thinking_tokens": 12,
            "cache_read_tokens": 31800,
            "total_tokens": 70568
        },
        "structured_output": null,
        "json_schema": null
    }"#;

    /// Synthetic stream-json SUCCESS transcript (init → step_update →
    /// unknown event → result).
    const SYNTH_STREAM_SUCCESS: &str = concat!(
        r#"{"type":"init","conversation_id":"conv-synth-003","model":"gemini-3.1-pro","cwd":"D:\\OP\\worktree","tools":["read_file","run_command"],"permission_mode":"acceptEdits"}"#,
        "\n",
        r#"{"type":"step_update","text_delta":"Analyzing the workspace","state":{"step":1},"usage":{"input_tokens":37133,"output_tokens":10,"thinking_tokens":0,"cache_read_tokens":0,"total_tokens":37143}}"#,
        "\n",
        r#"{"type":"step_update","text_delta":"Writing the plan","state":{"step":2}}"#,
        "\n",
        r#"{"type":"future_event_v2","payload":{"unknown":"schema drift"}}"#,
        "\n",
        r#"{"type":"result","conversation_id":"conv-synth-003","status":"SUCCESS","response":"Done","error":null,"usage":{"input_tokens":40133,"output_tokens":512,"thinking_tokens":64,"cache_read_tokens":32100,"total_tokens":72809},"structured_output":null}"#,
        "\n",
    );

    // -- discovery surface --------------------------------------------------

    #[tokio::test]
    async fn adapter_id_and_capability_surface_reflect_observed_agy() {
        let adapter: Box<dyn RuntimeAdapter> = Box::new(AgyAdapter::new());
        assert_eq!(adapter.id(), "antigravity-agy");

        let caps = adapter.capabilities().await;
        // Filesystem edit is --add-dir-mediated; resume is --conversation;
        // structured output is --json-schema; all verified in the battery.
        assert!(caps.filesystem_edit && caps.resume && caps.structured_output);
        assert!(caps.shell && caps.network && caps.long_context && caps.multimodal);
        assert!(caps.mcp_client);
        // NOTE: the per-tool allow/deny gap is deliberately NOT a boolean
        // here — Capabilities has no such field; the gap is documented in
        // the F-doc and compensated by harness-side gating (F-10).
    }

    // -- arg builder ---------------------------------------------------------

    /// A stream-json run delivers the prompt on stdin. Putting it in argv
    /// capped it at the OS command-line limit: a large phase envelope failed
    /// to spawn with os error 206 before reaching the model.
    #[test]
    fn stream_json_runs_deliver_the_prompt_on_stdin_not_argv() {
        let objective = "fix the flaky test -- and run it \"twice\"";
        let invocation = AgyInvocation::from_spec(&spec(objective, vec![], 600));
        assert_eq!(invocation.output_format, AgyOutputFormat::StreamJson);
        let args = invocation.args();

        assert!(
            !args.iter().any(|arg| arg.starts_with("--print=")),
            "prompt must not ride in argv: {args:?}"
        );
        assert!(!args.iter().any(|arg| arg.contains(objective)));
        assert_eq!(args[args.len() - 2], "--input-format");
        assert_eq!(args[args.len() - 1], "stream-json");
        assert!(args.contains(&"--output-format".to_owned()));

        // agy keys frames on `event`, and `message` must be an object.
        let frame = invocation
            .stdin_frame()
            .expect("stream-json run reads stdin");
        assert!(frame.ends_with('\n'), "frame must be one NDJSON line");
        let parsed: Value = serde_json::from_str(frame.trim()).expect("valid NDJSON");
        assert_eq!(parsed["event"], "user");
        assert_eq!(parsed["message"]["role"], "user");
        assert_eq!(parsed["message"]["content"][0]["type"], "text");
        assert_eq!(parsed["message"]["content"][0]["text"], objective);

        // An envelope far past the OS cap changes argv not at all.
        let mut huge = invocation.clone();
        huge.objective = "x".repeat(200 * 1024);
        let huge_args = huge.args();
        assert_eq!(huge_args, args, "argv must not grow with the prompt");
        let budget: usize = huge_args.iter().map(|arg| arg.len() + 1).sum();
        assert!(budget < 32_767, "argv must stay under the OS cap: {budget}");
    }

    /// A write turn must auto-approve tool permissions. Without it agy runs at
    /// `permission_mode=request-review`, no tool executes, and the run reports
    /// SUCCESS with an empty response and no deliverable — the exact signature
    /// that stalled Phase 6.
    #[test]
    fn write_mode_bypasses_permission_prompts_and_plan_mode_does_not() {
        let write = AgyInvocation::from_spec(&spec(
            "author the design doc",
            vec!["worktrees/task-agy/docs/DESIGN.md".to_owned()],
            600,
        ));
        assert_eq!(write.mode, AgyMode::AcceptEdits);
        assert!(
            write
                .args()
                .contains(&"--dangerously-skip-permissions".to_owned()),
            "a scoped write turn would silently no-op: {:?}",
            write.args()
        );

        let mut plan = write.clone();
        plan.mode = AgyMode::Plan;
        assert!(
            !plan
                .args()
                .contains(&"--dangerously-skip-permissions".to_owned()),
            "read-only planning must not auto-approve tools"
        );
    }

    /// `--dangerously-skip-permissions` lifts the terminal restriction as well
    /// as the write one — verified by a shell-only command creating a file on
    /// disk. agy cannot deny tools individually, so a turn whose caller denies
    /// shell must carry `--sandbox`, which was verified to stop that same
    /// command while leaving file writes working.
    #[test]
    fn a_turn_that_denies_shell_is_sandboxed() {
        let mut authoring = spec(
            "author the design doc",
            vec!["worktrees/task-agy/docs/DESIGN.md".to_owned()],
            600,
        );
        authoring.tool_denylist = vec!["Bash".to_owned(), "PowerShell".to_owned()];
        let guarded = AgyInvocation::from_spec(&authoring);
        assert!(guarded.sandbox);
        let args = guarded.args();
        assert!(args.contains(&"--sandbox".to_owned()));
        assert!(args.contains(&"--dangerously-skip-permissions".to_owned()));

        // A turn that never denied shell keeps it: build tasks legitimately
        // run commands, and --sandbox fails them silently.
        let open = AgyInvocation::from_spec(&spec(
            "run the build",
            vec!["worktrees/task-agy/src".to_owned()],
            600,
        ));
        assert!(!open.sandbox);
        assert!(!open.args().contains(&"--sandbox".to_owned()));
    }

    /// `--output-format json` has no stdin channel, so it keeps the verified
    /// variadic-safe equals form.
    #[test]
    fn json_runs_keep_the_equals_form_print_argument() {
        let mut invocation = AgyInvocation::from_spec(&spec("summarize the repo", vec![], 600));
        invocation.output_format = AgyOutputFormat::Json;
        let args = invocation.args();

        assert_eq!(args.last(), Some(&"--print=summarize the repo".to_owned()));
        assert!(!args.iter().any(|arg| arg == "--print"));
        assert!(invocation.stdin_frame().is_none());
    }

    #[test]
    fn arg_builder_always_grants_workspace_via_add_dir() {
        // Read-only spec (plan mode) still gets the workspace granted:
        // --add-dir is the access grant, --mode is the enforcement.
        let plan = AgyInvocation::from_spec(&spec("summarize the repo", vec![], 300));
        let plan_args = plan.args();
        let grant = plan_args
            .windows(2)
            .find(|w| w[0] == "--add-dir")
            .expect("plan-mode run must still pass --add-dir");
        assert_eq!(grant[1], "worktrees/task-agy");
        assert!(plan_args.contains(&"plan".to_owned()));

        let write = AgyInvocation::from_spec(&spec(
            "refactor module",
            vec!["worktrees/task-agy".to_owned()],
            300,
        ));
        let write_args = write.args();
        let grants = write_args
            .windows(2)
            .filter(|w| w[0] == "--add-dir")
            .collect::<Vec<_>>();
        assert_eq!(
            grants.len(),
            1,
            "allowed_paths entry equal to the workspace must be deduplicated: {:?}",
            write_args
        );
    }

    #[test]
    fn arg_builder_maps_write_intent_to_mode_from_allowed_paths() {
        // Documented mapping: empty allowed_paths → no declared write
        // targets → verified read-only plan mode.
        let read_only = AgyInvocation::from_spec(&spec("analyze", vec![], 60));
        assert!(read_only
            .args()
            .windows(2)
            .any(|w| w[0] == "--mode" && w[1] == "plan"));

        // Non-empty allowed_paths → declared write targets → accept-edits,
        // and every extra dir is its own repeatable --add-dir.
        let writing = AgyInvocation::from_spec(&spec(
            "implement",
            vec!["C:/repo/docs".to_owned(), "C:/repo/notes".to_owned()],
            60,
        ));
        let args = writing.args();
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--mode" && w[1] == "accept-edits"));
        let grants: Vec<&String> = args
            .windows(2)
            .filter(|w| w[0] == "--add-dir")
            .map(|w| &w[1])
            .collect();
        assert_eq!(
            grants,
            vec![
                &"worktrees/task-agy".to_owned(),
                &"C:/repo/docs".to_owned(),
                &"C:/repo/notes".to_owned()
            ]
        );
    }

    #[test]
    fn arg_builder_passes_model_effort_schema_and_conversation_flags() {
        let spec_with_model = {
            let mut base = spec("do the thing", vec!["C:/repo/docs".to_owned()], 600);
            base.model = Some("gemini-3.1-pro".to_owned());
            base
        };
        let invocation = AgyInvocation::from_spec(&spec_with_model)
            .with_effort(AgyEffort::Low)
            .with_json_schema("schemas/report.json")
            .with_conversation("conv-synth-003");
        let args = invocation.args();

        for flag in ["--model", "--effort", "--json-schema", "--conversation"] {
            assert!(
                args.contains(&flag.to_owned()),
                "missing {flag} in {args:?}"
            );
        }
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--model" && w[1] == "gemini-3.1-pro"));
        assert!(args.windows(2).any(|w| w[0] == "--effort" && w[1] == "low"));
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--json-schema" && w[1] == "schemas/report.json"));
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--conversation" && w[1] == "conv-synth-003"));
    }

    #[test]
    fn arg_builder_maps_timeout_to_print_timeout_with_backstop_slack() {
        let timed = AgyInvocation::from_spec(&spec("work", vec!["C:/repo/docs".to_owned()], 600));
        assert!(
            timed
                .args()
                .windows(2)
                .any(|w| w[0] == "--print-timeout" && w[1] == "630s"),
            "CLI gets harness budget + {}s slack so the harness watchdog \
             (which classifies timeout as Transient) fires first",
            AGY_PRINT_TIMEOUT_SLACK_SECS
        );

        // 0 = disabled: no flag, CLI default (5m) applies.
        let untimed = AgyInvocation::from_spec(&spec("work", vec!["C:/repo/d".to_owned()], 0));
        assert!(!untimed.args().iter().any(|arg| arg == "--print-timeout"));
    }

    #[test]
    fn arg_builder_renders_full_vector_in_canonical_order() {
        let invocation = AgyInvocation {
            objective: "objective text".to_owned(),
            workspace: PathBuf::from("wt"),
            extra_dirs: vec![PathBuf::from("extra")],
            mode: AgyMode::AcceptEdits,
            sandbox: false,
            output_format: AgyOutputFormat::Json,
            model: Some("gemini-3.1-pro".to_owned()),
            effort: Some(AgyEffort::High),
            json_schema: Some(PathBuf::from("s.json")),
            conversation: Some("conv-1".to_owned()),
            print_timeout_secs: 120,
        };
        assert_eq!(
            invocation.args(),
            vec![
                "--mode",
                "accept-edits",
                "--output-format",
                "json",
                "--dangerously-skip-permissions",
                "--model",
                "gemini-3.1-pro",
                "--effort",
                "high",
                "--json-schema",
                "s.json",
                "--conversation",
                "conv-1",
                "--add-dir",
                "wt",
                "--add-dir",
                "extra",
                "--print-timeout",
                "150s",
                "--print=objective text",
            ]
        );
    }

    // -- json-mode parser ----------------------------------------------------

    #[test]
    fn json_parser_happy_path_maps_success_result() {
        let reducer = AgyStreamReducer::from_json_output(SYNTH_JSON_SUCCESS);
        let events = json_run_events(reducer.result(), 0);
        assert_eq!(events.len(), 3);

        assert_eq!(
            events[0],
            AdapterEvent::Started {
                session_id: "conv-synth-001".to_owned(),
                // json mode has no init event; the model is unknown here.
                model: None,
            }
        );
        match &events[1] {
            AdapterEvent::UsageUpdate(usage) => {
                assert_eq!(usage.input_tokens, 39_211);
                assert_eq!(usage.total_tokens, 73_279);
                assert_eq!(usage.session_overhead_tokens, AGY_SESSION_OVERHEAD_TOKENS);
            }
            other => panic!("expected UsageUpdate, got {other:?}"),
        }
        assert_eq!(
            events[2],
            AdapterEvent::Finished {
                exit_code: 0,
                final_result: Some("Implemented the feature.".to_owned()),
                structured: Some(json!({"summary": "done", "files_changed": ["src/lib.rs"]})),
            }
        );
    }

    #[test]
    fn json_parser_error_status_preserves_usage_and_classifies_task_failure() {
        let reducer = AgyStreamReducer::from_json_output(SYNTH_JSON_ERROR);
        assert!(reducer.result().is_some(), "ERROR payload must parse");
        let events = json_run_events(reducer.result(), 2);

        assert_eq!(events[0].kind(), "started");
        match &events[1] {
            AdapterEvent::UsageUpdate(usage) => {
                // Observed: ERROR → exit 2 with usage still present.
                assert_eq!(usage.input_tokens, 38_455);
                assert_eq!(usage.total_tokens, 70_568);
                assert_eq!(usage.cost_usd, None, "agy reports tokens only");
                assert!(usage.per_model.is_empty(), "single-model CLI, no breakdown");
                assert_eq!(usage.session_overhead_tokens, 37_000);
            }
            other => panic!("expected UsageUpdate before Failed, got {other:?}"),
        }
        match &events[2] {
            AdapterEvent::Failed(failure) => {
                assert_eq!(failure.kind(), "task_failure");
                assert!(!failure.is_retryable());
            }
            other => panic!("expected Failed terminal, got {other:?}"),
        }
    }

    #[test]
    fn json_parser_tolerates_noise_around_the_result_object() {
        let noisy = format!("warmup noise\n{SYNTH_JSON_SUCCESS}\ntrailing line\n");
        let reducer = AgyStreamReducer::from_json_output(&noisy);
        assert_eq!(
            reducer.result().unwrap().conversation_id.as_deref(),
            Some("conv-synth-001")
        );

        let garbage = "not json at all";
        assert!(AgyStreamReducer::from_json_output(garbage)
            .result()
            .is_none());
    }

    // -- stream-json parser --------------------------------------------------

    /// agy exposes no asking tool, so a plan-mode question rides in the
    /// answer text as a fenced block (the `decision-protocol` skill teaches
    /// the shape). The reducer lifts it into a `Decision` at result time,
    /// before the terminal event, so the desktop gets its buttons.
    #[test]
    fn result_text_carrying_a_fenced_ask_becomes_a_decision() {
        let mut reducer = AgyStreamReducer::default();
        let response = "I need one call first.\n\n```json\n            {\"ask\": {\"question\": \"Which database?\",             \"options\": [\"Postgres\", \"SQLite\"]}}\n```";
        let line = json!({
            "type": "result",
            "conversation_id": "conv-ask-1",
            "status": "SUCCESS",
            "response": response,
            "error": null,
            "structured_output": null,
        })
        .to_string();

        let events = reducer.push_line(&line);
        let kinds: Vec<&str> = events.iter().map(AdapterEvent::kind).collect();
        assert_eq!(kinds, vec!["decision"], "usage is absent in this line");
        match &events[0] {
            AdapterEvent::Decision {
                tool,
                prompt,
                options,
                multi_select,
            } => {
                assert_eq!(tool, "ask");
                assert_eq!(prompt, "Which database?");
                assert_eq!(options, &["Postgres".to_owned(), "SQLite".to_owned()]);
                assert!(!multi_select);
            }
            other => panic!("expected Decision, got {other:?}"),
        }
        assert!(
            reducer
                .push_line(
                    &json!({"type": "result", "status": "SUCCESS",
                "response": "Plain answer, no block."})
                    .to_string()
                )
                .is_empty(),
            "ordinary answers are not decisions"
        );
    }

    // -- frozen LIVE transcripts (cli-agy-output/<stamp>/fixtures) --------
    //
    // F-05 shipped against synthetic FLAT fixtures; the real CLI nests each
    // payload under a key equal to its kind. These tests exist so that can
    // never regress unnoticed again.

    fn live_fixture(name: &str) -> Option<String> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../cli-agy-output");
        let mut runs: Vec<_> = std::fs::read_dir(root)
            .ok()?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect();
        runs.sort();
        std::fs::read_to_string(runs.last()?.join("fixtures").join(name)).ok()
    }

    /// The live envelope, end to end: nested payloads, the conversation id
    /// hoisted onto `init`, streamed text fragments, one ToolUse per tool
    /// call, usage, and a parsed result. Before the fix this transcript
    /// produced exactly one event (`Started`) — no text, no result, no
    /// resume handle.
    #[test]
    fn the_live_envelope_maps_to_events() {
        let Some(transcript) = live_fixture("agy.A1-tool-retries.stdout.jsonl") else {
            eprintln!("agy live fixture corpus unavailable; skipping");
            return;
        };
        let mut reducer = AgyStreamReducer::default();
        let events: Vec<AdapterEvent> = transcript
            .lines()
            .flat_map(|line| reducer.push_line(line))
            .collect();

        match events.first().expect("init maps first") {
            AdapterEvent::Started { session_id, model } => {
                assert!(
                    !session_id.is_empty() && session_id != "unknown",
                    "conversation_id is hoisted onto the envelope: {session_id}"
                );
                assert_eq!(
                    model.as_deref(),
                    Some("claude-sonnet-4-6"),
                    "model is nested inside `init`"
                );
            }
            other => panic!("expected Started, got {other:?}"),
        }
        assert!(
            events.iter().any(
                |event| matches!(event, AdapterEvent::TextDelta(text) if !text.trim().is_empty())
            ),
            "text_delta fragments must reach the stream"
        );
        let tool_uses: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                AdapterEvent::ToolUse { tool, .. } => Some(tool.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            tool_uses,
            vec!["find_by_name", "find_by_name", "find_by_name"],
            "one ToolUse per ACTIVE edge; DONE/ERROR edges must not double-count"
        );
        assert!(
            events.iter().any(
                |event| matches!(event, AdapterEvent::UsageUpdate(usage) if usage.input_tokens > 0)
            ),
            "usage rides DONE steps and the result"
        );

        let result = reducer.result().expect("the result payload parses");
        assert!(reducer.conversation_id().is_some(), "resume handle learned");
        assert!(
            result
                .response
                .as_deref()
                .is_some_and(|text| !text.trim().is_empty()),
            "the response text is what the daemon reports as finalResult"
        );
    }

    /// Observed on both captured ERROR runs: agy reports `status: ERROR`
    /// while exiting **0**, contradicting the synthetic canon's
    /// "ERROR → exit 2". The payload's status is authoritative (F-00 §4),
    /// so the terminal event must be a failure even on a clean exit.
    #[test]
    fn an_error_status_with_exit_zero_is_still_a_failure() {
        let Some(transcript) = live_fixture("agy.A1-tool-retries.stdout.jsonl") else {
            eprintln!("agy live fixture corpus unavailable; skipping");
            return;
        };
        let mut reducer = AgyStreamReducer::default();
        for line in transcript.lines() {
            reducer.push_line(line);
        }
        let result = reducer.result().expect("result parses");
        assert_eq!(result.status.as_deref(), Some("ERROR"));
        assert!(
            !matches!(reducer.finish(0), AdapterEvent::Finished { .. }),
            "an ERROR payload never finishes successfully, whatever the exit code"
        );
    }

    /// A quota window is retryable, not a task failure — and the
    /// provider's own reset time survives into the detail so the backoff
    /// policy can use it. Text observed verbatim on 2026-08-22.
    #[test]
    fn a_quota_window_is_transient_with_its_reset_time() {
        let result = AgyResult {
            conversation_id: Some("conv-quota".to_owned()),
            status: Some("ERROR".to_owned()),
            response: Some(String::new()),
            error: Some(
                "Individual quota reached. Please upgrade your subscription to \
                 increase your limits. Resets in 146h41m26s."
                    .to_owned(),
            ),
            usage: None,
            structured_output: None,
        };
        // Exit 0 — the observed combination, which the generic ladder
        // would otherwise read as "succeeded".
        match terminal_event(0, Some(&result)) {
            AdapterEvent::Failed(failure) => {
                assert!(failure.is_retryable(), "quota windows retry: {failure:?}");
                assert!(
                    format!("{failure:?}").contains("146h41m26s"),
                    "the reset window is preserved: {failure:?}"
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// The quota path against a REAL transcript (gpt-oss-120b-medium,
    /// exit 1): the reducer must reach the quota classification, not the
    /// generic ladder. Exit code varies (0 and 1 both observed), so the
    /// payload error is what decides.
    #[test]
    fn a_real_quota_transcript_classifies_as_transient() {
        let Some(transcript) = live_fixture("agy.A4-quota-exhausted.stdout.jsonl") else {
            eprintln!("agy live fixture corpus unavailable; skipping");
            return;
        };
        let mut reducer = AgyStreamReducer::default();
        for line in transcript.lines() {
            reducer.push_line(line);
        }
        let result = reducer.result().expect("result payload parses");
        assert!(result.error.as_deref().is_some_and(|e| e.contains("quota")));
        match reducer.finish(1) {
            AdapterEvent::Failed(failure) => assert!(
                failure.is_retryable(),
                "a quota window is retryable whatever the exit code: {failure:?}"
            ),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// agy's `ask_question` / `ask_permission` tools exist in the init
    /// inventory but are auto-skipped headless — no tool step ever reaches
    /// the stream. That is why decisions on agy ride the text protocol
    /// (`decision::from_text`) instead of a tool surface.
    #[test]
    fn headless_ask_tools_never_reach_the_stream() {
        let Some(transcript) = live_fixture("agy.A3-ask-question-skipped.stdout.jsonl") else {
            eprintln!("agy live fixture corpus unavailable; skipping");
            return;
        };
        let mut reducer = AgyStreamReducer::default();
        let events: Vec<AdapterEvent> = transcript
            .lines()
            .flat_map(|line| reducer.push_line(line))
            .collect();
        assert!(
            !events.iter().any(|event| matches!(
                event,
                AdapterEvent::ToolUse { tool, .. } if tool.starts_with("ask_")
            )),
            "no ask_* tool step is observable headless"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AdapterEvent::Decision { .. })),
            "and the model's prose is not a decision either"
        );
        let inventory = transcript.lines().next().expect("init line");
        assert!(
            inventory.contains("ask_question"),
            "the tool IS in the inventory; it is the headless path that skips it"
        );
    }

    #[test]
    fn stream_reducer_maps_init_step_and_result_events() {
        let mut reducer = AgyStreamReducer::default();
        let mut all = Vec::new();
        for line in SYNTH_STREAM_SUCCESS.lines() {
            all.extend(reducer.push_line(line));
        }

        assert_eq!(
            all,
            vec![
                AdapterEvent::Started {
                    session_id: "conv-synth-003".to_owned(),
                    model: Some("gemini-3.1-pro".to_owned()),
                },
                AdapterEvent::TextDelta("Analyzing the workspace".to_owned()),
                // Per-step usage is forwarded as its own snapshot.
                AdapterEvent::UsageUpdate(
                    AgyUsage {
                        input_tokens: Some(37_133),
                        output_tokens: Some(10),
                        thinking_tokens: Some(0),
                        cache_read_tokens: Some(0),
                        total_tokens: Some(37_143),
                    }
                    .to_snapshot()
                ),
                AdapterEvent::TextDelta("Writing the plan".to_owned()),
                // result event: usage first, terminal deferred to exit code.
                AdapterEvent::UsageUpdate(
                    AgyUsage {
                        input_tokens: Some(40_133),
                        output_tokens: Some(512),
                        thinking_tokens: Some(64),
                        cache_read_tokens: Some(32_100),
                        total_tokens: Some(72_809),
                    }
                    .to_snapshot()
                ),
            ]
        );

        // Unknown mid-stream event produced nothing and did not panic
        // (implicitly proven: no stray events above).
        assert_eq!(reducer.conversation_id(), Some("conv-synth-003"));
        assert_eq!(
            reducer.finish(0),
            AdapterEvent::Finished {
                exit_code: 0,
                final_result: Some("Done".to_owned()),
                structured: None,
            }
        );
    }

    #[test]
    fn stream_reducer_skips_unknown_and_malformed_lines() {
        let mut reducer = AgyStreamReducer::default();
        assert!(reducer.push_line("").is_empty());
        assert!(reducer.push_line("   ").is_empty());
        assert!(reducer.push_line("not json").is_empty());
        assert!(
            reducer
                .push_line(r#"{"type":"brand_new_event","data":{}}"#)
                .is_empty(),
            "schema drift must be tolerated (log + skip)"
        );
        assert!(
            reducer.push_line(r#"{"event":"step_update","text_delta":"alt envelope"}"#)
                == vec![AdapterEvent::TextDelta("alt envelope".to_owned())],
            "envelope field `event` accepted as a fallback"
        );

        // No result event at all + non-zero exit → pre-model death.
        assert_eq!(reducer.finish(1).kind(), "failed");
        assert!(matches!(
            reducer.finish(1),
            AdapterEvent::Failed(AdapterFailure::SpawnFailure { .. })
        ));
    }

    #[test]
    fn stream_reducer_learns_conversation_id_from_init_before_result() {
        let mut reducer = AgyStreamReducer::default();
        reducer
            .push_line(r#"{"type":"init","conversation_id":"conv-early","model":"gpt-oss-120b"}"#);
        assert_eq!(reducer.conversation_id(), Some("conv-early"));
    }

    // -- usage + classification tables ---------------------------------------

    #[test]
    fn usage_mapping_carries_agy_fields_and_overhead_constant() {
        let snapshot = AgyUsage {
            input_tokens: Some(1_000),
            output_tokens: Some(500),
            thinking_tokens: Some(64),
            cache_read_tokens: Some(22_000),
            // total missing → falls back to the field sum.
            total_tokens: None,
        }
        .to_snapshot();
        assert_eq!(snapshot.total_tokens, 23_564);
        assert_eq!(snapshot.thinking_tokens, 64);
        assert_eq!(snapshot.cache_read_tokens, 22_000);
        assert_eq!(snapshot.cost_usd, None);
        assert!(snapshot.per_model.is_empty());
        assert_eq!(snapshot.session_overhead_tokens, 37_000);
        assert_eq!(AGY_SESSION_OVERHEAD_TOKENS, 37_000);

        // Fully-absent usage maps to zeros, never a parse failure.
        let empty = AgyUsage::default().to_snapshot();
        assert_eq!(empty.total_tokens, 0);
    }

    #[test]
    fn classification_table_for_agy_run_endings() {
        /// (exit code, result payload, expected failure kind) — `None`
        /// expected kind means the terminal event is `Finished`.
        type Case = ((i32, Option<AgyResult>), Option<&'static str>);

        let cases: Vec<Case> = vec![
            // SUCCESS + exit 0 + result present: the F-00 §4 conjunction.
            ((0, Some(synth_result("SUCCESS", None))), None),
            // Live regression: agy can omit or drift the optional status even
            // though it exits 0 with a complete final result. The generic
            // success conjunction must still win when no explicit failure is
            // present.
            (
                (
                    0,
                    Some(AgyResult {
                        conversation_id: Some("conv-missing-status".to_owned()),
                        status: None,
                        response: Some("VERDICT: FAIL\nreal review text".to_owned()),
                        error: None,
                        usage: None,
                        structured_output: None,
                    }),
                ),
                None,
            ),
            // Observed live while replaying Phase 9: the provider asks the
            // caller to continue after an interrupted stream. That is
            // retryable infrastructure loss, not a permanent task failure.
            (
                (
                    0,
                    Some(AgyResult {
                        conversation_id: Some("conv-interrupted".to_owned()),
                        status: None,
                        response: None,
                        error: Some(
                            "The stream was interrupted. Please continue the task you were working on."
                                .to_owned(),
                        ),
                        usage: None,
                        structured_output: None,
                    }),
                ),
                Some("transient"),
            ),
            (
                (
                    0,
                    Some(AgyResult {
                        conversation_id: Some("conv-drifted-status".to_owned()),
                        status: Some("COMPLETED".to_owned()),
                        response: Some("done".to_owned()),
                        error: None,
                        usage: None,
                        structured_output: None,
                    }),
                ),
                None,
            ),
            // agy ERROR shape: exit 2 with the result event present → the
            // model round-tripped and failed on its own merits.
            (
                (2, Some(synth_result("ERROR", Some("tests failed")))),
                Some("task_failure"),
            ),
            // Observable denial inside the structured error field.
            (
                (
                    2,
                    Some(synth_result(
                        "ERROR",
                        Some("run_command: user denied permission"),
                    )),
                ),
                Some("policy_denial"),
            ),
            // Pre-model death: exit 1, no final result event.
            ((1, None), Some("spawn_failure")),
            // Exit 0 alone is not success without a final result event.
            ((0, None), Some("spawn_failure")),
            // Payload status outranks a zero exit code.
            (
                (0, Some(synth_result("ERROR", Some("boom")))),
                Some("task_failure"),
            ),
            // Non-zero exit with a SUCCESS payload is still a failed run.
            (
                (2, Some(synth_result("SUCCESS", None))),
                Some("task_failure"),
            ),
        ];
        for ((exit_code, result), expected_kind) in cases {
            let event = terminal_event(exit_code, result.as_ref());
            match expected_kind {
                None => assert!(
                    matches!(event, AdapterEvent::Finished { .. }),
                    "exit {exit_code}: expected Finished, got {event:?}"
                ),
                Some(kind) => match &event {
                    AdapterEvent::Failed(failure) => assert_eq!(
                        failure.kind(),
                        kind,
                        "exit {exit_code}, result {result:?}: {failure}"
                    ),
                    other => panic!("exit {exit_code}: expected Failed, got {other:?}"),
                },
            }
        }
    }

    #[test]
    fn timeout_maps_to_transient_failure() {
        let failure = harness_timeout_failure(600);
        assert_eq!(failure.kind(), "transient");
        assert!(failure.is_retryable(), "timeouts clear on their own");
    }

    // -- e2e (free-only, opt-in) ----------------------------------------------
    //
    // The ONLY live invocations in this module: `--version` and the free
    // `agy models` catalog probe. Skipped by default (`#[ignore]`) and
    // doubly gated behind AGENTOS_AGY_E2E=1. Never a billable call.

    #[tokio::test]
    #[ignore = "live agy invocation (free probes only): set AGENTOS_AGY_E2E=1 to run"]
    async fn e2e_free_probes_version_and_models_catalog() {
        if std::env::var(AGY_E2E_ENV).ok().as_deref() != Some("1") {
            return;
        }
        let adapter = AgyAdapter::new();

        let info = adapter.detect().await;
        assert!(
            info.version.is_some(),
            "agy --version produced no output: {info:?}"
        );

        // `agy models` is free and is the authoritative auth check (handoff
        // addendum: multi-provider catalog — gemini-3.x, claude-sonnet/
        // opus-4-6, gpt-oss-120b).
        let catalog = adapter.list_models().await.expect("free models probe");
        assert!(!catalog.is_empty(), "empty model catalog");
        assert!(
            catalog.iter().any(|entry| entry.id.starts_with("gemini-")),
            "expected gemini rows, got {catalog:?}"
        );
    }

    #[test]
    fn models_catalog_parser_is_total_over_the_observed_shape() {
        let stdout = "Fetching available models...\r\n\
                      gemini-3.1-pro-high\tGemini 3.1 Pro (High)\r\n\
                      claude-sonnet-4-6\tClaude Sonnet 4.6 (Thinking)\r\n\
                      no-tab-line\r\n\
                      \tlabel-without-id\r\n";
        let entries = parse_models_catalog(stdout);
        assert_eq!(entries.len(), 2, "{entries:?}");
        assert_eq!(entries[0].id, "gemini-3.1-pro-high");
        assert_eq!(entries[0].label, "Gemini 3.1 Pro (High)");
        assert_eq!(entries[1].id, "claude-sonnet-4-6");
        assert!(parse_models_catalog("").is_empty());
    }
}
