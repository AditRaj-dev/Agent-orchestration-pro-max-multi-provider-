//! Claude Code adapter — F-03.
//!
//! Drives the Claude Code CLI (observed v2.1.238 at
//! `%USERPROFILE%\.local\bin\claude.exe`) through the F-02
//! [`RuntimeAdapter`] contract. Canon sources (binding):
//! `F-00-CONVENTIONS.md` §4 claude rows, `handoff.md` §3.1 + ADDENDUM
//! (T1–T5 battery, all observed on this machine 2026-08-22), and the frozen
//! fixture corpus under `cli-fix-output/2026-08-22T06-30-51-908Z/fixtures`.
//!
//! Observed contract this adapter encodes:
//!
//! - Headless form: `-p --output-format stream-json --verbose`. The prompt is
//!   delivered on **stdin by default** (T3-verified): immune to the variadic
//!   tool-list flags *and* to Windows command-line length limits.
//! - `--allowedTools` / `--disallowedTools` are **variadic** — they must be
//!   rendered in equals form (`--flag=a,b`) so they never swallow a following
//!   positional prompt (T1/T2/T4; the §3.2 root cause).
//! - Init event is `type=system && subtype=init` (other `system` subtypes,
//!   e.g. `thinking_tokens`, occur mid-run and must be skipped).
//! - The terminal `result` object carries `session_id` (the `--resume`
//!   handle, BANANA42 round-trip verified), `total_cost_usd` (the field
//!   `cost_usd` does not exist), `usage`, a `modelUsage` **map** keyed by
//!   model (a trivial run touches two models: main + auxiliary haiku),
//!   `permission_denials`, `subagent_stats`, `is_error`, `api_error_status`.
//! - `rate_limit_event` appears inside *successful* runs → telemetry for
//!   proactive provider backoff (F-06), never a failure by itself.
//! - `--max-turns` works but is absent from `--help` (battery used 6).
//!
//! Classification follows F-00 §4: exit code + presence of the final result
//! event, plus payload-content hooks (`is_error`, `api_error_status`,
//! `permission_denials`) — never stderr text. stderr is drained for debug
//! logging only.
//!
//! All offline tests run against the frozen fixture corpus (real observed
//! transcripts) plus synthetic edge cases; nothing spawns the real binary.
//! The only live invocations anywhere are free probes (`--version`,
//! auth-file existence) in an `#[ignore]`d test gated behind
//! `AGENTOS_CLAUDE_E2E=1`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::broadcast;

use crate::adapter::{RuntimeAdapter, SessionBackend, SessionHandle};
use crate::error::{AdapterError, AdapterFailure, Classifier};
use crate::events::AdapterEvent;
use crate::types::{
    AuthStatus, Capabilities, ModelUsageRow, RuntimeInfo, SpawnSpec, UsageSnapshot,
};

/// Stable adapter identifier (F-02 `RuntimeAdapter::id`).
pub const CLAUDE_ADAPTER_ID: &str = "claude-code";
/// Fixed per-session preamble Claude Code bills (F-00 §4: ~22k cache-read
/// tokens observed on trivial runs; core system prompt, *not* user skills —
/// the T5 A/B isolated-home experiment moved cacheRead 22115 → 22115).
pub const CLAUDE_SESSION_OVERHEAD_TOKENS: u64 = 22_000;
/// Env var overriding the claude binary location (same pattern as the probe
/// scripts' `PROBE_CLAUDE_BIN`).
pub const CLAUDE_BIN_ENV: &str = "AGENTOS_CLAUDE_BIN";
/// Env var that opts the ignored e2e test into live (free-only) claude probes.
pub const CLAUDE_E2E_ENV: &str = "AGENTOS_CLAUDE_E2E";
/// Env var pointing tests at the frozen fixture corpus directory (defaults
/// to the sibling `cli-fix-output/2026-08-22T06-30-51-908Z/fixtures`).
pub const CLAUDE_FIXTURES_ENV: &str = "AGENTOS_CLAUDE_FIXTURES";
/// Credentials file whose *existence* is the free auth check (observed on
/// this install; never read — existence only, no credential harvesting).
const CLAUDE_CREDENTIALS_FILE: &str = ".credentials.json";
/// Longest rendered tool-argument summary / single value within it.
const TOOL_SUMMARY_MAX: usize = 120;
/// Longest rendered single value inside a tool-argument summary.
const TOOL_SUMMARY_VALUE_MAX: usize = 60;

/// `--permission-mode` choice set (v2.1.238 `--help`:
/// `acceptEdits | auto | bypassPermissions | manual | dontAsk | plan`).
/// [`ClaudePermissionMode::Default`] is the CLI's implicit mode when the
/// flag is omitted (T1/T5 runs; init reported `permissionMode: "default"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClaudePermissionMode {
    /// Omit the flag. In headless `-p` runs, tools that would need approval
    /// cannot ask, so calls surface as denials (`permission_denials`) rather
    /// than blocking — a de-facto read-only-leaning default.
    Default,
    /// `--permission-mode acceptEdits` — the battery's verified headless
    /// write mode (probe.txt created exactly; T2/T3/T4 all ran it).
    AcceptEdits,
    /// `--permission-mode plan` — documented choice, NOT exercised by the
    /// battery; the adapter never selects it on its own.
    Plan,
    /// `--permission-mode dontAsk` — documented choice, not exercised.
    DontAsk,
    /// `--permission-mode auto` — documented choice, not exercised.
    Auto,
    /// `--permission-mode manual` — documented choice, not exercised.
    Manual,
    /// `--permission-mode bypassPermissions` — documented choice; dangerous,
    /// never selected by the adapter (callers must opt in explicitly).
    BypassPermissions,
}

impl ClaudePermissionMode {
    /// Flag value as the CLI expects it; `None` means "omit the flag"
    /// (only [`ClaudePermissionMode::Default`]).
    pub fn as_flag_value(self) -> Option<&'static str> {
        match self {
            ClaudePermissionMode::Default => None,
            ClaudePermissionMode::AcceptEdits => Some("acceptEdits"),
            ClaudePermissionMode::Plan => Some("plan"),
            ClaudePermissionMode::DontAsk => Some("dontAsk"),
            ClaudePermissionMode::Auto => Some("auto"),
            ClaudePermissionMode::Manual => Some("manual"),
            ClaudePermissionMode::BypassPermissions => Some("bypassPermissions"),
        }
    }
}

/// One fully-resolved claude headless invocation (pure, unit-testable).
///
/// Built from a [`SpawnSpec`] via [`ClaudeInvocation::from_spec`] plus
/// passthrough hooks for fields the spec does not carry (resume id,
/// max-turns, explicit permission mode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeInvocation {
    /// The objective prompt. Delivered on stdin by default
    /// ([`ClaudeInvocation::prompt_via_stdin`], T3 canon); when argv
    /// delivery is selected it is rendered as the LAST argument so the
    /// equals-form variadic flags before it cannot swallow it.
    pub objective: String,
    /// Working directory for the child process (cwd).
    pub workspace: PathBuf,
    /// Deliver the prompt via stdin (`true`, default, T3-verified) instead
    /// of a trailing positional argv element. Stdin is immune both to the
    /// variadic tool-list flags and to Windows cmdline length limits.
    pub prompt_via_stdin: bool,
    /// Coarse permission mode; see [`ClaudePermissionMode`].
    pub permission_mode: ClaudePermissionMode,
    /// Per-tool allowlist → `--allowedTools=a,b` (EQUALS form, T4 canon).
    /// Empty list omits the flag.
    pub tool_allowlist: Vec<String>,
    /// Per-tool denylist → `--disallowedTools=a,b` (EQUALS form, T1/T2
    /// canon). Empty list omits the flag.
    pub tool_denylist: Vec<String>,
    /// `--model <id>` passthrough when set.
    pub model: Option<String>,
    /// `--resume <session_id>` target when set (BANANA42-verified resume
    /// path; used for follow-up instructions on live sessions).
    pub resume_session_id: Option<String>,
    /// `--max-turns <n>` cap. Works but is UNDOCUMENTED in v2.1.238
    /// `--help` (the battery ran `--max-turns 6` successfully) — smoke-test
    /// on adapter upgrades rather than trusting a help scan.
    pub max_turns: Option<u32>,
    /// Redirected home for env isolation (handoff §4.1): the child's
    /// `USERPROFILE` and `HOME` point here. Junction creation (e.g. to the
    /// real `~/.claude` for auth) is the supervisor's job, not the
    /// adapter's — T5b proved config-copy isolation runs fine.
    pub isolated_home: Option<PathBuf>,
    /// Harness wall-clock budget in seconds; `0` disables the watchdog.
    pub timeout_secs: u64,
}

impl ClaudeInvocation {
    /// Derive an invocation from a spawn spec.
    ///
    /// Permission-mode mapping (documented F-03 decision): `SpawnSpec`
    /// carries no write-intent field, so F-03 pins
    /// [`ClaudePermissionMode::AcceptEdits`] — the battery's verified
    /// headless write mode (file writes verified; T2/T3/T4 ran it).
    /// Read-only and network-off policy is expressed through
    /// `tool_denylist` (T2-verified: `Bash,WebFetch,WebSearch` denied →
    /// zero tool uses, graceful exit 0) rather than through plan mode,
    /// which the battery never exercised. Override with
    /// [`ClaudeInvocation::with_permission_mode`].
    pub fn from_spec(spec: &SpawnSpec) -> Self {
        Self {
            objective: spec.objective.clone(),
            workspace: spec.workspace.clone(),
            prompt_via_stdin: true,
            permission_mode: ClaudePermissionMode::AcceptEdits,
            tool_allowlist: spec.tool_allowlist.clone(),
            tool_denylist: spec.tool_denylist.clone(),
            model: spec.model.clone(),
            resume_session_id: None,
            max_turns: None,
            isolated_home: spec.isolated_home.clone(),
            timeout_secs: spec.timeout_secs,
        }
    }

    /// Override the model.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Override the permission mode.
    pub fn with_permission_mode(mut self, mode: ClaudePermissionMode) -> Self {
        self.permission_mode = mode;
        self
    }

    /// Resume the given provider session (`--resume <id>`).
    pub fn with_resume_session(mut self, session_id: impl Into<String>) -> Self {
        self.resume_session_id = Some(session_id.into());
        self
    }

    /// Cap the run at `n` turns (`--max-turns`, works but undocumented).
    pub fn with_max_turns(mut self, turns: u32) -> Self {
        self.max_turns = Some(turns);
        self
    }

    /// Deliver the prompt as a trailing positional argv element instead of
    /// stdin. The tool-list flags stay in equals form, so the prompt cannot
    /// be swallowed (T1 shape). Stdin remains the recommended default.
    pub fn with_argv_prompt(mut self) -> Self {
        self.prompt_via_stdin = false;
        self
    }

    /// Render the argv vector (canonical order, stable for tests).
    ///
    /// Tool-list flags are ALWAYS single argv elements in equals form —
    /// `--allowedTools`/`--disallowedTools` are variadic, and the observed
    /// failure mode (`Error: Input must be provided either through stdin or
    /// as a prompt argument`, exit 1, zero events) comes exactly from the
    /// separate-value form consuming a following positional prompt.
    pub fn args(&self) -> Vec<String> {
        let mut args = vec![
            "-p".to_owned(),
            "--output-format".to_owned(),
            "stream-json".to_owned(),
            "--verbose".to_owned(),
        ];
        if let Some(mode) = self.permission_mode.as_flag_value() {
            args.push("--permission-mode".to_owned());
            args.push(mode.to_owned());
        }
        if !self.tool_allowlist.is_empty() {
            args.push(format!("--allowedTools={}", self.tool_allowlist.join(",")));
        }
        if !self.tool_denylist.is_empty() {
            args.push(format!(
                "--disallowedTools={}",
                self.tool_denylist.join(",")
            ));
        }
        if let Some(model) = &self.model {
            args.push("--model".to_owned());
            args.push(model.clone());
        }
        if let Some(session_id) = &self.resume_session_id {
            args.push("--resume".to_owned());
            args.push(session_id.clone());
        }
        if let Some(turns) = self.max_turns {
            args.push("--max-turns".to_owned());
            args.push(turns.to_string());
        }
        if !self.prompt_via_stdin {
            args.push(self.objective.clone());
        }
        args
    }

    /// Env-var redirection for home isolation: `USERPROFILE` and `HOME`
    /// both point at the isolated home (handoff §4.1 pattern). Empty when
    /// isolation is disabled. The adapter performs ONLY this env mapping —
    /// building the isolated home (junctions to provider config dirs) is
    /// the supervisor's job.
    pub fn env_overrides(&self) -> Vec<(&'static str, PathBuf)> {
        self.isolated_home
            .as_ref()
            .map(|home| vec![("USERPROFILE", home.clone()), ("HOME", home.clone())])
            .unwrap_or_default()
    }
}

/// `usage` object of the claude result contract (handoff §3.1). All fields
/// optional — provider schemas drift, parsing stays total.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaudeUsage {
    /// Prompt tokens.
    pub input_tokens: Option<u64>,
    /// Completion tokens (thinking tokens are a subset of these).
    pub output_tokens: Option<u64>,
    /// Cache-hit read tokens (the ~22k fixed preamble bills here).
    pub cache_read_input_tokens: Option<u64>,
    /// Cache-write tokens (`cache_creation.ephemeral_*` sum observed equal).
    pub cache_creation_input_tokens: Option<u64>,
    /// Completion-token breakdown.
    pub output_tokens_details: Option<ClaudeOutputTokensDetails>,
    /// Server-side tool request counts (SEC-02 network-compliance
    /// telemetry).
    pub server_tool_use: Option<ClaudeServerToolUse>,
}

/// `output_tokens_details` of the claude `usage` object.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaudeOutputTokensDetails {
    /// Reasoning tokens (subset of `output_tokens`).
    pub thinking_tokens: Option<u64>,
}

/// `server_tool_use` of the claude `usage` object.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaudeServerToolUse {
    /// WebSearch requests billed server-side.
    pub web_search_requests: Option<u64>,
    /// WebFetch requests billed server-side.
    pub web_fetch_requests: Option<u64>,
}

/// One row of the `modelUsage` **map** (keyed by model name — observed keys
/// `claude-sonnet-5` main + `claude-haiku-4-5-20251001` auxiliary on a
/// trivial run).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ClaudeModelRow {
    /// Prompt tokens for this model.
    pub input_tokens: Option<u64>,
    /// Completion tokens for this model.
    pub output_tokens: Option<u64>,
    /// Cache-hit read tokens for this model.
    pub cache_read_input_tokens: Option<u64>,
    /// Cache-write tokens for this model.
    pub cache_creation_input_tokens: Option<u64>,
    /// Server-side web requests for this model.
    pub web_search_requests: Option<u64>,
    /// Cost attributable to this model (wire key `costUSD`).
    #[serde(rename = "costUSD")]
    pub cost_usd: Option<f64>,
    /// Context window in tokens (sonnet-5 observed at 1M).
    pub context_window: Option<u64>,
    /// Max output tokens (sonnet-5 observed at 64k).
    pub max_output_tokens: Option<u64>,
    /// Canonical model id (e.g. `claude-haiku-4-5`).
    pub canonical_model: Option<String>,
    /// `firstParty` observed.
    pub provider: Option<String>,
}

/// The claude `result` event payload — the authoritative end-of-run object
/// (handoff §3.1 schema). Fields the adapter does not consume
/// (`duration_ms`, `num_turns`, `stop_reason`, `terminal_reason`, ttft
/// timings, fast-mode state) are tolerated and ignored rather than pinned.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClaudeResult {
    /// `success` observed; error runs carry other subtypes.
    pub subtype: Option<String>,
    /// Payload-level error flag — an additional success gate beyond the
    /// F-00 §4 exit-code + final-event conjunction.
    pub is_error: Option<bool>,
    /// Typed HTTP status surfaced by typed provider errors (401/403 →
    /// auth, 429 → transient via the F-02 Classifier ladder). `null` on
    /// success.
    pub api_error_status: Option<i64>,
    /// Provider session id — the `--resume` handle.
    pub session_id: Option<String>,
    /// Final result text (the F-02 `Finished.final_result` source).
    pub result: Option<String>,
    /// Total monetary cost. The ONLY cost field that exists — `cost_usd`
    /// does not (handoff §3.1).
    pub total_cost_usd: Option<f64>,
    /// Token usage aggregate.
    pub usage: Option<ClaudeUsage>,
    /// Per-model cost rows, keyed by model id.
    #[serde(rename = "modelUsage")]
    pub model_usage: BTreeMap<String, ClaudeModelRow>,
    /// Observable permission denials (empty on every battery run; a
    /// non-empty list is the F-02 Classifier's denial signal →
    /// [`AdapterFailure::PolicyDenial`]). Shape unpinned — carried
    /// verbatim.
    pub permission_denials: Vec<Value>,
    /// Provider-internal subagent counters (`spawned`, `max_depth`,
    /// `refused.budget`, `killed`, ...) — captured for the supervisor's
    /// depth/budget guards, which must sit ABOVE provider-internal
    /// subagents (handoff §3.1).
    pub subagent_stats: Option<Value>,
}

impl ClaudeResult {
    /// Whether an observable permission denial occurred (structured payload
    /// content, never stderr scraping).
    pub fn denial_observed(&self) -> bool {
        !self.permission_denials.is_empty()
    }

    /// Map the result payload onto the F-02 ledger snapshot.
    ///
    /// - `cost_usd` ← `total_cost_usd` (the only existing cost field).
    /// - `per_model` ← `modelUsage` rows (`costUSD`, `contextWindow`);
    ///   BTreeMap ordering keeps the vector deterministic.
    /// - `total_tokens` ← sum of input + output + cache_read +
    ///   cache_creation (claude reports no total field; F-02's
    ///   `UsageSnapshot` has no cache-creation slot, so it is folded into
    ///   the total only — documented deviation).
    /// - `session_overhead_tokens` ← the ~22k core-prompt constant (F-00
    ///   §4 / claude T5).
    pub fn usage_snapshot(&self) -> UsageSnapshot {
        let usage = self.usage.unwrap_or_default();
        let input = usage.input_tokens.unwrap_or(0);
        let output = usage.output_tokens.unwrap_or(0);
        let thinking = usage
            .output_tokens_details
            .and_then(|details| details.thinking_tokens)
            .unwrap_or(0);
        let cache_read = usage.cache_read_input_tokens.unwrap_or(0);
        let cache_creation = usage.cache_creation_input_tokens.unwrap_or(0);
        let per_model = self
            .model_usage
            .iter()
            .map(|(model, row)| ModelUsageRow {
                model: model.clone(),
                cost_usd: row.cost_usd,
                context_window: row.context_window,
            })
            .collect();
        UsageSnapshot {
            input_tokens: input,
            output_tokens: output,
            thinking_tokens: thinking,
            cache_read_tokens: cache_read,
            total_tokens: input + output + cache_read + cache_creation,
            cost_usd: self.total_cost_usd,
            per_model,
            session_overhead_tokens: CLAUDE_SESSION_OVERHEAD_TOKENS,
        }
    }
}

/// Terminal event for a finished headless run from the machine-reliable
/// signals (exit code + result payload) plus payload-content hooks.
///
/// Carry the CLI's own account of the failure into the classified error.
///
/// The classifier decides *what kind* of failure this is from exit code and
/// typed provider codes, which is right — but it never sees the result
/// payload, so its detail says only "final result event present but exit code
/// 1". That sentence is true and useless: the reason the run failed is sitting
/// in `result.result` and was being dropped on the floor, leaving the phase
/// review with "the author returned an empty response" and no way to tell a
/// crash from a refusal without probing the CLI by hand.
///
/// The `is_error` branch above already keeps that text; this makes the
/// classified path keep it too.
fn with_result_context(failure: AdapterFailure, result: &ClaudeResult) -> AdapterFailure {
    let Some(text) = result
        .result
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
    else {
        return failure;
    };
    let context = format!(" | claude subtype {:?}: {text}", result.subtype);
    let extend = |detail: String| format!("{detail}{context}");
    match failure {
        AdapterFailure::AuthFailure { detail } => AdapterFailure::AuthFailure {
            detail: extend(detail),
        },
        AdapterFailure::BillingFailure {
            provider_code,
            detail,
        } => AdapterFailure::BillingFailure {
            provider_code,
            detail: extend(detail),
        },
        AdapterFailure::BotGateOrCaptcha {
            provider_code,
            detail,
        } => AdapterFailure::BotGateOrCaptcha {
            provider_code,
            detail: extend(detail),
        },
        AdapterFailure::PolicyDenial { detail } => AdapterFailure::PolicyDenial {
            detail: extend(detail),
        },
        AdapterFailure::Transient { detail } => AdapterFailure::Transient {
            detail: extend(detail),
        },
        AdapterFailure::TaskFailure { detail } => AdapterFailure::TaskFailure {
            detail: extend(detail),
        },
        AdapterFailure::SpawnFailure { detail } => AdapterFailure::SpawnFailure {
            detail: extend(detail),
        },
    }
}

/// Success is the F-00 §4 conjunction (exit 0 AND final result event)
/// *plus* claude's own `is_error == false` — a result event can be present
/// with exit 0 while the payload reports an error, and that is a failed
/// run.
fn terminal_event(exit_code: i32, result: Option<&ClaudeResult>) -> AdapterEvent {
    let Some(result) = result else {
        // Pre-model death: the observed variadic-flag misuse shape
        // (exit 1, zero events, ~3s).
        return AdapterEvent::Failed(Classifier::classify(exit_code, false, None, false));
    };
    if exit_code == 0 && !result.is_error.unwrap_or(false) {
        return AdapterEvent::Finished {
            exit_code,
            final_result: result.result.clone(),
            structured: None,
        };
    }
    let provider_code = result.api_error_status;
    let denial_observed = result.denial_observed();
    if provider_code.is_none() && !denial_observed && result.is_error.unwrap_or(false) {
        // is_error without a stronger typed signal: the model round-tripped
        // and the run failed on its own merits.
        return AdapterEvent::Failed(AdapterFailure::TaskFailure {
            detail: format!(
                "claude result reports is_error (subtype {:?}): {}",
                result.subtype,
                result.result.as_deref().unwrap_or("<no result text>")
            ),
        });
    }
    AdapterEvent::Failed(with_result_context(
        Classifier::classify(exit_code, true, provider_code, denial_observed),
        result,
    ))
}

/// Incremental stream-json parser (pure). Feed it stdout lines as they
/// arrive; each maps to zero or more [`AdapterEvent`]s. The final `result`
/// payload is stashed; the terminal event is deferred until the process
/// exit code is known ([`ClaudeStreamReducer::finish`]).
#[derive(Debug, Default)]
pub struct ClaudeStreamReducer {
    session_id: Option<String>,
    model: Option<String>,
    result: Option<ClaudeResult>,
}

impl ClaudeStreamReducer {
    /// Ingest one stdout line. Unknown event types, blank lines, non-JSON
    /// noise, and unconsumed block kinds are skipped (tolerant parsing,
    /// F-00 §4.2: provider schemas drift) — never an error.
    pub fn push_line(&mut self, line: &str) -> Vec<AdapterEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            tracing::debug!(line = %trimmed, "claude adapter: skipping non-JSON stream line");
            return Vec::new();
        };
        let kind = value.get("type").and_then(Value::as_str);
        match kind {
            Some("system") => self.system_event(&value),
            Some("rate_limit_event") => vec![AdapterEvent::RateLimit {
                provider_notice: value
                    .get("rate_limit_info")
                    .cloned()
                    .unwrap_or_else(|| value.clone()),
            }],
            Some("assistant") => assistant_events(&value),
            Some("user") => {
                // Tool results have no F-02 event variant; skipped (the
                // ToolUse event already carried the invocation).
                tracing::debug!(target: "agentos_adapters::claude::stream",
                    "claude adapter: skipping user/tool_result event");
                Vec::new()
            }
            Some("result") => match parse_as::<ClaudeResult>(&value) {
                Some(result) => {
                    // Usage precedes the (deferred) terminal event: the
                    // ledger sees cost before the run closes.
                    let events = vec![AdapterEvent::UsageUpdate(result.usage_snapshot())];
                    if let Some(id) = &result.session_id {
                        self.session_id = Some(id.clone());
                    }
                    self.result = Some(result);
                    events
                }
                None => {
                    tracing::debug!("claude adapter: result event failed schema parse; skipping");
                    Vec::new()
                }
            },
            other => {
                tracing::debug!(kind = ?other, "claude adapter: skipping unknown stream event");
                Vec::new()
            }
        }
    }

    /// `system` events: only `subtype=init` is the init event (multiple
    /// `system` subtypes occur per run — `thinking_tokens` observed twice
    /// in T2/T4); everything else is skipped.
    fn system_event(&mut self, value: &Value) -> Vec<AdapterEvent> {
        if value.get("subtype").and_then(Value::as_str) != Some("init") {
            tracing::debug!(target: "agentos_adapters::claude::stream",
                subtype = ?value.get("subtype"),
                "claude adapter: skipping non-init system event");
            return Vec::new();
        }
        let session_id = string_field(value, "session_id");
        let model = string_field(value, "model");
        let event = AdapterEvent::Started {
            session_id: session_id.clone().unwrap_or_else(|| "unknown".to_owned()),
            model: model.clone(),
        };
        if session_id.is_some() {
            self.session_id = session_id;
        }
        if model.is_some() {
            self.model = model;
        }
        vec![event]
    }

    /// The provider session id learned so far (init first, else result).
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// The model learned from the init event.
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// The final result payload, when a `result` event was seen.
    pub fn result(&self) -> Option<&ClaudeResult> {
        self.result.as_ref()
    }

    /// Terminal event once the process exit code is known.
    pub fn finish(&self, exit_code: i32) -> AdapterEvent {
        terminal_event(exit_code, self.result.as_ref())
    }
}

/// Map an `assistant` event's content blocks: `text` →
/// [`AdapterEvent::TextDelta`], `tool_use` → [`AdapterEvent::ToolUse`];
/// `thinking` blocks are skipped (not assistant output). Tolerates missing
/// `message`/`content` and non-array content.
fn assistant_events(value: &Value) -> Vec<AdapterEvent> {
    let Some(blocks) = value
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        tracing::debug!("claude adapter: assistant event without message.content; skipping");
        return Vec::new();
    };
    let mut events = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = string_field(block, "text") {
                    if !text.is_empty() {
                        events.push(AdapterEvent::TextDelta(text));
                    }
                }
            }
            Some("tool_use") => {
                let tool = string_field(block, "name").unwrap_or_else(|| "unknown".to_owned());
                let input = block.get("input").cloned().unwrap_or(Value::Null);
                events.push(AdapterEvent::ToolUse {
                    tool: tool.clone(),
                    args_summary: summarize_tool_input(&input),
                });
                events.extend(crate::decision::from_tool_input(&tool, &input));
            }
            other => {
                tracing::debug!(block_type = ?other,
                    "claude adapter: skipping unconsumed assistant block");
            }
        }
    }
    events
}

/// Compact human-readable summary of a tool_use input: up to three
/// `key=value` entries (values truncated), never full payloads (F-00 §3:
/// large payloads belong in content-addressed artifacts).
fn summarize_tool_input(input: &Value) -> String {
    match input {
        Value::Object(map) if map.is_empty() => "(no arguments)".to_owned(),
        Value::Object(map) => {
            let parts: Vec<String> = map
                .iter()
                .take(3)
                .map(|(key, value)| {
                    let text = match value {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    format!("{key}={}", truncate(&text, TOOL_SUMMARY_VALUE_MAX))
                })
                .collect();
            truncate(&parts.join(", "), TOOL_SUMMARY_MAX)
        }
        other => truncate(&other.to_string(), TOOL_SUMMARY_MAX),
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

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn parse_as<T: serde::de::DeserializeOwned>(value: &Value) -> Option<T> {
    serde_json::from_value(value.clone()).ok()
}

// ---------------------------------------------------------------------------
// Live session plumbing
// ---------------------------------------------------------------------------

/// Shared mutable state of one claude session.
struct ClaudeSessionState {
    /// Adapter-internal handle id (`claude-<n>`); the *provider* session id
    /// is learned at runtime (init/result) and kept separately.
    handle_id: String,
    /// Resolved claude binary (recorded at session start).
    binary: PathBuf,
    /// The invocation template of the first run; follow-up instructions
    /// clone it and swap objective + `--resume`.
    base_invocation: ClaudeInvocation,
    /// Provider session id once learned (enables resume).
    session_id: Mutex<Option<String>>,
    /// The live child, parked here so `cancel` can tree-kill it.
    child: tokio::sync::Mutex<Option<Child>>,
    /// Cancel requested — stream goes quiet, no terminal event
    /// (process-kill semantics).
    cancelled: AtomicBool,
    /// Session permanently over (a `Failed` terminal or cancel).
    dead: AtomicBool,
    /// A headless run is in flight (one at a time).
    run_active: AtomicBool,
}

/// [`SessionBackend`] for one claude session.
struct ClaudeSessionBackend {
    state: std::sync::Arc<ClaudeSessionState>,
    events: broadcast::Sender<AdapterEvent>,
}

#[async_trait]
impl SessionBackend for ClaudeSessionBackend {
    /// Deliver one instruction to the session.
    ///
    /// Semantics (documented F-03 deviation, mirroring the agy sibling):
    /// headless `-p` runs consume stdin exactly once (the prompt, T3), so
    /// an instruction is delivered as a **new headless run resuming the
    /// provider session** (`--resume <id>`, the BANANA42-verified path)
    /// with the instruction as its stdin prompt. The resumed run's events —
    /// including its own terminal event — stream on the same session
    /// channel. `Finished` means *turn complete*: the session stays
    /// instructable until it `Failed` or is cancelled; instructions on
    /// dead sessions error with [`AdapterError::SessionNotActive`].
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
        let session_id = state
            .session_id
            .lock()
            .expect("claude session state poisoned")
            .clone();
        let Some(session_id) = session_id else {
            state.run_active.store(false, Ordering::Relaxed);
            return Err(AdapterError::Internal(
                "no claude session id known yet; wait for the first run's \
                 init/result event"
                    .to_owned(),
            ));
        };
        let mut invocation = state.base_invocation.clone();
        invocation.objective = text;
        invocation.resume_session_id = Some(session_id);
        match claude_command(&state.binary, &invocation).spawn() {
            Ok(child) => {
                let state = state.clone();
                let events = self.events.clone();
                tokio::spawn(async move {
                    drive_headless_run(state, invocation, events, child).await;
                });
                Ok(())
            }
            Err(error) => {
                state.run_active.store(false, Ordering::Relaxed);
                state.dead.store(true, Ordering::Relaxed);
                let _ = self
                    .events
                    .send(AdapterEvent::Failed(AdapterFailure::SpawnFailure {
                        detail: format!("failed to spawn resumed claude run: {error}"),
                    }));
                Ok(())
            }
        }
    }

    /// Cancel: process-kill semantics. The stream ends without a terminal
    /// event, exactly as a killed CLI behaves. The process *tree* is killed
    /// (see [`kill_child`]) because claude v2 spawns provider-internal
    /// subagents.
    async fn cancel(&self) -> Result<(), AdapterError> {
        self.state.cancelled.store(true, Ordering::Relaxed);
        self.state.dead.store(true, Ordering::Relaxed);
        if let Some(mut child) = self.state.child.lock().await.take() {
            kill_child(&mut child).await;
        }
        Ok(())
    }
}

/// The live Claude Code adapter.
///
/// Discovery (`detect`/`auth_status`/`capabilities`/`health`) is free and
/// makes no billable calls. The adapter never reads, stores, or exports
/// credentials — auth state is a file-existence check.
pub struct ClaudeAdapter {
    next_session: AtomicUsize,
    sessions: Mutex<Vec<SessionHandle>>,
}

impl ClaudeAdapter {
    /// Create the adapter.
    pub fn new() -> Self {
        Self {
            next_session: AtomicUsize::new(0),
            sessions: Mutex::new(Vec::new()),
        }
    }
    /// Spawn one session from a ready invocation. Shared by
    /// `start_session` (fresh) and `resume_session` (`--resume <session_id>`).
    async fn launch(&self, invocation: ClaudeInvocation) -> Result<SessionHandle, AdapterError> {
        let binary = resolve_binary().ok_or_else(|| {
            AdapterError::Internal(format!(
                "claude binary not found (checked ${}, %USERPROFILE%\\.local\\bin\\claude.exe, PATH)",
                CLAUDE_BIN_ENV
            ))
        })?;
        // Spawn synchronously so machinery failures (bad cwd, missing
        // binary) are `Result` errors, per the F-02 contract.
        let child = claude_command(&binary, &invocation)
            .spawn()
            .map_err(|error| {
                AdapterError::Internal(format!(
                    "failed to spawn claude in {}: {error}",
                    invocation.workspace.display()
                ))
            })?;

        let session_no = self.next_session.fetch_add(1, Ordering::Relaxed) + 1;
        let handle_id = format!("claude-{session_no}");
        let (events, _) = broadcast::channel(1024);
        let state = std::sync::Arc::new(ClaudeSessionState {
            handle_id: handle_id.clone(),
            binary,
            base_invocation: invocation.clone(),
            session_id: Mutex::new(None),
            child: tokio::sync::Mutex::new(None),
            cancelled: AtomicBool::new(false),
            dead: AtomicBool::new(false),
            run_active: AtomicBool::new(true),
        });
        let backend = std::sync::Arc::new(ClaudeSessionBackend {
            state: state.clone(),
            events: events.clone(),
        });
        let handle = SessionHandle::new(handle_id, events.clone(), backend);
        self.sessions
            .lock()
            .expect("claude session registry poisoned")
            .push(handle.clone());

        let driver_state = state;
        let driver_invocation = invocation;
        let driver_events = events;
        tokio::spawn(async move {
            drive_headless_run(driver_state, driver_invocation, driver_events, child).await;
        });
        Ok(handle)
    }
}

impl Default for ClaudeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RuntimeAdapter for ClaudeAdapter {
    fn id(&self) -> &str {
        CLAUDE_ADAPTER_ID
    }

    async fn detect(&self) -> RuntimeInfo {
        let path = resolve_binary();
        let version = match &path {
            Some(binary) => probe_version(binary).await,
            None => None,
        };
        RuntimeInfo {
            id: CLAUDE_ADAPTER_ID.to_owned(),
            version,
            path,
        }
    }

    /// Free tier-0 check only (F-00 §5): binary presence plus `~/.claude`
    /// and the credentials file (`~/.claude/.credentials.json`, present on
    /// the observed install). Existence only — the file is never opened.
    async fn auth_status(&self) -> AuthStatus {
        if resolve_binary().is_none() {
            return AuthStatus::Unknown;
        }
        match claude_home() {
            Some(home) if home.is_dir() => {
                if home.join(CLAUDE_CREDENTIALS_FILE).is_file() {
                    AuthStatus::Ready
                } else {
                    AuthStatus::NeedsLogin
                }
            }
            _ => AuthStatus::NeedsLogin,
        }
    }

    async fn capabilities(&self) -> Capabilities {
        claude_capabilities()
    }

    async fn start_session(&self, spec: SpawnSpec) -> Result<SessionHandle, AdapterError> {
        self.launch(ClaudeInvocation::from_spec(&spec)).await
    }

    /// Continue an earlier provider conversation: same invocation,
    /// plus `--resume <session_id>`.
    async fn resume_session(
        &self,
        spec: SpawnSpec,
        provider_session_id: String,
    ) -> Result<SessionHandle, AdapterError> {
        self.launch(ClaudeInvocation::from_spec(&spec).with_resume_session(provider_session_id))
            .await
    }

    async fn shutdown(&self) -> Result<(), AdapterError> {
        let sessions: Vec<SessionHandle> = self
            .sessions
            .lock()
            .expect("claude session registry poisoned")
            .clone();
        for handle in &sessions {
            // Best-effort, idempotent: cancel is process-kill semantics.
            let _ = handle.cancel().await;
        }
        Ok(())
    }
}

/// Capability surface, each flag annotated with its observed basis.
fn claude_capabilities() -> Capabilities {
    Capabilities {
        // acceptEdits file write verified (probe.txt created exactly).
        filesystem_edit: true,
        // Bash tool on native Windows verified (T4: bash_tool_use=1,
        // output seen); policy-gated via allow/deny lists.
        shell: true,
        // WebFetch/WebSearch in the init tool inventory (T2 denied them —
        // which proves they exist and are enforceable).
        network: true,
        // v2.1.238 surfaces no --output-schema flag and the battery never
        // exercised structured output — false until smoke-tested.
        structured_output: false,
        // --resume <session_id> round-trip verified (BANANA42 codeword).
        resume: true,
        // claude model family accepts image input (model-family inference,
        // not exercised by the battery — see F-doc).
        multimodal: true,
        // modelUsage rows show contextWindow 1M for claude-sonnet-5.
        long_context: true,
        // init event carries mcp_servers (live entries observed connected).
        mcp_client: true,
    }
}

/// Resolve the claude binary: `$AGENTOS_CLAUDE_BIN`, then the observed
/// canonical install `%USERPROFILE%\.local\bin\claude.exe`, then a PATH
/// scan. Free.
fn resolve_binary() -> Option<PathBuf> {
    if let Some(override_path) = std::env::var_os(CLAUDE_BIN_ENV) {
        if !override_path.is_empty() {
            return Some(PathBuf::from(override_path));
        }
    }
    if let Some(home) = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME")) {
        for name in EXECUTABLE_NAMES {
            let canonical = PathBuf::from(&home).join(".local").join("bin").join(name);
            if canonical.is_file() {
                return Some(canonical);
            }
        }
    }
    find_on_path()
}

#[cfg(windows)]
const EXECUTABLE_NAMES: [&str; 2] = ["claude.exe", "claude"];

#[cfg(not(windows))]
const EXECUTABLE_NAMES: [&str; 1] = ["claude"];

fn find_on_path() -> Option<PathBuf> {
    let path_env = std::env::var_os("PATH")?;
    std::env::split_paths(&path_env).find_map(|dir| {
        EXECUTABLE_NAMES
            .iter()
            .map(|name| dir.join(name))
            .find(|candidate| candidate.is_file())
    })
}

/// The claude config dir (`~/.claude`) — the root of the free auth
/// existence check.
fn claude_home() -> Option<PathBuf> {
    let home = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"))?;
    Some(PathBuf::from(home).join(".claude"))
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

/// Assemble the child command for one headless run. Argv-vector only — no
/// shell, no string quoting (F-03 hard rule). stdin is piped exactly when
/// the prompt rides it, so EOF after the prompt write is delivered.
fn claude_command(binary: &Path, invocation: &ClaudeInvocation) -> Command {
    let mut command = Command::new(binary);
    command
        .args(invocation.args())
        .current_dir(&invocation.workspace)
        .stdin(if invocation.prompt_via_stdin {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for (key, value) in invocation.env_overrides() {
        command.env(key, value);
    }
    command
}

/// Kill the claude child process.
///
/// Windows: tree-kill via `taskkill /PID <pid> /T /F` first (handoff §7.7)
/// because claude v2 spawns provider-internal subagents (`subagent_stats`
/// proves them); a bare `TerminateProcess` on the direct child would
/// orphan them. Then the direct kill reaps regardless of taskkill's
/// outcome.
async fn kill_child(child: &mut Child) {
    #[cfg(windows)]
    if let Some(pid) = child.id() {
        let tree_kill = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .output()
            .await;
        match &tree_kill {
            Ok(output) if output.status.success() => {}
            other => tracing::warn!(?other, pid, "claude adapter: taskkill tree-kill failed"),
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

/// Harness-side timeout classification (timeout → Transient, the only
/// retryable class — rate-limit windows and hangs clear on their own).
fn harness_timeout_failure(timeout_secs: u64) -> AdapterFailure {
    AdapterFailure::Transient {
        detail: format!(
            "claude headless run exceeded the {timeout_secs}s harness budget; process killed"
        ),
    }
}

/// Deliver the stdin prompt (T3 canon): write the objective bytes, flush,
/// then drop the handle so the child sees EOF and starts its turn.
async fn deliver_stdin_prompt(stdin: ChildStdin, prompt: &str) {
    let mut stdin = stdin;
    if let Err(error) = stdin.write_all(prompt.as_bytes()).await {
        tracing::warn!(%error, "claude adapter: stdin prompt write failed");
    }
    if let Err(error) = stdin.shutdown().await {
        tracing::warn!(%error, "claude adapter: stdin flush failed");
    }
}

async fn kill_stored_child(state: &ClaudeSessionState) {
    if let Some(mut child) = state.child.lock().await.take() {
        kill_child(&mut child).await;
    }
}

/// Reap the child and return its exit code. `None` child means cancel()
/// already took it to kill it — callers check the cancelled flag and
/// suppress the terminal event.
async fn take_exit_code(state: &ClaudeSessionState) -> i32 {
    match state.child.lock().await.take() {
        Some(mut child) => match child.wait().await {
            Ok(status) => status.code().unwrap_or(-1),
            Err(error) => {
                tracing::warn!(%error, "claude adapter: failed to reap child");
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
        tracing::debug!(target: "agentos_adapters::claude::stderr", line = %line);
    }
}

/// Consume stream-json stdout, forwarding mapped events until EOF or
/// cancellation.
async fn read_stream(
    stdout: ChildStdout,
    state: &ClaudeSessionState,
    events: &broadcast::Sender<AdapterEvent>,
) -> ClaudeStreamReducer {
    let mut reducer = ClaudeStreamReducer::default();
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

/// Drive one headless run to its terminal event. The caller marks
/// `run_active` before spawning; this clears it and records the provider
/// session id (enabling `--resume` instructions). Cancelled runs end
/// silently (no terminal event).
async fn drive_headless_run(
    state: std::sync::Arc<ClaudeSessionState>,
    invocation: ClaudeInvocation,
    events: broadcast::Sender<AdapterEvent>,
    mut child: Child,
) {
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    *state.child.lock().await = Some(child);
    if let Some(stderr) = stderr {
        tokio::spawn(drain_stderr(stderr));
    }
    if let Some(stdin) = stdin {
        // prompt_via_stdin is the default; argv delivery leaves stdin null.
        deliver_stdin_prompt(stdin, &invocation.objective).await;
    }

    let run = async {
        let reducer = match stdout {
            Some(stdout) => read_stream(stdout, &state, &events).await,
            None => ClaudeStreamReducer::default(),
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
        // Process-kill semantics: the stream just ends.
        return;
    }
    if let Some(id) = reducer.session_id().map(str::to_owned) {
        let mut guard = state
            .session_id
            .lock()
            .expect("claude session state poisoned");
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
// Tests — all offline. Real-transcript coverage runs against the frozen
// fixture corpus (handoff §6.4: fixtures ARE the adapter test corpus);
// edge cases are synthetic. No test spawns the real binary or makes any
// billable call.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    // -- helpers ---------------------------------------------------------------

    fn spec(objective: &str, allowlist: Vec<&str>, denylist: Vec<&str>) -> SpawnSpec {
        SpawnSpec {
            task_id: Uuid::new_v4(),
            objective: objective.to_owned(),
            workspace: PathBuf::from("worktrees/task-claude"),
            allowed_paths: vec![],
            forbidden_paths: vec![],
            tool_allowlist: allowlist.into_iter().map(str::to_owned).collect(),
            tool_denylist: denylist.into_iter().map(str::to_owned).collect(),
            model: None,
            timeout_secs: 600,
            isolated_home: None,
        }
    }

    /// The frozen corpus directory (env override, else the sibling
    /// cli-fix-output tree). `None` → corpus unavailable; fixture tests
    /// skip loudly rather than fail on machines without it.
    fn fixture_dir() -> Option<PathBuf> {
        if let Some(env_dir) = std::env::var_os(CLAUDE_FIXTURES_ENV) {
            let dir = PathBuf::from(env_dir);
            assert!(
                dir.is_dir(),
                "${} points at a missing directory",
                CLAUDE_FIXTURES_ENV
            );
            return Some(dir);
        }
        let default = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .join("cli-fix-output/2026-08-22T06-30-51-908Z/fixtures");
        default.is_dir().then_some(default)
    }

    fn fixture(name: &str) -> Option<String> {
        let dir = fixture_dir()?;
        std::fs::read_to_string(dir.join(name)).ok()
    }

    fn synth_result(
        is_error: Option<bool>,
        api_error_status: Option<i64>,
        denials: Vec<Value>,
    ) -> ClaudeResult {
        ClaudeResult {
            subtype: Some("error_during_processing".to_owned()),
            is_error,
            api_error_status,
            session_id: Some("6601ef88-5af5-4a77-8d4c-dec765ee34d9".to_owned()),
            result: Some("partial output".to_owned()),
            total_cost_usd: Some(0.01),
            usage: Some(ClaudeUsage::default()),
            model_usage: BTreeMap::new(),
            permission_denials: denials,
            subagent_stats: None,
        }
    }

    // -- discovery surface -------------------------------------------------

    #[tokio::test]
    async fn adapter_id_and_capability_surface_reflect_observed_claude() {
        let adapter: Box<dyn RuntimeAdapter> = Box::new(ClaudeAdapter::new());
        assert_eq!(adapter.id(), "claude-code");

        let caps = adapter.capabilities().await;
        // Verified: acceptEdits file write (probe.txt), Bash on native
        // Windows (T4), --resume round-trip (BANANA42), web tools in the
        // inventory (T2 denied them), 1M contextWindow (modelUsage),
        // mcp_servers in init.
        assert!(caps.filesystem_edit && caps.shell && caps.network && caps.resume);
        assert!(caps.long_context && caps.mcp_client);
        // Structured output: no flag in v2.1.238 help, battery never
        // exercised one — false until smoke-tested.
        assert!(!caps.structured_output);
    }

    // -- arg builder ---------------------------------------------------------

    #[test]
    fn arg_builder_headless_base_is_stdin_prompted_stream_json() {
        let invocation = ClaudeInvocation::from_spec(&spec(
            "a very long task contract that would blow the Windows cmdline limit",
            vec![],
            vec![],
        ));
        // Empty tool lists omit the flags entirely; the prompt is NOT on
        // argv (stdin delivery); no positional element exists.
        assert_eq!(
            invocation.args(),
            vec![
                "-p",
                "--output-format",
                "stream-json",
                "--verbose",
                "--permission-mode",
                "acceptEdits",
            ]
        );
        assert!(invocation.prompt_via_stdin);
    }

    #[test]
    fn arg_builder_renders_tool_lists_in_equals_form() {
        let invocation = ClaudeInvocation::from_spec(&spec(
            "probe",
            vec!["Bash(echo:*)", "Edit"],
            vec!["Bash", "WebFetch", "WebSearch"],
        ));
        let args = invocation.args();
        // T4/T2 canon: single argv elements in equals form.
        assert!(args.contains(&"--allowedTools=Bash(echo:*),Edit".to_owned()));
        assert!(args.contains(&"--disallowedTools=Bash,WebFetch,WebSearch".to_owned()));
        // The separate-value form must never appear (variadic root cause).
        assert!(!args.iter().any(|arg| arg == "--allowedTools"));
        assert!(!args.iter().any(|arg| arg == "--disallowedTools"));
    }

    #[test]
    fn arg_builder_omits_empty_tool_lists() {
        let allow_only = ClaudeInvocation::from_spec(&spec("p", vec!["Read"], vec![])).args();
        assert!(allow_only.contains(&"--allowedTools=Read".to_owned()));
        assert!(!allow_only
            .iter()
            .any(|arg| arg.starts_with("--disallowedTools")));

        let deny_only = ClaudeInvocation::from_spec(&spec("p", vec![], vec!["WebFetch"])).args();
        assert!(deny_only.contains(&"--disallowedTools=WebFetch".to_owned()));
        assert!(!deny_only
            .iter()
            .any(|arg| arg.starts_with("--allowedTools")));
    }

    #[test]
    fn arg_builder_passes_model_resume_and_undocumented_max_turns() {
        let invocation = ClaudeInvocation::from_spec(&spec("do the thing", vec![], vec![]))
            .with_model("claude-opus-5")
            .with_resume_session("1f1aec91-731f-481b-93ec-f82cd79e6f63")
            .with_max_turns(6);
        let args = invocation.args();
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--model" && w[1] == "claude-opus-5"));
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--resume" && w[1] == "1f1aec91-731f-481b-93ec-f82cd79e6f63"));
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--max-turns" && w[1] == "6"));
    }

    #[test]
    fn arg_builder_argv_prompt_rides_last_behind_equals_form_flags() {
        let invocation = ClaudeInvocation::from_spec(&spec(
            "fix the flaky test -- and run it \"twice\"",
            vec!["Bash(echo:*)"],
            vec!["WebFetch"],
        ))
        .with_argv_prompt();
        let args = invocation.args();
        assert_eq!(
            args.last(),
            Some(&"fix the flaky test -- and run it \"twice\"".to_owned()),
            "argv prompt must be the LAST element (T1 shape)"
        );
        // Tool flags stay single equals-form elements directly before it.
        assert!(args.contains(&"--allowedTools=Bash(echo:*)".to_owned()));
        assert!(args.contains(&"--disallowedTools=WebFetch".to_owned()));
    }

    #[test]
    fn arg_builder_permission_mode_default_omits_the_flag() {
        let invocation = ClaudeInvocation::from_spec(&spec("analyze", vec![], vec![]))
            .with_permission_mode(ClaudePermissionMode::Default);
        let args = invocation.args();
        assert!(!args.iter().any(|arg| arg == "--permission-mode"));
        assert_eq!(
            args,
            vec!["-p", "--output-format", "stream-json", "--verbose"]
        );

        let plan = ClaudeInvocation::from_spec(&spec("plan only", vec![], vec![]))
            .with_permission_mode(ClaudePermissionMode::Plan)
            .args();
        assert!(plan
            .windows(2)
            .any(|w| w[0] == "--permission-mode" && w[1] == "plan"));
    }

    #[test]
    fn arg_builder_full_vector_in_canonical_order() {
        let invocation = ClaudeInvocation {
            objective: "objective text".to_owned(),
            workspace: PathBuf::from("wt"),
            prompt_via_stdin: false,
            permission_mode: ClaudePermissionMode::AcceptEdits,
            tool_allowlist: vec!["Bash(echo:*)".to_owned()],
            tool_denylist: vec![],
            model: Some("claude-opus-5".to_owned()),
            resume_session_id: Some("1f1aec91-731f-481b-93ec-f82cd79e6f63".to_owned()),
            max_turns: Some(6),
            isolated_home: None,
            timeout_secs: 600,
        };
        assert_eq!(
            invocation.args(),
            vec![
                "-p",
                "--output-format",
                "stream-json",
                "--verbose",
                "--permission-mode",
                "acceptEdits",
                "--allowedTools=Bash(echo:*)",
                "--model",
                "claude-opus-5",
                "--resume",
                "1f1aec91-731f-481b-93ec-f82cd79e6f63",
                "--max-turns",
                "6",
                "objective text",
            ]
        );
    }

    #[test]
    fn env_overrides_redirect_home_only_when_isolated() {
        let plain = ClaudeInvocation::from_spec(&spec("p", vec![], vec![]));
        assert!(plain.env_overrides().is_empty());

        let mut isolated = spec("p", vec![], vec![]);
        isolated.isolated_home = Some(PathBuf::from("temp-homes/worker-1"));
        let invocation = ClaudeInvocation::from_spec(&isolated);
        assert_eq!(
            invocation.env_overrides(),
            vec![
                ("USERPROFILE", PathBuf::from("temp-homes/worker-1")),
                ("HOME", PathBuf::from("temp-homes/worker-1")),
            ]
        );
    }

    /// Plan-mode surfaces become structured [`AdapterEvent::Decision`]s so
    /// the desktop can render real buttons: one per `AskUserQuestion`
    /// question, one for the `ExitPlanMode` plan. The `ToolUse` event still
    /// lands beside them for the timeline.
    #[test]
    fn plan_mode_tools_yield_decisions_with_options() {
        let ask = json!({
            "type": "assistant",
            "message": {"content": [{
                "type": "tool_use",
                "name": "AskUserQuestion",
                "input": {"questions": [
                    {
                        "question": "Which database?",
                        "header": "Database",
                        "multiSelect": false,
                        "options": [
                            {"label": "Postgres", "description": "relational"},
                            {"label": "SQLite", "description": "embedded"}
                        ]
                    },
                    {
                        "question": "Which extras?",
                        "multiSelect": true,
                        "options": [{"label": "Auth", "description": "sign-in"}]
                    },
                    {"question": "No options here", "options": []}
                ]}
            }]}
        });
        let events = assistant_events(&ask);
        let kinds: Vec<&str> = events.iter().map(AdapterEvent::kind).collect();
        assert_eq!(kinds, vec!["tool_use", "decision", "decision"]);
        assert_eq!(
            events[1],
            AdapterEvent::Decision {
                tool: "AskUserQuestion".to_owned(),
                prompt: "Which database?".to_owned(),
                options: vec!["Postgres".to_owned(), "SQLite".to_owned()],
                multi_select: false,
            }
        );
        match &events[2] {
            AdapterEvent::Decision {
                prompt,
                multi_select,
                ..
            } => {
                assert_eq!(prompt, "Which extras?");
                assert!(multi_select, "multiSelect carries through");
            }
            other => panic!("expected Decision, got {other:?}"),
        }

        let plan = json!({
            "type": "assistant",
            "message": {"content": [{
                "type": "tool_use",
                "name": "ExitPlanMode",
                "input": {"plan": "1. scaffold\\n2. schema"}
            }]}
        });
        let events = assistant_events(&plan);
        match &events[1] {
            AdapterEvent::Decision {
                tool,
                prompt,
                options,
                ..
            } => {
                assert_eq!(tool, "ExitPlanMode");
                assert!(prompt.contains("scaffold"));
                assert_eq!(options.len(), 2, "approve or keep planning");
            }
            other => panic!("expected Decision, got {other:?}"),
        }

        // Ordinary tools stay ordinary.
        let bash = json!({
            "type": "assistant",
            "message": {"content": [{
                "type": "tool_use", "name": "Bash", "input": {"command": "ls"}
            }]}
        });
        assert_eq!(assistant_events(&bash).len(), 1);
    }

    // -- frozen-fixture parser (T4: allowlist run with a real tool use) -----

    #[test]
    fn stream_reducer_maps_t4_allowlist_fixture_lifecycle() {
        let Some(text) = fixture("claude.T4-allowlist.stdout.jsonl") else {
            eprintln!("claude fixture corpus unavailable; skipping fixture test");
            return;
        };
        let mut reducer = ClaudeStreamReducer::default();
        let mut events = Vec::new();
        for line in text.lines() {
            events.extend(reducer.push_line(line));
        }

        // init → rate_limit → (thinking_tokens system events skipped) →
        // tool_use → tool_result user event skipped → text → result usage.
        let kinds: Vec<&str> = events.iter().map(|event| event.kind()).collect();
        assert_eq!(
            kinds,
            vec![
                "started",
                "rate_limit",
                "tool_use",
                "text_delta",
                "usage_update"
            ]
        );

        assert_eq!(
            events[0],
            AdapterEvent::Started {
                session_id: "6601ef88-5af5-4a77-8d4c-dec765ee34d9".to_owned(),
                model: Some("claude-sonnet-5".to_owned()),
            }
        );
        match &events[1] {
            AdapterEvent::RateLimit { provider_notice } => {
                assert_eq!(provider_notice["rateLimitType"], json!("seven_day"));
                assert_eq!(provider_notice["status"], json!("allowed_warning"));
                assert_eq!(provider_notice["utilization"], json!(0.85));
            }
            other => panic!("expected RateLimit, got {other:?}"),
        }
        match &events[2] {
            AdapterEvent::ToolUse { tool, args_summary } => {
                assert_eq!(tool, "Bash");
                assert!(
                    args_summary.contains("command=echo PROBE_SHELL_TEST"),
                    "compact summary: {args_summary}"
                );
            }
            other => panic!("expected ToolUse, got {other:?}"),
        }
        assert_eq!(events[3], AdapterEvent::TextDelta("DONE".to_owned()));
        match &events[4] {
            AdapterEvent::UsageUpdate(usage) => {
                assert_eq!(usage.input_tokens, 4);
                assert_eq!(usage.output_tokens, 206);
                assert_eq!(usage.thinking_tokens, 112);
                assert_eq!(usage.cache_read_tokens, 73_060);
                // No total field exists; the adapter sums the components
                // (4 + 206 + 73060 + 29089).
                assert_eq!(usage.total_tokens, 102_359);
                assert_eq!(usage.cost_usd, Some(0.200_528_000_000_000_04));
                assert_eq!(
                    usage.session_overhead_tokens,
                    CLAUDE_SESSION_OVERHEAD_TOKENS
                );
                // Two models on a trivial run: auxiliary haiku + main
                // sonnet-5 (BTreeMap order is deterministic).
                assert_eq!(usage.per_model.len(), 2);
                assert_eq!(usage.per_model[0].model, "claude-haiku-4-5-20251001");
                assert_eq!(usage.per_model[0].cost_usd, Some(0.000_974));
                assert_eq!(usage.per_model[0].context_window, Some(200_000));
                assert_eq!(usage.per_model[1].model, "claude-sonnet-5");
                assert_eq!(usage.per_model[1].cost_usd, Some(0.199_554_000_000_000_04));
                assert_eq!(usage.per_model[1].context_window, Some(1_000_000));
            }
            other => panic!("expected UsageUpdate, got {other:?}"),
        }

        // Resume handle + captured telemetry fields.
        assert_eq!(
            reducer.session_id(),
            Some("6601ef88-5af5-4a77-8d4c-dec765ee34d9")
        );
        assert_eq!(reducer.model(), Some("claude-sonnet-5"));
        let result = reducer.result().expect("result captured");
        assert_eq!(result.result.as_deref(), Some("DONE"));
        assert_eq!(result.is_error, Some(false));
        assert!(result.permission_denials.is_empty());
        let stats = result.subagent_stats.as_ref().expect("subagent_stats");
        assert_eq!(stats["max_depth"], json!(0));
        assert_eq!(stats["spawned"], json!(0));

        assert_eq!(
            reducer.finish(0),
            AdapterEvent::Finished {
                exit_code: 0,
                final_result: Some("DONE".to_owned()),
                structured: None,
            }
        );
    }

    #[test]
    fn stream_reducer_maps_t3_stdin_fixture_lifecycle() {
        let Some(text) = fixture("claude.T3-stdin-prompt.stdout.jsonl") else {
            eprintln!("claude fixture corpus unavailable; skipping fixture test");
            return;
        };
        let mut reducer = ClaudeStreamReducer::default();
        let mut events = Vec::new();
        for line in text.lines() {
            events.extend(reducer.push_line(line));
        }

        let kinds: Vec<&str> = events.iter().map(|event| event.kind()).collect();
        assert_eq!(
            kinds,
            vec!["started", "rate_limit", "text_delta", "usage_update"]
        );
        assert_eq!(
            events[0],
            AdapterEvent::Started {
                session_id: "a0921d28-8027-4b11-b457-e5827df7c528".to_owned(),
                model: Some("claude-sonnet-5".to_owned()),
            }
        );
        assert_eq!(events[2], AdapterEvent::TextDelta("PROBE_OK".to_owned()));
        match &events[3] {
            AdapterEvent::UsageUpdate(usage) => {
                assert_eq!(usage.input_tokens, 2);
                assert_eq!(usage.output_tokens, 9);
                assert_eq!(usage.thinking_tokens, 0);
                assert_eq!(usage.cache_read_tokens, 14_321);
                assert_eq!(usage.total_tokens, 43_134);
                assert_eq!(usage.cost_usd, Some(0.178_254_300_000_000_03));
                assert_eq!(usage.per_model.len(), 2);
            }
            other => panic!("expected UsageUpdate, got {other:?}"),
        }
        assert_eq!(
            reducer.finish(0),
            AdapterEvent::Finished {
                exit_code: 0,
                final_result: Some("PROBE_OK".to_owned()),
                structured: None,
            }
        );
    }

    // -- tolerant parsing ------------------------------------------------------

    #[test]
    fn stream_reducer_skips_unknown_and_malformed_lines() {
        let mut reducer = ClaudeStreamReducer::default();
        assert!(reducer.push_line("").is_empty());
        assert!(reducer.push_line("   ").is_empty());
        assert!(reducer.push_line("not json").is_empty());
        assert!(
            reducer
                .push_line(r#"{"type":"brand_new_event","data":{}}"#)
                .is_empty(),
            "schema drift must be tolerated (log + skip)"
        );
        // Multiple system events per run; only subtype=init is the init.
        assert!(reducer
            .push_line(r#"{"type":"system","subtype":"thinking_tokens","estimated_tokens":512}"#)
            .is_empty());
        // Assistant without message.content, thinking-only assistant, and
        // tool results are all tolerated without events or panics.
        assert!(reducer.push_line(r#"{"type":"assistant"}"#).is_empty());
        assert!(reducer
            .push_line(
                r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"hm"}]}}"#
            )
            .is_empty());
        assert!(reducer
            .push_line(
                r#"{"type":"user","message":{"content":[{"type":"tool_result","content":"ok"}]}}"#
            )
            .is_empty());
        // A malformed line mid-stream is not fatal: parsing continues.
        assert_eq!(
            reducer.push_line(
                r#"{"type":"assistant","message":{"content":[{"type":"text","text":"still alive"}]}}"#
            ),
            vec![AdapterEvent::TextDelta("still alive".to_owned())]
        );
        // No result event + exit 1: the observed variadic-flag death shape.
        assert!(matches!(
            reducer.finish(1),
            AdapterEvent::Failed(AdapterFailure::SpawnFailure { .. })
        ));
    }

    #[test]
    fn result_event_with_missing_fields_still_counts_as_final_event() {
        let mut reducer = ClaudeStreamReducer::default();
        let events = reducer.push_line(r#"{"type":"result"}"#);
        // Tolerant parse: zero usage snapshot, but the final event exists.
        assert_eq!(events.len(), 1);
        match &events[0] {
            AdapterEvent::UsageUpdate(usage) => {
                assert_eq!(usage.total_tokens, 0);
                assert_eq!(usage.cost_usd, None);
                assert_eq!(
                    usage.session_overhead_tokens,
                    CLAUDE_SESSION_OVERHEAD_TOKENS
                );
            }
            other => panic!("expected UsageUpdate, got {other:?}"),
        }
        assert!(matches!(
            reducer.finish(0),
            AdapterEvent::Finished {
                final_result: None,
                ..
            }
        ));
    }

    #[test]
    fn tool_input_summarizer_is_compact_and_truncated() {
        // One oversized value: truncated at the value level, join stays
        // under the overall cap, later entries survive.
        let big = "x".repeat(500);
        let input = json!({"command": big, "description": "probe", "extra": 1});
        let summary = summarize_tool_input(&input);
        assert!(summary.starts_with("command=xxx"), "{summary}");
        assert!(summary.contains("..."), "{summary}");
        assert!(summary.contains("description=probe"), "{summary}");
        assert!(summary.chars().count() <= TOOL_SUMMARY_MAX, "{summary}");

        // Three oversized values: the JOIN itself exceeds the cap and is
        // truncated with an ellipsis.
        let huge = json!({"a": "y".repeat(80), "b": "z".repeat(80), "c": "w".repeat(80)});
        let summary = summarize_tool_input(&huge);
        assert!(summary.chars().count() <= TOOL_SUMMARY_MAX + 3, "{summary}");
        assert!(summary.ends_with("..."), "{summary}");

        assert_eq!(summarize_tool_input(&json!({})), "(no arguments)");
        assert_eq!(summarize_tool_input(&Value::Null), "null");
    }

    // -- classification table -------------------------------------------------

    #[test]
    fn classification_table_for_claude_run_endings() {
        /// (exit code, result payload, expected) — `Finished` means the
        /// terminal event is success; otherwise the failure kind.
        type Case = ((i32, Option<ClaudeResult>), Expectation);
        enum Expectation {
            Finished,
            Kind(&'static str),
        }

        let denial = json!({"tool_name": "Bash", "reason": "denied"});
        let cases: Vec<Case> = vec![
            // Battery shape: exit 0 + result present + is_error false.
            (
                (0, Some(synth_result(Some(false), None, vec![]))),
                Expectation::Finished,
            ),
            // Variadic-flag misuse: exit 1, zero events, no result (§3.2).
            ((1, None), Expectation::Kind("spawn_failure")),
            // Exit 0 without a final result event is NOT success (F-00 §4).
            ((0, None), Expectation::Kind("spawn_failure")),
            // Result present but non-zero exit: the model tried and failed.
            (
                (2, Some(synth_result(Some(false), None, vec![]))),
                Expectation::Kind("task_failure"),
            ),
            // is_error=true even with exit 0: payload error gate.
            (
                (0, Some(synth_result(Some(true), None, vec![]))),
                Expectation::Kind("task_failure"),
            ),
            // Typed HTTP statuses via api_error_status.
            (
                (1, Some(synth_result(Some(true), Some(401), vec![]))),
                Expectation::Kind("auth_failure"),
            ),
            (
                (1, Some(synth_result(Some(true), Some(403), vec![]))),
                Expectation::Kind("auth_failure"),
            ),
            (
                (1, Some(synth_result(Some(true), Some(429), vec![]))),
                Expectation::Kind("transient"),
            ),
            // Observable denial events outrank the ladder (§3.1 schema).
            (
                (1, Some(synth_result(Some(false), None, vec![denial]))),
                Expectation::Kind("policy_denial"),
            ),
            // Unrecognized status codes fall through, preserved in detail.
            (
                (1, Some(synth_result(Some(false), Some(500), vec![]))),
                Expectation::Kind("task_failure"),
            ),
        ];
        for ((exit_code, result), expected) in cases {
            let event = terminal_event(exit_code, result.as_ref());
            match expected {
                Expectation::Finished => assert!(
                    matches!(event, AdapterEvent::Finished { .. }),
                    "exit {exit_code}: expected Finished, got {event:?}"
                ),
                Expectation::Kind(kind) => match &event {
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
    fn classification_details_carry_the_claude_signals() {
        // is_error without a typed status cites the payload, not stderr.
        let is_error_run = synth_result(Some(true), None, vec![]);
        let event = terminal_event(0, Some(&is_error_run));
        match event {
            AdapterEvent::Failed(failure) => {
                assert_eq!(failure.kind(), "task_failure");
                assert!(failure.to_string().contains("is_error"), "{failure}");
                assert!(!failure.is_retryable());
            }
            other => panic!("expected Failed, got {other:?}"),
        }

        // Unrecognized api_error_status survives in the detail string.
        let status_500 = synth_result(Some(false), Some(500), vec![]);
        let event = terminal_event(1, Some(&status_500));
        match event {
            AdapterEvent::Failed(failure) => {
                assert!(failure.to_string().contains("500"), "{failure}");
            }
            other => panic!("expected Failed, got {other:?}"),
        }

        // 429 is the only automatically retryable class.
        let rate_limited = synth_result(Some(true), Some(429), vec![]);
        let event = terminal_event(1, Some(&rate_limited));
        match event {
            AdapterEvent::Failed(failure) => assert!(failure.is_retryable()),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    // -- usage mapping ---------------------------------------------------------

    #[test]
    fn usage_mapping_uses_total_cost_usd_and_per_model_rows() {
        let result: ClaudeResult = serde_json::from_value(json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "total_cost_usd": 0.2,
            "usage": {
                "input_tokens": 2,
                "output_tokens": 9,
                "cache_read_input_tokens": 22_115,
                "cache_creation_input_tokens": 28_810,
                "output_tokens_details": {"thinking_tokens": 0}
            },
            "modelUsage": {
                "claude-sonnet-5": {
                    "costUSD": 0.19,
                    "contextWindow": 1_000_000,
                    "maxOutputTokens": 64_000
                },
                "claude-haiku-4-5-20251001": {
                    "costUSD": 0.01,
                    "contextWindow": 200_000
                }
            }
        }))
        .unwrap();
        let snapshot = result.usage_snapshot();
        assert_eq!(snapshot.input_tokens, 2);
        assert_eq!(snapshot.output_tokens, 9);
        assert_eq!(snapshot.cache_read_tokens, 22_115);
        assert_eq!(snapshot.total_tokens, 2 + 9 + 22_115 + 28_810);
        assert_eq!(snapshot.cost_usd, Some(0.2), "total_cost_usd ONLY");
        assert_eq!(snapshot.session_overhead_tokens, 22_000);
        assert_eq!(snapshot.per_model.len(), 2);
        assert_eq!(snapshot.per_model[0].model, "claude-haiku-4-5-20251001");
        assert_eq!(snapshot.per_model[0].cost_usd, Some(0.01));
        assert_eq!(snapshot.per_model[1].context_window, Some(1_000_000));
    }

    // -- e2e (free-only, opt-in) ---------------------------------------------
    //
    // The ONLY live invocation in this module: the free `--version` probe
    // plus the auth-file existence check. Skipped by default (`#[ignore]`)
    // and doubly gated behind AGENTOS_CLAUDE_E2E=1. Never a billable call.

    #[tokio::test]
    #[ignore = "live claude invocation (free probes only): set AGENTOS_CLAUDE_E2E=1 to run"]
    async fn e2e_free_probes_version_and_auth_files() {
        if std::env::var(CLAUDE_E2E_ENV).ok().as_deref() != Some("1") {
            return;
        }
        let adapter = ClaudeAdapter::new();

        let info = adapter.detect().await;
        assert!(info.path.is_some(), "claude binary not found: {info:?}");
        assert!(
            info.version.is_some(),
            "claude --version produced no output: {info:?}"
        );

        // File-existence only; never opens the credentials file.
        let auth = adapter.auth_status().await;
        eprintln!("claude auth status (free probe): {auth:?}");
    }
}
