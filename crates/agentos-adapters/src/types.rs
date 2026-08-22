//! Adapter-domain types (F-02): runtime identity, free-check auth state,
//! capability surface, spawn specification, and the usage/cost snapshot.
//!
//! These are provider-facing types shared by every [`crate::RuntimeAdapter`]
//! implementation (the mock today; Claude Code / agy / codex / zcode in
//! F-03..F-05). Serialization follows the workspace convention of camelCase
//! field names (matching `agentos_core::Event`), so adapter payloads flow
//! into the daemon journal without renaming. Parsing stays total where
//! practical: unknown providers must never crash the supervisor.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identity of one detected runtime installation (PRD RT-06: connections
/// surface shows installed version; PRD §21: record runtime versions where
/// discoverable — all reference CLIs expose `--version`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeInfo {
    /// Stable adapter identifier, e.g. `"mock"` or `"claude-code"`.
    pub id: String,
    /// Discovered CLI version, when the runtime reports one.
    pub version: Option<String>,
    /// Resolved executable path, when detection found one.
    pub path: Option<PathBuf>,
}

/// Result of a *free* authentication probe (PRD RT-06 / F-00 §5: health
/// checks never make billable calls — tier 0 only: binary presence,
/// `--version`, auth-file existence, `login status`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthStatus {
    /// Credentials present and the free preflight passed.
    Ready,
    /// Runtime installed but not signed in; a blocking system event
    /// (PRD RT-04: "reauthentication required").
    NeedsLogin,
    /// Cannot be determined without a billable call; treated as not ready.
    Unknown,
}

/// Boolean capability surface (PRD RT-06: filesystem-edit, shell,
/// multimodal, long-context, MCP-client, structured-output — plus network
/// and resume which the scheduler also needs). Adapters report the
/// intersection of what the CLI supports and what the local install allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    /// Can create, edit, and delete files in the workspace.
    pub filesystem_edit: bool,
    /// Can execute arbitrary shell commands (subject to policy).
    pub shell: bool,
    /// Can reach the network (web fetch/search tools or raw sockets).
    pub network: bool,
    /// Can emit schema-constrained structured output
    /// (claude output schemas, agy `--json-schema`, codex `--output-schema`).
    pub structured_output: bool,
    /// Can resume a previous session by id
    /// (claude `--resume`, agy `--conversation`, zcode `--resume`).
    pub resume: bool,
    /// Accepts image/audio input.
    pub multimodal: bool,
    /// Supports ~1M-token class context windows.
    pub long_context: bool,
    /// Can act as an MCP client.
    pub mcp_client: bool,
}

impl Capabilities {
    /// Every capability enabled — the canned surface of the mock adapter.
    pub const fn full() -> Self {
        Self {
            filesystem_edit: true,
            shell: true,
            network: true,
            structured_output: true,
            resume: true,
            multimodal: true,
            long_context: true,
            mcp_client: true,
        }
    }
}

/// Result of one health probe (RT-06): detection + free auth check,
/// timestamped. Cached by callers with a short TTL; never billable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthReport {
    /// What was detected (id, version, path).
    pub runtime: RuntimeInfo,
    /// Free-check authentication state at probe time.
    pub auth: AuthStatus,
    /// When the probe ran.
    pub checked_at: DateTime<Utc>,
}

/// Everything an adapter needs to start one provider session (PRD RT-01
/// `startSession(task, workspace, policy)`).
///
/// `isolated_home` encodes the observed env-redirection isolation pattern
/// (handoff §4.1 / codex C1): redirect `USERPROFILE`/`HOME` to a temp home
/// containing junctions only to provider config dirs, so user-global agent
/// state (`~/.agents` skills) cannot leak into worker sessions. F-02 carries
/// the field; spawn-time enforcement lands with the concrete CLI adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpawnSpec {
    /// The task this session executes; stamped onto journal events.
    pub task_id: Uuid,
    /// The objective prompt delivered to the runtime.
    pub objective: String,
    /// Working directory for the session (typically a git worktree).
    pub workspace: PathBuf,
    /// Extra paths the runtime may access in addition to the workspace.
    pub allowed_paths: Vec<String>,
    /// Paths explicitly off-limits (harness-enforced).
    pub forbidden_paths: Vec<String>,
    /// Per-tool allowlist (claude `--allowedTools=Bash(echo:*)` form).
    pub tool_allowlist: Vec<String>,
    /// Per-tool denylist (claude `--disallowedTools=Bash,WebFetch` form).
    pub tool_denylist: Vec<String>,
    /// Model override; `None` means the adapter default.
    pub model: Option<String>,
    /// Wall-clock budget for the whole session, in seconds.
    pub timeout_secs: u64,
    /// Redirected home directory for env isolation, when enabled.
    pub isolated_home: Option<PathBuf>,
}

/// Point-in-time usage and cost for one session (or one turn within it).
///
/// Field set is the union of the observed provider contracts: claude's
/// `usage` + `modelUsage` (handoff §3.1) and agy's `usage` object (agy
/// battery). Providers without a field leave it zero/`None`; the ledger
/// (F-06) labels estimates only where estimates exist.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSnapshot {
    /// Prompt tokens billed this session/turn.
    pub input_tokens: u64,
    /// Completion tokens billed this session/turn.
    pub output_tokens: u64,
    /// Reasoning ("thinking") tokens, when the provider splits them out
    /// (agy `thinking_tokens`, claude `output_tokens_details.thinking_tokens`).
    pub thinking_tokens: u64,
    /// Cache-hit read tokens (claude `cache_read_input_tokens`,
    /// agy `cache_read_tokens`).
    pub cache_read_tokens: u64,
    /// Total tokens as reported by the provider.
    pub total_tokens: u64,
    /// Monetary cost in USD when the provider reports one. Claude is the
    /// only observed CLI exposing cost (`total_cost_usd` — the field named
    /// `cost_usd` does not exist); agy and codex report tokens only.
    pub cost_usd: Option<f64>,
    /// Per-model cost rows (claude `modelUsage` canon: a trivial run can
    /// touch two models — main + auxiliary haiku). Empty when the provider
    /// has no per-model breakdown.
    pub per_model: Vec<ModelUsageRow>,
    /// Fixed per-session preamble overhead (F-00 §4: ~22k tokens on Claude,
    /// ~37k on agy — core system prompt, *not* user skills, per claude T5
    /// and the agy `--disable-slash-commands` A/B). The budget ledger
    /// carries this as its per-session overhead line.
    pub session_overhead_tokens: u64,
}

/// One per-model cost row (claude `modelUsage[]` canon: `costUSD`,
/// `contextWindow`, `canonicalModel` observed per row).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelUsageRow {
    /// Model identifier as the provider reports it.
    pub model: String,
    /// Cost attributable to this model, when reported.
    pub cost_usd: Option<f64>,
    /// Context window size in tokens, when reported.
    pub context_window: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn capabilities_full_enables_every_flag() {
        let caps = Capabilities::full();
        for enabled in [
            caps.filesystem_edit,
            caps.shell,
            caps.network,
            caps.structured_output,
            caps.resume,
            caps.multimodal,
            caps.long_context,
            caps.mcp_client,
        ] {
            assert!(enabled);
        }
    }

    #[test]
    fn usage_snapshot_serializes_camel_case_and_round_trips() {
        let snapshot = UsageSnapshot {
            input_tokens: 1_000,
            output_tokens: 500,
            thinking_tokens: 64,
            cache_read_tokens: 22_000,
            total_tokens: 23_564,
            cost_usd: Some(0.02),
            per_model: vec![ModelUsageRow {
                model: "mock-model-1".to_owned(),
                cost_usd: Some(0.02),
                context_window: Some(1_000_000),
            }],
            session_overhead_tokens: 22_000,
        };

        let value = serde_json::to_value(&snapshot).unwrap();
        for key in [
            "inputTokens",
            "outputTokens",
            "thinkingTokens",
            "cacheReadTokens",
            "totalTokens",
            "costUsd",
            "perModel",
            "sessionOverheadTokens",
        ] {
            assert!(value.get(key).is_some(), "expected key {key} in {value}");
        }
        assert_eq!(value["perModel"][0]["contextWindow"], json!(1_000_000));

        let round_tripped: UsageSnapshot =
            serde_json::from_value(serde_json::to_value(&snapshot).unwrap()).unwrap();
        assert_eq!(round_tripped, snapshot);
    }

    #[test]
    fn spawn_spec_round_trips_with_isolated_home() {
        let spec = SpawnSpec {
            task_id: Uuid::new_v4(),
            objective: "implement the feature".to_owned(),
            workspace: PathBuf::from("worktrees/task-1"),
            allowed_paths: vec!["C:/repo/docs".to_owned()],
            forbidden_paths: vec!["C:/repo/.env".to_owned()],
            tool_allowlist: vec!["Bash(echo:*)".to_owned()],
            tool_denylist: vec!["WebFetch".to_owned()],
            model: Some("mock-model-1".to_owned()),
            timeout_secs: 900,
            isolated_home: Some(PathBuf::from("temp-homes/worker-1")),
        };

        let round_tripped: SpawnSpec =
            serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
        assert_eq!(round_tripped, spec);
        assert_eq!(round_tripped.isolated_home, spec.isolated_home);
    }

    #[test]
    fn auth_status_serializes_as_snake_case() {
        for (status, wire) in [
            (AuthStatus::Ready, "ready"),
            (AuthStatus::NeedsLogin, "needs_login"),
            (AuthStatus::Unknown, "unknown"),
        ] {
            assert_eq!(serde_json::to_value(status).unwrap(), json!(wire));
            let parsed: AuthStatus = serde_json::from_value(json!(wire)).unwrap();
            assert_eq!(parsed, status);
        }
    }
}
