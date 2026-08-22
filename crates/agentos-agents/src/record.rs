//! The agent record: one row = one spawnable worker definition.
//!
//! The field set maps onto the seams the runtime already exposes —
//! `adapter_id` selects the [`RuntimeAdapter`](agentos_adapters::RuntimeAdapter)
//! (the supervisor's role→adapter routing), `model` rides
//! [`SpawnSpec::model`](agentos_adapters::SpawnSpec), `mode` and the tool
//! lists compile into the same policy-bearing spec fields, and `skills`
//! become the objective preamble via
//! [`AgentRegistry::preamble_for`](crate::AgentRegistry::preamble_for).
//! Nothing here re-implements a rule another layer owns: budgets, leases
//! and review gates stay with the workflow engine.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::AgentsError;

/// Adapter ids this build can route to (F-02/F-03/F-05 adapters; codex and
/// zcode join this list when their adapters land).
pub const KNOWN_ADAPTERS: [&str; 3] = ["mock", "claude-code", "antigravity-agy"];

/// The agy adapter id (routes to claude/gemini/oss models upstream).
pub const ADAPTER_ANTIGRAVITY_AGY: &str = "antigravity-agy";

/// Session access mode. Maps onto the agy `--mode` flag (the adapter derives
/// it from `allowed_paths`: empty → plan, non-empty → accept-edits); for
/// adapters without a read-only mode (claude pins `acceptEdits`), plan-mode
/// agents get the write tools denied instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentMode {
    /// Read-only: research, chat, review, planning.
    Plan,
    /// May edit files in its workspace.
    AcceptEdits,
}

impl AgentMode {
    /// The canonical wire string.
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentMode::Plan => "plan",
            AgentMode::AcceptEdits => "accept_edits",
        }
    }

    /// Parse from the wire string (total; unknown → `None`).
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "plan" => Some(AgentMode::Plan),
            "accept_edits" => Some(AgentMode::AcceptEdits),
            _ => None,
        }
    }
}

/// Read-only is the safe wire default (a create payload that names no mode
/// cannot accidentally gain write access).
impl Default for AgentMode {
    fn default() -> Self {
        AgentMode::Plan
    }
}

/// Reasoning-effort knob (agy `--effort`; ignored by adapters without one).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentEffort {
    Low,
    Medium,
    High,
}

impl AgentEffort {
    /// The canonical wire string.
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentEffort::Low => "low",
            AgentEffort::Medium => "medium",
            AgentEffort::High => "high",
        }
    }

    /// Parse from the wire string (total; unknown → `None`).
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "low" => Some(AgentEffort::Low),
            "medium" => Some(AgentEffort::Medium),
            "high" => Some(AgentEffort::High),
            _ => None,
        }
    }
}

/// Bounds on the wall-clock timeout a record may carry. The floor keeps a
/// stuck session from being unkillable-by-timeout, the ceiling keeps a
/// fat-fingered edit from parking a session for a month.
pub const MIN_TIMEOUT_SECS: u64 = 30;
pub const MAX_TIMEOUT_SECS: u64 = 86_400;

/// Length bounds for the display strings.
pub const NAME_MAX_CHARS: usize = 80;
pub const DESCRIPTION_MAX_CHARS: usize = 500;

/// One spawnable agent definition (registry row).
///
/// Wire-friendly: every field a caller cannot know (timestamps) or should
/// not send (`builtin`) has a serde default, so `registry.agents.create`
/// payloads and agent-creator proposals can carry the minimum; the
/// registry stamps/owns the rest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRecord {
    /// Unique slug (`researcher`, `agent-creator`). Also the routing key:
    /// a workflow node's `agent_role` names an agent id.
    pub id: String,
    /// Human-facing label.
    pub name: String,
    /// What this agent is for — surfaced in the UI and in the
    /// orchestrator's worker roster so the planning model can route to it.
    pub description: String,
    /// Provider adapter (`mock` / `claude-code` / `antigravity-agy`).
    pub adapter_id: String,
    /// Model slug passed through to the adapter (`None` = adapter default).
    #[serde(default)]
    pub model: Option<String>,
    /// Reasoning effort (agy only; `None` = provider default).
    #[serde(default)]
    pub effort: Option<AgentEffort>,
    /// Read-only (`plan`) or workspace-writing (`accept_edits`).
    #[serde(default)]
    pub mode: AgentMode,
    /// Skill ids injected as the session's prompt preamble.
    #[serde(default)]
    pub skills: Vec<String>,
    /// Per-tool allowlist compiled into the spawn spec.
    #[serde(default)]
    pub tool_allowlist: Vec<String>,
    /// Per-tool denylist compiled into the spawn spec (deny beats allow).
    #[serde(default)]
    pub tool_denylist: Vec<String>,
    /// Wall-clock ceiling for one session, in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Seeded by [`seed_builtins`](crate::AgentRegistry::seed_builtins);
    /// editable, never deletable. Forced `false` on wire-created records.
    #[serde(default)]
    pub builtin: bool,
    /// Disabled agents stay on disk but stop routing and rostering.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "now")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "now")]
    pub updated_at: DateTime<Utc>,
}

fn default_timeout_secs() -> u64 {
    600
}

fn default_enabled() -> bool {
    true
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

impl AgentRecord {
    /// Validate the record's own shape (everything checkable without the
    /// registry: cross-row rules like skill existence live one layer up).
    ///
    /// The `builtin` flag and timestamps are *not* validated — callers
    /// (registry, daemon API) own those, and a proposal draft from the
    /// agent-creator arrives without them.
    pub fn validate(&self) -> Result<(), AgentsError> {
        ensure_slug(&self.id)?;
        if self.name.trim().is_empty() || self.name.chars().count() > NAME_MAX_CHARS {
            return Err(AgentsError::Validation(format!(
                "name must be 1..={NAME_MAX_CHARS} chars, got {:?}",
                self.name
            )));
        }
        if self.description.chars().count() > DESCRIPTION_MAX_CHARS {
            return Err(AgentsError::Validation(format!(
                "description must be <= {DESCRIPTION_MAX_CHARS} chars"
            )));
        }
        if !KNOWN_ADAPTERS.contains(&self.adapter_id.as_str()) {
            return Err(AgentsError::Validation(format!(
                "adapter {:?} not in {KNOWN_ADAPTERS:?}",
                self.adapter_id
            )));
        }
        if let Some(effort) = self.effort {
            // Effort is an agy flag; carrying it on another adapter would
            // silently do nothing.
            if self.adapter_id != ADAPTER_ANTIGRAVITY_AGY {
                return Err(AgentsError::Validation(format!(
                    "effort {:?} only applies to {ADAPTER_ANTIGRAVITY_AGY}",
                    effort.as_str()
                )));
            }
        }
        if let Some(model) = self.model.as_deref() {
            if model.trim().is_empty() {
                return Err(AgentsError::Validation(
                    "model must be a non-empty slug when present".to_owned(),
                ));
            }
        }
        if !(MIN_TIMEOUT_SECS..=MAX_TIMEOUT_SECS).contains(&self.timeout_secs) {
            return Err(AgentsError::Validation(format!(
                "timeoutSecs must be {MIN_TIMEOUT_SECS}..={MAX_TIMEOUT_SECS}, got {}",
                self.timeout_secs
            )));
        }
        if self.skills.len() > 16 {
            return Err(AgentsError::Validation(
                "an agent may hold at most 16 skills (preamble budget)".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Slug shape: lowercase letters/digits/dashes/dots/underscores, 2..=64
/// chars, starting alphanumeric. The routing key everywhere (WS params,
/// `agent_role`), so it stays conservative on purpose.
fn valid_slug(id: &str) -> bool {
    let len = id.chars().count();
    (2..=64).contains(&len)
        && id.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' || c == '.'
        })
        && id.chars().next().is_some_and(|c| c.is_ascii_alphanumeric())
}

/// Validate a slug shape with the typed error (shared by agents and skills).
pub(crate) fn ensure_slug(id: &str) -> Result<(), AgentsError> {
    valid_slug(id)
        .then_some(())
        .ok_or_else(|| AgentsError::Validation(format!("id {id:?} is not a lowercase slug")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal valid record for tests to build on.
    pub(crate) fn sample(id: &str) -> AgentRecord {
        AgentRecord {
            id: id.to_owned(),
            name: "Sample".to_owned(),
            description: "sample agent".to_owned(),
            adapter_id: "mock".to_owned(),
            model: Some("mock-model-1".to_owned()),
            effort: None,
            mode: AgentMode::Plan,
            skills: vec![],
            tool_allowlist: vec![],
            tool_denylist: vec![],
            timeout_secs: 600,
            builtin: false,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn slug_shape_is_conservative() {
        for good in ["ab", "researcher", "agent-creator", "dev.2_fast"] {
            assert!(valid_slug(good), "{good} should be a valid slug");
        }
        for bad in [
            "a",             // too short
            "",              // empty
            "Bad",           // uppercase
            "-leading",      // leading dash
            "has space",     // space
            "häs",           // non-ascii
            &"x".repeat(65), // too long
        ] {
            assert!(!valid_slug(bad), "{bad:?} should be invalid");
        }
    }

    #[test]
    fn sample_record_validates() {
        sample("sample").validate().expect("sample is valid");
    }

    #[test]
    fn unknown_adapter_is_rejected() {
        let mut record = sample("sample");
        record.adapter_id = "skynet".to_owned();
        assert!(record.validate().is_err());
    }

    #[test]
    fn effort_is_rejected_off_agy() {
        let mut record = sample("sample");
        record.effort = Some(AgentEffort::High);
        assert!(matches!(record.validate(), Err(AgentsError::Validation(_))));
        record.adapter_id = ADAPTER_ANTIGRAVITY_AGY.to_owned();
        record.validate().expect("effort is fine on agy");
    }

    #[test]
    fn timeout_bounds_are_enforced() {
        let mut record = sample("sample");
        record.timeout_secs = MIN_TIMEOUT_SECS - 1;
        assert!(record.validate().is_err());
        record.timeout_secs = MAX_TIMEOUT_SECS + 1;
        assert!(record.validate().is_err());
        record.timeout_secs = MAX_TIMEOUT_SECS;
        record.validate().expect("ceiling is inclusive");
    }

    #[test]
    fn mode_and_effort_wire_strings_round_trip() {
        for mode in [AgentMode::Plan, AgentMode::AcceptEdits] {
            assert_eq!(AgentMode::parse(mode.as_str()), Some(mode));
        }
        for effort in [AgentEffort::Low, AgentEffort::Medium, AgentEffort::High] {
            assert_eq!(AgentEffort::parse(effort.as_str()), Some(effort));
        }
        assert_eq!(AgentMode::parse("yolo"), None);
        assert_eq!(AgentEffort::parse("extreme"), None);
    }

    #[test]
    fn camel_case_wire_shape() {
        let value = serde_json::to_value(sample("sample")).unwrap();
        for key in [
            "adapterId",
            "timeoutSecs",
            "toolAllowlist",
            "toolDenylist",
            "createdAt",
            "updatedAt",
        ] {
            assert!(value.get(key).is_some(), "missing {key} in {value}");
        }
    }

    #[test]
    fn minimal_wire_payload_takes_the_safe_defaults() {
        let record: AgentRecord = serde_json::from_value(serde_json::json!({
            "id": "wire-agent",
            "name": "Wire Agent",
            "description": "from the wire",
            "adapterId": "mock"
        }))
        .expect("minimal payload deserializes");
        assert_eq!(record.mode, AgentMode::Plan, "read-only default");
        assert_eq!(record.timeout_secs, 600);
        assert!(!record.builtin, "wire records are never builtin");
        assert!(record.enabled);
        assert!(record.skills.is_empty());
        record.validate().expect("defaults validate");
    }
}
