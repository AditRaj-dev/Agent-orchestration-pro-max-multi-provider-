//! A skill: one named markdown document in the registry.
//!
//! Skills are *injected as prompt preambles*, not mounted from the
//! filesystem — the F-00 isolation canon keeps `~/.agents` (1278 skills on
//! the reference machine) out of worker sessions, so the registry owns its
//! own small, curated set instead. A skill body is plain markdown the
//! provider renders as instructions; it must never carry secrets (records
//! and skills are plain rows in a local SQLite file, surfaced over the WS
//! API — secrets live in the F-10 secrets interface, not here).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::AgentsError;

/// Length bound for the body — the preamble rides at the front of every
/// session objective, so an unbounded skill is an unbounded token bill.
pub const BODY_MAX_CHARS: usize = 12_000;

/// One skill definition (registry row).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRecord {
    /// Unique slug (validated with the same shape rule as agent ids).
    pub id: String,
    /// Human-facing label.
    pub name: String,
    /// One-line summary shown in the UI's skill picker.
    pub description: String,
    /// The markdown body injected into sessions that hold this skill.
    pub body: String,
    /// Seeded built-ins are editable but not deletable. Wire records omit
    /// it (the server forces it false on create, preserves it on update),
    /// so it defaults rather than making callers send a field they cannot
    /// influence — same shape rule `AgentRecord` already follows.
    #[serde(default)]
    pub builtin: bool,
    #[serde(default = "crate::record::now")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "crate::record::now")]
    pub updated_at: DateTime<Utc>,
}

impl SkillRecord {
    /// Validate the skill's own shape (cross-row rules live in the
    /// registry). Timestamps and the builtin flag are caller-owned.
    pub fn validate(&self) -> Result<(), AgentsError> {
        crate::record::ensure_slug(&self.id)?;
        if self.name.trim().is_empty() || self.name.chars().count() > crate::record::NAME_MAX_CHARS
        {
            return Err(AgentsError::Validation(format!(
                "name must be 1..={} chars",
                crate::record::NAME_MAX_CHARS
            )));
        }
        if self.description.chars().count() > crate::record::DESCRIPTION_MAX_CHARS {
            return Err(AgentsError::Validation(format!(
                "description must be <= {} chars",
                crate::record::DESCRIPTION_MAX_CHARS
            )));
        }
        if self.body.trim().is_empty() {
            return Err(AgentsError::Validation(
                "body must be non-empty markdown".to_owned(),
            ));
        }
        if self.body.chars().count() > BODY_MAX_CHARS {
            return Err(AgentsError::Validation(format!(
                "body must be <= {BODY_MAX_CHARS} chars (preamble budget)"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(id: &str) -> SkillRecord {
        SkillRecord {
            id: id.to_owned(),
            name: "Sample".to_owned(),
            description: "a sample skill".to_owned(),
            body: "# Sample\n\nDo the thing.".to_owned(),
            builtin: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn sample_skill_validates() {
        sample("sample").validate().expect("valid");
    }

    #[test]
    fn empty_body_is_rejected() {
        let mut skill = sample("sample");
        skill.body = "   ".to_owned();
        assert!(matches!(skill.validate(), Err(AgentsError::Validation(_))));
    }

    #[test]
    fn oversized_body_is_rejected() {
        let mut skill = sample("sample");
        skill.body = "x".repeat(BODY_MAX_CHARS + 1);
        assert!(matches!(skill.validate(), Err(AgentsError::Validation(_))));
    }
}
