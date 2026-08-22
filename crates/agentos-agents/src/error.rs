//! Error taxonomy for the agent registry.
//!
//! Follows the workspace pattern (see `agentos-workflow`'s `WorkflowError`):
//! a thin thiserror enum over [`CoreError`] plus the registry's own
//! deterministic failures. Nothing here is retryable — SQLITE_BUSY stays
//! the only retryable case, and it surfaces as
//! [`CoreError::SqliteBusy`](agentos_core::CoreError::SqliteBusy).

use agentos_core::CoreError;

/// Registry failures: validation, uniqueness, builtin guards, storage.
#[derive(Debug, thiserror::Error)]
pub enum AgentsError {
    /// A record failed domain validation (slug shape, unknown adapter,
    /// effort on a non-agy adapter, out-of-bounds timeout, …).
    #[error("invalid agent/skill: {0}")]
    Validation(String),
    /// The referenced agent or skill id does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// An agent or skill with this id already exists.
    #[error("duplicate id: {0}")]
    Duplicate(String),
    /// Built-ins are editable but never deletable.
    #[error("builtin {0} cannot be deleted (edit it instead)")]
    BuiltinProtected(String),
    /// Storage/serialization failure under a valid request.
    #[error(transparent)]
    Core(#[from] CoreError),
}

impl AgentsError {
    /// Whether the failed operation is safe to retry. Only SQLITE_BUSY
    /// qualifies; every registry-logic failure is deterministic.
    pub fn is_retryable(&self) -> bool {
        matches!(self, AgentsError::Core(CoreError::SqliteBusy))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_busy_is_retryable() {
        assert!(AgentsError::Core(CoreError::SqliteBusy).is_retryable());
        assert!(!AgentsError::Validation("bad slug".to_owned()).is_retryable());
        assert!(!AgentsError::NotFound("agent x".to_owned()).is_retryable());
        assert!(!AgentsError::Duplicate("agent x".to_owned()).is_retryable());
        assert!(!AgentsError::BuiltinProtected("orchestrator".to_owned()).is_retryable());
    }
}
