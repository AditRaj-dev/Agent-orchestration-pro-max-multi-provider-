//! Shared error taxonomy.

use crate::task::TaskState;

/// Errors surfaced by agentos-core and shared across every dependent crate.
///
/// [`CoreError::SqliteBusy`] must remain a distinct variant: it stems from an
/// observed SQLite race (SQLITE_BUSY under concurrent WAL writers) and is
/// always safe to retry with backoff. Flatten it into a generic failure and
/// the whole workspace loses the ability to distinguish "wait and retry"
/// from "give up" — do not merge it away.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CoreError {
    /// SQLite returned SQLITE_BUSY (concurrent writer contention).
    /// Retryable: retry with backoff.
    #[error("sqlite busy (SQLITE_BUSY): concurrent writer; retry with backoff")]
    SqliteBusy,
    /// The requested task state change is not a legal arc of the
    /// [`TaskState`] lifecycle graph.
    #[error("illegal task state transition: {from} -> {to}")]
    IllegalTransition {
        /// The current state.
        from: TaskState,
        /// The rejected target state.
        to: TaskState,
    },
    /// A payload or entity failed to (de)serialize.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// The referenced entity does not exist.
    #[error("not found: {0}")]
    NotFound(String),
}

impl CoreError {
    /// Whether the failed operation is safe to retry (e.g. after backoff).
    /// Only transient contention qualifies today; logic errors, missing
    /// entities, and bad payloads are deterministic and must not be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(self, CoreError::SqliteBusy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_busy_is_retryable() {
        assert!(CoreError::SqliteBusy.is_retryable());
    }

    #[test]
    fn other_errors_are_not_retryable() {
        let illegal = CoreError::IllegalTransition {
            from: TaskState::Done,
            to: TaskState::Running,
        };
        assert!(!illegal.is_retryable());
        assert!(!CoreError::Serialization("truncated json".to_owned()).is_retryable());
        assert!(!CoreError::NotFound("task 42".to_owned()).is_retryable());
    }

    #[test]
    fn illegal_transition_reports_both_states() {
        let error = CoreError::IllegalTransition {
            from: TaskState::Created,
            to: TaskState::Done,
        };
        assert_eq!(
            error.to_string(),
            "illegal task state transition: created -> done"
        );
    }
}
