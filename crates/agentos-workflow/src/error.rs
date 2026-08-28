//! Error taxonomy for the workflow crate.

use agentos_core::CoreError;
use serde::{Deserialize, Serialize};

/// A machine-readable workflow-spec rejection (PRD §9 OR-03: validation
/// happens BEFORE execution; OR-01: rejections carry machine-readable
/// reasons).
///
/// Serializes as an internally-tagged JSON object (`{"code": "cycle_detected",
/// "cycle": [...]}`) so callers can branch on the failure without parsing
/// error strings.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum ValidationError {
    /// The spec declares no nodes.
    #[error("workflow must declare at least one node")]
    EmptyWorkflow,
    /// More than one node shares an id.
    #[error("duplicate node ids: {duplicates:?}")]
    DuplicateNodeIds {
        /// The offending ids, sorted and deduplicated.
        duplicates: Vec<String>,
    },
    /// A `dependsOn` entry refers to a node that does not exist.
    #[error("node `{node}` depends on unknown node `{dependency}`")]
    UnknownDependency {
        /// The node holding the dangling reference.
        node: String,
        /// The referenced but undeclared node id.
        dependency: String,
    },
    /// The dependency graph contains a cycle; `cycle` is a concrete path
    /// (first node repeated at the end), e.g. `["a", "b", "a"]`.
    #[error("dependency cycle detected: {cycle:?}")]
    CycleDetected {
        /// One concrete cycle, starting and ending on the same node id.
        cycle: Vec<String>,
    },
    /// A loop node declared `max_iterations: 0` (loops must be bounded).
    #[error("loop node `{node}` must declare max_iterations >= 1 (got {max_iterations})")]
    UnboundedLoop {
        /// The offending loop node.
        node: String,
        /// The (invalid) declared bound.
        max_iterations: u32,
    },
}

/// Every failure the workflow crate can surface.
///
/// [`WorkflowError::Storage`] wraps agentos-core's taxonomy so SQLITE_BUSY
/// stays distinguishable and retryable
/// (`Storage(CoreError::SqliteBusy)`); all other variants are deterministic
/// logic errors.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum WorkflowError {
    /// The configured worker cap is outside the deliberately small,
    /// operator-safe range. Keeping this bounded makes admission behaviour
    /// predictable for desktop and daemon embedders.
    #[error("max concurrency must be between 1 and 8 (got {value})")]
    InvalidMaxConcurrency { value: usize },
    /// The workflow spec was rejected by validation.
    #[error("spec rejected: {0}")]
    Validation(#[from] ValidationError),
    /// A storage-layer failure (busy, serialization, not found, illegal
    /// transition). See [`CoreError`] for the retryability rules.
    #[error("storage error: {0}")]
    Storage(#[from] CoreError),
    /// A lease operation was attempted by someone other than the holder.
    #[error("task {task} lease is held by `{holder}`, not `{owner}`")]
    LeaseOwnerMismatch {
        /// The task whose lease was touched.
        task: String,
        /// The actual lease holder.
        holder: String,
        /// The identity that attempted the operation.
        owner: String,
    },
    /// The bounded driver loop exhausted its tick allowance with work still
    /// pending — a runaway loop guard, not a scheduler failure.
    #[error("driver exceeded {max_ticks} ticks with work still pending")]
    MaxTicksExceeded {
        /// The tick allowance that was exhausted.
        max_ticks: u32,
    },
}

impl WorkflowError {
    /// Whether the failed operation is safe to retry with backoff. Delegates
    /// to the wrapped [`CoreError`] for storage failures; every other
    /// variant is deterministic.
    pub fn is_retryable(&self) -> bool {
        match self {
            WorkflowError::Storage(inner) => inner.is_retryable(),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_error_serializes_as_tagged_machine_readable_code() {
        let error = ValidationError::CycleDetected {
            cycle: vec!["a".to_owned(), "b".to_owned(), "a".to_owned()],
        };
        let wire = serde_json::to_value(&error).unwrap();
        assert_eq!(wire["code"], serde_json::json!("cycle_detected"));
        assert_eq!(wire["cycle"], serde_json::json!(["a", "b", "a"]));
        let parsed: ValidationError = serde_json::from_value(wire).unwrap();
        assert_eq!(parsed, error);
    }

    #[test]
    fn busy_storage_error_is_retryable_others_are_not() {
        assert!(WorkflowError::Storage(CoreError::SqliteBusy).is_retryable());
        assert!(!WorkflowError::Validation(ValidationError::EmptyWorkflow).is_retryable());
        assert!(!WorkflowError::MaxTicksExceeded { max_ticks: 10 }.is_retryable());
    }
}
