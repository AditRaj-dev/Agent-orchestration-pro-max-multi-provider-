//! Error taxonomy for the F-07 runtime/supervisor crate.

use thiserror::Error;

/// Every failure the runtime crate can surface.
///
/// Wrapped taxonomies keep their retryability identities intact:
/// [`agentos_core::CoreError::SqliteBusy`] stays reachable through
/// [`RuntimeError::Core`] / [`RuntimeError::Workflow`] /
/// [`RuntimeError::Git`], so callers can distinguish "back off and retry"
/// from deterministic failure.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// A workflow-engine failure (validation, storage, lease mismatch).
    #[error("workflow error: {0}")]
    Workflow(#[from] agentos_workflow::WorkflowError),
    /// A git-manager failure (worktree, queue, ledger, CLI).
    #[error("git error: {0}")]
    Git(#[from] agentos_git::GitError),
    /// An adapter-machinery failure (spawn refused, session dead).
    #[error("adapter error: {0}")]
    Adapter(#[from] agentos_adapters::AdapterError),
    /// A shared core failure (journal/storage SQLITE_BUSY, serialization).
    #[error("core error: {0}")]
    Core(#[from] agentos_core::CoreError),
    /// An ownership-map conflict or invalid hold.
    #[error("ownership error: {0}")]
    Ownership(#[from] agentos_git::ownership::OwnershipError),
    /// Filesystem failure (state dir, manifests, artifacts).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON (de)serialization failure.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// A task contract failed a validation rule (Appendix B shape).
    #[error("contract rejected: {0}")]
    Contract(#[from] crate::contract::ContractRule),
    /// A contract-level input was structurally invalid.
    #[error("invalid contract input: {0}")]
    ContractInvalid(String),
    /// No registered adapter satisfies the requested role.
    #[error("no adapter registered for role `{role}` (wanted `{default}`)")]
    NoAdapter {
        /// The node's `agent_role` (empty when the node declared none).
        role: String,
        /// The adapter id that was selected (role mapping or default).
        default: String,
    },
    /// The run has no manifest (neither in memory nor on disk under the
    /// state dir) — it was not started by this supervisor installation.
    #[error("no manifest for run {0}")]
    MissingManifest(uuid::Uuid),
    /// `start_run` was called without a full contract for every node.
    #[error("nodes missing full contracts: {0:?}")]
    MissingContracts(Vec<String>),
    /// The bounded drive loop exhausted its tick allowance with the run
    /// still active — a runaway-loop guard, not a scheduling failure.
    #[error("drive exceeded {max_ticks} ticks with the run still active")]
    DriveExhausted { max_ticks: u32 },
}
