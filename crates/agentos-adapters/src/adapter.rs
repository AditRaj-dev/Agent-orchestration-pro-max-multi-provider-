//! The provider-independent runtime contract (PRD RT-01) and the live
//! session handle.
//!
//! [`RuntimeAdapter`] is object-safe and async; concrete implementations
//! are the mock (F-02), Claude Code (F-03), codex (F-04), agy/zcode
//! (F-05/F-05b). The trait deliberately separates three concern groups:
//!
//! 1. Discovery — [`RuntimeAdapter::detect`], [`RuntimeAdapter::auth_status`],
//!    [`RuntimeAdapter::capabilities`], [`RuntimeAdapter::health`]: all
//!    free, never billable (RT-06, F-00 §5).
//! 2. Sessions — [`RuntimeAdapter::start_session`] /
//!    [`RuntimeAdapter::shutdown`].
//! 3. Streaming — [`SessionHandle::events`] plus instruction/cancel.
//!
//! Run failures are *not* `Result` errors: they stream as
//! [`crate::AdapterEvent::Failed`] so the supervisor journals the full
//! lifecycle. `Result` errors mean the adapter machinery itself failed
//! (spawn refused, session already dead, internal state).

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::broadcast;

use crate::error::AdapterError;
use crate::events::AdapterEvent;
use crate::types::{AuthStatus, Capabilities, HealthReport, RuntimeInfo, SpawnSpec};

/// A provider runtime the harness can drive (PRD RT-01).
///
/// Implementations must be `Send + Sync` (shared across the supervisor's
/// tasks) and may hold per-provider global state such as semaphores
/// (account-wide limits, handoff §4.3).
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    /// Stable adapter identifier (`"mock"`, `"claude-code"`, ...).
    fn id(&self) -> &str;

    /// Locate the runtime and report id/version/path. Free.
    async fn detect(&self) -> RuntimeInfo;

    /// Free-check authentication state. Never makes a billable call
    /// (RT-06): auth-file existence, `login status`, or equivalent.
    async fn auth_status(&self) -> AuthStatus;

    /// Boolean capability surface (RT-06).
    async fn capabilities(&self) -> Capabilities;

    /// Health report: detection + free auth check, timestamped. The default
    /// composes the two free probes; never billable.
    async fn health(&self) -> HealthReport {
        HealthReport {
            runtime: self.detect().await,
            auth: self.auth_status().await,
            checked_at: Utc::now(),
        }
    }

    /// Start one live session for the given spec. The objective is the
    /// initial prompt; progress streams via [`SessionHandle::events`].
    async fn start_session(&self, spec: SpawnSpec) -> Result<SessionHandle, AdapterError>;

    /// Start a session that **continues an earlier provider conversation**
    /// instead of opening a fresh one: the objective in `spec` becomes the
    /// next turn of `provider_session_id`, with all of its context.
    ///
    /// `provider_session_id` is the id the provider reported through
    /// [`crate::AdapterEvent::Started`] (claude session id, agy
    /// conversation id, codex thread id) — the daemon keeps it in the
    /// journal, so a chat survives a daemon restart.
    ///
    /// The default refuses with [`AdapterError::Unsupported`]: a runtime
    /// with no resume surface must say so rather than silently starting a
    /// session with no memory of the conversation the human is looking at.
    async fn resume_session(
        &self,
        spec: SpawnSpec,
        provider_session_id: String,
    ) -> Result<SessionHandle, AdapterError> {
        let _ = (spec, provider_session_id);
        Err(AdapterError::Unsupported(format!(
            "adapter {} cannot resume a provider session",
            self.id()
        )))
    }

    /// Stop every session this adapter owns and release resources.
    /// Idempotent.
    async fn shutdown(&self) -> Result<(), AdapterError>;
}

/// Provider-specific half of a [`SessionHandle`]: instruction delivery and
/// cancellation. The event plumbing (broadcast) is shared and lives in the
/// handle itself.
#[async_trait]
pub trait SessionBackend: Send + Sync {
    /// Deliver one instruction to the live session. Responses arrive as
    /// events; this returns once the instruction is accepted, not when the
    /// turn completes.
    async fn send_instruction(&self, text: String) -> Result<(), AdapterError>;

    /// Request cancellation. Best-effort and cooperative: the stream ends
    /// without a terminal event (process-kill semantics — a killed CLI
    /// emits no `Finished`/`Failed`).
    async fn cancel(&self) -> Result<(), AdapterError>;
}

/// Ownership of one live session (PRD RT-01 `streamEvents` /
/// `sendInstruction` / `cancel`).
///
/// Cloneable (the supervisor, the journaler, and a UI projection can each
/// hold one) and thread-safe. Events fan out over a tokio broadcast
/// channel — chosen over mpsc because a session stream has *multiple*
/// natural consumers in this architecture (event journal, budget ledger,
/// live UI), all of which need the same ordered feed, and any of which may
/// attach late without stealing ownership of the stream. The cost is
/// bounded buffers per consumer, which every consumer already needs for
/// idempotent journaling (F-00 §3).
#[derive(Clone)]
pub struct SessionHandle {
    session_id: String,
    events: broadcast::Sender<AdapterEvent>,
    backend: Arc<dyn SessionBackend>,
}

impl SessionHandle {
    /// Assemble a handle from its parts. The sender must be the same
    /// channel the adapter's driving task publishes on.
    pub(crate) fn new(
        session_id: String,
        events: broadcast::Sender<AdapterEvent>,
        backend: Arc<dyn SessionBackend>,
    ) -> Self {
        Self {
            session_id,
            events,
            backend,
        }
    }

    /// Provider session id (resumable where the runtime supports it).
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Subscribe to the session's event stream from now on. Broadcast has
    /// no replay: subscribe before the session's first event to observe
    /// the full lifecycle. Each call yields an independent subscriber.
    ///
    /// Termination semantics: a completed session ends its stream with a
    /// terminal event (`Finished`/`Failed`); the channel itself stays open
    /// for the handle's lifetime. A cancelled session simply goes quiet —
    /// cancel is process-kill semantics, so no terminal event is emitted.
    pub fn events(&self) -> broadcast::Receiver<AdapterEvent> {
        self.events.subscribe()
    }

    /// Send one instruction to the session. Fails with
    /// [`AdapterError::SessionNotActive`] once the session has finished or
    /// been cancelled.
    pub async fn send_instruction(&self, text: String) -> Result<(), AdapterError> {
        self.backend.send_instruction(text).await
    }

    /// Request cancellation; see [`SessionBackend::cancel`].
    pub async fn cancel(&self) -> Result<(), AdapterError> {
        self.backend.cancel().await
    }
}

impl fmt::Debug for SessionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionHandle")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubBackend;

    #[async_trait]
    impl SessionBackend for StubBackend {
        async fn send_instruction(&self, _text: String) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn cancel(&self) -> Result<(), AdapterError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn handle_clones_share_the_stream_and_backend() {
        let (tx, mut rx) = broadcast::channel(8);
        let handle = SessionHandle::new("stub-1".to_owned(), tx.clone(), Arc::new(StubBackend));
        let clone = handle.clone();

        assert_eq!(handle.session_id(), "stub-1");
        assert_eq!(clone.session_id(), "stub-1");

        // Publishing through the original sender reaches subscribers that
        // subscribed via the clone.
        let mut rx_via_clone = clone.events();
        tx.send(AdapterEvent::TextDelta("hello".to_owned()))
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            AdapterEvent::TextDelta("hello".to_owned())
        );
        assert_eq!(
            rx_via_clone.recv().await.unwrap(),
            AdapterEvent::TextDelta("hello".to_owned())
        );

        assert!(handle.send_instruction("go".to_owned()).await.is_ok());
        assert!(clone.cancel().await.is_ok());
    }
}
