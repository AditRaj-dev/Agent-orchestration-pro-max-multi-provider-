//! Credential-free mock adapter (F-02) — the reference
//! [`crate::RuntimeAdapter`] implementation that makes end-to-end runs
//! possible from day one (F-00 §6 provider-mix table).
//!
//! Deterministic by construction: no randomness, no wall-clock dependence
//! in the event script, token counts derived from arithmetic on the spec.
//! The usage snapshot pins `session_overhead_tokens` to the Claude-observed
//! ~22k preamble (handoff claude T5: cache-read 22115, core system prompt —
//! not user skills) so downstream budget-ledger code exercises the
//! per-session overhead line against realistic numbers.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::broadcast;

use crate::adapter::{RuntimeAdapter, SessionBackend, SessionHandle};
use crate::error::{AdapterError, AdapterFailure};
use crate::events::AdapterEvent;
use crate::types::{
    AuthStatus, Capabilities, ModelUsageRow, RuntimeInfo, SpawnSpec, UsageSnapshot,
};

/// The canned model identity the mock reports.
pub const MOCK_MODEL: &str = "mock-model-1";
/// Fixed per-session preamble the mock bills, matching the Claude-observed
/// overhead class (F-00 §4: ~22k tokens; agy is ~37k).
pub const MOCK_SESSION_OVERHEAD_TOKENS: u64 = 22_000;
/// Context window the mock advertises for [`MOCK_MODEL`] (claude
/// `modelUsage[].contextWindow` observed at 1M).
pub const MOCK_CONTEXT_WINDOW: u64 = 1_000_000;

/// Scripted behavior of a [`MockAdapter`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MockBehavior {
    /// Every session streams a full success lifecycle and finishes with a
    /// completion packet referencing `files_changed`.
    Success {
        /// Number of streamed turns (deltas + one tool use per turn).
        turns: usize,
        /// File paths reported in the completion packet.
        files_changed: Vec<String>,
    },
    /// Every session fails with the given classified failure. A
    /// [`AdapterFailure::SpawnFailure`] skips the `Started` event (the
    /// process died before the session came up).
    FailWith(AdapterFailure),
    /// The first `failures_before_success` sessions fail with a retryable
    /// transient (preceded by a `RateLimit` notice, mirroring the observed
    /// claude `rate_limit_event` shape); every later session succeeds.
    /// Counting is per adapter instance, across `start_session` calls —
    /// the exact path a supervisor's retry loop takes.
    FlakyThenSuccess {
        /// How many leading sessions fail before the first success.
        failures_before_success: usize,
    },
}

/// The mock adapter: canned discovery surfaces, scripted sessions,
/// deterministic token arithmetic.
pub struct MockAdapter {
    behavior: MockBehavior,
    attempts: AtomicUsize,
    next_session: AtomicUsize,
    sessions: Mutex<Vec<SessionHandle>>,
}

impl MockAdapter {
    /// Create a mock adapter with the given scripted behavior.
    pub fn new(behavior: MockBehavior) -> Self {
        Self {
            behavior,
            attempts: AtomicUsize::new(0),
            next_session: AtomicUsize::new(0),
            sessions: Mutex::new(Vec::new()),
        }
    }

    /// The scripted behavior.
    pub fn behavior(&self) -> &MockBehavior {
        &self.behavior
    }
}

#[async_trait]
impl RuntimeAdapter for MockAdapter {
    fn id(&self) -> &str {
        "mock"
    }

    async fn detect(&self) -> RuntimeInfo {
        RuntimeInfo {
            id: "mock".to_owned(),
            version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            path: None,
        }
    }

    async fn auth_status(&self) -> AuthStatus {
        AuthStatus::Ready
    }

    async fn capabilities(&self) -> Capabilities {
        Capabilities::full()
    }

    async fn start_session(&self, spec: SpawnSpec) -> Result<SessionHandle, AdapterError> {
        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed) + 1;
        let session_no = self.next_session.fetch_add(1, Ordering::Relaxed) + 1;
        let session_id = format!("mock-{session_no}");

        let (events, _) = broadcast::channel(256);
        let state = Arc::new(SessionState {
            cancelled: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        });
        let backend = Arc::new(MockSessionBackend {
            session_id: session_id.clone(),
            state: state.clone(),
        });
        let handle = SessionHandle::new(session_id.clone(), events.clone(), backend);

        self.sessions
            .lock()
            .expect("mock session registry poisoned")
            .push(handle.clone());

        let behavior = self.behavior.clone();
        let driver_id = session_id;
        tokio::spawn(async move {
            drive_session(&driver_id, &spec, behavior, attempt, &events, &state).await;
        });

        Ok(handle)
    }

    async fn shutdown(&self) -> Result<(), AdapterError> {
        let sessions: Vec<SessionHandle> = self
            .sessions
            .lock()
            .expect("mock session registry poisoned")
            .clone();
        for handle in &sessions {
            // Best-effort: cancel idempotently, never fail shutdown.
            let _ = handle.cancel().await;
        }
        Ok(())
    }
}

/// Shared mutable state of one mock session.
struct SessionState {
    cancelled: AtomicBool,
    finished: AtomicBool,
}

/// [`SessionBackend`] for a mock session.
struct MockSessionBackend {
    session_id: String,
    state: Arc<SessionState>,
}

#[async_trait]
impl SessionBackend for MockSessionBackend {
    async fn send_instruction(&self, text: String) -> Result<(), AdapterError> {
        if self.state.finished.load(Ordering::Relaxed)
            || self.state.cancelled.load(Ordering::Relaxed)
        {
            return Err(AdapterError::SessionNotActive(self.session_id.clone()));
        }
        // The mock's runs are fully scripted from the `SpawnSpec`, so
        // instructions are accepted (API surface stays exercisable) but do
        // not perturb the deterministic script.
        tracing::debug!(session = %self.session_id, instruction = %text,
            "mock session accepted instruction (scripted run: no effect)");
        Ok(())
    }

    async fn cancel(&self) -> Result<(), AdapterError> {
        self.state.cancelled.store(true, Ordering::Relaxed);
        Ok(())
    }
}

/// Publish one event unless the session was cancelled first. Returns
/// `false` when the script must stop (process-kill semantics: the stream
/// just ends, with no terminal event).
fn emit(
    events: &broadcast::Sender<AdapterEvent>,
    state: &SessionState,
    event: AdapterEvent,
) -> bool {
    if state.cancelled.load(Ordering::Relaxed) {
        return false;
    }
    // A send error means nobody is listening anymore; the script continues
    // so the run still reaches its terminal state for the ledger.
    let _ = events.send(event);
    true
}

/// The session driver. Runs the scripted lifecycle once at least one
/// subscriber exists.
///
/// Broadcast has no replay, so the script holds until someone subscribes:
/// a caller that subscribes immediately after `start_session` observes the
/// full stream from `Started` onward. Cancel/shutdown releases the wait.
async fn drive_session(
    session_id: &str,
    spec: &SpawnSpec,
    behavior: MockBehavior,
    attempt: usize,
    events: &broadcast::Sender<AdapterEvent>,
    state: &SessionState,
) {
    while events.receiver_count() == 0 {
        if state.cancelled.load(Ordering::Relaxed) {
            return;
        }
        // Async yield (not a blocking spin): the waiting task must not
        // starve the runtime — the subscriber it waits for runs on it.
        tokio::task::yield_now().await;
    }

    match behavior {
        MockBehavior::Success {
            turns,
            files_changed,
        } => {
            run_success(session_id, spec, turns, &files_changed, events, state);
        }
        MockBehavior::FailWith(failure) => {
            run_failure(session_id, events, state, failure);
        }
        MockBehavior::FlakyThenSuccess {
            failures_before_success,
        } => {
            if attempt <= failures_before_success {
                // Observed claude shape: the session starts, a rate-limit
                // notice fires mid-run, then the run fails transiently.
                if emit_started(session_id, events, state) {
                    emit(
                        events,
                        state,
                        AdapterEvent::RateLimit {
                            provider_notice: json!({
                                "adapter": "mock",
                                "attempt": attempt,
                                "reason": "simulated provider rate limit",
                            }),
                        },
                    );
                    fail(
                        events,
                        state,
                        AdapterFailure::Transient {
                            detail: format!(
                                "mock flake {attempt} of {failures_before_success}: \
                             simulated rate limit"
                            ),
                        },
                    );
                }
            } else {
                run_success(session_id, spec, 1, &[], events, state);
            }
        }
    }
}

/// Stream the success lifecycle:
/// `Started` -> per turn (`TextDelta` x2 + `ToolUse`) -> `UsageUpdate` ->
/// `Finished` with a completion packet.
fn run_success(
    session_id: &str,
    spec: &SpawnSpec,
    turns: usize,
    files_changed: &[String],
    events: &broadcast::Sender<AdapterEvent>,
    state: &SessionState,
) {
    if !emit_started(session_id, events, state) {
        return;
    }

    for turn in 1..=turns {
        if !emit(
            events,
            state,
            AdapterEvent::TextDelta(format!(
                "turn {turn}/{turns}: planning changes for `{}`",
                spec.objective
            )),
        ) {
            return;
        }
        if !emit(
            events,
            state,
            AdapterEvent::TextDelta(format!(
                "turn {turn}/{turns}: applying edits under {}",
                spec.workspace.display()
            )),
        ) {
            return;
        }
        let args_summary = match files_changed.len() {
            0 => format!("edit workspace {}", spec.workspace.display()),
            n => format!("edit {}", files_changed[(turn - 1) % n]),
        };
        if !emit(
            events,
            state,
            AdapterEvent::ToolUse {
                tool: "Edit".to_owned(),
                args_summary,
            },
        ) {
            return;
        }
    }

    if !emit(
        events,
        state,
        AdapterEvent::UsageUpdate(usage_snapshot(turns)),
    ) {
        return;
    }

    let summary = format!(
        "completed `{}` in {turns} turn(s), {} file(s) changed",
        spec.objective,
        files_changed.len()
    );
    let structured = json!({
        "summary": summary,
        "filesChanged": files_changed,
        "tests": {
            "status": "skipped",
            "reason": "mock adapter never executes tests",
        },
    });
    if !emit(
        events,
        state,
        AdapterEvent::Finished {
            exit_code: 0,
            final_result: Some(summary),
            structured: Some(structured),
        },
    ) {
        return;
    }
    state.finished.store(true, Ordering::Relaxed);
}

/// Emit the `Started` event. Returns `false` if the session was cancelled
/// first and the script must stop.
fn emit_started(
    session_id: &str,
    events: &broadcast::Sender<AdapterEvent>,
    state: &SessionState,
) -> bool {
    emit(
        events,
        state,
        AdapterEvent::Started {
            session_id: session_id.to_owned(),
            model: Some(MOCK_MODEL.to_owned()),
        },
    )
}

/// Terminal failure: emit `Failed` and mark the session finished.
fn fail(events: &broadcast::Sender<AdapterEvent>, state: &SessionState, failure: AdapterFailure) {
    if !emit(events, state, AdapterEvent::Failed(failure)) {
        return;
    }
    state.finished.store(true, Ordering::Relaxed);
}

/// Stream a failure lifecycle: `Started` (skipped for spawn-class deaths,
/// where the process died before the session came up) then `Failed`.
fn run_failure(
    session_id: &str,
    events: &broadcast::Sender<AdapterEvent>,
    state: &SessionState,
    failure: AdapterFailure,
) {
    if !matches!(failure, AdapterFailure::SpawnFailure { .. })
        && !emit_started(session_id, events, state)
    {
        return;
    }
    fail(events, state, failure);
}

/// Deterministic token arithmetic for a `turns`-turn success run. The
/// overhead line pins the Claude-observed fixed preamble; per canon
/// (claude T5) that preamble bills as cache-read tokens, so
/// `cache_read_tokens` equals the overhead here.
fn usage_snapshot(turns: usize) -> UsageSnapshot {
    let turns = turns as u64;
    let input = 1_000 + 500 * turns;
    let output = 250 * turns;
    let thinking = 64 * turns;
    let cache_read = MOCK_SESSION_OVERHEAD_TOKENS;
    UsageSnapshot {
        input_tokens: input,
        output_tokens: output,
        thinking_tokens: thinking,
        cache_read_tokens: cache_read,
        total_tokens: input + output + thinking + cache_read,
        cost_usd: Some(0.02),
        per_model: vec![ModelUsageRow {
            model: MOCK_MODEL.to_owned(),
            cost_usd: Some(0.02),
            context_window: Some(MOCK_CONTEXT_WINDOW),
        }],
        session_overhead_tokens: MOCK_SESSION_OVERHEAD_TOKENS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Classifier;
    use crate::types::Capabilities;
    use chrono::Utc;
    use std::path::PathBuf;
    use std::time::Duration;
    use uuid::Uuid;

    fn spec() -> SpawnSpec {
        SpawnSpec {
            task_id: Uuid::new_v4(),
            objective: "implement the mocked feature".to_owned(),
            workspace: PathBuf::from("mock-workspace"),
            allowed_paths: vec![],
            forbidden_paths: vec![],
            tool_allowlist: vec![],
            tool_denylist: vec![],
            model: None,
            timeout_secs: 600,
            isolated_home: None,
        }
    }

    /// Collect a session stream until its terminal event (`Finished` /
    /// `Failed`) or channel close — whichever comes first. The handle keeps
    /// a sender alive for its whole lifetime, so a completed session ends
    /// on the terminal event, not on channel close. Times out instead of
    /// hanging.
    async fn collect_stream(rx: &mut broadcast::Receiver<AdapterEvent>) -> Vec<AdapterEvent> {
        let mut out = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Ok(event)) => {
                    let terminal = event.is_terminal();
                    out.push(event);
                    if terminal {
                        return out;
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(dropped))) => panic!(
                    "test subscriber lagged ({dropped} events dropped) — raise channel capacity"
                ),
                Ok(Err(broadcast::error::RecvError::Closed)) => return out,
                Err(_) => panic!("timed out waiting for mock session events"),
            }
        }
    }

    /// Assert the stream stays quiet (no events) for a short window. A
    /// closed channel also passes: the point is that nothing is emitted.
    async fn assert_quiet(rx: &mut broadcast::Receiver<AdapterEvent>) {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(event)) => panic!("expected a quiet stream, got {event:?}"),
            Ok(Err(broadcast::error::RecvError::Lagged(dropped))) => {
                panic!("test subscriber lagged ({dropped} events dropped)");
            }
            Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => {}
        }
    }

    fn kinds(events: &[AdapterEvent]) -> Vec<&'static str> {
        events.iter().map(|event| event.kind()).collect()
    }

    #[tokio::test]
    async fn discovery_surfaces_are_canned_and_free() {
        // Prove object safety while exercising every discovery method.
        let adapter: Box<dyn RuntimeAdapter> = Box::new(MockAdapter::new(MockBehavior::Success {
            turns: 1,
            files_changed: vec![],
        }));

        assert_eq!(adapter.id(), "mock");
        assert_eq!(adapter.auth_status().await, AuthStatus::Ready);
        assert_eq!(adapter.capabilities().await, Capabilities::full());

        let health = adapter.health().await;
        assert_eq!(health.auth, AuthStatus::Ready);
        assert_eq!(health.runtime.id, "mock");
        assert!(health.runtime.version.is_some());
        assert!(health.checked_at <= Utc::now());
    }

    #[tokio::test]
    async fn success_lifecycle_streams_ordered_events_with_completion_packet() {
        let adapter = MockAdapter::new(MockBehavior::Success {
            turns: 2,
            files_changed: vec!["src/lib.rs".to_owned(), "src/main.rs".to_owned()],
        });
        let handle = adapter.start_session(spec()).await.unwrap();
        assert!(handle.session_id().starts_with("mock-"));
        let mut rx = handle.events();
        let events = collect_stream(&mut rx).await;

        assert_eq!(
            kinds(&events),
            vec![
                "started",
                "text_delta",
                "text_delta",
                "tool_use",
                "text_delta",
                "text_delta",
                "tool_use",
                "usage_update",
                "finished",
            ],
            "event ordering is the scripted lifecycle"
        );

        match events.first() {
            Some(AdapterEvent::Started { model, .. }) => {
                assert_eq!(model.as_deref(), Some(MOCK_MODEL));
            }
            other => panic!("expected Started first, got {other:?}"),
        }
        assert_eq!(
            events[3],
            AdapterEvent::ToolUse {
                tool: "Edit".to_owned(),
                args_summary: "edit src/lib.rs".to_owned(),
            }
        );

        // Usage snapshot carries the overhead ledger line and a per-model row.
        match &events[7] {
            AdapterEvent::UsageUpdate(usage) => {
                assert_eq!(usage.session_overhead_tokens, 22_000);
                assert_eq!(usage.cache_read_tokens, 22_000);
                assert_eq!(usage.input_tokens, 2_000);
                assert_eq!(
                    usage.total_tokens,
                    usage.input_tokens
                        + usage.output_tokens
                        + usage.thinking_tokens
                        + usage.cache_read_tokens
                );
                assert_eq!(usage.per_model.len(), 1);
                assert_eq!(usage.per_model[0].model, MOCK_MODEL);
                assert_eq!(usage.per_model[0].context_window, Some(1_000_000));
            }
            other => panic!("expected UsageUpdate before Finished, got {other:?}"),
        }

        // Completion packet.
        match events.last() {
            Some(AdapterEvent::Finished {
                exit_code,
                final_result,
                structured,
            }) => {
                assert_eq!(*exit_code, 0);
                let summary = final_result.as_deref().expect("final result text");
                assert!(summary.contains("2 turn(s)"), "summary: {summary}");
                let structured = structured.as_ref().expect("structured packet");
                assert_eq!(structured["filesChanged"][0], "src/lib.rs");
                assert_eq!(structured["filesChanged"][1], "src/main.rs");
                assert_eq!(structured["tests"]["status"], "skipped");
                assert_eq!(
                    structured["summary"].as_str().expect("summary text"),
                    summary
                );
            }
            other => panic!("expected Finished terminal event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fail_with_streams_started_then_classified_failure() {
        let failure = AdapterFailure::BillingFailure {
            provider_code: Some(Classifier::PROVIDER_CODE_BILLING_INSUFFICIENT),
            detail: "insufficient balance".to_owned(),
        };
        let adapter = MockAdapter::new(MockBehavior::FailWith(failure.clone()));
        let handle = adapter.start_session(spec()).await.unwrap();
        let mut rx = handle.events();
        let events = collect_stream(&mut rx).await;

        assert_eq!(kinds(&events), vec!["started", "failed"]);
        assert_eq!(events.last(), Some(&AdapterEvent::Failed(failure)));

        // The session is dead: further instructions are rejected.
        let err = handle.send_instruction("again".to_owned()).await;
        assert!(matches!(err, Err(AdapterError::SessionNotActive(_))));
    }

    #[test]
    fn fail_with_spawn_failure_skips_started_event() {
        // Sync shape check of the script helper: a spawn-class failure must
        // not emit Started (the process died before the session came up).
        let (events, _) = broadcast::channel(16);
        let state = SessionState {
            cancelled: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        };
        let mut rx = events.subscribe();
        run_failure(
            "mock-spawn",
            &events,
            &state,
            AdapterFailure::SpawnFailure {
                detail: "binary missing".to_owned(),
            },
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            AdapterEvent::Failed(AdapterFailure::SpawnFailure {
                detail: "binary missing".to_owned()
            })
        );
        assert!(state.finished.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn flaky_then_success_fails_transiently_then_recovers() {
        let adapter = MockAdapter::new(MockBehavior::FlakyThenSuccess {
            failures_before_success: 2,
        });

        for expected_attempt in 1..=2 {
            let handle = adapter.start_session(spec()).await.unwrap();
            let mut rx = handle.events();
            let events = collect_stream(&mut rx).await;

            assert_eq!(
                kinds(&events),
                vec!["started", "rate_limit", "failed"],
                "attempt {expected_attempt}"
            );
            match events.last() {
                Some(AdapterEvent::Failed(failure)) => {
                    assert_eq!(failure.kind(), "transient");
                    assert!(failure.is_retryable());
                }
                other => panic!("attempt {expected_attempt}: expected Failed, got {other:?}"),
            }
        }

        // Third attempt succeeds and terminates with a completion packet.
        let handle = adapter.start_session(spec()).await.unwrap();
        let mut rx = handle.events();
        let events = collect_stream(&mut rx).await;
        assert_eq!(events.last().map(|event| event.kind()), Some("finished"));
    }

    #[tokio::test]
    async fn cancel_before_any_event_terminates_stream_without_terminal_event() {
        let adapter = MockAdapter::new(MockBehavior::Success {
            turns: 1,
            files_changed: vec![],
        });
        let handle = adapter.start_session(spec()).await.unwrap();

        // Cancel while the driver is still waiting for a subscriber:
        // deterministic — the script never starts. The channel stays open
        // (the handle owns a sender), so "quiet for a while" is the
        // observable proof that no event — terminal or otherwise — was
        // emitted.
        handle.cancel().await.unwrap();
        let mut rx = handle.events();
        assert_quiet(&mut rx).await;

        assert!(matches!(
            handle.send_instruction("too late".to_owned()).await,
            Err(AdapterError::SessionNotActive(_))
        ));
    }

    #[tokio::test]
    async fn shutdown_cancels_every_live_session() {
        let adapter = MockAdapter::new(MockBehavior::Success {
            turns: 3,
            files_changed: vec![],
        });
        let first = adapter.start_session(spec()).await.unwrap();
        let second = adapter.start_session(spec()).await.unwrap();

        adapter.shutdown().await.unwrap();

        // Both drivers were parked waiting for subscribers; cancel releases
        // them before the script starts, so neither stream emits anything
        // (no terminal events) and both sessions are inactive.
        for handle in [first, second] {
            let mut rx = handle.events();
            assert_quiet(&mut rx).await;
            assert!(matches!(
                handle.send_instruction("post-shutdown".to_owned()).await,
                Err(AdapterError::SessionNotActive(_))
            ));
        }
    }

    #[tokio::test]
    async fn behavior_accessor_reports_the_script() {
        let behavior = MockBehavior::Success {
            turns: 4,
            files_changed: vec![],
        };
        let adapter = MockAdapter::new(behavior.clone());
        assert_eq!(adapter.behavior(), &behavior);
    }
}
