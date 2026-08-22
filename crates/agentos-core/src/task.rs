//! Task lifecycle state machine (PRD §6.2).
//!
//! [`TaskState`] serializes as lowercase snake_case (`"review_pending"`,
//! `"human_required"`, ...) so it can live in a SQLite TEXT column and match
//! the snake_case style of [`crate::EventType`] wire strings.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Lifecycle state of a task (PRD §6.2), a conservative-but-recovering
/// state machine.
///
/// Legal transitions, as encoded by [`TaskState::can_transition`]:
///
/// ```text
/// happy path (PRD §6.2):
///   Created -> Planned -> Ready -> Leased -> Running -> OutputReady
///             -> ReviewPending -> Approved -> GitQueued -> Committed -> Done
///
/// early abort:
///   Created -> Cancelled                      (abandoned before planning)
///
/// lease expiry / requeue:
///   Leased -> Ready                           (lease expired, task re-queued)
///
/// failure branches, from any active state
/// S in {Planned, Ready, Leased, Running, OutputReady, ReviewPending,
///       Approved, GitQueued}:
///   S -> Blocked | Retryable | Failed | Cancelled | HumanRequired
///   (ReviewPending -> Retryable is the bounded-rework path after a failed
///    review; it is covered by this rule.)
///
/// recovery:
///   Blocked       -> Ready                    (dependency satisfied)
///   Retryable     -> Ready                    (re-queue under retry policy)
///   HumanRequired -> Ready                    (human approves resume/rework)
///   HumanRequired -> Approved                 (human signs off directly)
///
/// parking-state exits (prevent un-cancellable deadlocks):
///   {Blocked, Retryable, HumanRequired} -> Cancelled | Failed
///
/// terminal states (no outgoing arcs): Done, Failed, Cancelled
/// ```
///
/// Deliberate omissions: `Created` may only be planned or cancelled (nothing
/// has happened yet, so it cannot be blocked/failed/retried), and `Committed`
/// may only complete (the work is already in git, so it cannot divert). All
/// recovery re-enters the pipeline at `Ready`, replaying the full chain
/// (including review) rather than jumping ahead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// Entered the system; awaiting planning.
    Created,
    /// Planned; dependencies and contract known; not yet claimable.
    Planned,
    /// Claimable by a worker (all dependencies satisfied).
    Ready,
    /// A worker holds the lease; execution has not started.
    Leased,
    /// Executing.
    Running,
    /// Produced an output artifact; awaiting review handoff.
    OutputReady,
    /// A quality gate is evaluating the output.
    ReviewPending,
    /// Review passed; waiting for the serialized git queue.
    Approved,
    /// Queued in the git mutation queue.
    GitQueued,
    /// Mutation committed to the repository.
    Committed,
    /// Terminal success.
    Done,
    /// Parked: waiting on an unsatisfied dependency.
    Blocked,
    /// Parked: transient failure awaiting a bounded retry.
    Retryable,
    /// Parked: a human decision is required before any further mutation.
    HumanRequired,
    /// Terminal failure.
    Failed,
    /// Terminal: abandoned before completion.
    Cancelled,
}

impl TaskState {
    /// The canonical snake_case wire string for this state.
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskState::Created => "created",
            TaskState::Planned => "planned",
            TaskState::Ready => "ready",
            TaskState::Leased => "leased",
            TaskState::Running => "running",
            TaskState::OutputReady => "output_ready",
            TaskState::ReviewPending => "review_pending",
            TaskState::Approved => "approved",
            TaskState::GitQueued => "git_queued",
            TaskState::Committed => "committed",
            TaskState::Done => "done",
            TaskState::Blocked => "blocked",
            TaskState::Retryable => "retryable",
            TaskState::HumanRequired => "human_required",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
        }
    }

    /// Whether `self -> to` is a legal arc of the lifecycle graph documented
    /// on the enum. Terminal states have no outgoing arcs.
    pub fn can_transition(&self, to: &TaskState) -> bool {
        if self == to {
            return false;
        }
        use TaskState::*;
        matches!(
            (self, to),
            // happy path (PRD §6.2)
            (Created, Planned)
                | (Planned, Ready)
                | (Ready, Leased)
                | (Leased, Running)
                | (Running, OutputReady)
                | (OutputReady, ReviewPending)
                | (ReviewPending, Approved)
                | (Approved, GitQueued)
                | (GitQueued, Committed)
                | (Committed, Done)
                // early abort
                | (Created, Cancelled)
                // lease expiry / requeue
                | (Leased, Ready)
                // failure branches from any active state
                | (
                    Planned | Ready | Leased | Running | OutputReady
                        | ReviewPending | Approved | GitQueued,
                    Blocked | Retryable | Failed | Cancelled | HumanRequired
                )
                // recovery
                | (Blocked, Ready)
                | (Retryable, Ready)
                | (HumanRequired, Ready)
                | (HumanRequired, Approved)
                // parking-state exits (prevent un-cancellable deadlocks)
                | (Blocked | Retryable | HumanRequired, Cancelled | Failed)
        )
    }

    /// Whether this state is terminal: no further transitions may occur.
    /// Terminal states are `Done`, `Failed`, and `Cancelled`.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Done | TaskState::Failed | TaskState::Cancelled
        )
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use TaskState::*;

    fn all_states() -> Vec<TaskState> {
        vec![
            Created,
            Planned,
            Ready,
            Leased,
            Running,
            OutputReady,
            ReviewPending,
            Approved,
            GitQueued,
            Committed,
            Done,
            Blocked,
            Retryable,
            Failed,
            Cancelled,
            HumanRequired,
        ]
    }

    #[test]
    fn happy_path_chain_is_legal() {
        let chain = [
            Created,
            Planned,
            Ready,
            Leased,
            Running,
            OutputReady,
            ReviewPending,
            Approved,
            GitQueued,
            Committed,
            Done,
        ];
        for pair in chain.windows(2) {
            assert!(
                pair[0].can_transition(&pair[1]),
                "{} -> {} should be legal",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn active_states_may_divert_to_every_failure_branch() {
        let active = [
            Planned,
            Ready,
            Leased,
            Running,
            OutputReady,
            ReviewPending,
            Approved,
            GitQueued,
        ];
        let branches = [Blocked, Retryable, Failed, Cancelled, HumanRequired];
        for from in active {
            for to in branches {
                assert!(from.can_transition(&to), "{from} -> {to} should be legal");
            }
        }
    }

    #[test]
    fn recovery_requeue_and_early_abort_arcs_are_legal() {
        assert!(Blocked.can_transition(&Ready));
        assert!(Retryable.can_transition(&Ready));
        assert!(HumanRequired.can_transition(&Ready));
        assert!(HumanRequired.can_transition(&Approved));
        assert!(
            Leased.can_transition(&Ready),
            "lease expiry re-queues at Ready"
        );
        assert!(
            ReviewPending.can_transition(&Retryable),
            "failed review reworks"
        );
        assert!(Created.can_transition(&Cancelled), "abort before planning");
    }

    #[test]
    fn parking_states_may_be_abandoned() {
        for from in [Blocked, Retryable, HumanRequired] {
            assert!(from.can_transition(&Cancelled), "{from} -> cancelled");
            assert!(from.can_transition(&Failed), "{from} -> failed");
        }
    }

    #[test]
    fn illegal_transitions_are_rejected() {
        assert!(!Done.can_transition(&Running), "terminal must not revive");
        assert!(!Created.can_transition(&Done), "no skipping the pipeline");
        assert!(!Created.can_transition(&Running), "must be planned first");
        assert!(
            !Planned.can_transition(&Leased),
            "must become Ready to be leased"
        );
        assert!(!Blocked.can_transition(&Running), "recover via Ready only");
        assert!(
            !Approved.can_transition(&Running),
            "approved work does not rerun"
        );
        assert!(
            !Committed.can_transition(&Failed),
            "committed work only completes"
        );
        assert!(!Failed.can_transition(&Ready));
        assert!(!Cancelled.can_transition(&Ready));
        assert!(!Done.can_transition(&Done), "no self-loops");
        assert!(!Running.can_transition(&Running), "no self-loops");
    }

    #[test]
    fn terminal_states_have_no_outgoing_arcs() {
        for from in [Done, Failed, Cancelled] {
            for to in all_states() {
                assert!(
                    !from.can_transition(&to),
                    "{from} is terminal; {from} -> {to} must be illegal"
                );
            }
        }
    }

    #[test]
    fn is_terminal_marks_exactly_done_failed_cancelled() {
        for state in all_states() {
            let expected = matches!(state, Done | Failed | Cancelled);
            assert_eq!(state.is_terminal(), expected, "{state}");
        }
    }

    #[test]
    fn task_state_serializes_as_snake_case() {
        for (state, wire) in [
            (Created, "created"),
            (OutputReady, "output_ready"),
            (ReviewPending, "review_pending"),
            (GitQueued, "git_queued"),
            (HumanRequired, "human_required"),
            (Retryable, "retryable"),
            (Cancelled, "cancelled"),
            (Failed, "failed"),
            (Done, "done"),
        ] {
            assert_eq!(serde_json::to_value(state).unwrap(), json!(wire));
            assert_eq!(state.as_str(), wire);
            assert_eq!(state.to_string(), wire);
            let parsed: TaskState = serde_json::from_value(json!(wire)).unwrap();
            assert_eq!(parsed, state);
        }
    }
}
