//! Adapter error taxonomy and the run classifier (F-02 canon).
//!
//! Two layers, deliberately separate:
//!
//! - [`AdapterFailure`] answers *why a provider run ended badly*. The
//!   variant set is distilled from observed provider evidence (handoff §3 +
//!   addenda), not from vendor docs: typed provider business errors
//!   (zcode `ProviderBusinessError` with `providerCode`), observable
//!   denial events (agy "user denied permission", claude
//!   `permission_denials`), rate-limit notices (claude `rate_limit_event`),
//!   and the exit-code/final-event classification rule (F-00 §4: never
//!   stderr text).
//! - [`AdapterError`] covers failures of the adapter machinery itself — a
//!   session already finished, or internal adapter state.
//!
//! [`Classifier`] turns the two signals every CLI reliably provides (exit
//! code + presence of a final result event) plus optional typed provider
//! codes and an observable-denial flag into an [`AdapterFailure`].

use serde::{Deserialize, Serialize};

/// Why a provider run ended badly. Variant set + evidence:
///
/// | Variant | Observed origin (handoff) |
/// |---|---|
/// | `AuthFailure` | RT-04 reauthentication; HTTP 401/403 surfaced through typed provider errors (zcode `responseStatus`, claude `api_error_status`) |
/// | `BillingFailure` | zcode `ProviderBusinessError [1113]` insufficient balance (session 4) |
/// | `BotGateOrCaptcha` | zcode `ProviderBusinessError [3007]` captcha verify failed — provider anti-bot, not locally fixable (session 4) |
/// | `PolicyDenial` | agy headless accept-edits denies shell with explicit "user denied permission"; claude `permission_denials` field (§3.1) |
/// | `Transient` | claude `rate_limit_event` at 0.86 seven-day utilization; codex usage limit until Sep 10 — retry later, never misdiagnose as capability failure (§7.8) |
/// | `TaskFailure` | agy `status: ERROR` with a result event present and exit 2 — the model round-tripped and still failed (agy battery) |
/// | `SpawnFailure` | pre-model deaths: claude variadic-flag exit 1 with zero events (§3.2), codex exit 2 "unexpected argument" and session-init death exit 1 (§3.3) |
#[derive(Debug, Clone, PartialEq, Eq, Hash, thiserror::Error, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AdapterFailure {
    /// Credentials missing, expired, or rejected. Blocking; requires user
    /// action (login), never an automatic retry.
    #[error("authentication failure: {detail}")]
    AuthFailure {
        /// What the adapter observed.
        detail: String,
    },
    /// Provider account has no balance or resource pack (zcode code 1113).
    /// Blocking; requires recharge, never an automatic retry.
    #[error("billing failure (provider code {provider_code:?}): {detail}")]
    BillingFailure {
        /// Typed provider business code when the provider supplied one
        /// (observed: zcode `1113`).
        provider_code: Option<i64>,
        /// What the adapter observed.
        detail: String,
    },
    /// Provider anti-bot wall or captcha challenge (zcode code 3007).
    /// Blocking by design — do not attempt to bypass (handoff session 4).
    #[error("bot-gate/captcha failure (provider code {provider_code:?}): {detail}")]
    BotGateOrCaptcha {
        /// Typed provider business code when the provider supplied one
        /// (observed: zcode `3007`).
        provider_code: Option<i64>,
        /// What the adapter observed.
        detail: String,
    },
    /// The runtime refused a tool/action for policy reasons, observable as
    /// a denial *event* (never inferred from stderr text).
    #[error("policy denial: {detail}")]
    PolicyDenial {
        /// Which denial was observed.
        detail: String,
    },
    /// Rate limit / quota / transient provider condition. The only class
    /// that is automatically retryable (with backoff).
    #[error("transient provider condition (retryable): {detail}")]
    Transient {
        /// What the adapter observed.
        detail: String,
    },
    /// The model ran and produced a final result, but the run failed on its
    /// own merits (non-zero exit with a final result event present).
    #[error("task failure (model tried and failed): {detail}")]
    TaskFailure {
        /// Why the run failed.
        detail: String,
    },
    /// The session never reached a model round-trip: process died before
    /// any final result event (usage/flag errors, session-init death).
    #[error("spawn failure: {detail}")]
    SpawnFailure {
        /// Why the spawn died.
        detail: String,
    },
}

impl AdapterFailure {
    /// Stable machine-readable kind, one per variant, for journal payloads
    /// and metrics labels.
    pub fn kind(&self) -> &'static str {
        match self {
            AdapterFailure::AuthFailure { .. } => "auth_failure",
            AdapterFailure::BillingFailure { .. } => "billing_failure",
            AdapterFailure::BotGateOrCaptcha { .. } => "bot_gate_or_captcha",
            AdapterFailure::PolicyDenial { .. } => "policy_denial",
            AdapterFailure::Transient { .. } => "transient",
            AdapterFailure::TaskFailure { .. } => "task_failure",
            AdapterFailure::SpawnFailure { .. } => "spawn_failure",
        }
    }

    /// Whether a retry of the *same* session under the *same* conditions
    /// can plausibly succeed.
    ///
    /// Only [`AdapterFailure::Transient`] qualifies: rate limits and quota
    /// windows clear on their own (claude `rate_limit_event`, codex usage
    /// limit). [`AdapterFailure::SpawnFailure`] is deliberately **not**
    /// retryable automatically: every observed pre-model death was
    /// deterministic in its environment (missing binary, unsupported flag,
    /// config-pinned model) — a blind retry re-hits the same wall and hides
    /// the root cause. Remediate the environment, then start a fresh
    /// session.
    pub fn is_retryable(&self) -> bool {
        matches!(self, AdapterFailure::Transient { .. })
    }
}

/// Errors of the adapter machinery itself (as opposed to classified run
/// failures, which stream as [`crate::AdapterEvent::Failed`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdapterError {
    /// An operation referenced a classified provider failure.
    #[error("session failed: {0}")]
    SessionFailed(#[from] AdapterFailure),
    /// The session already finished or was cancelled; no further
    /// instructions are accepted.
    #[error("session {0} is not active")]
    SessionNotActive(String),
    /// Adapter-internal infrastructure error (channel, process plumbing).
    #[error("adapter internal error: {0}")]
    Internal(String),
}

impl AdapterError {
    /// Delegates to [`AdapterFailure::is_retryable`] for classified
    /// failures; machinery errors are never automatically retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            AdapterError::SessionFailed(failure) => failure.is_retryable(),
            AdapterError::SessionNotActive(_) | AdapterError::Internal(_) => false,
        }
    }
}

/// Classifies a finished CLI run from machine-reliable signals only:
/// exit code, presence of a final result event, an optional typed provider
/// code, and whether an observable denial event occurred.
///
/// Binding rule (F-00 §4, handoff §7.5): **never** classify by matching
/// stderr text — CLI stderr is wall-to-wall noise (codex skill-load
/// errors are non-fatal) and localized. Exit code + final-event presence
/// is the contract every observed CLI honors.
pub struct Classifier;

impl Classifier {
    /// zcode `ProviderBusinessError [1113]`: insufficient balance or no
    /// usable resource pack (observed, session 4).
    pub const PROVIDER_CODE_BILLING_INSUFFICIENT: i64 = 1113;
    /// zcode `ProviderBusinessError [3007]`: captcha verify failed
    /// (observed, session 4 — provider anti-bot).
    pub const PROVIDER_CODE_CAPTCHA_VERIFY_FAILED: i64 = 3007;

    /// HTTP 401 as surfaced by typed provider errors (claude
    /// `api_error_status`, zcode `responseStatus`).
    const HTTP_UNAUTHORIZED: i64 = 401;
    /// HTTP 403 as surfaced by typed provider errors.
    const HTTP_FORBIDDEN: i64 = 403;
    /// HTTP 429 rate limiting as surfaced by typed provider errors.
    const HTTP_TOO_MANY_REQUESTS: i64 = 429;

    /// Success rule (F-00 §4): exit code 0 **and** a final result event was
    /// observed. Either signal alone is not success — an exit 0 without a
    /// final result is a malformed run, and a final result with a non-zero
    /// exit is a failed run (agy `ERROR` -> exit 2 with result present).
    pub fn is_success(exit_code: i32, saw_final_result_event: bool) -> bool {
        exit_code == 0 && saw_final_result_event
    }

    /// Classify a run that did not succeed. Precondition: callers gate on
    /// [`Classifier::is_success`] first; calling `classify` on success
    /// inputs returns a [`AdapterFailure::TaskFailure`] whose detail says
    /// so (defensive, never a panic).
    ///
    /// Precedence ladder, most-reliable signal first:
    ///
    /// 1. Typed provider business code (zcode-style `providerCode`, or an
    ///    HTTP status surfaced by a typed error): 1113 -> billing, 3007 ->
    ///    bot-gate, 401/403 -> auth, 429 -> transient. Unknown codes fall
    ///    through to the ladder and are preserved in the detail string.
    /// 2. Observable denial event -> [`AdapterFailure::PolicyDenial`].
    /// 3. No final result event -> [`AdapterFailure::SpawnFailure`]
    ///    (pre-model death: usage errors exit 2, session-init deaths exit 1
    ///    — handoff §3.2/§3.3/§7.3).
    /// 4. Final result event present but non-zero exit ->
    ///    [`AdapterFailure::TaskFailure`] (model tried and failed).
    pub fn classify(
        exit_code: i32,
        saw_final_result_event: bool,
        provider_code: Option<i64>,
        denial_observed: bool,
    ) -> AdapterFailure {
        // 1. Typed codes outrank everything: they are provider-authored and
        //    machine-parsed, the strongest evidence available.
        let mut prefix = String::new();
        if let Some(code) = provider_code {
            match code {
                Self::PROVIDER_CODE_BILLING_INSUFFICIENT => {
                    return AdapterFailure::BillingFailure {
                        provider_code: Some(code),
                        detail: "provider reports insufficient balance or no usable resource pack"
                            .to_owned(),
                    };
                }
                Self::PROVIDER_CODE_CAPTCHA_VERIFY_FAILED => {
                    return AdapterFailure::BotGateOrCaptcha {
                        provider_code: Some(code),
                        detail: "provider anti-bot/captcha gate rejected the request".to_owned(),
                    };
                }
                Self::HTTP_UNAUTHORIZED | Self::HTTP_FORBIDDEN => {
                    return AdapterFailure::AuthFailure {
                        detail: format!("provider rejected credentials (status {code})"),
                    };
                }
                Self::HTTP_TOO_MANY_REQUESTS => {
                    return AdapterFailure::Transient {
                        detail: format!("provider rate limit (status {code})"),
                    };
                }
                unrecognized => {
                    prefix = format!("unrecognized provider business code {unrecognized}; ");
                }
            }
        }

        // 2. Denial events are observable structured output (agy "user
        //    denied permission" error, claude `permission_denials`), not
        //    stderr scraping.
        if denial_observed {
            return AdapterFailure::PolicyDenial {
                detail: format!("{prefix}observable permission-denial event"),
            };
        }

        // 3. No final result event: the process died before completing a
        //    model round-trip (claude variadic exit 1 with zero events;
        //    codex exit 2 "unexpected argument"; codex session-init death).
        if !saw_final_result_event {
            return AdapterFailure::SpawnFailure {
                detail: format!(
                    "{prefix}exited with code {exit_code} before any final result event"
                ),
            };
        }

        // 4. Final result present but the CLI still reports failure
        //    (agy `status: ERROR` -> exit 2 with the result event emitted).
        if exit_code != 0 {
            return AdapterFailure::TaskFailure {
                detail: format!("{prefix}final result event present but exit code {exit_code}"),
            };
        }

        AdapterFailure::TaskFailure {
            detail: format!(
                "{prefix}exit code 0 with final result event: run succeeded \
                 (gate on Classifier::is_success before calling classify)"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kind_of(exit: i32, final_event: bool, code: Option<i64>, denial: bool) -> &'static str {
        Classifier::classify(exit, final_event, code, denial).kind()
    }

    #[test]
    fn only_transient_is_retryable() {
        let failures = [
            AdapterFailure::AuthFailure {
                detail: "no credentials".to_owned(),
            },
            AdapterFailure::BillingFailure {
                provider_code: Some(1113),
                detail: "insufficient balance".to_owned(),
            },
            AdapterFailure::BotGateOrCaptcha {
                provider_code: Some(3007),
                detail: "captcha verify failed".to_owned(),
            },
            AdapterFailure::PolicyDenial {
                detail: "user denied permission".to_owned(),
            },
            AdapterFailure::Transient {
                detail: "rate limited".to_owned(),
            },
            AdapterFailure::TaskFailure {
                detail: "model failed".to_owned(),
            },
            AdapterFailure::SpawnFailure {
                detail: "flag parse".to_owned(),
            },
        ];
        for failure in &failures {
            assert_eq!(
                failure.is_retryable(),
                matches!(failure, AdapterFailure::Transient { .. }),
                "{failure:?}"
            );
        }
    }

    #[test]
    fn failure_wraps_into_adapter_error_and_delegates_retryability() {
        let transient = AdapterFailure::Transient {
            detail: "rate limited".to_owned(),
        };
        let error = AdapterError::from(transient);
        assert!(error.is_retryable());
        assert!(!AdapterError::SessionNotActive("mock-1".to_owned()).is_retryable());
        assert!(!AdapterError::Internal("channel closed".to_owned()).is_retryable());
    }

    #[test]
    fn is_success_requires_zero_exit_and_final_event() {
        assert!(Classifier::is_success(0, true));
        assert!(!Classifier::is_success(0, false), "no final result event");
        assert!(!Classifier::is_success(2, true), "agy ERROR shape");
        assert!(!Classifier::is_success(1, false));
    }

    /// The observed-evidence mapping table. Each row cites where the
    /// signal combination was actually seen (handoff section in comment).
    #[test]
    fn classifier_maps_the_observed_evidence_table() {
        /// (exit code, saw final result event, typed provider code,
        /// observable denial) -> expected failure kind.
        type Case = ((i32, bool, Option<i64>, bool), &'static str);

        let cases: Vec<Case> = vec![
            // codex `--ask-for-approval` exit 2, no final event (§3.3)
            ((2, false, None, false), "spawn_failure"),
            // claude variadic-flag death / codex session-init death:
            // exit 1, zero events (§3.2, §3.3)
            ((1, false, None, false), "spawn_failure"),
            // agy `status: ERROR` -> exit 2 with result event present (agy battery)
            ((2, true, None, false), "task_failure"),
            // zcode [1113] insufficient balance (session 4)
            (
                (
                    1,
                    false,
                    Some(Classifier::PROVIDER_CODE_BILLING_INSUFFICIENT),
                    false,
                ),
                "billing_failure",
            ),
            // zcode [3007] captcha verify failed (session 4)
            (
                (
                    1,
                    false,
                    Some(Classifier::PROVIDER_CODE_CAPTCHA_VERIFY_FAILED),
                    false,
                ),
                "bot_gate_or_captcha",
            ),
            // HTTP 401 surfaced through a typed provider error
            ((1, false, Some(401), false), "auth_failure"),
            // HTTP 429 rate limit surfaced through a typed provider error
            ((1, false, Some(429), false), "transient"),
            // agy headless accept-edits shell denial: observable event (agy battery)
            ((0, false, None, true), "policy_denial"),
            // claude `permission_denials` populated (§3.1 schema)
            ((1, true, None, true), "policy_denial"),
        ];
        for ((exit, final_event, code, denial), expected) in cases {
            assert_eq!(kind_of(exit, final_event, code, denial), expected);
        }
    }

    #[test]
    fn classifier_preserves_typed_provider_codes_on_specialized_variants() {
        let billing = Classifier::classify(1, false, Some(1113), false);
        assert!(matches!(
            billing,
            AdapterFailure::BillingFailure {
                provider_code: Some(Classifier::PROVIDER_CODE_BILLING_INSUFFICIENT),
                ..
            }
        ));

        let captcha = Classifier::classify(1, false, Some(3007), false);
        assert!(matches!(
            captcha,
            AdapterFailure::BotGateOrCaptcha {
                provider_code: Some(Classifier::PROVIDER_CODE_CAPTCHA_VERIFY_FAILED),
                ..
            }
        ));
    }

    #[test]
    fn unrecognized_provider_codes_fall_through_without_being_lost() {
        let failure = Classifier::classify(2, false, Some(778_899), false);
        assert_eq!(failure.kind(), "spawn_failure");
        assert!(
            failure.to_string().contains("778899"),
            "unrecognized code must survive in the detail: {failure}"
        );
    }

    #[test]
    fn classify_on_success_inputs_is_defensive_not_panicking() {
        let failure = Classifier::classify(0, true, None, false);
        assert_eq!(failure.kind(), "task_failure");
        assert!(failure.to_string().contains("succeeded"));
    }

    #[test]
    fn kinds_are_stable_wire_strings() {
        assert_eq!(
            AdapterFailure::Transient {
                detail: String::new()
            }
            .kind(),
            "transient"
        );
        assert_eq!(
            AdapterFailure::BotGateOrCaptcha {
                provider_code: None,
                detail: String::new()
            }
            .kind(),
            "bot_gate_or_captcha"
        );
    }
}
