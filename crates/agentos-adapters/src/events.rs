//! Adapter event model (F-02): the streaming surface of one session.
//!
//! [`AdapterEvent`] is the normalized stream every adapter publishes; the
//! daemon (F-01) translates it into journal `agentos_core::Event`s. Wire
//! form is adjacently tagged (`{"type": "...", "data": ...}`) with camelCase
//! variant names, so new variants and unknown future fields stay
//! forward-compatible.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::AdapterFailure;
use crate::types::UsageSnapshot;

/// One streamed event from a live adapter session.
///
/// Variant origins (observed provider evidence, handoff §3 + addenda):
///
/// - `Started` — claude init event (`type=system && subtype=init`), agy
///   `init` stream event (model + cwd + tool inventory).
/// - `TextDelta` — incremental assistant text (claude stream-json deltas,
///   agy `step_update.text_delta`).
/// - `ToolUse` — a tool invocation with a compact argument summary (claude
///   `tool_use`, agy tool steps).
/// - `UsageUpdate` — usage/cost snapshots; claude emits at result time
///   (`usage` + `modelUsage`), agy per step and at result.
/// - `RateLimit` — claude `rate_limit_event`, observed inside *successful*
///   runs: telemetry for proactive provider-global backoff (F-06).
/// - `Finished` — terminal success signal: exit code + final result text +
///   optional structured output (agy `structured_output`, F-07 contracts).
/// - `Failed` — terminal failure with a classified [`AdapterFailure`]
///   (typed provider business errors, zcode-style).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "camelCase")]
pub enum AdapterEvent {
    /// The session came up and identified itself.
    #[serde(rename_all = "camelCase")]
    Started {
        /// Provider session id (resumable where the runtime supports it).
        session_id: String,
        /// Model the session runs on, when reported at init.
        model: Option<String>,
    },
    /// Incremental assistant text.
    TextDelta(String),
    /// A tool was invoked.
    #[serde(rename_all = "camelCase")]
    ToolUse {
        /// Tool name as the provider reports it.
        tool: String,
        /// Compact human-readable argument summary (never full payloads —
        /// large payloads belong in content-addressed artifacts, F-00 §3).
        args_summary: String,
    },
    /// Usage/cost snapshot at a point in the session.
    UsageUpdate(UsageSnapshot),
    /// The provider reported rate-limit pressure. Not a failure by itself —
    /// observed in successful claude runs (seven-day utilization warnings).
    #[serde(rename_all = "camelCase")]
    RateLimit {
        /// Raw provider notice, preserved verbatim for backoff policy (F-06).
        provider_notice: Value,
    },
    /// Terminal success: the run completed.
    #[serde(rename_all = "camelCase")]
    Finished {
        /// Process exit code (canon signal, F-00 §4).
        exit_code: i32,
        /// Final result text (claude `result.result`, agy `response`).
        final_result: Option<String>,
        /// Structured output when one was requested and produced.
        structured: Option<Value>,
    },
    /// Terminal failure with the classified cause.
    Failed(AdapterFailure),
}

impl AdapterEvent {
    /// Stable machine-readable kind, one per variant — mirrors the wire tag.
    pub fn kind(&self) -> &'static str {
        match self {
            AdapterEvent::Started { .. } => "started",
            AdapterEvent::TextDelta(_) => "text_delta",
            AdapterEvent::ToolUse { .. } => "tool_use",
            AdapterEvent::UsageUpdate(_) => "usage_update",
            AdapterEvent::RateLimit { .. } => "rate_limit",
            AdapterEvent::Finished { .. } => "finished",
            AdapterEvent::Failed(_) => "failed",
        }
    }

    /// Whether this event terminates a session stream (no further events
    /// follow it).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            AdapterEvent::Finished { .. } | AdapterEvent::Failed(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ModelUsageRow;
    use serde_json::json;

    fn sample_usage() -> UsageSnapshot {
        UsageSnapshot {
            input_tokens: 1_000,
            output_tokens: 500,
            thinking_tokens: 64,
            cache_read_tokens: 22_000,
            total_tokens: 23_564,
            cost_usd: Some(0.02),
            per_model: vec![ModelUsageRow {
                model: "mock-model-1".to_owned(),
                cost_usd: Some(0.02),
                context_window: Some(1_000_000),
            }],
            session_overhead_tokens: 22_000,
        }
    }

    fn samples() -> Vec<AdapterEvent> {
        vec![
            AdapterEvent::Started {
                session_id: "mock-1".to_owned(),
                model: Some("mock-model-1".to_owned()),
            },
            AdapterEvent::TextDelta("working".to_owned()),
            AdapterEvent::ToolUse {
                tool: "Edit".to_owned(),
                args_summary: "edit src/lib.rs".to_owned(),
            },
            AdapterEvent::UsageUpdate(sample_usage()),
            AdapterEvent::RateLimit {
                provider_notice: json!({"utilization": 0.86}),
            },
            AdapterEvent::Finished {
                exit_code: 0,
                final_result: Some("done".to_owned()),
                structured: Some(json!({"ok": true})),
            },
            AdapterEvent::Failed(AdapterFailure::Transient {
                detail: "rate limited".to_owned(),
            }),
        ]
    }

    #[test]
    fn every_variant_round_trips_through_adjacently_tagged_json() {
        for sample in samples() {
            let value = serde_json::to_value(&sample).unwrap();
            let round_tripped: AdapterEvent = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(round_tripped, sample, "wire form was {value}");
        }
    }

    #[test]
    fn wire_tags_are_camel_case_and_inner_fields_follow() {
        let started = serde_json::to_value(AdapterEvent::Started {
            session_id: "mock-1".to_owned(),
            model: None,
        })
        .unwrap();
        assert_eq!(started["type"], json!("started"));
        assert!(started["data"].get("sessionId").is_some(), "{started}");

        let finished = serde_json::to_value(AdapterEvent::Finished {
            exit_code: 0,
            final_result: None,
            structured: None,
        })
        .unwrap();
        assert_eq!(finished["type"], json!("finished"));
        assert!(finished["data"].get("finalResult").is_some(), "{finished}");

        let delta = serde_json::to_value(AdapterEvent::TextDelta("x".to_owned())).unwrap();
        assert_eq!(delta["type"], json!("textDelta"));
        assert_eq!(delta["data"], json!("x"));
    }

    #[test]
    fn terminal_kinds_are_finished_and_failed() {
        for sample in samples() {
            let expected = matches!(
                sample,
                AdapterEvent::Finished { .. } | AdapterEvent::Failed(_)
            );
            assert_eq!(sample.is_terminal(), expected, "{sample:?}");
        }
    }

    #[test]
    fn kinds_match_variant_names() {
        let expected = [
            "started",
            "text_delta",
            "tool_use",
            "usage_update",
            "rate_limit",
            "finished",
            "failed",
        ];
        for (sample, kind) in samples().iter().zip(expected) {
            assert_eq!(sample.kind(), kind);
        }
    }
}
