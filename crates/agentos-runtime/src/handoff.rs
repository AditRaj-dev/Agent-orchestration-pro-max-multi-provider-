//! Typed handoff packets (PRD §11 HO-01, canonical shape in §25
//! Appendix C).
//!
//! Worker completion produces one structured packet — task outcome, files
//! changed, artifacts, context refs, decisions, tests, unresolved risks,
//! requested next action — so downstream agents never need the transcript.
//! HO-01 rules encoded here:
//!
//! - validation happens **before** the packet is accepted (queued) — a
//!   packet that fails [`HandoffPacket::validate`] never enters the
//!   supervisor's stores;
//! - artifacts are **references** (ids/paths + optional hashes), never
//!   inline blobs: `ArtifactRef` rejects unknown keys (a `content`/`data`
//!   field fails deserialization) and validation rejects reference strings
//!   that look like embedded payloads (`data:` URIs, embedded newlines,
//!   control characters, oversized values);
//! - the transcript is reachable **on demand only**: the packet carries at
//!   most a `transcript_ref` handle (e.g. a resumable session id) and has
//!   no field that could hold transcript content.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Maximum length of a reference string (URI/path/id). Real references are
/// short; anything approaching this size is presumed to be an inline
/// payload smuggled into a ref field.
pub const MAX_REF_LEN: usize = 512;

/// Lifecycle status of the handed-off work. Closed set: wire values are
/// `completed`, `output_ready`, `blocked`, `failed` (Appendix C uses
/// `output_ready`). Unknown strings fail deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffStatus {
    /// The objective is fully met.
    Completed,
    /// Output is ready for the next stage (review) — matches the task
    /// lifecycle's `output_ready`.
    OutputReady,
    /// Handed off while still blocked on something.
    Blocked,
    /// The work failed.
    Failed,
}

/// What the handing-off agent asks the supervisor to do next. Closed set:
/// `review`, `merge`, `escalate`, `none`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedAction {
    /// Send the output through a review gate.
    Review,
    /// Merge/reconcile with other outputs.
    Merge,
    /// Raise to a stronger agent or a human.
    Escalate,
    /// Nothing further.
    None,
}

/// Outcome of one test named in the packet (`passed`/`failed`/`skipped`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    /// The check passed.
    Passed,
    /// The check failed — blocks the stub reviewer.
    Failed,
    /// The check did not run (with a reason carried elsewhere).
    Skipped,
}

impl TestStatus {
    /// Parse the wire string; anything unknown counts as skipped rather
    /// than silently passing.
    pub fn parse(s: &str) -> Self {
        match s {
            "passed" => TestStatus::Passed,
            "failed" => TestStatus::Failed,
            _ => TestStatus::Skipped,
        }
    }
}

/// One test/entry from the packet's `tests` list (Appendix C:
/// `{"name": "test:auth", "status": "passed", "count": 28}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestReport {
    /// Check name, e.g. `test:auth`.
    pub name: String,
    /// Outcome.
    pub status: TestStatus,
    /// Number of cases, when the check is aggregate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
}

/// A reference to an immutable artifact. `deny_unknown_fields` makes this
/// the inline-blob firewall: an object carrying `content`, `data`,
/// `base64` or any other payload key fails deserialization outright.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    /// Reference itself: a path, artifact id, or `sha256:<hex>` address.
    pub uri: String,
    /// Integrity hash of the referenced content, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// Which rule of [`HandoffPacket::validate`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HandoffRule {
    /// `from_agent` was empty.
    #[error("from_agent must be non-empty")]
    EmptyFromAgent,
    /// `task_id` was empty.
    #[error("task_id must be non-empty")]
    EmptyTaskId,
    /// `summary` was empty.
    #[error("summary must be non-empty")]
    EmptySummary,
    /// `files_changed` carried an empty entry.
    #[error("files_changed must not contain empty entries")]
    EmptyFileEntry,
    /// `context_refs` carried an empty entry.
    #[error("context_refs must not contain empty entries")]
    EmptyContextRef,
    /// A test report carried an empty name.
    #[error("test report names must be non-empty")]
    EmptyTestName,
    /// An artifact/transcript field held something that is not a reference
    /// (inline payloads are forbidden, HO-01).
    #[error("not a reference (inline payloads are forbidden): {0}")]
    InlineRef(String),
}

/// The typed handoff packet (PRD §25 Appendix C).
///
/// ```json
/// {
///   "fromAgent": "codex-backend-03",
///   "taskId": "TASK-AUTH-042",
///   "status": "output_ready",
///   "summary": "Implemented transactional refresh-token rotation.",
///   "filesChanged": ["src/auth/token.ts", "tests/auth/token.test.ts"],
///   "contextRefs": ["context.auth@18", "context.db@7"],
///   "tests": [{"name": "test:auth", "status": "passed", "count": 28}],
///   "decisions": ["Uses existing transaction helper; no new dependency"],
///   "unresolved": [],
///   "requestedAction": "review"
/// }
/// ```
///
/// `id` and `artifacts` are absent from the Appendix C example but part of
/// the §11 key-state shape, so they default in on parse (a fresh UUID; an
/// empty artifact list) and always serialize.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandoffPacket {
    /// Packet identity (UUIDv7; generated when the wire form omits it).
    #[serde(default = "fresh_packet_id")]
    pub id: Uuid,
    /// Producing agent instance (e.g. `mock#mock-3`).
    pub from_agent: String,
    /// Task the packet hands off from.
    pub task_id: String,
    /// Handoff status (closed set, see [`HandoffStatus`]).
    pub status: HandoffStatus,
    /// Human-readable result summary.
    pub summary: String,
    /// Files the agent changed.
    pub files_changed: Vec<String>,
    /// Immutable artifact references (never inline blobs).
    #[serde(default)]
    pub artifacts: Vec<ArtifactRef>,
    /// Context references the agent operated against.
    pub context_refs: Vec<String>,
    /// Decisions the agent made.
    pub decisions: Vec<String>,
    /// Test/check evidence.
    pub tests: Vec<TestReport>,
    /// Unresolved risks/open questions.
    pub unresolved: Vec<String>,
    /// What the agent asks to happen next (closed set).
    pub requested_action: RequestedAction,
    /// Handle for on-demand transcript access (a session id or artifact
    /// address) — never transcript content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_ref: Option<String>,
}

fn fresh_packet_id() -> Uuid {
    Uuid::now_v7()
}

impl HandoffPacket {
    /// Validate before queueing (HO-01): required fields present and
    /// non-empty, statuses from the closed sets (enforced by serde
    /// already), and every artifact/transcript entry actually shaped like a
    /// reference.
    pub fn validate(&self) -> Result<(), HandoffRule> {
        if self.from_agent.trim().is_empty() {
            return Err(HandoffRule::EmptyFromAgent);
        }
        if self.task_id.trim().is_empty() {
            return Err(HandoffRule::EmptyTaskId);
        }
        if self.summary.trim().is_empty() {
            return Err(HandoffRule::EmptySummary);
        }
        if self.files_changed.iter().any(|f| f.trim().is_empty()) {
            return Err(HandoffRule::EmptyFileEntry);
        }
        if self.context_refs.iter().any(|r| r.trim().is_empty()) {
            return Err(HandoffRule::EmptyContextRef);
        }
        if self.tests.iter().any(|t| t.name.trim().is_empty()) {
            return Err(HandoffRule::EmptyTestName);
        }
        for artifact in &self.artifacts {
            if !ref_is_shaped_like_a_reference(&artifact.uri) {
                return Err(HandoffRule::InlineRef(artifact.uri.clone()));
            }
            if let Some(hash) = &artifact.hash {
                if !ref_is_shaped_like_a_reference(hash) {
                    return Err(HandoffRule::InlineRef(hash.clone()));
                }
            }
        }
        if let Some(transcript) = &self.transcript_ref {
            if !ref_is_shaped_like_a_reference(transcript) {
                return Err(HandoffRule::InlineRef(transcript.clone()));
            }
        }
        Ok(())
    }

    /// Map a terminal `Finished` adapter event into a packet. The mapping
    /// is tolerant (missing structured keys degrade, never panic): the
    /// summary falls back to the final result text, `filesChanged`,
    /// `decisions` and `unresolved` are read as string arrays when present,
    /// and `tests` accepts either a single object or an array of objects
    /// (the mock adapter's completion packet is a single object with
    /// `status: "skipped"`). Files become artifact references; the
    /// transcript is reachable only via the session handle.
    pub fn from_adapter_finish(
        task_id: &str,
        from_agent: &str,
        context_refs: &[String],
        exit_code: i32,
        final_result: Option<&str>,
        structured: Option<&Value>,
    ) -> Self {
        // Only the mock adapter and agy's `--json-schema` path produce a
        // structured payload; claude and codex have no such surface, and
        // `SpawnSpec` carries no schema to request one. Without a fallback
        // every real-provider packet arrives with an empty `tests` array and
        // review can never pass. The completion report the objective asks
        // for — a fenced JSON object at the end of the final message — is
        // the provider-independent channel, used only when the adapter
        // gave us nothing structured of its own.
        let parsed = structured
            .filter(|value| value.is_object())
            .cloned()
            .or_else(|| final_result.and_then(completion_report));
        let structured = parsed.as_ref().unwrap_or(&Value::Null);
        let summary = structured
            .get("summary")
            .and_then(Value::as_str)
            .or(final_result)
            .map(str::to_owned)
            .unwrap_or_else(|| format!("session finished with exit code {exit_code}"));
        let files_changed = strings_at(structured, "filesChanged");
        let artifacts = files_changed
            .iter()
            .map(|uri| ArtifactRef {
                uri: uri.clone(),
                hash: None,
            })
            .collect();
        Self {
            id: Uuid::now_v7(),
            from_agent: from_agent.to_owned(),
            task_id: task_id.to_owned(),
            status: HandoffStatus::OutputReady,
            summary,
            files_changed,
            artifacts,
            context_refs: context_refs.to_vec(),
            decisions: strings_at(structured, "decisions"),
            tests: tests_from(structured.get("tests")),
            unresolved: strings_at(structured, "unresolved"),
            requested_action: RequestedAction::Review,
            transcript_ref: None,
        }
    }
}

/// Recover a completion report from a worker's final message.
///
/// Accepts the last ```` ```json ```` fence, else the last balanced
/// top-level `{...}` run in the text. Anything that does not parse as a
/// JSON *object* degrades to `None` — a worker that ignored the format
/// simply reports no evidence, exactly as before.
fn completion_report(text: &str) -> Option<Value> {
    fenced_json(text)
        .or_else(|| trailing_object(text))
        .and_then(|candidate| serde_json::from_str::<Value>(&candidate).ok())
        .filter(Value::is_object)
}

/// The body of the last ```` ```json ```` (or bare ```` ``` ````) fence.
fn fenced_json(text: &str) -> Option<String> {
    let mut best: Option<String> = None;
    let mut rest = text;
    while let Some(open) = rest.find("```") {
        let after = &rest[open + 3..];
        // Skip the info string on the fence's opening line.
        let (info, body) = match after.find('\n') {
            Some(nl) => (after[..nl].trim(), &after[nl + 1..]),
            None => break,
        };
        let Some(close) = body.find("```") else { break };
        if info.is_empty() || info.eq_ignore_ascii_case("json") {
            let candidate = body[..close].trim();
            if candidate.starts_with('{') {
                best = Some(candidate.to_owned());
            }
        }
        rest = &body[close + 3..];
    }
    best
}

/// The last balanced `{...}` run in the text, ignoring braces inside JSON
/// strings. Scans backwards from the final `}` so trailing prose after the
/// object is tolerated.
fn trailing_object(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let end = text.rfind('}')?;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut index = end as isize;
    while index >= 0 {
        let byte = bytes[index as usize];
        if in_string {
            // A quote is an opener unless escaped by an odd backslash run.
            if byte == b'"' {
                let mut slashes = 0isize;
                let mut back = index - 1;
                while back >= 0 && bytes[back as usize] == b'\\' {
                    slashes += 1;
                    back -= 1;
                }
                if slashes % 2 == 0 {
                    in_string = false;
                }
            }
        } else {
            match byte {
                b'"' => in_string = true,
                b'}' => depth += 1,
                b'{' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(text[index as usize..=end].to_owned());
                    }
                }
                _ => {}
            }
        }
        index -= 1;
    }
    None
}

/// Read `key` from `value` as an array of strings (anything else degrades
/// to empty).
fn strings_at(value: &Value, key: &str) -> Vec<String> {
    match value.get(key) {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

/// Map a `tests` value: single object -> one report; array -> many;
/// anything else -> no evidence.
fn tests_from(tests: Option<&Value>) -> Vec<TestReport> {
    match tests {
        Some(Value::Array(items)) => items.iter().filter_map(test_from).collect(),
        Some(object @ Value::Object(_)) => test_from(object).into_iter().collect(),
        _ => Vec::new(),
    }
}

fn test_from(value: &Value) -> Option<TestReport> {
    if !value.is_object() {
        return None;
    }
    Some(TestReport {
        name: value
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("adapter-tests")
            .to_owned(),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(TestStatus::parse)
            .unwrap_or(TestStatus::Skipped),
        count: value
            .get("count")
            .and_then(Value::as_u64)
            .and_then(|count| u32::try_from(count).ok()),
    })
}

/// Whether `candidate` is shaped like a reference: short, single line, no
/// control characters, and not a `data:` inline URI.
fn ref_is_shaped_like_a_reference(candidate: &str) -> bool {
    !candidate.trim().is_empty()
        && candidate.len() <= MAX_REF_LEN
        && !candidate.starts_with("data:")
        && !candidate.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The whole point of the fallback: a provider with no structured
    /// surface still delivers reviewable test evidence.
    #[test]
    fn a_fenced_report_in_the_final_message_becomes_the_packet() {
        let text = "Did the thing.

```json
{\"summary\": \"rotated tokens\", \"filesChanged\": [\"src/a.rs\"], \"tests\": [{\"name\": \"cargo test\", \"status\": \"passed\", \"count\": 3}], \"unresolved\": []}
```";
        let packet = HandoffPacket::from_adapter_finish("T1", "worker", &[], 0, Some(text), None);
        assert_eq!(packet.summary, "rotated tokens");
        assert_eq!(packet.files_changed, vec!["src/a.rs".to_owned()]);
        assert_eq!(packet.tests.len(), 1);
        assert_eq!(packet.tests[0].status, TestStatus::Passed);
        assert!(packet.unresolved.is_empty());
    }

    /// An adapter that *does* speak structured output keeps winning, and a
    /// trailing object survives prose after it.
    #[test]
    fn structured_output_wins_and_a_bare_trailing_object_still_parses() {
        let text =
            "{\"summary\": \"from text\", \"tests\": [{\"name\": \"t\", \"status\": \"skipped\"}]}

Let me know if you want changes.";
        let from_text =
            HandoffPacket::from_adapter_finish("T1", "worker", &[], 0, Some(text), None);
        assert_eq!(from_text.summary, "from text");
        assert_eq!(from_text.tests.len(), 1);

        let structured = json!({"summary": "from adapter", "tests": []});
        let from_adapter = HandoffPacket::from_adapter_finish(
            "T1",
            "worker",
            &[],
            0,
            Some(text),
            Some(&structured),
        );
        assert_eq!(from_adapter.summary, "from adapter");
        assert!(from_adapter.tests.is_empty());
    }

    /// Prose that merely mentions braces must not become evidence.
    #[test]
    fn a_message_without_a_report_yields_no_evidence() {
        let packet = HandoffPacket::from_adapter_finish(
            "T1",
            "worker",
            &[],
            0,
            Some("I edited the { thing } and it works."),
            None,
        );
        assert!(packet.tests.is_empty());
        assert_eq!(packet.summary, "I edited the { thing } and it works.");
    }

    /// The Appendix C example, verbatim.
    fn appendix_c() -> serde_json::Value {
        json!({
            "fromAgent": "codex-backend-03",
            "taskId": "TASK-AUTH-042",
            "status": "output_ready",
            "summary": "Implemented transactional refresh-token rotation.",
            "filesChanged": ["src/auth/token.ts", "tests/auth/token.test.ts"],
            "contextRefs": ["context.auth@18", "context.db@7"],
            "tests": [{"name": "test:auth", "status": "passed", "count": 28}],
            "decisions": ["Uses existing transaction helper; no new dependency"],
            "unresolved": [],
            "requestedAction": "review"
        })
    }

    #[test]
    fn appendix_c_example_parses_and_round_trips() {
        let packet: HandoffPacket = serde_json::from_value(appendix_c()).expect("parse");
        assert_eq!(packet.from_agent, "codex-backend-03");
        assert_eq!(packet.task_id, "TASK-AUTH-042");
        assert_eq!(packet.status, HandoffStatus::OutputReady);
        assert_eq!(packet.requested_action, RequestedAction::Review);
        assert_eq!(packet.files_changed.len(), 2);
        assert_eq!(
            packet.tests,
            vec![TestReport {
                name: "test:auth".to_owned(),
                status: TestStatus::Passed,
                count: Some(28),
            }]
        );
        // Fields the example omits default in.
        assert!(packet.artifacts.is_empty());
        assert!(packet.transcript_ref.is_none());
        assert_ne!(packet.id, Uuid::nil(), "id was generated on parse");

        packet.validate().expect("Appendix C is valid");

        // Serialize -> parse round-trips with the defaulted fields stable.
        let wire = serde_json::to_value(&packet).unwrap();
        assert_eq!(wire["fromAgent"], json!("codex-backend-03"));
        assert_eq!(wire["requestedAction"], json!("review"));
        let round_tripped: HandoffPacket = serde_json::from_value(wire).unwrap();
        assert_eq!(round_tripped, packet);
    }

    #[test]
    fn missing_required_fields_are_rejected() {
        for field in [
            "summary",
            "taskId",
            "fromAgent",
            "status",
            "requestedAction",
        ] {
            let mut wire = appendix_c();
            wire.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<HandoffPacket>(wire).is_err(),
                "missing {field} must fail to parse"
            );
        }
    }

    #[test]
    fn statuses_and_actions_form_closed_sets() {
        let mut bad_status = appendix_c();
        bad_status["status"] = json!("exploded");
        assert!(serde_json::from_value::<HandoffPacket>(bad_status).is_err());

        let mut bad_action = appendix_c();
        bad_action["requestedAction"] = json!("do-a-backflip");
        assert!(serde_json::from_value::<HandoffPacket>(bad_action).is_err());

        for status in ["completed", "blocked", "failed"] {
            let mut wire = appendix_c();
            wire["status"] = json!(status);
            let packet: HandoffPacket = serde_json::from_value(wire).unwrap();
            packet.validate().unwrap();
        }
    }

    #[test]
    fn inline_blobs_in_artifacts_are_rejected() {
        // A payload-carrying object is not an ArtifactRef: unknown keys are
        // denied, so the blob never becomes a packet in the first place.
        let mut wire = appendix_c();
        wire["artifacts"] = json!([{
            "uri": "artifacts/token-diff",
            "content": "diff --git a/src/auth/token.ts b/src/auth/token.ts +++ BINARY SOUP"
        }]);
        assert!(serde_json::from_value::<HandoffPacket>(wire).is_err());

        // Even a well-formed object whose uri *is* the blob is rejected by
        // validation.
        let mut packet: HandoffPacket = serde_json::from_value(appendix_c()).unwrap();
        packet.artifacts = vec![ArtifactRef {
            uri: "data:application/json;base64,eyJ0cmFuc2NyaXB0Ijoi".to_owned(),
            hash: None,
        }];
        assert_eq!(
            packet.validate().unwrap_err(),
            HandoffRule::InlineRef("data:application/json;base64,eyJ0cmFuc2NyaXB0Ijoi".to_owned())
        );

        // Oversized and multiline uris are presumed inline payloads too.
        packet.artifacts = vec![ArtifactRef {
            uri: "a".repeat(MAX_REF_LEN + 1),
            hash: None,
        }];
        assert!(matches!(
            packet.validate().unwrap_err(),
            HandoffRule::InlineRef(_)
        ));
        packet.artifacts = vec![ArtifactRef {
            uri: "line-one\nline-two".to_owned(),
            hash: None,
        }];
        assert!(matches!(
            packet.validate().unwrap_err(),
            HandoffRule::InlineRef(_)
        ));
    }

    #[test]
    fn transcript_may_only_ever_be_a_handle() {
        let mut packet: HandoffPacket = serde_json::from_value(appendix_c()).unwrap();
        packet.transcript_ref = Some("adapter-session:mock-7".to_owned());
        packet.validate().expect("a session handle is a valid ref");

        packet.transcript_ref = Some("data:text/plain;base64,dXNlciByZXF1ZXN0ZWQ...".to_owned());
        assert!(matches!(
            packet.validate().unwrap_err(),
            HandoffRule::InlineRef(_)
        ));

        // And there is no field on the wire shape that could hold content:
        // serializing never emits transcript bytes, only the optional ref.
        let wire = serde_json::to_value(&packet).unwrap();
        let object = wire.as_object().unwrap();
        assert!(
            !object.contains_key("transcript"),
            "the packet shape has no transcript content field"
        );
    }

    #[test]
    fn field_level_validation_rules_reject_empty_values() {
        let mut packet: HandoffPacket = serde_json::from_value(appendix_c()).unwrap();

        packet.summary = "   ".to_owned();
        assert_eq!(packet.validate().unwrap_err(), HandoffRule::EmptySummary);

        packet.summary = "ok".to_owned();
        packet.files_changed.push("  ".to_owned());
        assert_eq!(packet.validate().unwrap_err(), HandoffRule::EmptyFileEntry);

        packet.files_changed.pop();
        packet.context_refs.push(String::new());
        assert_eq!(packet.validate().unwrap_err(), HandoffRule::EmptyContextRef);

        packet.context_refs.pop();
        packet.tests.push(TestReport {
            name: " ".to_owned(),
            status: TestStatus::Passed,
            count: None,
        });
        assert_eq!(packet.validate().unwrap_err(), HandoffRule::EmptyTestName);
    }

    #[test]
    fn mock_completion_packet_maps_into_a_valid_handoff() {
        // The exact structured shape the mock adapter finishes with.
        let structured = json!({
            "summary": "completed `implement the mocked feature` in 2 turn(s), 1 file(s) changed",
            "filesChanged": ["src/a/mod.rs"],
            "tests": {"status": "skipped", "reason": "mock adapter never executes tests"}
        });
        let packet = HandoffPacket::from_adapter_finish(
            "0a1b2c3d-0000-4000-8000-444455556666",
            "mock#mock-1",
            &["context.repo@3".to_owned()],
            0,
            Some("completed `implement the mocked feature`"),
            Some(&structured),
        );
        packet.validate().expect("mapped packet is valid");

        assert_eq!(packet.status, HandoffStatus::OutputReady);
        assert_eq!(packet.requested_action, RequestedAction::Review);
        assert_eq!(packet.files_changed, vec!["src/a/mod.rs".to_owned()]);
        assert_eq!(
            packet.artifacts,
            vec![ArtifactRef {
                uri: "src/a/mod.rs".to_owned(),
                hash: None
            }]
        );
        assert_eq!(packet.context_refs, vec!["context.repo@3".to_owned()]);
        assert_eq!(packet.tests.len(), 1);
        assert_eq!(packet.tests[0].status, TestStatus::Skipped);
        assert!(packet.unresolved.is_empty());
        assert!(packet.decisions.is_empty());

        // Round-trips through serde.
        let round_tripped: HandoffPacket =
            serde_json::from_value(serde_json::to_value(&packet).unwrap()).unwrap();
        assert_eq!(round_tripped, packet);
    }

    #[test]
    fn finish_without_structured_output_degrades_gracefully() {
        let packet = HandoffPacket::from_adapter_finish(
            "task-1",
            "mock#mock-2",
            &[],
            0,
            Some("did the thing"),
            None,
        );
        assert_eq!(packet.summary, "did the thing");
        assert!(packet.files_changed.is_empty());
        assert!(packet.tests.is_empty());
        assert_eq!(
            HandoffPacket::from_adapter_finish("task-1", "mock#mock-3", &[], 1, None, None).summary,
            "session finished with exit code 1"
        );
    }
}
