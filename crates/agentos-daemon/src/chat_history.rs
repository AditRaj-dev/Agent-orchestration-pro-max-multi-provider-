//! Chat history: past conversations folded out of the journal (F-13b).
//!
//! Live transcripts exist only in the desktop's memory, so a reload used to
//! throw every conversation away. Nothing was actually lost — the journal
//! already records the whole shape of a chat:
//!
//! | event | what it contributes |
//! |---|---|
//! | `session.spawn` | the session exists: agent, provider, model, start time |
//! | `session.started` | `providerSessionId` — the handle a resume needs |
//! | `session.instruction` | one **user** message (the opening one included) |
//! | `session.finished` | one **agent** message (`finalResult`) |
//! | `agent.decision` | a question with options the human was asked |
//! | `agent.session_failed` / `session.cancelled` | how the session ended |
//!
//! This module folds those into [`ChatSessionSummary`] rows and
//! [`ChatMessage`] transcripts. The fold is server-side on purpose: the
//! desktop asks for one agent's history and gets it, instead of paging the
//! whole journal into the browser and folding it there.
//!
//! Only `chat: true` events participate — worker runs (F-07) journal the
//! same session vocabulary and must never surface as chat.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::events::SequencedEvent;

/// How a chat session ended (or that it has not).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatSessionStatus {
    /// Spawned, no terminal event seen yet.
    Live,
    /// At least one turn completed and nothing failed.
    Finished,
    /// Ended in a classified failure.
    Failed,
    /// Cancelled by the human, or aged out while idle.
    Cancelled,
}

/// One past (or live) conversation, newest activity last.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatSessionSummary {
    /// Daemon chat session id (`chat-<n>-<uuid>`).
    pub session_id: String,
    /// Registry agent the conversation belongs to.
    pub agent_id: String,
    /// Adapter that ran it.
    pub provider: Option<String>,
    /// Model, as recorded at spawn.
    pub model: Option<String>,
    /// Provider-side conversation handle: what a resume passes back
    /// (claude session id / agy conversation id / codex thread id).
    /// `None` means this conversation cannot be continued.
    pub provider_session_id: Option<String>,
    /// First user message, trimmed — the human-facing title of the chat.
    pub title: String,
    /// When the session was spawned (journal timestamp).
    pub started_at: String,
    /// Timestamp of the most recent event in the session.
    pub last_at: String,
    /// Completed agent turns.
    pub turns: u32,
    /// Messages in the transcript (user + agent + system).
    pub message_count: u32,
    /// Terminal state.
    pub status: ChatSessionStatus,
    /// Failure detail when `status` is `failed`.
    pub error: Option<String>,
    /// Journal sequence of the newest event, for newest-first ordering.
    pub last_seq: i64,
}

/// One rendered transcript line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessage {
    /// Session the message belongs to.
    pub session_id: String,
    /// Agent the session belongs to.
    pub agent_id: String,
    /// `user` | `agent` | `system` | `decision`.
    pub role: String,
    /// Message body (the question text, for a decision).
    pub text: String,
    /// Options offered, when this line was a decision.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub options: Vec<String>,
    /// Journal timestamp.
    pub at: String,
    /// Journal sequence — the transcript's stable order.
    pub seq: i64,
}

const EVT_SPAWN: &str = "session.spawn";
const EVT_STARTED: &str = "session.started";
const EVT_INSTRUCTION: &str = "session.instruction";
const EVT_FINISHED: &str = "session.finished";
const EVT_FAILED: &str = "agent.session_failed";
const EVT_CANCELLED: &str = "session.cancelled";
const EVT_DECISION: &str = "agent.decision";

/// Title length before elision — a chat list row, not a paragraph.
const TITLE_MAX: usize = 80;

fn str_field(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

fn is_chat(payload: &Value) -> bool {
    payload
        .get("chat")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn session_of(event: &SequencedEvent) -> Option<String> {
    str_field(&event.event.payload, "sessionId")
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_owned()
    } else {
        let cut: String = text.chars().take(max).collect();
        format!("{cut}…")
    }
}

/// Fold the journal into chat session summaries, newest activity first.
///
/// `agent_id` filters to one agent's conversations; `None` returns all.
pub fn fold_sessions(
    journal: &[SequencedEvent],
    agent_id: Option<&str>,
) -> Vec<ChatSessionSummary> {
    let mut sessions: Vec<ChatSessionSummary> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    for sequenced in journal {
        let event = &sequenced.event;
        let payload = &event.payload;
        let name = event.event_type.to_string();
        // `agent.decision` carries no `chat` flag when a worker run asks;
        // it is only chat history if its session is already known.
        let known = session_of(sequenced)
            .map(|session| index.contains_key(&session))
            .unwrap_or(false);
        if !is_chat(payload) && !known {
            continue;
        }
        let Some(session_id) = session_of(sequenced) else {
            continue;
        };
        let at = event.occurred_at.to_rfc3339();

        if name == EVT_SPAWN {
            let Some(agent) = event.agent_id.clone() else {
                continue;
            };
            if agent_id.is_some_and(|wanted| wanted != agent) {
                continue;
            }
            index.insert(session_id.clone(), sessions.len());
            sessions.push(ChatSessionSummary {
                session_id,
                agent_id: agent,
                provider: str_field(payload, "provider"),
                model: str_field(payload, "model"),
                provider_session_id: None,
                title: String::new(),
                started_at: at.clone(),
                last_at: at,
                turns: 0,
                message_count: 0,
                status: ChatSessionStatus::Live,
                error: None,
                last_seq: sequenced.seq,
            });
            continue;
        }

        let Some(position) = index.get(&session_id).copied() else {
            continue; // an event for a session we are not tracking
        };
        let summary = &mut sessions[position];
        summary.last_at = at;
        summary.last_seq = sequenced.seq;

        match name.as_str() {
            EVT_STARTED => {
                summary.provider_session_id = str_field(payload, "providerSessionId");
                if summary.model.is_none() {
                    summary.model = str_field(payload, "model");
                }
            }
            EVT_INSTRUCTION => {
                summary.message_count += 1;
                if summary.title.is_empty() {
                    if let Some(message) = str_field(payload, "message") {
                        summary.title = truncate(&message, TITLE_MAX);
                    }
                }
            }
            EVT_FINISHED => {
                summary.turns += 1;
                summary.message_count += 1;
                if summary.status == ChatSessionStatus::Live {
                    summary.status = ChatSessionStatus::Finished;
                }
            }
            EVT_DECISION => summary.message_count += 1,
            EVT_FAILED => {
                summary.status = ChatSessionStatus::Failed;
                summary.error = str_field(payload, "error");
                summary.message_count += 1;
            }
            EVT_CANCELLED => {
                summary.status = ChatSessionStatus::Cancelled;
                summary.message_count += 1;
            }
            _ => {}
        }
    }

    sessions.sort_by(|a, b| b.last_seq.cmp(&a.last_seq));
    sessions
}

/// Fold one session's transcript in journal order.
pub fn fold_transcript(journal: &[SequencedEvent], session_id: &str) -> Vec<ChatMessage> {
    let mut messages = Vec::new();
    let mut agent = String::new();

    for sequenced in journal {
        if session_of(sequenced).as_deref() != Some(session_id) {
            continue;
        }
        let event = &sequenced.event;
        let payload = &event.payload;
        if let Some(id) = event.agent_id.clone() {
            agent = id;
        }
        let at = event.occurred_at.to_rfc3339();
        let mut push = |role: &str, text: String, options: Vec<String>| {
            messages.push(ChatMessage {
                session_id: session_id.to_owned(),
                agent_id: agent.clone(),
                role: role.to_owned(),
                text,
                options,
                at: at.clone(),
                seq: sequenced.seq,
            });
        };

        match event.event_type.to_string().as_str() {
            EVT_INSTRUCTION => {
                if let Some(message) = str_field(payload, "message") {
                    push("user", message, Vec::new());
                }
            }
            EVT_FINISHED => {
                if let Some(text) = str_field(payload, "finalResult") {
                    push("agent", text, Vec::new());
                }
            }
            EVT_DECISION => {
                let options: Vec<String> = payload
                    .get("options")
                    .and_then(Value::as_array)
                    .map(|options| {
                        options
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                if !options.is_empty() {
                    push(
                        "decision",
                        str_field(payload, "prompt").unwrap_or_default(),
                        options,
                    );
                }
            }
            EVT_FAILED => push(
                "system",
                format!(
                    "Session failed: {}",
                    str_field(payload, "error").unwrap_or_else(|| "unknown error".to_owned())
                ),
                Vec::new(),
            ),
            EVT_CANCELLED => push(
                "system",
                format!(
                    "Session ended: {}",
                    str_field(payload, "reason").unwrap_or_else(|| "cancelled".to_owned())
                ),
                Vec::new(),
            ),
            _ => {}
        }
    }

    messages
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_core::{Event, EventType};
    use serde_json::json;

    fn sequenced(seq: i64, name: &str, agent: &str, payload: Value) -> SequencedEvent {
        SequencedEvent {
            seq,
            event: Event::new(EventType::Other(name.to_owned()))
                .with_agent_id(agent.to_owned())
                .with_payload(payload),
        }
    }

    /// One complete conversation: opening message, answer, follow-up,
    /// answer — plus the provider handle a resume needs.
    fn two_turn_journal() -> Vec<SequencedEvent> {
        vec![
            sequenced(
                1,
                EVT_SPAWN,
                "codex-scout",
                json!({"sessionId": "chat-1", "chat": true, "provider": "codex",
                       "model": "gpt-5.6-terra"}),
            ),
            sequenced(
                2,
                EVT_INSTRUCTION,
                "codex-scout",
                json!({"sessionId": "chat-1", "chat": true, "message": "Remember PLUM7"}),
            ),
            sequenced(
                3,
                EVT_STARTED,
                "codex-scout",
                json!({"sessionId": "chat-1", "chat": true,
                       "providerSessionId": "thread-abc"}),
            ),
            sequenced(
                4,
                EVT_FINISHED,
                "codex-scout",
                json!({"sessionId": "chat-1", "chat": true, "finalResult": "STORED"}),
            ),
            sequenced(
                5,
                EVT_INSTRUCTION,
                "codex-scout",
                json!({"sessionId": "chat-1", "chat": true, "message": "Which token?"}),
            ),
            sequenced(
                6,
                EVT_FINISHED,
                "codex-scout",
                json!({"sessionId": "chat-1", "chat": true, "finalResult": "PLUM7"}),
            ),
        ]
    }

    #[test]
    fn a_conversation_folds_into_one_summary() {
        let sessions = fold_sessions(&two_turn_journal(), None);
        assert_eq!(sessions.len(), 1);
        let summary = &sessions[0];
        assert_eq!(summary.session_id, "chat-1");
        assert_eq!(summary.agent_id, "codex-scout");
        assert_eq!(summary.provider.as_deref(), Some("codex"));
        assert_eq!(
            summary.provider_session_id.as_deref(),
            Some("thread-abc"),
            "the resume handle must survive"
        );
        assert_eq!(summary.title, "Remember PLUM7", "title is the opening ask");
        assert_eq!(summary.turns, 2);
        assert_eq!(summary.message_count, 4);
        assert_eq!(summary.status, ChatSessionStatus::Finished);
    }

    #[test]
    fn the_transcript_alternates_user_and_agent_in_journal_order() {
        let messages = fold_transcript(&two_turn_journal(), "chat-1");
        let rendered: Vec<(&str, &str)> = messages
            .iter()
            .map(|m| (m.role.as_str(), m.text.as_str()))
            .collect();
        assert_eq!(
            rendered,
            vec![
                ("user", "Remember PLUM7"),
                ("agent", "STORED"),
                ("user", "Which token?"),
                ("agent", "PLUM7"),
            ]
        );
        assert!(messages.iter().all(|m| m.agent_id == "codex-scout"));
    }

    #[test]
    fn decisions_and_failures_are_part_of_the_history() {
        let mut journal = two_turn_journal();
        journal.push(sequenced(
            7,
            EVT_DECISION,
            "codex-scout",
            json!({"sessionId": "chat-1", "chat": true, "prompt": "Which database?",
                   "options": ["Postgres", "SQLite"], "multiSelect": false}),
        ));
        journal.push(sequenced(
            8,
            EVT_FAILED,
            "codex-scout",
            json!({"sessionId": "chat-1", "chat": true, "error": "quota exhausted"}),
        ));

        let messages = fold_transcript(&journal, "chat-1");
        let decision = messages
            .iter()
            .find(|m| m.role == "decision")
            .expect("the question the human was asked is history too");
        assert_eq!(decision.text, "Which database?");
        assert_eq!(decision.options, vec!["Postgres", "SQLite"]);
        assert!(messages
            .last()
            .is_some_and(|m| m.role == "system" && m.text.contains("quota exhausted")));

        let summary = &fold_sessions(&journal, None)[0];
        assert_eq!(summary.status, ChatSessionStatus::Failed);
        assert_eq!(summary.error.as_deref(), Some("quota exhausted"));
    }

    /// Worker runs (F-07) journal the same session vocabulary without the
    /// `chat` flag; they must never appear in chat history.
    #[test]
    fn worker_runs_are_not_chat_history() {
        let journal = vec![
            sequenced(
                1,
                EVT_SPAWN,
                "nodejs-dev",
                json!({"sessionId": "task-run-9", "provider": "codex"}),
            ),
            sequenced(
                2,
                EVT_FINISHED,
                "nodejs-dev",
                json!({"sessionId": "task-run-9", "finalResult": "built"}),
            ),
        ];
        assert!(fold_sessions(&journal, None).is_empty());
    }

    #[test]
    fn sessions_filter_by_agent_and_sort_newest_first() {
        let mut journal = two_turn_journal();
        journal.push(sequenced(
            9,
            EVT_SPAWN,
            "agent-creator",
            json!({"sessionId": "chat-2", "chat": true, "provider": "antigravity-agy"}),
        ));
        journal.push(sequenced(
            10,
            EVT_INSTRUCTION,
            "agent-creator",
            json!({"sessionId": "chat-2", "chat": true, "message": "Build me an auditor"}),
        ));

        let all = fold_sessions(&journal, None);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].session_id, "chat-2", "newest activity first");

        let mine = fold_sessions(&journal, Some("codex-scout"));
        assert_eq!(mine.len(), 1);
        assert_eq!(mine[0].session_id, "chat-1");
    }

    #[test]
    fn a_live_session_has_no_terminal_status() {
        let journal = &two_turn_journal()[..3];
        let summary = &fold_sessions(journal, None)[0];
        assert_eq!(summary.status, ChatSessionStatus::Live);
        assert_eq!(summary.turns, 0);
    }
}
