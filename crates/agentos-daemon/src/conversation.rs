//! Read-side conversation context derived from the append-only journal.
//!
//! The manifest is a snapshot of what was bound when a conversation started;
//! it does not pretend that changing a registry agent retroactively changed an
//! older chat.  This is the durable handoff boundary for a future richer UI.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::events::SequencedEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConversationLineage {
    pub session_id: String,
    pub root_session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_from: Option<String>,
    #[serde(default)]
    pub descendants: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SkillBinding {
    pub id: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContextManifest {
    pub session_id: String,
    pub project_id: Option<String>,
    pub workspace: Option<String>,
    pub adapter_id: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub mode: Option<String>,
    #[serde(default)]
    pub skills: Vec<SkillBinding>,
    pub timeout_secs: Option<u64>,
}

/// Curated, transportable context declaration.  The daemon stores the source
/// data in journal events; callers can use this type for handoff previews
/// without copying full transcripts into another persistent store.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CuratedHandoff {
    pub session_id: String,
    pub from_agent: String,
    pub to_agent: String,
    pub summary: String,
    #[serde(default)]
    pub artifact_refs: Vec<String>,
    #[serde(default)]
    pub skill_bindings: Vec<SkillBinding>,
}

pub fn lineage(journal: &[SequencedEvent], session_id: &str) -> ConversationLineage {
    let parents = spawn_parents(journal);
    let resumed_from = parents.get(session_id).cloned().flatten();
    let mut root_session_id = session_id.to_owned();
    let mut cursor = resumed_from.clone();
    while let Some(parent) = cursor {
        root_session_id = parent.clone();
        cursor = parents.get(&parent).cloned().flatten();
    }
    let descendants = parents
        .iter()
        .filter_map(|(child, parent)| {
            (parent.as_deref() == Some(session_id)).then_some(child.clone())
        })
        .collect();
    ConversationLineage {
        session_id: session_id.to_owned(),
        root_session_id,
        resumed_from,
        descendants,
    }
}

pub fn context_manifest(journal: &[SequencedEvent], session_id: &str) -> Option<ContextManifest> {
    journal.iter().find_map(|entry| {
        let event = &entry.event;
        (event.event_type.to_string() == "session.spawn"
            && event.payload["chat"] == Value::Bool(true)
            && event.payload["sessionId"].as_str() == Some(session_id))
        .then(|| manifest_from_payload(session_id, &event.payload))
    })
}

fn spawn_parents(journal: &[SequencedEvent]) -> std::collections::HashMap<String, Option<String>> {
    journal
        .iter()
        .filter_map(|entry| {
            let event = &entry.event;
            (event.event_type.to_string() == "session.spawn"
                && event.payload["chat"] == Value::Bool(true))
            .then(|| {
                event.payload["sessionId"].as_str().map(|id| {
                    (
                        id.to_owned(),
                        event.payload["resumedFrom"].as_str().map(ToOwned::to_owned),
                    )
                })
            })
            .flatten()
        })
        .collect()
}

fn manifest_from_payload(session_id: &str, payload: &Value) -> ContextManifest {
    let skills = payload["skills"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str())
        .map(|id| SkillBinding {
            id: id.to_owned(),
            source: "session".to_owned(),
        })
        .collect();
    ContextManifest {
        session_id: session_id.to_owned(),
        project_id: payload["projectId"].as_str().map(ToOwned::to_owned),
        workspace: payload["workspace"].as_str().map(ToOwned::to_owned),
        adapter_id: payload["adapterId"].as_str().map(ToOwned::to_owned),
        model: payload["model"].as_str().map(ToOwned::to_owned),
        effort: payload["effort"].as_str().map(ToOwned::to_owned),
        mode: payload["mode"].as_str().map(ToOwned::to_owned),
        skills,
        timeout_secs: payload["timeoutSecs"].as_u64(),
    }
}
