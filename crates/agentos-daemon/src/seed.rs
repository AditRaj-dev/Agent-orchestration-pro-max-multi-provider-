//! F-11a demo seeding (§3.4): append a frozen fixture into an explicit,
//! still-empty journal — the UI-dev/acceptance corpus.
//!
//! Deliberately hostile to the two ways this could destroy real data:
//!
//! - the **default journal path** is refused outright (`--db` must name a
//!   throwaway file, never the journal a live daemon owns), and
//! - a target that **already has events** is refused — a seeded journal is
//!   disposable state, and appending a second corpus would interleave two
//!   runs' seq ranges into one fold.
//!
//! Every appended payload is stamped `"demo": true` even if the fixture
//! forgot it, so demo rows stay greppable in any journal they somehow land
//! in. The empty-check and the appends race only against another writer
//! aimed at the same brand-new file — dev tooling, accepted.

use std::path::{Path, PathBuf};

use agentos_core::{CoreError, Event};
use serde_json::Value;

use crate::db;
use crate::events;

/// What [`seed_demo`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedReport {
    /// Number of fixture events appended (== the fixture's length).
    pub appended: usize,
}

/// Refusals and failures of [`seed_demo`], each with a says-why message.
#[derive(Debug, thiserror::Error)]
pub enum SeedError {
    /// `--db` pointed at the default daemon journal (§3.4 refuses it).
    #[error(
        "refusing to seed the default journal path {path} (pass an explicit throwaway \
             --db; demo data must never mix into a real daemon's journal)"
    )]
    DefaultJournalPath {
        /// The refused path.
        path: PathBuf,
    },
    /// `--db` pointed at a journal that already holds events.
    #[error(
        "refusing to seed non-empty journal {path}: {event_count} event(s) already present \
             (seed targets must be fresh files)"
    )]
    JournalNotEmpty {
        /// The refused path.
        path: PathBuf,
        /// Events already in that journal.
        event_count: i64,
    },
    /// The fixture could not be read or parsed as an array of §3.1 events.
    #[error("fixture {path} is not a valid event array: {reason}")]
    FixtureInvalid {
        /// The fixture path.
        path: PathBuf,
        /// What was wrong with it.
        reason: String,
    },
    /// Filesystem failure (reading the fixture, creating the journal).
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Journal failure (open or append).
    #[error(transparent)]
    Core(#[from] CoreError),
}

/// Append the fixture's events into `db_path` (§3.4).
///
/// Creates the journal when missing, refuses it when non-empty or the
/// default path, stamps `demo: true` into every payload, and appends in
/// fixture order via the normal [`events::append_event`] path — the seeded
/// corpus is indistinguishable from a real journal on the wire.
pub fn seed_demo(db_path: &Path, fixture_path: &Path) -> Result<SeedReport, SeedError> {
    if is_same_path(db_path, &db::default_journal_path()) {
        return Err(SeedError::DefaultJournalPath {
            path: db_path.to_path_buf(),
        });
    }

    let fixture = std::fs::read_to_string(fixture_path)?;
    let events = parse_fixture(&fixture).map_err(|reason| SeedError::FixtureInvalid {
        path: fixture_path.to_path_buf(),
        reason,
    })?;

    let conn = db::open_db(db_path)?;
    let stats = events::journal_stats(&conn)?;
    if stats.event_count > 0 {
        return Err(SeedError::JournalNotEmpty {
            path: db_path.to_path_buf(),
            event_count: stats.event_count,
        });
    }

    let mut appended = 0;
    for event in stamp_demo(events) {
        events::append_event(&conn, &event)?;
        appended += 1;
    }
    Ok(SeedReport { appended })
}

/// Parse the fixture text as a JSON array of §3.1 event objects. Each
/// element goes through agentos-core's own (total) `Event` serde, so the
/// fixture exercises the same forward-compat rules as journal reads.
fn parse_fixture(fixture: &str) -> Result<Vec<Event>, String> {
    let root: Value =
        serde_json::from_str(fixture).map_err(|err| format!("not valid JSON: {err}"))?;
    let array = root
        .as_array()
        .ok_or_else(|| "top level must be an array".to_owned())?;
    if array.is_empty() {
        return Err("array is empty".to_owned());
    }
    array
        .iter()
        .enumerate()
        .map(|(index, element)| {
            serde_json::from_value(element.clone())
                .map_err(|err| format!("element {index} is not a contract §3.1 event: {err}"))
        })
        .collect()
}

/// Force `"demo": true` into every payload. Non-object payloads (a bare
/// string, `null`) are replaced by `{"demo": true}` — the stamp is the
/// marker that makes demo rows greppable, and no fixture payload carries
/// information worth keeping without it.
fn stamp_demo(events: Vec<Event>) -> impl Iterator<Item = Event> {
    events.into_iter().map(|mut event| {
        let payload = event.payload.take();
        event.payload = match payload {
            Value::Object(mut map) => {
                map.insert("demo".to_owned(), Value::Bool(true));
                Value::Object(map)
            }
            _ => serde_json::json!({ "demo": true }),
        };
        event
    })
}

/// Path equality robust to the Windows mixtures a user can type at `--db`
/// (case, separators): compare normalized absolute-ish string forms when
/// direct equality fails. Fall back to direct comparison when the paths
/// cannot be canonicalized (not existing yet is normal for `--db`).
fn is_same_path(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    let normalize =
        |path: &Path| -> String { path.to_string_lossy().to_lowercase().replace('\\', "/") };
    normalize(a) == normalize(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_stamp_marks_every_payload_kind() {
        let events = vec![
            Event::new(agentos_core::EventType::RunCreated)
                .with_payload(serde_json::json!({"goal": "x"})),
            Event::new(agentos_core::EventType::TaskDone),
        ];
        let stamped: Vec<Event> = stamp_demo(events).collect();
        assert_eq!(stamped[0].payload["demo"], serde_json::json!(true));
        assert_eq!(stamped[0].payload["goal"], serde_json::json!("x"));
        assert_eq!(stamped[1].payload, serde_json::json!({ "demo": true }));
    }

    #[test]
    fn fixture_parsing_rejects_non_arrays_and_bad_events() {
        assert!(parse_fixture("{}").is_err());
        assert!(parse_fixture("[]").is_err());
        assert!(parse_fixture("[{]}").is_err());
        // Missing the required id/eventType/occurredAt triple.
        assert!(parse_fixture(r#"[{"id": "019250ab-1e70-7c9a-9a1e-4f2b6c8d0001"}]"#).is_err());
        let ok = parse_fixture(
            r#"[{"id": "019250ab-1e70-7c9a-9a1e-4f2b6c8d0001", "eventType": "run.created",
                 "occurredAt": "2026-08-22T09:15:00Z"}]"#,
        );
        assert!(ok.is_ok());
    }
}
