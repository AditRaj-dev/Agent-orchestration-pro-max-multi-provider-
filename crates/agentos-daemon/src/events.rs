//! Append-only event journal on SQLite (PRD §18.1/§18.2; F-00 §3).
//!
//! The `events` table is created by migration v1 in [`crate::db`]; UPDATE
//! and DELETE are rejected by database triggers, so immutability is enforced
//! by the storage engine itself, not by caller discipline.
//!
//! Row mapping deliberately shuttles through agentos-core's own serde
//! implementation for [`Event`]: on append the event is serialized once and
//! the wire fields (camelCase names, RFC 3339 timestamps, string event
//! types) are stored in the corresponding columns; on read the columns are
//! reassembled into that same wire shape and deserialized. Wire conventions
//! — including [`EventType::Other`] round-tripping verbatim and tolerance
//! for unknown JSON keys — are therefore guaranteed by construction instead
//! of being re-implemented here.

use agentos_core::{CoreError, Event};
use rusqlite::{Connection, Row};
use serde_json::Value;
use uuid::Uuid;

use crate::db::map_sqlite_error;

/// Column list shared by every SELECT in this module.
const EVENT_COLUMNS: &str = "id, event_type, occurred_at, run_id, trace_id, task_id, \
                            agent_id, payload, payload_ref, payload_hash, schema_version";

const INSERT_EVENT: &str = "INSERT INTO events (
    id, event_type, occurred_at, run_id, trace_id, task_id, agent_id,
    payload, payload_ref, payload_hash, schema_version
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";

/// Flattened wire representation of an [`Event`], ready for column binding.
struct EventColumns {
    id: String,
    event_type: String,
    occurred_at: String,
    run_id: Option<String>,
    trace_id: Option<String>,
    task_id: Option<String>,
    agent_id: Option<String>,
    payload: String,
    payload_ref: Option<String>,
    payload_hash: Option<String>,
    schema_version: i64,
}

/// Serialize `event` via agentos-core's serde impl and split the wire object
/// into row columns.
fn event_columns(event: &Event) -> Result<EventColumns, CoreError> {
    let wire = serde_json::to_value(event)
        .map_err(|err| CoreError::Serialization(format!("event serialization failed: {err}")))?;

    let required = |key: &str| -> Result<String, CoreError> {
        wire.get(key)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                CoreError::Serialization(format!(
                    "event serialization produced no string for required field {key}"
                ))
            })
    };
    let optional = |key: &str| wire.get(key).and_then(Value::as_str).map(str::to_owned);

    let schema_version = wire
        .get("schemaVersion")
        .and_then(Value::as_u64)
        .ok_or_else(|| CoreError::Serialization("event is missing schemaVersion".to_owned()))?;

    Ok(EventColumns {
        id: required("id")?,
        event_type: required("eventType")?,
        occurred_at: required("occurredAt")?,
        run_id: optional("runId"),
        trace_id: optional("traceId"),
        task_id: optional("taskId"),
        agent_id: optional("agentId"),
        payload: wire
            .get("payload")
            .cloned()
            .unwrap_or(Value::Null)
            .to_string(),
        payload_ref: optional("payloadRef"),
        payload_hash: optional("payloadHash"),
        schema_version: schema_version as i64,
    })
}

/// Append one event to the journal inside an explicit transaction and return
/// its assigned `seq`.
///
/// `BEGIN IMMEDIATE` takes the write lock up front (better WAL citizenship
/// than an upgrade mid-transaction); on failure the transaction is rolled
/// back and the error mapped — SQLITE_BUSY surfaces as retryable
/// [`CoreError::SqliteBusy`].
pub fn append_event(conn: &Connection, event: &Event) -> Result<i64, CoreError> {
    let columns = event_columns(event)?;

    conn.execute("BEGIN IMMEDIATE", [])
        .map_err(map_sqlite_error)?;
    let inserted = conn.execute(
        INSERT_EVENT,
        rusqlite::params![
            columns.id,
            columns.event_type,
            columns.occurred_at,
            columns.run_id,
            columns.trace_id,
            columns.task_id,
            columns.agent_id,
            columns.payload,
            columns.payload_ref,
            columns.payload_hash,
            columns.schema_version,
        ],
    );
    match inserted {
        Ok(_) => {
            conn.execute("COMMIT", []).map_err(map_sqlite_error)?;
            Ok(conn.last_insert_rowid())
        }
        Err(err) => {
            let _ = conn.execute("ROLLBACK", []);
            Err(map_sqlite_error(err))
        }
    }
}

/// All events of one run, oldest first (index: `run_id, seq`).
pub fn events_for_run(conn: &Connection, run_id: &Uuid) -> Result<Vec<Event>, CoreError> {
    let run_id = run_id.to_string();
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE run_id = ?1 ORDER BY seq ASC"
        ))
        .map_err(map_sqlite_error)?;
    let rows = stmt
        .query_map(rusqlite::params![run_id], row_to_event)
        .map_err(map_sqlite_error)?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite_error)
}

/// Events with `seq > after_seq`, oldest first, at most `limit` — the feed
/// projections tail to stay current without re-reading the journal.
pub fn tail(conn: &Connection, after_seq: i64, limit: u32) -> Result<Vec<Event>, CoreError> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {EVENT_COLUMNS} FROM events WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2"
        ))
        .map_err(map_sqlite_error)?;
    let rows = stmt
        .query_map(rusqlite::params![after_seq, limit], row_to_event)
        .map_err(map_sqlite_error)?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite_error)
}

/// An [`Event`] together with the journal `seq` it was assigned.
///
/// The F-11 wire shape (§3.1) is the core event **plus** `seq`, and the
/// projections (§3.3) need `firstSeq`/`lastSeq` — so the F-11a read paths
/// return this pair instead of a bare [`Event`]. [`tail`] stays as the
/// F-01 primitive; [`tail_with_seq`] is its F-11 sibling.
#[derive(Debug, Clone, PartialEq)]
pub struct SequencedEvent {
    /// Journal sequence number (append order, gapless per journal).
    pub seq: i64,
    /// The event itself.
    pub event: Event,
}

/// Events with `seq > after_seq`, oldest first, at most `limit`, each
/// carrying its `seq` — the F-11a read primitive behind `events.list` and
/// the subscription replay/tail loops.
pub fn tail_with_seq(
    conn: &Connection,
    after_seq: i64,
    limit: u32,
) -> Result<Vec<SequencedEvent>, CoreError> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT seq, {EVENT_COLUMNS} FROM events WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2"
        ))
        .map_err(map_sqlite_error)?;
    let rows = stmt
        .query_map(rusqlite::params![after_seq, limit], row_to_sequenced_event)
        .map_err(map_sqlite_error)?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite_error)
}

/// Whole-journal counters for `daemon.info` and `events.list`: total event
/// count and the highest assigned `seq` (0 for an empty journal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalStats {
    /// `COUNT(*)` over the journal.
    pub event_count: i64,
    /// `COALESCE(MAX(seq), 0)`.
    pub last_seq: i64,
}

/// One cheap aggregate query for [`JournalStats`].
pub fn journal_stats(conn: &Connection) -> Result<JournalStats, CoreError> {
    conn.query_row(
        "SELECT COUNT(*), COALESCE(MAX(seq), 0) FROM events",
        [],
        |row| {
            Ok(JournalStats {
                event_count: row.get(0)?,
                last_seq: row.get(1)?,
            })
        },
    )
    .map_err(map_sqlite_error)
}

/// Reuse [`row_to_event`]'s wire reconstruction for the seq-carrying reads.
fn row_to_sequenced_event(row: &Row<'_>) -> Result<SequencedEvent, rusqlite::Error> {
    let seq: i64 = row.get("seq")?;
    Ok(SequencedEvent {
        seq,
        event: row_to_event(row)?,
    })
}

/// Rebuild an [`Event`] from a row by reconstructing agentos-core's wire
/// shape and deserializing it. Deserialization is total for event types
/// (unknown strings become [`agentos_core::EventType::Other`]) and ignores
/// unknown JSON keys, so journals written by newer producers stay readable.
fn row_to_event(row: &Row<'_>) -> Result<Event, rusqlite::Error> {
    let id: String = row.get("id")?;
    let event_type: String = row.get("event_type")?;
    let occurred_at: String = row.get("occurred_at")?;
    let run_id: Option<String> = row.get("run_id")?;
    let trace_id: Option<String> = row.get("trace_id")?;
    let task_id: Option<String> = row.get("task_id")?;
    let agent_id: Option<String> = row.get("agent_id")?;
    let payload_text: String = row.get("payload")?;
    let payload_ref: Option<String> = row.get("payload_ref")?;
    let payload_hash: Option<String> = row.get("payload_hash")?;
    let schema_version: i64 = row.get("schema_version")?;

    let payload: Value = serde_json::from_str(&payload_text).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
    })?;

    let wire = serde_json::json!({
        "id": id,
        "eventType": event_type,
        "occurredAt": occurred_at,
        "runId": run_id,
        "traceId": trace_id,
        "taskId": task_id,
        "agentId": agent_id,
        "payload": payload,
        "payloadRef": payload_ref,
        "payloadHash": payload_hash,
        "schemaVersion": schema_version,
    });

    serde_json::from_value(wire).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_core::EventType;
    use serde_json::json;

    /// The wire shape stored in rows must tolerate forward-compat additions:
    /// deserializing an event object with unknown top-level keys succeeds
    /// (serde ignores them) — the F-00 §3 / PRD §18.2 upgradability rule.
    #[test]
    fn event_deserialization_tolerates_unknown_keys() {
        let wire = json!({
            "id": Uuid::now_v7(),
            "eventType": "custom.x",
            "occurredAt": "2026-08-22T12:00:00.123456789Z",
            "futureFieldNoBuildKnows": { "anything": true },
            "payload": { "k": 1 },
            "schemaVersion": 9
        });

        let event: Event =
            serde_json::from_value(wire).expect("unknown top-level keys must not fail");
        assert_eq!(event.event_type, EventType::Other("custom.x".to_owned()));
        assert_eq!(event.payload, json!({ "k": 1 }));
        assert_eq!(event.schema_version, 9);
    }

    /// Event types this build does not know keep their exact wire string
    /// through column mapping, both directions.
    #[test]
    fn other_event_type_survives_column_flattening() {
        let event = Event::new(EventType::Other("custom.x".to_owned()))
            .with_payload(json!({ "items": [1, 2, 3] }));

        let columns = event_columns(&event).unwrap();
        assert_eq!(columns.event_type, "custom.x");
        assert_eq!(columns.payload, r#"{"items":[1,2,3]}"#);
    }

    /// Offloaded payloads (PRD §18.2: large payloads are artifacts; events
    /// hold refs + hashes) flatten to the ref/hash columns with a JSON
    /// `null` inline payload.
    #[test]
    fn offloaded_payload_flattens_to_ref_and_hash() {
        let event = Event::new(EventType::FileChanged)
            .with_payload_ref("sha256:9f2cfe1a")
            .with_payload_hash("sha256:9f2cfe1a");

        let columns = event_columns(&event).unwrap();
        assert_eq!(columns.payload, "null");
        assert_eq!(columns.payload_ref.as_deref(), Some("sha256:9f2cfe1a"));
        assert_eq!(columns.payload_hash.as_deref(), Some("sha256:9f2cfe1a"));
    }
}
