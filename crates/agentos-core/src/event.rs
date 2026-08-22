//! Canonical event model (PRD §18.1/§18.2; F-00 §3 event rules).
//!
//! Rules encoded here:
//!
//! - Events are append-only and immutable; corrections are new events.
//! - Every event carries `run_id` where applicable and `trace_id` for
//!   distributed correlation.
//! - Large payloads are offloaded to content-addressed artifacts; the event
//!   then holds only a `payload_ref` and a `payload_hash`.
//! - Unknown event type strings parse as [`EventType::Other`] so journals
//!   written by newer producers remain readable — events must never fail to
//!   parse on upgrade.
//! - Consumers are expected to be idempotent (retries may re-deliver); that
//!   is a consumer obligation, not something this type can enforce.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use uuid::Uuid;

/// Schema version stamped on every newly created [`Event`].
pub const EVENT_SCHEMA_VERSION: u32 = 1;

/// The type of a journal event, serialized as a dotted snake_case string.
///
/// The named variants are the canon wire types of PRD §18.2. Because events
/// are immutable and the journal outlives any single build, parsing is
/// total: an unknown type string deserializes to [`EventType::Other`] rather
/// than failing. Serializing [`EventType::Other`] emits the original string
/// verbatim, so unknown types round-trip losslessly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventType {
    /// A run was created (`run.created`).
    RunCreated,
    /// A workflow definition started executing (`workflow.started`).
    WorkflowStarted,
    /// A task entered the system (`task.created`).
    TaskCreated,
    /// A task became claimable (`task.ready`).
    TaskReady,
    /// An agent acquired a lease on a task (`agent.leased`).
    AgentLeased,
    /// Leased work began executing (`task.running`).
    TaskRunning,
    /// A watched file changed (`file.changed`).
    FileChanged,
    /// Compiled context became stale (`context.invalidated`).
    ContextInvalidated,
    /// A task produced its output artifact (`task.output_ready`).
    TaskOutputReady,
    /// A quality gate was requested (`review.requested`).
    ReviewRequested,
    /// A quality gate passed (`review.approved`).
    ReviewApproved,
    /// A quality gate failed (`review.failed`).
    ReviewFailed,
    /// A mutation request entered the serialized git queue (`git.queued`).
    GitQueued,
    /// A mutation request was committed (`git.committed`).
    GitCommitted,
    /// An agent process died unexpectedly (`agent.crashed`).
    AgentCrashed,
    /// A budget ceiling was hit (`budget.exceeded`).
    BudgetExceeded,
    /// A task reached its terminal success state (`task.done`).
    TaskDone,
    /// The run finished (`run.completed`).
    RunCompleted,
    /// Forward compatibility: any type string this build does not know.
    Other(String),
}

impl EventType {
    /// The canonical wire string for this event type.
    pub fn as_str(&self) -> &str {
        match self {
            EventType::RunCreated => "run.created",
            EventType::WorkflowStarted => "workflow.started",
            EventType::TaskCreated => "task.created",
            EventType::TaskReady => "task.ready",
            EventType::AgentLeased => "agent.leased",
            EventType::TaskRunning => "task.running",
            EventType::FileChanged => "file.changed",
            EventType::ContextInvalidated => "context.invalidated",
            EventType::TaskOutputReady => "task.output_ready",
            EventType::ReviewRequested => "review.requested",
            EventType::ReviewApproved => "review.approved",
            EventType::ReviewFailed => "review.failed",
            EventType::GitQueued => "git.queued",
            EventType::GitCommitted => "git.committed",
            EventType::AgentCrashed => "agent.crashed",
            EventType::BudgetExceeded => "budget.exceeded",
            EventType::TaskDone => "task.done",
            EventType::RunCompleted => "run.completed",
            EventType::Other(other) => other,
        }
    }

    /// Parse a wire string. Infallible by design: unknown strings become
    /// [`EventType::Other`] so that immutable events from newer producers
    /// still deserialize.
    pub fn parse(s: &str) -> Self {
        match s {
            "run.created" => EventType::RunCreated,
            "workflow.started" => EventType::WorkflowStarted,
            "task.created" => EventType::TaskCreated,
            "task.ready" => EventType::TaskReady,
            "agent.leased" => EventType::AgentLeased,
            "task.running" => EventType::TaskRunning,
            "file.changed" => EventType::FileChanged,
            "context.invalidated" => EventType::ContextInvalidated,
            "task.output_ready" => EventType::TaskOutputReady,
            "review.requested" => EventType::ReviewRequested,
            "review.approved" => EventType::ReviewApproved,
            "review.failed" => EventType::ReviewFailed,
            "git.queued" => EventType::GitQueued,
            "git.committed" => EventType::GitCommitted,
            "agent.crashed" => EventType::AgentCrashed,
            "budget.exceeded" => EventType::BudgetExceeded,
            "task.done" => EventType::TaskDone,
            "run.completed" => EventType::RunCompleted,
            other => EventType::Other(other.to_owned()),
        }
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for EventType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for EventType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = String::deserialize(deserializer)?;
        Ok(EventType::parse(&wire))
    }
}

/// A single append-only journal event (PRD §18.1 "Event" entity).
///
/// Field names serialize in camelCase to match the PRD data model
/// (`runId`, `traceId`, `taskId`, ...). Events are immutable after append;
/// corrections are new events, never mutations of existing ones.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    /// Time-ordered UUIDv7 identity. v7 keeps the journal sortable by
    /// creation time without a separate sequence column.
    pub id: Uuid,
    /// What happened. See [`EventType`] for the canon set.
    pub event_type: EventType,
    /// When the event actually occurred (not when it was persisted).
    pub occurred_at: DateTime<Utc>,
    /// Owning run, where applicable (F-00 §3: every event carries `run_id`
    /// where applicable).
    pub run_id: Option<Uuid>,
    /// Correlation ID for distributed tracing.
    pub trace_id: Option<Uuid>,
    /// The task this event concerns, if any.
    pub task_id: Option<Uuid>,
    /// Agent (instance) identifier, e.g. a worker name or session handle.
    pub agent_id: Option<String>,
    /// Inline payload. `null` when the payload was offloaded to a
    /// content-addressed artifact. Defaults to `null` when absent on the wire.
    #[serde(default)]
    pub payload: Value,
    /// Address of the content-addressed artifact holding a large payload.
    pub payload_ref: Option<String>,
    /// Integrity hash of the offloaded payload.
    pub payload_hash: Option<String>,
    /// Event schema version; newly created events carry
    /// [`EVENT_SCHEMA_VERSION`].
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
}

fn default_schema_version() -> u32 {
    EVENT_SCHEMA_VERSION
}

impl Event {
    /// Create a new event of the given type, stamped with a fresh UUIDv7 id
    /// and the current time. All correlation fields start as `None` and the
    /// payload as `null`; chain the `with_*` methods to fill them in.
    pub fn new(event_type: EventType) -> Self {
        Self {
            id: Uuid::now_v7(),
            event_type,
            occurred_at: Utc::now(),
            run_id: None,
            trace_id: None,
            task_id: None,
            agent_id: None,
            payload: Value::Null,
            payload_ref: None,
            payload_hash: None,
            schema_version: EVENT_SCHEMA_VERSION,
        }
    }

    /// Attach the owning run id.
    pub fn with_run_id(mut self, run_id: Uuid) -> Self {
        self.run_id = Some(run_id);
        self
    }

    /// Attach the distributed-tracing correlation id.
    pub fn with_trace_id(mut self, trace_id: Uuid) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    /// Attach the concerned task id.
    pub fn with_task_id(mut self, task_id: Uuid) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Attach the producing agent's identifier.
    pub fn with_agent_id(mut self, agent_id: impl Into<String>) -> Self {
        self.agent_id = Some(agent_id.into());
        self
    }

    /// Attach a small inline payload. For large payloads, offload to a
    /// content-addressed artifact and use [`Event::with_payload_ref`] and
    /// [`Event::with_payload_hash`] instead.
    pub fn with_payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }

    /// Reference a content-addressed artifact holding the (large) payload.
    pub fn with_payload_ref(mut self, payload_ref: impl Into<String>) -> Self {
        self.payload_ref = Some(payload_ref.into());
        self
    }

    /// Attach the integrity hash of the offloaded payload.
    pub fn with_payload_hash(mut self, payload_hash: impl Into<String>) -> Self {
        self.payload_hash = Some(payload_hash.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashSet;

    /// Exhaustive wire mapping for every named variant — the canon table.
    fn named_wire_types() -> Vec<(EventType, &'static str)> {
        vec![
            (EventType::RunCreated, "run.created"),
            (EventType::WorkflowStarted, "workflow.started"),
            (EventType::TaskCreated, "task.created"),
            (EventType::TaskReady, "task.ready"),
            (EventType::AgentLeased, "agent.leased"),
            (EventType::TaskRunning, "task.running"),
            (EventType::FileChanged, "file.changed"),
            (EventType::ContextInvalidated, "context.invalidated"),
            (EventType::TaskOutputReady, "task.output_ready"),
            (EventType::ReviewRequested, "review.requested"),
            (EventType::ReviewApproved, "review.approved"),
            (EventType::ReviewFailed, "review.failed"),
            (EventType::GitQueued, "git.queued"),
            (EventType::GitCommitted, "git.committed"),
            (EventType::AgentCrashed, "agent.crashed"),
            (EventType::BudgetExceeded, "budget.exceeded"),
            (EventType::TaskDone, "task.done"),
            (EventType::RunCompleted, "run.completed"),
        ]
    }

    #[test]
    fn named_event_types_serialize_to_exact_snake_case_strings() {
        for (variant, wire) in named_wire_types() {
            assert_eq!(
                serde_json::to_value(&variant).unwrap(),
                json!(wire),
                "serializing {variant:?}"
            );
            assert_eq!(variant.as_str(), wire);
            assert_eq!(variant.to_string(), wire);
            // ...and the exact wire string parses back to the named variant.
            assert_eq!(EventType::parse(wire), variant, "parsing {wire}");
            let round_tripped: EventType = serde_json::from_value(json!(wire)).expect(wire);
            assert_eq!(round_tripped, variant);
        }
    }

    #[test]
    fn named_wire_strings_are_unique() {
        let names: HashSet<&str> = named_wire_types().iter().map(|(_, w)| *w).collect();
        assert_eq!(names.len(), named_wire_types().len());
    }

    #[test]
    fn other_variant_round_trips_verbatim() {
        let variant = EventType::Other("custom.thing".to_owned());
        assert_eq!(
            serde_json::to_value(&variant).unwrap(),
            json!("custom.thing")
        );
        let round_tripped: EventType = serde_json::from_value(json!("custom.thing")).unwrap();
        assert_eq!(round_tripped, variant);
    }

    #[test]
    fn unknown_wire_string_parses_to_other() {
        let parsed: EventType = serde_json::from_value(json!("future.event")).unwrap();
        assert_eq!(parsed, EventType::Other("future.event".to_owned()));
        assert_eq!(EventType::parse("future.event"), parsed);
    }

    #[test]
    fn new_event_is_stamped_with_v7_id_and_now() {
        let before = Utc::now();
        let event = Event::new(EventType::TaskCreated);
        let after = Utc::now();

        assert_eq!(
            event.id.get_version_num(),
            7,
            "event id must be UUIDv7 (time-ordered)"
        );
        assert!((before..=after).contains(&event.occurred_at));
        assert_eq!(event.event_type, EventType::TaskCreated);
        assert_eq!(event.payload, Value::Null);
        assert_eq!(event.schema_version, EVENT_SCHEMA_VERSION);
        assert!(event.run_id.is_none());
        assert!(event.trace_id.is_none());
        assert!(event.task_id.is_none());
        assert!(event.agent_id.is_none());
        assert!(event.payload_ref.is_none());
        assert!(event.payload_hash.is_none());
    }

    #[test]
    fn event_json_round_trip_uses_camel_case_fields() {
        let event = Event::new(EventType::TaskDone)
            .with_run_id(Uuid::now_v7())
            .with_trace_id(Uuid::now_v7())
            .with_task_id(Uuid::now_v7())
            .with_agent_id("worker-07")
            .with_payload(json!({ "ok": true, "attempts": 2 }));

        let value = serde_json::to_value(&event).unwrap();
        for key in [
            "id",
            "eventType",
            "occurredAt",
            "runId",
            "traceId",
            "taskId",
            "agentId",
            "payload",
            "payloadRef",
            "payloadHash",
            "schemaVersion",
        ] {
            assert!(
                value.get(key).is_some(),
                "expected camelCase key {key} in {value}"
            );
        }
        assert_eq!(value["eventType"], json!("task.done"));
        assert_eq!(value["agentId"], json!("worker-07"));

        let round_tripped: Event = serde_json::from_value(value).unwrap();
        assert_eq!(round_tripped, event);
    }

    #[test]
    fn event_with_offloaded_payload_round_trips() {
        let event = Event::new(EventType::FileChanged)
            .with_payload_ref("sha256:9f2cfe1a")
            .with_payload_hash("sha256:9f2cfe1a");

        assert_eq!(event.payload, Value::Null);
        assert_eq!(event.payload_ref.as_deref(), Some("sha256:9f2cfe1a"));

        let round_tripped: Event =
            serde_json::from_value(serde_json::to_value(&event).unwrap()).unwrap();
        assert_eq!(round_tripped, event);
    }

    #[test]
    fn event_deserialization_tolerates_missing_optional_fields() {
        // A minimal journal row must still parse: payload and schemaVersion
        // carry defaults, everything optional is absent.
        let parsed: Event = serde_json::from_value(json!({
            "id": Uuid::now_v7(),
            "eventType": "task.ready",
            "occurredAt": "2026-08-22T12:00:00Z"
        }))
        .unwrap();
        assert_eq!(parsed.event_type, EventType::TaskReady);
        assert_eq!(parsed.payload, Value::Null);
        assert_eq!(parsed.schema_version, EVENT_SCHEMA_VERSION);
        assert!(parsed.run_id.is_none());
    }
}
