//! Audit trail (PRD §14 SEC-05): a chronological, append-only record of
//! user/agent/tool/permission/secret/git/workflow/policy events, with run
//! bundle export.
//!
//! Append-only is enforced **in the database**, not by convention: `UPDATE`
//! and `DELETE` on `audit_log` are blocked by `RAISE(ABORT, ...)` triggers,
//! so even a rogue direct connection cannot rewrite history without dropping
//! the schema. Corrections are new events (F-00 §3 event rules). The
//! `record_*` helpers make the security-critical shapes hard to get wrong —
//! notably [`AuditStore::record_secret_grant`] takes a **scope, never a
//! value** (SEC-03: secrets never appear in transcripts or audit).

use std::path::Path;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use uuid::Uuid;

use crate::approval::{ApprovalDecision, ApprovalRequest};
use crate::compile::PolicyDenial;
use crate::error::{db, PolicyError};
use crate::permission::GitAction;
use crate::store::{lock_guard, now_ts, open_db};

/// One audit record as appended (id and timestamp are assigned by the
/// store).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEntry {
    /// Who acted: a user id, agent id, or `"system"`.
    pub actor: String,
    /// Dotted kind, e.g. `permission.denied` (see the `record_*` helpers).
    pub action_kind: String,
    /// What was acted on (path, tool, scope, request id, ...).
    pub resource: String,
    /// Structured, JSON-object details. Never secret values.
    pub details: serde_json::Value,
    /// Owning run, where applicable.
    pub run_id: Option<String>,
}

/// SQLite-backed append-only audit log (F-01 canon open; see
/// [`crate::store`]).
#[derive(Debug)]
pub struct AuditStore {
    conn: Mutex<Connection>,
}

impl AuditStore {
    /// Open (creating if needed) the audit database at `path` and install
    /// the idempotent schema + tamper triggers.
    pub fn open(path: &Path) -> Result<Self, PolicyError> {
        let conn = open_db(path)?;
        conn.execute_batch(SCHEMA).map_err(db)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Append one entry; returns the assigned id (UUID v7, time-ordered).
    pub fn append(&self, entry: AuditEntry) -> Result<String, PolicyError> {
        if !entry.details.is_object() {
            return Err(PolicyError::Invalid(
                "audit details must be a JSON object".to_owned(),
            ));
        }
        let id = Uuid::now_v7().to_string();
        let conn = lock_guard(&self.conn);
        conn.execute(
            "INSERT INTO audit_log (id, ts, actor, action_kind, resource, details, run_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id,
                now_ts(),
                entry.actor,
                entry.action_kind,
                entry.resource,
                entry.details.to_string(),
                entry.run_id
            ],
        )
        .map_err(db)?;
        Ok(id)
    }

    /// Export the SEC-05 run audit bundle as JSON: everything for `run_id`,
    /// or the whole log when `run_id` is `None`, in chronological order.
    pub fn export_bundle(&self, run_id: Option<&str>) -> Result<serde_json::Value, PolicyError> {
        let conn = lock_guard(&self.conn);
        let mut stmt = conn
            .prepare(
                "SELECT id, ts, actor, action_kind, resource, details, run_id FROM audit_log \
                 WHERE (?1 IS NULL OR run_id = ?1) ORDER BY ts ASC, id ASC",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map(params![run_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })
            .map_err(db)?;

        let mut events = Vec::new();
        for row in rows {
            let (id, ts, actor, action_kind, resource, details, row_run_id) = row.map_err(db)?;
            let details: serde_json::Value =
                serde_json::from_str(&details).map_err(PolicyError::Json)?;
            events.push(serde_json::json!({
                "id": id,
                "ts": ts,
                "actor": actor,
                "actionKind": action_kind,
                "resource": resource,
                "details": details,
                "runId": row_run_id,
            }));
        }
        Ok(serde_json::json!({
            "runId": run_id,
            "generatedAt": now_ts(),
            "eventCount": events.len(),
            "events": events,
        }))
    }

    /// A policy refusal (SEC-05 "permission events"): what was denied, to
    /// whom, and why.
    pub fn record_permission_denial(
        &self,
        actor: &str,
        resource: &str,
        denial: &PolicyDenial,
        run_id: Option<&str>,
    ) -> Result<String, PolicyError> {
        self.append(AuditEntry {
            actor: actor.to_owned(),
            action_kind: "permission.denied".to_owned(),
            resource: resource.to_owned(),
            details: serde_json::json!({ "reason": denial.to_string() }),
            run_id: run_id.map(str::to_owned),
        })
    }

    /// A secret grant. Takes the **scope only — never the value** (the
    /// signature makes leaking a value impossible; SEC-03).
    pub fn record_secret_grant(
        &self,
        actor: &str,
        scope: &str,
        task_id: &str,
        expires_at: DateTime<Utc>,
        run_id: Option<&str>,
    ) -> Result<String, PolicyError> {
        self.append(AuditEntry {
            actor: actor.to_owned(),
            action_kind: "secret.granted".to_owned(),
            resource: scope.to_owned(),
            details: serde_json::json!({
                "scope": scope,
                "taskId": task_id,
                "expiresAt": expires_at.to_rfc3339(),
            }),
            run_id: run_id.map(str::to_owned),
        })
    }

    /// A secret revocation (scope only, as above).
    pub fn record_secret_revoke(
        &self,
        actor: &str,
        scope: &str,
        task_id: &str,
        run_id: Option<&str>,
    ) -> Result<String, PolicyError> {
        self.append(AuditEntry {
            actor: actor.to_owned(),
            action_kind: "secret.revoked".to_owned(),
            resource: scope.to_owned(),
            details: serde_json::json!({ "scope": scope, "taskId": task_id }),
            run_id: run_id.map(str::to_owned),
        })
    }

    /// An approval request being made or resolved (`decision` `None` =
    /// requested).
    pub fn record_approval(
        &self,
        actor: &str,
        request: &ApprovalRequest,
        decision: Option<ApprovalDecision>,
        run_id: Option<&str>,
    ) -> Result<String, PolicyError> {
        let (kind, decision_json) = match decision {
            None => ("approval.requested", serde_json::Value::Null),
            Some(decision) => ("approval.resolved", serde_json::json!(decision.as_str())),
        };
        self.append(AuditEntry {
            actor: actor.to_owned(),
            action_kind: kind.to_owned(),
            resource: request.id.clone(),
            details: serde_json::json!({
                "gate": request.gate.as_str(),
                "requestId": request.id,
                "operationFingerprint": request.operation_fingerprint,
                "requestedBy": request.requested_by,
                "expiresAt": request.expires_at.to_rfc3339(),
                "decision": decision_json,
            }),
            run_id: run_id.map(str::to_owned),
        })
    }

    /// A git-gate decision (permission and/or approval outcome).
    pub fn record_git_gate(
        &self,
        actor: &str,
        action: GitAction,
        allowed: bool,
        reason: &str,
        run_id: Option<&str>,
    ) -> Result<String, PolicyError> {
        self.append(AuditEntry {
            actor: actor.to_owned(),
            action_kind: "git.gate".to_owned(),
            resource: action.as_str().to_owned(),
            details: serde_json::json!({ "action": action.as_str(), "allowed": allowed, "reason": reason }),
            run_id: run_id.map(str::to_owned),
        })
    }
}

/// Schema (idempotent): one `audit_log` table; `UPDATE`/`DELETE` are blocked
/// by triggers so the log is append-only at the storage layer.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS audit_log (\
    id TEXT PRIMARY KEY,\
    ts TEXT NOT NULL,\
    actor TEXT NOT NULL,\
    action_kind TEXT NOT NULL,\
    resource TEXT NOT NULL,\
    details TEXT NOT NULL,\
    run_id TEXT\
);\
CREATE INDEX IF NOT EXISTS idx_audit_run_id ON audit_log(run_id);\
CREATE TRIGGER IF NOT EXISTS audit_log_no_update BEFORE UPDATE ON audit_log \
BEGIN SELECT RAISE(ABORT, 'audit_log is append-only: UPDATE is forbidden'); END;\
CREATE TRIGGER IF NOT EXISTS audit_log_no_delete BEFORE DELETE ON audit_log \
BEGIN SELECT RAISE(ABORT, 'audit_log is append-only: DELETE is forbidden'); END;\
";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::Gate;
    use crate::secrets::SecretsBroker;
    use chrono::Duration;
    use serde_json::json;

    fn store() -> (tempfile::TempDir, AuditStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = AuditStore::open(&dir.path().join("audit.sqlite3")).expect("open");
        (dir, store)
    }

    fn entry(actor: &str, resource: &str, run_id: Option<&str>) -> AuditEntry {
        AuditEntry {
            actor: actor.to_owned(),
            action_kind: "tool.used".to_owned(),
            resource: resource.to_owned(),
            details: json!({ "tool": "Read", "ok": true }),
            run_id: run_id.map(str::to_owned),
        }
    }

    #[test]
    fn append_and_export_bundle_shape() {
        let (_dir, store) = store();
        store
            .append(entry("user", "goal.md", Some("run-1")))
            .expect("append");
        store
            .append(entry("agent-07", "src/lib.rs", Some("run-1")))
            .expect("append");
        store
            .append(entry("agent-08", "src/other.rs", Some("run-2")))
            .expect("append");
        store
            .append(entry("system", "cleanup", None))
            .expect("append");

        let bundle = store.export_bundle(Some("run-1")).expect("export");
        assert_eq!(bundle["runId"], json!("run-1"));
        assert_eq!(bundle["eventCount"], json!(2));
        let events = bundle["events"].as_array().expect("events array");
        assert_eq!(events.len(), 2);
        for key in [
            "id",
            "ts",
            "actor",
            "actionKind",
            "resource",
            "details",
            "runId",
        ] {
            assert!(
                events[0].get(key).is_some(),
                "expected {key} in {:?}",
                events[0]
            );
        }
        assert_eq!(events[0]["actor"], json!("user"));
        assert_eq!(events[1]["actor"], json!("agent-07"));
        assert_eq!(events[0]["details"]["tool"], json!("Read"));

        // No filter -> the whole log.
        let all = store.export_bundle(None).expect("export all");
        assert_eq!(all["eventCount"], json!(4));
        // Unknown run -> empty bundle, not an error.
        let none = store
            .export_bundle(Some("run-404"))
            .expect("export missing");
        assert_eq!(none["eventCount"], json!(0));
    }

    #[test]
    fn updates_and_deletes_are_blocked_by_triggers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("audit.sqlite3");
        let store = AuditStore::open(&db_path).expect("open");
        store
            .append(entry("agent-07", "src/lib.rs", None))
            .expect("append");

        // A rogue direct connection still cannot rewrite history.
        let rogue = Connection::open(&db_path).expect("rogue open");
        let update = rogue.execute("UPDATE audit_log SET actor = 'attacker'", []);
        assert!(update.is_err(), "UPDATE must be aborted by the trigger");
        let delete = rogue.execute("DELETE FROM audit_log", []);
        assert!(delete.is_err(), "DELETE must be aborted by the trigger");

        // And the row survived untouched.
        let bundle = store.export_bundle(None).expect("export");
        assert_eq!(bundle["eventCount"], json!(1));
        assert_eq!(bundle["events"][0]["actor"], json!("agent-07"));
    }

    #[test]
    fn record_helpers_produce_the_expected_kinds() {
        let (_dir, store) = store();
        let run = Some("run-9");

        store
            .record_permission_denial(
                "agent-07",
                "push",
                &PolicyDenial::GitActionNotPermitted {
                    action: GitAction::Push,
                },
                run,
            )
            .expect("record");

        store
            .record_secret_grant(
                "user",
                "deploy/token",
                "task-1",
                Utc::now() + Duration::seconds(60),
                run,
            )
            .expect("record");
        store
            .record_secret_revoke("user", "deploy/token", "task-1", run)
            .expect("record");

        let request = ApprovalRequest {
            id: "req-1".to_owned(),
            gate: Gate::GitPush,
            operation_fingerprint: "fnv1a64:0123456789abcdef".to_owned(),
            requested_by: "agent-07".to_owned(),
            expires_at: Utc::now() + Duration::seconds(600),
        };
        store
            .record_approval("user", &request, None, run)
            .expect("record");
        store
            .record_approval("user", &request, Some(ApprovalDecision::Approved), run)
            .expect("record");

        store
            .record_git_gate("agent-07", GitAction::Push, false, "not permitted", run)
            .expect("record");

        let bundle = store.export_bundle(None).expect("export");
        let events = bundle["events"].as_array().expect("events");
        let kinds: Vec<&str> = events
            .iter()
            .map(|event| event["actionKind"].as_str().expect("kind"))
            .collect();
        assert_eq!(
            kinds,
            vec![
                "permission.denied",
                "secret.granted",
                "secret.revoked",
                "approval.requested",
                "approval.resolved",
                "git.gate",
            ]
        );
        let rendered = bundle.to_string();
        assert!(rendered.contains("git_push"));
        assert!(rendered.contains("deploy/token"));
    }

    #[test]
    fn secret_grant_records_never_contain_values() {
        let (_dir, store) = store();
        let broker =
            crate::secrets::EphemeralBroker::new([("deploy/token", "tok_live_9f2cfe1a7b44")]);
        let lease = broker
            .grant("deploy/token", "task-1", Duration::seconds(300))
            .expect("grant");

        // The blessed path: record via scope; the raw value is not even a
        // parameter of the helper.
        store
            .record_secret_grant(
                "user",
                &lease.scope,
                &lease.task_id,
                lease.expires_at,
                Some("run-1"),
            )
            .expect("record");

        let bundle = store.export_bundle(Some("run-1")).expect("export");
        let rendered = bundle.to_string();
        assert!(rendered.contains("deploy/token"));
        assert!(
            !rendered.contains("tok_live"),
            "the value must never reach the audit log"
        );
    }

    #[test]
    fn non_object_details_are_rejected() {
        let (_dir, store) = store();
        let err = store
            .append(AuditEntry {
                actor: "agent".to_owned(),
                action_kind: "tool.used".to_owned(),
                resource: "x".to_owned(),
                details: json!(["not", "an", "object"]),
                run_id: None,
            })
            .unwrap_err();
        assert!(matches!(err, PolicyError::Invalid(_)));
    }
}
