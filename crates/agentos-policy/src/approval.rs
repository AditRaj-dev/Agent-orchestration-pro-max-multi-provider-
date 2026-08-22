//! Human approval gates (PRD §14 SEC-04): approvals are bound to an
//! **operation fingerprint** and an expiry; changing the operation
//! invalidates the approval.
//!
//! Fingerprint = FNV-1a 64-bit hash of the **canonical JSON** of the
//! operation. Dependency-free by design: FNV-1a is a change-detection hash,
//! not a security primitive — collisions are astronomically unlikely for the
//! small operation sets a local daemon sees, and a collision would require an
//! attacker-chosen operation *identical in canonical form* to an already
//! human-approved one within the approval ttl. When a keyed hash is needed
//! (`blake3` is already a workspace dependency elsewhere), swap
//! [`FNV_OFFSET`]/[`FNV_PRIME`] for the keyed variant — the stored
//! `fnv1a64:`-prefixed format is the only contract.
//!
//! Canonical JSON: object keys sorted recursively, compact separators, arrays
//! order-sensitive (reordering an argument list IS a different operation),
//! numbers by their serde_json representation (`1` ≠ `1.0` — strict change
//! detection). Expiry is enforced **at check time**, so a wall-clock jump
//! after resolve cannot resurrect a stale approval.

use std::path::Path;
use std::sync::Mutex;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{db, PolicyError};
use crate::store::{lock_guard, now_ts, open_db};

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// The gates that can require human authorization (SEC-04). Organization
/// policy may make some of these non-disablable; that flag lives in the
/// daemon config, not here — this type only names the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Gate {
    /// `git commit` on a task branch. Distinct from [`Gate::GitPush`] on
    /// purpose: an approval for one must never authorize the other, and a
    /// fingerprint alone would not separate them if they shared a gate.
    GitCommit,
    /// `git push` to any remote.
    GitPush,
    /// Installing/adding a dependency.
    PackageInstall,
    /// Touching production (deploys, migrations, prod data).
    ProdAction,
    /// Destructive shell (`rm -rf`, force-push flags, ...).
    DestructiveShell,
    /// Reading a secret via the broker (SEC-03).
    SecretAccess,
}

impl Gate {
    /// Canonical snake_case storage/wire string.
    pub fn as_str(self) -> &'static str {
        match self {
            Gate::GitCommit => "git_commit",
            Gate::GitPush => "git_push",
            Gate::PackageInstall => "package_install",
            Gate::ProdAction => "prod_action",
            Gate::DestructiveShell => "destructive_shell",
            Gate::SecretAccess => "secret_access",
        }
    }

    /// Parse the canonical storage string.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "git_commit" => Some(Gate::GitCommit),
            "git_push" => Some(Gate::GitPush),
            "package_install" => Some(Gate::PackageInstall),
            "prod_action" => Some(Gate::ProdAction),
            "destructive_shell" => Some(Gate::DestructiveShell),
            "secret_access" => Some(Gate::SecretAccess),
            _ => None,
        }
    }
}

impl std::fmt::Display for Gate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A human's resolution of an approval request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    /// Authorized — subject to fingerprint + expiry at check time.
    Approved,
    /// Refused.
    Denied,
}

impl ApprovalDecision {
    /// Canonical snake_case storage string.
    pub fn as_str(self) -> &'static str {
        match self {
            ApprovalDecision::Approved => "approved",
            ApprovalDecision::Denied => "denied",
        }
    }
}

/// Lifecycle status of a stored request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    /// Waiting for a human.
    Pending,
    /// Human approved; valid until `expires_at` for the exact fingerprint.
    Approved,
    /// Human denied (or superseded).
    Denied,
    /// A single-use approval that has already authorized its operation.
    /// Terminal: it never authorizes anything again.
    Consumed,
}

impl ApprovalStatus {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(ApprovalStatus::Pending),
            "approved" => Some(ApprovalStatus::Approved),
            "denied" => Some(ApprovalStatus::Denied),
            "consumed" => Some(ApprovalStatus::Consumed),
            _ => None,
        }
    }
}

/// A pending or resolved approval request (SEC-04 "approval event").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRequest {
    /// Queue-assigned id (UUID v7, time-ordered).
    pub id: String,
    /// The gate being requested.
    pub gate: Gate,
    /// FNV-1a fingerprint of the canonical JSON of the exact operation.
    pub operation_fingerprint: String,
    /// Who/what asked (agent id or `user`).
    pub requested_by: String,
    /// When the approval stops being honored (checked at read time).
    pub expires_at: DateTime<Utc>,
    /// Whether the approval is spent by its first use ([`ApprovalStore::consume`]).
    #[serde(default)]
    pub single_use: bool,
}

/// Compact canonical JSON: object keys sorted recursively, no whitespace.
/// Arrays keep their order (they are semantically ordered).
pub fn canonical_json(value: &serde_json::Value) -> String {
    use serde_json::Value;
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => serde_json::to_string(s).expect("string serialization is infallible"),
        Value::Array(items) => {
            let body: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", body.join(","))
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let body: Vec<String> = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("string serialization is infallible"),
                        canonical_json(&map[key])
                    )
                })
                .collect();
            format!("{{{}}}", body.join(","))
        }
    }
}

/// FNV-1a 64 over the byte stream.
fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME)
    })
}

/// The operation fingerprint: `fnv1a64:<16 hex chars>` over the canonical
/// JSON of the operation. Any change to the operation — key order aside —
/// produces a different fingerprint and therefore invalidates approvals
/// bound to the old one.
pub fn operation_fingerprint(operation: &serde_json::Value) -> String {
    format!(
        "fnv1a64:{:016x}",
        fnv1a64(canonical_json(operation).as_bytes())
    )
}

/// SQLite-backed approval store (F-01 canon open; see [`crate::store`]).
#[derive(Debug)]
pub struct ApprovalStore {
    conn: Mutex<Connection>,
}

impl ApprovalStore {
    /// Open (creating if needed) the approval database at `path` and run
    /// idempotent migrations.
    pub fn open(path: &Path) -> Result<Self, PolicyError> {
        let conn = open_db(path)?;
        conn.execute_batch(SCHEMA).map_err(db)?;
        for statement in ADDED_COLUMNS {
            match conn.execute(statement, []) {
                Ok(_) => {}
                // Already migrated: SQLite reports a duplicate column name.
                Err(err) if err.to_string().contains("duplicate column name") => {}
                Err(err) => return Err(db(err)),
            }
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Record a **reusable** request for `gate` covering exactly
    /// `operation`, valid for `ttl` after now. `ttl == 0` produces an
    /// instantly-expired request (useful for tests/audits of the expiry
    /// path). Reusable is right for a gate that governs a task's
    /// *progression*: a retry of the same task asks the same question.
    pub fn request(
        &self,
        gate: Gate,
        operation: &serde_json::Value,
        requested_by: &str,
        ttl: Duration,
    ) -> Result<ApprovalRequest, PolicyError> {
        self.request_scoped(gate, operation, requested_by, ttl, false)
    }

    /// Record a **single-use** request: once [`Self::consume`] cashes the
    /// approval in, it is spent and authorizes nothing further, even inside
    /// its ttl. This is the right scope for an irreversible side effect —
    /// a commit or a push — where "approved once" must not silently mean
    /// "approved for the next ten minutes of retries".
    pub fn request_once(
        &self,
        gate: Gate,
        operation: &serde_json::Value,
        requested_by: &str,
        ttl: Duration,
    ) -> Result<ApprovalRequest, PolicyError> {
        self.request_scoped(gate, operation, requested_by, ttl, true)
    }

    fn request_scoped(
        &self,
        gate: Gate,
        operation: &serde_json::Value,
        requested_by: &str,
        ttl: Duration,
        single_use: bool,
    ) -> Result<ApprovalRequest, PolicyError> {
        if ttl < Duration::zero() {
            return Err(PolicyError::Invalid("ttl must be non-negative".to_owned()));
        }
        let id = Uuid::now_v7().to_string();
        let fingerprint = operation_fingerprint(operation);
        let expires_at = Utc::now() + ttl;
        let expires_ts = expires_at.to_rfc3339_opts(SecondsFormat::Millis, true);
        let conn = lock_guard(&self.conn);
        conn.execute(
            "INSERT INTO approvals (id, gate, operation_fingerprint, requested_by, status, \
             created_at, expires_at, single_use) \
             VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?6, ?7)",
            params![
                id,
                gate.as_str(),
                fingerprint,
                requested_by,
                now_ts(),
                expires_ts,
                i64::from(single_use)
            ],
        )
        .map_err(db)?;
        Ok(ApprovalRequest {
            id,
            gate,
            operation_fingerprint: fingerprint,
            requested_by: requested_by.to_owned(),
            expires_at,
            single_use,
        })
    }

    /// Spend the live approval covering `gate` + `operation`, if it is
    /// single-use. Returns the id of the approval that was consumed, or
    /// `None` when the live approval is reusable (nothing to spend) or when
    /// there is no live approval at all.
    ///
    /// Call this **after** the authorized side effect actually happened —
    /// consuming first would burn a human's decision on an attempt that
    /// then failed. The `UPDATE ... WHERE status = 'approved'` is the
    /// atomic step: two racing consumers cannot both spend one approval.
    pub fn consume(
        &self,
        gate: Gate,
        operation: &serde_json::Value,
        consumed_by: &str,
    ) -> Result<Option<String>, PolicyError> {
        let fingerprint = operation_fingerprint(operation);
        let conn = lock_guard(&self.conn);
        let id: Option<String> = conn
            .query_row(
                "SELECT id FROM approvals \
                 WHERE gate = ?1 AND operation_fingerprint = ?2 AND status = 'approved' \
                   AND single_use = 1 \
                 ORDER BY expires_at DESC LIMIT 1",
                params![gate.as_str(), fingerprint],
                |row| row.get(0),
            )
            .optional()
            .map_err(db)?;
        let Some(id) = id else {
            return Ok(None);
        };
        let updated = conn
            .execute(
                "UPDATE approvals SET status = 'consumed', consumed_at = ?1, consumed_by = ?2 \
                 WHERE id = ?3 AND status = 'approved'",
                params![now_ts(), consumed_by, id],
            )
            .map_err(db)?;
        if updated == 1 {
            tracing::info!(approval = %id, gate = %gate, consumer = %consumed_by,
                "single-use approval consumed");
            Ok(Some(id))
        } else {
            // Another consumer won the race; the approval is already spent.
            Ok(None)
        }
    }

    /// Resolve a `pending` request. Returns whether a row transitioned
    /// (`false` = unknown id or already resolved).
    pub fn resolve(&self, id: &str, decision: ApprovalDecision) -> Result<bool, PolicyError> {
        let conn = lock_guard(&self.conn);
        let updated = conn
            .execute(
                "UPDATE approvals SET status = ?1, resolved_at = ?2 \
                 WHERE id = ?3 AND status = 'pending'",
                params![decision.as_str(), now_ts(), id],
            )
            .map_err(db)?;
        Ok(updated == 1)
    }

    /// Whether `gate` + `operation` carries a live approval: an `approved`
    /// row exists whose fingerprint matches **exactly** and whose expiry is
    /// still in the future. A mutated operation yields a different
    /// fingerprint and therefore no matching row; expiry is re-checked on
    /// every call.
    pub fn is_approved(
        &self,
        gate: Gate,
        operation: &serde_json::Value,
    ) -> Result<bool, PolicyError> {
        let fingerprint = operation_fingerprint(operation);
        let conn = lock_guard(&self.conn);
        let expires_ts: Option<String> = conn
            .query_row(
                "SELECT expires_at FROM approvals \
                 WHERE gate = ?1 AND operation_fingerprint = ?2 AND status = 'approved' \
                 ORDER BY expires_at DESC LIMIT 1",
                params![gate.as_str(), fingerprint],
                |row| row.get(0),
            )
            .optional()
            .map_err(db)?;
        let Some(expires_ts) = expires_ts else {
            return Ok(false);
        };
        let expires_at = DateTime::parse_from_rfc3339(&expires_ts)
            .map_err(|err| {
                PolicyError::Invalid(format!("corrupt expires_at `{expires_ts}`: {err}"))
            })?
            .with_timezone(&Utc);
        Ok(Utc::now() < expires_at)
    }

    /// Lifecycle status of a request by id.
    pub fn status(&self, id: &str) -> Result<Option<ApprovalStatus>, PolicyError> {
        let conn = lock_guard(&self.conn);
        let status: Option<String> = conn
            .query_row(
                "SELECT status FROM approvals WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()
            .map_err(db)?;
        Ok(status.and_then(|s| ApprovalStatus::parse(&s)))
    }
}

/// Schema (idempotent): one `approvals` table. Requests are immutable once
/// resolved — re-approving a changed operation is a NEW request with a NEW
/// fingerprint; history is never rewritten.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS approvals (\
    id TEXT PRIMARY KEY,\
    gate TEXT NOT NULL,\
    operation_fingerprint TEXT NOT NULL,\
    requested_by TEXT NOT NULL,\
    status TEXT NOT NULL DEFAULT 'pending',\
    created_at TEXT NOT NULL,\
    expires_at TEXT NOT NULL,\
    resolved_at TEXT,\
    single_use INTEGER NOT NULL DEFAULT 0,\
    consumed_at TEXT,\
    consumed_by TEXT\
);\
CREATE INDEX IF NOT EXISTS idx_approvals_gate_fp_status \
    ON approvals(gate, operation_fingerprint, status);\
";

/// Columns added after the first release. SQLite has no
/// `ADD COLUMN IF NOT EXISTS`, so each is attempted on open and a
/// "duplicate column name" error means the migration already ran.
const ADDED_COLUMNS: [&str; 3] = [
    "ALTER TABLE approvals ADD COLUMN single_use INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE approvals ADD COLUMN consumed_at TEXT",
    "ALTER TABLE approvals ADD COLUMN consumed_by TEXT",
];

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_single_use_approval_authorizes_exactly_once() {
        let (_dir, store) = store();
        let op = json!({"kind": "git", "action": "commit", "taskId": "t-1"});
        let request = store
            .request_once(Gate::GitCommit, &op, "supervisor", Duration::seconds(600))
            .expect("request");
        assert!(request.single_use);
        store
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve");
        assert!(store.is_approved(Gate::GitCommit, &op).unwrap());

        // Cash it in after the side effect: it is spent, inside its ttl.
        assert_eq!(
            store.consume(Gate::GitCommit, &op, "supervisor").unwrap(),
            Some(request.id.clone())
        );
        assert!(
            !store.is_approved(Gate::GitCommit, &op).unwrap(),
            "a consumed approval authorizes nothing further"
        );
        assert_eq!(
            store.status(&request.id).unwrap(),
            Some(ApprovalStatus::Consumed)
        );
        // A second consumer gets nothing — one decision, one use.
        assert_eq!(store.consume(Gate::GitCommit, &op, "other").unwrap(), None);
    }

    #[test]
    fn a_reusable_approval_is_never_spent_by_consume() {
        let (_dir, store) = store();
        let op = json!({"kind": "human", "node": "gate"});
        let request = store
            .request(Gate::ProdAction, &op, "supervisor", Duration::seconds(600))
            .expect("request");
        assert!(!request.single_use);
        store
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve");
        assert_eq!(
            store.consume(Gate::ProdAction, &op, "supervisor").unwrap(),
            None
        );
        assert!(
            store.is_approved(Gate::ProdAction, &op).unwrap(),
            "a retry of the same node asks the same question"
        );
    }

    #[test]
    fn commit_and_push_approvals_never_transfer_between_gates() {
        let (_dir, store) = store();
        let op = json!({"kind": "git", "taskId": "t-1"});
        let request = store
            .request_once(Gate::GitCommit, &op, "supervisor", Duration::seconds(600))
            .expect("request");
        store
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve");
        assert!(store.is_approved(Gate::GitCommit, &op).unwrap());
        assert!(
            !store.is_approved(Gate::GitPush, &op).unwrap(),
            "an approved commit is not an approved push"
        );
        assert_eq!(
            store.consume(Gate::GitPush, &op, "supervisor").unwrap(),
            None
        );
    }

    #[test]
    fn opening_a_pre_migration_database_adds_the_consumption_columns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("legacy.sqlite3");
        {
            // The pre-consumption schema, as shipped in the first release.
            let conn = crate::store::open_db(&path).expect("open");
            conn.execute_batch(
                "CREATE TABLE approvals (id TEXT PRIMARY KEY, gate TEXT NOT NULL,                  operation_fingerprint TEXT NOT NULL, requested_by TEXT NOT NULL,                  status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL,                  expires_at TEXT NOT NULL, resolved_at TEXT);",
            )
            .expect("legacy schema");
        }
        let store = ApprovalStore::open(&path).expect("migrate");
        let op = json!({"kind": "git"});
        let request = store
            .request_once(Gate::GitCommit, &op, "supervisor", Duration::seconds(60))
            .expect("request on the migrated database");
        store
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve");
        assert_eq!(
            store.consume(Gate::GitCommit, &op, "supervisor").unwrap(),
            Some(request.id)
        );
        // Idempotent: opening again must not fail on the added columns.
        ApprovalStore::open(&path).expect("reopen");
    }

    fn store() -> (tempfile::TempDir, ApprovalStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ApprovalStore::open(&dir.path().join("approvals.sqlite3")).expect("open");
        (dir, store)
    }

    #[test]
    fn gates_and_decisions_round_trip_wire_strings() {
        for gate in [
            Gate::GitPush,
            Gate::PackageInstall,
            Gate::ProdAction,
            Gate::DestructiveShell,
            Gate::SecretAccess,
        ] {
            assert_eq!(Gate::parse(gate.as_str()), Some(gate));
        }
        assert_eq!(Gate::parse("reboot"), None);
        assert_eq!(ApprovalDecision::Approved.as_str(), "approved");
    }

    #[test]
    fn canonical_json_sorts_keys_recursively_and_stays_compact() {
        assert_eq!(canonical_json(&json!({"b": 1, "a": 2})), r#"{"a":2,"b":1}"#);
        assert_eq!(
            canonical_json(&json!({"z": {"d": 1, "c": [2, 1]}})),
            r#"{"z":{"c":[2,1],"d":1}}"#,
            "arrays keep their order (they are semantically ordered)"
        );
        assert_eq!(canonical_json(&json!("x\"y")), r#""x\"y""#);
        assert_eq!(canonical_json(&json!(null)), "null");
    }

    #[test]
    fn fingerprint_is_key_order_independent_and_change_sensitive() {
        let a = json!({"action": "push", "ref": "main", "commits": ["a", "b"]});
        let b = json!({"ref": "main", "commits": ["a", "b"], "action": "push"});
        assert_eq!(
            operation_fingerprint(&a),
            operation_fingerprint(&b),
            "key order must not matter"
        );

        let mutated_value = json!({"action": "push", "ref": "develop", "commits": ["a", "b"]});
        assert_ne!(
            operation_fingerprint(&a),
            operation_fingerprint(&mutated_value)
        );

        let mutated_array = json!({"action": "push", "ref": "main", "commits": ["b", "a"]});
        assert_ne!(
            operation_fingerprint(&a),
            operation_fingerprint(&mutated_array),
            "array order is part of the operation"
        );

        let added_key =
            json!({"action": "push", "ref": "main", "commits": ["a", "b"], "force": true});
        assert_ne!(operation_fingerprint(&a), operation_fingerprint(&added_key));

        assert!(operation_fingerprint(&a).starts_with("fnv1a64:"));
        assert_eq!(operation_fingerprint(&a).len(), "fnv1a64:".len() + 16);
    }

    #[test]
    fn request_resolve_check_round_trip() {
        let (_dir, store) = store();
        let op = json!({"action": "push", "remote": "origin", "ref": "main"});
        let request = store
            .request(Gate::GitPush, &op, "agent-07", Duration::seconds(600))
            .expect("request");

        assert_eq!(
            store.status(&request.id).unwrap(),
            Some(ApprovalStatus::Pending)
        );
        assert!(
            !store.is_approved(Gate::GitPush, &op).unwrap(),
            "pending is not approved"
        );

        assert!(store
            .resolve(&request.id, ApprovalDecision::Approved)
            .unwrap());
        assert!(
            !store
                .resolve(&request.id, ApprovalDecision::Denied)
                .unwrap(),
            "terminal rows stay put"
        );
        assert_eq!(
            store.status(&request.id).unwrap(),
            Some(ApprovalStatus::Approved)
        );
        assert!(store.is_approved(Gate::GitPush, &op).unwrap());
    }

    #[test]
    fn mutated_operation_invalidates_approval() {
        let (_dir, store) = store();
        let approved_op = json!({"action": "push", "ref": "main", "commit": "9f2cfe1"});
        let request = store
            .request(
                Gate::GitPush,
                &approved_op,
                "git-manager",
                Duration::seconds(600),
            )
            .expect("request");
        store
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve");

        // Same gate, one field changed -> different fingerprint -> NOT approved.
        let mutated = json!({"action": "push", "ref": "main", "commit": "0ff1ce0"});
        assert!(!store.is_approved(Gate::GitPush, &mutated).unwrap());

        // Added field -> different fingerprint -> NOT approved.
        let amended = json!({"action": "push", "ref": "main", "commit": "9f2cfe1", "force": true});
        assert!(!store.is_approved(Gate::GitPush, &amended).unwrap());

        // Original still approved.
        assert!(store.is_approved(Gate::GitPush, &approved_op).unwrap());
    }

    #[test]
    fn gate_mismatch_is_not_approved() {
        let (_dir, store) = store();
        let op = json!({"package": "anyhow", "version": "1"});
        let request = store
            .request(
                Gate::PackageInstall,
                &op,
                "worker-1",
                Duration::seconds(600),
            )
            .expect("request");
        store
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve");

        // Identical operation under a DIFFERENT gate carries no approval.
        assert!(!store.is_approved(Gate::ProdAction, &op).unwrap());
        assert!(store.is_approved(Gate::PackageInstall, &op).unwrap());
    }

    #[test]
    fn expired_approval_is_denied_at_check_time() {
        let (_dir, store) = store();
        let op = json!({"action": "push", "ref": "main"});
        // ttl 0 -> expires immediately.
        let request = store
            .request(Gate::GitPush, &op, "worker-1", Duration::zero())
            .expect("request");
        store
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve");
        assert!(
            !store.is_approved(Gate::GitPush, &op).unwrap(),
            "expiry enforced at check time"
        );
    }

    #[test]
    fn denied_and_unknown_are_never_approved() {
        let (_dir, store) = store();
        let op = json!({"action": "push", "ref": "main"});
        let request = store
            .request(Gate::GitPush, &op, "worker-1", Duration::seconds(600))
            .expect("request");
        store
            .resolve(&request.id, ApprovalDecision::Denied)
            .expect("resolve");
        assert!(!store.is_approved(Gate::GitPush, &op).unwrap());

        let never_requested = json!({"action": "push", "ref": "release"});
        assert!(!store.is_approved(Gate::GitPush, &never_requested).unwrap());
    }

    #[test]
    fn negative_ttl_is_rejected() {
        let (_dir, store) = store();
        let err = store
            .request(Gate::GitPush, &json!({}), "x", Duration::seconds(-1))
            .unwrap_err();
        assert!(matches!(err, PolicyError::Invalid(_)));
    }

    #[test]
    fn real_sqlite_busy_surfaces_as_retryable_core_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("approvals.sqlite3");
        let store = ApprovalStore::open(&db_path).expect("open");

        // Hold an IMMEDIATE (write) transaction open on a second connection
        // to the same file.
        let blocker = Connection::open(&db_path).expect("blocker open");
        blocker
            .busy_timeout(std::time::Duration::from_secs(1))
            .expect("blocker timeout");
        blocker
            .execute_batch("BEGIN IMMEDIATE;")
            .expect("begin immediate");

        let result = store.request(Gate::GitPush, &json!({}), "worker", Duration::seconds(60));
        let err = result.expect_err("request must hit SQLITE_BUSY while the write lock is held");
        assert!(
            matches!(err, PolicyError::Core(agentos_core::CoreError::SqliteBusy)),
            "busy must surface as retryable CoreError::SqliteBusy, got: {err}"
        );
        drop(blocker);
        // After the lock is released the same call succeeds — busy is retryable.
        store
            .request(Gate::GitPush, &json!({}), "worker", Duration::seconds(60))
            .expect("request succeeds after lock release");
    }
}
