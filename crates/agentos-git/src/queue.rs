//! Serialized git mutation queue (PRD §12 GIT-02).
//!
//! One ordered queue per repository with a single mutating consumer:
//!
//! - `enqueue` appends a request (push actions are approval-gated in code,
//!   GIT-01);
//! - `claim_next(repo, owner, ttl)` grants the oldest `Pending` item an
//!   exclusive lease — but only when no live lease exists for that repo;
//!   while a lease is held, `claim_next` returns `None` even if other items
//!   are pending;
//! - an expired lease is reclaimed automatically (dead-consumer recovery):
//!   the stale item returns to `Pending` and the next claim wins it;
//! - `stale_base_check` runs before execution: if the integration branch
//!   head moved off the recorded base commit — fast-forwarded past it or
//!   diverged from it — the item is marked `Rejected` with reason
//!   `stale_base` and callers route it to the rebase/review path instead of
//!   executing it.

use std::path::Path;
use std::sync::Mutex;

use agentos_core::CoreError;
use chrono::Duration;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::cli;
use crate::error::{db, GitError};
use crate::store::{self, lock_guard, now_ts};

/// Rejection reason recorded when a base commit is no longer an ancestor of
/// the integration branch head.
pub const STALE_BASE: &str = "stale_base";

/// The mutation an item asks the git manager to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationAction {
    /// Commit staged work on the task branch.
    Commit,
    /// Merge a task branch into the integration branch.
    Merge,
    /// Rebase a task branch onto a new base.
    Rebase,
    /// Push to a remote. Approval-gated by default (GIT-01).
    Push,
}

impl MutationAction {
    /// Canonical snake_case storage/wire string.
    pub fn as_str(self) -> &'static str {
        match self {
            MutationAction::Commit => "commit",
            MutationAction::Merge => "merge",
            MutationAction::Rebase => "rebase",
            MutationAction::Push => "push",
        }
    }

    /// Parse the canonical storage string.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "commit" => Some(MutationAction::Commit),
            "merge" => Some(MutationAction::Merge),
            "rebase" => Some(MutationAction::Rebase),
            "push" => Some(MutationAction::Push),
            _ => None,
        }
    }
}

impl std::fmt::Display for MutationAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lifecycle status of a queued request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestStatus {
    /// Waiting for the single mutating consumer.
    Pending,
    /// Leased to a consumer; lease expires at `lease_expires_at`.
    InProgress,
    /// Executed successfully.
    Done,
    /// Refused (stale base, unapproved push, manual rejection).
    Rejected,
    /// Attempted but conflicted; awaiting reconciliation.
    Conflict,
}

impl RequestStatus {
    /// Canonical snake_case storage/wire string.
    pub fn as_str(self) -> &'static str {
        match self {
            RequestStatus::Pending => "pending",
            RequestStatus::InProgress => "in_progress",
            RequestStatus::Done => "done",
            RequestStatus::Rejected => "rejected",
            RequestStatus::Conflict => "conflict",
        }
    }

    /// Parse the canonical storage string.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(RequestStatus::Pending),
            "in_progress" => Some(RequestStatus::InProgress),
            "done" => Some(RequestStatus::Done),
            "rejected" => Some(RequestStatus::Rejected),
            "conflict" => Some(RequestStatus::Conflict),
            _ => None,
        }
    }
}

impl std::fmt::Display for RequestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A queued git mutation request (row of `git_requests`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRequest {
    /// Queue-assigned id (UUID v7, time-ordered).
    pub id: String,
    /// Repository path as passed to `enqueue` — callers must consistently
    /// use one canonical absolute form per repo.
    pub repo_path: String,
    /// Task that produced the request.
    pub task_id: String,
    /// Integration-branch commit the request was planned against.
    pub base_commit: String,
    /// Requested mutation.
    pub action: MutationAction,
    /// Current lifecycle status.
    pub status: RequestStatus,
    /// Current lease owner, when `InProgress`.
    pub lease_owner: Option<String>,
    /// Lease expiry (RFC 3339 UTC), when `InProgress`.
    pub lease_expires_at: Option<String>,
    /// Whether approval was presented at enqueue time (push gating).
    pub approved: bool,
    /// Creation time (RFC 3339 UTC).
    pub created_at: String,
    /// Last update time (RFC 3339 UTC).
    pub updated_at: String,
    /// Resulting commit sha on success.
    pub result_sha: Option<String>,
    /// Rejection/failure reason.
    pub error: Option<String>,
}

/// Raw row shape, mirroring column order in [`SCHEMA`].
struct RawRequest {
    id: String,
    repo_path: String,
    task_id: String,
    base_commit: String,
    action: String,
    status: String,
    lease_owner: Option<String>,
    lease_expires_at: Option<String>,
    approved: i64,
    created_at: String,
    updated_at: String,
    result_sha: Option<String>,
    error: Option<String>,
}

impl RawRequest {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(RawRequest {
            id: row.get(0)?,
            repo_path: row.get(1)?,
            task_id: row.get(2)?,
            base_commit: row.get(3)?,
            action: row.get(4)?,
            status: row.get(5)?,
            lease_owner: row.get(6)?,
            lease_expires_at: row.get(7)?,
            approved: row.get(8)?,
            created_at: row.get(9)?,
            updated_at: row.get(10)?,
            result_sha: row.get(11)?,
            error: row.get(12)?,
        })
    }

    fn into_typed(self) -> Result<GitRequest, GitError> {
        let action = MutationAction::parse(&self.action)
            .ok_or_else(|| GitError::Invalid(format!("unknown action `{}`", self.action)))?;
        let status = RequestStatus::parse(&self.status)
            .ok_or_else(|| GitError::Invalid(format!("unknown status `{}`", self.status)))?;
        Ok(GitRequest {
            id: self.id,
            repo_path: self.repo_path,
            task_id: self.task_id,
            base_commit: self.base_commit,
            action,
            status,
            lease_owner: self.lease_owner,
            lease_expires_at: self.lease_expires_at,
            approved: self.approved != 0,
            created_at: self.created_at,
            updated_at: self.updated_at,
            result_sha: self.result_sha,
            error: self.error,
        })
    }
}

const SELECT_COLUMNS: &str = "id, repo_path, task_id, base_commit, action, status, \
     lease_owner, lease_expires_at, approved, created_at, updated_at, result_sha, error";

/// Per-repo ordered queue of git mutations, persisted in its own SQLite file.
#[derive(Debug)]
pub struct MutationQueue {
    conn: Mutex<Connection>,
}

impl MutationQueue {
    /// Open (creating if needed) the queue database at `path` and run
    /// idempotent migrations.
    pub fn open(path: &Path) -> Result<Self, GitError> {
        let conn = store::open_db(path)?;
        conn.execute_batch(SCHEMA).map_err(db)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Append a request for `repo_path`.
    ///
    /// Push actions carry the `approved` flag: a `Push` with
    /// `approved == false` is rejected in code unless the
    /// `allow_unapproved_push` override was explicitly set via
    /// [`MutationQueue::set_allow_unapproved_push`] (GIT-01:
    /// approval-gated by default — never prompt-based).
    pub fn enqueue(
        &self,
        repo_path: &Path,
        task_id: &str,
        base_commit: &str,
        action: MutationAction,
        approved: bool,
    ) -> Result<String, GitError> {
        if action == MutationAction::Push && !approved && !self.allow_unapproved_push()? {
            return Err(GitError::PushNotApproved);
        }
        let id = Uuid::now_v7().to_string();
        let now = now_ts();
        let conn = lock_guard(&self.conn);
        conn.execute(
            "INSERT INTO git_requests \
             (id, repo_path, task_id, base_commit, action, status, approved, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6, ?7, ?7)",
            params![
                id,
                repo_path.to_string_lossy(),
                task_id,
                base_commit,
                action.as_str(),
                approved as i64,
                now
            ],
        )
        .map_err(db)?;
        Ok(id)
    }

    /// Read the persisted `allow_unapproved_push` override (default: off).
    pub fn allow_unapproved_push(&self) -> Result<bool, GitError> {
        let conn = lock_guard(&self.conn);
        let value: Option<String> = conn
            .query_row(
                "SELECT value FROM queue_config WHERE key = 'allow_unapproved_push'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(db)?;
        Ok(value.as_deref() == Some("1"))
    }

    /// Explicitly set the `allow_unapproved_push` override (persisted).
    pub fn set_allow_unapproved_push(&self, allowed: bool) -> Result<(), GitError> {
        let conn = lock_guard(&self.conn);
        conn.execute(
            "INSERT INTO queue_config (key, value) VALUES ('allow_unapproved_push', ?1) \
             ON CONFLICT(key) DO UPDATE SET value = ?1",
            params![if allowed { "1" } else { "0" }],
        )
        .map_err(db)?;
        Ok(())
    }

    /// Claim the next item of `repo_path` for `owner` with a lease of `ttl`.
    ///
    /// Strict single-consumer ordering (GIT-02), executed atomically in one
    /// `IMMEDIATE` transaction:
    ///
    /// 1. expired `in_progress` leases return to `pending` (dead-consumer
    ///    recovery);
    /// 2. if any live lease remains for the repo, return `None` — even when
    ///    other items are pending;
    /// 3. otherwise lease the oldest `pending` item (insertion order) to
    ///    `owner` and return it as `in_progress`.
    pub fn claim_next(
        &self,
        repo_path: &Path,
        owner: &str,
        ttl: Duration,
    ) -> Result<Option<GitRequest>, GitError> {
        if ttl < Duration::zero() {
            return Err(GitError::Invalid("ttl must be non-negative".to_string()));
        }
        let repo = repo_path.to_string_lossy().into_owned();
        let mut conn = lock_guard(&self.conn);
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db)?;

        // 1. expired leases go back to pending
        tx.execute(
            "UPDATE git_requests \
             SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL, updated_at = ?1 \
             WHERE status = 'in_progress' AND lease_expires_at <= ?1",
            params![now_ts()],
        )
        .map_err(db)?;

        // 2. a live lease blocks the whole repo queue
        let live_leases: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM git_requests WHERE repo_path = ?1 AND status = 'in_progress'",
                params![repo],
                |row| row.get(0),
            )
            .map_err(db)?;
        if live_leases > 0 {
            tx.commit().map_err(db)?;
            return Ok(None);
        }

        // 3. oldest pending item (FIFO by insertion order)
        let oldest: Option<String> = tx
            .query_row(
                "SELECT id FROM git_requests WHERE repo_path = ?1 AND status = 'pending' \
                 ORDER BY rowid ASC LIMIT 1",
                params![repo],
                |row| row.get(0),
            )
            .optional()
            .map_err(db)?;
        let Some(id) = oldest else {
            tx.commit().map_err(db)?;
            return Ok(None);
        };

        let expires_at =
            (chrono::Utc::now() + ttl).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        tx.execute(
            "UPDATE git_requests \
             SET status = 'in_progress', lease_owner = ?1, lease_expires_at = ?2, updated_at = ?3 \
             WHERE id = ?4 AND status = 'pending'",
            params![owner, expires_at, now_ts(), id],
        )
        .map_err(db)?;

        let claimed = tx
            .query_row(
                &format!("SELECT {SELECT_COLUMNS} FROM git_requests WHERE id = ?1"),
                params![id],
                RawRequest::from_row,
            )
            .optional()
            .map_err(db)?
            .map(RawRequest::into_typed)
            .transpose()?;
        tx.commit().map_err(db)?;
        Ok(claimed)
    }

    /// Mark a `pending`/`in_progress` item executed, recording `sha` when
    /// the mutation produced a commit. Returns whether a row transitioned.
    pub fn complete(&self, id: &str, sha: Option<&str>) -> Result<bool, GitError> {
        let conn = lock_guard(&self.conn);
        let updated = conn
            .execute(
                "UPDATE git_requests \
                 SET status = 'done', result_sha = ?1, lease_owner = NULL, \
                     lease_expires_at = NULL, updated_at = ?2 \
                 WHERE id = ?3 AND status IN ('pending', 'in_progress')",
                params![sha, now_ts(), id],
            )
            .map_err(db)?;
        Ok(updated == 1)
    }

    /// Mark a `pending`/`in_progress` item rejected with `reason`.
    /// Returns whether a row transitioned.
    pub fn reject(&self, id: &str, reason: &str) -> Result<bool, GitError> {
        let conn = lock_guard(&self.conn);
        let updated = conn
            .execute(
                "UPDATE git_requests \
                 SET status = 'rejected', error = ?1, lease_owner = NULL, \
                     lease_expires_at = NULL, updated_at = ?2 \
                 WHERE id = ?3 AND status IN ('pending', 'in_progress')",
                params![reason, now_ts(), id],
            )
            .map_err(db)?;
        Ok(updated == 1)
    }

    /// Fetch a request by id.
    pub fn get(&self, id: &str) -> Result<Option<GitRequest>, GitError> {
        let conn = lock_guard(&self.conn);
        let raw = conn
            .query_row(
                &format!("SELECT {SELECT_COLUMNS} FROM git_requests WHERE id = ?1"),
                params![id],
                RawRequest::from_row,
            )
            .optional()
            .map_err(db)?;
        raw.map(RawRequest::into_typed).transpose()
    }

    /// Base-commit staleness gate (GIT-02), run before executing an item.
    ///
    /// `integration_head` is the current head of the integration branch. An
    /// item is fresh only while that head is **exactly** the recorded
    /// `base_commit`: any commit that landed on the integration branch after
    /// enqueue invalidates the item's assumptions, even a fast-forward that
    /// keeps `base_commit` an ancestor. Stale items are marked `Rejected`
    /// with reason [`STALE_BASE`] and `Ok(false)` is returned — callers must
    /// route the work to the rebase/review path. The merge-base of the two
    /// commits is computed for routing: `base_commit` still an ancestor
    /// means a plain rebase suffices; a diverged base needs review.
    pub fn stale_base_check(&self, id: &str, integration_head: &str) -> Result<bool, GitError> {
        let request = self
            .get(id)?
            .ok_or_else(|| GitError::Core(CoreError::NotFound(format!("git request {id}"))))?;
        let fresh = integration_head.eq_ignore_ascii_case(&request.base_commit);
        if fresh
            || !matches!(
                request.status,
                RequestStatus::Pending | RequestStatus::InProgress
            )
        {
            return Ok(fresh);
        }
        let merge_base = cli::merge_base(
            Path::new(&request.repo_path),
            &request.base_commit,
            integration_head,
        );
        match merge_base {
            Ok(Some(base)) if base.eq_ignore_ascii_case(&request.base_commit) => {
                tracing::debug!(
                    request = %id,
                    "stale base: integration head fast-forwarded past base; rebase path"
                );
            }
            Ok(_) => {
                tracing::debug!(
                    request = %id,
                    "stale base: integration history diverged from base; review path"
                );
            }
            Err(error) => {
                tracing::debug!(%error, request = %id, "merge-base classification failed")
            }
        }
        let conn = lock_guard(&self.conn);
        conn.execute(
            "UPDATE git_requests \
             SET status = 'rejected', error = ?1, lease_owner = NULL, \
                 lease_expires_at = NULL, updated_at = ?2 \
             WHERE id = ?3",
            params![STALE_BASE, now_ts(), id],
        )
        .map_err(db)?;
        Ok(false)
    }
}

/// Schema (idempotent): one `git_requests` table plus the `queue_config`
/// key-value store holding the push-approval override.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS git_requests (\
    id TEXT PRIMARY KEY,\
    repo_path TEXT NOT NULL,\
    task_id TEXT NOT NULL,\
    base_commit TEXT NOT NULL,\
    action TEXT NOT NULL,\
    status TEXT NOT NULL,\
    lease_owner TEXT,\
    lease_expires_at TEXT,\
    approved INTEGER NOT NULL DEFAULT 0,\
    created_at TEXT NOT NULL,\
    updated_at TEXT NOT NULL,\
    result_sha TEXT,\
    error TEXT\
);\
CREATE INDEX IF NOT EXISTS idx_git_requests_repo_status \
    ON git_requests(repo_path, status);\
CREATE TABLE IF NOT EXISTS queue_config (\
    key TEXT PRIMARY KEY,\
    value TEXT NOT NULL\
);\
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actions_round_trip_through_storage_strings() {
        for action in [
            MutationAction::Commit,
            MutationAction::Merge,
            MutationAction::Rebase,
            MutationAction::Push,
        ] {
            assert_eq!(MutationAction::parse(action.as_str()), Some(action));
        }
        assert_eq!(MutationAction::parse("cherry-pick"), None);
    }

    #[test]
    fn statuses_round_trip_through_storage_strings() {
        for status in [
            RequestStatus::Pending,
            RequestStatus::InProgress,
            RequestStatus::Done,
            RequestStatus::Rejected,
            RequestStatus::Conflict,
        ] {
            assert_eq!(RequestStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(RequestStatus::parse("paused"), None);
    }
}
