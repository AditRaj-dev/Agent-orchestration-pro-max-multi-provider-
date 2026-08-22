//! Agent ledger (PRD §12 GIT-03): commit → originating task/agent/
//! orchestrator/reviewers/context versions/workflow.
//!
//! This is harness-DB metadata only — it never touches commit messages
//! (GIT-03: no machine metadata forced into human commit subjects/bodies).
//! Repository-portable provenance (git notes / custom refs) is a deliberate
//! non-goal for F-09.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::{db, GitError};
use crate::store::{self, lock_guard, now_ts};

/// Attribution entry supplied by callers when a commit lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    /// Commit sha being attributed.
    pub commit_sha: String,
    /// Task that produced the change.
    pub task_id: String,
    /// Agent instance that authored the change (e.g. `claude-code/opus-5#s1`).
    pub agent_instance: String,
    /// Orchestrator identity that supervised the run.
    pub orchestrator: String,
    /// Reviewers that signed off on the change.
    pub reviewers: Vec<String>,
    /// Context bundle versions the agent operated against
    /// (name → version/sha).
    pub context_versions: BTreeMap<String, String>,
    /// Workflow that framed the task.
    pub workflow_id: String,
}

/// A persisted ledger row: the entry plus its recording timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerRecord {
    /// The attributed entry.
    pub entry: LedgerEntry,
    /// When the ledger row was written (RFC 3339 UTC).
    pub recorded_at: String,
}

/// Commit attribution store backed by its own SQLite file (or any file —
/// tables coexist peacefully with [`crate::queue::MutationQueue`]).
#[derive(Debug)]
pub struct AgentLedger {
    conn: Mutex<Connection>,
}

impl AgentLedger {
    /// Open (creating if needed) the ledger database at `path`.
    pub fn open(path: &Path) -> Result<Self, GitError> {
        let conn = store::open_db(path)?;
        conn.execute_batch(SCHEMA).map_err(db)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Record attribution for a commit. Recording the same sha twice is an
    /// idempotent upsert (last write wins), so crash-retry loops cannot
    /// poison the ledger.
    pub fn record(&self, entry: &LedgerEntry) -> Result<LedgerRecord, GitError> {
        let reviewers = serde_json::to_string(&entry.reviewers)?;
        let context_versions = serde_json::to_string(&entry.context_versions)?;
        let recorded_at = now_ts();
        let conn = lock_guard(&self.conn);
        conn.execute(
            "INSERT INTO ledger \
             (commit_sha, task_id, agent_instance, orchestrator, reviewers, \
              context_versions, workflow_id, recorded_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT(commit_sha) DO UPDATE SET \
                 task_id = ?2, agent_instance = ?3, orchestrator = ?4, \
                 reviewers = ?5, context_versions = ?6, workflow_id = ?7, \
                 recorded_at = ?8",
            params![
                entry.commit_sha,
                entry.task_id,
                entry.agent_instance,
                entry.orchestrator,
                reviewers,
                context_versions,
                entry.workflow_id,
                recorded_at
            ],
        )
        .map_err(db)?;
        Ok(LedgerRecord {
            entry: entry.clone(),
            recorded_at,
        })
    }

    /// Look up the attribution recorded for a commit sha.
    pub fn by_commit(&self, commit_sha: &str) -> Result<Option<LedgerRecord>, GitError> {
        let conn = lock_guard(&self.conn);
        let raw = conn
            .query_row(
                "SELECT commit_sha, task_id, agent_instance, orchestrator, reviewers, \
                        context_versions, workflow_id, recorded_at \
                 FROM ledger WHERE commit_sha = ?1",
                params![commit_sha],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .map_err(db)?;
        raw.map(
            |(
                commit_sha,
                task_id,
                agent_instance,
                orchestrator,
                reviewers,
                context_versions,
                workflow_id,
                recorded_at,
            )| {
                Ok(LedgerRecord {
                    entry: LedgerEntry {
                        commit_sha,
                        task_id,
                        agent_instance,
                        orchestrator,
                        reviewers: serde_json::from_str(&reviewers)?,
                        context_versions: serde_json::from_str(&context_versions)?,
                        workflow_id,
                    },
                    recorded_at,
                })
            },
        )
        .transpose()
    }

    /// All ledger rows attributed to a task, oldest first.
    pub fn by_task(&self, task_id: &str) -> Result<Vec<LedgerRecord>, GitError> {
        let conn = lock_guard(&self.conn);
        let mut stmt = conn
            .prepare(
                "SELECT commit_sha, task_id, agent_instance, orchestrator, reviewers, \
                        context_versions, workflow_id, recorded_at \
                 FROM ledger WHERE task_id = ?1 \
                 ORDER BY recorded_at ASC, rowid ASC",
            )
            .map_err(db)?;
        let raw = stmt
            .query_map(params![task_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            })
            .map_err(db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db)?;
        raw.into_iter()
            .map(
                |(
                    commit_sha,
                    task_id,
                    agent_instance,
                    orchestrator,
                    reviewers,
                    context_versions,
                    workflow_id,
                    recorded_at,
                )| {
                    Ok(LedgerRecord {
                        entry: LedgerEntry {
                            commit_sha,
                            task_id,
                            agent_instance,
                            orchestrator,
                            reviewers: serde_json::from_str(&reviewers)?,
                            context_versions: serde_json::from_str(&context_versions)?,
                            workflow_id,
                        },
                        recorded_at,
                    })
                },
            )
            .collect()
    }
}

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS ledger (\
    commit_sha TEXT PRIMARY KEY,\
    task_id TEXT NOT NULL,\
    agent_instance TEXT NOT NULL,\
    orchestrator TEXT NOT NULL,\
    reviewers TEXT NOT NULL,\
    context_versions TEXT NOT NULL,\
    workflow_id TEXT NOT NULL,\
    recorded_at TEXT NOT NULL\
);\
CREATE INDEX IF NOT EXISTS idx_ledger_task_id ON ledger(task_id);\
";

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(commit_sha: &str) -> LedgerEntry {
        let mut context_versions = BTreeMap::new();
        context_versions.insert("prompts".to_string(), "v3".to_string());
        context_versions.insert("repo_snapshot".to_string(), "sha-abc".to_string());
        LedgerEntry {
            commit_sha: commit_sha.to_string(),
            task_id: "task-1".to_string(),
            agent_instance: "claude-code/opus-5#s1".to_string(),
            orchestrator: "mastermind".to_string(),
            reviewers: vec!["sonnet-review-1".to_string()],
            context_versions,
            workflow_id: "wf-42".to_string(),
        }
    }

    #[test]
    fn record_and_query_both_directions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = AgentLedger::open(&dir.path().join("ledger.sqlite3")).expect("open");

        let first = ledger.record(&sample_entry("aa11")).expect("record");
        let second = ledger
            .record(&sample_entry_with_sha("bb22"))
            .expect("record");
        assert!(!first.recorded_at.is_empty());
        assert!(!second.recorded_at.is_empty());

        let by_commit = ledger.by_commit("aa11").expect("query").expect("present");
        assert_eq!(by_commit, first);

        let by_task = ledger.by_task("task-1").expect("query");
        assert_eq!(by_task.len(), 2);
        assert_eq!(by_task[0].entry.commit_sha, "aa11");
        assert_eq!(by_task[1].entry.commit_sha, "bb22");

        assert!(ledger.by_commit("nosuchsha").expect("query").is_none());
        assert!(ledger.by_task("nope").expect("query").is_empty());
    }

    fn sample_entry_with_sha(commit_sha: &str) -> LedgerEntry {
        let mut entry = sample_entry(commit_sha);
        entry.reviewers = vec!["sonnet-review-2".to_string()];
        entry
    }

    #[test]
    fn recording_the_same_sha_is_an_idempotent_upsert() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ledger = AgentLedger::open(&dir.path().join("ledger.sqlite3")).expect("open");
        ledger.record(&sample_entry("aa11")).expect("record");
        let mut updated = sample_entry("aa11");
        updated.reviewers = vec!["human-reviewer".to_string()];
        ledger.record(&updated).expect("upsert");

        let record = ledger.by_commit("aa11").expect("query").expect("present");
        assert_eq!(record.entry.reviewers, vec!["human-reviewer".to_string()]);
        assert_eq!(ledger.by_task("task-1").expect("query").len(), 1);
    }
}
