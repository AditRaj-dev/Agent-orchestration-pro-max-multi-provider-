//! Worktree isolation (PRD §12 GIT-04).
//!
//! One worktree per leased write task, created at a caller-supplied base
//! commit under `<repo>/.agentos-worktrees/<task_id>`, on a branch whose name
//! is derived exclusively from run/task UUIDs — no model-controlled text can
//! ever reach a ref name. The `.agentos-worktrees/` directory is hidden via
//! `.git/info/exclude` (best effort): local ignore metadata, never a
//! `.gitignore` edit in the user's repository without opt-in.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use uuid::Uuid;

use crate::cli::{self, WorktreeEntry};
use crate::error::GitError;

/// Directory (relative to the repository root) holding all managed worktrees.
pub const WORKTREE_DIR: &str = ".agentos-worktrees";

/// Line added to `.git/info/exclude` when creatable.
const EXCLUDE_LINE: &str = ".agentos-worktrees/";

/// A worktree created by [`WorktreeManager::create`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeRef {
    /// Repository the worktree belongs to.
    pub repo: PathBuf,
    /// Absolute path of the worktree checkout.
    pub path: PathBuf,
    /// Generated branch name (`agentos/<run_id_short>/<task_id_short>`).
    pub branch: String,
    /// Full task id the worktree was created for.
    pub task_id: String,
    /// Commit the branch was created at.
    pub base_commit: String,
}

/// Retention rules honored by [`WorktreeManager::gc_eligible`]. Garbage
/// collection may only run after retention is satisfied (GIT-04).
#[derive(Debug, Clone)]
pub struct RetentionRules {
    /// Minimum age of the worktree's last modification before GC.
    pub min_age: chrono::Duration,
    /// When set, the worktree `HEAD` must be an ancestor of this commit-ish
    /// (i.e. its work is already merged) before GC.
    pub require_merged_into: Option<String>,
}

impl Default for RetentionRules {
    fn default() -> Self {
        Self {
            min_age: chrono::Duration::zero(),
            require_merged_into: None,
        }
    }
}

/// Manages isolated worktrees for one repository.
#[derive(Debug, Clone)]
pub struct WorktreeManager {
    repo: PathBuf,
}

impl WorktreeManager {
    /// Manager for the repository at `repo` (the main checkout, not a
    /// linked worktree).
    pub fn new(repo: impl Into<PathBuf>) -> Self {
        Self { repo: repo.into() }
    }

    /// Root directory holding this repo's managed worktrees.
    pub fn managed_root(&self) -> PathBuf {
        self.repo.join(WORKTREE_DIR)
    }

    /// Deterministic branch name: exactly `agentos/<run_id_short>/<task_id_short>`
    /// where both shorts are the last 8 hex characters of the parsed UUIDs
    /// (the random leg of a UUIDv7 — heads collide within a mint burst).
    ///
    /// Both ids must parse as UUIDs — this structurally guarantees that no
    /// model-generated free text can enter a ref name (GIT-04).
    pub fn branch_name(run_id: &str, task_id: &str) -> Result<String, GitError> {
        let run_short = short_hex(run_id, "run_id")?;
        let task_short = short_hex(task_id, "task_id")?;
        Ok(format!("agentos/{run_short}/{task_short}"))
    }

    /// Create an isolated worktree for `task_id` at `base_commit`.
    ///
    /// Fails if the branch or worktree path already exists. The worktree
    /// lands under `<repo>/.agentos-worktrees/<task_id>` and checks out the
    /// generated branch at `base_commit`.
    pub fn create(
        &self,
        run_id: &str,
        task_id: &str,
        base_commit: &str,
    ) -> Result<WorktreeRef, GitError> {
        let branch = Self::branch_name(run_id, task_id)?;
        let path = self.managed_root().join(task_id);
        ensure_excluded(&self.repo);
        cli::worktree_add(&self.repo, &path, &branch, base_commit)?;
        Ok(WorktreeRef {
            repo: self.repo.clone(),
            path,
            branch,
            task_id: task_id.to_string(),
            base_commit: base_commit.to_string(),
        })
    }

    /// List every worktree of the repository (main checkout included).
    pub fn list(&self) -> Result<Vec<WorktreeEntry>, GitError> {
        Ok(cli::worktree_list(&self.repo)?)
    }

    /// Remove a worktree, falling back to `--force` when a plain remove
    /// fails (e.g. untracked or modified files). The branch and its commits
    /// are intentionally left in place — they are protected by retention
    /// rules (GIT-04) and reclaimed by the GC path, not by removal.
    pub fn remove(&self, path: &Path) -> Result<(), GitError> {
        match cli::worktree_remove(&self.repo, path, false) {
            Ok(()) => Ok(()),
            Err(first) => match cli::worktree_remove(&self.repo, path, true) {
                Ok(()) => {
                    tracing::debug!(error = %first, ?path, "worktree remove required --force");
                    Ok(())
                }
                Err(second) => Err(GitError::Cli(second)),
            },
        }
    }

    /// Whether `entry` may be garbage-collected under `rules`.
    ///
    /// F-09 stub honoring the retention hook: a worktree is eligible only if
    /// it lives under the managed root, is at least `min_age` old (by
    /// filesystem modification time), and — when `require_merged_into` is
    /// set — its `HEAD` is already an ancestor of that commit-ish. Entries
    /// whose paths no longer exist are never eligible.
    pub fn gc_eligible(
        &self,
        entry: &WorktreeEntry,
        rules: &RetentionRules,
    ) -> Result<bool, GitError> {
        let Ok(root) = self.managed_root().canonicalize() else {
            return Ok(false);
        };
        let Ok(path) = entry.path.canonicalize() else {
            return Ok(false);
        };
        if !path.starts_with(&root) {
            return Ok(false);
        }
        let modified = fs::metadata(&path)?.modified()?;
        let age = SystemTime::now()
            .duration_since(modified)
            .unwrap_or_default();
        if age < rules.min_age.to_std().unwrap_or_default() {
            return Ok(false);
        }
        if let Some(target) = &rules.require_merged_into {
            let base = cli::merge_base(&self.repo, &entry.head, target)?;
            return Ok(base.as_deref() == Some(entry.head.as_str()));
        }
        Ok(true)
    }
}

/// Last 8 hex characters of a UUID string, rejecting anything that is not a
/// UUID (branch names must never carry model-controlled text).
///
/// The TAIL, not the head: a run's tasks are minted as UUIDv7s in one burst,
/// so their leading timestamp bits collide and head-derived branch names
/// clash in `git worktree add -b`. The tail is the random leg.
fn short_hex(id: &str, field: &str) -> Result<String, GitError> {
    let parsed = Uuid::parse_str(id)
        .map_err(|e| GitError::Invalid(format!("{field} `{id}` is not a UUID: {e}")))?;
    let hex = parsed.simple().to_string();
    hex.get(hex.len() - 8..)
        .map(str::to_string)
        .ok_or_else(|| GitError::Invalid(format!("{field} `{id}` yielded truncated hex")))
}

/// Append the managed-worktree ignore line to `.git/info/exclude`, creating
/// the file and its parent directory when possible. Best effort: a missing
/// or unusual `.git` layout (e.g. a linked worktree's `.git` file) only
/// logs — it must not fail worktree creation.
fn ensure_excluded(repo: &Path) {
    let exclude = repo.join(".git").join("info").join("exclude");
    let attempt = || -> std::io::Result<()> {
        if let Some(parent) = exclude.parent() {
            fs::create_dir_all(parent)?;
        }
        let existing = fs::read_to_string(&exclude).unwrap_or_default();
        if !existing.lines().any(|line| line.trim() == EXCLUDE_LINE) {
            use std::io::Write;
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&exclude)?;
            writeln!(file, "{EXCLUDE_LINE}")?;
        }
        Ok(())
    };
    if let Err(error) = attempt() {
        tracing::debug!(%error, ?exclude, "could not update .git/info/exclude");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_names_are_uuid_derived_and_fixed_width() {
        let run_id = "0a1b2c3d-1111-2222-3333-444455556666";
        let task_id = "f00dcafe-7777-8888-9999-aaaabbbbcccc";
        let name = WorktreeManager::branch_name(run_id, task_id).expect("branch name");
        assert_eq!(name, "agentos/55556666/bbbbcccc");
        // three slash-free segments: prefix + two 8-char hex shorts
        let segments: Vec<&str> = name.split('/').collect();
        assert_eq!(segments.len(), 3);
        assert!(segments[1].len() == 8 && segments[1].bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(segments[2].len() == 8 && segments[2].bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn uuidv7_ids_minted_in_one_burst_get_distinct_branches() {
        // Same millisecond timestamp, different random tails: the exact
        // shape `create_run` mints, which head-derived names collided on.
        let run = "01931f2a-1c00-7000-8000-000000000000";
        let a = "01931f2a-1c00-7abc-8000-0000deadbeef";
        let b = "01931f2a-1c00-7abd-8000-0000feedface";
        assert_ne!(
            WorktreeManager::branch_name(run, a).expect("a"),
            WorktreeManager::branch_name(run, b).expect("b")
        );
    }

    #[test]
    fn non_uuid_ids_are_rejected() {
        assert!(WorktreeManager::branch_name(
            "main-please",
            "f00dcafe-7777-8888-9999-aaaabbbbcccc"
        )
        .is_err());
        assert!(
            WorktreeManager::branch_name("0a1b2c3d-1111-2222-3333-444455556666", "../../evil")
                .is_err()
        );
    }
}
