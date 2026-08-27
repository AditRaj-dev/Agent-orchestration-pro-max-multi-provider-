//! Thin `git` CLI wrapper (F-00 §1: gix/libgit2 + CLI fallback — for the MVP
//! the `git` CLI via `std::process`, no new dependencies).
//!
//! Every argument is passed as an [`OsString`] on the argv vector — there is
//! no shell anywhere in this path, so Windows paths with spaces or special
//! characters are never subject to string quoting (Windows is the reference
//! platform, F-00 §5).

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use thiserror::Error;

/// A failed `git` invocation: the lossy-rendered argv (diagnostics only),
/// the process exit code (`None` when `git` could not be spawned at all),
/// and the captured stderr.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("git {args:?} failed (exit code {exit_code:?}): {stderr}")]
pub struct GitCliError {
    /// The argv passed to `git` (excluding the `git` binary itself),
    /// lossily decoded for diagnostics.
    pub args: Vec<String>,
    /// Exit code of the `git` process; `None` if the process never ran.
    pub exit_code: Option<i32>,
    /// Captured stderr, right-trimmed.
    pub stderr: String,
}

/// Raw output of a completed `git` process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitOutput {
    /// Captured stdout (lossily decoded, not trimmed).
    pub stdout: String,
    /// Captured stderr (lossily decoded, not trimmed).
    pub stderr: String,
    /// Process exit code.
    pub code: i32,
}

/// One entry of `git worktree list --porcelain`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeEntry {
    /// Absolute path of the worktree checkout.
    pub path: PathBuf,
    /// `HEAD` commit of the worktree (empty if unknown).
    pub head: String,
    /// Branch short name, or `None` when detached.
    pub branch: Option<String>,
    /// Whether this entry is a bare repository.
    pub bare: bool,
}

fn str_args(items: &[&str]) -> Vec<OsString> {
    items.iter().map(OsString::from).collect()
}

fn display_args(args: &[OsString]) -> Vec<String> {
    args.iter()
        .map(|a| a.to_string_lossy().into_owned())
        .collect()
}

fn failure(args: &[OsString], code: Option<i32>, stderr: String) -> GitCliError {
    GitCliError {
        args: display_args(args),
        exit_code: code,
        stderr: stderr.trim_end().to_string(),
    }
}

/// Run `git` and capture stdout/stderr/exit code regardless of the exit
/// status. Only spawn failures (git missing, cwd missing) produce errors.
fn capture(repo: Option<&Path>, args: &[OsString]) -> Result<GitOutput, GitCliError> {
    let mut cmd = Command::new("git");
    if let Some(dir) = repo {
        cmd.current_dir(dir);
    }
    cmd.args(args);
    let out = cmd
        .output()
        .map_err(|e| failure(args, None, e.to_string()))?;
    Ok(GitOutput {
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        code: out.status.code().unwrap_or(-1),
    })
}

/// Run `git` and fail with a typed [`GitCliError`] on any non-zero exit.
///
/// Prefer the named helpers below over raw `run`; it exists for operations
/// not yet wrapped.
pub fn run(repo: Option<&Path>, args: &[OsString]) -> Result<GitOutput, GitCliError> {
    let out = capture(repo, args)?;
    if out.code != 0 {
        return Err(failure(args, Some(out.code), out.stderr));
    }
    Ok(out)
}

fn trimmed(out: &GitOutput) -> String {
    out.stdout.trim().to_string()
}

/// `git init <path>`.
pub fn init(path: &Path) -> Result<(), GitCliError> {
    let mut args = str_args(&["init"]);
    args.push(path.as_os_str().to_os_string());
    run(None, &args).map(|_| ())
}

/// `git rev-parse HEAD` in `repo`.
pub fn rev_parse_head(repo: &Path) -> Result<String, GitCliError> {
    run(Some(repo), &str_args(&["rev-parse", "HEAD"])).map(|out| trimmed(&out))
}

/// Current branch short name of `repo` (`git rev-parse --abbrev-ref HEAD`).
pub fn current_branch(repo: &Path) -> Result<String, GitCliError> {
    run(
        Some(repo),
        &str_args(&["rev-parse", "--abbrev-ref", "HEAD"]),
    )
    .map(|out| trimmed(&out))
}

/// Stage everything (`git add --all`) and commit with an explicit identity
/// (`-c user.name=<author_name> -c user.email=<author_name>@agentos.invalid`),
/// so brand-new repositories without user config still work. Returns the new
/// `HEAD` sha.
pub fn add_all_and_commit(
    repo: &Path,
    author_name: &str,
    message: &str,
) -> Result<String, GitCliError> {
    run(Some(repo), &str_args(&["add", "--all"]))?;
    let user = format!("user.name={author_name}");
    let email = format!("user.email={author_name}@agentos.invalid");
    let commit_args: Vec<OsString> = vec![
        "-c".into(),
        user.into(),
        "-c".into(),
        email.into(),
        "commit".into(),
        "-m".into(),
        message.into(),
    ];
    run(Some(repo), &commit_args)?;
    rev_parse_head(repo)
}

/// `git worktree add -b <branch> <path> <base_commit>`.
pub fn worktree_add(
    repo: &Path,
    path: &Path,
    branch: &str,
    base_commit: &str,
) -> Result<(), GitCliError> {
    let mut args = str_args(&["worktree", "add", "-b", branch]);
    args.push(path.as_os_str().to_os_string());
    args.push(base_commit.into());
    run(Some(repo), &args).map(|_| ())
}

/// `git worktree remove [--force] <path>`.
pub fn worktree_remove(repo: &Path, path: &Path, force: bool) -> Result<(), GitCliError> {
    let mut args = str_args(&["worktree", "remove"]);
    if force {
        args.push("--force".into());
    }
    args.push(path.as_os_str().to_os_string());
    run(Some(repo), &args).map(|_| ())
}

/// Parse `git worktree list --porcelain` into entries.
pub fn worktree_list(repo: &Path) -> Result<Vec<WorktreeEntry>, GitCliError> {
    let out = run(Some(repo), &str_args(&["worktree", "list", "--porcelain"]))?;
    let mut entries = Vec::new();
    let mut current: Option<WorktreeEntry> = None;
    for line in out.stdout.lines() {
        if line.is_empty() {
            if let Some(entry) = current.take() {
                entries.push(entry);
            }
            continue;
        }
        if let Some(path) = line.strip_prefix("worktree ") {
            if let Some(entry) = current.take() {
                entries.push(entry);
            }
            current = Some(WorktreeEntry {
                path: PathBuf::from(path),
                head: String::new(),
                branch: None,
                bare: false,
            });
        } else if let Some(entry) = current.as_mut() {
            if let Some(head) = line.strip_prefix("HEAD ") {
                entry.head = head.to_string();
            } else if let Some(branch) = line.strip_prefix("branch ") {
                entry.branch = Some(
                    branch
                        .strip_prefix("refs/heads/")
                        .unwrap_or(branch)
                        .to_string(),
                );
            } else if line == "bare" {
                entry.bare = true;
            }
        }
    }
    if let Some(entry) = current.take() {
        entries.push(entry);
    }
    Ok(entries)
}

/// Whether a local branch exists (`git show-ref --verify --quiet`).
pub fn branch_exists(repo: &Path, branch: &str) -> Result<bool, GitCliError> {
    let refname = format!("refs/heads/{branch}");
    let out = capture(
        Some(repo),
        &str_args(&["show-ref", "--verify", "--quiet", &refname]),
    )?;
    match out.code {
        0 => Ok(true),
        1 => Ok(false),
        code => Err(failure(
            &str_args(&["show-ref", "--verify", "--quiet", &refname]),
            Some(code),
            out.stderr,
        )),
    }
}

/// `git merge-base a b`. `Ok(None)` means the two commits have no common
/// ancestor (git exit code 1).
pub fn merge_base(repo: &Path, a: &str, b: &str) -> Result<Option<String>, GitCliError> {
    let args = str_args(&["merge-base", a, b]);
    let out = capture(Some(repo), &args)?;
    match out.code {
        0 => Ok(Some(trimmed(&out))),
        1 => Ok(None),
        code => Err(failure(&args, Some(code), out.stderr)),
    }
}

/// `git diff --name-only a b` as a list of changed paths.
pub fn diff_name_only(repo: &Path, a: &str, b: &str) -> Result<Vec<String>, GitCliError> {
    let out = run(Some(repo), &str_args(&["diff", "--name-only", a, b]))?;
    Ok(out
        .stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

/// `git status --porcelain` reduced to repo-relative paths, for describing a
/// commit before it is made. Rename entries (`old -> new`) report the new
/// path; quoted paths keep git's quoting, which is fine for prose.
pub fn status_paths(repo: &Path) -> Result<Vec<String>, GitCliError> {
    // `-uall` expands untracked *directories* into their files. Without it
    // git collapses a new directory to a single `dir/` entry, so a task that
    // created `components/Timer.tsx` reports only `components/` — no
    // extension, and therefore invisible to any per-file classification.
    let out = run(Some(repo), &str_args(&["status", "--porcelain", "-uall"]))?;
    Ok(out
        .stdout
        .lines()
        .filter_map(|line| line.get(3..))
        .map(|path| match path.split_once(" -> ") {
            Some((_, new)) => new.trim(),
            None => path.trim(),
        })
        .filter(|path| !path.is_empty())
        .map(str::to_string)
        .collect())
}

/// `git rebase --onto <onto> <upstream>`: replay this branch's own commits
/// onto `onto`. Both arguments must already be resolved object names — the
/// caller validates them with [`rev_parse_verify`] first, so nothing that
/// is not a real commit in this repository ever reaches argv.
///
/// On failure the rebase is aborted before the error is returned, so the
/// worktree is never left mid-rebase for the next lease to trip over.
pub fn rebase_onto(repo: &Path, onto: &str, upstream: &str) -> Result<(), GitCliError> {
    let args = str_args(&["rebase", "--onto", onto, upstream]);
    let out = capture(Some(repo), &args)?;
    if out.code == 0 {
        return Ok(());
    }
    let _ = run(Some(repo), &str_args(&["rebase", "--abort"]));
    Err(failure(&args, Some(out.code), out.stderr))
}

/// `git rev-parse --verify <rev>^{commit}` — resolves to a full sha, or
/// `None` when the revision does not name a commit in this repository.
pub fn rev_parse_verify(repo: &Path, rev: &str) -> Result<Option<String>, GitCliError> {
    let spec = format!("{rev}^{{commit}}");
    let args = vec![
        OsString::from("rev-parse"),
        OsString::from("--verify"),
        OsString::from("--quiet"),
        OsString::from(spec),
    ];
    let out = capture(Some(repo), &args)?;
    match out.code {
        0 => Ok(Some(trimmed(&out))),
        1 => Ok(None),
        code => Err(failure(&args, Some(code), out.stderr)),
    }
}
