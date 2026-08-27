//! Memex CLI bridge used by Mastermind.
//!
//! Memex is intentionally provider-neutral: Claude, Codex, Gemini and the
//! daemon all talk to the same CLI contract and therefore the same WAL
//! SQLite database. AgentOS does not duplicate or migrate Memex's schema.

use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};
use serde_json::Value;

const MEMEX_CLI_ENV: &str = "AGENTOS_MEMEX_CLI";

#[derive(Debug, thiserror::Error)]
pub enum MemexError {
    #[error("could not start Memex CLI {program:?}: {source}")]
    Spawn {
        program: OsString,
        source: std::io::Error,
    },
    #[error("Memex CLI failed ({status}): {detail}")]
    Failed { status: i32, detail: String },
    #[error("Memex returned invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryRecord {
    pub id: i64,
    pub project: String,
    pub agent: String,
    pub provider: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub content: String,
    pub tags: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandoffRecord {
    pub id: i64,
    pub project: String,
    pub task_id: String,
    pub from_agent: String,
    pub to_agent: String,
    pub status: String,
    pub summary: String,
    pub artifacts: String,
    pub blockers: String,
    pub created_at: String,
    pub accepted_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoardSnapshot {
    pub project: String,
    #[serde(default)]
    pub tasks: Vec<Value>,
    #[serde(default)]
    pub open_handoffs: Vec<HandoffRecord>,
    #[serde(default)]
    pub memory_count: i64,
}

/// Thin, synchronous wrapper. Calls are short local SQLite operations and
/// are made only at Mastermind boundaries, never in the workflow tick loop.
#[derive(Debug, Clone)]
pub struct MemexClient {
    program: OsString,
    db_path: Option<PathBuf>,
}

impl Default for MemexClient {
    fn default() -> Self {
        Self::from_env()
    }
}

impl MemexClient {
    pub fn from_env() -> Self {
        Self {
            program: std::env::var_os(MEMEX_CLI_ENV).unwrap_or_else(resolve_memex_program),
            db_path: std::env::var_os("MEMEX_DB").map(PathBuf::from),
        }
    }

    pub(crate) fn with_db(mut self, path: impl Into<PathBuf>) -> Self {
        self.db_path = Some(path.into());
        self
    }

    #[cfg(test)]
    pub(crate) fn with_program(mut self, program: impl Into<OsString>) -> Self {
        self.program = program.into();
        self
    }

    pub fn project_key(repo: &Path) -> String {
        let resolved = std::fs::canonicalize(repo).unwrap_or_else(|_| repo.to_path_buf());
        let mut normalized = resolved.to_string_lossy().replace('\\', "/");
        if cfg!(windows) {
            if let Some(path) = normalized.strip_prefix("//?/UNC/") {
                normalized = format!("//{path}");
            } else if let Some(path) = normalized.strip_prefix("//?/") {
                normalized = path.to_owned();
            }
            format!("agentos:{}", normalized.to_lowercase())
        } else {
            format!("agentos:{normalized}")
        }
    }

    pub fn remember(
        &self,
        project: &str,
        content: &str,
        agent: &str,
        provider: &str,
        kind: &str,
        tags: &str,
    ) -> Result<i64, MemexError> {
        // Content is piped, never passed as an argument: a stored document can
        // exceed the OS command-line cap (8191 chars through cmd.exe on
        // Windows), which failed every Phase 5 approval with "The command line
        // is too long." The `-` positional tells the CLI to read stdin.
        let value =
            self.run_with_stdin(remember_args(project, agent, provider, kind, tags), content)?;
        Ok(value.get("id").and_then(Value::as_i64).unwrap_or_default())
    }

    pub fn search(
        &self,
        project: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryRecord>, MemexError> {
        let value = self.run([
            "search",
            query,
            "--project",
            project,
            "--limit",
            &limit.to_string(),
            "--json",
        ])?;
        Ok(serde_json::from_value(value)?)
    }

    pub fn board(&self, project: &str) -> Result<BoardSnapshot, MemexError> {
        let value = self.run(["board", "--project", project, "--json"])?;
        Ok(serde_json::from_value(value)?)
    }

    pub fn handoffs(
        &self,
        project: &str,
        status: Option<&str>,
    ) -> Result<Vec<HandoffRecord>, MemexError> {
        let mut args = vec![
            OsString::from("handoff"),
            OsString::from("list"),
            OsString::from("--project"),
            OsString::from(project),
        ];
        if let Some(status) = status {
            args.push(OsString::from("--status"));
            args.push(OsString::from(status));
        }
        args.push(OsString::from("--json"));
        let value = self.run_os(args)?;
        Ok(serde_json::from_value(value)?)
    }

    pub fn task_set(
        &self,
        project: &str,
        task_id: &str,
        title: &str,
        domain: &str,
        status: &str,
        assignee: &str,
    ) -> Result<Value, MemexError> {
        self.run([
            "task",
            "set",
            task_id,
            "--project",
            project,
            "--title",
            title,
            "--domain",
            domain,
            "--status",
            status,
            "--assignee",
            assignee,
            "--json",
        ])
    }

    // This boundary intentionally mirrors the Memex CLI's handoff record
    // fields one-for-one; wrapping them would only move the same contract
    // into a duplicate AgentOS type.
    #[allow(clippy::too_many_arguments)]
    pub fn handoff_create(
        &self,
        project: &str,
        from: &str,
        to: &str,
        task_id: &str,
        summary: &str,
        artifacts: &str,
        blockers: &str,
    ) -> Result<i64, MemexError> {
        let value = self.run([
            "handoff",
            "create",
            "--project",
            project,
            "--from",
            from,
            "--to",
            to,
            "--task",
            task_id,
            "--summary",
            summary,
            "--artifacts",
            artifacts,
            "--blockers",
            blockers,
            "--json",
        ])?;
        Ok(value.get("id").and_then(Value::as_i64).unwrap_or_default())
    }

    fn run<'a, I>(&self, args: I) -> Result<Value, MemexError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.run_os(args.into_iter().map(OsString::from).collect())
    }

    fn run_with_stdin<'a, I>(&self, args: I, stdin: &str) -> Result<Value, MemexError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.run_os_with_stdin(args.into_iter().map(OsString::from).collect(), stdin)
    }

    fn run_os_with_stdin(
        &self,
        args: Vec<OsString>,
        stdin_content: &str,
    ) -> Result<Value, MemexError> {
        let mut command = command_for(&self.program, &args);
        if let Some(path) = &self.db_path {
            command.env("MEMEX_DB", path);
        }
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|source| MemexError::Spawn {
            program: self.program.clone(),
            source,
        })?;
        // Write and drop the handle before waiting: the child reads stdin to
        // EOF, so holding the pipe open deadlocks both processes.
        {
            let mut handle = child.stdin.take().ok_or_else(|| MemexError::Failed {
                status: -1,
                detail: "could not open a stdin pipe to the Memex CLI".to_owned(),
            })?;
            handle
                .write_all(stdin_content.as_bytes())
                .map_err(|source| MemexError::Spawn {
                    program: self.program.clone(),
                    source,
                })?;
        }
        let output = child
            .wait_with_output()
            .map_err(|source| MemexError::Spawn {
                program: self.program.clone(),
                source,
            })?;
        if !output.status.success() {
            return Err(MemexError::Failed {
                status: output.status.code().unwrap_or(-1),
                detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    fn run_os(&self, args: Vec<OsString>) -> Result<Value, MemexError> {
        let mut command = command_for(&self.program, &args);
        if let Some(path) = &self.db_path {
            command.env("MEMEX_DB", path);
        }
        let output = command.output().map_err(|source| MemexError::Spawn {
            program: self.program.clone(),
            source,
        })?;
        if !output.status.success() {
            return Err(MemexError::Failed {
                status: output.status.code().unwrap_or(-1),
                detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        Ok(serde_json::from_slice(&output.stdout)?)
    }
}

fn resolve_memex_program() -> OsString {
    #[cfg(windows)]
    {
        if let Ok(output) = Command::new("where.exe").arg("memex").output() {
            if output.status.success() {
                if let Some(first) = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .map(str::trim)
                    .find(|line| !line.is_empty())
                {
                    return OsString::from(first);
                }
            }
        }
        // The Memex installer uses this location even when a GUI-launched
        // process inherited an older PATH. Desktop apps must still honor the
        // installed default without requiring a shell restart.
        if let Some(profile) = std::env::var_os("USERPROFILE") {
            let installed = PathBuf::from(profile).join("bin").join("memex.cmd");
            if installed.is_file() {
                return installed.into_os_string();
            }
        }
    }
    OsString::from("memex")
}

/// Argument vector for `remember`. The content is deliberately absent — it is
/// piped to stdin behind the `-` positional — so a stored document can never
/// push this past the OS command-line cap.
fn remember_args<'a>(
    project: &'a str,
    agent: &'a str,
    provider: &'a str,
    kind: &'a str,
    tags: &'a str,
) -> [&'a str; 13] {
    [
        "remember",
        "-",
        "--project",
        project,
        "--agent",
        agent,
        "--provider",
        provider,
        "--type",
        kind,
        "--tags",
        tags,
        "--json",
    ]
}

fn command_for(program: &OsStr, args: &[OsString]) -> Command {
    #[cfg(windows)]
    {
        let is_script = Path::new(program)
            .extension()
            .and_then(OsStr::to_str)
            .is_some_and(|ext| ext.eq_ignore_ascii_case("cmd") || ext.eq_ignore_ascii_case("bat"));
        if is_script {
            let mut command = Command::new("cmd.exe");
            command.args(["/D", "/S", "/C"]);
            command.arg(program);
            command.args(args);
            return command;
        }
    }
    let mut command = Command::new(program);
    command.args(args);
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Phase 5 approval regression: a whole API record was passed as an
    /// argument and cmd.exe rejected the 8191-char command line. Content must
    /// travel on stdin, so it must never appear in the argument vector.
    #[test]
    fn remember_never_puts_content_in_the_argument_vector() {
        let args = remember_args(
            "agentos:proj",
            "orchestrator",
            "claude",
            "api",
            "mastermind",
        );
        assert_eq!(args[0], "remember");
        assert_eq!(args[1], "-", "content must be read from stdin, not argv");
        assert!(args.contains(&"--json"), "callers parse JSON: {args:?}");

        // A document far past the cmd.exe cap changes nothing about the args.
        let document = "x".repeat(64 * 1024);
        assert!(
            !args.iter().any(|arg| arg.contains(&document[..64])),
            "content leaked into argv: {args:?}"
        );
        let budget: usize = args.iter().map(|arg| arg.len() + 1).sum();
        assert!(
            budget < 8191,
            "argv must stay under the cmd.exe cap: {budget}"
        );
    }

    #[test]
    fn project_key_is_stable_and_namespaced() {
        let key = MemexClient::project_key(Path::new("D:/Work/My App"));
        assert!(key.starts_with("agentos:"));
        assert!(key.contains("My App") || key.contains("my app"));
        assert!(!key.contains('\\'));
        let existing = tempfile::tempdir().expect("existing directory");
        let existing_key = MemexClient::project_key(existing.path());
        assert!(!existing_key.contains("//?/"));
    }

    #[test]
    fn installed_memex_round_trip_uses_an_isolated_database() {
        let probe = MemexClient::from_env();
        let available = command_for(&probe.program, &[OsString::from("--help")])
            .output()
            .is_ok();
        if !available {
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let client = probe.with_db(dir.path().join("memex.db"));
        let project = "agentos:test-mastermind-memory";
        let id = client
            .remember(
                project,
                "approved color system is cobalt",
                "orchestrator",
                "codex",
                "decision",
                "mastermind,session:test,phase:6",
            )
            .expect("remember");
        assert!(id > 0);
        let found = client.search(project, "cobalt", 10).expect("search");
        assert_eq!(found[0].id, id);
        let board = client.board(project).expect("board");
        assert_eq!(board.memory_count, 1);
        client
            .task_set(
                project,
                "session:test:task-1",
                "Build one thing",
                "rust",
                "in_progress",
                "rust-specialist",
            )
            .expect("task set");
        let handoff_id = client
            .handoff_create(
                project,
                "rust-specialist",
                "code-reviewer",
                "session:test:task-1",
                "AgentOS packet reference is ready",
                "agentos-handoff:packet-1",
                "",
            )
            .expect("handoff create");
        assert!(handoff_id > 0);
        let board = client.board(project).expect("updated board");
        assert_eq!(board.tasks.len(), 1);
        assert_eq!(board.open_handoffs.len(), 1);
        assert_eq!(
            client
                .handoffs(project, Some("open"))
                .expect("handoff list")[0]
                .id,
            handoff_id
        );
    }
}
