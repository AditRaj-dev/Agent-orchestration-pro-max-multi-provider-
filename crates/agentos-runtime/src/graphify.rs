//! Code-graph refresh after a worker node writes.
//!
//! Review agents run under the orchestrator tool denylist (no `Bash`, no
//! `PowerShell`, no `REPL`) and agy additionally runs `--sandbox`, so a
//! reviewer **cannot build a code graph** — it can only read one that
//! already exists. Left to the workers, the graph appeared only when a
//! worker happened to run `graphify update` itself, which is prompt text in
//! `SKILL_CODE_GRAPH_DISCIPLINE` and therefore unenforced. This module moves
//! that step into the harness: after a worker finishes, the graph is
//! refreshed so whoever reads next finds it current.
//!
//! What `graphify update <path>` costs: it is tree-sitter AST extraction
//! behind a SHA256 content cache — **no LLM, no network, zero tokens**. A
//! rebuild after a small change re-parses only the files whose content
//! changed.
//!
//! Two properties this module holds onto deliberately:
//!
//! - **Nothing model-written ever reaches argv.** The worker's claimed
//!   file list is journaled as a discrepancy signal and never executed.
//!   `graphify update` takes a directory; the cache does the incremental
//!   work.
//! - **A graph failure can never fail a worker task.** Every path returns a
//!   [`GraphifyOutcome`], never an `Err`.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

/// Override the resolved graphify binary (tests point this at a fixture).
pub const GRAPHIFY_BIN_ENV: &str = "AGENTOS_GRAPHIFY_BIN";

/// Gate for the live graphify tests, matching the adapter E2E convention.
pub const GRAPHIFY_E2E_ENV: &str = "AGENTOS_GRAPHIFY_E2E";

/// Output directory graphify writes, relative to the path it is given.
/// Hardcoded upstream — there is no output-directory flag.
pub const GRAPHIFY_OUT_DIR: &str = "graphify-out";

/// Extensions that make a change worth re-graphing.
///
/// A slight superset of what graphify parses, on purpose: a false positive
/// costs one cache-warm update, a false negative silently leaves the graph
/// stale for the next reviewer. The asymmetry sets the bias.
const CODE_EXTENSIONS: &[&str] = &[
    "rs", "ts", "tsx", "js", "jsx", "mjs", "cjs", "py", "pyi", "go", "java", "kt", "kts", "scala",
    "cs", "rb", "swift", "php", "c", "h", "cc", "cpp", "cxx", "hpp", "hh", "vue", "svelte", "lua",
    "ps1", "sql", "sol",
];

/// Directories whose churn must never trigger a refresh — graphify's own
/// output most of all, or every run would re-trigger itself.
const IGNORED_PREFIXES: &[&str] = &[GRAPHIFY_OUT_DIR, ".agentos-worktrees", ".git"];

/// Cap on journaled path samples. Under a shared workspace `git status`
/// reports every uncommitted change in the run, which runs to hundreds.
const SAMPLE_CAP: usize = 20;
/// Cap on journaled discrepancy lists.
const DISCREPANCY_CAP: usize = 10;
/// Trailing stderr kept for diagnostics on failure.
const STDERR_TAIL: usize = 400;

/// Whether the refresh runs, and with which binary.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum GraphRefresh {
    /// Refresh when a graphify binary can be resolved. The default.
    #[default]
    Auto,
    /// Never refresh. Tests use this so behavior does not depend on whether
    /// the developer happens to have graphify installed.
    Disabled,
    /// Use this binary, skipping resolution.
    Binary(PathBuf),
}

/// Why a refresh did not run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphifySkip {
    /// Turned off by configuration.
    Disabled,
    /// Nothing that a code graph can parse changed.
    NoCodeChanges,
    /// No graphify binary on this machine.
    BinaryMissing,
}

impl GraphifySkip {
    /// Stable wire label for the journal payload.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::NoCodeChanges => "no-code-changes",
            Self::BinaryMissing => "binary-missing",
        }
    }
}

/// What one refresh attempt did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphifyOutcome {
    /// The graph was rebuilt.
    Updated {
        /// Wall-clock duration of the child process.
        elapsed_ms: u64,
    },
    /// No process was spawned.
    Skipped(GraphifySkip),
    /// graphify ran and failed, timed out, or could not be spawned.
    Failed {
        /// Human-readable cause.
        reason: String,
        /// Process exit code when there was one.
        exit_code: Option<i32>,
        /// Trailing stderr, for diagnostics only — never classification.
        stderr_tail: String,
    },
}

/// Ground truth reconciled against what the worker claimed it changed.
///
/// `git status` is the fact; `packet.files_changed` is the worker's account
/// of itself. Keeping both makes the disagreement visible: `claimed_only`
/// entries are files the worker said it touched that never changed, and
/// `unclaimed` entries are edits it did not report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeContext {
    /// Every uncommitted path, repo-relative (`git status --porcelain`).
    pub observed: Vec<String>,
    /// The subset a code graph can parse.
    pub observed_code: Vec<String>,
    /// The worker's claim, normalized.
    pub claimed: Vec<String>,
    /// Claimed but never actually changed.
    pub claimed_only: Vec<String>,
    /// Changed but never claimed.
    pub unclaimed: Vec<String>,
}

impl ChangeContext {
    /// Whether a refresh is warranted.
    pub fn has_code_changes(&self) -> bool {
        !self.observed_code.is_empty()
    }

    /// Capped journal payload — never the raw lists.
    pub fn payload(&self) -> serde_json::Value {
        serde_json::json!({
            "changedFiles": self.observed.len(),
            "codeFiles": self.observed_code.len(),
            "changedSample": cap(&self.observed_code, SAMPLE_CAP),
            "claimedFiles": self.claimed.len(),
            "claimedOnly": cap(&self.claimed_only, DISCREPANCY_CAP),
            "unclaimed": cap(&self.unclaimed, DISCREPANCY_CAP),
        })
    }
}

fn cap(paths: &[String], limit: usize) -> Vec<String> {
    paths.iter().take(limit).cloned().collect()
}

/// Whether `path` is something a code graph can parse.
pub fn is_code_path(path: &str) -> bool {
    let normalized = normalize(path);
    if normalized.is_empty() {
        return false;
    }
    if IGNORED_PREFIXES.iter().any(|prefix| {
        normalized == *prefix
            || normalized.starts_with(&format!("{prefix}/"))
            || normalized.contains(&format!("/{prefix}/"))
    }) {
        return false;
    }
    match normalized.rsplit_once('.') {
        // A trailing dot, or a dot only in a directory name, is not an
        // extension — `src/a.b/README` must not read as code.
        Some((_, ext)) if !ext.is_empty() && !ext.contains('/') => CODE_EXTENSIONS
            .iter()
            .any(|known| known.eq_ignore_ascii_case(ext)),
        _ => false,
    }
}

/// Normalize a path for comparison: forward slashes, no `./` prefix, no
/// surrounding quotes or whitespace.
fn normalize(path: &str) -> String {
    let trimmed = path.trim().trim_matches('"').trim_matches('`').trim();
    let slashed = trimmed.replace('\\', "/");
    let stripped = slashed.strip_prefix("./").unwrap_or(&slashed);
    stripped.trim_start_matches('/').to_owned()
}

/// Make a model-written path comparable to a repo-relative one: strip the
/// workspace prefix from absolute entries that land inside it, and drop
/// anything that is plainly not a path.
fn normalize_claim(claim: &str, workspace: &Path) -> Option<String> {
    let normalized = normalize(claim);
    if normalized.is_empty() {
        return None;
    }
    // Globs and prose are claims about files, not files. They stay out of
    // the matched set, which is exactly what surfaces them as `claimed_only`.
    if normalized.contains('*') || normalized.contains(' ') && !normalized.contains('/') {
        return Some(normalized);
    }
    let root = normalize(&workspace.to_string_lossy());
    if !root.is_empty() {
        let lowered = normalized.to_ascii_lowercase();
        let lowered_root = root.to_ascii_lowercase();
        if let Some(rest) = lowered.strip_prefix(&format!("{lowered_root}/")) {
            return Some(normalized[normalized.len() - rest.len()..].to_owned());
        }
    }
    Some(normalized)
}

/// Reconcile observed changes with the worker's claim.
pub fn reconcile_changes(
    observed: &[String],
    claimed: &[String],
    workspace: &Path,
) -> ChangeContext {
    let observed: Vec<String> = observed
        .iter()
        .map(|path| normalize(path))
        .filter(|path| !path.is_empty())
        .collect();
    let claimed: Vec<String> = claimed
        .iter()
        .filter_map(|claim| normalize_claim(claim, workspace))
        .collect();

    // Windows paths are case-insensitive; comparing case-sensitively there
    // would report every differently-cased claim as a hallucination.
    let fold = |path: &String| {
        if cfg!(windows) {
            path.to_ascii_lowercase()
        } else {
            path.clone()
        }
    };
    let observed_set: BTreeSet<String> = observed.iter().map(fold).collect();
    let claimed_set: BTreeSet<String> = claimed.iter().map(fold).collect();

    let observed_code: Vec<String> = observed
        .iter()
        .filter(|path| is_code_path(path))
        .cloned()
        .collect();
    let claimed_only: Vec<String> = claimed
        .iter()
        .filter(|claim| !observed_set.contains(&fold(claim)))
        .cloned()
        .collect();
    let unclaimed: Vec<String> = observed
        .iter()
        .filter(|path| !claimed_set.contains(&fold(path)))
        .cloned()
        .collect();

    ChangeContext {
        observed,
        observed_code,
        claimed,
        claimed_only,
        unclaimed,
    }
}

/// A resolved graphify binary.
#[derive(Debug, Clone)]
pub struct Graphifier {
    binary: PathBuf,
    timeout: Duration,
}

impl Graphifier {
    /// Resolve a binary for `refresh`, or `None` when the refresh is off or
    /// graphify is not installed.
    pub fn resolve(refresh: &GraphRefresh, timeout: Duration) -> Option<Self> {
        let binary = match refresh {
            GraphRefresh::Disabled => return None,
            GraphRefresh::Binary(path) => path.clone(),
            GraphRefresh::Auto => resolve_binary()?,
        };
        Some(Self { binary, timeout })
    }

    /// The resolved binary.
    pub fn binary(&self) -> &Path {
        &self.binary
    }

    /// Argv for a refresh over `workspace`.
    ///
    /// Exactly `["update", <workspace>]`. No file list, no model-written
    /// text, no shell — the incremental work is graphify's content cache.
    pub fn update_args(workspace: &Path) -> Vec<OsString> {
        vec![OsString::from("update"), workspace.as_os_str().to_owned()]
    }

    /// Refresh the graph over `workspace`. Never returns `Err`: a graph
    /// problem must not be able to fail the task that triggered it.
    pub async fn update(&self, workspace: &Path) -> GraphifyOutcome {
        // Keep graphify's output out of the product repo's history. Local
        // ignore metadata, never a `.gitignore` edit in the user's repo.
        agentos_git::worktree::ensure_excluded(workspace, &format!("{GRAPHIFY_OUT_DIR}/"));

        let args = Self::update_args(workspace);
        let mut command = command_for(&self.binary, &args);
        command
            .current_dir(workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let started = Instant::now();
        let child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                return GraphifyOutcome::Failed {
                    reason: format!("could not spawn {}: {error}", self.binary.display()),
                    exit_code: None,
                    stderr_tail: String::new(),
                }
            }
        };

        let output = match tokio::time::timeout(self.timeout, child.wait_with_output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                return GraphifyOutcome::Failed {
                    reason: format!("graphify io failure: {error}"),
                    exit_code: None,
                    stderr_tail: String::new(),
                }
            }
            Err(_) => {
                // `kill_on_drop` is best effort on Windows; be explicit.
                return GraphifyOutcome::Failed {
                    reason: format!(
                        "graphify exceeded the {}s budget; process killed",
                        self.timeout.as_secs()
                    ),
                    exit_code: None,
                    stderr_tail: String::new(),
                };
            }
        };

        if output.status.success() {
            return GraphifyOutcome::Updated {
                elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            };
        }
        GraphifyOutcome::Failed {
            reason: format!("graphify exited {}", output.status),
            exit_code: output.status.code(),
            stderr_tail: tail(&String::from_utf8_lossy(&output.stderr), STDERR_TAIL),
        }
    }
}

fn tail(text: &str, limit: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= limit {
        return trimmed.to_owned();
    }
    let skip = trimmed.chars().count() - limit;
    trimmed.chars().skip(skip).collect()
}

/// Resolve a graphify binary: explicit override, then `PATH`, then the
/// Python user-scripts directory a `pip install --user` leaves it in.
fn resolve_binary() -> Option<PathBuf> {
    if let Some(path) = binary_from_env(std::env::var(GRAPHIFY_BIN_ENV).ok().as_deref()) {
        return Some(path);
    }
    which_graphify().or_else(python_scripts_graphify)
}

/// The env-override rule as a pure function, so it is testable without
/// mutating process environment (which races across test threads).
fn binary_from_env(value: Option<&str>) -> Option<PathBuf> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    Some(PathBuf::from(value))
}

fn which_graphify() -> Option<PathBuf> {
    let (program, args) = if cfg!(windows) {
        ("where.exe", ["graphify"])
    } else {
        ("which", ["graphify"])
    };
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(PathBuf::from)
}

fn python_scripts_graphify() -> Option<PathBuf> {
    if !cfg!(windows) {
        return None;
    }
    let appdata = std::env::var("APPDATA").ok()?;
    let root = Path::new(&appdata).join("Python");
    let entries = std::fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let candidate = entry.path().join("Scripts").join("graphify.exe");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Build the command, routing `.cmd`/`.bat` shims through `cmd.exe` — a
/// batch shim cannot be executed directly on Windows. Console-script
/// installs are real `.exe`s, but `uv tool` and npm-style installs are not.
fn command_for(binary: &Path, args: &[OsString]) -> tokio::process::Command {
    let is_script = binary
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("cmd") || ext.eq_ignore_ascii_case("bat"));
    if cfg!(windows) && is_script {
        let mut command = tokio::process::Command::new("cmd.exe");
        command.arg("/D").arg("/S").arg("/C").arg(binary).args(args);
        return command;
    }
    let mut command = tokio::process::Command::new(binary);
    command.args(args);
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The load-bearing safety property: model-written text never becomes a
    /// process argument. graphify takes a directory; the file list is
    /// evidence for the journal, not input to a command.
    #[test]
    fn update_args_is_argv_only_and_carries_no_file_list() {
        let args = Graphifier::update_args(Path::new("/proj"));
        assert_eq!(args.len(), 2, "{args:?}");
        assert_eq!(args[0], OsString::from("update"));
        assert_eq!(args[1], OsString::from("/proj"));
        assert!(
            !args
                .iter()
                .any(|arg| arg.to_string_lossy().starts_with('-')),
            "no flags are passed, so none can be injected: {args:?}"
        );
    }

    /// Phases 2-7 emit markdown and wireframe HTML. Those must skip with no
    /// process spawned at all, or every document phase pays for a rebuild
    /// that can parse nothing.
    #[test]
    fn code_extension_set_selects_code_and_rejects_documents() {
        for code in [
            "src/a.rs",
            "web/app.tsx",
            "h.py",
            "components/Timer.jsx",
            "SRC/B.RS",
        ] {
            assert!(is_code_path(code), "{code} should be code");
        }
        for not_code in [
            "docs/PRD.md",
            "docs/features/01-timer.md",
            "wireframe/index.html",
            "wireframe/tokens.css",
            "package.json",
            "Cargo.lock",
            "LICENSE",
            "src/a.b/README",
            "trailing.",
        ] {
            assert!(!is_code_path(not_code), "{not_code} should not be code");
        }
    }

    /// graphify's own output must never trigger a refresh, or each run
    /// re-triggers the next.
    #[test]
    fn graphify_output_and_worktrees_never_count_as_changes() {
        for ignored in [
            "graphify-out/graph.json",
            "graphify-out/cache/abc.json",
            ".agentos-worktrees/task/src/a.rs",
            "nested/graphify-out/graph.json",
        ] {
            assert!(!is_code_path(ignored), "{ignored} must be ignored");
        }
    }

    #[test]
    fn reconcile_normalizes_windows_paths_and_flags_hallucinations() {
        let workspace = Path::new("D:/proj");
        let observed = vec!["src/a.rs".to_owned(), "src/b.rs".to_owned()];
        let claimed = vec![
            "src\\a.rs".to_owned(),
            "D:/proj/src/b.rs".to_owned(),
            "src/never.rs".to_owned(),
            "the whole src directory".to_owned(),
            "src/**/*.rs".to_owned(),
        ];
        let context = reconcile_changes(&observed, &claimed, workspace);

        assert!(
            context.claimed_only.contains(&"src/never.rs".to_owned()),
            "a file that never changed is a hallucination: {:?}",
            context.claimed_only
        );
        assert!(context.claimed_only.iter().any(|c| c.contains('*')));
        assert!(
            !context.claimed_only.contains(&"src/a.rs".to_owned()),
            "a backslash path is the same file: {:?}",
            context.claimed_only
        );
        assert!(
            !context.claimed_only.iter().any(|c| c.ends_with("src/b.rs")),
            "an absolute path under the workspace is the same file: {:?}",
            context.claimed_only
        );
        assert!(context.unclaimed.is_empty(), "{:?}", context.unclaimed);
        assert!(context.has_code_changes());
    }

    #[test]
    fn reconcile_reports_edits_the_worker_never_mentioned() {
        let context = reconcile_changes(
            &["src/a.rs".to_owned(), "src/secret.rs".to_owned()],
            &["src/a.rs".to_owned()],
            Path::new("/proj"),
        );
        assert_eq!(context.unclaimed, vec!["src/secret.rs".to_owned()]);
    }

    /// Under a shared workspace `git status` reports the whole run's
    /// uncommitted state; an uncapped payload would be a fat journal event.
    #[test]
    fn reconcile_caps_every_journaled_list() {
        let observed: Vec<String> = (0..500).map(|i| format!("src/f{i}.rs")).collect();
        let claimed: Vec<String> = (0..500).map(|i| format!("src/ghost{i}.rs")).collect();
        let context = reconcile_changes(&observed, &claimed, Path::new("/proj"));
        let payload = context.payload();

        assert_eq!(payload["changedFiles"], 500);
        assert_eq!(payload["codeFiles"], 500);
        assert_eq!(
            payload["changedSample"].as_array().unwrap().len(),
            SAMPLE_CAP
        );
        assert_eq!(
            payload["claimedOnly"].as_array().unwrap().len(),
            DISCREPANCY_CAP
        );
        assert_eq!(
            payload["unclaimed"].as_array().unwrap().len(),
            DISCREPANCY_CAP
        );
    }

    #[test]
    fn only_documents_changing_means_no_refresh() {
        let context = reconcile_changes(
            &["docs/PRD.md".to_owned(), "wireframe/index.html".to_owned()],
            &[],
            Path::new("/proj"),
        );
        assert!(!context.has_code_changes());
    }

    #[test]
    fn binary_resolution_prefers_the_env_override() {
        assert_eq!(
            binary_from_env(Some("C:/tools/graphify.exe")),
            Some(PathBuf::from("C:/tools/graphify.exe"))
        );
        assert_eq!(binary_from_env(Some("   ")), None);
        assert_eq!(binary_from_env(None), None);
    }

    /// Disabled must resolve to nothing even where graphify is installed —
    /// this is what keeps the test suite independent of the dev machine.
    #[test]
    fn disabled_never_resolves_and_an_explicit_binary_always_does() {
        assert!(Graphifier::resolve(&GraphRefresh::Disabled, Duration::from_secs(1)).is_none());
        let pinned = Graphifier::resolve(
            &GraphRefresh::Binary(PathBuf::from("/tmp/graphify")),
            Duration::from_secs(1),
        )
        .expect("an explicit binary is used as given");
        assert_eq!(pinned.binary(), Path::new("/tmp/graphify"));
    }

    #[test]
    fn default_refresh_is_auto() {
        assert_eq!(GraphRefresh::default(), GraphRefresh::Auto);
    }

    #[cfg(windows)]
    #[test]
    fn windows_shim_dispatch_wraps_cmd_scripts_only() {
        let args = Graphifier::update_args(Path::new("C:/proj"));
        let shim = command_for(Path::new("C:/tools/graphify.cmd"), &args);
        assert_eq!(shim.as_std().get_program(), std::ffi::OsStr::new("cmd.exe"));
        let direct = command_for(Path::new("C:/tools/graphify.exe"), &args);
        assert_eq!(
            direct.as_std().get_program(),
            std::ffi::OsStr::new("C:/tools/graphify.exe")
        );
    }

    #[test]
    fn stderr_tail_keeps_the_end_not_the_start() {
        let long: String = (0..1000).map(|_| 'x').collect();
        assert_eq!(tail(&long, 10).chars().count(), 10);
        assert_eq!(tail("short", 10), "short");
    }

    /// Live: free (no LLM, no network) but needs graphify installed.
    #[tokio::test]
    #[ignore = "requires a graphify install; set AGENTOS_GRAPHIFY_E2E=1"]
    async fn live_graphify_update_produces_a_graph_json() {
        if std::env::var(GRAPHIFY_E2E_ENV).is_err() {
            return;
        }
        let Some(graphifier) = Graphifier::resolve(&GraphRefresh::Auto, Duration::from_secs(180))
        else {
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("lib.rs"),
            "pub fn add(a: i32) -> i32 { a }\n",
        )
        .expect("write");

        let outcome = graphifier.update(dir.path()).await;
        assert!(
            matches!(outcome, GraphifyOutcome::Updated { .. }),
            "{outcome:?}"
        );
        let graph = dir.path().join(GRAPHIFY_OUT_DIR).join("graph.json");
        assert!(graph.is_file(), "graph.json lands under the path argument");
        let parsed: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&graph).expect("read")).expect("json");
        assert!(parsed.get("nodes").is_some(), "{parsed}");
    }
}
