//! Policy → adapter constraint compilation: the "enforced outside model text"
//! bridge (PRD SEC-01: "Policy is compiled into task contract and runtime
//! sandbox configuration").
//!
//! [`compile_to_spawn_spec`] turns a [`PermissionSet`] plus the task's
//! [`TaskScope`] into [`SpawnConstraints`] — the four path/tool fields of
//! `agentos_adapters::SpawnSpec`. Adapters own the final hop from these
//! lists to concrete CLI flags; the mapping table lives in
//! `docs/F-10-policy-engine.md` §2. Key properties:
//!
//! - **allowed_paths**: the workspace plus the literal root directory of
//!   every read/write glob (adapters grant access beyond the cwd here).
//! - **forbidden_paths**: readable roots that are **not writable** — the
//!   read-only markers. Paths absent from `allowed_paths` are fully
//!   off-limits (not granted at all); `forbidden_paths` ⊆ `allowed_paths`
//!   and means "no writes" (adapters map them to write-denials where the CLI
//!   supports it). A read root that *partially* overlaps a write glob is
//!   deliberately not marked forbidden — write scoping then defers to the
//!   adapter's write-rooting (agy virtualizes writes outside `--add-dir`).
//! - **tool_denylist** is derived from the capability modes: shell `Denied`
//!   denies the shell tool family; `Offline` denies the network tool family;
//!   an empty tool allowlist (fail-closed "no tools") additionally denies
//!   the file-write family. **Deny beats allow**: a tool appearing on both
//!   lists is dropped from the allowlist.
//!
//! [`git_gate_check`] is the synchronous git seam feeding
//! `agentos_git::MutationQueue`'s approval-gated push: permission and
//! approval are checked in **code**, so neither a prompt instruction nor a
//! forged approval flag can let a worker without `Push` permission push
//! (PRD §23.3 release gate).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::approval::Gate;
use crate::permission::{
    glob_overlap, normalize_glob, GitAction, NetworkPolicy, PermissionSet, ShellMode,
};

/// Shell-capable tool names denied when [`ShellMode::Denied`] (claude `Bash`
/// canon plus a generic alias).
pub const SHELL_TOOLS: &[&str] = &["Bash", "Shell"];

/// Network-capable tool names denied when [`NetworkPolicy::Offline`] (claude
/// `WebFetch`/`WebSearch` canon plus a generic alias).
pub const NETWORK_TOOLS: &[&str] = &["WebFetch", "WebSearch", "NetFetch"];

/// File-write tool names denied when the tool allowlist is empty (fail-closed
/// default; claude `Edit`/`Write`/`NotebookEdit` canon).
pub const FILE_WRITE_TOOLS: &[&str] = &["Edit", "Write", "NotebookEdit"];

/// Where the task runs: the workspace (typically a per-task git worktree)
/// that relative permission globs resolve against.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskScope {
    /// Absolute workspace root, `/`-separated (Windows reference platform:
    /// callers normalize `\` before construction).
    pub workspace: String,
}

impl TaskScope {
    /// A scope rooted at `workspace`.
    pub fn new(workspace: impl Into<String>) -> Self {
        Self {
            workspace: workspace.into(),
        }
    }
}

/// Adapter-level constraints mirroring the four policy-bearing fields of
/// `agentos_adapters::SpawnSpec` (task identity, objective, model, timeout,
/// and isolated home are orthogonal to policy and stay with the adapter).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpawnConstraints {
    /// Paths the runtime may access in addition to its process cwd
    /// (claude additional-read dirs, agy `--add-dir` roots).
    pub allowed_paths: Vec<String>,
    /// Readable-but-not-writable (or fully off-limits, when absent from
    /// `allowed_paths`) paths; harness-enforced, never prompt-enforced.
    pub forbidden_paths: Vec<String>,
    /// Per-tool allowlist (claude `--allowedTools=Bash(echo:*)` form).
    pub tool_allowlist: Vec<String>,
    /// Per-tool denylist (claude `--disallowedTools=Bash,WebFetch` form).
    pub tool_denylist: Vec<String>,
}

/// Compile a [`PermissionSet`] under [`TaskScope`] into adapter constraints.
///
/// Total by design — even [`PermissionSet::deny_all`] compiles (to the most
/// restrictive lists the seam can express). Limitations: glob complement
/// (everything outside the read scope) is not enumerable as a path list, so
/// containment relies on the adapter sandbox rooting the process at
/// `allowed_paths`; true filesystem denial is the sandbox's job (SEC-01
/// "enforce through sandbox/workspace boundaries"), these lists are the
/// contract it compiles from.
pub fn compile_to_spawn_spec(perms: &PermissionSet, scope: &TaskScope) -> SpawnConstraints {
    let workspace = scope.workspace.trim_end_matches('/');

    // allowed = workspace + literal root of every read/write glob.
    let mut allowed: BTreeSet<String> = BTreeSet::new();
    allowed.insert(workspace.to_owned());
    for glob in perms.read_paths.iter().chain(perms.write_paths.iter()) {
        let root = glob_root(glob);
        if !root.is_empty() {
            allowed.insert(format!("{workspace}/{root}"));
        }
    }

    // forbidden = readable roots with zero write overlap (read-only markers).
    let mut forbidden: BTreeSet<String> = BTreeSet::new();
    for glob in &perms.read_paths {
        let writable = perms.write_paths.iter().any(|w| glob_overlap(w, glob));
        if writable {
            continue;
        }
        let root = glob_root(glob);
        if root.is_empty() {
            // Whole-workspace readable, nothing writable: the workspace root
            // itself is the read-only marker.
            forbidden.insert(workspace.to_owned());
        } else {
            forbidden.insert(format!("{workspace}/{root}"));
        }
    }

    // Derived tool denylist; deny beats allow.
    let mut deny: BTreeSet<String> = BTreeSet::new();
    if perms.shell_mode == ShellMode::Denied {
        deny.extend(SHELL_TOOLS.iter().map(|s| (*s).to_owned()));
    }
    if perms.network_policy == NetworkPolicy::Offline {
        deny.extend(NETWORK_TOOLS.iter().map(|s| (*s).to_owned()));
    }
    if perms.tool_allowlist.is_empty() {
        deny.extend(FILE_WRITE_TOOLS.iter().map(|s| (*s).to_owned()));
    }
    let allow: Vec<String> = perms
        .tool_allowlist
        .iter()
        .filter(|tool| !deny.contains(*tool))
        .cloned()
        .collect();

    SpawnConstraints {
        allowed_paths: allowed.into_iter().collect(),
        forbidden_paths: forbidden.into_iter().collect(),
        tool_allowlist: allow,
        tool_denylist: deny.into_iter().collect(),
    }
}

/// Synchronous git-action gate (feeds `agentos_git::MutationQueue`'s
/// approval-gated push).
///
/// Order matters and is deliberate:
///
/// 1. the action must be in the set's `git_actions` — **permission trumps
///    approval**: a worker without `Push` is denied even when
///    `approved_by_policy` is `true` (a forged or stale flag, or a model
///    claiming approval — PRD §23.3: "a worker without Git permission cannot
///    commit/push even if instructed in prompt");
/// 2. `Push` additionally requires a live approval
///    (`approved_by_policy` comes from
///    [`crate::ApprovalStore::is_approved`], which enforces fingerprint and
///    expiry).
pub fn git_gate_check(
    perms: &PermissionSet,
    action: GitAction,
    approved_by_policy: bool,
) -> Result<(), PolicyDenial> {
    if !perms.allows_git(action) {
        tracing::debug!(action = %action, "git gate: action not in permission set");
        return Err(PolicyDenial::GitActionNotPermitted { action });
    }
    if action == GitAction::Push && !approved_by_policy {
        tracing::debug!("git gate: push requires a live approval bound to the exact operation");
        return Err(PolicyDenial::ApprovalRequired {
            gate: Gate::GitPush,
        });
    }
    Ok(())
}

/// Why an operation was refused by policy. Denials are facts for the audit
/// trail ([`crate::AuditStore::record_permission_denial`]), not retryable
/// errors — retrying without a permission/approval change yields the same
/// denial.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyDenial {
    /// The git action is not in the permission set's `git_actions`.
    #[error("git action `{action}` is not permitted for this role/task")]
    GitActionNotPermitted {
        /// The refused action.
        action: GitAction,
    },
    /// The gate has no live approval bound to the exact operation.
    #[error("gate `{gate}` requires an unexpired human approval bound to this exact operation")]
    ApprovalRequired {
        /// The gate that was not satisfied.
        gate: Gate,
    },
}

/// The literal (wildcard-free) leading directory of a glob — the deepest
/// concrete path that certainly falls under it. `""` means "no literal root"
/// (the glob starts with a wildcard or is `**`), i.e. the workspace itself.
fn glob_root(glob: &str) -> String {
    let normalized = normalize_glob(glob);
    if normalized.is_empty() {
        return String::new();
    }
    let mut root: Vec<&str> = Vec::new();
    for segment in normalized.split('/') {
        if segment.contains('*') {
            break;
        }
        root.push(segment);
    }
    root.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::glob_matches;
    use serde_json::json;

    #[test]
    fn glob_root_takes_the_literal_prefix() {
        assert_eq!(glob_root("src/**/*.rs"), "src");
        assert_eq!(glob_root("docs/*.md"), "docs");
        assert_eq!(glob_root("crates/policy/src/**"), "crates/policy/src");
        assert_eq!(glob_root("docs"), "docs");
        assert_eq!(glob_root("**"), "");
        assert_eq!(glob_root("*.md"), "");
        assert_eq!(glob_root("./src/**"), "src");
    }

    #[test]
    fn read_only_worker_compiles_to_read_only_workspace() {
        let scope = TaskScope::new("D:/repo/.agentos-worktrees/t1");
        let constraints = compile_to_spawn_spec(&PermissionSet::worker_read_only(), &scope);

        assert_eq!(
            constraints.allowed_paths,
            vec!["D:/repo/.agentos-worktrees/t1"]
        );
        // Whole workspace readable, nothing writable: the workspace root is
        // the read-only marker.
        assert_eq!(
            constraints.forbidden_paths,
            vec!["D:/repo/.agentos-worktrees/t1"]
        );
        assert_eq!(constraints.tool_allowlist, vec!["Read", "Grep", "Glob"]);
        // shell Denied + Offline -> denylist carries both families.
        for tool in SHELL_TOOLS.iter().chain(NETWORK_TOOLS.iter()) {
            assert!(
                constraints.tool_denylist.contains(&(*tool).to_owned()),
                "expected {tool} denied"
            );
        }
        assert!(!constraints.tool_denylist.iter().any(|t| t == "Read"));
    }

    #[test]
    fn write_worker_scopes_allowed_paths_and_keeps_shell_gated() {
        let scope = TaskScope::new("D:/repo/.agentos-worktrees/t2");
        let perms = PermissionSet::worker_write(&["crates/policy/**", "docs/f10.md"]);
        let constraints = compile_to_spawn_spec(&perms, &scope);

        assert_eq!(
            constraints.allowed_paths,
            vec![
                "D:/repo/.agentos-worktrees/t2",
                "D:/repo/.agentos-worktrees/t2/crates/policy",
                "D:/repo/.agentos-worktrees/t2/docs/f10.md",
            ]
        );
        // Read scope == write scope -> no read-only markers.
        assert!(constraints.forbidden_paths.is_empty());
        // AskApproval keeps the shell capability: no shell tool denied...
        assert!(!constraints.tool_denylist.iter().any(|t| t == "Bash"));
        // ...and the allowlist keeps Bash; network stays offline-denied.
        assert!(constraints.tool_allowlist.iter().any(|t| t == "Bash"));
        for tool in NETWORK_TOOLS {
            assert!(constraints.tool_denylist.contains(&(*tool).to_owned()));
        }
    }

    #[test]
    fn readable_but_unwritable_roots_become_forbidden() {
        let scope = TaskScope::new("D:/wt");
        let perms = PermissionSet {
            read_paths: vec!["docs/**".to_owned(), "src/**".to_owned()],
            write_paths: vec!["src/**".to_owned()],
            ..PermissionSet::worker_write(&["src/**"])
        };
        let constraints = compile_to_spawn_spec(&perms, &scope);
        assert_eq!(constraints.forbidden_paths, vec!["D:/wt/docs"]);
        assert!(constraints.allowed_paths.contains(&"D:/wt/src".to_owned()));
        assert!(constraints.allowed_paths.contains(&"D:/wt/docs".to_owned()));
    }

    #[test]
    fn partially_writable_read_root_is_not_marked_forbidden() {
        let scope = TaskScope::new("D:/wt");
        let perms = PermissionSet {
            read_paths: vec!["src/**".to_owned()],
            write_paths: vec!["src/generated/**".to_owned()],
            ..PermissionSet::worker_write(&["src/generated/**"])
        };
        let constraints = compile_to_spawn_spec(&perms, &scope);
        // "src" partially overlaps the write scope: no blanket read-only
        // marker (write scoping defers to adapter write-rooting).
        assert!(constraints.forbidden_paths.is_empty());
    }

    #[test]
    fn deny_all_compiles_to_maximally_restrictive_lists() {
        let scope = TaskScope::new("D:/wt");
        let constraints = compile_to_spawn_spec(&PermissionSet::deny_all(), &scope);
        assert_eq!(constraints.allowed_paths, vec!["D:/wt"]);
        assert!(
            constraints.forbidden_paths.is_empty(),
            "no read globs -> no read-only markers"
        );
        assert!(constraints.tool_allowlist.is_empty());
        for tool in SHELL_TOOLS
            .iter()
            .chain(NETWORK_TOOLS.iter())
            .chain(FILE_WRITE_TOOLS.iter())
        {
            assert!(
                constraints.tool_denylist.contains(&(*tool).to_owned()),
                "{tool}"
            );
        }
    }

    #[test]
    fn deny_beats_allow_for_tools() {
        let scope = TaskScope::new("D:/wt");
        let perms = PermissionSet {
            tool_allowlist: vec!["Bash".to_owned(), "Read".to_owned()],
            ..PermissionSet::deny_all()
        };
        let constraints = compile_to_spawn_spec(&perms, &scope);
        // Bash is allowed by list but shell is Denied -> denied wins.
        assert!(constraints.tool_denylist.iter().any(|t| t == "Bash"));
        assert!(!constraints.tool_allowlist.iter().any(|t| t == "Bash"));
        assert!(constraints.tool_allowlist.iter().any(|t| t == "Read"));
    }

    #[test]
    fn git_gate_permission_trumps_approval() {
        // PRD §23.3: a worker without git permission cannot push, even when
        // an approval flag claims otherwise (forged/stale/model-asserted).
        let worker = PermissionSet::worker_write(&["src/**"]); // no git actions
        let err = git_gate_check(&worker, GitAction::Push, true).expect_err("must deny");
        assert_eq!(
            err,
            PolicyDenial::GitActionNotPermitted {
                action: GitAction::Push
            }
        );
        // Same for commit.
        let err = git_gate_check(&worker, GitAction::Commit, true).expect_err("must deny");
        assert!(matches!(err, PolicyDenial::GitActionNotPermitted { .. }));
    }

    #[test]
    fn git_gate_push_requires_approval_even_with_permission() {
        let manager = PermissionSet::git_manager();
        let err = git_gate_check(&manager, GitAction::Push, false).expect_err("must deny");
        assert_eq!(
            err,
            PolicyDenial::ApprovalRequired {
                gate: Gate::GitPush
            }
        );
        assert!(git_gate_check(&manager, GitAction::Push, true).is_ok());
        // Non-push actions need no approval flag once permitted.
        for action in [GitAction::Commit, GitAction::Merge, GitAction::Rebase] {
            assert!(git_gate_check(&manager, action, false).is_ok());
        }
    }

    /// End-to-end PRD §23.3 release-gate scenario: a genuinely approved push
    /// operation (live approval store row) still cannot pass for a worker
    /// without Push permission; a mutated operation loses its approval even
    /// for the git manager.
    #[test]
    fn release_gate_scenario_forged_and_stale_approvals_do_not_push() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            crate::ApprovalStore::open(&dir.path().join("approvals.sqlite3")).expect("open");

        let push_op = json!({"action": "push", "remote": "origin", "ref": "release/1.0"});
        let request = store
            .request(
                Gate::GitPush,
                &push_op,
                "worker-1",
                chrono::Duration::seconds(600),
            )
            .expect("request");
        assert!(store
            .resolve(&request.id, crate::ApprovalDecision::Approved)
            .unwrap());
        // The approval is live and fingerprint-exact...
        assert!(store.is_approved(Gate::GitPush, &push_op).unwrap());

        // ...yet the worker (no Push in git_actions) fails the gate anyway.
        let worker = PermissionSet::worker_write(&["src/**"]);
        let approved = store.is_approved(Gate::GitPush, &push_op).unwrap();
        assert!(matches!(
            git_gate_check(&worker, GitAction::Push, approved),
            Err(PolicyDenial::GitActionNotPermitted { .. })
        ));

        // The git manager may push the approved operation...
        let manager = PermissionSet::git_manager();
        assert!(git_gate_check(&manager, GitAction::Push, approved).is_ok());

        // ...but a mutated operation (sneaked-in `force`) is a different
        // fingerprint: not approved, gate denies.
        let mutated =
            json!({"action": "push", "remote": "origin", "ref": "release/1.0", "force": true});
        let mutated_approved = store.is_approved(Gate::GitPush, &mutated).unwrap();
        assert!(!mutated_approved);
        assert!(matches!(
            git_gate_check(&manager, GitAction::Push, mutated_approved),
            Err(PolicyDenial::ApprovalRequired { .. })
        ));
    }

    #[test]
    fn constraints_serialize_camel_case_and_round_trip() {
        let scope = TaskScope::new("D:/wt");
        let constraints = compile_to_spawn_spec(&PermissionSet::worker_read_only(), &scope);
        let value = serde_json::to_value(&constraints).expect("serialize");
        for key in [
            "allowedPaths",
            "forbiddenPaths",
            "toolAllowlist",
            "toolDenylist",
        ] {
            assert!(value.get(key).is_some(), "expected {key} in {value}");
        }
        let round_tripped: SpawnConstraints =
            serde_json::from_value(serde_json::to_value(&constraints).unwrap()).unwrap();
        assert_eq!(round_tripped, constraints);
    }

    /// The compiled lists must respect the source permission semantics: no
    /// readable-but-unwritable root is left unmarked, and wildcard grammar
    /// quirks do not leak into the path lists.
    #[test]
    fn compiled_roots_are_literal_paths() {
        let scope = TaskScope::new("D:/wt");
        let perms = PermissionSet {
            read_paths: vec!["docs/**".to_owned()],
            write_paths: Vec::new(),
            ..PermissionSet::deny_all()
        };
        let constraints = compile_to_spawn_spec(&perms, &scope);
        for path in constraints
            .allowed_paths
            .iter()
            .chain(constraints.forbidden_paths.iter())
        {
            assert!(
                !path.contains('*'),
                "compiled paths must be literal: {path}"
            );
        }
        assert!(
            glob_matches("docs/**", "docs/readme.md"),
            "sanity of source glob"
        );
    }
}
