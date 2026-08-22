//! Capability permission sets (PRD §14 SEC-01): per role/task controls for
//! read/write paths, shell, network, git actions, secret scopes, tools, and
//! approval rules.
//!
//! Enforcement is **contract-level, never prompt text** (PRD §3 "deterministic
//! shell around probabilistic intelligence"): these sets are compiled into
//! adapter-level [`crate::SpawnConstraints`] (see [`crate::compile`]) that
//! adapters turn into CLI flags/sandboxes, and consulted synchronously by the
//! git gate ([`crate::git_gate_check`]). A model claiming it "was approved"
//! changes nothing — permission lives outside model text.
//!
//! Glob grammar (identical to F-09 `agentos-git` ownership so the whole
//! workspace speaks one path language):
//!
//! - `*` matches zero or more characters **within one path segment**;
//! - `**` as a whole segment matches zero or more whole segments (so `src/**`
//!   also matches `src` itself — a documented divergence from gitignore);
//! - everything else matches literally; backslashes are normalized to `/`
//!   (Windows reference platform); matching is case-sensitive.
//!
//! Scope independence: **write permission does not imply read permission and
//! vice versa** — `can_read` and `can_write` consult disjoint glob lists by
//! design. A set granting writes but no reads is representable; adapters and
//! callers must check both.

use crate::approval::Gate;
use serde::{Deserialize, Serialize};

/// Whether shell execution is available at all (SEC-01 `shellMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShellMode {
    /// No shell execution, ever. Compiles to shell tools on the denylist.
    Denied,
    /// Shell available, but gated behind a human approval
    /// (SEC-04 `destructive_shell` gate by convention).
    AskApproval,
    /// Shell available within the other policy constraints.
    Allowed,
}

impl Default for ShellMode {
    /// Fail-closed: an unspecified shell mode denies shell.
    fn default() -> Self {
        ShellMode::Denied
    }
}

/// Outbound network profile (SEC-02).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicy {
    /// No outbound network. Compiles to network tools on the denylist.
    Offline,
    /// Only the listed hosts (exact, case-insensitive names — no wildcards,
    /// ports, or CIDR ranges; that is a sandbox/proxy concern, SEC-02).
    Allowlist(Vec<String>),
    /// Any host. Reserve for roles that genuinely cannot enumerate remotes
    /// (e.g. a git manager pushing to user-configured remotes).
    Unrestricted,
}

impl Default for NetworkPolicy {
    /// Fail-closed: an unspecified network policy is offline.
    fn default() -> Self {
        NetworkPolicy::Offline
    }
}

impl NetworkPolicy {
    /// Whether `host` is reachable under this profile.
    pub fn allows(&self, host: &str) -> bool {
        match self {
            NetworkPolicy::Offline => false,
            NetworkPolicy::Allowlist(hosts) => {
                let host = host.trim();
                hosts
                    .iter()
                    .any(|allowed| allowed.trim().eq_ignore_ascii_case(host))
            }
            NetworkPolicy::Unrestricted => true,
        }
    }
}

/// A git mutation a role may request of the git manager (wire strings match
/// `agentos_git::MutationAction`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitAction {
    /// Commit staged work on a task branch.
    Commit,
    /// Merge a task branch into the integration branch.
    Merge,
    /// Rebase a task branch onto a new base.
    Rebase,
    /// Push to a remote. Always approval-gated on top of the permission
    /// (SEC-04 / GIT-01); see [`crate::git_gate_check`].
    Push,
}

impl GitAction {
    /// Canonical snake_case storage/wire string.
    pub fn as_str(self) -> &'static str {
        match self {
            GitAction::Commit => "commit",
            GitAction::Merge => "merge",
            GitAction::Rebase => "rebase",
            GitAction::Push => "push",
        }
    }

    /// Parse the canonical storage string.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "commit" => Some(GitAction::Commit),
            "merge" => Some(GitAction::Merge),
            "rebase" => Some(GitAction::Rebase),
            "push" => Some(GitAction::Push),
            _ => None,
        }
    }
}

impl std::fmt::Display for GitAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One human-approval rule (SEC-01 `approvalRules[]`): operations matching
/// `gate` require a human approval that stays valid for `ttl_secs`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRule {
    /// The gate this rule switches on.
    pub gate: Gate,
    /// How long an approval for this gate remains valid, in seconds.
    pub ttl_secs: u64,
}

/// Per role/task capability set (PRD SEC-01 key state).
///
/// Serde defaults are **fail-closed**: a JSON fragment that omits fields
/// deserializes to a set that denies reads/writes/shell/network/git/secrets.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionSet {
    /// Globs the role may read. Empty = no reads.
    #[serde(default)]
    pub read_paths: Vec<String>,
    /// Globs the role may write. Empty = no writes. Independent of
    /// `read_paths` — write does NOT imply read.
    #[serde(default)]
    pub write_paths: Vec<String>,
    /// Shell availability.
    #[serde(default)]
    pub shell_mode: ShellMode,
    /// Outbound network profile.
    #[serde(default)]
    pub network_policy: NetworkPolicy,
    /// Git mutations the role may request of the git manager.
    #[serde(default)]
    pub git_actions: Vec<GitAction>,
    /// Secret scopes the role may request leases for (SEC-03).
    #[serde(default)]
    pub secret_scopes: Vec<String>,
    /// Exact tool names allowed. **Fail-closed**: empty = no tools allowed.
    /// Names are provider-canonical identifiers (claude `Read`/`Edit`/`Bash`,
    /// ...), matched case-sensitively.
    #[serde(default)]
    pub tool_allowlist: Vec<String>,
    /// Gates that additionally require human approval, with their approval
    /// ttl.
    #[serde(default)]
    pub approval_rules: Vec<ApprovalRule>,
}

impl PermissionSet {
    /// Whether `path` (workspace-relative, `/`-separated) falls under any
    /// read glob. Empty `read_paths` denies everything.
    pub fn can_read(&self, path: &str) -> bool {
        self.read_paths.iter().any(|glob| glob_matches(glob, path))
    }

    /// Whether `path` falls under any write glob. **Independent of
    /// [`PermissionSet::can_read`]** — granting writes grants no reads.
    pub fn can_write(&self, path: &str) -> bool {
        self.write_paths.iter().any(|glob| glob_matches(glob, path))
    }

    /// Whether the shell capability is available at all. `true` for
    /// [`ShellMode::AskApproval`] (available, gated) — check
    /// [`PermissionSet::shell_mode`] to distinguish gated from free.
    pub fn allows_shell(&self) -> bool {
        self.shell_mode != ShellMode::Denied
    }

    /// Whether git action `action` is in this set's `git_actions`. This is
    /// the permission half of the gate; push additionally requires an
    /// approval (see [`crate::git_gate_check`]).
    pub fn allows_git(&self, action: GitAction) -> bool {
        self.git_actions.contains(&action)
    }

    /// Whether `host` is reachable under the network policy.
    pub fn allows_network(&self, host: &str) -> bool {
        self.network_policy.allows(host)
    }

    /// Whether tool `name` is allowed. **Fail-closed**: an empty allowlist
    /// allows nothing — the PRD allowlist narrows; total tool denial is
    /// expressed by leaving it empty, and the compile step then denies the
    /// file-write tool families at the adapter seam.
    pub fn allows_tool(&self, name: &str) -> bool {
        self.tool_allowlist.iter().any(|tool| tool == name)
    }

    /// The approval rule configured for `gate`, if any (first match).
    pub fn approval_rule(&self, gate: Gate) -> Option<&ApprovalRule> {
        self.approval_rules.iter().find(|rule| rule.gate == gate)
    }

    /// Read-only analyst worker: whole-repo reads, no writes, no shell,
    /// offline, no git, no secrets. Only read/search tools.
    pub fn worker_read_only() -> Self {
        Self {
            read_paths: vec!["**".to_owned()],
            write_paths: Vec::new(),
            shell_mode: ShellMode::Denied,
            network_policy: NetworkPolicy::Offline,
            git_actions: Vec::new(),
            secret_scopes: Vec::new(),
            tool_allowlist: ["Read", "Grep", "Glob"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            approval_rules: Vec::new(),
        }
    }

    /// Write worker scoped to `paths`: reads and writes exactly the given
    /// globs, shell only behind approval, offline by default (network must be
    /// widened explicitly — least privilege, PRD §3), no git actions (all
    /// mutations flow through the git manager), package installs and
    /// destructive shell gated.
    pub fn worker_write(paths: &[&str]) -> Self {
        let paths: Vec<String> = paths.iter().map(|p| (*p).to_owned()).collect();
        Self {
            read_paths: paths.clone(),
            write_paths: paths,
            shell_mode: ShellMode::AskApproval,
            network_policy: NetworkPolicy::Offline,
            git_actions: Vec::new(),
            secret_scopes: Vec::new(),
            tool_allowlist: ["Read", "Grep", "Glob", "Edit", "Write", "Bash"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            approval_rules: vec![
                ApprovalRule {
                    gate: Gate::PackageInstall,
                    ttl_secs: 900,
                },
                ApprovalRule {
                    gate: Gate::DestructiveShell,
                    ttl_secs: 900,
                },
            ],
        }
    }

    /// Git manager: whole-repo read/write, shell for the git CLI, all four
    /// git actions. Network is `Unrestricted` because remotes are
    /// user-configured and not enumerable by a preset — push remains the one
    /// action that additionally requires a human approval (rule below, and
    /// [`crate::git_gate_check`] enforces it even without the rule).
    pub fn git_manager() -> Self {
        Self {
            read_paths: vec!["**".to_owned()],
            write_paths: vec!["**".to_owned()],
            shell_mode: ShellMode::Allowed,
            network_policy: NetworkPolicy::Unrestricted,
            git_actions: vec![
                GitAction::Commit,
                GitAction::Merge,
                GitAction::Rebase,
                GitAction::Push,
            ],
            secret_scopes: Vec::new(),
            tool_allowlist: ["Read", "Grep", "Glob", "Edit", "Write", "Bash"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            approval_rules: vec![ApprovalRule {
                gate: Gate::GitPush,
                ttl_secs: 600,
            }],
        }
    }

    /// Quarantine set: everything denied, every list empty. Compiles to the
    /// most restrictive adapter constraints the seam can express.
    pub fn deny_all() -> Self {
        Self {
            read_paths: Vec::new(),
            write_paths: Vec::new(),
            shell_mode: ShellMode::Denied,
            network_policy: NetworkPolicy::Offline,
            git_actions: Vec::new(),
            secret_scopes: Vec::new(),
            tool_allowlist: Vec::new(),
            approval_rules: Vec::new(),
        }
    }
}

/// Whether two permission sets have **overlapping write scopes** — some
/// concrete path could match a write glob of each. The scheduler must
/// serialize such tasks (they may touch the same files); see the ownership
/// map in F-09 which uses the same glob grammar. Conservative within a
/// segment: `a*` vs `b*` reports overlap (never misses a real conflict).
pub fn overlapping_write_conflict(a: &PermissionSet, b: &PermissionSet) -> bool {
    a.write_paths
        .iter()
        .any(|wa| b.write_paths.iter().any(|wb| glob_overlap(wa, wb)))
}

/// Normalize a glob/path: backslashes to forward slashes (Windows reference
/// platform), strip a leading `./` and surrounding separators.
pub(crate) fn normalize_glob(glob: &str) -> String {
    glob.replace('\\', "/")
        .trim()
        .trim_start_matches("./")
        .trim_matches('/')
        .to_string()
}

/// Whether `pattern` matches `path` under the workspace glob grammar
/// (see the [module documentation](self) for semantics and limitations).
pub fn glob_matches(pattern: &str, path: &str) -> bool {
    let normalized_pattern = normalize_glob(pattern);
    let normalized_path = normalize_glob(path);
    if normalized_pattern.is_empty() || normalized_path.is_empty() {
        return false;
    }
    let pattern_segs: Vec<&str> = normalized_pattern.split('/').collect();
    let path_segs: Vec<&str> = normalized_path.split('/').collect();
    match_segments(&pattern_segs, &path_segs)
}

/// Whether some concrete path can match both `a` and `b`.
///
/// Exact for concrete/directory globs; **conservative** within a single
/// segment: any segment containing `*` is treated as potentially overlapping
/// any other segment (e.g. `a*` vs `b*` overlaps), so real conflicts are
/// never missed at the cost of occasional false ones.
pub fn glob_overlap(a: &str, b: &str) -> bool {
    let normalized_a = normalize_glob(a);
    let normalized_b = normalize_glob(b);
    if normalized_a.is_empty() || normalized_b.is_empty() {
        return false;
    }
    let a_segs: Vec<&str> = normalized_a.split('/').collect();
    let b_segs: Vec<&str> = normalized_b.split('/').collect();
    compatible(&a_segs, &b_segs)
}

fn match_segments(pattern: &[&str], path: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((&"**", rest)) => {
            match_segments(rest, path) || (!path.is_empty() && match_segments(pattern, &path[1..]))
        }
        Some((segment, rest)) => match path.split_first() {
            Some((path_segment, path_rest)) => {
                segment_matches(segment, path_segment) && match_segments(rest, path_rest)
            }
            None => false,
        },
    }
}

fn segment_matches(pattern: &str, segment: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let segment: Vec<char> = segment.chars().collect();
    wildcard_match(&pattern, &segment)
}

/// Classic single-segment wildcard match: `*` matches zero or more chars.
fn wildcard_match(pattern: &[char], text: &[char]) -> bool {
    match (pattern.split_first(), text.split_first()) {
        (None, None) => true,
        (Some((&'*', pattern_rest)), _) => {
            wildcard_match(pattern_rest, text)
                || (!text.is_empty() && wildcard_match(pattern, &text[1..]))
        }
        (Some((&c, pattern_rest)), Some((&d, text_rest))) => {
            c == d && wildcard_match(pattern_rest, text_rest)
        }
        _ => false,
    }
}

/// Segment-list compatibility: can both patterns be instantiated to one
/// concrete path?
fn compatible(a: &[&str], b: &[&str]) -> bool {
    match (a.split_first(), b.split_first()) {
        (None, None) => true,
        (Some((&"**", a_rest)), _) => {
            compatible(a_rest, b)
                || b.split_first()
                    .is_some_and(|(_, b_tail)| compatible(a, b_tail))
        }
        (_, Some((&"**", b_rest))) => {
            compatible(a, b_rest)
                || a.split_first()
                    .is_some_and(|(_, a_tail)| compatible(a_tail, b))
        }
        (Some((a_seg, a_rest)), Some((b_seg, b_rest))) => {
            segments_overlap(a_seg, b_seg) && compatible(a_rest, b_rest)
        }
        _ => false,
    }
}

fn segments_overlap(a: &str, b: &str) -> bool {
    a == b || a.contains('*') || b.contains('*')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matching_table_including_double_star() {
        // (pattern, path, expected)
        let table = [
            ("**", "any/thing/at/all.rs", true),
            ("**", "top.md", true),
            ("src/**", "src", true), // ** spans zero segments (documented divergence)
            ("src/**", "src/core/mod.rs", true),
            ("src/**", "docs/readme.md", false),
            ("src/*", "src/mod.rs", true),
            ("src/*", "src/core/mod.rs", false), // * stays within one segment
            ("*.md", "readme.md", true),
            ("*.md", "docs/readme.md", false),
            ("docs/*.md", "docs/readme.md", true),
            ("crates/*/src/**", "crates/policy/src/lib.rs", true),
            ("crates/*/src/**", "crates/policy/tests/lib.rs", false),
            ("src/**/*.rs", "src/deep/nested/mod.rs", true),
            ("src/**/*.rs", "src/deep/nested/mod.txt", false),
            ("a/b", "a/b", true),
            ("a/b", "a/bb", false),
            ("a*b", "aXb", true),
            ("a*b", "aXbY", false),
            ("Cargo.lock", "Cargo.lock", true),
            ("Cargo.lock", "Cargo.toml", false),
        ];
        for (pattern, path, expected) in table {
            assert_eq!(
                glob_matches(pattern, path),
                expected,
                "glob_matches({pattern:?}, {path:?})"
            );
        }
    }

    #[test]
    fn windows_separators_and_dot_prefixes_are_normalized() {
        assert!(glob_matches("src\\**", "src\\core\\mod.rs"));
        assert!(glob_matches("./src/**", "src/x.rs"));
        assert!(glob_matches("src/**", "src\\x.rs"));
    }

    #[test]
    fn empty_patterns_and_paths_match_nothing() {
        assert!(!glob_matches("", "src"));
        assert!(!glob_matches("src/**", ""));
        assert!(!glob_matches("/", "/"));
    }

    #[test]
    fn overlap_is_exact_for_concrete_and_directory_globs() {
        assert!(glob_overlap("src/**", "src/core/mod.rs"));
        assert!(glob_overlap("src/**", "src"));
        assert!(!glob_overlap("src/**", "docs/**"));
        assert!(glob_overlap("**", "x"));
        // conservative: wildcards within a segment may over-report overlap
        assert!(glob_overlap("a*", "b*"));
    }

    #[test]
    fn overlap_is_symmetric() {
        for (a, b) in [
            ("src/**", "docs/**"),
            ("src/core/**", "src/mod.rs"),
            ("**", "x"),
            ("x", "**"),
        ] {
            assert_eq!(glob_overlap(a, b), glob_overlap(b, a), "({a}, {b})");
        }
    }

    #[test]
    fn write_scope_is_independent_of_read_scope() {
        let write_only = PermissionSet {
            write_paths: vec!["src/**".to_owned()],
            ..PermissionSet::deny_all()
        };
        assert!(write_only.can_write("src/main.rs"));
        assert!(
            !write_only.can_read("src/main.rs"),
            "write must not imply read"
        );

        let read_only = PermissionSet {
            read_paths: vec!["src/**".to_owned()],
            ..PermissionSet::deny_all()
        };
        assert!(read_only.can_read("src/main.rs"));
        assert!(
            !read_only.can_write("src/main.rs"),
            "read must not imply write"
        );
    }

    #[test]
    fn deny_all_denies_everything() {
        let set = PermissionSet::deny_all();
        assert!(!set.can_read("any/path"));
        assert!(!set.can_write("any/path"));
        assert!(!set.allows_shell());
        assert!(!set.allows_network("example.com"));
        for action in [
            GitAction::Commit,
            GitAction::Merge,
            GitAction::Rebase,
            GitAction::Push,
        ] {
            assert!(!set.allows_git(action));
        }
        assert!(!set.allows_tool("Read"));
        assert!(!set.allows_tool("Bash"));
    }

    #[test]
    fn worker_presets_encapsulate_their_capabilities() {
        let reader = PermissionSet::worker_read_only();
        assert!(reader.can_read("crates/agentos-policy/src/lib.rs"));
        assert!(!reader.can_write("crates/agentos-policy/src/lib.rs"));
        assert!(!reader.allows_shell());
        assert!(reader.allows_tool("Read"));
        assert!(!reader.allows_tool("Bash"));
        assert!(!reader.allows_tool("Edit"));

        let writer = PermissionSet::worker_write(&["crates/policy/**"]);
        assert!(writer.can_read("crates/policy/src/lib.rs"));
        assert!(writer.can_write("crates/policy/src/lib.rs"));
        assert!(!writer.can_read("crates/other/src/lib.rs"));
        assert!(!writer.can_write("crates/other/src/lib.rs"));
        assert!(writer.allows_shell(), "AskApproval keeps the capability");
        assert_eq!(writer.shell_mode, ShellMode::AskApproval);
        assert!(writer.allows_tool("Bash"));
        assert_eq!(
            writer
                .approval_rule(Gate::PackageInstall)
                .map(|r| r.ttl_secs),
            Some(900)
        );

        let manager = PermissionSet::git_manager();
        for action in [
            GitAction::Commit,
            GitAction::Merge,
            GitAction::Rebase,
            GitAction::Push,
        ] {
            assert!(manager.allows_git(action));
        }
        assert!(manager.allows_network("github.com"));
        assert_eq!(
            manager.approval_rule(Gate::GitPush).map(|r| r.ttl_secs),
            Some(600)
        );
    }

    #[test]
    fn network_policy_profiles() {
        let offline = NetworkPolicy::Offline;
        assert!(!offline.allows("crates.io"));

        let allow = NetworkPolicy::Allowlist(vec!["CRATES.io".to_owned(), "github.com".to_owned()]);
        assert!(allow.allows("crates.io"), "host match is case-insensitive");
        assert!(allow.allows("github.com"));
        assert!(!allow.allows("evil.example"));

        assert!(NetworkPolicy::Unrestricted.allows("anything.example"));
    }

    #[test]
    fn git_action_wire_strings_round_trip() {
        for action in [
            GitAction::Commit,
            GitAction::Merge,
            GitAction::Rebase,
            GitAction::Push,
        ] {
            assert_eq!(GitAction::parse(action.as_str()), Some(action));
        }
        assert_eq!(GitAction::parse("cherry-pick"), None);
    }

    #[test]
    fn overlapping_write_conflict_detects_shared_scope() {
        let a = PermissionSet::worker_write(&["crates/policy/**"]);
        let b = PermissionSet::worker_write(&["crates/policy/src/**"]);
        let c = PermissionSet::worker_write(&["docs/**"]);
        let reader = PermissionSet::worker_read_only();

        assert!(overlapping_write_conflict(&a, &b), "nested scopes overlap");
        assert!(overlapping_write_conflict(&b, &a), "symmetric");
        assert!(!overlapping_write_conflict(&a, &c), "disjoint scopes");
        assert!(
            !overlapping_write_conflict(&a, &reader),
            "no writes = no conflict"
        );
    }

    #[test]
    fn permission_set_serializes_with_prd_camel_case_fields() {
        let set = PermissionSet::worker_write(&["src/**"]);
        let value = serde_json::to_value(&set).expect("serialize");
        for key in [
            "readPaths",
            "writePaths",
            "shellMode",
            "networkPolicy",
            "gitActions",
            "secretScopes",
            "toolAllowlist",
            "approvalRules",
        ] {
            assert!(value.get(key).is_some(), "expected key {key} in {value}");
        }
        assert_eq!(value["shellMode"], serde_json::json!("ask_approval"));
        assert_eq!(value["networkPolicy"], serde_json::json!("offline"));
        assert_eq!(
            value["approvalRules"][0]["gate"],
            serde_json::json!("package_install")
        );

        let round_tripped: PermissionSet =
            serde_json::from_value(serde_json::to_value(&set).unwrap()).unwrap();
        assert_eq!(round_tripped, set);
    }

    #[test]
    fn missing_serde_fields_fail_closed() {
        let parsed: PermissionSet = serde_json::from_value(serde_json::json!({})).expect("parse");
        assert_eq!(parsed, PermissionSet::deny_all());
    }
}
