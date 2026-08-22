//! CTX-04 — the role-specific context pack compiler (PRD §10).
//!
//! Deterministic retrieval first, semantic retrieval second (the semantic
//! stage is a documented seam; nothing here calls a model):
//!
//! 1. files under the request's `allowed_paths` (explicit task scope),
//! 2. source files of the referenced context nodes,
//! 3. summaries of those nodes and of their transitive dependency nodes,
//! 4. decision-ledger refs attached to any of the above (MEM-02: refs
//!    only).
//!
//! Chunks are ranked by an explicit score (base-by-kind + role weight −
//! path depth) and packed greedily under a **strict** token budget — the
//! lowest-ranked chunks are trimmed first. Tokens are approximated as
//! `bytes / 4` (rounded up): a deliberately coarse estimate with no false
//! precision, documented in `docs/F-08-context-system.md`.
//!
//! The output is one ephemeral markdown context file (summaries, then
//! decisions, then file contents behind path headers) materialized into a
//! caller-provided directory — the CTX-04 shape for CLI workers that
//! prefer files — plus a structured [`Manifest`] of included/omitted
//! references.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::file_index::{is_under, language_of, normalize_rel, FileIndex, Language};
use crate::graph::{load_edges, ContextGraph, ContextNode};
use crate::ContextError;

/// Base score for the summary of an explicitly referenced node.
const BASE_REFERENCED_NODE_SUMMARY: i32 = 100;
/// Base score for a decision-ledger ref (CTX-04: prioritize the decision
/// ledger).
const BASE_DECISION_REF: i32 = 90;
/// Base score for a file under an explicit allowed path (CTX-04:
/// "prioritize explicit task paths").
const BASE_SCOPED_FILE: i32 = 85;
/// Base score for a source file of a referenced node.
const BASE_NODE_SOURCE_FILE: i32 = 80;
/// Base score for the summary of a dependency node.
const BASE_DEPENDENCY_SUMMARY: i32 = 60;

/// Worker role the pack is compiled for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Role {
    /// Frontend worker: prefers ts/tsx/css/html paths.
    Frontend,
    /// Backend worker: prefers rs/py/sql/api paths.
    Backend,
    /// Security worker: prefers auth/crypto/secret/token/permission paths.
    Security,
    /// QA worker: prefers test/spec paths.
    Qa,
    /// No role preference.
    General,
}

impl Role {
    /// Canonical wire string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Frontend => "frontend",
            Role::Backend => "backend",
            Role::Security => "security",
            Role::Qa => "qa",
            Role::General => "general",
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A compile request (CTX-04 "compile role-specific packs").
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackRequest {
    /// Role whose weighting ranks the chunks.
    pub role: Role,
    /// Repository-relative directories/files in explicit task scope.
    pub allowed_paths: Vec<String>,
    /// Referenced context node ids.
    pub context_refs: Vec<String>,
    /// Hard ceiling on approximate tokens (`bytes / 4`).
    pub token_budget: u64,
}

/// What a manifest entry points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PackItemKind {
    /// A repository file, included by content.
    File,
    /// A context node summary.
    NodeSummary,
    /// A decision-ledger reference (MEM-02; ref only).
    Decision,
}

impl PackItemKind {
    /// Canonical wire string.
    pub fn as_str(&self) -> &'static str {
        match self {
            PackItemKind::File => "file",
            PackItemKind::NodeSummary => "node-summary",
            PackItemKind::Decision => "decision",
        }
    }
}

/// One included or omitted reference in the manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackRef {
    /// File path, node id, or decision id.
    pub target: String,
    /// What kind of chunk it is.
    pub kind: PackItemKind,
    /// Approximate tokens (`bytes / 4`) of the chunk.
    pub est_tokens: u64,
    /// Why the chunk was omitted (`"budget"`, `"unknown-node"`,
    /// `"missing-file"`); `None` when included.
    pub reason: Option<String>,
}

/// The pack manifest: what went in, what was left out, and the strict
/// token accounting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    /// Role the pack was compiled for.
    pub role: Role,
    /// Requested ceiling.
    pub token_budget: u64,
    /// Sum of included chunk approximations; always `<= token_budget`.
    pub est_tokens: u64,
    /// Included references, highest-ranked first.
    pub included: Vec<PackRef>,
    /// Omitted references with reasons, in rank order.
    pub omitted: Vec<PackRef>,
}

/// A compiled context pack: manifest + the materialized ephemeral file.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextPack {
    /// Included/omitted references and token accounting.
    pub manifest: Manifest,
    /// Path of the materialized markdown context file (inside the
    /// caller-provided output directory).
    pub materialized_path: PathBuf,
}

/// One rankable chunk before budgeting.
struct Candidate {
    target: String,
    kind: PackItemKind,
    score: i32,
    /// Final bytes (file body or rendered summary).
    bytes: Vec<u8>,
}

/// CTX-04 compiler over the shared context database and repo root.
#[derive(Debug)]
pub struct ContextCompiler<'a> {
    conn: &'a Connection,
    root: &'a Path,
}

impl<'a> ContextCompiler<'a> {
    /// Bind a compiler view to an open context connection and repo root.
    pub fn new(conn: &'a Connection, root: &'a Path) -> Self {
        Self { conn, root }
    }

    /// Compile a role-specific pack for `req`, materializing the context
    /// file into `output_dir` (created if needed).
    pub fn compile(
        &self,
        req: &PackRequest,
        output_dir: &Path,
    ) -> Result<ContextPack, ContextError> {
        let graph = ContextGraph::new(self.conn);
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut omitted: Vec<PackRef> = Vec::new();

        // Referenced nodes: summary + source files + decisions.
        let mut referenced: Vec<ContextNode> = Vec::new();
        for node_id in &req.context_refs {
            match graph.get(node_id)? {
                None => omitted.push(PackRef {
                    target: node_id.clone(),
                    kind: PackItemKind::NodeSummary,
                    est_tokens: 0,
                    reason: Some("unknown-node".to_owned()),
                }),
                Some(node) => {
                    referenced.push(node);
                }
            }
        }
        for node in &referenced {
            candidates.push(Candidate {
                target: node.id.clone(),
                kind: PackItemKind::NodeSummary,
                score: BASE_REFERENCED_NODE_SUMMARY,
                bytes: render_node_summary(node).into_bytes(),
            });
            candidates.extend(node.source_files.iter().map(|file| Candidate {
                target: file.clone(),
                kind: PackItemKind::File,
                score: file_score(file, BASE_NODE_SOURCE_FILE, req.role),
                bytes: Vec::new(), // filled during resolution below
            }));
            candidates.extend(node.decisions.iter().map(|decision| Candidate {
                target: decision.clone(),
                kind: PackItemKind::Decision,
                score: BASE_DECISION_REF,
                bytes: render_decision_ref(decision).into_bytes(),
            }));
        }

        // Transitive dependency closure: summaries + decisions (not their
        // full sources — CTX-04 asks for dependency *summaries*).
        let mut dep_ids: BTreeSet<String> = BTreeSet::new();
        for node in &referenced {
            collect_dependency_closure(self.conn, &node.id, &mut dep_ids)?;
        }
        for node in &referenced {
            dep_ids.remove(&node.id);
        }
        for dep_id in &dep_ids {
            if let Some(node) = graph.get(dep_id)? {
                candidates.push(Candidate {
                    target: node.id.clone(),
                    kind: PackItemKind::NodeSummary,
                    score: BASE_DEPENDENCY_SUMMARY,
                    bytes: render_node_summary(&node).into_bytes(),
                });
                candidates.extend(node.decisions.iter().map(|decision| Candidate {
                    target: decision.clone(),
                    kind: PackItemKind::Decision,
                    score: BASE_DECISION_REF,
                    bytes: render_decision_ref(decision).into_bytes(),
                }));
            }
        }

        // Scoped files come from the CTX-01 index (deterministic reuse of
        // the cache: no ad-hoc tree walk here).
        for entry in FileIndex::new(self.conn, self.root).entries()? {
            let scoped = req
                .allowed_paths
                .iter()
                .any(|allowed| is_under(&entry.path, &normalize_rel(allowed)));
            if scoped {
                candidates.push(Candidate {
                    target: entry.path.clone(),
                    kind: PackItemKind::File,
                    score: file_score(&entry.path, BASE_SCOPED_FILE, req.role),
                    bytes: Vec::new(),
                });
            }
        }

        // Deduplicate (kind, target), keeping the highest score.
        candidates.sort_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| a.target.cmp(&b.target))
                .then_with(|| kind_rank(a.kind).cmp(&kind_rank(b.kind)))
        });
        candidates.dedup_by(|a, b| a.kind == b.kind && a.target == b.target);

        // Resolve file bodies from disk; missing files drop to omitted.
        let mut resolved: Vec<Candidate> = Vec::new();
        for candidate in candidates {
            if candidate.kind == PackItemKind::File && candidate.bytes.is_empty() {
                let abs = self.root.join(&candidate.target);
                match fs::read(&abs) {
                    Ok(bytes) => resolved.push(Candidate { bytes, ..candidate }),
                    Err(err) => {
                        tracing::warn!(file = %candidate.target, error = %err, "pack source missing");
                        omitted.push(PackRef {
                            target: candidate.target,
                            kind: candidate.kind,
                            est_tokens: 0,
                            reason: Some("missing-file".to_owned()),
                        });
                    }
                }
            } else {
                resolved.push(candidate);
            }
        }

        // Strict budget: greedy in rank order; lowest-ranked trimmed first.
        let mut included: Vec<Candidate> = Vec::new();
        let mut running: u64 = 0;
        for candidate in resolved {
            let est = approx_tokens(&candidate.bytes);
            if running + est <= req.token_budget {
                running += est;
                included.push(candidate);
            } else {
                omitted.push(PackRef {
                    target: candidate.target,
                    kind: candidate.kind,
                    est_tokens: est,
                    reason: Some("budget".to_owned()),
                });
            }
        }
        debug_assert!(
            running <= req.token_budget,
            "strict budget invariant violated"
        );

        let manifest = Manifest {
            role: req.role,
            token_budget: req.token_budget,
            est_tokens: running,
            included: included
                .iter()
                .map(|c| PackRef {
                    target: c.target.clone(),
                    kind: c.kind,
                    est_tokens: approx_tokens(&c.bytes),
                    reason: None,
                })
                .collect(),
            omitted,
        };

        fs::create_dir_all(output_dir)?;
        let materialized_path = output_dir.join(format!("context-pack-{}.md", Uuid::now_v7()));
        fs::write(&materialized_path, render_markdown(&manifest, &included))?;

        Ok(ContextPack {
            manifest,
            materialized_path,
        })
    }
}

/// Role-weighted score of a file chunk: base + role bonus − path depth.
/// The weight tables are explicit (and mirrored in
/// `docs/F-08-context-system.md`); they are heuristics, not canon.
fn file_score(path: &str, base: i32, role: Role) -> i32 {
    let depth = path.matches('/').count() as i32;
    base + role_bonus(role, path) - depth
}

/// Explicit per-role weight tables over the path's language and keywords.
fn role_bonus(role: Role, path: &str) -> i32 {
    let lang = language_of(path);
    let lower = path.to_ascii_lowercase();
    match role {
        Role::Frontend => match lang {
            Language::Tsx => 10,
            Language::TypeScript => 8,
            Language::Css => 6,
            Language::Html => 4,
            Language::Markdown | Language::Json => 2,
            Language::Rust | Language::Python | Language::Sql => -3,
            _ => 0,
        },
        Role::Backend => match lang {
            Language::Rust => 8,
            Language::Python | Language::Sql => 6,
            Language::Toml => 2,
            Language::Json => 1,
            Language::TypeScript | Language::Tsx => -2,
            Language::Css => -4,
            Language::Html => -2,
            _ => 0,
        },
        Role::Security => {
            if [
                "auth",
                "crypto",
                "secret",
                "token",
                "permission",
                "session",
                "key",
            ]
            .iter()
            .any(|keyword| lower.contains(keyword))
            {
                8
            } else {
                match lang {
                    Language::Rust | Language::Python => 2,
                    Language::TypeScript | Language::Tsx => 1,
                    Language::Sql => 1,
                    Language::Markdown => -2,
                    Language::Css => -4,
                    _ => 0,
                }
            }
        }
        Role::Qa => {
            if lower.contains("test") || lower.contains("spec") {
                8
            } else {
                match lang {
                    Language::Rust | Language::TypeScript | Language::Tsx | Language::Python => 1,
                    Language::Css => -3,
                    _ => 0,
                }
            }
        }
        Role::General => 0,
    }
}

/// Deterministic tie-break order for manifest output.
fn kind_rank(kind: PackItemKind) -> i32 {
    match kind {
        PackItemKind::NodeSummary => 0,
        PackItemKind::Decision => 1,
        PackItemKind::File => 2,
    }
}

/// Approximate token count: `bytes / 4`, rounded up. Deliberately coarse —
/// see the module docs and the F-08 doc before relying on it.
fn approx_tokens(bytes: &[u8]) -> u64 {
    bytes.len().div_ceil(4) as u64
}

/// Collect the transitive dependency closure of `start` into `out`
/// (excluding `start` itself). Cycle-safe.
fn collect_dependency_closure(
    conn: &Connection,
    start: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), ContextError> {
    let edges = load_edges(conn)?;
    let mut forward: std::collections::HashMap<&str, Vec<&str>> = std::collections::HashMap::new();
    for (node, dep) in &edges {
        forward.entry(node.as_str()).or_default().push(dep.as_str());
    }
    let mut queue: Vec<&str> = vec![start];
    while let Some(current) = queue.pop() {
        for dep in forward.get(current).into_iter().flatten() {
            if out.insert((*dep).to_owned()) {
                queue.push(dep);
            }
        }
    }
    out.remove(start);
    Ok(())
}

/// Markdown rendering of one node summary chunk (with provenance so
/// workers can see freshness — CTX-05).
fn render_node_summary(node: &ContextNode) -> String {
    let mut rendered = format!(
        "### {} — {} (v{}, {})\n{}\n",
        node.id, node.topic, node.version, node.invalidation_state, node.summary
    );
    if !node.source_files.is_empty() {
        rendered.push_str(&format!("Sources: {}\n", node.source_files.join(", ")));
    }
    if !node.dependencies.is_empty() {
        rendered.push_str(&format!("Depends on: {}\n", node.dependencies.join(", ")));
    }
    if !node.decisions.is_empty() {
        rendered.push_str(&format!("Decisions: {}\n", node.decisions.join(", ")));
    }
    rendered
}

/// Markdown rendering of one decision ref line.
fn render_decision_ref(decision: &str) -> String {
    format!("- {decision} (see decision ledger)\n")
}

/// Markdown fence info string for a language.
fn fence_tag(language: &Language) -> &str {
    match language {
        Language::Rust => "rust",
        Language::TypeScript => "typescript",
        Language::Tsx => "tsx",
        Language::JavaScript => "javascript",
        Language::Python => "python",
        Language::Markdown => "markdown",
        Language::Toml => "toml",
        Language::Json => "json",
        Language::Yaml => "yaml",
        Language::Sql => "sql",
        Language::Css => "css",
        Language::Html => "html",
        Language::Shell => "bash",
        Language::Other(_) => "text",
    }
}

/// Assemble the materialized markdown context file.
///
/// Four-backtick fences are used so file contents containing triple
/// backticks cannot break the structure.
fn render_markdown(manifest: &Manifest, included: &[Candidate]) -> String {
    let mut md = String::new();
    md.push_str("# AgentOS context pack\n\n");
    md.push_str(&format!("- role: {}\n", manifest.role));
    md.push_str(&format!(
        "- token budget: {} (approximate tokens, bytes/4)\n",
        manifest.token_budget
    ));
    md.push_str(&format!(
        "- estimated tokens: {} ({} included / {} omitted refs)\n",
        manifest.est_tokens,
        manifest.included.len(),
        manifest.omitted.len()
    ));
    if !manifest.omitted.is_empty() {
        md.push_str("\n## Omitted\n\n");
        for reference in &manifest.omitted {
            md.push_str(&format!(
                "- `{}` ({}, ~{} tok) — {}\n",
                reference.target,
                reference.kind.as_str(),
                reference.est_tokens,
                reference.reason.as_deref().unwrap_or("unknown"),
            ));
        }
    }

    let summaries: Vec<&Candidate> = included
        .iter()
        .filter(|c| c.kind == PackItemKind::NodeSummary)
        .collect();
    if !summaries.is_empty() {
        md.push_str("\n## Context nodes\n\n");
        for candidate in summaries {
            md.push_str(&String::from_utf8_lossy(&candidate.bytes));
            md.push('\n');
        }
    }

    let decisions: Vec<&Candidate> = included
        .iter()
        .filter(|c| c.kind == PackItemKind::Decision)
        .collect();
    if !decisions.is_empty() {
        md.push_str("\n## Decisions\n\n");
        for candidate in decisions {
            md.push_str(String::from_utf8_lossy(&candidate.bytes).trim_end());
            md.push('\n');
        }
    }

    let files: Vec<&Candidate> = included
        .iter()
        .filter(|c| c.kind == PackItemKind::File)
        .collect();
    if !files.is_empty() {
        md.push_str("\n## Files\n\n");
        for candidate in files {
            let language = language_of(&candidate.target);
            md.push_str(&format!("### `{}`\n\n", candidate.target));
            md.push_str(&format!("````{}\n", fence_tag(&language)));
            md.push_str(&String::from_utf8_lossy(&candidate.bytes));
            if !candidate.bytes.ends_with(b"\n") {
                md.push('\n');
            }
            md.push_str("````\n\n");
        }
    }

    md
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_index::ScanOptions;
    use crate::invalidation::InvalidationState;
    use crate::{ContextNode, ContextStore};
    use std::collections::HashMap;

    fn store_in(dir: &Path) -> ContextStore {
        ContextStore::open(dir, &dir.join(".agentos").join("ctx.sqlite3")).expect("open store")
    }

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("mkdir");
        }
        fs::write(path, contents).expect("write");
    }

    fn node(
        id: &str,
        summary: &str,
        files: Vec<&str>,
        deps: Vec<&str>,
        decisions: Vec<&str>,
    ) -> ContextNode {
        ContextNode {
            id: id.to_owned(),
            topic: format!("topic-{id}"),
            version: 1,
            summary: summary.to_owned(),
            source_files: files.iter().map(|f| (*f).to_owned()).collect(),
            source_hashes: files
                .iter()
                .map(|f| ((*f).to_owned(), format!("hash-{f}")))
                .collect(),
            symbols: vec![],
            dependencies: deps.into_iter().map(str::to_owned).collect(),
            decisions: decisions.into_iter().map(str::to_owned).collect(),
            invalidation_state: InvalidationState::Clean,
        }
    }

    fn fixture(dir: &Path) -> ContextStore {
        // Two equal-size competing files (role weighting must decide) plus
        // a node graph: app (sources src/main.rs) -> db; decision DEC-1.
        write(&dir.join("src").join("main.rs"), &"a".repeat(200));
        write(&dir.join("web").join("app.tsx"), &"b".repeat(200));
        let store = store_in(dir);
        store
            .file_index()
            .scan(&ScanOptions::default())
            .expect("scan");
        let main_hash = store
            .file_index()
            .entry("src/main.rs")
            .expect("e")
            .expect("p")
            .content_hash;
        let mut app = node(
            "app",
            "Application shell summary.",
            vec!["src/main.rs"],
            vec!["db"],
            vec!["DEC-1"],
        );
        app.source_hashes = [("src/main.rs".to_owned(), main_hash)]
            .into_iter()
            .collect();
        store
            .graph()
            .upsert_node(&node("db", "Database summary.", vec![], vec![], vec![]))
            .expect("db");
        store.graph().upsert_node(&app).expect("app");
        store
    }

    #[test]
    fn compile_under_budget_includes_everything_and_materializes_well_formed_markdown() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = fixture(dir.path());

        let out = dir.path().join("packs");
        let pack = store
            .compiler()
            .compile(
                &PackRequest {
                    role: Role::General,
                    allowed_paths: vec!["src".to_owned(), "web".to_owned()],
                    context_refs: vec!["app".to_owned()],
                    token_budget: 1_000,
                },
                &out,
            )
            .expect("compile");

        assert!(pack.manifest.est_tokens <= 1_000);
        assert!(pack.manifest.omitted.is_empty());
        // Referenced summary, dependency summary, decision ref, both files.
        let targets: Vec<(&str, PackItemKind)> = pack
            .manifest
            .included
            .iter()
            .map(|r| (r.target.as_str(), r.kind))
            .collect();
        for expected in [
            ("app", PackItemKind::NodeSummary),
            ("DEC-1", PackItemKind::Decision),
            ("db", PackItemKind::NodeSummary),
            ("src/main.rs", PackItemKind::File),
            ("web/app.tsx", PackItemKind::File),
        ] {
            assert!(
                targets.contains(&expected),
                "missing {expected:?} in {targets:?}"
            );
        }

        assert!(pack.materialized_path.is_file());
        let md = fs::read_to_string(&pack.materialized_path).expect("read pack");
        assert!(md.starts_with("# AgentOS context pack\n"));
        assert!(md.contains("role: general"));
        assert!(md.contains("## Context nodes"));
        assert!(md.contains("### app — topic-app (v1, clean)"));
        assert!(md.contains("Application shell summary."));
        assert!(md.contains("## Decisions"));
        assert!(md.contains("- DEC-1 (see decision ledger)"));
        assert!(md.contains("## Files"));
        assert!(md.contains("### `src/main.rs`"));
        assert!(md.contains("````rust"));
        assert!(md.contains(&"a".repeat(200)));
        // Estimated tokens match the sum of the manifest entries.
        let summed: u64 = pack.manifest.included.iter().map(|r| r.est_tokens).sum();
        assert_eq!(summed, pack.manifest.est_tokens);
    }

    #[test]
    fn budget_is_enforced_strictly_and_trims_lowest_ranked_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = fixture(dir.path());

        // app summary alone is ~ (55 bytes)/4 = 14 tokens; give only room
        // for the two summaries and the decision — both 200-byte files
        // (50 tok each) must drop out for budget reasons.
        let out = dir.path().join("packs");
        let pack = store
            .compiler()
            .compile(
                &PackRequest {
                    role: Role::General,
                    allowed_paths: vec!["src".to_owned(), "web".to_owned()],
                    context_refs: vec!["app".to_owned()],
                    token_budget: 40,
                },
                &out,
            )
            .expect("compile");

        assert!(pack.manifest.est_tokens <= 40, "strict budget violated");
        let omitted_targets: Vec<&str> = pack
            .manifest
            .omitted
            .iter()
            .map(|r| r.target.as_str())
            .collect();
        assert!(omitted_targets.contains(&"src/main.rs"));
        assert!(omitted_targets.contains(&"web/app.tsx"));
        assert!(pack
            .manifest
            .omitted
            .iter()
            .all(|r| r.reason.as_deref() == Some("budget")));
        assert!(pack
            .manifest
            .included
            .iter()
            .all(|r| r.kind != PackItemKind::File));
        // Still materialized (header + summaries only).
        let md = fs::read_to_string(&pack.materialized_path).expect("read pack");
        assert!(md.contains("## Omitted"));
        assert!(!md.contains("## Files"));
    }

    #[test]
    fn role_weighting_decides_between_equal_sized_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = fixture(dir.path());
        let out = dir.path().join("packs");

        // Budget for exactly one 200-byte file (50 tokens).
        let request = |role: Role| PackRequest {
            role,
            allowed_paths: vec!["src".to_owned(), "web".to_owned()],
            context_refs: vec![],
            token_budget: 50,
        };

        let frontend = store
            .compiler()
            .compile(&request(Role::Frontend), &out)
            .expect("frontend pack");
        let frontend_files: Vec<&str> = frontend
            .manifest
            .included
            .iter()
            .filter(|r| r.kind == PackItemKind::File)
            .map(|r| r.target.as_str())
            .collect();
        assert_eq!(frontend_files, vec!["web/app.tsx"], "frontend prefers tsx");

        let backend = store
            .compiler()
            .compile(&request(Role::Backend), &out)
            .expect("backend pack");
        let backend_files: Vec<&str> = backend
            .manifest
            .included
            .iter()
            .filter(|r| r.kind == PackItemKind::File)
            .map(|r| r.target.as_str())
            .collect();
        assert_eq!(backend_files, vec!["src/main.rs"], "backend prefers rust");
    }

    #[test]
    fn unknown_nodes_and_missing_files_are_reported_not_hidden() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = fixture(dir.path());

        // Referenced node whose source file has since vanished, plus an
        // unknown node id.
        fs::remove_file(dir.path().join("src").join("main.rs")).expect("remove");
        let out = dir.path().join("packs");
        let pack = store
            .compiler()
            .compile(
                &PackRequest {
                    role: Role::General,
                    allowed_paths: vec![],
                    context_refs: vec!["app".to_owned(), "ghost".to_owned()],
                    token_budget: 1_000,
                },
                &out,
            )
            .expect("compile");

        let omitted: HashMap<&str, &str> = pack
            .manifest
            .omitted
            .iter()
            .map(|r| (r.target.as_str(), r.reason.as_deref().unwrap_or("")))
            .collect();
        assert_eq!(omitted.get("ghost"), Some(&"unknown-node"));
        assert_eq!(omitted.get("src/main.rs"), Some(&"missing-file"));
        // The summaries themselves still ship.
        assert!(pack
            .manifest
            .included
            .iter()
            .any(|r| r.target == "app" && r.kind == PackItemKind::NodeSummary));
    }
}
