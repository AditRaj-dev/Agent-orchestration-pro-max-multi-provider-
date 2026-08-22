//! CTX-02 — the context graph (PRD §10).
//!
//! Context nodes are the unit of compiled shared knowledge: a topic, a
//! version, a summary, the source files (with content hashes) the node was
//! compiled from, extracted symbols, dependencies on other nodes, and
//! decision-ledger refs (MEM-02 stores refs only — the ledger itself lives
//! elsewhere). Everything is persisted in normalized tables
//! (`context_nodes` + `node_*`) in the shared context database.
//!
//! Provenance is mandatory: [`ContextGraph::upsert_node`] rejects any
//! sourced node whose source files lack a matching non-empty content hash
//! (and any hash without a matching source file), so no summary can ever
//! lose its link to the bytes it was derived from.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::db::{map_sqlite, now_ts};
use crate::file_index::Language;
use crate::invalidation::InvalidationState;
use crate::ContextError;

/// One context node (PRD CTX-02 "Key state").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextNode {
    /// Stable node identifier (curator-assigned).
    pub id: String,
    /// Human topic, e.g. "authentication", "api-contracts".
    pub topic: String,
    /// Monotonic compile version; bumped by the invalidation engine's
    /// `mark_clean`.
    pub version: u64,
    /// Compiled summary text.
    pub summary: String,
    /// Repository-relative source paths this node was compiled from.
    pub source_files: Vec<String>,
    /// Provenance: content hash per source file (validated non-empty).
    pub source_hashes: HashMap<String, String>,
    /// Symbols extracted from the sources (see [`symbols_in_source`]).
    pub symbols: Vec<String>,
    /// Ids of nodes this node depends on (edges: this -> depends-on).
    pub dependencies: Vec<String>,
    /// Decision ledger refs (MEM-02); refs only.
    pub decisions: Vec<String>,
    /// CTX-05 dirty state.
    pub invalidation_state: InvalidationState,
}

/// CTX-02 context graph store over the shared context database.
#[derive(Debug)]
pub struct ContextGraph<'a> {
    conn: &'a Connection,
}

impl<'a> ContextGraph<'a> {
    /// Bind a graph view to an open context connection.
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    /// Insert or replace a node (full replace of row + child tables, in one
    /// explicit transaction). Validates provenance first: every source file
    /// must carry a non-empty hash and vice versa; dependencies must exist
    /// and must not include the node itself.
    pub fn upsert_node(&self, node: &ContextNode) -> Result<(), ContextError> {
        validate_node(node)?;

        let tx = self.conn.unchecked_transaction().map_err(map_sqlite)?;
        for dep in &node.dependencies {
            let known: bool = tx
                .query_row(
                    "SELECT 1 FROM context_nodes WHERE id = ?1",
                    params![dep],
                    |_| Ok(true),
                )
                .optional()
                .map_err(map_sqlite)?
                .unwrap_or(false);
            if !known {
                return Err(ContextError::NotFound(format!(
                    "dependency {dep} of node {} does not exist",
                    node.id
                )));
            }
        }

        let now = now_ts();
        tx.execute(
            "INSERT INTO context_nodes (id, topic, version, summary, invalidation_state,
                                        created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
             ON CONFLICT(id) DO UPDATE SET
                topic = excluded.topic,
                version = excluded.version,
                summary = excluded.summary,
                invalidation_state = excluded.invalidation_state,
                updated_at = excluded.updated_at",
            params![
                node.id,
                node.topic,
                i64::try_from(node.version).unwrap_or(i64::MAX),
                node.summary,
                node.invalidation_state.as_str(),
                now,
            ],
        )
        .map_err(map_sqlite)?;

        tx.execute(
            "DELETE FROM node_source_files WHERE node_id = ?1",
            params![node.id],
        )
        .map_err(map_sqlite)?;
        tx.execute(
            "DELETE FROM node_symbols WHERE node_id = ?1",
            params![node.id],
        )
        .map_err(map_sqlite)?;
        tx.execute(
            "DELETE FROM node_dependencies WHERE node_id = ?1",
            params![node.id],
        )
        .map_err(map_sqlite)?;
        tx.execute(
            "DELETE FROM node_decisions WHERE node_id = ?1",
            params![node.id],
        )
        .map_err(map_sqlite)?;

        for file in &node.source_files {
            let hash = &node.source_hashes[file];
            tx.execute(
                "INSERT OR REPLACE INTO node_source_files (node_id, path, hash) VALUES (?1, ?2, ?3)",
                params![node.id, file, hash],
            )
            .map_err(map_sqlite)?;
        }
        for symbol in &node.symbols {
            tx.execute(
                "INSERT OR REPLACE INTO node_symbols (node_id, symbol) VALUES (?1, ?2)",
                params![node.id, symbol],
            )
            .map_err(map_sqlite)?;
        }
        for dep in &node.dependencies {
            tx.execute(
                "INSERT OR REPLACE INTO node_dependencies (node_id, depends_on) VALUES (?1, ?2)",
                params![node.id, dep],
            )
            .map_err(map_sqlite)?;
        }
        for decision in &node.decisions {
            tx.execute(
                "INSERT OR REPLACE INTO node_decisions (node_id, decision_ref) VALUES (?1, ?2)",
                params![node.id, decision],
            )
            .map_err(map_sqlite)?;
        }

        tx.commit().map_err(map_sqlite)?;
        Ok(())
    }

    /// Fetch one node by id, including all child rows.
    pub fn get(&self, id: &str) -> Result<Option<ContextNode>, ContextError> {
        let base = self
            .conn
            .query_row(
                "SELECT id, topic, version, summary, invalidation_state
                 FROM context_nodes WHERE id = ?1",
                params![id],
                base_from_row,
            )
            .optional()
            .map_err(map_sqlite)?;
        match base {
            None => Ok(None),
            Some(mut node) => {
                attach_children(self.conn, &mut node)?;
                Ok(Some(node))
            }
        }
    }

    /// Delete a node and its child rows (one transaction). Missing id is a
    /// [`ContextError::NotFound`].
    pub fn delete(&self, id: &str) -> Result<(), ContextError> {
        let tx = self.conn.unchecked_transaction().map_err(map_sqlite)?;
        let removed = tx
            .execute("DELETE FROM context_nodes WHERE id = ?1", params![id])
            .map_err(map_sqlite)?;
        if removed == 0 {
            return Err(ContextError::NotFound(format!(
                "context node {id} not found"
            )));
        }
        for table in [
            "node_source_files",
            "node_symbols",
            "node_dependencies",
            "node_decisions",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE node_id = ?1"),
                params![id],
            )
            .map_err(map_sqlite)?;
        }
        tx.commit().map_err(map_sqlite)?;
        Ok(())
    }

    /// All nodes with the given topic, including child rows.
    pub fn by_topic(&self, topic: &str) -> Result<Vec<ContextNode>, ContextError> {
        self.fetch_nodes(
            "SELECT id, topic, version, summary, invalidation_state
             FROM context_nodes WHERE topic = ?1 ORDER BY id",
            params![topic],
        )
    }

    /// Every node, ordered by id (deterministic), including child rows.
    pub fn all(&self) -> Result<Vec<ContextNode>, ContextError> {
        self.fetch_nodes(
            "SELECT id, topic, version, summary, invalidation_state
             FROM context_nodes ORDER BY id",
            [],
        )
    }

    /// Transitive closure of nodes that (directly or indirectly) depend on
    /// `id`, sorted. Cycle-safe.
    pub fn dependents_of(&self, id: &str) -> Result<Vec<String>, ContextError> {
        let edges = load_edges(self.conn)?;
        let mut reverse: HashMap<&str, Vec<&str>> = HashMap::new();
        for (node, dep) in &edges {
            reverse.entry(dep.as_str()).or_default().push(node.as_str());
        }
        let mut visited: Vec<String> = Vec::new();
        let mut queue: Vec<&str> = vec![id];
        while let Some(current) = queue.pop() {
            for dependent in reverse.get(current).into_iter().flatten() {
                if !visited.iter().any(|seen| seen == dependent) && *dependent != id {
                    visited.push((*dependent).to_owned());
                    queue.push(dependent);
                }
            }
        }
        visited.sort();
        Ok(visited)
    }

    /// Run `sql`, build base nodes, attach child rows to each.
    fn fetch_nodes<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<Vec<ContextNode>, ContextError> {
        let mut stmt = self.conn.prepare(sql).map_err(map_sqlite)?;
        let bases = stmt
            .query_map(params, base_from_row)
            .map_err(map_sqlite)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite)?;
        let mut nodes = Vec::with_capacity(bases.len());
        for mut node in bases {
            attach_children(self.conn, &mut node)?;
            nodes.push(node);
        }
        Ok(nodes)
    }
}

/// Base row mapper: the `context_nodes` columns only, children left empty.
fn base_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContextNode> {
    Ok(ContextNode {
        id: row.get(0)?,
        topic: row.get(1)?,
        version: u64::try_from(row.get::<_, i64>(2)?).unwrap_or_default(),
        summary: row.get(3)?,
        invalidation_state: InvalidationState::parse(&row.get::<_, String>(4)?)
            .unwrap_or(InvalidationState::NeedsReview),
        source_files: vec![],
        source_hashes: HashMap::new(),
        symbols: vec![],
        dependencies: vec![],
        decisions: vec![],
    })
}

/// Attach the four child tables (`node_*`) to a base node.
fn attach_children(conn: &Connection, node: &mut ContextNode) -> Result<(), ContextError> {
    let mut sources = conn
        .prepare("SELECT path, hash FROM node_source_files WHERE node_id = ?1 ORDER BY path")
        .map_err(map_sqlite)?;
    let pairs = sources
        .query_map(params![node.id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(map_sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite)?;
    node.source_files = pairs.iter().map(|(p, _)| p.clone()).collect();
    node.source_hashes = pairs.into_iter().collect();

    let mut symbols = conn
        .prepare("SELECT symbol FROM node_symbols WHERE node_id = ?1 ORDER BY symbol")
        .map_err(map_sqlite)?;
    node.symbols = symbols
        .query_map(params![node.id], |row| row.get(0))
        .map_err(map_sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite)?;

    let mut deps = conn
        .prepare("SELECT depends_on FROM node_dependencies WHERE node_id = ?1 ORDER BY depends_on")
        .map_err(map_sqlite)?;
    node.dependencies = deps
        .query_map(params![node.id], |row| row.get(0))
        .map_err(map_sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite)?;

    let mut decisions = conn
        .prepare("SELECT decision_ref FROM node_decisions WHERE node_id = ?1 ORDER BY decision_ref")
        .map_err(map_sqlite)?;
    node.decisions = decisions
        .query_map(params![node.id], |row| row.get(0))
        .map_err(map_sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite)?;
    Ok(())
}

/// Load all dependency edges as (dependent, depends-on) pairs.
pub(crate) fn load_edges(conn: &Connection) -> Result<Vec<(String, String)>, ContextError> {
    let mut stmt = conn
        .prepare("SELECT node_id, depends_on FROM node_dependencies")
        .map_err(map_sqlite)?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(map_sqlite)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(map_sqlite)
}

/// Provenance + shape validation shared by all node writes (CTX-02).
fn validate_node(node: &ContextNode) -> Result<(), ContextError> {
    if node.id.trim().is_empty() {
        return Err(ContextError::Validation(
            "context node id must not be empty".to_owned(),
        ));
    }
    if node.topic.trim().is_empty() {
        return Err(ContextError::Validation(
            "context node topic must not be empty".to_owned(),
        ));
    }
    if node.version == 0 {
        return Err(ContextError::Validation(
            "context node version must be >= 1".to_owned(),
        ));
    }
    for file in &node.source_files {
        let proven = matches!(
            node.source_hashes.get(file),
            Some(hash) if !hash.trim().is_empty()
        );
        if !proven {
            return Err(ContextError::Validation(format!(
                "source file {file} of node {} lacks a non-empty content hash \
                 (provenance is mandatory for sourced nodes)",
                node.id
            )));
        }
    }
    let orphan_hashes: Vec<&String> = node
        .source_hashes
        .keys()
        .filter(|path| !node.source_files.contains(path))
        .collect();
    if !orphan_hashes.is_empty() {
        let listed = orphan_hashes
            .iter()
            .map(|path| path.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ContextError::Validation(format!(
            "node {} carries hashes without matching source files: {listed}",
            node.id
        )));
    }
    if node.dependencies.iter().any(|dep| dep == &node.id) {
        return Err(ContextError::Validation(format!(
            "node {} cannot depend on itself",
            node.id
        )));
    }
    Ok(())
}

/// Stub symbol extraction over source text (documented seam: replaceable by
/// tree-sitter per F-00 §1 "Parsing"; the line-scanner below is only a
/// deterministic first pass).
///
/// Recognizes, per language:
///
/// - Rust: `fn`, `pub fn`, `struct`, `enum`, `trait`, `mod` items
///   (visibility and `pub(crate)` prefixes stripped);
/// - TypeScript/TSX/JavaScript: `function`, `const`, `class`, `interface`,
///   `type` items (`export`/`declare`/`async` prefixes stripped);
/// - Python: `def` and `class` items (`async` prefix stripped).
///
/// Duplicate names are deduplicated in first-seen order.
pub fn symbols_in_source(source: &str, language: &Language) -> Vec<String> {
    let mut symbols: Vec<String> = Vec::new();
    for line in source.lines() {
        let trimmed = line.trim_start();
        let name = match language {
            Language::Rust => rust_item_name(trimmed),
            Language::TypeScript | Language::Tsx | Language::JavaScript => js_item_name(trimmed),
            Language::Python => python_item_name(trimmed),
            _ => None,
        };
        if let Some(name) = name {
            if !symbols.contains(&name) {
                symbols.push(name);
            }
        }
    }
    symbols
}

/// [`symbols_in_source`] for a file on disk, read as UTF-8 (lossy for
/// non-UTF-8 bytes).
pub fn extract_symbols(path: &Path, language: &Language) -> Result<Vec<String>, ContextError> {
    let source = fs::read_to_string(path)?;
    Ok(symbols_in_source(&source, language))
}

/// Repeatedly strip any of `prefixes` from the head of `line`.
fn strip_leading<'s>(line: &'s str, prefixes: &[&str]) -> &'s str {
    let mut rest = line;
    loop {
        let stripped = prefixes.iter().find_map(|p| rest.strip_prefix(p));
        match stripped {
            Some(next) => rest = next,
            None => return rest,
        }
    }
}

/// Leading alphanumeric/underscore run of `s` (an identifier or empty).
fn identifier(s: &str) -> String {
    s.chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

fn rust_item_name(line: &str) -> Option<String> {
    let rest = strip_leading(line, &["pub(crate) ", "pub(super) ", "pub ", "async "]);
    ["fn ", "struct ", "enum ", "trait ", "mod "]
        .iter()
        .find_map(|keyword| rest.strip_prefix(keyword))
        .map(identifier)
        .filter(|name| !name.is_empty())
}

fn js_item_name(line: &str) -> Option<String> {
    let rest = strip_leading(line, &["export default ", "export ", "declare ", "async "]);
    ["function ", "const ", "class ", "interface ", "type "]
        .iter()
        .find_map(|keyword| rest.strip_prefix(keyword))
        .map(identifier)
        .filter(|name| !name.is_empty())
}

fn python_item_name(line: &str) -> Option<String> {
    let rest = strip_leading(line, &["async "]);
    ["def ", "class "]
        .iter()
        .find_map(|keyword| rest.strip_prefix(keyword))
        .map(identifier)
        .filter(|name| !name.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContextStore;

    fn store_in(dir: &Path) -> ContextStore {
        ContextStore::open(dir, &dir.join(".agentos").join("ctx.sqlite3")).expect("open store")
    }

    fn sourced_node(id: &str, files: Vec<(&str, &str)>) -> ContextNode {
        ContextNode {
            id: id.to_owned(),
            topic: format!("topic-{id}"),
            version: 1,
            summary: format!("summary of {id}"),
            source_files: files.iter().map(|(p, _)| (*p).to_owned()).collect(),
            source_hashes: files
                .into_iter()
                .map(|(p, h)| (p.to_owned(), h.to_owned()))
                .collect(),
            symbols: vec!["sym_a".to_owned()],
            dependencies: vec![],
            decisions: vec!["DEC-0001".to_owned()],
            invalidation_state: InvalidationState::Clean,
        }
    }

    #[test]
    fn upsert_get_delete_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_in(dir.path());
        let graph = store.graph();

        let mut node = sourced_node("auth", vec![("src/auth.rs", "abc123")]);
        node.dependencies = vec![];
        graph.upsert_node(&node).expect("upsert");

        let loaded = graph.get("auth").expect("get").expect("present");
        assert_eq!(loaded.id, "auth");
        assert_eq!(loaded.topic, "topic-auth");
        assert_eq!(loaded.source_files, vec!["src/auth.rs".to_owned()]);
        assert_eq!(
            loaded.source_hashes.get("src/auth.rs").map(String::as_str),
            Some("abc123")
        );
        assert_eq!(loaded.symbols, vec!["sym_a".to_owned()]);
        assert_eq!(loaded.decisions, vec!["DEC-0001".to_owned()]);
        assert_eq!(loaded.invalidation_state, InvalidationState::Clean);

        assert_eq!(graph.by_topic("topic-auth").expect("by_topic").len(), 1);
        assert_eq!(graph.all().expect("all").len(), 1);

        graph.delete("auth").expect("delete");
        assert!(graph.get("auth").expect("get").is_none());
        assert!(matches!(
            graph.delete("auth"),
            Err(ContextError::NotFound(_))
        ));
    }

    #[test]
    fn provenance_is_enforced_on_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_in(dir.path());
        let graph = store.graph();

        // Source file without any hash entry.
        let mut missing = sourced_node("n1", vec![]);
        missing.source_files = vec!["src/a.rs".to_owned()];
        assert!(matches!(
            graph.upsert_node(&missing),
            Err(ContextError::Validation(_))
        ));

        // Source file with an empty hash.
        let mut empty = sourced_node("n2", vec![]);
        empty.source_files = vec!["src/a.rs".to_owned()];
        empty.source_hashes = [("src/a.rs".to_owned(), String::new())]
            .into_iter()
            .collect();
        assert!(matches!(
            graph.upsert_node(&empty),
            Err(ContextError::Validation(_))
        ));

        // Hash without a matching source file.
        let mut orphan = sourced_node("n3", vec![]);
        orphan.source_hashes = [("src/ghost.rs".to_owned(), "deadbeef".to_owned())]
            .into_iter()
            .collect();
        assert!(matches!(
            graph.upsert_node(&orphan),
            Err(ContextError::Validation(_))
        ));

        // Empty id / topic, version 0.
        let bad_id = sourced_node("", vec![("a.rs", "h")]);
        assert!(matches!(
            graph.upsert_node(&bad_id),
            Err(ContextError::Validation(_))
        ));
        let mut bad_topic = sourced_node("n4", vec![("a.rs", "h")]);
        bad_topic.topic = "  ".to_owned();
        assert!(matches!(
            graph.upsert_node(&bad_topic),
            Err(ContextError::Validation(_))
        ));
        let mut bad_version = sourced_node("n5", vec![("a.rs", "h")]);
        bad_version.version = 0;
        assert!(matches!(
            graph.upsert_node(&bad_version),
            Err(ContextError::Validation(_))
        ));

        // Nothing was written.
        assert!(graph.all().expect("all").is_empty());
    }

    #[test]
    fn dependencies_must_exist_and_dependents_are_transitive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_in(dir.path());
        let graph = store.graph();

        // Unknown dependency target rejected.
        let mut bad = sourced_node("a", vec![("f.rs", "h")]);
        bad.dependencies = vec!["ghost".to_owned()];
        assert!(matches!(
            graph.upsert_node(&bad),
            Err(ContextError::NotFound(_))
        ));

        graph
            .upsert_node(&sourced_node("a", vec![("f.rs", "h")]))
            .expect("upsert a");
        let mut b = sourced_node("b", vec![("g.rs", "h")]);
        b.dependencies = vec!["a".to_owned()];
        graph.upsert_node(&b).expect("upsert b");
        let mut c = sourced_node("c", vec![("i.rs", "h")]);
        c.dependencies = vec!["b".to_owned()];
        graph.upsert_node(&c).expect("upsert c");

        assert_eq!(graph.dependents_of("a").expect("deps"), {
            let mut v = vec!["b".to_owned(), "c".to_owned()];
            v.sort();
            v
        });
        assert_eq!(
            graph.dependents_of("b").expect("deps"),
            vec!["c".to_owned()]
        );
        assert!(graph.dependents_of("c").expect("deps").is_empty());
    }

    #[test]
    fn symbol_stub_extracts_expected_items() {
        let rust = "\
use std::fmt;
pub fn parse(input: &str) -> u32 { 0 }
fn helper<T>(x: T) -> T { x }
pub(crate) struct Engine { on: bool }
enum Mode { A, B }
trait Runner { }
mod inner { }
// fn commented_out() {}
";
        assert_eq!(
            symbols_in_source(rust, &Language::Rust),
            vec![
                "parse".to_owned(),
                "helper".to_owned(),
                "Engine".to_owned(),
                "Mode".to_owned(),
                "Runner".to_owned(),
                "inner".to_owned()
            ]
        );

        let ts = "\
export function load(): void {}
const MAX = 10;
export default class App extends Base {}
interface Widget { }
type Mode = 'a' | 'b';
async function boot() {}
// function ignored() {}
";
        assert_eq!(
            symbols_in_source(ts, &Language::TypeScript),
            vec![
                "load".to_owned(),
                "MAX".to_owned(),
                "App".to_owned(),
                "Widget".to_owned(),
                "Mode".to_owned(),
                "boot".to_owned()
            ]
        );

        let py = "\
async def fetch_all():
    def nested():
        pass
class Repo:
    pass
# def commented():
";
        assert_eq!(
            symbols_in_source(py, &Language::Python),
            vec![
                "fetch_all".to_owned(),
                "nested".to_owned(),
                "Repo".to_owned()
            ]
        );

        // Non-code languages extract nothing; duplicates deduplicate.
        assert!(symbols_in_source("# fn fake() {}", &Language::Markdown).is_empty());
        let dup = "fn same() {}\nfn same() {}\n";
        assert_eq!(
            symbols_in_source(dup, &Language::Rust),
            vec!["same".to_owned()]
        );
    }

    #[test]
    fn extract_symbols_reads_from_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("lib.rs");
        fs::write(&file, "pub struct Store;\nfn build() {}\n").expect("write");
        assert_eq!(
            extract_symbols(&file, &Language::Rust).expect("symbols"),
            vec!["Store".to_owned(), "build".to_owned()]
        );
        assert!(extract_symbols(&dir.path().join("missing.rs"), &Language::Rust).is_err());
    }
}
