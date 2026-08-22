//! CTX-05 — the invalidation engine (PRD §10).
//!
//! Dirty-state machine over the context graph:
//!
//! ```text
//!                      file change (sourced path)
//!   clean ────────────────────────────────────────> direct-dirty
//!      ^                                                │
//!      │ mark_clean(id, new_hashes)                     │ (transitive edges)
//!      │                                                v
//!   clean <──── mark_clean ──── dependency-dirty <── clean
//!
//!   needs-review is sticky: set manually via mark_needs_review, it is
//!   never overwritten by automatic propagation until an explicit
//!   mark_clean clears it.
//! ```
//!
//! Changing a set of paths marks every node sourcing one of them
//! `direct-dirty`, and every transitive dependent of those nodes
//! `dependency-dirty` (conservatively, per CTX-02). Each propagation batch
//! returns `context.invalidated` events (payload = affected node ids +
//! reason + changed files); the caller appends them to the journal.

use std::collections::{BTreeSet, HashMap};

use agentos_core::{Event, EventType};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;

use crate::db::{map_sqlite, now_ts};
use crate::ContextError;

/// CTX-05 dirty state of a context node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InvalidationState {
    /// Content verified against current source hashes.
    Clean,
    /// A file this node directly sources changed.
    DirectDirty,
    /// A node this node (transitively) depends on is dirty.
    DependencyDirty,
    /// Flagged for manual/curator review; sticky under automatic
    /// propagation.
    NeedsReview,
}

impl InvalidationState {
    /// Canonical wire/storage string.
    pub fn as_str(&self) -> &'static str {
        match self {
            InvalidationState::Clean => "clean",
            InvalidationState::DirectDirty => "direct-dirty",
            InvalidationState::DependencyDirty => "dependency-dirty",
            InvalidationState::NeedsReview => "needs-review",
        }
    }

    /// Parse a stored state string; `None` for unknown values.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "clean" => Some(InvalidationState::Clean),
            "direct-dirty" => Some(InvalidationState::DirectDirty),
            "dependency-dirty" => Some(InvalidationState::DependencyDirty),
            "needs-review" => Some(InvalidationState::NeedsReview),
            _ => None,
        }
    }
}

impl std::fmt::Display for InvalidationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// States serialize as their canonical dash-case string; deserializing an
/// unknown string is an error (these values are internal, not a
/// forward-compat wire surface).
impl Serialize for InvalidationState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for InvalidationState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = String::deserialize(deserializer)?;
        InvalidationState::parse(&wire).ok_or_else(|| {
            <D::Error as serde::de::Error>::unknown_variant(
                &wire,
                &["clean", "direct-dirty", "dependency-dirty", "needs-review"],
            )
        })
    }
}

/// CTX-05 engine over the context graph.
#[derive(Debug)]
pub struct InvalidationEngine<'a> {
    conn: &'a Connection,
}

impl<'a> InvalidationEngine<'a> {
    /// Bind an engine view to an open context connection.
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    /// Apply a file-change batch: direct sourcers become `direct-dirty`
    /// (unless `needs-review`), transitive dependents become
    /// `dependency-dirty` (only from `clean`). Returns the
    /// `context.invalidated` events for the journal — one per reason group,
    /// each carrying the affected node ids, the reason, and the changed
    /// files.
    pub fn on_files_changed(&self, paths: &[String]) -> Result<Vec<Event>, ContextError> {
        on_files_changed(self.conn, paths)
    }

    /// Current state of every node, ordered by id (deterministic).
    pub fn states(&self) -> Result<Vec<(String, InvalidationState)>, ContextError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, invalidation_state FROM context_nodes ORDER BY id")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(map_sqlite)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite)?
            .into_iter()
            .map(|(id, state)| {
                let parsed = InvalidationState::parse(&state).unwrap_or_else(|| {
                    tracing::warn!(node = %id, raw = %state, "unknown invalidation state; treating as needs-review");
                    InvalidationState::NeedsReview
                });
                Ok((id, parsed))
            })
            .collect()
    }

    /// Recompute queue: `direct-dirty` nodes first, then `dependency-dirty`
    /// nodes (the input the CTX-05 "prioritize recomputation for nodes
    /// needed by ready tasks" hook filters against; ordering within each
    /// group is by id for determinism).
    pub fn recompute_queue(&self) -> Result<Vec<String>, ContextError> {
        let mut queue = Vec::new();
        for state in [
            InvalidationState::DirectDirty,
            InvalidationState::DependencyDirty,
        ] {
            let mut stmt = self
                .conn
                .prepare("SELECT id FROM context_nodes WHERE invalidation_state = ?1 ORDER BY id")
                .map_err(map_sqlite)?;
            let ids = stmt
                .query_map(params![state.as_str()], |row| row.get::<_, String>(0))
                .map_err(map_sqlite)?;
            queue.extend(ids.collect::<Result<Vec<_>, _>>().map_err(map_sqlite)?);
        }
        Ok(queue)
    }

    /// Flag a node for manual review (sticky under automatic propagation).
    pub fn mark_needs_review(&self, id: &str) -> Result<(), ContextError> {
        let updated = self
            .conn
            .execute(
                "UPDATE context_nodes SET invalidation_state = 'needs-review', updated_at = ?2
                 WHERE id = ?1",
                params![id, now_ts()],
            )
            .map_err(map_sqlite)?;
        if updated == 0 {
            return Err(ContextError::NotFound(format!(
                "context node {id} not found"
            )));
        }
        Ok(())
    }

    /// Declare a node recomputed against `new_hashes`: sets the state to
    /// `clean`, bumps the version, and upserts the given (path, hash)
    /// provenance pairs. Hash values must be non-empty.
    pub fn mark_clean(
        &self,
        id: &str,
        new_hashes: &HashMap<String, String>,
    ) -> Result<(), ContextError> {
        if new_hashes.values().any(|hash| hash.trim().is_empty()) {
            return Err(ContextError::Validation(format!(
                "mark_clean on node {id} received an empty content hash"
            )));
        }
        let tx = self.conn.unchecked_transaction().map_err(map_sqlite)?;
        let exists: bool = tx
            .query_row(
                "SELECT 1 FROM context_nodes WHERE id = ?1",
                params![id],
                |_| Ok(true),
            )
            .optional()
            .map_err(map_sqlite)?
            .unwrap_or(false);
        if !exists {
            return Err(ContextError::NotFound(format!(
                "context node {id} not found"
            )));
        }
        tx.execute(
            "UPDATE context_nodes
             SET version = version + 1, invalidation_state = 'clean', updated_at = ?2
             WHERE id = ?1",
            params![id, now_ts()],
        )
        .map_err(map_sqlite)?;
        for (path, hash) in new_hashes {
            tx.execute(
                "INSERT INTO node_source_files (node_id, path, hash) VALUES (?1, ?2, ?3)
                 ON CONFLICT(node_id, path) DO UPDATE SET hash = excluded.hash",
                params![id, path, hash],
            )
            .map_err(map_sqlite)?;
        }
        tx.commit().map_err(map_sqlite)?;
        Ok(())
    }
}

/// Shared propagation core, also driven by [`crate::FileIndex`] rescans.
pub(crate) fn on_files_changed(
    conn: &Connection,
    paths: &[String],
) -> Result<Vec<Event>, ContextError> {
    let mut changed: Vec<String> = paths.to_vec();
    changed.sort();
    changed.dedup();
    if changed.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = vec!["?"; changed.len()].join(",");
    let sql =
        format!("SELECT DISTINCT node_id FROM node_source_files WHERE path IN ({placeholders})");
    let mut stmt = conn.prepare(&sql).map_err(map_sqlite)?;
    let direct: BTreeSet<String> = stmt
        .query_map(rusqlite::params_from_iter(changed.iter()), |row| {
            row.get::<_, String>(0)
        })
        .map_err(map_sqlite)?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(map_sqlite)?;

    let now = now_ts();
    let mut events = Vec::new();

    if !direct.is_empty() {
        let direct_ids: Vec<String> = direct.iter().cloned().collect();
        let placeholders = vec!["?"; direct_ids.len()].join(",");
        conn.execute(
            &format!(
                "UPDATE context_nodes
                 SET invalidation_state = 'direct-dirty', updated_at = '{now}'
                 WHERE invalidation_state <> 'needs-review'
                   AND id IN ({placeholders})"
            ),
            rusqlite::params_from_iter(direct_ids.iter()),
        )
        .map_err(map_sqlite)?;
        events.push(invalidated_event(direct_ids, "direct-dirty", &changed));
    }

    // Transitive dependents of the direct set: reverse-edge closure.
    let edges = crate::graph::load_edges(conn)?;
    let mut reverse: HashMap<&str, Vec<&str>> = HashMap::new();
    for (node, dep) in &edges {
        reverse.entry(dep.as_str()).or_default().push(node.as_str());
    }
    let mut dependents: BTreeSet<String> = BTreeSet::new();
    let mut queue: Vec<&str> = direct.iter().map(String::as_str).collect();
    while let Some(current) = queue.pop() {
        for dependent in reverse.get(current).into_iter().flatten() {
            if !direct.contains(*dependent) && dependents.insert((*dependent).to_owned()) {
                queue.push(dependent);
            }
        }
    }

    if !dependents.is_empty() {
        let dep_ids: Vec<String> = dependents.iter().cloned().collect();
        let placeholders = vec!["?"; dep_ids.len()].join(",");
        conn.execute(
            &format!(
                "UPDATE context_nodes
                 SET invalidation_state = 'dependency-dirty', updated_at = '{now}'
                 WHERE invalidation_state = 'clean'
                   AND id IN ({placeholders})"
            ),
            rusqlite::params_from_iter(dep_ids.iter()),
        )
        .map_err(map_sqlite)?;
        events.push(invalidated_event(dep_ids, "dependency-dirty", &changed));
    }

    Ok(events)
}

/// Build one `context.invalidated` journal event (returned, not appended —
/// the caller owns the journal, F-00 §3).
fn invalidated_event(node_ids: Vec<String>, reason: &str, changed_files: &[String]) -> Event {
    Event::new(EventType::ContextInvalidated)
        .with_agent_id("agentos-context")
        .with_payload(json!({
            "nodeIds": node_ids,
            "reason": reason,
            "changedFiles": changed_files,
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use crate::file_index::ScanOptions;
    use crate::{ContextNode, ContextStore};

    fn store_in(dir: &Path) -> ContextStore {
        ContextStore::open(dir, &dir.join(".agentos").join("ctx.sqlite3")).expect("open store")
    }

    fn node(id: &str, files: Vec<(&str, &str)>, deps: Vec<&str>) -> ContextNode {
        ContextNode {
            id: id.to_owned(),
            topic: format!("topic-{id}"),
            version: 1,
            summary: format!("summary {id}"),
            source_files: files.iter().map(|(p, _)| (*p).to_owned()).collect(),
            source_hashes: files
                .into_iter()
                .map(|(p, h)| (p.to_owned(), h.to_owned()))
                .collect(),
            symbols: vec![],
            dependencies: deps.into_iter().map(str::to_owned).collect(),
            decisions: vec![],
            invalidation_state: InvalidationState::Clean,
        }
    }

    /// A sourced by file f; B depends on A; C depends on B. f changes:
    /// A direct-dirty, B and C dependency-dirty; events carry ids + reason;
    /// the queue serves A first; mark_clean bumps the version.
    #[test]
    fn cascade_direct_and_transitive_dependency_dirty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        std::fs::write(root.join("f.rs"), "fn f() {}\n").expect("write");

        let store = store_in(root);
        store
            .file_index()
            .scan(&ScanOptions::default())
            .expect("scan");
        let hash = store
            .file_index()
            .entry("f.rs")
            .expect("entry")
            .expect("present")
            .content_hash;

        store
            .graph()
            .upsert_node(&node("a", vec![("f.rs", &hash)], vec![]))
            .expect("a");
        store
            .graph()
            .upsert_node(&node("b", vec![], vec!["a"]))
            .expect("b");
        store
            .graph()
            .upsert_node(&node("c", vec![], vec!["b"]))
            .expect("c");

        let events = store
            .invalidation()
            .on_files_changed(&["f.rs".to_owned()])
            .expect("propagate");
        assert_eq!(events.len(), 2);
        for event in &events {
            assert_eq!(event.event_type, EventType::ContextInvalidated);
            assert_eq!(event.agent_id.as_deref(), Some("agentos-context"));
            assert_eq!(event.payload["changedFiles"], json!(["f.rs"]));
        }
        let direct = events
            .iter()
            .find(|e| e.payload["reason"] == json!("direct-dirty"))
            .expect("direct event");
        assert_eq!(direct.payload["nodeIds"], json!(["a"]));
        let dependency = events
            .iter()
            .find(|e| e.payload["reason"] == json!("dependency-dirty"))
            .expect("dependency event");
        assert_eq!(dependency.payload["nodeIds"], json!(["b", "c"]));

        assert_eq!(
            store.invalidation().states().expect("states"),
            vec![
                ("a".to_owned(), InvalidationState::DirectDirty),
                ("b".to_owned(), InvalidationState::DependencyDirty),
                ("c".to_owned(), InvalidationState::DependencyDirty),
            ]
        );

        // Queue: direct-dirty first, then dependency-dirty.
        assert_eq!(
            store.invalidation().recompute_queue().expect("queue"),
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
        );

        // mark_clean bumps version and restores provenance.
        store
            .invalidation()
            .mark_clean(
                "a",
                &[("f.rs".to_owned(), format!("{hash}x"))]
                    .into_iter()
                    .collect(),
            )
            .expect("mark clean");
        let refreshed = store.graph().get("a").expect("get").expect("present");
        assert_eq!(refreshed.version, 2);
        assert_eq!(refreshed.invalidation_state, InvalidationState::Clean);
        assert_eq!(
            refreshed.source_hashes.get("f.rs").map(String::as_str),
            Some(format!("{hash}x").as_str())
        );

        // Unknown node and empty hashes are rejected.
        assert!(matches!(
            store.invalidation().mark_clean("ghost", &HashMap::new()),
            Err(ContextError::NotFound(_))
        ));
        assert!(matches!(
            store.invalidation().mark_clean(
                "b",
                &[("x.rs".to_owned(), String::new())].into_iter().collect()
            ),
            Err(ContextError::Validation(_))
        ));
        assert!(matches!(
            store.invalidation().mark_needs_review("ghost"),
            Err(ContextError::NotFound(_))
        ));
    }

    #[test]
    fn needs_review_is_sticky_under_propagation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        std::fs::write(root.join("f.rs"), "fn f() {}\n").expect("write");
        std::fs::write(root.join("g.rs"), "fn g() {}\n").expect("write");

        let store = store_in(root);
        store
            .file_index()
            .scan(&ScanOptions::default())
            .expect("scan");
        let f_hash = store
            .file_index()
            .entry("f.rs")
            .expect("e")
            .expect("p")
            .content_hash;
        let g_hash = store
            .file_index()
            .entry("g.rs")
            .expect("e")
            .expect("p")
            .content_hash;

        store
            .graph()
            .upsert_node(&node("a", vec![("f.rs", &f_hash)], vec![]))
            .expect("a");
        // d sources g AND depends on a: would be dependency-dirty, but is
        // flagged needs-review first — propagation must not overwrite it.
        store
            .graph()
            .upsert_node(&node("d", vec![("g.rs", &g_hash)], vec!["a"]))
            .expect("d");
        store.invalidation().mark_needs_review("d").expect("review");

        store
            .invalidation()
            .on_files_changed(&["f.rs".to_owned(), "f.rs".to_owned()])
            .expect("propagate");

        assert_eq!(
            store.invalidation().states().expect("states"),
            vec![
                ("a".to_owned(), InvalidationState::DirectDirty),
                ("d".to_owned(), InvalidationState::NeedsReview),
            ]
        );

        // Changing d's own source also must not un-stick needs-review...
        store
            .invalidation()
            .on_files_changed(&["g.rs".to_owned()])
            .expect("propagate");
        assert_eq!(
            store.invalidation().states().expect("states")[1],
            ("d".to_owned(), InvalidationState::NeedsReview)
        );
        // ...but an explicit mark_clean clears it.
        store
            .invalidation()
            .mark_clean("d", &[("g.rs".to_owned(), g_hash)].into_iter().collect())
            .expect("mark clean");
        assert_eq!(
            store.invalidation().states().expect("states")[1],
            ("d".to_owned(), InvalidationState::Clean)
        );
    }

    #[test]
    fn empty_change_list_is_a_noop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_in(dir.path());
        assert!(store
            .invalidation()
            .on_files_changed(&[])
            .expect("propagate")
            .is_empty());
        assert!(store
            .invalidation()
            .recompute_queue()
            .expect("queue")
            .is_empty());
        assert!(store.invalidation().states().expect("states").is_empty());
    }

    #[test]
    fn rescan_drives_invalidation_and_emits_events() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        std::fs::write(root.join("f.rs"), "fn f() {}\n").expect("write");

        let store = store_in(root);
        let options = ScanOptions::default();
        store.file_index().scan(&options).expect("scan");
        let hash = store
            .file_index()
            .entry("f.rs")
            .expect("e")
            .expect("p")
            .content_hash;
        store
            .graph()
            .upsert_node(&node("a", vec![("f.rs", &hash)], vec![]))
            .expect("a");

        std::fs::write(root.join("f.rs"), "fn f() { changed }\n").expect("rewrite");
        let report = store.file_index().rescan(&options).expect("rescan");
        assert_eq!(report.changed, vec!["f.rs".to_owned()]);
        assert_eq!(report.invalidation_events.len(), 1);
        assert_eq!(
            report.invalidation_events[0].payload["nodeIds"],
            json!(["a"])
        );
        assert_eq!(
            store.invalidation().states().expect("states"),
            vec![("a".to_owned(), InvalidationState::DirectDirty)]
        );
    }
}
