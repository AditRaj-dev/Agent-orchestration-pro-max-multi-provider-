//! # agentos-context
//!
//! F-08 — the context compiler (PRD §10): compiled shared knowledge built
//! from the repository itself.
//!
//! Four cooperating pieces, all persisted in one SQLite database opened
//! through the F-01 canon helper ([`db`], private to this crate):
//!
//! - [`FileIndex`] (CTX-01): the repository file cache — every relevant file
//!   hashed with BLAKE3, with language detection and per-path hash history.
//!   Cache validity is content-based; size+mtime equality is only a fast
//!   path, never proof of validity ([`FileIndex::verify_all`] re-hashes
//!   everything).
//! - [`ContextGraph`] (CTX-02): normalized context nodes (topic, version,
//!   summary, source files, source hashes, symbols, dependencies, decision
//!   refs). Every sourced write requires provenance — a non-empty content
//!   hash per source file — enforced by validation.
//! - [`InvalidationEngine`] (CTX-05): the dirty-state machine
//!   (clean / direct-dirty / dependency-dirty / needs-review). Changed files
//!   mark sourcing nodes direct-dirty and transitive dependents
//!   dependency-dirty, conservatively; each batch of emitted
//!   `context.invalidated` events is returned to the caller for the
//!   append-only journal (F-00 §3).
//! - [`ContextCompiler`] (CTX-04): deterministic, role-weighted retrieval of
//!   files and node summaries under a strict token budget (approximated as
//!   bytes/4), materialized as one ephemeral markdown context file plus a
//!   manifest of included/omitted references.
//!
//! [`ContextStore`] ties them together over one connection and one
//! repository root.

#![forbid(unsafe_code)]

pub mod compiler;
pub(crate) mod db;
pub mod file_index;
pub mod graph;
pub mod invalidation;

use std::path::{Path, PathBuf};

use agentos_core::CoreError;

pub use compiler::{
    ContextCompiler, ContextPack, Manifest, PackItemKind, PackRef, PackRequest, Role,
};
pub use file_index::{FileIndex, FileIndexEntry, Language, ScanOptions, ScanReport};
pub use graph::{extract_symbols, symbols_in_source, ContextGraph, ContextNode};
pub use invalidation::{InvalidationEngine, InvalidationState};

/// Errors surfaced by `agentos-context`.
///
/// [`ContextError::Core`] carries [`CoreError::SqliteBusy`] for every
/// `SQLITE_BUSY` outcome (F-01 canon: busy is retryable and must stay
/// distinguishable from a generic failure) — see [`db::map_sqlite`].
#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    /// A shared core error (e.g. retryable `SQLITE_BUSY`, or `NotFound`).
    #[error(transparent)]
    Core(#[from] CoreError),
    /// A non-busy SQLite failure.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Filesystem failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON (de)serialization failure.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A caller-supplied node/pack failed validation (e.g. missing
    /// provenance: a source file without a non-empty content hash).
    #[error("invalid context state: {0}")]
    Validation(String),
    /// The referenced entity does not exist.
    #[error("not found: {0}")]
    NotFound(String),
}

/// One repository root + one SQLite database holding the file index, the
/// context graph, and the invalidation state.
///
/// All views ([`file_index`](Self::file_index), [`graph`](Self::graph),
/// [`invalidation`](Self::invalidation), [`compiler`](Self::compiler))
/// borrow the shared connection, so they are cheap to create per operation.
#[derive(Debug)]
pub struct ContextStore {
    conn: rusqlite::Connection,
    root: PathBuf,
}

impl ContextStore {
    /// Open (creating if needed) the context database for the repository at
    /// `root`, applying the F-01 SQLite canon and the schema migration.
    /// The database file's parent directory is created if missing.
    pub fn open(root: &Path, db_path: &Path) -> Result<Self, ContextError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = db::open_db(db_path)?;
        Ok(Self {
            conn,
            root: root.to_path_buf(),
        })
    }

    /// Repository file cache view (CTX-01).
    pub fn file_index(&self) -> FileIndex<'_> {
        FileIndex::new(&self.conn, &self.root)
    }

    /// Context graph view (CTX-02).
    pub fn graph(&self) -> ContextGraph<'_> {
        ContextGraph::new(&self.conn)
    }

    /// Invalidation engine view (CTX-05).
    pub fn invalidation(&self) -> InvalidationEngine<'_> {
        InvalidationEngine::new(&self.conn)
    }

    /// Role-specific pack compiler view (CTX-04).
    pub fn compiler(&self) -> ContextCompiler<'_> {
        ContextCompiler::new(&self.conn, &self.root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_core::{Event, EventType};

    #[test]
    fn sqlite_busy_maps_to_retryable_core_error() {
        let err = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                extended_code: 5 | (1 << 8), // SQLITE_BUSY_SNAPSHOT
            },
            None,
        );
        let mapped = db::map_sqlite(err);
        assert!(matches!(mapped, ContextError::Core(CoreError::SqliteBusy)));
        assert!(CoreError::SqliteBusy.is_retryable());
    }

    /// End-to-end happy path: scan a tree, compile a node over it, change a
    /// source file, rescan, observe the invalidation cascade, recompute,
    /// then compile a pack for a role.
    #[test]
    fn store_happy_path_scan_invalidate_compile() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        std::fs::create_dir_all(root.join("src")).expect("mkdir");
        std::fs::write(root.join("src").join("main.rs"), "fn main() {}\n").expect("write");
        std::fs::create_dir_all(root.join("web")).expect("mkdir");
        std::fs::write(
            root.join("web").join("app.tsx"),
            "export function App() {}\n",
        )
        .expect("write");

        let db_path = root.join(".agentos").join("ctx.sqlite3");
        let store = ContextStore::open(root, &db_path).expect("open store");

        let report = store
            .file_index()
            .scan(&ScanOptions::default())
            .expect("scan");
        assert_eq!(report.added.len(), 2);
        let main_hash = store
            .file_index()
            .entry("src/main.rs")
            .expect("entry lookup")
            .expect("present")
            .content_hash;

        // Node "app" sources src/main.rs; node "db" is a dependency of "app".
        store
            .graph()
            .upsert_node(&ContextNode {
                id: "db".to_owned(),
                topic: "database schema".to_owned(),
                version: 1,
                summary: "SQLite WAL, three tables.".to_owned(),
                source_files: vec![],
                source_hashes: std::collections::HashMap::new(),
                symbols: vec![],
                dependencies: vec![],
                decisions: vec!["DEC-0007".to_owned()],
                invalidation_state: InvalidationState::Clean,
            })
            .expect("upsert db node");
        store
            .graph()
            .upsert_node(&ContextNode {
                id: "app".to_owned(),
                topic: "application shell".to_owned(),
                version: 1,
                summary: "Entry point and wiring.".to_owned(),
                source_files: vec!["src/main.rs".to_owned()],
                source_hashes: [("src/main.rs".to_owned(), main_hash.clone())]
                    .into_iter()
                    .collect(),
                symbols: vec!["main".to_owned()],
                dependencies: vec!["db".to_owned()],
                decisions: vec![],
                invalidation_state: InvalidationState::Clean,
            })
            .expect("upsert app node");

        // Change the sourced file; rescan must flag it and cascade.
        std::fs::write(root.join("src").join("main.rs"), "fn main() { longer() }\n")
            .expect("rewrite");
        let report = store
            .file_index()
            .rescan(&ScanOptions::default())
            .expect("rescan");
        assert_eq!(report.changed, vec!["src/main.rs".to_owned()]);
        assert!(report
            .invalidation_events
            .iter()
            .all(|e| e.event_type == EventType::ContextInvalidated));
        assert_eq!(
            store.invalidation().states().expect("states"),
            vec![
                ("app".to_owned(), InvalidationState::DirectDirty),
                ("db".to_owned(), InvalidationState::Clean),
            ]
        );

        // Recompute queue serves the direct-dirty node first.
        assert_eq!(
            store.invalidation().recompute_queue().expect("queue"),
            vec!["app".to_owned()]
        );

        // Mark clean with the new hash: version bumps.
        let new_hash = store
            .file_index()
            .entry("src/main.rs")
            .expect("entry")
            .expect("present")
            .content_hash;
        store
            .invalidation()
            .mark_clean(
                "app",
                &[("src/main.rs".to_owned(), new_hash)].into_iter().collect(),
            )
            .expect("mark clean");
        assert_eq!(
            store
                .graph()
                .get("app")
                .expect("get")
                .expect("present")
                .version,
            2
        );

        // Compile a frontend pack that fits both files plus both summaries.
        let out_dir = root.join("packs");
        let pack = store
            .compiler()
            .compile(
                &PackRequest {
                    role: Role::Frontend,
                    allowed_paths: vec!["src".to_owned(), "web".to_owned()],
                    context_refs: vec!["app".to_owned()],
                    token_budget: 4_000,
                },
                &out_dir,
            )
            .expect("compile");
        assert!(pack.materialized_path.is_file());
        assert!(pack.manifest.est_tokens <= 4_000);
        assert!(pack
            .manifest
            .included
            .iter()
            .any(|r| r.target == "web/app.tsx"));
        assert!(pack
            .manifest
            .included
            .iter()
            .any(|r| r.target == "db" && r.kind == PackItemKind::NodeSummary));
        // Journal events carry node ids + reason in the payload.
        let event: &Event = report.invalidation_events.first().expect("event");
        assert!(event.payload["changedFiles"].is_array());
        assert!(event.payload["nodeIds"].is_array());
        assert!(event.payload["reason"].is_string());
    }
}
