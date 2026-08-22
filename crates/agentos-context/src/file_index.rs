//! CTX-01 — repository file cache (PRD §10).
//!
//! Every relevant file under the repository root is hashed with BLAKE3 and
//! recorded with its detected [`Language`], size, parse version, and a
//! per-path content-hash history. Cache validity is **content-based**:
//! size+mtime equality is only a rescan fast path, never proof of validity
//! — [`FileIndex::verify_all`] re-hashes everything precisely because
//! timestamps can be spoofed (PRD CTX-01: "never assume cache validity from
//! timestamps alone").
//!
//! Files that changed (or appeared/vanished) between scans are reported in a
//! [`ScanReport`], and the invalidation engine marks every context node
//! sourcing those paths dirty; the resulting `context.invalidated` events
//! are attached to the report for the caller to append to the journal.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use agentos_core::Event;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::db::{map_sqlite, now_ts};
use crate::ContextError;

/// Default cap: files larger than 1 MiB are treated as non-source artifacts
/// and skipped by the scanner.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 1024 * 1024;

/// Default directory names pruned from the walk: tool noise plus
/// `.agentos` — the daemon's own state directory, so the file index never
/// indexes its own database.
pub const DEFAULT_IGNORE_DIRS: &[&str] = &[
    ".git",
    "target",
    "node_modules",
    ".agentos-worktrees",
    ".agentos",
];

/// Default binary-ish extensions skipped by the scanner.
pub const DEFAULT_BINARY_EXTENSIONS: &[&str] = &[
    "png", "jpg", "jpeg", "gif", "ico", "webp", "pdf", "zip", "tar", "gz", "7z", "exe", "dll",
    "so", "dylib", "lib", "obj", "pdb", "class", "jar", "woff", "woff2", "ttf", "eot", "bin",
];

/// Programming/markup language of an indexed file, detected purely from the
/// path extension by [`language_of`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Language {
    /// `.rs`
    Rust,
    /// `.ts`
    TypeScript,
    /// `.tsx`
    Tsx,
    /// `.js`, `.mjs`, `.cjs`
    JavaScript,
    /// `.py`
    Python,
    /// `.md`
    Markdown,
    /// `.toml`
    Toml,
    /// `.json`
    Json,
    /// `.yaml`, `.yml`
    Yaml,
    /// `.sql`
    Sql,
    /// `.css`, `.scss`
    Css,
    /// `.html`
    Html,
    /// `.sh`, `.bash`
    Shell,
    /// Any other extension; carries the lowercased extension.
    Other(String),
}

impl Language {
    /// Canonical lowercase name used in the `files.language` column.
    pub fn as_str(&self) -> &str {
        match self {
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
            Language::Shell => "shell",
            Language::Other(ext) => ext,
        }
    }
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Languages serialize as their canonical lowercase name; unknown names
/// deserialize to [`Language::Other`] so index rows written by newer builds
/// still parse.
impl Serialize for Language {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Language {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let name = String::deserialize(deserializer)?;
        Ok(language_from_name(&name))
    }
}

/// Detect the [`Language`] of a path purely from its extension
/// (case-insensitive). No filesystem access — deterministic and unit-testable.
pub fn language_of(path: &str) -> Language {
    let ext = Path::new(path)
        .extension()
        .map(|e| e.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "rs" => Language::Rust,
        "ts" => Language::TypeScript,
        "tsx" => Language::Tsx,
        "js" | "mjs" | "cjs" => Language::JavaScript,
        "py" => Language::Python,
        "md" => Language::Markdown,
        "toml" => Language::Toml,
        "json" => Language::Json,
        "yaml" | "yml" => Language::Yaml,
        "sql" => Language::Sql,
        "css" | "scss" => Language::Css,
        "html" => Language::Html,
        "sh" | "bash" => Language::Shell,
        other => Language::Other(other.to_owned()),
    }
}

/// Parse a language name produced by [`Language::as_str`]. Used when reading
/// the `files` table; unknown names round-trip through [`Language::Other`].
fn language_from_name(name: &str) -> Language {
    match name {
        "rust" => Language::Rust,
        "typescript" => Language::TypeScript,
        "tsx" => Language::Tsx,
        "javascript" => Language::JavaScript,
        "python" => Language::Python,
        "markdown" => Language::Markdown,
        "toml" => Language::Toml,
        "json" => Language::Json,
        "yaml" => Language::Yaml,
        "sql" => Language::Sql,
        "css" => Language::Css,
        "html" => Language::Html,
        "shell" => Language::Shell,
        other => Language::Other(other.to_owned()),
    }
}

/// Tuning knobs for a scan walk.
#[derive(Debug, Clone)]
pub struct ScanOptions {
    /// Directory names pruned anywhere in the tree.
    pub ignore_dirs: Vec<String>,
    /// Lowercased extensions treated as binary and skipped.
    pub binary_extensions: Vec<String>,
    /// Files above this byte length are skipped (oversize artifacts).
    pub max_file_bytes: u64,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            ignore_dirs: DEFAULT_IGNORE_DIRS
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
            binary_extensions: DEFAULT_BINARY_EXTENSIONS
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
        }
    }
}

/// One row of the repository file cache (`files` table).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileIndexEntry {
    /// Repository-relative path, `/`-separated.
    pub path: String,
    /// BLAKE3 content hash, hex.
    pub content_hash: String,
    /// Size in bytes at last hash.
    pub size: u64,
    /// Detected language.
    pub language: Language,
    /// Version of the last recorded parse/symbol extraction (bumped by
    /// [`FileIndex::mark_parsed`]).
    pub parsed_version: u64,
    /// Content changed since the last recorded parse.
    pub dirty: bool,
}

/// Outcome of a scan pass.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanReport {
    /// Regular files seen on disk during the walk (after pruning).
    pub files_on_disk: usize,
    /// Files actually re-hashed this pass.
    pub hashed: usize,
    /// Files accepted via the size+mtime fast path (rescan only).
    pub reused_fast_path: usize,
    /// Files skipped as binary or oversize.
    pub skipped_filter: usize,
    /// Newly indexed paths.
    pub added: Vec<String>,
    /// Paths whose content hash changed.
    pub changed: Vec<String>,
    /// Paths present in the cache but gone from disk.
    pub deleted: Vec<String>,
    /// `context.invalidated` events produced for nodes sourcing the
    /// added/changed/deleted paths; caller appends them to the journal.
    pub invalidation_events: Vec<Event>,
}

/// Cached row fields needed for change detection.
#[derive(Debug, Clone)]
struct CachedFile {
    hash: String,
    size: u64,
    mtime_secs: i64,
    mtime_nanos: i64,
    dirty: bool,
}

/// Write payload for the `files` upsert.
struct FileRow<'a> {
    path: &'a str,
    hash: &'a str,
    size: u64,
    language: &'a Language,
    dirty: bool,
    mtime_secs: i64,
    mtime_nanos: i64,
}

/// CTX-01 repository file cache bound to one connection and one repo root.
#[derive(Debug)]
pub struct FileIndex<'a> {
    conn: &'a Connection,
    root: &'a Path,
}

impl<'a> FileIndex<'a> {
    /// Bind a file-index view to an open context connection.
    pub fn new(conn: &'a Connection, root: &'a Path) -> Self {
        Self { conn, root }
    }

    /// Full scan: hash every eligible file under the root, regardless of any
    /// cached metadata. Use for the initial index.
    pub fn scan(&self, options: &ScanOptions) -> Result<ScanReport, ContextError> {
        self.scan_inner(options, false)
    }

    /// Incremental rescan: files whose cached size **and** mtime match skip
    /// re-hashing (fast path only — not a validity proof). Changed, added
    /// and deleted paths are reported and their consumers invalidated.
    pub fn rescan(&self, options: &ScanOptions) -> Result<ScanReport, ContextError> {
        self.scan_inner(options, true)
    }

    /// Audit pass: re-hash **everything** on disk, ignoring the fast path.
    /// This is the canonical validity check (CTX-01: timestamps alone are
    /// never sufficient); it catches same-size, mtime-spoofed rewrites the
    /// fast path cannot see.
    pub fn verify_all(&self, options: &ScanOptions) -> Result<ScanReport, ContextError> {
        self.scan_inner(options, false)
    }

    /// All indexed entries, ordered by path (deterministic).
    pub fn entries(&self) -> Result<Vec<FileIndexEntry>, ContextError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT path, content_hash, size, language, parsed_version, dirty
                 FROM files ORDER BY path",
            )
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(FileIndexEntry {
                    path: row.get(0)?,
                    content_hash: row.get(1)?,
                    size: u64::try_from(row.get::<_, i64>(2)?).unwrap_or_default(),
                    language: language_from_name(&row.get::<_, String>(3)?),
                    parsed_version: u64::try_from(row.get::<_, i64>(4)?).unwrap_or_default(),
                    dirty: row.get::<_, i64>(5)? != 0,
                })
            })
            .map_err(map_sqlite)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(map_sqlite)
    }

    /// Look up one indexed entry by repository-relative path.
    pub fn entry(&self, path: &str) -> Result<Option<FileIndexEntry>, ContextError> {
        self.conn
            .query_row(
                "SELECT path, content_hash, size, language, parsed_version, dirty
                 FROM files WHERE path = ?1",
                params![normalize_rel(path)],
                |row| {
                    Ok(FileIndexEntry {
                        path: row.get(0)?,
                        content_hash: row.get(1)?,
                        size: u64::try_from(row.get::<_, i64>(2)?).unwrap_or_default(),
                        language: language_from_name(&row.get::<_, String>(3)?),
                        parsed_version: u64::try_from(row.get::<_, i64>(4)?).unwrap_or_default(),
                        dirty: row.get::<_, i64>(5)? != 0,
                    })
                },
            )
            .optional()
            .map_err(map_sqlite)
    }

    /// Record that `path` was parsed/symbol-extracted at its current content
    /// hash: clears the dirty flag and bumps `parsed_version`.
    pub fn mark_parsed(&self, path: &str) -> Result<(), ContextError> {
        let updated = self
            .conn
            .execute(
                "UPDATE files SET dirty = 0, parsed_version = parsed_version + 1 WHERE path = ?1",
                params![normalize_rel(path)],
            )
            .map_err(map_sqlite)?;
        if updated == 0 {
            return Err(ContextError::NotFound(format!(
                "indexed file {path} not found"
            )));
        }
        Ok(())
    }

    fn scan_inner(
        &self,
        options: &ScanOptions,
        allow_fast_path: bool,
    ) -> Result<ScanReport, ContextError> {
        let mut report = ScanReport::default();

        let mut disk_paths: Vec<PathBuf> = Vec::new();
        walk(self.root, options, &mut disk_paths);
        report.files_on_disk = disk_paths.len();

        let cached = self.load_cached()?;
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let now = now_ts();

        for abs in disk_paths {
            let rel = rel_from(self.root, &abs);
            let Ok(metadata) = fs::metadata(&abs) else {
                tracing::warn!(path = %rel, "file vanished mid-scan; skipping");
                continue;
            };
            if has_binary_extension(&rel, &options.binary_extensions)
                || metadata.len() > options.max_file_bytes
            {
                report.skipped_filter += 1;
                continue;
            }
            seen.insert(rel.clone());
            let (mtime_secs, mtime_nanos) = mtime_parts(&metadata);

            // Fast path: size+mtime match the cached row. This is an
            // optimization ONLY — verify_all exists because it can be fooled
            // by same-size rewrites with a restored mtime.
            if allow_fast_path {
                if let Some(hit) = cached.get(&rel) {
                    if hit.size == metadata.len()
                        && hit.mtime_secs == mtime_secs
                        && hit.mtime_nanos == mtime_nanos
                    {
                        report.reused_fast_path += 1;
                        continue;
                    }
                }
            }

            let Some(hash) = hash_file(&abs) else {
                tracing::warn!(path = %rel, "unreadable or vanished while hashing; skipping");
                continue;
            };
            report.hashed += 1;

            let (status, dirty) = match cached.get(&rel) {
                None => (Status::Added, true),
                Some(prev) if prev.hash != hash => (Status::Changed, true),
                // Unchanged content: keep the previous dirty flag so an
                // unparsed change is not silently forgotten.
                Some(prev) => (Status::Unchanged, prev.dirty),
            };
            if let Status::Added = status {
                report.added.push(rel.clone());
            }
            if let Status::Changed = status {
                report.changed.push(rel.clone());
            }
            upsert_file(
                self.conn,
                &FileRow {
                    path: &rel,
                    hash: &hash,
                    size: metadata.len(),
                    language: &language_of(&rel),
                    dirty,
                    mtime_secs,
                    mtime_nanos,
                },
                &now,
            )?;
            self.conn
                .execute(
                    "INSERT OR IGNORE INTO file_hash_history (path, content_hash, seen_at)
                     VALUES (?1, ?2, ?3)",
                    params![rel, hash, now],
                )
                .map_err(map_sqlite)?;
        }

        for rel in cached.keys() {
            if !seen.contains(rel) {
                self.conn
                    .execute("DELETE FROM files WHERE path = ?1", params![rel])
                    .map_err(map_sqlite)?;
                report.deleted.push(rel.clone());
            }
        }

        // Conservative invalidation trigger: added, changed and deleted
        // paths can all invalidate nodes sourcing them (a node may reference
        // a path that only now appeared, or just vanished).
        let mut trigger: Vec<String> = Vec::new();
        trigger.extend(report.added.iter().cloned());
        trigger.extend(report.changed.iter().cloned());
        trigger.extend(report.deleted.iter().cloned());
        if !trigger.is_empty() {
            report.invalidation_events =
                crate::invalidation::on_files_changed(self.conn, &trigger)?;
        }

        Ok(report)
    }

    fn load_cached(&self) -> Result<BTreeMap<String, CachedFile>, ContextError> {
        let mut stmt = self
            .conn
            .prepare("SELECT path, content_hash, size, mtime_secs, mtime_nanos, dirty FROM files")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    CachedFile {
                        hash: row.get(1)?,
                        size: u64::try_from(row.get::<_, i64>(2)?).unwrap_or_default(),
                        mtime_secs: row.get(3)?,
                        mtime_nanos: row.get(4)?,
                        dirty: row.get::<_, i64>(5)? != 0,
                    },
                ))
            })
            .map_err(map_sqlite)?;
        Ok(rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite)?
            .into_iter()
            .collect())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Added,
    Changed,
    Unchanged,
}

/// Upsert one `files` row.
fn upsert_file(conn: &Connection, row: &FileRow<'_>, now: &str) -> Result<(), ContextError> {
    conn.execute(
        "INSERT INTO files (path, content_hash, size, language, parsed_version, dirty,
                            mtime_secs, mtime_nanos, first_seen_at, last_hashed_at)
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?7, ?8, ?8)
         ON CONFLICT(path) DO UPDATE SET
            content_hash = excluded.content_hash,
            size = excluded.size,
            language = excluded.language,
            dirty = excluded.dirty,
            mtime_secs = excluded.mtime_secs,
            mtime_nanos = excluded.mtime_nanos,
            last_hashed_at = excluded.last_hashed_at",
        params![
            row.path,
            row.hash,
            i64::try_from(row.size).unwrap_or(i64::MAX),
            row.language.as_str(),
            i64::from(row.dirty),
            row.mtime_secs,
            row.mtime_nanos,
            now,
        ],
    )
    .map_err(map_sqlite)?;
    Ok(())
}

/// Deterministic depth-first walk: children sorted by name, ignored
/// directories pruned, unreadable directories logged and skipped.
fn walk(dir: &Path, options: &ScanOptions, out: &mut Vec<PathBuf>) {
    let Ok(read_dir) = fs::read_dir(dir) else {
        tracing::warn!(dir = %dir.display(), "skipping unreadable directory");
        return;
    };
    let mut children: Vec<_> = read_dir.flatten().collect();
    children.sort_by_key(|entry| entry.file_name());
    for entry in children {
        let path = entry.path();
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        if is_dir {
            let name = entry.file_name().to_string_lossy().into_owned();
            if options.ignore_dirs.contains(&name) {
                continue;
            }
            walk(&path, options, out);
        } else {
            out.push(path);
        }
    }
}

/// BLAKE3 content hash of a file, hex-encoded. `None` if unreadable.
fn hash_file(path: &Path) -> Option<String> {
    let bytes = fs::read(path).ok()?;
    Some(blake3::hash(&bytes).to_hex().to_string())
}

/// (seconds, nanoseconds) since the Unix epoch of an mtime; pre-epoch or
/// unavailable mtimes collapse to (0, 0).
fn mtime_parts(metadata: &fs::Metadata) -> (i64, i64) {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| (d.as_secs() as i64, i64::from(d.subsec_nanos())))
        .unwrap_or((0, 0))
}

/// Whether the path's extension is in the binary blocklist.
fn has_binary_extension(rel: &str, extensions: &[String]) -> bool {
    let Some(ext) = Path::new(rel)
        .extension()
        .map(|e| e.to_string_lossy().to_ascii_lowercase())
    else {
        return false;
    };
    extensions
        .iter()
        .any(|blocked| blocked.eq_ignore_ascii_case(&ext))
}

/// Repository-relative, `/`-separated form of `abs` under `root`.
pub(crate) fn rel_from(root: &Path, abs: &Path) -> String {
    let rel = abs.strip_prefix(root).unwrap_or(abs);
    normalize_rel(&rel.to_string_lossy())
}

/// Normalize a relative path: backslashes to `/`, no leading `./` or `/`.
pub(crate) fn normalize_rel(path: &str) -> String {
    let mut normalized = path.replace('\\', "/");
    while let Some(stripped) = normalized.strip_prefix("./") {
        normalized = stripped.to_owned();
    }
    normalized.trim_start_matches('/').to_owned()
}

/// True when `path` equals `prefix` or lives underneath it.
pub(crate) fn is_under(path: &str, prefix: &str) -> bool {
    prefix.is_empty() || path == prefix || path.starts_with(&format!("{prefix}/"))
}

/// Convert a stored (secs, nanos) pair back to a `SystemTime` (tests only).
#[cfg(test)]
pub(crate) fn mtime_from_parts(secs: i64, nanos: i64) -> std::time::SystemTime {
    UNIX_EPOCH
        + std::time::Duration::new(
            secs.unsigned_abs(),
            u32::try_from(nanos.unsigned_abs()).unwrap_or(0),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContextStore;

    fn store_in(dir: &Path) -> ContextStore {
        ContextStore::open(dir, &dir.join(".agentos").join("ctx.sqlite3")).expect("open store")
    }

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("mkdir");
        }
        fs::write(path, contents).expect("write");
    }

    #[test]
    fn language_of_maps_extensions_case_insensitively() {
        assert_eq!(language_of("src/main.rs"), Language::Rust);
        assert_eq!(language_of("a.TS"), Language::TypeScript);
        assert_eq!(language_of("b.tsx"), Language::Tsx);
        assert_eq!(language_of("c.mjs"), Language::JavaScript);
        assert_eq!(language_of("d.py"), Language::Python);
        assert_eq!(language_of("e.md"), Language::Markdown);
        assert_eq!(language_of("f.toml"), Language::Toml);
        assert_eq!(language_of("g.json"), Language::Json);
        assert_eq!(language_of("h.yaml"), Language::Yaml);
        assert_eq!(language_of("i.yml"), Language::Yaml);
        assert_eq!(language_of("j.sql"), Language::Sql);
        assert_eq!(language_of("k.scss"), Language::Css);
        assert_eq!(language_of("l.html"), Language::Html);
        assert_eq!(language_of("m.sh"), Language::Shell);
        assert_eq!(language_of("n.xyz"), Language::Other("xyz".to_owned()));
        assert_eq!(language_of("no-extension"), Language::Other(String::new()));
        // Round trip through the stored name.
        for lang in [
            Language::Rust,
            Language::TypeScript,
            Language::Tsx,
            Language::JavaScript,
            Language::Python,
            Language::Markdown,
            Language::Toml,
            Language::Json,
            Language::Yaml,
            Language::Sql,
            Language::Css,
            Language::Html,
            Language::Shell,
        ] {
            assert_eq!(language_from_name(lang.as_str()), lang);
        }
        assert_eq!(
            language_from_name("kotlin"),
            Language::Other("kotlin".to_owned())
        );
    }

    #[test]
    fn scan_is_deterministic_and_change_detection_works() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(&root.join("src").join("a.rs"), "fn a() {}\n");
        write(&root.join("src").join("b.rs"), "fn b() {}\n");
        write(&root.join("README.md"), "# readme\n");

        let store = store_in(root);
        let options = ScanOptions::default();

        let first = store.file_index().scan(&options).expect("scan");
        assert_eq!(first.added.len(), 3);
        assert!(first.changed.is_empty() && first.deleted.is_empty());
        let entries = store.file_index().entries().expect("entries");
        assert_eq!(entries.len(), 3);
        assert!(entries.iter().all(|e| e.dirty));

        // Determinism: re-hashing unchanged content yields identical entries.
        let second = store.file_index().scan(&options).expect("rescan full");
        assert!(second.added.is_empty() && second.changed.is_empty());
        assert_eq!(
            store.file_index().entries().expect("entries"),
            entries,
            "hashing must be deterministic"
        );

        // Incremental: untouched files take the fast path; a modified file
        // (size changes) is re-hashed and flagged.
        write(
            &root.join("src").join("a.rs"),
            "fn a() { renamed() }\n// longer\n",
        );
        let third = store.file_index().rescan(&options).expect("rescan");
        assert_eq!(third.reused_fast_path, 2);
        assert_eq!(third.hashed, 1);
        assert_eq!(third.changed, vec!["src/a.rs".to_owned()]);
        assert!(
            store
                .file_index()
                .entry("src/a.rs")
                .expect("entry")
                .expect("present")
                .dirty
        );

        // mark_parsed clears the dirty flag and bumps the parse version.
        store.file_index().mark_parsed("src/a.rs").expect("parsed");
        let parsed = store
            .file_index()
            .entry("src/a.rs")
            .expect("entry")
            .expect("present");
        assert!(!parsed.dirty);
        assert_eq!(parsed.parsed_version, 2);
    }

    #[test]
    fn verify_all_catches_mtime_spoofed_same_size_change() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let file = root.join("spoof.rs");
        write(&file, "fn one() {}\n"); // 12 bytes

        let store = store_in(root);
        let options = ScanOptions::default();
        store.file_index().scan(&options).expect("scan");

        // Record the mtime the cache stored, then rewrite the file with
        // same-length content and restore the old mtime: the fast path will
        // be fooled, verify_all must not be.
        let cached = store.file_index().load_cached().expect("cached");
        let (secs, nanos) = {
            let row = cached.get("spoof.rs").expect("cached row");
            (row.mtime_secs, row.mtime_nanos)
        };
        fs::write(&file, "fn two() {}\n").expect("rewrite"); // also 12 bytes
        let handle = fs::File::options().write(true).open(&file).expect("open");
        handle
            .set_times(fs::FileTimes::new().set_modified(mtime_from_parts(secs, nanos)))
            .expect("restore mtime");
        drop(handle);

        let rescan = store.file_index().rescan(&options).expect("rescan");
        assert_eq!(
            rescan.reused_fast_path, 1,
            "spoofed mtime fools the fast path (by design)"
        );
        assert!(rescan.changed.is_empty());

        let verify = store.file_index().verify_all(&options).expect("verify");
        assert_eq!(verify.hashed, 1);
        assert_eq!(verify.changed, vec!["spoof.rs".to_owned()]);
    }

    #[test]
    fn scan_skips_ignored_directories_binary_and_oversize_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(&root.join("src").join("ok.rs"), "fn ok() {}\n");
        write(&root.join(".git").join("objects").join("x.txt"), "git\n");
        write(&root.join("target").join("debug").join("x.exe"), "binary\n");
        write(&root.join("node_modules").join("pkg").join("i.js"), "x\n");
        write(&root.join("logo.png"), "not really png\n");
        write(&root.join("huge.rs"), &"x".repeat(64));

        let options = ScanOptions {
            max_file_bytes: 32,
            ..ScanOptions::default()
        };
        let store = store_in(root);
        let report = store.file_index().scan(&options).expect("scan");

        let entries = store.file_index().entries().expect("entries");
        assert_eq!(entries.len(), 1, "only src/ok.rs is indexed");
        assert_eq!(entries[0].path, "src/ok.rs");
        assert_eq!(
            report.skipped_filter, 2,
            "logo.png (binary) + huge.rs (oversize)"
        );
        assert_eq!(
            report.files_on_disk, 3,
            "ignored directories are pruned from the walk"
        );
    }

    #[test]
    fn rescan_tracks_new_and_deleted_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(&root.join("keep.rs"), "fn keep() {}\n");
        write(&root.join("gone.rs"), "fn gone() {}\n");

        let store = store_in(root);
        let options = ScanOptions::default();
        store.file_index().scan(&options).expect("scan");

        fs::remove_file(root.join("gone.rs")).expect("remove");
        write(&root.join("new.rs"), "fn new() {}\n");
        let report = store.file_index().rescan(&options).expect("rescan");

        assert_eq!(report.added, vec!["new.rs".to_owned()]);
        assert_eq!(report.deleted, vec!["gone.rs".to_owned()]);
        assert!(report.changed.is_empty());
        assert!(store
            .file_index()
            .entry("gone.rs")
            .expect("entry")
            .is_none());
        assert!(store.file_index().entry("new.rs").expect("entry").is_some());
    }

    #[test]
    fn normalize_rel_and_is_under_helpers() {
        assert_eq!(normalize_rel("\\src\\main.rs"), "src/main.rs");
        assert_eq!(normalize_rel("./a/b.rs"), "a/b.rs");
        assert_eq!(normalize_rel("/leading.rs"), "leading.rs");
        assert!(is_under("src/a.rs", "src"));
        assert!(is_under("src", "src"));
        assert!(!is_under("srcx/a.rs", "src"));
        assert!(is_under("anything", ""));
    }
}
