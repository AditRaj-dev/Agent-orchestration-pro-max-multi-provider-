//! Code ownership map (PRD §12 GIT-05): exclusive and advisory locks over
//! path-glob sets so two agents never unknowingly edit the same
//! high-conflict area.
//!
//! F-09 keeps the map in memory; persistence (crash-timeout release,
//! journal replay) arrives with the daemon event journal. Release on
//! completion/cancellation is [`OwnershipMap::release`]; crash-timeout
//! release is the daemon's lease-expiry job once the map is journaled.

use std::sync::Mutex;

use crate::store::{lock_guard, now_ts};

/// Whether a hold excludes every other task (`Exclusive`) or merely warns
/// (`Advisory`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldKind {
    /// Conflicts with every overlapping hold (advisory or exclusive).
    Exclusive,
    /// Conflicts only with overlapping exclusive holds.
    Advisory,
}

impl HoldKind {
    fn as_str(self) -> &'static str {
        match self {
            HoldKind::Exclusive => "exclusive",
            HoldKind::Advisory => "advisory",
        }
    }
}

impl std::fmt::Display for HoldKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One held path glob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    /// Task holding the glob.
    pub task_id: String,
    /// Normalized path glob (forward slashes, no leading `./`).
    pub glob: String,
    /// Hold strength.
    pub kind: HoldKind,
    /// Acquisition time (RFC 3339 UTC).
    pub acquired_at: String,
}

/// Why an [`OwnershipMap::acquire`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OwnershipError {
    /// The requested hold overlaps an incompatible existing hold:
    /// exclusive requests conflict with any overlapping hold; advisory
    /// requests conflict only with exclusive ones.
    #[error(
        "ownership conflict: task `{task_id}` cannot acquire a {desired} hold on `{glob}` \
         because task `{held_by}` already holds a {held} hold on `{held_glob}`"
    )]
    Conflict {
        /// Task that requested the hold.
        task_id: String,
        /// Glob the task wanted.
        glob: String,
        /// Strength the task wanted.
        desired: HoldKind,
        /// Task that already holds the overlapping glob.
        held_by: String,
        /// The overlapping held glob.
        held_glob: String,
        /// Strength of the held glob.
        held: HoldKind,
    },
    /// A path glob failed validation (empty or whitespace).
    #[error("invalid path glob: `{0}` (must be a non-empty repo-relative pattern)")]
    InvalidGlob(String),
    /// The task id failed validation.
    #[error("invalid task id: `{0}`")]
    InvalidTask(String),
}

/// In-memory ownership map with exclusive/advisory locking over path globs.
///
/// Sharing across threads is by `&self` (internal mutex), so the daemon can
/// hold one map for the whole process.
#[derive(Debug, Default)]
pub struct OwnershipMap {
    holds: Mutex<Vec<Hold>>,
}

impl OwnershipMap {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire holds on `path_globs` for `task_id`.
    ///
    /// Conflict rules (GIT-05):
    ///
    /// - an **exclusive** request conflicts with any overlapping hold,
    ///   advisory or exclusive, held by another task;
    /// - an **advisory** request conflicts only with overlapping
    ///   **exclusive** holds;
    /// - holds never conflict with the task's own earlier holds; re-acquiring
    ///   a glob the task already holds replaces it (strength upgrade or
    ///   downgrade) instead of duplicating it.
    ///
    /// All-or-nothing: on conflict, nothing is acquired.
    pub fn acquire(
        &self,
        task_id: &str,
        path_globs: &[&str],
        exclusive: bool,
    ) -> Result<(), OwnershipError> {
        if task_id.trim().is_empty() {
            return Err(OwnershipError::InvalidTask(task_id.to_string()));
        }
        let desired = if exclusive {
            HoldKind::Exclusive
        } else {
            HoldKind::Advisory
        };
        let mut wants = Vec::with_capacity(path_globs.len());
        for glob in path_globs {
            let normalized = normalize_glob(glob);
            if normalized.is_empty() {
                return Err(OwnershipError::InvalidGlob((*glob).to_string()));
            }
            wants.push(normalized);
        }

        let mut holds = lock_guard(&self.holds);
        for want in &wants {
            for held in holds.iter() {
                if held.task_id == task_id {
                    continue;
                }
                let incompatible = held.kind == HoldKind::Exclusive || exclusive;
                if incompatible && glob_overlap(&held.glob, want) {
                    return Err(OwnershipError::Conflict {
                        task_id: task_id.to_string(),
                        glob: want.clone(),
                        desired,
                        held_by: held.task_id.clone(),
                        held_glob: held.glob.clone(),
                        held: held.kind,
                    });
                }
            }
        }
        for want in wants {
            holds.retain(|held| !(held.task_id == task_id && held.glob == want));
            holds.push(Hold {
                task_id: task_id.to_string(),
                glob: want,
                kind: desired,
                acquired_at: now_ts(),
            });
        }
        Ok(())
    }

    /// Release every hold of `task_id` (completion/cancellation path).
    /// Returns whether any hold was dropped.
    pub fn release(&self, task_id: &str) -> bool {
        let mut holds = lock_guard(&self.holds);
        let before = holds.len();
        holds.retain(|held| held.task_id != task_id);
        before != holds.len()
    }

    /// All holds whose globs overlap `glob` (a concrete path is a legal,
    /// degenerate glob). Invalid globs match nothing.
    pub fn holds_for_path(&self, glob: &str) -> Vec<Hold> {
        let normalized = normalize_glob(glob);
        if normalized.is_empty() {
            return Vec::new();
        }
        let holds = lock_guard(&self.holds);
        holds
            .iter()
            .filter(|held| glob_overlap(&held.glob, &normalized))
            .cloned()
            .collect()
    }

    /// All holds belonging to `task_id`.
    pub fn holds_for_task(&self, task_id: &str) -> Vec<Hold> {
        let holds = lock_guard(&self.holds);
        holds
            .iter()
            .filter(|held| held.task_id == task_id)
            .cloned()
            .collect()
    }
}

/// Normalize a glob/path: backslashes to forward slashes (Windows reference
/// platform), strip a leading `./` and surrounding separators.
fn normalize_glob(glob: &str) -> String {
    glob.replace('\\', "/")
        .trim()
        .trim_start_matches("./")
        .trim_matches('/')
        .to_string()
}

/// Whether `pattern` matches `path`.
///
/// Supported grammar (deliberately small — see the F-doc for limitations):
///
/// - `*` matches zero or more characters **within one path segment**;
/// - `**` as a whole segment matches zero or more whole segments;
/// - everything else matches literally.
///
/// Note one approximation versus gitignore: `src/**` also matches `src`
/// itself, because `**` may expand to zero segments.
pub fn glob_matches(pattern: &str, path: &str) -> bool {
    let normalized_pattern = normalize_glob(pattern);
    let normalized_path = normalize_glob(path);
    let pattern_segs: Vec<&str> = normalized_pattern.split('/').collect();
    let path_segs: Vec<&str> = normalized_path.split('/').collect();
    match_segments(&pattern_segs, &path_segs)
}

/// Whether some concrete path can match both `a` and `b` — i.e. two tasks
/// holding these globs may touch the same file.
///
/// Exact for the `*`/`**` grammar except within a single segment: any
/// segment containing `*` is treated as potentially overlapping any other
/// segment (conservative — it can report a conflict where none is possible,
/// e.g. `a*` vs `b*`, but never misses a real one).
pub fn glob_overlap(a: &str, b: &str) -> bool {
    let normalized_a = normalize_glob(a);
    let normalized_b = normalize_glob(b);
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
    fn double_star_spans_segments_single_star_does_not() {
        assert!(glob_matches("src/**", "src/core/mod.rs"));
        assert!(glob_matches("src/**", "src"));
        assert!(!glob_matches("src/*", "src/core/mod.rs"));
        assert!(glob_matches("src/*", "src/mod.rs"));
        assert!(glob_matches("*.lock", "Cargo.lock"));
        assert!(!glob_matches("src/**", "docs/readme.md"));
        assert!(glob_matches("**", "any/thing/at/all.rs"));
        assert!(glob_matches("a/b", "a/b"));
        assert!(!glob_matches("a/b", "a/bb"));
    }

    #[test]
    fn windows_separators_are_normalized() {
        assert!(glob_matches("src\\**", "src\\core\\mod.rs"));
        assert!(glob_matches("./src/**", "src/x.rs"));
    }

    #[test]
    fn overlap_is_exact_for_concrete_and_directory_globs() {
        assert!(glob_overlap("src/**", "src/core/mod.rs"));
        assert!(glob_overlap("src/**", "src"));
        assert!(glob_overlap("src/*", "src/mod.rs"));
        assert!(!glob_overlap("src/**", "docs/**"));
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
}
