//! Code ownership map (PRD §12 GIT-05): exclusive and advisory locks over
//! path-glob sets so two agents never unknowingly edit the same
//! high-conflict area.
//!
//! F-09 keeps the map in memory; persistence (crash-timeout release,
//! journal replay) arrives with the daemon event journal. Release on
//! completion/cancellation is [`OwnershipMap::release`]; crash-timeout
//! release is the daemon's lease-expiry job once the map is journaled.

use std::sync::atomic::{AtomicU64, Ordering};
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
    /// The glob is free right now, but another task has been waiting for it
    /// longer. Yielding to the older waiter is what stops a task from
    /// losing the race indefinitely; the oldest waiter never yields, so the
    /// queue always drains.
    #[error(
        "ownership queued: task `{task_id}` yields `{glob}` to task `{ahead_of}`,          which has been waiting longer"
    )]
    Queued {
        /// Task that requested the hold.
        task_id: String,
        /// Glob the task wanted.
        glob: String,
        /// The task ahead of it in the queue.
        ahead_of: String,
        /// How many tasks are queued ahead of this one.
        queued_ahead: usize,
    },
    /// A path glob failed validation (empty or whitespace).
    #[error("invalid path glob: `{0}` (must be a non-empty repo-relative pattern)")]
    InvalidGlob(String),
    /// The task id failed validation.
    #[error("invalid task id: `{0}`")]
    InvalidTask(String),
}

/// One task queued behind an overlapping hold.
///
/// Recorded the first time a task loses the race for a glob and kept until
/// that task acquires or releases, so "who has been waiting longest" is a
/// fact rather than a guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Waiter {
    /// Task waiting for the glob.
    pub task_id: String,
    /// Normalized glob it is waiting for.
    pub glob: String,
    /// Monotonic ticket; lower is older. Ties are impossible, which is why
    /// this is a counter rather than a timestamp.
    pub ticket: u64,
    /// First time this task queued for this glob (RFC 3339 UTC).
    pub since: String,
}

/// In-memory ownership map with exclusive/advisory locking over path globs.
///
/// Sharing across threads is by `&self` (internal mutex), so the daemon can
/// hold one map for the whole process.
///
/// Acquisition is **FIFO-fair**: a task that loses the race is recorded as a
/// waiter, and a task whose glob is free still yields while an older waiter
/// is queued for it. Without this, a serialized graph (every task holding
/// `**` under a shared workspace) lets the same task lose every round
/// indefinitely — observed as one task deferring 364 times while its peers
/// took turns. The oldest waiter is by construction never behind anyone, so
/// it always proceeds and the queue drains.
#[derive(Debug, Default)]
pub struct OwnershipMap {
    holds: Mutex<Vec<Hold>>,
    waiters: Mutex<Vec<Waiter>>,
    next_ticket: AtomicU64,
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

        // Lock order is always holds -> waiters.
        let mut holds = lock_guard(&self.holds);
        for want in &wants {
            for held in holds.iter() {
                if held.task_id == task_id {
                    continue;
                }
                let incompatible = held.kind == HoldKind::Exclusive || exclusive;
                if incompatible && glob_overlap(&held.glob, want) {
                    let conflict = OwnershipError::Conflict {
                        task_id: task_id.to_string(),
                        glob: want.clone(),
                        desired,
                        held_by: held.task_id.clone(),
                        held_glob: held.glob.clone(),
                        held: held.kind,
                    };
                    self.enqueue(task_id, &wants);
                    return Err(conflict);
                }
            }
        }

        // Nothing holds these globs, but someone may have been waiting for
        // them longer. Yield to them rather than jumping the queue.
        if let Some((ahead_of, queued_ahead, glob)) = self.older_waiter(task_id, &wants) {
            self.enqueue(task_id, &wants);
            return Err(OwnershipError::Queued {
                task_id: task_id.to_string(),
                glob,
                ahead_of,
                queued_ahead,
            });
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
        // The task is running now, so it is no longer waiting for anything.
        self.dequeue(task_id);
        Ok(())
    }

    /// Record `task_id` as waiting for each of `globs`, preserving the
    /// ticket of any glob it is already queued for — re-queuing must not
    /// send a task to the back of the line, or it could never reach the
    /// front.
    fn enqueue(&self, task_id: &str, globs: &[String]) {
        let mut waiters = lock_guard(&self.waiters);
        for glob in globs {
            let already = waiters
                .iter()
                .any(|waiter| waiter.task_id == task_id && waiter.glob == *glob);
            if already {
                continue;
            }
            waiters.push(Waiter {
                task_id: task_id.to_string(),
                glob: glob.clone(),
                ticket: self.next_ticket.fetch_add(1, Ordering::Relaxed),
                since: now_ts(),
            });
        }
    }

    /// Drop every queue entry for `task_id`.
    fn dequeue(&self, task_id: &str) {
        lock_guard(&self.waiters).retain(|waiter| waiter.task_id != task_id);
    }

    /// The oldest *other* task queued for a glob overlapping `wants`, when
    /// it is older than this task's own oldest ticket. Returns the task, how
    /// many distinct tasks are ahead, and the glob at issue.
    fn older_waiter(&self, task_id: &str, wants: &[String]) -> Option<(String, usize, String)> {
        let waiters = lock_guard(&self.waiters);
        // A task not yet queued is the newest arrival: everyone waiting for
        // an overlapping glob is ahead of it.
        let mine = waiters
            .iter()
            .filter(|waiter| waiter.task_id == task_id)
            .map(|waiter| waiter.ticket)
            .min()
            .unwrap_or(u64::MAX);
        let mut ahead: Vec<&Waiter> = waiters
            .iter()
            .filter(|waiter| waiter.task_id != task_id && waiter.ticket < mine)
            .filter(|waiter| wants.iter().any(|want| glob_overlap(&waiter.glob, want)))
            .collect();
        ahead.sort_by_key(|waiter| waiter.ticket);
        let first = ahead.first()?;
        let mut distinct: Vec<&str> = ahead.iter().map(|waiter| waiter.task_id.as_str()).collect();
        distinct.sort_unstable();
        distinct.dedup();
        Some((first.task_id.clone(), distinct.len(), first.glob.clone()))
    }

    /// Tasks currently queued for `glob`, oldest first (diagnostics).
    pub fn waiters_for(&self, glob: &str) -> Vec<Waiter> {
        let normalized = normalize_glob(glob);
        let mut queued: Vec<Waiter> = lock_guard(&self.waiters)
            .iter()
            .filter(|waiter| glob_overlap(&waiter.glob, &normalized))
            .cloned()
            .collect();
        queued.sort_by_key(|waiter| waiter.ticket);
        queued
    }

    /// Release every hold of `task_id` (completion/cancellation path).
    /// Returns whether any hold was dropped.
    pub fn release(&self, task_id: &str) -> bool {
        let dropped = {
            let mut holds = lock_guard(&self.holds);
            let before = holds.len();
            holds.retain(|held| held.task_id != task_id);
            before != holds.len()
        };
        // A finished task must not stay in the queue: a dead waiter at the
        // front would stall everyone behind it forever.
        self.dequeue(task_id);
        dropped
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
    /// The defect this queue exists for: under a shared workspace every task
    /// holds `**`, so one runs and the rest lose. Without fairness the same
    /// task can lose every single round — observed as 364 consecutive
    /// deferrals for one node while its peers took turns. The invariant is
    /// not "everyone wins equally", it is "nobody loses forever".
    #[test]
    fn contending_tasks_take_turns_instead_of_one_starving() {
        let map = OwnershipMap::new();
        let contenders = ["a", "b", "c"];
        let mut wins = [0_u32; 3];
        let mut worst_streak = [0_u32; 3];
        let mut streak = [0_u32; 3];

        for _ in 0..60 {
            for (index, task) in contenders.iter().enumerate() {
                match map.acquire(task, &["**"], true) {
                    Ok(()) => {
                        wins[index] += 1;
                        streak[index] = 0;
                        map.release(task);
                    }
                    Err(_) => {
                        streak[index] += 1;
                        worst_streak[index] = worst_streak[index].max(streak[index]);
                    }
                }
            }
        }

        for (index, task) in contenders.iter().enumerate() {
            assert!(
                wins[index] > 0,
                "{task} never acquired in 60 rounds: wins={wins:?}"
            );
            assert!(
                worst_streak[index] <= contenders.len() as u32,
                "{task} lost {} rounds in a row; the queue is not fair: {worst_streak:?}",
                worst_streak[index]
            );
        }
    }

    /// Yielding must never deadlock: with everyone queued, whoever is oldest
    /// still gets in, and releasing drains the queue in arrival order.
    #[test]
    fn the_queue_drains_in_arrival_order_and_never_deadlocks() {
        let map = OwnershipMap::new();
        map.acquire("holder", &["src/**"], true).unwrap();
        for task in ["a", "b", "c"] {
            assert!(map.acquire(task, &["src/**"], true).is_err());
        }
        assert_eq!(
            map.waiters_for("src/**")
                .iter()
                .map(|waiter| waiter.task_id.clone())
                .collect::<Vec<_>>(),
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
        );
        map.release("holder");
        for expected in ["a", "b", "c"] {
            map.acquire(expected, &["src/**"], true)
                .unwrap_or_else(|err| panic!("{expected} should be next: {err}"));
            map.release(expected);
        }
        assert!(map.waiters_for("src/**").is_empty());
    }

    /// A task that finishes must not linger in the queue — a dead waiter at
    /// the front would stall everyone behind it.
    #[test]
    fn releasing_removes_the_task_from_the_queue() {
        let map = OwnershipMap::new();
        map.acquire("holder", &["**"], true).unwrap();
        assert!(map.acquire("waiter", &["**"], true).is_err());
        assert_eq!(map.waiters_for("**").len(), 1);
        map.release("waiter");
        assert!(map.waiters_for("**").is_empty());
        map.release("holder");
        // With the queue empty, a fresh task acquires immediately.
        map.acquire("fresh", &["**"], true)
            .expect("no stale waiter blocks");
    }

    /// Non-overlapping globs are independent: a queue on one must not make
    /// an unrelated task yield.
    #[test]
    fn queuing_is_scoped_to_overlapping_globs() {
        let map = OwnershipMap::new();
        map.acquire("holder", &["src/api/**"], true).unwrap();
        assert!(map.acquire("queued", &["src/api/**"], true).is_err());
        map.acquire("elsewhere", &["docs/**"], true)
            .expect("an unrelated glob is unaffected by another glob's queue");
    }
}
