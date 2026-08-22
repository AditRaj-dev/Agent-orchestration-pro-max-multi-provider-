//! GIT-04 integration: worktree create / branch-name format / list /
//! remove round-trip on a real temp repository driven by the `git` CLI.

mod common;

use std::fs;
use std::path::{Path, PathBuf};

use agentos_git::cli;
use agentos_git::worktree::{RetentionRules, WorktreeManager};
use chrono::Duration;
use uuid::Uuid;

fn canon(path: &Path) -> PathBuf {
    fs::canonicalize(path).expect("canonicalize")
}

#[test]
fn worktree_create_branch_name_list_remove_round_trip() {
    let (dir, base) = common::init_repo();
    let repo = dir.path();
    let manager = WorktreeManager::new(repo);
    let run_id = Uuid::new_v4().to_string();
    let task_id = Uuid::new_v4().to_string();

    let worktree = manager
        .create(&run_id, &task_id, &base)
        .expect("create worktree");

    // branch name is EXACTLY agentos/<run8>/<task8>, uuid hex TAILS
    let run_hex = Uuid::parse_str(&run_id).unwrap().simple().to_string();
    let task_hex = Uuid::parse_str(&task_id).unwrap().simple().to_string();
    let run_short = &run_hex[run_hex.len() - 8..];
    let task_short = &task_hex[task_hex.len() - 8..];
    assert_eq!(worktree.branch, format!("agentos/{run_short}/{task_short}"));

    // path lives under <repo>/.agentos-worktrees/<task_id>
    assert!(worktree.path.starts_with(repo.join(".agentos-worktrees")));
    assert!(worktree.path.ends_with(&task_id));
    assert!(worktree.path.is_dir());

    // checks out the generated branch at the base commit
    assert!(cli::branch_exists(repo, &worktree.branch).expect("branch_exists"));
    assert_eq!(cli::rev_parse_head(&worktree.path).expect("head"), base);
    assert_eq!(
        cli::current_branch(&worktree.path).expect("branch"),
        worktree.branch
    );

    // the worktree dir is hidden via .git/info/exclude (never .gitignore)
    let exclude = fs::read_to_string(repo.join(".git").join("info").join("exclude"))
        .expect("exclude readable");
    assert!(exclude
        .lines()
        .any(|line| line.trim() == ".agentos-worktrees/"));

    // list sees it
    let listed = manager.list().expect("list");
    let worktree_canon = canon(&worktree.path);
    assert!(listed
        .iter()
        .any(|entry| canon(&entry.path) == worktree_canon));

    // dirty worktree forces the --force removal fallback
    fs::write(worktree.path.join("scratch.txt"), "untracked\n").expect("dirty the worktree");
    manager
        .remove(&worktree.path)
        .expect("remove (force fallback)");
    let after = manager.list().expect("list");
    assert!(!after
        .iter()
        .any(|entry| canon(&entry.path) == worktree_canon));

    // branch and commits survive removal — GC is retention-gated (GIT-04)
    assert!(cli::branch_exists(repo, &worktree.branch).expect("branch_exists"));
}

#[test]
fn two_tasks_get_isolated_worktrees_and_gc_honors_retention() {
    let (dir, base) = common::init_repo();
    let repo = dir.path();
    let manager = WorktreeManager::new(repo);
    let run_id = Uuid::new_v4().to_string();

    let first = manager
        .create(&run_id, &Uuid::new_v4().to_string(), &base)
        .expect("first worktree");
    let second = manager
        .create(&run_id, &Uuid::new_v4().to_string(), &base)
        .expect("second worktree");
    assert_ne!(first.path, second.path);
    assert_ne!(first.branch, second.branch);
    assert_eq!(manager.list().expect("list").len(), 3); // main + two worktrees

    let find_entry = |path: &Path| {
        manager
            .list()
            .expect("list")
            .into_iter()
            .find(|entry| canon(&entry.path) == canon(path))
            .expect("worktree listed")
    };

    // GC eligible under empty retention rules
    let entry = find_entry(&first.path);
    assert!(manager
        .gc_eligible(&entry, &RetentionRules::default())
        .expect("gc check"));

    // too young for a 1-hour minimum age
    let aged = RetentionRules {
        min_age: Duration::hours(1),
        require_merged_into: None,
    };
    assert!(!manager.gc_eligible(&entry, &aged).expect("gc check"));

    // unmerged work in the worktree blocks GC under require_merged_into
    common::write_file(&first.path, "feature.txt", "task output\n");
    let work_head = common::commit(&first.path, "worker-agent", "task output");
    let entry = find_entry(&first.path);
    let unmerged = RetentionRules {
        min_age: Duration::zero(),
        require_merged_into: Some(base.clone()),
    };
    assert!(!manager.gc_eligible(&entry, &unmerged).expect("gc check"));

    // merged work (head reachable from the target) is eligible again
    let merged = RetentionRules {
        min_age: Duration::zero(),
        require_merged_into: Some(work_head),
    };
    assert!(manager.gc_eligible(&entry, &merged).expect("gc check"));

    manager.remove(&first.path).expect("remove first");
    manager.remove(&second.path).expect("remove second");
}
