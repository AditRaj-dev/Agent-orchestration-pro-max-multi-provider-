//! Shared helpers for agentos-git integration tests: real `git` CLI on
//! auto-cleaning temp directories.

#![allow(dead_code)]

use std::fs;
use std::path::Path;

use agentos_git::cli;

/// Create a temp repository with one seed commit; returns the dir (keeping
/// it alive) and the seed commit's sha (the initial integration head).
pub fn init_repo() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    cli::init(dir.path()).expect("git init");
    write_file(dir.path(), "README.md", "agentos-git test repository\n");
    let head = commit(dir.path(), "seed-agent", "seed commit");
    (dir, head)
}

/// Write a file into `repo`.
pub fn write_file(repo: &Path, name: &str, contents: &str) {
    fs::write(repo.join(name), contents).expect("write file");
}

/// Stage everything and commit as `author`; returns the new head sha.
pub fn commit(repo: &Path, author: &str, message: &str) -> String {
    cli::add_all_and_commit(repo, author, message).expect("git commit")
}
