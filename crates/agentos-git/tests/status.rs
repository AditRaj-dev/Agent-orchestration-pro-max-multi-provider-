//! `status_paths` must name individual files, including inside brand-new
//! untracked directories.
//!
//! Plain `git status --porcelain` collapses an untracked directory to a
//! single `dir/` entry. Anything classifying changes per file — the code
//! graph refresh decides whether to rebuild by looking for source
//! extensions — then sees `components/` with no extension and concludes
//! nothing relevant changed, even though a whole component tree was just
//! written. Observed live: a worker created `components/*.tsx` and the
//! refresh counted exactly one code file, the unrelated `vite.config.ts`.

mod common;

use std::fs;

use agentos_git::cli;

#[test]
fn untracked_directories_are_expanded_into_their_files() {
    let (dir, _head) = common::init_repo();
    let repo = dir.path();

    fs::create_dir_all(repo.join("components")).expect("components dir");
    fs::write(
        repo.join("components").join("Timer.tsx"),
        "export const Timer = () => null;\n",
    )
    .expect("Timer.tsx");
    fs::write(
        repo.join("components").join("Header.tsx"),
        "export const Header = () => null;\n",
    )
    .expect("Header.tsx");

    let paths = cli::status_paths(repo).expect("status");

    assert!(
        paths.iter().any(|path| path.ends_with("Timer.tsx")),
        "a file inside a new directory must be named individually: {paths:?}"
    );
    assert!(
        paths.iter().any(|path| path.ends_with("Header.tsx")),
        "every file in the new directory is reported: {paths:?}"
    );
    assert!(
        !paths.iter().any(|path| path == "components/"),
        "the bare directory entry is what hid the files: {paths:?}"
    );
}

#[test]
fn tracked_modifications_are_still_reported() {
    let (dir, _head) = common::init_repo();
    let repo = dir.path();
    fs::write(repo.join("README.md"), "changed\n").expect("modify");

    let paths = cli::status_paths(repo).expect("status");
    assert!(
        paths.iter().any(|path| path.ends_with("README.md")),
        "{paths:?}"
    );
}

#[test]
fn a_clean_repository_reports_nothing() {
    let (dir, _head) = common::init_repo();
    assert!(cli::status_paths(dir.path()).expect("status").is_empty());
}
