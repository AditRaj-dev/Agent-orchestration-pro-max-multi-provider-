//! GIT-05 integration: exclusive vs advisory holds, conflicts, release.

mod common;

use agentos_git::ownership::{HoldKind, OwnershipError, OwnershipMap};

#[test]
fn exclusive_conflicts_with_everything_advisory_coexists() {
    let map = OwnershipMap::new();

    map.acquire("task-a", &["src/**"], true)
        .expect("exclusive src");

    // advisory on an overlapping path conflicts with the exclusive hold
    let err = map
        .acquire("task-b", &["src/core/**"], false)
        .expect_err("advisory vs exclusive");
    match &err {
        OwnershipError::Conflict {
            task_id,
            glob,
            desired,
            held_by,
            held_glob,
            held,
        } => {
            assert_eq!(task_id, "task-b");
            assert_eq!(glob, "src/core/**");
            assert_eq!(*desired, HoldKind::Advisory);
            assert_eq!(held_by, "task-a");
            assert_eq!(held_glob, "src/**");
            assert_eq!(*held, HoldKind::Exclusive);
        }
        other => panic!("expected conflict, got {other:?}"),
    }

    // a second exclusive hold conflicts too
    assert!(map.acquire("task-c", &["src/main.rs"], true).is_err());

    // disjoint areas are unaffected: advisory and exclusive alike
    map.acquire("task-b", &["docs/**"], false)
        .expect("advisory docs");
    map.acquire("task-c", &["apps/**"], true)
        .expect("exclusive apps");
}

#[test]
fn advisory_holds_stack_on_the_same_paths() {
    let map = OwnershipMap::new();
    map.acquire("task-a", &["src/**"], false)
        .expect("advisory a");
    map.acquire("task-b", &["src/core/**"], false)
        .expect("advisory b");

    // ...until someone requests exclusivity
    let err = map
        .acquire("task-c", &["src/core/mod.rs"], true)
        .expect_err("exclusive vs advisory");
    assert!(matches!(err, OwnershipError::Conflict { .. }));

    // the failed exclusive request acquired nothing
    assert!(map.holds_for_task("task-c").is_empty());
}

#[test]
fn release_frees_paths_and_reports_whether_it_dropped_holds() {
    let map = OwnershipMap::new();
    map.acquire("task-a", &["src/**"], true).expect("acquire");

    let overlapping = map.holds_for_path("src/core/mod.rs");
    assert_eq!(overlapping.len(), 1);
    assert_eq!(overlapping[0].task_id, "task-a");
    assert_eq!(overlapping[0].kind, HoldKind::Exclusive);

    assert!(map.release("task-a"));
    assert!(!map.release("task-a"), "second release drops nothing");
    assert!(map.holds_for_path("src/core/mod.rs").is_empty());

    // the path is now free for an exclusive hold by another task
    map.acquire("task-b", &["src/**"], true)
        .expect("acquire after release");
}

#[test]
fn same_task_may_reacquire_and_upgrade_its_own_holds() {
    let map = OwnershipMap::new();
    map.acquire("task-a", &["src/**"], false).expect("advisory");
    // no self-conflict: advisory -> exclusive upgrade replaces the hold
    map.acquire("task-a", &["src/**"], true).expect("upgrade");

    let holds = map.holds_for_task("task-a");
    assert_eq!(holds.len(), 1);
    assert_eq!(holds[0].kind, HoldKind::Exclusive);

    // while a DIFFERENT task still cannot overlap the upgraded hold
    assert!(map.acquire("task-b", &["src/x.rs"], false).is_err());
}

#[test]
fn invalid_inputs_are_rejected() {
    let map = OwnershipMap::new();
    assert!(matches!(
        map.acquire("task-a", &["  "], false),
        Err(OwnershipError::InvalidGlob(_))
    ));
    assert!(matches!(
        map.acquire("  ", &["src/**"], false),
        Err(OwnershipError::InvalidTask(_))
    ));
    // all-or-nothing: the valid glob in a batch with an invalid one is not kept
    assert!(map.acquire("task-a", &["docs/**", ""], false).is_err());
    assert!(map.holds_for_task("task-a").is_empty());
}
