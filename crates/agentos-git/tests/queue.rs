//! GIT-01/GIT-02 integration: FIFO ordering, strict single-consumer leases,
//! lease-expiry reclaim, stale-base rejection, and approval-gated push.

mod common;

use agentos_git::cli;
use agentos_git::error::GitError;
use agentos_git::queue::{MutationAction, MutationQueue, RequestStatus};
use chrono::Duration;

#[test]
fn fifo_order_with_single_consumer_enforcement() {
    let (dir, base) = common::init_repo();
    let queue = MutationQueue::open(&dir.path().join("queue.sqlite3")).expect("open queue");

    let a = queue
        .enqueue(dir.path(), "task-a", &base, MutationAction::Commit, false)
        .expect("enqueue a");
    let b = queue
        .enqueue(dir.path(), "task-b", &base, MutationAction::Merge, false)
        .expect("enqueue b");
    let c = queue
        .enqueue(dir.path(), "task-c", &base, MutationAction::Rebase, false)
        .expect("enqueue c");

    // only the oldest pending item is claimable
    let first = queue
        .claim_next(dir.path(), "consumer-1", Duration::minutes(10))
        .expect("claim")
        .expect("oldest item");
    assert_eq!(first.id, a);
    assert_eq!(first.status, RequestStatus::InProgress);
    assert_eq!(first.lease_owner.as_deref(), Some("consumer-1"));
    assert!(first.lease_expires_at.is_some());

    // live lease: NO other claim while it holds, even with b/c pending
    assert!(queue
        .claim_next(dir.path(), "consumer-2", Duration::minutes(10))
        .expect("claim")
        .is_none());

    assert!(queue
        .complete(&a, Some("deadbeefdeadbeef"))
        .expect("complete"));
    let done = queue.get(&a).expect("get").expect("row");
    assert_eq!(done.status, RequestStatus::Done);
    assert_eq!(done.result_sha.as_deref(), Some("deadbeefdeadbeef"));
    assert_eq!(done.lease_owner, None);

    // completing a terminal row again is a no-op
    assert!(!queue.complete(&a, None).expect("complete again"));

    let second = queue
        .claim_next(dir.path(), "consumer-2", Duration::minutes(10))
        .expect("claim")
        .expect("next item");
    assert_eq!(second.id, b);
    assert!(queue.reject(&b, "manual review rejection").expect("reject"));
    let rejected = queue.get(&b).expect("get").expect("row");
    assert_eq!(rejected.status, RequestStatus::Rejected);
    assert_eq!(rejected.error.as_deref(), Some("manual review rejection"));

    let third = queue
        .claim_next(dir.path(), "consumer-2", Duration::minutes(10))
        .expect("claim")
        .expect("last item");
    assert_eq!(third.id, c);
    assert!(queue.complete(&c, None).expect("complete"));

    // queue drained
    assert!(queue
        .claim_next(dir.path(), "consumer-2", Duration::minutes(10))
        .expect("claim")
        .is_none());
}

#[test]
fn expired_lease_is_reclaimed_by_next_consumer() {
    let (dir, base) = common::init_repo();
    let queue = MutationQueue::open(&dir.path().join("queue.sqlite3")).expect("open queue");

    let a = queue
        .enqueue(dir.path(), "task-a", &base, MutationAction::Commit, false)
        .expect("enqueue a");
    let b = queue
        .enqueue(dir.path(), "task-b", &base, MutationAction::Commit, false)
        .expect("enqueue b");

    // zero-ttl lease: expires the moment it is granted
    let first = queue
        .claim_next(dir.path(), "consumer-1", Duration::zero())
        .expect("claim")
        .expect("item");
    assert_eq!(first.id, a);

    // the dead lease is reclaimed and the SAME item re-leased
    let reclaimed = queue
        .claim_next(dir.path(), "consumer-2", Duration::minutes(10))
        .expect("claim")
        .expect("reclaimed item");
    assert_eq!(reclaimed.id, a);
    assert_eq!(reclaimed.lease_owner.as_deref(), Some("consumer-2"));

    // consumer-2's live lease now blocks everyone
    assert!(queue
        .claim_next(dir.path(), "consumer-3", Duration::minutes(10))
        .expect("claim")
        .is_none());

    assert!(queue.complete(&a, None).expect("complete"));
    let next = queue
        .claim_next(dir.path(), "consumer-3", Duration::minutes(10))
        .expect("claim")
        .expect("next item");
    assert_eq!(next.id, b);
}

#[test]
fn stale_base_is_rejected_when_integration_moves_on() {
    let (dir, base) = common::init_repo();
    let repo = dir.path();
    let queue = MutationQueue::open(&repo.join("queue.sqlite3")).expect("open queue");

    let id = queue
        .enqueue(repo, "task-a", &base, MutationAction::Merge, false)
        .expect("enqueue");
    let claimed = queue
        .claim_next(repo, "consumer-1", Duration::minutes(10))
        .expect("claim")
        .expect("item");
    assert_eq!(claimed.id, id);

    // base == head: fresh
    assert!(queue.stale_base_check(&id, &base).expect("stale check"));

    // the integration branch moves forward after enqueue
    common::write_file(repo, "feature.txt", "frontend integration\n");
    let head = common::commit(repo, "integration-agent", "integrate frontend");

    // the recorded base is no longer an ancestor of head -> stale_base
    assert!(!queue.stale_base_check(&id, &head).expect("stale check"));
    let row = queue.get(&id).expect("get").expect("row");
    assert_eq!(row.status, RequestStatus::Rejected);
    assert_eq!(row.error.as_deref(), Some("stale_base"));

    // rejected items never return to the queue
    assert!(queue
        .claim_next(repo, "consumer-1", Duration::minutes(10))
        .expect("claim")
        .is_none());

    // the diff helper surfaces what moved between base and head
    let changed = cli::diff_name_only(repo, &base, &head).expect("diff");
    assert!(changed.iter().any(|path| path == "feature.txt"));

    // unknown ids surface as NotFound
    let err = queue
        .stale_base_check("no-such-id", &head)
        .expect_err("missing row");
    assert!(matches!(
        err,
        GitError::Core(agentos_core::CoreError::NotFound(_))
    ));
}

#[test]
fn push_is_approval_gated_by_default() {
    let (dir, base) = common::init_repo();
    let db_path = dir.path().join("queue.sqlite3");

    let queue = MutationQueue::open(&db_path).expect("open queue");
    assert!(!queue.allow_unapproved_push().expect("default flag"));

    // unapproved push is rejected in code, not by prompting
    let err = queue
        .enqueue(dir.path(), "task-a", &base, MutationAction::Push, false)
        .expect_err("unapproved push");
    assert!(matches!(err, GitError::PushNotApproved));

    // non-push actions never need approval
    queue
        .enqueue(dir.path(), "task-b", &base, MutationAction::Commit, false)
        .expect("commit enqueue");

    // explicit override unlocks unapproved push
    queue.set_allow_unapproved_push(true).expect("set override");
    assert!(queue.allow_unapproved_push().expect("flag"));
    let unapproved_id = queue
        .enqueue(dir.path(), "task-c", &base, MutationAction::Push, false)
        .expect("enqueue under override");
    let row = queue.get(&unapproved_id).expect("get").expect("row");
    assert_eq!(row.action, MutationAction::Push);
    assert!(!row.approved, "unapproved flag is still recorded for audit");

    // override is persisted across reopen
    drop(queue);
    let reopened = MutationQueue::open(&db_path).expect("reopen");
    assert!(reopened.allow_unapproved_push().expect("persisted flag"));
    reopened
        .enqueue(dir.path(), "task-d", &base, MutationAction::Push, false)
        .expect("enqueue under persisted override");

    // gating back on: approval is still accepted, absence is not
    reopened
        .set_allow_unapproved_push(false)
        .expect("reset override");
    reopened
        .enqueue(dir.path(), "task-e", &base, MutationAction::Push, true)
        .expect("approved push enqueues");
    let err = reopened
        .enqueue(dir.path(), "task-f", &base, MutationAction::Push, false)
        .expect_err("unapproved push");
    assert!(matches!(err, GitError::PushNotApproved));
}

#[test]
fn queues_of_different_repos_are_independent() {
    let (dir_a, base_a) = common::init_repo();
    let (dir_b, base_b) = common::init_repo();
    let queue = MutationQueue::open(&dir_a.path().join("queue.sqlite3")).expect("open queue");

    let a = queue
        .enqueue(
            dir_a.path(),
            "task-a",
            &base_a,
            MutationAction::Commit,
            false,
        )
        .expect("enqueue a");
    let b = queue
        .enqueue(
            dir_b.path(),
            "task-b",
            &base_b,
            MutationAction::Commit,
            false,
        )
        .expect("enqueue b");

    // a live lease on repo A does not block repo B
    let claimed_a = queue
        .claim_next(dir_a.path(), "consumer-1", Duration::minutes(10))
        .expect("claim")
        .expect("item a");
    assert_eq!(claimed_a.id, a);
    let claimed_b = queue
        .claim_next(dir_b.path(), "consumer-2", Duration::minutes(10))
        .expect("claim")
        .expect("item b");
    assert_eq!(claimed_b.id, b);
}
