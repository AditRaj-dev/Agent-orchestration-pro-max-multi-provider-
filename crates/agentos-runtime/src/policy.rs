//! F-10 policy wiring for the supervisor — the enforcement half of the
//! composition root.
//!
//! `agentos-policy` owns the *decisions* (permission sets, fingerprint-bound
//! approvals, the append-only audit log); this module owns the *plumbing*
//! that makes the supervisor ask before it acts:
//!
//! - [`PolicyGate`] — the pair of policy stores (approvals + audit) plus the
//!   task-keyed record of which approval request covers which operation, so
//!   a gate can tell *pending* from *denied* from *expired* rather than just
//!   "not approved".
//! - [`git_mutation_operation`] / [`human_approval_operation`] — the
//!   canonical operation payloads a human approves. They are pure functions
//!   of durable state (run/task/node ids, workflow id, repo, base commit),
//!   so a caller can compute the fingerprint **before** the gate runs (that
//!   is how a human approves ahead of time) and the gate recomputes exactly
//!   the same value when it executes. Any drift — a different base commit, a
//!   different task, a different action — is a different fingerprint and
//!   therefore not approved (SEC-04).
//! - [`derive_permissions`] / [`compile_constraints`] — the SEC-01 bridge:
//!   the task's [`PermissionSet`] compiled into [`SpawnConstraints`], the
//!   four policy-bearing fields of `agentos_adapters::SpawnSpec`.
//!
//! Every path here **fails closed**: a store error, a missing record, an
//! expired ttl, a mutated operation, or a contract that claims more than
//! policy grants all resolve to "not authorized", never to "proceed".

use std::path::{Path, PathBuf};

use agentos_policy::{
    compile_to_spawn_spec, glob_overlap, operation_fingerprint, ApprovalRequest, ApprovalStatus,
    ApprovalStore, AuditStore, Gate, PermissionSet, SpawnConstraints, TaskScope,
};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::contract::TaskContract;
use crate::error::RuntimeError;

/// Operation kind stamped into a git-mutation approval payload.
pub const OP_KIND_GIT: &str = "git_mutation";
/// Operation kind stamped into a human-approval-node payload.
pub const OP_KIND_HUMAN: &str = "human_approval";

/// Why a gate is (not) satisfied.
///
/// [`GateVerdict::Approved`] is the **only** authorizing value; every other
/// variant blocks. The variants exist so the supervisor can distinguish a
/// bounded wait (`Pending`) from a deterministic refusal (`Denied`,
/// `Expired`, `Mutated`) and journal an honest reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateVerdict {
    /// A live approval bound to this exact operation exists (fingerprint
    /// matches, ttl not elapsed).
    Approved {
        /// Fingerprint the approval is bound to.
        fingerprint: String,
    },
    /// A request for this exact operation is waiting for a human.
    Pending {
        /// The tracked request id.
        request_id: String,
    },
    /// A human refused this operation.
    Denied {
        /// The tracked request id.
        request_id: String,
    },
    /// The approval was granted but its ttl has run out (checked at read
    /// time — a wall-clock jump cannot resurrect it).
    Expired {
        /// The tracked request id.
        request_id: String,
    },
    /// The tracked approval covers a **different** operation: the mutation
    /// changed after it was approved.
    Mutated {
        /// The tracked request id.
        request_id: String,
        /// Fingerprint the tracked request was bound to.
        approved_fingerprint: String,
    },
    /// No approval has ever been requested for this operation.
    Missing,
}

impl GateVerdict {
    /// Whether the operation is authorized. The single place "may I act?"
    /// is answered.
    pub fn is_approved(&self) -> bool {
        matches!(self, GateVerdict::Approved { .. })
    }

    /// Whether the gate is merely waiting for a human (a bounded wait) as
    /// opposed to having been refused.
    pub fn is_waiting(&self) -> bool {
        matches!(self, GateVerdict::Pending { .. } | GateVerdict::Missing)
    }

    /// Stable snake_case reason string for journal payloads and audit rows.
    pub fn reason(&self) -> &'static str {
        match self {
            GateVerdict::Approved { .. } => "approved",
            GateVerdict::Pending { .. } => "pending",
            GateVerdict::Denied { .. } => "denied",
            GateVerdict::Expired { .. } => "expired",
            GateVerdict::Mutated { .. } => "operation_mutated",
            GateVerdict::Missing => "no_approval_requested",
        }
    }

    /// The tracked request id, when the verdict has one.
    pub fn request_id(&self) -> Option<&str> {
        match self {
            GateVerdict::Pending { request_id }
            | GateVerdict::Denied { request_id }
            | GateVerdict::Expired { request_id }
            | GateVerdict::Mutated { request_id, .. } => Some(request_id),
            GateVerdict::Approved { .. } | GateVerdict::Missing => None,
        }
    }
}

/// The supervisor's handle on the F-10 stores.
///
/// The approval store is the authority on *whether* an operation is
/// approved; the task-keyed request records under `<state_dir>/approvals/`
/// are only a **pointer** (which request covers which task's operation) so a
/// blocked gate can report why. The record is never trusted for
/// authorization: [`PolicyGate::evaluate`] always asks
/// [`ApprovalStore::is_approved`] first, which re-checks fingerprint and
/// expiry in SQL.
#[derive(Debug)]
pub struct PolicyGate {
    approvals: ApprovalStore,
    audit: AuditStore,
    requests_dir: PathBuf,
}

impl PolicyGate {
    /// Open both policy databases and the request-record directory.
    pub fn open(
        approvals_db: &Path,
        audit_db: &Path,
        requests_dir: &Path,
    ) -> Result<Self, RuntimeError> {
        std::fs::create_dir_all(requests_dir)?;
        Ok(Self {
            approvals: ApprovalStore::open(approvals_db)?,
            audit: AuditStore::open(audit_db)?,
            requests_dir: requests_dir.to_path_buf(),
        })
    }

    /// The SEC-04 approval store (humans resolve requests through it).
    pub fn approvals(&self) -> &ApprovalStore {
        &self.approvals
    }

    /// The SEC-05 append-only audit log.
    pub fn audit(&self) -> &AuditStore {
        &self.audit
    }

    /// Classify `operation` against `gate` for `task_id`.
    ///
    /// Fail-closed order: the store's own `is_approved` (fingerprint +
    /// expiry, in SQL) decides authorization; the tracked record only
    /// explains a negative answer.
    pub fn evaluate(
        &self,
        gate: Gate,
        operation: &Value,
        task_id: &Uuid,
    ) -> Result<GateVerdict, RuntimeError> {
        let fingerprint = operation_fingerprint(operation);
        if self.approvals.is_approved(gate, operation)? {
            return Ok(GateVerdict::Approved { fingerprint });
        }
        let Some(tracked) = self.tracked(task_id) else {
            return Ok(GateVerdict::Missing);
        };
        if tracked.operation_fingerprint != fingerprint || tracked.gate != gate {
            return Ok(GateVerdict::Mutated {
                request_id: tracked.id,
                approved_fingerprint: tracked.operation_fingerprint,
            });
        }
        // The tracked request covers exactly this operation, yet the store
        // did not authorize it: either a human has not resolved it, refused
        // it, or the ttl elapsed after approval.
        Ok(match self.approvals.status(&tracked.id)? {
            Some(ApprovalStatus::Pending) => GateVerdict::Pending {
                request_id: tracked.id,
            },
            Some(ApprovalStatus::Denied) => GateVerdict::Denied {
                request_id: tracked.id,
            },
            Some(ApprovalStatus::Approved) => GateVerdict::Expired {
                request_id: tracked.id,
            },
            // The row is gone (a different installation's database): treat
            // the pointer as stale, never as an approval.
            None => GateVerdict::Missing,
        })
    }

    /// Request a human approval for `operation` and track it under
    /// `task_id`. Requests are immutable once resolved — a changed operation
    /// is a NEW request, never a rewrite (SEC-04).
    pub fn request(
        &self,
        gate: Gate,
        operation: &Value,
        task_id: &Uuid,
        requested_by: &str,
        ttl_secs: u64,
    ) -> Result<ApprovalRequest, RuntimeError> {
        let ttl = chrono::Duration::seconds(i64::try_from(ttl_secs).unwrap_or(i64::MAX));
        let request = self.approvals.request(gate, operation, requested_by, ttl)?;
        self.track(task_id, &request)?;
        Ok(request)
    }

    /// The request record tracked for `task_id`, if any. A corrupt or
    /// unreadable record reads as "none" — the fail-closed direction (the
    /// gate then requests a fresh approval instead of trusting a partial
    /// pointer).
    pub fn tracked(&self, task_id: &Uuid) -> Option<ApprovalRequest> {
        let bytes = std::fs::read(self.record_path(task_id)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    fn track(&self, task_id: &Uuid, request: &ApprovalRequest) -> Result<(), RuntimeError> {
        std::fs::write(
            self.record_path(task_id),
            serde_json::to_vec_pretty(request)?,
        )?;
        Ok(())
    }

    fn record_path(&self, task_id: &Uuid) -> PathBuf {
        self.requests_dir.join(format!("{task_id}.json"))
    }
}

/// The canonical operation payload of a git mutation. Pure function of
/// durable identity + the exact base commit the mutation assumes: rebasing
/// onto a new base, re-targeting another task, or switching the action all
/// change the fingerprint and therefore invalidate an existing approval.
pub fn git_mutation_operation(
    run_id: &Uuid,
    task_id: &Uuid,
    node_id: &str,
    workflow_id: &str,
    repo: &Path,
    action: &str,
    base_commit: &str,
) -> Value {
    json!({
        "kind": OP_KIND_GIT,
        "action": action,
        "repo": normalize_path(repo),
        "runId": run_id.to_string(),
        "taskId": task_id.to_string(),
        "node": node_id,
        "workflowId": workflow_id,
        "baseCommit": base_commit,
    })
}

/// The canonical operation payload of a `HumanApproval` node: the decision
/// a human is being asked to make, bound to the contract the run agreed to
/// (an amended objective or base commit is a different question).
pub fn human_approval_operation(
    run_id: &Uuid,
    task_id: &Uuid,
    node_id: &str,
    workflow_id: &str,
    contract: &TaskContract,
) -> Value {
    json!({
        "kind": OP_KIND_HUMAN,
        "runId": run_id.to_string(),
        "taskId": task_id.to_string(),
        "node": node_id,
        "workflowId": workflow_id,
        "contractId": contract.id,
        "contractVersion": contract.version,
        "objective": contract.objective,
        "baseCommit": contract.base_commit,
    })
}

/// The permission set a task executes under when no role mapping applies:
/// derived from the leased contract snapshot.
///
/// A contract with write scopes gets [`PermissionSet::worker_write`] over
/// exactly those globs (shell behind approval, offline, **no git actions** —
/// every mutation flows through the git manager, GIT-01); a contract without
/// write scopes gets [`PermissionSet::worker_read_only`]. Deriving rather
/// than defaulting to a broad set keeps least privilege (PRD §3) the
/// unconfigured behavior.
pub fn derive_permissions(contract: &TaskContract) -> PermissionSet {
    if contract.allowed_paths.is_empty() {
        PermissionSet::worker_read_only()
    } else {
        let globs: Vec<&str> = contract.allowed_paths.iter().map(String::as_str).collect();
        PermissionSet::worker_write(&globs)
    }
}

/// Compile `perms` into adapter constraints for a session rooted at
/// `workspace`, refusing the spawn when policy and contract disagree.
///
/// Fail-closed checks, in order:
///
/// 1. every `allowedPaths` glob the contract claims must be covered by a
///    write glob of the permission set — a contract may never widen policy;
/// 2. no write glob of the permission set may reach into a
///    `forbiddenPaths` glob — a role may never write where the contract
///    forbids writes;
/// 3. the compiled `allowed_paths` must be non-empty (a session with no
///    reachable root is a misconfiguration, not a sandbox).
///
/// Only then is [`compile_to_spawn_spec`] consulted for the actual lists.
pub fn compile_constraints(
    perms: &PermissionSet,
    workspace: &Path,
    contract: &TaskContract,
) -> Result<SpawnConstraints, RuntimeError> {
    for claimed in &contract.allowed_paths {
        if !perms
            .write_paths
            .iter()
            .any(|granted| glob_overlap(granted, claimed))
        {
            return Err(RuntimeError::PolicyRefusedSpawn {
                contract: contract.id.clone(),
                reason: format!(
                    "contract claims write scope `{claimed}`, which the permission set does not grant"
                ),
            });
        }
    }
    for forbidden in &contract.forbidden_paths {
        if let Some(granted) = perms
            .write_paths
            .iter()
            .find(|granted| glob_overlap(granted, forbidden))
        {
            return Err(RuntimeError::PolicyRefusedSpawn {
                contract: contract.id.clone(),
                reason: format!(
                    "permission set grants write scope `{granted}`, which reaches the \
                     contract-forbidden path `{forbidden}`"
                ),
            });
        }
    }
    let constraints = compile_to_spawn_spec(perms, &TaskScope::new(normalize_path(workspace)));
    if constraints.allowed_paths.is_empty() {
        return Err(RuntimeError::PolicyRefusedSpawn {
            contract: contract.id.clone(),
            reason: "compiled constraints grant no reachable path".to_owned(),
        });
    }
    Ok(constraints)
}

/// `/`-separated rendering of a path (Windows is the reference platform;
/// F-10's globs and `TaskScope` speak POSIX separators).
fn normalize_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_policy::{ApprovalDecision, GitAction};

    fn contract(allowed: &[&str], forbidden: &[&str]) -> TaskContract {
        TaskContract::builder("TASK-P", "policy wiring")
            .allowed_paths(allowed.iter().map(|p| (*p).to_owned()).collect())
            .forbidden_paths(forbidden.iter().map(|p| (*p).to_owned()).collect())
            .base_commit("abc1234")
            .build()
            .expect("contract")
    }

    fn gate() -> (tempfile::TempDir, PolicyGate) {
        let dir = tempfile::tempdir().expect("tempdir");
        let gate = PolicyGate::open(
            &dir.path().join("approvals.db"),
            &dir.path().join("audit.db"),
            &dir.path().join("approvals"),
        )
        .expect("policy gate");
        (dir, gate)
    }

    #[test]
    fn derived_permissions_follow_the_contract_scopes() {
        let write = derive_permissions(&contract(&["src/a/**"], &[]));
        assert_eq!(write.write_paths, vec!["src/a/**".to_owned()]);
        assert!(
            !write.allows_git(GitAction::Commit),
            "workers never get git actions: mutations go through the manager"
        );
        let read_only = derive_permissions(&contract(&[], &[]));
        assert!(read_only.write_paths.is_empty());
    }

    #[test]
    fn compilation_carries_policy_into_the_spawn_constraints() {
        let contract = contract(&["src/a/**"], &["infra/prod/**"]);
        let perms = derive_permissions(&contract);
        let constraints =
            compile_constraints(&perms, Path::new("C:\\work\\wt"), &contract).expect("compiles");
        assert!(constraints
            .allowed_paths
            .contains(&"C:/work/wt/src/a".to_owned()));
        assert!(constraints.allowed_paths.contains(&"C:/work/wt".to_owned()));
        // Offline worker: the network tool family is denied at the seam.
        for tool in ["WebFetch", "WebSearch"] {
            assert!(
                constraints.tool_denylist.contains(&tool.to_owned()),
                "`{tool}` must be denied for an offline worker: {constraints:?}"
            );
        }
        assert!(constraints.tool_allowlist.contains(&"Edit".to_owned()));
    }

    #[test]
    fn compilation_fails_closed_when_policy_and_contract_disagree() {
        // The contract claims a scope the permission set does not grant.
        let narrow = PermissionSet::worker_write(&["src/a/**"]);
        let err = compile_constraints(&narrow, Path::new("/wt"), &contract(&["src/b/**"], &[]))
            .expect_err("must refuse to spawn");
        assert!(err.to_string().contains("src/b/**"), "{err}");

        // The permission set can write where the contract forbids writes.
        let broad = PermissionSet::worker_write(&["**"]);
        let err = compile_constraints(
            &broad,
            Path::new("/wt"),
            &contract(&["**"], &["infra/prod/**"]),
        )
        .expect_err("must refuse to spawn");
        assert!(err.to_string().contains("infra/prod/**"), "{err}");

        // A quarantined set grants nothing but still compiles a rooted
        // sandbox — and any claimed scope is refused.
        let denied = PermissionSet::deny_all();
        assert!(
            compile_constraints(&denied, Path::new("/wt"), &contract(&["src/**"], &[])).is_err()
        );
        assert!(compile_constraints(&denied, Path::new("/wt"), &contract(&[], &[])).is_ok());
    }

    #[test]
    fn operations_are_deterministic_and_change_sensitive() {
        let run = Uuid::now_v7();
        let task = Uuid::now_v7();
        let a = git_mutation_operation(
            &run,
            &task,
            "commit",
            "wf",
            Path::new("/repo"),
            "commit",
            "abc1234",
        );
        let same = git_mutation_operation(
            &run,
            &task,
            "commit",
            "wf",
            Path::new("/repo"),
            "commit",
            "abc1234",
        );
        assert_eq!(operation_fingerprint(&a), operation_fingerprint(&same));
        let rebased = git_mutation_operation(
            &run,
            &task,
            "commit",
            "wf",
            Path::new("/repo"),
            "commit",
            "deadbee",
        );
        assert_ne!(
            operation_fingerprint(&a),
            operation_fingerprint(&rebased),
            "a moved base commit is a different operation"
        );
        let pushed = git_mutation_operation(
            &run,
            &task,
            "commit",
            "wf",
            Path::new("/repo"),
            "push",
            "abc1234",
        );
        assert_ne!(operation_fingerprint(&a), operation_fingerprint(&pushed));
    }

    #[test]
    fn verdicts_cover_pending_approved_denied_expired_and_mutation() {
        let (_dir, gate_store) = gate();
        let run = Uuid::now_v7();
        let task = Uuid::now_v7();
        let operation = git_mutation_operation(
            &run,
            &task,
            "commit",
            "wf",
            Path::new("/repo"),
            "commit",
            "abc1234",
        );

        // Nothing requested yet.
        assert_eq!(
            gate_store
                .evaluate(Gate::GitPush, &operation, &task)
                .expect("evaluate"),
            GateVerdict::Missing
        );

        // Requested but unresolved -> a bounded wait, never an approval.
        let request = gate_store
            .request(Gate::GitPush, &operation, &task, "agent", 600)
            .expect("request");
        let verdict = gate_store
            .evaluate(Gate::GitPush, &operation, &task)
            .expect("evaluate");
        assert_eq!(
            verdict,
            GateVerdict::Pending {
                request_id: request.id.clone()
            }
        );
        assert!(verdict.is_waiting() && !verdict.is_approved());

        // A live approval on a DIFFERENT operation never transfers.
        let mut mutated = operation.clone();
        mutated["baseCommit"] = json!("deadbee");
        let other = gate_store
            .request(Gate::GitPush, &mutated, &Uuid::now_v7(), "agent", 600)
            .expect("request");
        gate_store
            .approvals()
            .resolve(&other.id, ApprovalDecision::Approved)
            .expect("resolve");
        assert!(!gate_store
            .evaluate(Gate::GitPush, &operation, &task)
            .expect("evaluate")
            .is_approved());

        // Approving the tracked request authorizes exactly this operation.
        gate_store
            .approvals()
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve");
        assert!(gate_store
            .evaluate(Gate::GitPush, &operation, &task)
            .expect("evaluate")
            .is_approved());

        // A denied request is a deterministic refusal.
        let denied_task = Uuid::now_v7();
        let denied = gate_store
            .request(Gate::GitPush, &operation, &denied_task, "agent", 600)
            .expect("request");
        gate_store
            .approvals()
            .resolve(&denied.id, ApprovalDecision::Denied)
            .expect("resolve");
        // (the first request's approval is still live for this operation, so
        // check the denial through a fresh operation payload)
        let denied_op = git_mutation_operation(
            &run,
            &denied_task,
            "commit",
            "wf",
            Path::new("/repo"),
            "commit",
            "abc1234",
        );
        let redo = gate_store
            .request(Gate::GitPush, &denied_op, &denied_task, "agent", 600)
            .expect("request");
        gate_store
            .approvals()
            .resolve(&redo.id, ApprovalDecision::Denied)
            .expect("resolve");
        assert_eq!(
            gate_store
                .evaluate(Gate::GitPush, &denied_op, &denied_task)
                .expect("evaluate"),
            GateVerdict::Denied {
                request_id: redo.id
            }
        );

        // An approval whose ttl elapsed reads as expired, not approved.
        let expiring_task = Uuid::now_v7();
        let expiring_op = git_mutation_operation(
            &run,
            &expiring_task,
            "commit",
            "wf",
            Path::new("/repo"),
            "commit",
            "abc1234",
        );
        let expiring = gate_store
            .request(Gate::GitPush, &expiring_op, &expiring_task, "agent", 0)
            .expect("request");
        gate_store
            .approvals()
            .resolve(&expiring.id, ApprovalDecision::Approved)
            .expect("resolve");
        assert_eq!(
            gate_store
                .evaluate(Gate::GitPush, &expiring_op, &expiring_task)
                .expect("evaluate"),
            GateVerdict::Expired {
                request_id: expiring.id
            }
        );
    }

    #[test]
    fn a_tracked_request_for_another_operation_reads_as_mutated() {
        let (_dir, gate_store) = gate();
        let run = Uuid::now_v7();
        let task = Uuid::now_v7();
        let approved_op =
            human_approval_operation(&run, &task, "gate", "wf", &contract(&["src/**"], &[]));
        let request = gate_store
            .request(Gate::ProdAction, &approved_op, &task, "agent", 600)
            .expect("request");

        let mut amended = contract(&["src/**"], &[]);
        amended.objective = "a different question entirely".to_owned();
        let amended_op = human_approval_operation(&run, &task, "gate", "wf", &amended);

        assert_eq!(
            gate_store
                .evaluate(Gate::ProdAction, &amended_op, &task)
                .expect("evaluate"),
            GateVerdict::Mutated {
                request_id: request.id,
                approved_fingerprint: operation_fingerprint(&approved_op),
            }
        );
    }

    #[test]
    fn a_gate_never_transfers_an_approval_across_gates() {
        let (_dir, gate_store) = gate();
        let task = Uuid::now_v7();
        let operation =
            human_approval_operation(&Uuid::now_v7(), &task, "gate", "wf", &contract(&[], &[]));
        let request = gate_store
            .request(Gate::ProdAction, &operation, &task, "agent", 600)
            .expect("request");
        gate_store
            .approvals()
            .resolve(&request.id, ApprovalDecision::Approved)
            .expect("resolve");

        assert!(gate_store
            .evaluate(Gate::ProdAction, &operation, &task)
            .expect("evaluate")
            .is_approved());
        assert!(
            !gate_store
                .evaluate(Gate::GitPush, &operation, &task)
                .expect("evaluate")
                .is_approved(),
            "an approval is bound to its gate as well as its operation"
        );
    }
}
