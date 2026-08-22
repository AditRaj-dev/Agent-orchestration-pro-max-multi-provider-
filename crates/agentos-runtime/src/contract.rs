//! Full task contract (PRD §9 OR-04, canonical shape in §25 Appendix B).
//!
//! The workflow store (F-06) persists only a typed *subset* of the contract
//! (`objective`, path lists, criteria, checks) because the orchestrator and
//! policy engine that fill the rest did not exist yet. F-07 owns the full
//! shape: it adds `dependencies`, `contextRefs`, `baseCommit`,
//! `expectedArtifacts`, `budgets {maxMinutes, maxAttempts}` and `gitPolicy`,
//! and carries it alongside every run (see
//! [`crate::supervisor::Supervisor::start_run`]) so executors, reviewers and
//! the git gate all read one typed agreement.
//!
//! ## Immutable-after-lease semantics
//!
//! A contract is *immutable for the duration of a lease*: when the supervisor
//! leases a task it snapshots the contract version recorded in the run
//! manifest and everything downstream (the `SpawnSpec`, the review, the git
//! gate) reads only that snapshot. Changes are expressed with
//! [`TaskContract::amend`], which produces a **new** value with a bumped
//! `version` — the original is never mutated in place — so any audit
//! question of the form "what did the agent agree to when it started?" has a
//! deterministic answer. The supervisor exposes no path that amends a
//! running run's contracts; wiring orchestrator-driven amendments into
//! re-lease is the F-10+ orchestration seam.

use agentos_workflow::NodeType;
use serde::{Deserialize, Serialize};

/// Ceiling pair of the Appendix B `budgets` object.
///
/// Note this is the *contract's* human-scale budget (minutes/attempts). The
/// engine's machine-enforced ceilings (`max_elapsed_secs`, `max_cost_usd`)
/// live on the node spec ([`agentos_workflow::Budgets`]); the supervisor
/// takes the stricter of the two when building a `SpawnSpec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractBudgets {
    /// Wall-clock ceiling in minutes.
    pub max_minutes: u32,
    /// Maximum lease attempts.
    pub max_attempts: u32,
}

impl ContractBudgets {
    /// A budget pair (both values are validated to be `>= 1`).
    pub const fn new(max_minutes: u32, max_attempts: u32) -> Self {
        Self {
            max_minutes,
            max_attempts,
        }
    }
}

impl Default for ContractBudgets {
    fn default() -> Self {
        Self::new(15, 3)
    }
}

/// Whether the worker may drive git itself or must go through the
/// serialized mutation queue (Appendix B `gitPolicy: "no-direct-git"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GitPolicy {
    /// Workers never mutate git directly; every mutation goes through the
    /// git gate and the mutation queue (the F-07 default; the supervisor
    /// reflects it in the spawn spec's tool denylist).
    #[serde(rename = "no-direct-git")]
    NoDirectGit,
    /// The worker may run git in its workspace (still queue-gated at the
    /// integration branch).
    #[serde(rename = "direct-allowed")]
    DirectAllowed,
}

impl GitPolicy {
    /// The canonical wire string.
    pub fn as_str(self) -> &'static str {
        match self {
            GitPolicy::NoDirectGit => "no-direct-git",
            GitPolicy::DirectAllowed => "direct-allowed",
        }
    }
}

/// Which rule of [`TaskContract::validate`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ContractRule {
    /// `id` was empty or whitespace.
    #[error("contract id must be non-empty")]
    EmptyId,
    /// `objective` was empty or whitespace.
    #[error("objective must be non-empty")]
    EmptyObjective,
    /// `base_commit` was empty or whitespace.
    #[error("base_commit must be non-empty")]
    EmptyBaseCommit,
    /// A write-capable node declared no `allowed_paths`.
    #[error("node `{node}` executes writes and must declare at least one allowed path")]
    WriteNeedsAllowedPaths {
        /// The node that failed the check.
        node: String,
    },
    /// Either budget leg is below the minimum of 1.
    #[error("budgets must be >= 1 (got max_minutes={max_minutes}, max_attempts={max_attempts})")]
    BudgetTooSmall {
        /// The declared minute ceiling.
        max_minutes: u32,
        /// The declared attempt ceiling.
        max_attempts: u32,
    },
    /// A path list carried an empty/whitespace entry.
    #[error("path lists must not contain empty entries")]
    EmptyPathEntry,
}

/// The full task contract (PRD §25 Appendix B).
///
/// Wire shape is the Appendix B example verbatim, camelCase, with the two
/// fields the example omits (`dependencies`, `expectedArtifacts`) and the
/// internal `version` stamp defaulting in:
///
/// ```json
/// {
///   "id": "TASK-AUTH-042",
///   "objective": "Implement refresh-token rotation for authenticated sessions",
///   "allowedPaths": ["src/auth/**", "tests/auth/**"],
///   "forbiddenPaths": ["infra/prod/**", "billing/**"],
///   "baseCommit": "8f7291c",
///   "contextRefs": ["context.auth@18", "context.db@7", "decision.adr-014"],
///   "acceptanceCriteria": ["old token is revoked atomically", "..."],
///   "requiredChecks": ["typecheck", "test:auth"],
///   "budgets": {"maxMinutes": 25, "maxAttempts": 2},
///   "gitPolicy": "no-direct-git"
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskContract {
    /// Revision of this contract value: `0` means "unversioned/legacy"
    /// (parsed without a version), builders stamp `1`, and every
    /// [`TaskContract::amend`] increments it.
    #[serde(default)]
    pub version: u32,
    /// Stable contract identity (e.g. `TASK-AUTH-042`).
    pub id: String,
    /// What the worker must accomplish.
    pub objective: String,
    /// Repository path globs the worker may touch (ownership holds are
    /// taken on these, exclusive).
    pub allowed_paths: Vec<String>,
    /// Repository path globs explicitly off-limits.
    #[serde(default)]
    pub forbidden_paths: Vec<String>,
    /// Other task ids whose output this task builds on.
    #[serde(default)]
    pub dependencies: Vec<String>,
    /// Immutable context references (`context.auth@18`, `decision.adr-014`).
    #[serde(default)]
    pub context_refs: Vec<String>,
    /// Conditions under which the objective is met.
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
    /// Checks that must pass before output is accepted.
    #[serde(default)]
    pub required_checks: Vec<String>,
    /// Human-scale budget ceilings (see [`ContractBudgets`]).
    pub budgets: ContractBudgets,
    /// Integration-branch commit the task is planned against.
    pub base_commit: String,
    /// Artifacts (paths/refs) the task is expected to produce.
    #[serde(default)]
    pub expected_artifacts: Vec<String>,
    /// Whether the worker may drive git itself.
    pub git_policy: GitPolicy,
}

impl TaskContract {
    /// Start building a contract; see [`TaskContractBuilder`].
    pub fn builder(id: impl Into<String>, objective: impl Into<String>) -> TaskContractBuilder {
        TaskContractBuilder {
            contract: TaskContract {
                version: 1,
                id: id.into(),
                objective: objective.into(),
                allowed_paths: Vec::new(),
                forbidden_paths: Vec::new(),
                dependencies: Vec::new(),
                context_refs: Vec::new(),
                acceptance_criteria: Vec::new(),
                required_checks: Vec::new(),
                budgets: ContractBudgets::default(),
                base_commit: String::new(),
                expected_artifacts: Vec::new(),
                git_policy: GitPolicy::NoDirectGit,
            },
        }
    }

    /// Validate the node-independent rules: non-empty id/objective/
    /// base_commit, budgets `>= 1`, and no empty entries in path lists.
    pub fn validate(&self) -> Result<(), ContractRule> {
        if self.id.trim().is_empty() {
            return Err(ContractRule::EmptyId);
        }
        if self.objective.trim().is_empty() {
            return Err(ContractRule::EmptyObjective);
        }
        if self.base_commit.trim().is_empty() {
            return Err(ContractRule::EmptyBaseCommit);
        }
        if self.budgets.max_minutes < 1 || self.budgets.max_attempts < 1 {
            return Err(ContractRule::BudgetTooSmall {
                max_minutes: self.budgets.max_minutes,
                max_attempts: self.budgets.max_attempts,
            });
        }
        if self
            .allowed_paths
            .iter()
            .chain(&self.forbidden_paths)
            .any(|p| p.trim().is_empty())
        {
            return Err(ContractRule::EmptyPathEntry);
        }
        Ok(())
    }

    /// Validate in the context of the workflow node that will execute the
    /// contract: everything [`TaskContract::validate`] checks, plus the
    /// write-task rule — nodes that execute agent writes (`Run`, `Branch`,
    /// `Loop`) must declare at least one allowed path. Gate and review
    /// nodes (`Review`, `GitGate`, `HumanApproval`, `Parallel`) and pure
    /// synchronization points may leave the list empty.
    pub fn validate_for(&self, node_id: &str, node_type: NodeType) -> Result<(), ContractRule> {
        self.validate()?;
        let writes = matches!(
            node_type,
            NodeType::Run | NodeType::Branch | NodeType::Loop { .. }
        );
        if writes && self.allowed_paths.is_empty() {
            return Err(ContractRule::WriteNeedsAllowedPaths {
                node: node_id.to_owned(),
            });
        }
        Ok(())
    }

    /// Produce the next version of this contract: apply `change` to a
    /// clone, bump `version`, re-validate, and return the new value. The
    /// receiver is untouched — immutable-after-lease semantics mean the
    /// leased version keeps answering "what was agreed when work started".
    pub fn amend(
        &self,
        change: impl FnOnce(&mut TaskContract),
    ) -> Result<TaskContract, ContractRule> {
        let mut next = self.clone();
        change(&mut next);
        next.version = self.version + 1;
        next.validate()?;
        Ok(next)
    }
}

/// Builder for [`TaskContract`] (validating on `build`, stamping
/// `version = 1`).
#[derive(Debug, Clone)]
pub struct TaskContractBuilder {
    contract: TaskContract,
}

impl TaskContractBuilder {
    /// Set the allowed path globs (ownership holds are acquired on these).
    pub fn allowed_paths(mut self, paths: Vec<String>) -> Self {
        self.contract.allowed_paths = paths;
        self
    }

    /// Set the forbidden path globs.
    pub fn forbidden_paths(mut self, paths: Vec<String>) -> Self {
        self.contract.forbidden_paths = paths;
        self
    }

    /// Set the task dependencies.
    pub fn dependencies(mut self, deps: Vec<String>) -> Self {
        self.contract.dependencies = deps;
        self
    }

    /// Set the immutable context references.
    pub fn context_refs(mut self, refs: Vec<String>) -> Self {
        self.contract.context_refs = refs;
        self
    }

    /// Set the acceptance criteria.
    pub fn acceptance_criteria(mut self, criteria: Vec<String>) -> Self {
        self.contract.acceptance_criteria = criteria;
        self
    }

    /// Set the required checks.
    pub fn required_checks(mut self, checks: Vec<String>) -> Self {
        self.contract.required_checks = checks;
        self
    }

    /// Set the budgets (must be `>= 1` on both legs).
    pub fn budgets(mut self, budgets: ContractBudgets) -> Self {
        self.contract.budgets = budgets;
        self
    }

    /// Set the base commit.
    pub fn base_commit(mut self, commit: impl Into<String>) -> Self {
        self.contract.base_commit = commit.into();
        self
    }

    /// Set the expected artifacts.
    pub fn expected_artifacts(mut self, artifacts: Vec<String>) -> Self {
        self.contract.expected_artifacts = artifacts;
        self
    }

    /// Set the git policy.
    pub fn git_policy(mut self, policy: GitPolicy) -> Self {
        self.contract.git_policy = policy;
        self
    }

    /// Finish: validate and return the version-1 contract.
    pub fn build(self) -> Result<TaskContract, ContractRule> {
        self.contract.validate()?;
        Ok(self.contract)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The Appendix B example, verbatim (fields the example omits default).
    fn appendix_b() -> serde_json::Value {
        json!({
            "id": "TASK-AUTH-042",
            "objective": "Implement refresh-token rotation for authenticated sessions",
            "allowedPaths": ["src/auth/**", "tests/auth/**"],
            "forbiddenPaths": ["infra/prod/**", "billing/**"],
            "baseCommit": "8f7291c",
            "contextRefs": ["context.auth@18", "context.db@7", "decision.adr-014"],
            "acceptanceCriteria": [
                "old token is revoked atomically",
                "replay is rejected",
                "existing login flow remains compatible"
            ],
            "requiredChecks": ["typecheck", "test:auth"],
            "budgets": {"maxMinutes": 25, "maxAttempts": 2},
            "gitPolicy": "no-direct-git"
        })
    }

    #[test]
    fn appendix_b_example_parses_and_round_trips() {
        let contract: TaskContract = serde_json::from_value(appendix_b()).expect("parse");
        assert_eq!(contract.id, "TASK-AUTH-042");
        assert_eq!(contract.allowed_paths, vec!["src/auth/**", "tests/auth/**"]);
        assert_eq!(contract.base_commit, "8f7291c");
        assert_eq!(contract.budgets, ContractBudgets::new(25, 2));
        assert_eq!(contract.git_policy, GitPolicy::NoDirectGit);
        assert_eq!(contract.git_policy.as_str(), "no-direct-git");
        // Fields the example omits default in.
        assert!(contract.dependencies.is_empty());
        assert!(contract.expected_artifacts.is_empty());
        assert_eq!(contract.version, 0, "parsed without a version = legacy 0");

        let wire = serde_json::to_value(&contract).unwrap();
        let round_tripped: TaskContract = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(round_tripped, contract);
        assert_eq!(wire["contextRefs"][0], json!("context.auth@18"));
        assert_eq!(wire["budgets"]["maxMinutes"], json!(25));

        contract.validate().expect("Appendix B is valid");
    }

    #[test]
    fn missing_required_field_is_rejected() {
        let mut wire = appendix_b();
        wire.as_object_mut().unwrap().remove("objective");
        assert!(serde_json::from_value::<TaskContract>(wire).is_err());
    }

    #[test]
    fn empty_objective_and_empty_budgets_are_rejected() {
        let contract = TaskContract::builder("T-1", "do the thing")
            .base_commit("abc1234")
            .build()
            .unwrap();
        contract.validate().unwrap();

        let empty_objective = contract
            .clone()
            .amend(|c| c.objective = "   ".to_owned())
            .unwrap_err();
        assert_eq!(empty_objective, ContractRule::EmptyObjective);

        let bad_budget = contract
            .amend(|c| c.budgets = ContractBudgets::new(0, 2))
            .unwrap_err();
        assert_eq!(
            bad_budget,
            ContractRule::BudgetTooSmall {
                max_minutes: 0,
                max_attempts: 2
            }
        );
    }

    #[test]
    fn empty_base_commit_and_empty_path_entries_are_rejected() {
        // A missing base commit is caught at build time already.
        let err = TaskContract::builder("T-1", "do the thing")
            .build()
            .unwrap_err();
        assert_eq!(err, ContractRule::EmptyBaseCommit);

        // Empty path entries are caught at build time already.
        let err = TaskContract::builder("T-1", "obj")
            .base_commit("abc")
            .allowed_paths(vec!["src/**".to_owned(), "  ".to_owned()])
            .build()
            .unwrap_err();
        assert_eq!(err, ContractRule::EmptyPathEntry);
    }

    #[test]
    fn write_tasks_need_allowed_paths_gate_tasks_do_not() {
        let read_only = TaskContract::builder("T-1", "review it")
            .base_commit("abc")
            .build()
            .unwrap();
        read_only
            .validate_for("review", NodeType::Review)
            .expect("review nodes need no allowed paths");
        read_only
            .validate_for("commit", NodeType::GitGate)
            .expect("git gates need no allowed paths");
        read_only
            .validate_for("join", NodeType::Parallel)
            .expect("parallel fan-in points need no allowed paths");

        let err = read_only.validate_for("build", NodeType::Run).unwrap_err();
        assert_eq!(
            err,
            ContractRule::WriteNeedsAllowedPaths {
                node: "build".to_owned()
            }
        );

        let writing = TaskContract::builder("T-2", "build it")
            .base_commit("abc")
            .allowed_paths(vec!["src/**".to_owned()])
            .build()
            .unwrap();
        writing.validate_for("build", NodeType::Run).unwrap();
    }

    #[test]
    fn amend_versions_changes_and_never_mutates_the_original() {
        let v1 = TaskContract::builder("T-1", "original objective")
            .base_commit("abc1234")
            .allowed_paths(vec!["src/**".to_owned()])
            .build()
            .unwrap();
        assert_eq!(v1.version, 1);

        let v2 = v1
            .amend(|c| {
                c.objective = "revised objective".to_owned();
                c.allowed_paths.push("tests/**".to_owned());
            })
            .unwrap();
        assert_eq!(v2.version, 2);
        assert_eq!(v2.objective, "revised objective");
        assert_eq!(v2.allowed_paths.len(), 2);

        // Immutable-after-lease: the leased version is untouched.
        assert_eq!(v1.version, 1);
        assert_eq!(v1.objective, "original objective");
        assert_eq!(v1.allowed_paths.len(), 1);
        assert_ne!(v1, v2);

        // Invalid amendments are rejected without producing a version.
        let rejected = v1.amend(|c| c.objective = String::new());
        assert_eq!(rejected.unwrap_err(), ContractRule::EmptyObjective);
    }

    #[test]
    fn builder_requires_validation_before_use() {
        let unbuilt = TaskContract::builder("", "objective").base_commit("abc");
        assert_eq!(unbuilt.build().unwrap_err(), ContractRule::EmptyId);
    }
}
