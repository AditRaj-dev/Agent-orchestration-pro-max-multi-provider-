//! Real multi-agent calculator build driven through the backend stack:
//! workflow engine → supervisor → adapters → policy gate → git queue →
//! journal. `claude-code` implements, `antigravity-agy` reviews, and the
//! git gate commits the audit bundle — no codex adapter is registered.
//!
//! Two documented F-07 seams this runner stands in for (the real reviewer
//! pool and worker-content integration land later):
//!
//! - `NodeType::Review` is a deterministic stub that requires handoff test
//!   evidence, which real CLI adapters cannot produce yet (claude's
//!   structured-output capability is unexercised). The review here is a
//!   `Run` node executed by a real agent instead.
//! - The git gate commits only the audit bundle; worker content stays on
//!   the task worktree branch. This runner commits that branch when the
//!   implement task finishes and fast-forward merges it to `main` after
//!   the run completes.
//!
//! Journal: `D:/OP/calc-build/state/journal.db` — serve read-only with
//! `AGENTOS_DB=D:/OP/calc-build/state/journal.db agentos-daemon serve`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use agentos_adapters::adapter::RuntimeAdapter;
use agentos_adapters::agy::AgyAdapter;
use agentos_adapters::claude::ClaudeAdapter;
use agentos_core::TaskState;
use agentos_runtime::supervisor::DriveSummary;
use agentos_runtime::{
    ApprovalDecision, ContractBudgets, Supervisor, SupervisorConfig, TaskContract,
};
use agentos_workflow::{Budgets, NodeSpec, NodeType, RetryPolicy, RunStatus, WorkflowSpec};
use uuid::Uuid;

const ROOT: &str = "D:/OP/calc-build";

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(err) = rt.block_on(run()) {
        eprintln!("calc-build failed: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let repo = PathBuf::from(ROOT).join("repo");
    let state = PathBuf::from(ROOT).join("state");
    std::fs::create_dir_all(&state).map_err(|e| e.to_string())?;
    let head = seed_repo(&repo)?;

    println!("repo:   {}", repo.display());
    println!("state:  {}", state.display());
    println!("head:   {head}");

    // implement (claude-code) -> review (antigravity-agy) -> commit (gate)
    let spec = WorkflowSpec {
        id: "calculator-build".to_owned(),
        version: 1,
        nodes: vec![
            node("implement", NodeType::Run, &[], "builder"),
            node("review", NodeType::Run, &["implement"], "reviewer"),
            node("commit", NodeType::GitGate, &["review"], ""),
        ],
    };
    let contracts = contracts_for(&spec, &head);

    let config = SupervisorConfig::for_repo(&state, &repo)
        .with_role_adapter("builder", "claude-code")
        .with_role_adapter("reviewer", &reviewer_adapter());
    let adapters: Vec<Arc<dyn RuntimeAdapter>> =
        vec![Arc::new(ClaudeAdapter::new()), Arc::new(AgyAdapter::new())];
    let supervisor = Supervisor::new(config, adapters).map_err(|e| e.to_string())?;

    let run_id = supervisor
        .start_run(
            &spec,
            "ship the calculator: claude implements, claude reviews",
            contracts,
        )
        .map_err(|e| e.to_string())?;
    println!("run:    {run_id}");

    // Pre-create the review worktree BEFORE driving: the supervisor would
    // otherwise create it (empty) in the same tick that marks implement
    // done, racing the file staging below.
    stage_review_worktree(&supervisor, &run_id, &repo, &head)?;

    // The human half of the git gate (F-10): approve the exact mutation
    // before the gate node is ever leased.
    let request = supervisor
        .request_gate_approval(&run_id, "commit", "user@terminal")
        .map_err(|e| e.to_string())?;
    if !supervisor
        .approvals()
        .resolve(&request.id, ApprovalDecision::Approved)
        .map_err(|e| e.to_string())?
    {
        return Err("git-gate approval did not transition".to_owned());
    }
    println!("gate:   commit approved (request {})", request.id);

    println!("driving (agent sessions may take a few minutes)...");
    let summary = drive_with_integration(&supervisor, &run_id, &repo).await?;
    println!(
        "drive:  {} ticks, status = {:?}",
        summary.ticks, summary.status
    );

    report_tasks(&supervisor, &run_id)?;
    report_handoffs(&supervisor, &run_id)?;

    if summary.status != RunStatus::Completed {
        return Err(format!("run ended {:?}", summary.status));
    }

    integrate_to_main(&supervisor, &run_id, &repo)?;
    verify_calculator(&repo)?;
    println!("calc-build: OK");
    Ok(())
}

/// Drive one tick at a time so the implement worktree can be committed and
/// the review worktree staged BETWEEN implement finishing and the review
/// leasing (a single multi-tick `drive` would run the whole workflow
/// through before returning control). `DriveExhausted` with max_ticks == 1
/// means "one tick consumed" here; a stall (no task-state change across
/// several ticks) aborts instead of spinning.
async fn drive_with_integration(
    supervisor: &Supervisor,
    run_id: &Uuid,
    repo: &Path,
) -> Result<DriveSummary, String> {
    let mut integrated = false;
    let mut ticks = 0u32;
    let mut stall = 0u32;
    let mut last_signature = String::new();
    loop {
        // Productive single ticks end in `DriveExhausted` (the tick budget
        // hit before the status check); idle or terminal ticks return Ok.
        let status = match supervisor.drive(run_id, 1).await {
            Ok(summary) => Some(summary.status),
            Err(agentos_runtime::RuntimeError::DriveExhausted { .. }) => None,
            Err(other) => return Err(other.to_string()),
        };
        ticks += 1;

        if !integrated && implement_state(supervisor, run_id)? == Some(TaskState::Done) {
            commit_implement_worktree(supervisor, run_id, repo)?;
            integrated = true;
        }

        let signature = state_signature(supervisor, run_id)?;
        if signature == last_signature {
            stall += 1;
            if stall >= 5 {
                return Err(format!("stalled after {ticks} ticks (state: {signature})"));
            }
        } else {
            stall = 0;
            last_signature = signature;
        }

        if let Some(status) = status {
            if status != RunStatus::Running {
                return Ok(DriveSummary { ticks, status });
            }
        }
    }
}

fn state_signature(supervisor: &Supervisor, run_id: &Uuid) -> Result<String, String> {
    let tasks = supervisor
        .engine()
        .store()
        .tasks_for_run(run_id)
        .map_err(|e| e.to_string())?;
    Ok(tasks
        .iter()
        .map(|t| format!("{}:{:?}/{}", t.node_id, t.state, t.attempt_count))
        .collect::<Vec<_>>()
        .join(" "))
}

fn implement_state(supervisor: &Supervisor, run_id: &Uuid) -> Result<Option<TaskState>, String> {
    Ok(task_by_node(supervisor, run_id, "implement")?.map(|task| task.state))
}

fn task_by_node(
    supervisor: &Supervisor,
    run_id: &Uuid,
    node: &str,
) -> Result<Option<agentos_workflow::TaskRecord>, String> {
    Ok(supervisor
        .engine()
        .store()
        .tasks_for_run(run_id)
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|task| task.node_id == node))
}

/// The reviewer adapter: `claude-code` by default. The gemini side
/// (`antigravity-agy`) was the intended reviewer, but the account hit
/// "Individual quota reached ... resets in ~150h" mid-run, so the review
/// falls back to the other allowed CLI. Override with
/// `CALC_REVIEWER_ADAPTER=antigravity-agy` once quota resets.
fn reviewer_adapter() -> String {
    std::env::var("CALC_REVIEWER_ADAPTER").unwrap_or_else(|_| "claude-code".to_owned())
}

/// Pre-create the review worktree at the run's base so the supervisor
/// reuses it instead of creating an empty one mid-tick.
fn stage_review_worktree(
    supervisor: &Supervisor,
    run_id: &Uuid,
    repo: &Path,
    base: &str,
) -> Result<(), String> {
    let Some(review) = task_by_node(supervisor, run_id, "review")? else {
        return Err("review task missing".to_owned());
    };
    let review_wt = repo.join(".agentos-worktrees").join(review.id.to_string());
    if review_wt.exists() {
        return Ok(());
    }
    let run_id_str = run_id.to_string();
    let review_id_str = review.id.to_string();
    let branch = format!("agentos/{}/{}", tail8(&run_id_str), tail8(&review_id_str));
    git(
        repo,
        &[
            "worktree",
            "add",
            "-b",
            &branch,
            &review_wt.to_string_lossy(),
            base,
        ],
    )?;
    println!("wt:     pre-created review worktree on {branch}");
    Ok(())
}

/// Commit the implement task's worktree files on its own branch (the F-09
/// seam: sessions write into worktrees; landing content is serialized
/// later), then stage the implementation into the pre-created review
/// worktree so the reviewer receives the files in its workspace.
fn commit_implement_worktree(
    supervisor: &Supervisor,
    run_id: &Uuid,
    repo: &Path,
) -> Result<(), String> {
    let task = task_by_node(supervisor, run_id, "implement")?.ok_or("implement task missing")?;
    let worktree = repo.join(".agentos-worktrees").join(task.id.to_string());
    if !worktree.exists() {
        return Err(format!("worktree missing: {}", worktree.display()));
    }
    let branch = git(&worktree, &["branch", "--show-current"])?;
    let agent = supervisor
        .handoff_packets(run_id)
        .map_err(|e| e.to_string())?
        .iter()
        .find(|p| p.task_id == task.id.to_string())
        .map(|p| p.from_agent.clone())
        .unwrap_or_else(|| "claude-code".to_owned());
    git(&worktree, &["add", "-A"])?;
    git(
        &worktree,
        &[
            "-c",
            "user.name=agentos",
            "-c",
            "user.email=agentos@local",
            "commit",
            "-m",
            &format!("calc: implement calculator ({agent}, task {})", task.id),
        ],
    )?;
    println!("wt:     committed implement worktree on {branch}");

    if let Some(review) = task_by_node(supervisor, run_id, "review")? {
        let review_wt = repo.join(".agentos-worktrees").join(review.id.to_string());
        if review_wt.exists() {
            copy_file(&worktree.join("calc.py"), &review_wt.join("calc.py"))?;
            copy_dir(&worktree.join("tests"), &review_wt.join("tests"))?;
            println!("wt:     staged calc.py + tests/ into review worktree");
        }
    }
    Ok(())
}

/// Last 8 hex chars of the UUID with dashes stripped — the same short
/// agentos-git's `WorktreeManager::branch_name` derives.
fn tail8(id: &str) -> String {
    let hex: String = id.chars().filter(|c| *c != '-').collect();
    hex.chars().skip(hex.len().saturating_sub(8)).collect()
}

fn copy_file(src: &Path, dst: &Path) -> Result<(), String> {
    std::fs::copy(src, dst)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn copy_dir(src: &Path, dst: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dst).map_err(|e| e.to_string())?;
    for entry in std::fs::read_dir(src).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let to = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            copy_file(&entry.path(), &to)?;
        }
    }
    Ok(())
}

/// Fast-forward `main` to the implement branch (worker content lands last,
/// after the gate has serialized its audit-bundle commit).
fn integrate_to_main(supervisor: &Supervisor, run_id: &Uuid, repo: &Path) -> Result<(), String> {
    let task = task_by_node(supervisor, run_id, "implement")?.ok_or("implement task missing")?;
    let worktree = repo.join(".agentos-worktrees").join(task.id.to_string());
    let branch = git(&worktree, &["branch", "--show-current"])?;
    let before = git(repo, &["rev-parse", "HEAD"])?;
    git(repo, &["merge", "--ff-only", &branch])?;
    let after = git(repo, &["rev-parse", "HEAD"])?;
    println!("merge:  main {before} -> {after} (ff from {branch})");
    Ok(())
}

fn node(id: &str, node_type: NodeType, depends_on: &[&str], role: &str) -> NodeSpec {
    NodeSpec {
        id: id.to_owned(),
        node_type,
        depends_on: depends_on.iter().map(|d| (*d).to_owned()).collect(),
        agent_role: if role.is_empty() {
            None
        } else {
            Some(role.to_owned())
        },
        budgets: Budgets::default(),
        retry: RetryPolicy::default(),
    }
}

fn contracts_for(spec: &WorkflowSpec, head: &str) -> HashMap<String, TaskContract> {
    let objective = |id: &str| {
        match id {
        "implement" => concat!(
            "Implement a Python 3 (stdlib only) command-line calculator in this worktree. ",
            "Create `calc.py`: `python calc.py \"<expression>\"` evaluates the expression ",
            "and prints the result. Must support + - * / // % ** and parentheses, unary minus, ",
            "integers and floats (print 2.5 not 2.5000001), and operator precedence. ",
            "Division by zero or any invalid expression prints `error: <reason>` to stderr ",
            "and exits 1. Create `tests/test_calc.py` using unittest with at least 8 cases ",
            "covering precedence, parentheses, floats, division by zero, and invalid input. ",
            "Run `python -m unittest discover -s tests` and make it pass before finishing."
        )
        .to_owned(),
        "review" => concat!(
            "Independent review of a Python calculator another agent implemented. ",
            "Your workspace already contains the implementation under review: `calc.py` and `tests/`. ",
            "Review the tokenizer/parser statically for correctness: operator precedence and ",
            "associativity, unary minus, parentheses, division-by-zero and invalid-input handling, ",
            "float formatting; and read the tests for coverage gaps. ",
            "If shell access is available, also run `python -m unittest discover -s tests -v` and spot-check ",
            "the acceptance expressions with `python calc.py`; if commands are denied, say so and rely on ",
            "the static review. Do not modify the files. ",
            "Finish with a clear PASS or FAIL verdict and your findings."
        )
        .to_owned(),
        _ => "Commit the reviewed calculator through the git gate.".to_owned(),
    }
    };
    let allowed = |id: &str| match id {
        // The reviewer needs read+write on the calculator paths (restored
        // into its workspace) so policy grants Bash; read-only sets deny
        // the shell outright.
        "implement" | "review" => vec!["calc.py".to_owned(), "tests/**".to_owned()],
        _ => Vec::new(),
    };
    let criteria = |id: &str| match id {
        "implement" => vec![
            "`python calc.py \"1 + 2 * 3\"` prints 7".to_owned(),
            "`python calc.py \"(1 + 2) * -4\"` prints -12".to_owned(),
            "`python calc.py \"10 / 4\"` prints 2.5".to_owned(),
            "`python calc.py \"7 % 3\"` prints 1".to_owned(),
            "division by zero exits 1 with an error on stderr".to_owned(),
            "`python -m unittest discover -s tests` passes".to_owned(),
        ],
        "review" => vec![
            "the implementation was re-verified by running the tests and the listed expressions"
                .to_owned(),
            "a clear PASS or FAIL verdict with evidence is produced".to_owned(),
        ],
        _ => vec!["change lands through the gate".to_owned()],
    };

    spec.nodes
        .iter()
        .map(|node| {
            (
                node.id.clone(),
                TaskContract::builder(
                    format!("TASK-CALC-{}", node.id.to_uppercase()),
                    objective(&node.id),
                )
                .allowed_paths(allowed(&node.id))
                .acceptance_criteria(criteria(&node.id))
                .required_checks(vec!["python -m unittest discover -s tests".to_owned()])
                .budgets(ContractBudgets::new(25, 2))
                .base_commit(head.to_owned())
                .build()
                .expect("contract"),
            )
        })
        .collect()
}

/// Create the governed repo if missing; always returns the current HEAD.
fn seed_repo(repo: &Path) -> Result<String, String> {
    std::fs::create_dir_all(repo).map_err(|e| e.to_string())?;
    if !repo.join(".git").exists() {
        git(repo, &["init", "-b", "main"])?;
        std::fs::write(
            repo.join("README.md"),
            "# calc-build\n\nAgent-built calculator.\n",
        )
        .map_err(|e| e.to_string())?;
        git(repo, &["add", "-A"])?;
        git(
            repo,
            &[
                "-c",
                "user.name=agentos",
                "-c",
                "user.email=agentos@local",
                "commit",
                "-m",
                "seed: empty repo",
            ],
        )?;
    }
    git(repo, &["rev-parse", "HEAD"])
}

fn git(repo: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn report_tasks(supervisor: &Supervisor, run_id: &Uuid) -> Result<(), String> {
    let tasks = supervisor
        .engine()
        .store()
        .tasks_for_run(run_id)
        .map_err(|e| e.to_string())?;
    for task in &tasks {
        println!(
            "task:   {:<10} {:?} ({} attempt(s))",
            task.node_id, task.state, task.attempt_count
        );
    }
    Ok(())
}

fn report_handoffs(supervisor: &Supervisor, run_id: &Uuid) -> Result<(), String> {
    let packets = supervisor
        .handoff_packets(run_id)
        .map_err(|e| e.to_string())?;
    for packet in &packets {
        println!(
            "handoff: {} [{:?}] files={:?} tests={} action={:?}",
            packet.from_agent,
            packet.status,
            packet.files_changed,
            packet.tests.len(),
            packet.requested_action
        );
        if !packet.summary.is_empty() {
            println!(
                "         {}",
                packet.summary.chars().take(200).collect::<String>()
            );
        }
    }
    Ok(())
}

/// Ground truth: invoke what actually landed in the governed repo.
fn verify_calculator(repo: &Path) -> Result<(), String> {
    let calc = |expr: &str| -> Result<String, String> {
        let output = Command::new("python")
            .args(["calc.py", expr])
            .current_dir(repo)
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "`calc.py {expr}` failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };

    for (expr, want) in [
        ("1 + 2 * 3", "7"),
        ("(1 + 2) * -4", "-12"),
        ("10 / 4", "2.5"),
        ("7 % 3", "1"),
        ("2 ** 10", "1024"),
    ] {
        let got = calc(expr)?;
        if got != want {
            return Err(format!("`calc.py {expr}`: expected {want}, got {got}"));
        }
        println!("calc:   {expr} = {got}");
    }

    let tests = Command::new("python")
        .args(["-m", "unittest", "discover", "-s", "tests"])
        .current_dir(repo)
        .output()
        .map_err(|e| e.to_string())?;
    if !tests.status.success() {
        return Err(format!(
            "unittest failed: {}",
            String::from_utf8_lossy(&tests.stderr)
        ));
    }
    println!("calc:   unittest OK");
    println!("git:    {}", git(repo, &["log", "--oneline", "-3"])?);
    Ok(())
}
