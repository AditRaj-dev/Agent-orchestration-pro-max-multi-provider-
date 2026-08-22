//! Per-run budget/usage ledger (F-07; feeds the OR-08 budget gate).
//!
//! The ledger consumes [`agentos_adapters::AdapterEvent::UsageUpdate`]
//! snapshots as sessions stream them, keyed by task, and rolls up:
//!
//! - token counters (input/output/thinking/cache-read/total);
//! - monetary cost — the claude canon reports `total_cost_usd` only, so the
//!   top-line cost is the sum of snapshot costs;
//! - **per-model rows** (the claude `modelUsage` canon: a trivial run
//!   touches main + auxiliary models) accumulated by model id;
//! - the **fixed per-session preamble overhead** (F-00 §4: ~22k tokens on
//!   claude, ~37k on agy — the core system prompt, not user skills), summed
//!   as its own line so budgets can price it honestly.
//!
//! [`UsageLedger::status_of`] enforces `Budgets { max_cost_usd,
//! max_elapsed_secs }` and returns Ok / Warn (>= 80% of a ceiling) /
//! Exceeded (>= a ceiling). The supervisor escalates on Exceeded — it emits
//! a `budget.exceeded` journal event and fails the task — instead of
//! silently continuing; the ledger never mutates budgets on its own.
//!
//! The ledger also implements [`agentos_workflow::CostLedger`], so it is a
//! drop-in cost source for any scheduler once the engine exposes
//! cost-ledger injection (the F-07 supervisor enforces cost at its own
//! layer in the meantime; see the seam note in
//! `docs/F-07-runtime-supervisor.md`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use agentos_adapters::UsageSnapshot;
use agentos_workflow::{Budgets, CostLedger, TaskRecord};
use uuid::Uuid;

/// Fraction of a ceiling at which the ledger reports [`BudgetStatus::Warn`].
pub const WARN_FRACTION: f64 = 0.8;

/// Budget verdict for one task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetStatus {
    /// Comfortably within every ceiling.
    Ok,
    /// At or beyond 80% of at least one ceiling (still runnable).
    Warn,
    /// At or beyond a ceiling: the supervisor must escalate (emit
    /// `budget.exceeded` and fail the task), never silently continue.
    Exceeded,
}

impl BudgetStatus {
    /// Whether this verdict is [`BudgetStatus::Exceeded`].
    pub fn is_exceeded(self) -> bool {
        self == BudgetStatus::Exceeded
    }
}

/// Accumulated cost of one model (claude `modelUsage` row, aggregated).
#[derive(Debug, Clone, Default)]
pub struct ModelUsage {
    /// Cost attributable to this model across all updates.
    pub cost_usd: f64,
    /// Largest context window the provider reported for the model.
    pub context_window: Option<u64>,
    /// Number of usage updates that mentioned the model.
    pub updates: u32,
}

/// Accumulated usage of one task (a run's total is the sum over its tasks,
/// see [`UsageLedger::run_total`]).
#[derive(Debug, Clone, Default)]
pub struct TaskUsage {
    /// Prompt tokens billed.
    pub input_tokens: u64,
    /// Completion tokens billed.
    pub output_tokens: u64,
    /// Reasoning/"thinking" tokens, where the provider splits them out.
    pub thinking_tokens: u64,
    /// Cache-hit read tokens (where the per-session preamble bills).
    pub cache_read_tokens: u64,
    /// Total tokens as reported by providers.
    pub total_tokens: u64,
    /// Monetary cost in USD (claude `total_cost_usd` canon; providers
    /// without a cost field contribute zero).
    pub cost_usd: f64,
    /// Fixed per-session preamble overhead tokens, as its own ledger line.
    pub session_overhead_tokens: u64,
    /// Number of usage updates consumed.
    pub usage_updates: u32,
    /// Per-model cost rows, keyed by model id (claude `modelUsage` canon).
    pub per_model: BTreeMap<String, ModelUsage>,
}

/// Thread-safe usage ledger, keyed by task id.
#[derive(Debug, Default)]
pub struct UsageLedger {
    rows: Mutex<HashMap<Uuid, TaskUsage>>,
}

impl UsageLedger {
    /// An empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Consume one usage snapshot into the task's row. Snapshots are
    /// cumulative-per-provider but additive here, matching how the engine
    /// consumes attempts: each attempt's snapshots land in the same row so
    /// budgets see everything a task has spent, including retries.
    pub fn consume(&self, task_id: Uuid, snapshot: &UsageSnapshot) {
        let mut rows = Self::recover(&self.rows);
        let row = rows.entry(task_id).or_default();
        row.input_tokens += snapshot.input_tokens;
        row.output_tokens += snapshot.output_tokens;
        row.thinking_tokens += snapshot.thinking_tokens;
        row.cache_read_tokens += snapshot.cache_read_tokens;
        row.total_tokens += snapshot.total_tokens;
        row.cost_usd += snapshot.cost_usd.unwrap_or(0.0);
        row.session_overhead_tokens += snapshot.session_overhead_tokens;
        row.usage_updates += 1;
        for model in &snapshot.per_model {
            let entry = row.per_model.entry(model.model.clone()).or_default();
            entry.cost_usd += model.cost_usd.unwrap_or(0.0);
            entry.context_window = entry.context_window.max(model.context_window);
            entry.updates += 1;
        }
    }

    /// The task's accumulated usage, if any update has been consumed.
    pub fn usage(&self, task_id: Uuid) -> Option<TaskUsage> {
        Self::recover(&self.rows).get(&task_id).cloned()
    }

    /// Roll up the usage of every listed task (a run's total).
    pub fn run_total(&self, task_ids: &[Uuid]) -> TaskUsage {
        let rows = Self::recover(&self.rows);
        let mut total = TaskUsage::default();
        for task_id in task_ids {
            let Some(row) = rows.get(task_id) else {
                continue;
            };
            total.input_tokens += row.input_tokens;
            total.output_tokens += row.output_tokens;
            total.thinking_tokens += row.thinking_tokens;
            total.cache_read_tokens += row.cache_read_tokens;
            total.total_tokens += row.total_tokens;
            total.cost_usd += row.cost_usd;
            total.session_overhead_tokens += row.session_overhead_tokens;
            total.usage_updates += row.usage_updates;
            for (model, usage) in &row.per_model {
                let entry = total.per_model.entry(model.clone()).or_default();
                entry.cost_usd += usage.cost_usd;
                entry.context_window = entry.context_window.max(usage.context_window);
                entry.updates += usage.updates;
            }
        }
        total
    }

    /// Budget verdict for a task under `budgets`, given the wall-clock
    /// seconds elapsed since the task was created.
    pub fn check(&self, task_id: &Uuid, budgets: &Budgets, elapsed_secs: u64) -> BudgetStatus {
        Self::status_of(self.usage(*task_id).as_ref(), budgets, elapsed_secs)
    }

    /// Pure budget verdict (unit-testable without a ledger):
    ///
    /// - elapsed `>= max_elapsed_secs` or spend `>= max_cost_usd` (when
    ///   both are known) is [`BudgetStatus::Exceeded`] — mirroring the
    ///   scheduler's gate, which uses `>=` on both legs;
    /// - `>= 80%` of either ceiling is [`BudgetStatus::Warn`];
    /// - otherwise [`BudgetStatus::Ok`]. An unknown cost never trips the
    ///   cost leg (it cannot — that is the OR-05 "no cost data" rule).
    pub fn status_of(
        usage: Option<&TaskUsage>,
        budgets: &Budgets,
        elapsed_secs: u64,
    ) -> BudgetStatus {
        if budgets.max_elapsed_secs == 0 || elapsed_secs >= budgets.max_elapsed_secs {
            return BudgetStatus::Exceeded;
        }
        let mut status = BudgetStatus::Ok;
        // Integer arithmetic for the elapsed warn band (x10/x8 keeps one
        // decimal of precision without floats).
        if elapsed_secs * 10 >= budgets.max_elapsed_secs * 8 {
            status = BudgetStatus::Warn;
        }
        if let (Some(max_cost), Some(row)) = (budgets.max_cost_usd, usage) {
            if row.cost_usd >= max_cost {
                return BudgetStatus::Exceeded;
            }
            if row.cost_usd >= max_cost * WARN_FRACTION {
                status = BudgetStatus::Warn;
            }
        }
        status
    }

    /// Recover a lock guard even from a poisoned mutex — the ledger's data
    /// is still consistent (every operation is a self-contained update).
    fn recover(
        mutex: &Mutex<HashMap<Uuid, TaskUsage>>,
    ) -> std::sync::MutexGuard<'_, HashMap<Uuid, TaskUsage>> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl CostLedger for UsageLedger {
    fn spent_usd(&self, task: &TaskRecord) -> Option<f64> {
        self.usage(task.id).map(|row| row.cost_usd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_adapters::ModelUsageRow;

    fn snapshot(cost: Option<f64>, models: &[(&str, f64)]) -> UsageSnapshot {
        UsageSnapshot {
            input_tokens: 1_000,
            output_tokens: 400,
            thinking_tokens: 50,
            cache_read_tokens: 22_000,
            total_tokens: 23_450,
            cost_usd: cost,
            per_model: models
                .iter()
                .map(|&(model, model_cost)| ModelUsageRow {
                    model: model.to_owned(),
                    cost_usd: Some(model_cost),
                    context_window: Some(1_000_000),
                })
                .collect(),
            session_overhead_tokens: 22_000,
        }
    }

    fn budgets(max_cost: Option<f64>, max_elapsed: u64) -> Budgets {
        Budgets {
            max_attempts: 3,
            max_elapsed_secs: max_elapsed,
            max_cost_usd: max_cost,
        }
    }

    #[test]
    fn accumulates_tokens_cost_and_overhead_across_updates() {
        let ledger = UsageLedger::new();
        let task = Uuid::now_v7();

        ledger.consume(task, &snapshot(Some(0.02), &[("claude-opus-5", 0.02)]));
        ledger.consume(task, &snapshot(Some(0.01), &[("claude-haiku", 0.01)]));
        ledger.consume(task, &snapshot(Some(0.01), &[("claude-opus-5", 0.01)]));

        let row = ledger.usage(task).expect("row exists");
        assert_eq!(row.usage_updates, 3);
        assert_eq!(row.input_tokens, 3_000);
        assert_eq!(row.output_tokens, 1_200);
        assert_eq!(row.total_tokens, 70_350);
        // The fixed per-session preamble is tracked as its own line: one
        // overhead charge per session/update, per the claude T5 canon.
        assert_eq!(row.session_overhead_tokens, 66_000);
        assert!((row.cost_usd - 0.04).abs() < 1e-9);

        // Per-model rows merged by model id.
        assert_eq!(row.per_model.len(), 2);
        let opus = &row.per_model["claude-opus-5"];
        assert!((opus.cost_usd - 0.03).abs() < 1e-9);
        assert_eq!(opus.updates, 2);
        assert_eq!(opus.context_window, Some(1_000_000));
        assert_eq!(row.per_model["claude-haiku"].updates, 1);
    }

    #[test]
    fn providers_without_cost_contribute_zero() {
        let ledger = UsageLedger::new();
        let task = Uuid::now_v7();
        ledger.consume(task, &snapshot(None, &[]));
        let row = ledger.usage(task).unwrap();
        assert_eq!(row.cost_usd, 0.0);
        assert_eq!(row.usage_updates, 1);
    }

    #[test]
    fn run_total_sums_across_tasks() {
        let ledger = UsageLedger::new();
        let a = Uuid::now_v7();
        let b = Uuid::now_v7();
        let c = Uuid::now_v7(); // never touched
        ledger.consume(a, &snapshot(Some(0.02), &[("m1", 0.02)]));
        ledger.consume(b, &snapshot(Some(0.03), &[("m1", 0.01), ("m2", 0.02)]));

        let total = ledger.run_total(&[a, b, c]);
        assert_eq!(total.usage_updates, 2);
        assert!((total.cost_usd - 0.05).abs() < 1e-9);
        assert!((total.per_model["m1"].cost_usd - 0.03).abs() < 1e-9);
        assert!((total.per_model["m2"].cost_usd - 0.02).abs() < 1e-9);
    }

    #[test]
    fn cost_budget_ok_warn_exceeded_ladder() {
        let unknown = None;
        let mut usage = TaskUsage {
            cost_usd: 0.02,
            ..TaskUsage::default()
        };
        let generous = budgets(Some(1.0), 3_600);
        assert_eq!(
            UsageLedger::status_of(Some(&usage), &generous, 0),
            BudgetStatus::Ok
        );
        assert_eq!(
            UsageLedger::status_of(None, &generous, 0),
            BudgetStatus::Ok,
            "no cost data never trips the cost leg"
        );

        // Warn at >= 80% of the cost ceiling.
        usage.cost_usd = 0.85;
        assert_eq!(
            UsageLedger::status_of(Some(&usage), &generous, 0),
            BudgetStatus::Warn
        );

        // Exceeded at >= the ceiling.
        usage.cost_usd = 1.0;
        let verdict = UsageLedger::status_of(Some(&usage), &generous, 0);
        assert_eq!(verdict, BudgetStatus::Exceeded);
        assert!(verdict.is_exceeded());

        // Cost leg inert without a ceiling.
        assert_eq!(
            UsageLedger::status_of(Some(&usage), &budgets(unknown, 3_600), 0),
            BudgetStatus::Ok
        );
    }

    #[test]
    fn elapsed_budget_exceeded_and_warn_bands() {
        let usage = None; // elapsed is cost-independent
        let b = budgets(Some(1.0), 100);
        assert_eq!(UsageLedger::status_of(usage, &b, 0), BudgetStatus::Ok);
        assert_eq!(UsageLedger::status_of(usage, &b, 79), BudgetStatus::Ok);
        assert_eq!(UsageLedger::status_of(usage, &b, 80), BudgetStatus::Warn);
        assert_eq!(UsageLedger::status_of(usage, &b, 99), BudgetStatus::Warn);
        assert_eq!(
            UsageLedger::status_of(usage, &b, 100),
            BudgetStatus::Exceeded,
            ">= max_elapsed_secs is exceeded, mirroring the scheduler gate"
        );
        // max_elapsed_secs = 0 is exhausted immediately (F-06 semantics).
        assert_eq!(
            UsageLedger::status_of(usage, &budgets(None, 0), 0),
            BudgetStatus::Exceeded
        );
    }

    #[test]
    fn check_uses_the_ledger_row_and_cost_ledger_hook_answers_spent() {
        let ledger = UsageLedger::new();
        let task_id = Uuid::now_v7();
        let record = TaskRecord {
            id: task_id,
            run_id: Uuid::now_v7(),
            workflow_id: "wf".to_owned(),
            node_id: "build".to_owned(),
            state: agentos_core::TaskState::Running,
            priority: agentos_core::Priority::P2,
            lease_owner: None,
            lease_expires_at: None,
            heartbeat_at: None,
            attempt_count: 0,
            contract: agentos_workflow::TaskContract {
                objective: "o".to_owned(),
                allowed_paths: vec![],
                forbidden_paths: vec![],
                acceptance_criteria: vec![],
                required_checks: vec![],
            },
            node: agentos_workflow::NodeSpec {
                id: "build".to_owned(),
                node_type: agentos_workflow::NodeType::Run,
                depends_on: vec![],
                agent_role: None,
                budgets: budgets(Some(0.01), 3_600),
                retry: agentos_workflow::RetryPolicy::default(),
            },
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        // Before any usage: no spend known, within budget.
        assert_eq!(
            ledger.check(&task_id, &record.node.budgets, 0),
            BudgetStatus::Ok
        );

        // The scheduler cost hook sees the same number the check does.
        ledger.consume(task_id, &snapshot(Some(0.02), &[("m", 0.02)]));
        assert_eq!(
            CostLedger::spent_usd(&ledger, &record),
            Some(ledger.usage(task_id).unwrap().cost_usd)
        );
        assert_eq!(
            ledger.check(&task_id, &record.node.budgets, 0),
            BudgetStatus::Exceeded,
            "0.02 spent against a 0.01 ceiling must escalate, not continue"
        );
    }
}
