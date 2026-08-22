//! The orchestrator loop: snapshot → model → operations → engine.
//!
//! One cycle is deliberately small and total:
//!
//! ```text
//! refresh engine state ─▶ build snapshot ─▶ prompt model ─▶ parse (total)
//!        ▲                                                       │
//!        │                                                       ▼
//!        └──────── rejections fed back next cycle ◀──── apply to plan draft
//! ```
//!
//! Three invariants hold at every step:
//!
//! 1. **The engine is authoritative.** The orchestrator only ever calls
//!    [`PlanSink`], whose whole surface is "validate and materialize",
//!    "read state back" and "raise a priority". It cannot write task rows.
//! 2. **The orchestrator is a proposer, not a liveness dependency.** A
//!    model outage, a parse failure or a batch of rejections produces a
//!    [`PlanCycleReport`] — never an error that could propagate into engine
//!    control flow. Once a plan is committed the run finishes with or
//!    without this type (PRD §9 OR-01 acceptance criterion 2).
//! 3. **The user gates the phases** (mastermind pattern, HANDOFF-BUILD-2
//!    §2): planning cycles never commit themselves. [`Orchestrator::commit`]
//!    is an explicit call the caller makes after showing the plan.

use std::sync::Arc;

use uuid::Uuid;

use crate::error::{excerpt, OrchestratorError, Rejection, RejectionReason};
use crate::model::{ModelResponse, PlanningModel};
use crate::operation::PlanOperation;
use crate::parse::parse_operations;
use crate::plan::{Plan, PlanPolicy};
use crate::sink::{PlanSink, RunView};
use crate::snapshot::PlanSnapshot;

/// What one orchestrator cycle did.
///
/// Every field is observational: a cycle report is a record, not a control
/// signal. `model_error` and `engine_error` are *reported*, never returned
/// as `Err`, because a failing orchestrator must not stop a run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PlanCycleReport {
    /// Zero-based cycle index.
    pub cycle: u32,
    /// Operations accepted into the plan, in order.
    pub accepted: Vec<PlanOperation>,
    /// Everything refused, with machine-readable reasons.
    pub rejected: Vec<Rejection>,
    /// The proposer was unavailable this cycle (no operations were lost —
    /// none were produced).
    pub model_error: Option<String>,
    /// A read or priority write against the engine failed. The plan draft
    /// is still valid; the next cycle re-reads.
    pub engine_error: Option<String>,
    /// The provider reported rate-limit pressure; back off before the next
    /// cycle (F-00 §4: observed on this account at 0.86 seven-day
    /// utilization).
    pub rate_limited: bool,
    /// Bounded excerpt of the raw model output, for the audit trail.
    pub raw_excerpt: String,
}

impl PlanCycleReport {
    /// Whether the model proposed anything at all (accepted or refused).
    pub fn did_propose(&self) -> bool {
        !self.accepted.is_empty() || !self.rejected.is_empty()
    }
}

/// The master orchestrator (PRD §9 OR-01).
pub struct Orchestrator {
    model: Arc<dyn PlanningModel>,
    sink: Arc<dyn PlanSink>,
    plan: Plan,
    cycle: u32,
    /// Rejections from the previous cycle, replayed into the next prompt as
    /// the self-correction signal.
    pending_rejections: Vec<Rejection>,
}

impl Orchestrator {
    /// Build an orchestrator for `goal`.
    pub fn new(
        goal: impl Into<String>,
        policy: PlanPolicy,
        model: Arc<dyn PlanningModel>,
        sink: Arc<dyn PlanSink>,
    ) -> Self {
        Self {
            model,
            sink,
            plan: Plan::new(goal, policy),
            cycle: 0,
            pending_rejections: Vec::new(),
        }
    }

    /// The current plan.
    pub fn plan(&self) -> &Plan {
        &self.plan
    }

    /// Cycles consumed.
    pub fn cycle_count(&self) -> u32 {
        self.cycle
    }

    /// The rejections the next prompt will carry.
    pub fn pending_rejections(&self) -> &[Rejection] {
        &self.pending_rejections
    }

    /// The state snapshot as the model would see it right now.
    pub fn snapshot(&self) -> PlanSnapshot {
        PlanSnapshot::build(&self.plan, None, self.cycle, &self.pending_rejections)
    }

    /// Read the engine's view of the committed run and refresh the plan's
    /// cached status. `Ok(None)` when nothing is committed yet.
    pub fn refresh(&mut self) -> Result<Option<RunView>, OrchestratorError> {
        let Some(run_id) = self.plan.run_id() else {
            return Ok(None);
        };
        let view = self.sink.run_view(&run_id)?;
        self.plan.observe_run_status(view.status);
        Ok(Some(view))
    }

    /// Commit the plan into a run — the user-gated phase transition.
    ///
    /// The engine validates the compiled spec again and materializes every
    /// task in one transaction. After this returns, the run proceeds under
    /// the engine's own scheduler whether or not this orchestrator ever
    /// runs another cycle.
    pub fn commit(&mut self) -> Result<Uuid, OrchestratorError> {
        if let Some(run_id) = self.plan.run_id() {
            return Err(RejectionReason::RunAlreadyStarted {
                op: "commit".to_owned(),
                run_id: run_id.to_string(),
            }
            .into());
        }
        if self.plan.nodes().is_empty() {
            return Err(RejectionReason::EmptyPlan.into());
        }
        let run_id = self.sink.commit(&self.plan)?;
        self.plan.mark_committed(run_id);
        let _ = self.refresh();
        tracing::info!(run_id = %run_id, goal = %self.plan.goal,
            nodes = self.plan.nodes().len(), "orchestrator committed a plan");
        Ok(run_id)
    }

    /// Run one cycle. Never fails: every problem is reported.
    pub async fn cycle(&mut self) -> PlanCycleReport {
        let mut report = PlanCycleReport {
            cycle: self.cycle,
            ..PlanCycleReport::default()
        };
        self.cycle += 1;

        // 1. The engine's state, not our memory of it.
        let run = match self.refresh() {
            Ok(view) => view,
            Err(err) => {
                report.engine_error = Some(err.to_string());
                None
            }
        };

        // 2. Compact snapshot + last cycle's corrections.
        let snapshot = PlanSnapshot::build(
            &self.plan,
            run.as_ref(),
            report.cycle,
            &self.pending_rejections,
        );
        let prompt = snapshot.render_prompt();

        // 3. Ask the proposer. An outage ends the cycle without touching
        //    anything the engine owns.
        let response: ModelResponse = match self.model.propose(&prompt).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(error = %err,
                    "orchestrator model unavailable; the run continues without new proposals");
                report.model_error = Some(err.to_string());
                self.pending_rejections.clear();
                return report;
            }
        };
        report.rate_limited = response.rate_limited;
        report.raw_excerpt = excerpt(&response.text);

        // 4. Total parse. Anything unparseable becomes a rejection.
        let parsed = parse_operations(&response.text);
        let mut rejections = parsed.rejected;

        // 5. Per-cycle operation ceiling. Surplus operations are rejected,
        //    never truncated away (PRD Appendix G: bounded, and visible).
        let limit = self.plan.policy.max_operations_per_cycle;
        let (within_budget, surplus): (Vec<_>, Vec<_>) = parsed
            .accepted
            .into_iter()
            .partition(|(index, _)| *index < limit);
        for (index, operation) in surplus {
            rejections.push(Rejection::new(
                index,
                serde_json::to_value(&operation).unwrap_or(serde_json::Value::Null),
                RejectionReason::OperationBudgetExceeded { limit, index },
            ));
        }

        // 6. Apply to the draft; rejections do not abort the batch.
        let indexed: Vec<(usize, &PlanOperation)> = within_budget
            .iter()
            .map(|(index, operation)| (*index, operation))
            .collect();
        let apply_rejections = self.plan.apply_all(indexed);
        let refused: std::collections::BTreeSet<usize> =
            apply_rejections.iter().map(|r| r.index).collect();
        rejections.extend(apply_rejections);

        report.accepted = within_budget
            .into_iter()
            .filter(|(index, _)| !refused.contains(index))
            .map(|(_, operation)| operation)
            .collect();

        // 7. Engine-side effect of accepted escalations on a live run.
        if let Some(run_id) = self.plan.run_id() {
            for operation in &report.accepted {
                if let PlanOperation::Escalate(payload) = operation {
                    match self.sink.escalate(&run_id, payload.node_id.as_deref()) {
                        Ok(retuned) => tracing::info!(retuned, "escalation applied to the run"),
                        Err(err) => {
                            report.engine_error = Some(err.to_string());
                        }
                    }
                }
            }
        }

        rejections.sort_by_key(|rejection| rejection.index);
        self.pending_rejections = rejections.clone();
        report.rejected = rejections;
        tracing::info!(
            cycle = report.cycle,
            accepted = report.accepted.len(),
            rejected = report.rejected.len(),
            "orchestrator cycle complete"
        );
        report
    }

    /// Run cycles until the model proposes nothing, the goal closes, the
    /// proposer goes down, or the cycle ceiling is reached.
    ///
    /// Bounded by `policy.max_cycles` (PRD Appendix G: every loop is
    /// bounded by count, time or budget). Returns one report per cycle;
    /// never returns `Err`.
    pub async fn run_planning(&mut self) -> Vec<PlanCycleReport> {
        let mut reports = Vec::new();
        while self.cycle < self.plan.policy.max_cycles && !self.plan.is_closed() {
            let report = self.cycle().await;
            let stop = report.model_error.is_some() || !report.did_propose();
            reports.push(report);
            if stop {
                break;
            }
        }
        reports
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ScriptedPlanningModel;
    use crate::sink::WorkflowSink;
    use agentos_workflow::{RunStatus, TaskStore};

    fn sink() -> Arc<WorkflowSink> {
        Arc::new(WorkflowSink::new(Arc::new(
            TaskStore::open_in_memory().unwrap(),
        )))
    }

    fn orchestrator(script: Vec<&str>) -> Orchestrator {
        Orchestrator::new(
            "ship the API",
            PlanPolicy::default(),
            Arc::new(ScriptedPlanningModel::new(script)),
            sink(),
        )
    }

    const PLAN_JSON: &str = r#"```json
[
  {"op":"create_task","nodeId":"spec","pool":"backend","objective":"write the contract"},
  {"op":"create_task","nodeId":"build","dependsOn":["spec"],"pool":"backend"},
  {"op":"request_review","nodeId":"build"}
]
```"#;

    #[tokio::test]
    async fn a_cycle_turns_model_text_into_an_engine_valid_plan() {
        let mut orchestrator = orchestrator(vec![PLAN_JSON, "[]"]);
        let report = orchestrator.cycle().await;
        assert!(report.rejected.is_empty(), "{:?}", report.rejected);
        assert_eq!(report.accepted.len(), 3);
        assert_eq!(
            orchestrator.plan().node_ids(),
            vec!["spec", "build", "review-build"]
        );
        orchestrator.plan().validate().unwrap();
        assert!(!report.raw_excerpt.is_empty());
    }

    #[tokio::test]
    async fn rejections_are_reported_and_replayed_into_the_next_prompt() {
        let mut orchestrator = orchestrator(vec![
            r#"[{"op":"create_task","nodeId":"a"},{"op":"create_task","nodeId":"a"},{"op":"wat"}]"#,
        ]);
        let report = orchestrator.cycle().await;
        assert_eq!(report.accepted.len(), 1);
        assert_eq!(report.rejected.len(), 2);
        let codes: Vec<&str> = report
            .rejected
            .iter()
            .map(|rejection| rejection.reason.code())
            .collect();
        assert_eq!(codes, vec!["duplicate_node", "unknown_operation"]);
        // Indices are preserved and the rejections become the next prompt's
        // correction block.
        assert_eq!(orchestrator.pending_rejections().len(), 2);
        let prompt = orchestrator.snapshot().render_prompt();
        assert!(prompt.contains("REJECTED"), "{prompt}");
        assert!(prompt.contains("duplicate_node"));
    }

    #[tokio::test]
    async fn a_model_outage_is_reported_and_never_becomes_an_error() {
        let mut orchestrator = Orchestrator::new(
            "ship it",
            PlanPolicy::default(),
            Arc::new(ScriptedPlanningModel::with_failures(vec![Err(
                "claude binary not found".to_owned(),
            )])),
            sink(),
        );
        let report = orchestrator.cycle().await;
        assert!(report.model_error.is_some());
        assert!(report.accepted.is_empty());
        assert!(report.rejected.is_empty());
        assert!(orchestrator.plan().nodes().is_empty());
    }

    #[tokio::test]
    async fn prose_only_output_produces_one_rejection_and_no_plan_change() {
        let mut orchestrator = orchestrator(vec!["I think we should start with a spec."]);
        let report = orchestrator.cycle().await;
        assert!(report.accepted.is_empty());
        assert_eq!(report.rejected.len(), 1);
        assert_eq!(report.rejected[0].reason.code(), "unparseable_output");
        assert!(orchestrator.plan().nodes().is_empty());
    }

    #[tokio::test]
    async fn the_per_cycle_operation_ceiling_rejects_the_surplus() {
        let policy = PlanPolicy {
            max_operations_per_cycle: 2,
            ..PlanPolicy::default()
        };
        let script = r#"[{"op":"create_task","nodeId":"a"},{"op":"create_task","nodeId":"b"},{"op":"create_task","nodeId":"c"}]"#;
        let mut orchestrator = Orchestrator::new(
            "ship it",
            policy,
            Arc::new(ScriptedPlanningModel::new([script])),
            sink(),
        );
        let report = orchestrator.cycle().await;
        assert_eq!(report.accepted.len(), 2);
        assert_eq!(report.rejected.len(), 1);
        assert_eq!(
            report.rejected[0].reason.code(),
            "operation_budget_exceeded"
        );
        assert_eq!(orchestrator.plan().node_ids(), vec!["a", "b"]);
    }

    #[tokio::test]
    async fn commit_is_an_explicit_user_gate_and_freezes_the_graph() {
        let mut orchestrator =
            orchestrator(vec![PLAN_JSON, r#"[{"op":"create_task","nodeId":"late"}]"#]);
        orchestrator.cycle().await;
        // Nothing was committed by the planning cycle itself.
        assert!(orchestrator.plan().run_id().is_none());

        let run_id = orchestrator.commit().unwrap();
        assert_eq!(orchestrator.plan().run_id(), Some(run_id));
        assert_eq!(orchestrator.plan().run_status(), Some(RunStatus::Running));

        // A second commit is refused.
        let err = orchestrator.commit().unwrap_err();
        assert!(matches!(
            err,
            OrchestratorError::Rejected(RejectionReason::RunAlreadyStarted { .. })
        ));

        // Structural proposals after the commit are refused by the plan.
        let report = orchestrator.cycle().await;
        assert!(report.accepted.is_empty());
        assert_eq!(report.rejected[0].reason.code(), "run_already_started");
    }

    #[tokio::test]
    async fn committing_an_empty_plan_is_refused_before_any_durable_state() {
        let mut orchestrator = orchestrator(vec!["[]"]);
        let err = orchestrator.commit().unwrap_err();
        assert!(matches!(
            err,
            OrchestratorError::Rejected(RejectionReason::EmptyPlan)
        ));
    }

    #[tokio::test]
    async fn an_accepted_escalation_retunes_priority_through_the_engine() {
        let mut orchestrator = orchestrator(vec![
            PLAN_JSON,
            r#"[{"op":"escalate","nodeId":"build","target":"human","reason":"contract unclear"}]"#,
        ]);
        orchestrator.cycle().await;
        let run_id = orchestrator.commit().unwrap();

        let report = orchestrator.cycle().await;
        assert_eq!(report.accepted.len(), 1);
        assert!(report.engine_error.is_none(), "{:?}", report.engine_error);
        let view = orchestrator.sink.run_view(&run_id).unwrap();
        let build = view.tasks.iter().find(|t| t.node_id == "build").unwrap();
        assert_eq!(build.priority, agentos_core::Priority::P0);
        assert_eq!(orchestrator.plan().escalations().len(), 1);
    }

    #[tokio::test]
    async fn run_planning_is_bounded_by_max_cycles() {
        // A model that keeps proposing forever must still terminate.
        let policy = PlanPolicy {
            max_cycles: 3,
            ..PlanPolicy::default()
        };
        let mut orchestrator = Orchestrator::new(
            "ship it",
            policy,
            // Always the same duplicate proposal: every cycle rejects, so
            // the loop only stops on the cycle ceiling.
            Arc::new(ScriptedPlanningModel::new([
                r#"[{"op":"create_task","nodeId":"a"}]"#,
            ])),
            sink(),
        );
        let reports = orchestrator.run_planning().await;
        assert_eq!(reports.len(), 3);
        assert_eq!(orchestrator.cycle_count(), 3);
        assert_eq!(orchestrator.plan().node_ids(), vec!["a"]);
        assert_eq!(reports[1].rejected[0].reason.code(), "duplicate_node");
    }

    #[tokio::test]
    async fn run_planning_stops_when_the_model_proposes_nothing() {
        let mut orchestrator = orchestrator(vec![PLAN_JSON, "[]"]);
        let reports = orchestrator.run_planning().await;
        assert_eq!(reports.len(), 2);
        assert!(reports[1].accepted.is_empty() && reports[1].rejected.is_empty());
    }
}
