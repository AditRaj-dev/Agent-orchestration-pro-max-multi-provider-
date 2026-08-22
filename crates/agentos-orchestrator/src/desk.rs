//! Where an escalation *goes* when the engine cannot absorb it.
//!
//! Raising a task's priority is the engine-side lever (F-06
//! `set_priority`), and retargeting it at a stronger pool is the other
//! ([`PlanSink::retarget`](crate::PlanSink::retarget)). Neither reaches a
//! person. `EscalationTarget::Human` needs a surface a human actually
//! watches, and in this system that surface is F-10's approval store: the
//! same table the supervisor's gates block on, so an escalation shows up
//! next to the approvals rather than in a log nobody reads.
//!
//! The desk is a **seam, not a requirement**. An orchestrator without one
//! still records the escalation in its ledger and raises priority; the
//! cycle report says plainly that nothing routed it further, instead of
//! implying a human was told.

use std::sync::Arc;

use agentos_policy::{ApprovalStore, Gate};
use chrono::Duration;
use serde_json::json;
use uuid::Uuid;

use crate::error::OrchestratorError;
use crate::operation::EscalationTarget;

/// Somewhere an escalation can be raised for human attention.
pub trait EscalationDesk: Send + Sync {
    /// Raise `reason` for `node_id` (or the whole run) and return an
    /// identifier a human can be pointed at.
    fn raise(
        &self,
        run_id: &Uuid,
        node_id: Option<&str>,
        target: EscalationTarget,
        reason: &str,
    ) -> Result<String, OrchestratorError>;
}

/// An [`EscalationDesk`] over F-10's approval store: the escalation becomes
/// a pending approval request bound to a canonical operation payload, so it
/// appears on the same surface as every other decision waiting on a person.
///
/// Requests are **reusable**, not single-use: an escalation is a question
/// about a task, not authorization for one irreversible act.
pub struct ApprovalDesk {
    approvals: Arc<ApprovalStore>,
    gate: Gate,
    ttl: Duration,
    requested_by: String,
}

impl ApprovalDesk {
    /// Default desk: [`Gate::ProdAction`] (F-10's "irreversible action
    /// requiring a human"; its gate set has no generic human-decision
    /// member) with a one-hour ttl.
    pub fn new(approvals: Arc<ApprovalStore>, requested_by: impl Into<String>) -> Self {
        Self {
            approvals,
            gate: Gate::ProdAction,
            ttl: Duration::seconds(3600),
            requested_by: requested_by.into(),
        }
    }

    /// Override the gate the escalation is filed under.
    pub fn with_gate(mut self, gate: Gate) -> Self {
        self.gate = gate;
        self
    }

    /// Override how long the raised request stays live.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }
}

impl EscalationDesk for ApprovalDesk {
    fn raise(
        &self,
        run_id: &Uuid,
        node_id: Option<&str>,
        target: EscalationTarget,
        reason: &str,
    ) -> Result<String, OrchestratorError> {
        // Canonical payload: the same run/node/target/reason always
        // fingerprints identically, so a repeated escalation of the same
        // problem does not spam a human with distinct-looking requests.
        let operation = json!({
            "kind": "escalation",
            "runId": run_id.to_string(),
            "node": node_id,
            "target": target.as_str(),
            "reason": reason,
        });
        let request =
            self.approvals
                .request(self.gate, &operation, &self.requested_by, self.ttl)?;
        tracing::info!(run_id = %run_id, node = node_id.unwrap_or("<run>"),
            target = target.as_str(), request = %request.id,
            "escalation raised to a human on the approval surface");
        Ok(request.id)
    }
}
