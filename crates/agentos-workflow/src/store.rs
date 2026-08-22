//! Durable SQLite task store (PRD §9 OR-03: "durable task records so
//! application restarts do not lose run state").
//!
//! The open helper is a **private copy of the F-01 SQLite canon**
//! (`docs/HANDOFF-BUILD.md` §4, as implemented by `agentos-daemon/src/db.rs`,
//! deliberately not a dependency): busy timeout on every connection first;
//! `journal_mode` read before it is ever set; versioned migrations in an
//! explicit transaction; SQLITE_BUSY surfaced as retryable
//! [`CoreError::SqliteBusy`].
//!
//! State changes are compare-and-swap single statements —
//! `UPDATE tasks SET state = ? WHERE id = ? AND state = ?` — with the arc
//! legality checked through agentos-core's [`TaskState::can_transition`]
//! *before* the SQL runs (transition legality is core's job; this store
//! delegates and then enforces durably). Zero rows affected means the
//! precondition state no longer holds; the row is re-read and the failure is
//! reported as [`CoreError::IllegalTransition`] with the *actual* state (or
//! [`CoreError::NotFound`] when the row vanished). Multi-write operations
//! (run creation, success walk, failure recording) run inside explicit
//! transactions.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use agentos_core::{CoreError, Priority, TaskState};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, Row, Transaction};
use uuid::Uuid;

use crate::error::WorkflowError;
use crate::spec::{NodeSpec, TaskContract, WorkflowSpec};

/// Busy-handler wait applied to every connection opened by [`open_db`].
const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

/// Retries for the conditional `journal_mode=WAL` set. This pragma can
/// return SQLITE_BUSY *without honoring* the busy handler, so contention is
/// resolved by re-trying with a delay.
const WAL_SET_ATTEMPTS: u32 = 5;
const WAL_SET_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Highest schema version this build understands.
const SCHEMA_VERSION: i64 = 1;

/// Migration v1: the durable `runs` and `tasks` tables.
const MIGRATION_V1: &str = r#"
-- One row per workflow execution (PRD §9 OR-03 durable run state).
CREATE TABLE IF NOT EXISTS runs (
    id                TEXT PRIMARY KEY,   -- run id (UUIDv7)
    workflow_id       TEXT NOT NULL,
    workflow_version  INTEGER NOT NULL,
    goal              TEXT NOT NULL,
    status            TEXT NOT NULL,      -- RunStatus wire string
    created_at        TEXT NOT NULL       -- RFC 3339
);

-- One durable task record per workflow node. Each row is self-describing:
-- the `node` column embeds the full NodeSpec (type, deps, budgets, retry),
-- so the scheduler enforces governance after a restart without re-reading
-- the original spec.
CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,    -- task id (UUIDv7)
    run_id           TEXT NOT NULL REFERENCES runs(id),
    workflow_id      TEXT NOT NULL,
    node_id          TEXT NOT NULL,
    state            TEXT NOT NULL,       -- TaskState wire string
    priority         TEXT NOT NULL,       -- Priority wire string
    lease_owner      TEXT,
    lease_expires_at TEXT,                -- RFC 3339
    heartbeat_at     TEXT,                -- RFC 3339
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    contract         TEXT NOT NULL,       -- TaskContract JSON
    node             TEXT NOT NULL,       -- NodeSpec JSON (budgets/retry/deps)
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    UNIQUE (run_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_state_priority_created
    ON tasks (state, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_run_id ON tasks (run_id);
CREATE INDEX IF NOT EXISTS idx_tasks_lease_expiry ON tasks (state, lease_expires_at);
"#;

/// Column list shared by every task SELECT.
const TASK_COLUMNS: &str = "id, run_id, workflow_id, node_id, state, priority, \
                            lease_owner, lease_expires_at, heartbeat_at, \
                            attempt_count, contract, node, created_at, updated_at";

/// Run status (projection over its tasks; the tasks are the source of truth).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Tasks still active or pending.
    Running,
    /// Every task reached `Done`.
    Completed,
    /// At least one task reached `Failed`.
    Failed,
}

impl RunStatus {
    /// The canonical snake_case wire string.
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
        }
    }

    /// Derive the run status from its tasks (the durable tasks are the
    /// source of truth; the stored column is a cached projection).
    pub fn from_tasks(tasks: &[TaskRecord]) -> Self {
        if tasks.iter().any(|task| task.state == TaskState::Failed) {
            RunStatus::Failed
        } else if tasks.iter().all(|task| task.state == TaskState::Done) {
            RunStatus::Completed
        } else {
            RunStatus::Running
        }
    }
}

/// A durable run record.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRecord {
    pub id: Uuid,
    pub workflow_id: String,
    pub workflow_version: u32,
    pub goal: String,
    pub status: RunStatus,
    pub created_at: DateTime<Utc>,
}

/// A durable task record — one per workflow node, self-describing via the
/// embedded [`NodeSpec`] (node type, dependencies, budgets, retry policy).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRecord {
    pub id: Uuid,
    pub run_id: Uuid,
    pub workflow_id: String,
    /// The workflow node this task materializes.
    pub node_id: String,
    pub state: TaskState,
    pub priority: Priority,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    /// Consumed attempts: incremented whenever an attempt ends without
    /// success (reported failure or lease-expiry reclaim). A lease is
    /// granted only while this is below `budgets.max_attempts`.
    pub attempt_count: u32,
    /// The OR-04 task contract handed to executors.
    pub contract: TaskContract,
    /// The governing node spec (type, deps, budgets, retry policy).
    pub node: NodeSpec,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TaskRecord {
    /// Whether every dependency of this task's node is in terminal success
    /// (`Done`), given the sibling tasks of the same run keyed by node id.
    pub fn dependencies_satisfied(
        &self,
        siblings: &std::collections::HashMap<String, TaskState>,
    ) -> bool {
        self.node
            .depends_on
            .iter()
            .all(|dep| siblings.get(dep) == Some(&TaskState::Done))
    }
}

/// The durable task store: one SQLite database, one connection behind a
/// mutex (short synchronous critical sections; the lock is never held
/// across an await).
pub struct TaskStore {
    conn: Mutex<Connection>,
}

impl TaskStore {
    /// Open (creating if needed) the workflow database at `path` and bring
    /// it to the current schema version. Applies the F-01 canon — see the
    /// [module docs](self).
    pub fn open(path: &Path) -> Result<Self, CoreError> {
        let conn = open_db(path)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open an in-memory database (tests and examples). Journal mode is
    /// already `memory` here, so the read-first WAL dance does not apply.
    pub fn open_in_memory() -> Result<Self, CoreError> {
        let mut conn = Connection::open_in_memory().map_err(map_sqlite_error)?;
        conn.busy_timeout(BUSY_TIMEOUT).map_err(map_sqlite_error)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(map_sqlite_error)?;
        migrate(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Lock the connection (poisoned locks surface as serialization
    /// errors rather than panics).
    fn lock(&self) -> Result<MutexGuard<'_, Connection>, CoreError> {
        self.conn
            .lock()
            .map_err(|_| CoreError::Serialization("task store mutex poisoned".to_owned()))
    }

    // ---------------------------------------------------------------- runs

    /// Materialize a validated spec into one run row plus one durable task
    /// row per node, in a single transaction (all-or-nothing run creation).
    ///
    /// Tasks are created `Planned`; nodes without dependencies are promoted
    /// to `Ready` in the same transaction (Planned -> Ready arc legality is
    /// checked via [`TaskState::can_transition`]).
    ///
    /// Task priority defaults to `P2` (NodeSpec carries no priority field;
    /// OR-01's orchestrator can retune with [`TaskStore::set_priority`]).
    pub fn create_run(&self, spec: &WorkflowSpec, goal: &str) -> Result<Uuid, CoreError> {
        let now = Utc::now();
        let run_id = Uuid::now_v7();
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        let result = (|| -> Result<(), CoreError> {
            tx.execute(
                "INSERT INTO runs (id, workflow_id, workflow_version, goal, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    run_id.to_string(),
                    spec.id,
                    spec.version as i64,
                    goal,
                    RunStatus::Running.as_str(),
                    now.to_rfc3339()
                ],
            )
            .map_err(map_sqlite_error)?;
            for node in &spec.nodes {
                let task_id = Uuid::now_v7();
                let contract = TaskContract::for_node(node, goal);
                let contract_json = serde_json::to_string(&contract)
                    .map_err(|err| CoreError::Serialization(format!("contract: {err}")))?;
                let node_json = serde_json::to_string(node)
                    .map_err(|err| CoreError::Serialization(format!("node spec: {err}")))?;
                tx.execute(
                    "INSERT INTO tasks (
                        id, run_id, workflow_id, node_id, state, priority,
                        lease_owner, lease_expires_at, heartbeat_at, attempt_count,
                        contract, node, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, 0, ?7, ?8, ?9, ?9)",
                    rusqlite::params![
                        task_id.to_string(),
                        run_id.to_string(),
                        spec.id,
                        node.id,
                        TaskState::Planned.as_str(),
                        priority_wire(&Priority::P2),
                        contract_json,
                        node_json,
                        now.to_rfc3339()
                    ],
                )
                .map_err(map_sqlite_error)?;
                if node.depends_on.is_empty() {
                    debug_assert!(TaskState::Planned.can_transition(&TaskState::Ready));
                    cas_update_state(&tx, &task_id, TaskState::Planned, TaskState::Ready, now)?;
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                tx.commit().map_err(map_sqlite_error)?;
                Ok(run_id)
            }
            Err(err) => {
                let _ = tx.rollback();
                Err(err)
            }
        }
    }

    /// Add one node to a **live** run — the F-12 re-planning path.
    ///
    /// Validate-then-insert in a single transaction: the tentative spec is
    /// `existing nodes + node`, run through [`crate::validate`], so
    /// duplicate ids, dangling dependencies, cycles and unbounded loops are
    /// rejected by exactly the rules that governed `create_run`. Validating
    /// *inside* the write transaction is what stops two concurrent adds
    /// from closing a cycle between themselves.
    ///
    /// The new task lands in the state its dependencies dictate — `Ready`
    /// when they are all `Done` (or there are none), `Blocked` when any of
    /// them already failed or was cancelled (nothing would ever promote it),
    /// `Planned` otherwise.
    ///
    /// There is deliberately **no run-status gate**: [`RunStatus`] is a
    /// projection recomputed from the tasks, so a run whose work has all
    /// landed reads `completed` — and re-planning onto exactly that run
    /// (add a follow-up, add a fix behind a failure) is the case this
    /// method exists for. The next status refresh flips the projection back
    /// to `running`. Goal *closure* is a planning-layer concept (F-12's
    /// `close_goal`), not a durable property of the run row.
    pub fn add_task(&self, run_id: &Uuid, node: &NodeSpec) -> Result<Uuid, WorkflowError> {
        let now = Utc::now();
        let task_id = Uuid::now_v7();
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        let result = (|| -> Result<(), WorkflowError> {
            let run = run_in_tx(&tx, run_id)?;
            let siblings = tasks_for_run_in_tx(&tx, run_id)?;

            // Whole-DAG validation, not a local dependency check: the new
            // edge set is only legal if the resulting graph is.
            let mut nodes: Vec<NodeSpec> = siblings.iter().map(|task| task.node.clone()).collect();
            nodes.push(node.clone());
            crate::validate::validate(&WorkflowSpec {
                id: run.workflow_id.clone(),
                version: run.workflow_version,
                nodes,
            })?;

            let contract = TaskContract::for_node(node, &run.goal);
            let contract_json = serde_json::to_string(&contract)
                .map_err(|err| CoreError::Serialization(format!("contract: {err}")))?;
            let node_json = serde_json::to_string(node)
                .map_err(|err| CoreError::Serialization(format!("node spec: {err}")))?;
            tx.execute(
                "INSERT INTO tasks (
                    id, run_id, workflow_id, node_id, state, priority,
                    lease_owner, lease_expires_at, heartbeat_at, attempt_count,
                    contract, node, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, 0, ?7, ?8, ?9, ?9)",
                rusqlite::params![
                    task_id.to_string(),
                    run_id.to_string(),
                    run.workflow_id,
                    node.id,
                    TaskState::Planned.as_str(),
                    priority_wire(&Priority::P2),
                    contract_json,
                    node_json,
                    now.to_rfc3339()
                ],
            )
            .map_err(map_sqlite_error)?;

            let states: std::collections::HashMap<&str, TaskState> = siblings
                .iter()
                .map(|task| (task.node_id.as_str(), task.state))
                .collect();
            let dep_state = |dep: &String| states.get(dep.as_str()).copied();
            if node.depends_on.iter().any(|dep| {
                matches!(
                    dep_state(dep),
                    Some(TaskState::Failed | TaskState::Cancelled)
                )
            }) {
                cas_update_state(&tx, &task_id, TaskState::Planned, TaskState::Blocked, now)?;
            } else if node
                .depends_on
                .iter()
                .all(|dep| dep_state(dep) == Some(TaskState::Done))
            {
                cas_update_state(&tx, &task_id, TaskState::Planned, TaskState::Ready, now)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                tx.commit().map_err(map_sqlite_error)?;
                Ok(task_id)
            }
            Err(err) => {
                let _ = tx.rollback();
                Err(err)
            }
        }
    }

    /// Fetch one run record.
    pub fn run(&self, run_id: &Uuid) -> Result<RunRecord, CoreError> {
        let conn = self.lock()?;
        conn.query_row(
            "SELECT id, workflow_id, workflow_version, goal, status, created_at
             FROM runs WHERE id = ?1",
            rusqlite::params![run_id.to_string()],
            row_to_run,
        )
        .map_err(|err| not_found_or_sqlite(err, "run", run_id))
    }

    /// All run records, oldest first.
    pub fn runs(&self) -> Result<Vec<RunRecord>, CoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, workflow_id, workflow_version, goal, status, created_at
                 FROM runs ORDER BY created_at ASC, id ASC",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt.query_map([], row_to_run).map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }

    /// Update the cached status projection of a run.
    pub fn set_run_status(&self, run_id: &Uuid, status: RunStatus) -> Result<(), CoreError> {
        let conn = self.lock()?;
        conn.execute(
            "UPDATE runs SET status = ?2 WHERE id = ?1",
            rusqlite::params![run_id.to_string(), status.as_str()],
        )
        .map_err(map_sqlite_error)?;
        Ok(())
    }

    // --------------------------------------------------------------- tasks

    /// Fetch one task record.
    pub fn task(&self, task_id: &Uuid) -> Result<TaskRecord, CoreError> {
        let conn = self.lock()?;
        conn.query_row(
            &format!("SELECT {TASK_COLUMNS} FROM tasks WHERE id = ?1"),
            rusqlite::params![task_id.to_string()],
            row_to_task,
        )
        .map_err(|err| not_found_or_sqlite(err, "task", task_id))
    }

    /// All tasks of one run, creation order.
    pub fn tasks_for_run(&self, run_id: &Uuid) -> Result<Vec<TaskRecord>, CoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {TASK_COLUMNS} FROM tasks
                 WHERE run_id = ?1 ORDER BY created_at ASC, id ASC"
            ))
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map(rusqlite::params![run_id.to_string()], row_to_task)
            .map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }

    /// Every task in the store, creation order.
    pub fn tasks(&self) -> Result<Vec<TaskRecord>, CoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {TASK_COLUMNS} FROM tasks ORDER BY created_at ASC, id ASC"
            ))
            .map_err(map_sqlite_error)?;
        let rows = stmt.query_map([], row_to_task).map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }

    /// All tasks currently holding a live lease (`Leased` or `Running`):
    /// expiry checks cover both — an executor hanging mid-run loses its
    /// lease exactly like one that crashed before starting.
    pub fn tasks_holding_leases(&self) -> Result<Vec<TaskRecord>, CoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {TASK_COLUMNS} FROM tasks
                 WHERE state IN (?1, ?2) AND lease_expires_at IS NOT NULL
                 ORDER BY created_at ASC, id ASC"
            ))
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map(
                rusqlite::params![TaskState::Leased.as_str(), TaskState::Running.as_str()],
                row_to_task,
            )
            .map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }

    /// Tasks leased by a specific owner (the engine polls its own leases).
    pub fn leased_by(&self, owner: &str) -> Result<Vec<TaskRecord>, CoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {TASK_COLUMNS} FROM tasks
                 WHERE state = ?1 AND lease_owner = ?2 ORDER BY created_at ASC, id ASC"
            ))
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map(
                rusqlite::params![TaskState::Leased.as_str(), owner],
                row_to_task,
            )
            .map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }

    /// Retune a task's priority (scheduler queue position). Single-row
    /// update; the priority ordering itself is applied at read time by
    /// [`crate::Scheduler::ready_tasks`].
    pub fn set_priority(&self, task_id: &Uuid, priority: Priority) -> Result<(), CoreError> {
        let conn = self.lock()?;
        conn.execute(
            "UPDATE tasks SET priority = ?2, updated_at = ?3 WHERE id = ?1",
            rusqlite::params![
                task_id.to_string(),
                priority_wire(&priority),
                Utc::now().to_rfc3339()
            ],
        )
        .map_err(map_sqlite_error)?;
        Ok(())
    }

    /// Retarget a task at another agent pool (OR-01 escalation to a
    /// stronger agent), rewriting the `agent_role` of its stored node spec.
    ///
    /// Legal only while the task is **not executing** and not terminal: a
    /// swap under a `Leased`/`Running` task would change the contract the
    /// current attempt is already working against, and a terminal task has
    /// no next attempt to route. The state check and the write share one
    /// transaction, so a lease granted concurrently cannot slip past it.
    ///
    /// Returns the updated record; `Ok(None)` when the task is in a state
    /// that refuses the retarget, so callers can report "not routed" without
    /// treating it as a storage failure.
    pub fn set_agent_role(
        &self,
        task_id: &Uuid,
        role: Option<&str>,
    ) -> Result<Option<TaskRecord>, CoreError> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        let result = (|| -> Result<Option<TaskRecord>, CoreError> {
            let task = task_in_tx(&tx, task_id)?;
            if !retargetable(task.state) {
                return Ok(None);
            }
            let mut node = task.node.clone();
            node.agent_role = role.map(str::to_owned);
            let node_json = serde_json::to_string(&node)
                .map_err(|err| CoreError::Serialization(format!("node spec: {err}")))?;
            let updated = tx
                .execute(
                    "UPDATE tasks SET node = ?2, updated_at = ?3 WHERE id = ?1 AND state = ?4",
                    rusqlite::params![
                        task_id.to_string(),
                        node_json,
                        Utc::now().to_rfc3339(),
                        task.state.as_str()
                    ],
                )
                .map_err(map_sqlite_error)?;
            if updated != 1 {
                // The task moved (most likely into a lease) between the read
                // and the write: refuse rather than rewrite it mid-flight.
                return Ok(None);
            }
            Ok(Some(task_in_tx(&tx, task_id)?))
        })();
        match result {
            Ok(record) => {
                tx.commit().map_err(map_sqlite_error)?;
                Ok(record)
            }
            Err(err) => {
                let _ = tx.rollback();
                Err(err)
            }
        }
    }

    // ------------------------------------------------- compare-and-swap ops

    /// Atomically move a task between states: legality is checked through
    /// [`TaskState::can_transition`] first (core owns the graph), then
    /// `UPDATE ... WHERE id = ? AND state = ?expected` enforces it durably.
    ///
    /// Lease columns are cleared: every arc out of `Leased` releases the
    /// lease, and other states carry none.
    pub fn cas_transition(
        &self,
        task_id: &Uuid,
        expect: TaskState,
        to: TaskState,
    ) -> Result<TaskRecord, CoreError> {
        if !expect.can_transition(&to) {
            return Err(CoreError::IllegalTransition { from: expect, to });
        }
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        match cas_update_state(&tx, task_id, expect, to, Utc::now()) {
            Ok(()) => {
                let record = task_in_tx(&tx, task_id)?;
                tx.commit().map_err(map_sqlite_error)?;
                Ok(record)
            }
            Err(err) => {
                let _ = tx.rollback();
                Err(err)
            }
        }
    }

    /// CAS `Ready -> Leased` while stamping the lease columns (OR-05):
    /// owner, expiry `now + ttl`, and the initial heartbeat.
    pub fn grant_lease(
        &self,
        task_id: &Uuid,
        owner: &str,
        ttl: Duration,
    ) -> Result<TaskRecord, CoreError> {
        let expect = TaskState::Ready;
        let to = TaskState::Leased;
        if !expect.can_transition(&to) {
            return Err(CoreError::IllegalTransition { from: expect, to });
        }
        let now = Utc::now();
        let expires = now
            + chrono::Duration::from_std(ttl).map_err(|err| {
                CoreError::Serialization(format!("lease ttl out of range: {err}"))
            })?;
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        let updated = tx
            .execute(
                "UPDATE tasks SET state = ?2, lease_owner = ?3, lease_expires_at = ?4,
                 heartbeat_at = ?5, updated_at = ?5
                 WHERE id = ?1 AND state = ?6",
                rusqlite::params![
                    task_id.to_string(),
                    to.as_str(),
                    owner,
                    expires.to_rfc3339(),
                    now.to_rfc3339(),
                    expect.as_str()
                ],
            )
            .map_err(map_sqlite_error);
        match updated {
            Ok(1) => {
                let record = task_in_tx(&tx, task_id)?;
                tx.commit().map_err(map_sqlite_error)?;
                Ok(record)
            }
            Ok(_) => {
                let err = classify_cas_miss(&tx, task_id, to);
                let _ = tx.rollback();
                Err(err)
            }
            Err(err) => {
                let _ = tx.rollback();
                Err(err)
            }
        }
    }

    /// Renew a lease's liveness (works from `Leased` and `Running` — the
    /// worker holds the lease throughout execution): stamps
    /// `heartbeat_at = now` and extends `lease_expires_at` by the same ttl
    /// the lease was granted with (derived durably from
    /// `lease_expires_at - heartbeat_at`).
    ///
    /// CAS on `lease_owner = ?owner`; a miss surfaces as
    /// [`CoreError::NotFound`] (lease no longer held as this owner).
    pub fn heartbeat(&self, task_id: &Uuid, owner: &str) -> Result<TaskRecord, CoreError> {
        let task = self.task(task_id)?;
        let now = Utc::now();
        // The granted ttl is recoverable from the row: expiry was stamped
        // at `heartbeat_at + ttl` when the lease was granted.
        let renewed = match (task.lease_expires_at, task.heartbeat_at) {
            (Some(expires), Some(beat)) => expires - beat,
            _ => chrono::Duration::zero(),
        };
        let expires = now + renewed;
        {
            let conn = self.lock()?;
            let updated = conn.execute(
                "UPDATE tasks SET heartbeat_at = ?3, lease_expires_at = ?4, updated_at = ?3
                 WHERE id = ?1 AND state IN (?2, ?6) AND lease_owner = ?5",
                rusqlite::params![
                    task_id.to_string(),
                    TaskState::Leased.as_str(),
                    now.to_rfc3339(),
                    expires.to_rfc3339(),
                    owner,
                    TaskState::Running.as_str()
                ],
            );
            match updated {
                Ok(1) => {}
                Ok(_) => {
                    return Err(CoreError::NotFound(format!(
                        "task {task_id}: lease not held as '{owner}' (state = {})",
                        task.state
                    )))
                }
                Err(err) => return Err(map_sqlite_error(err)),
            }
        }
        self.task(task_id)
    }

    /// CAS `Leased -> Ready` after lease expiry (OR-05 crash recovery):
    /// increments `attempt_count` (the crashed attempt is consumed) and
    /// clears the lease columns. Budget escalation afterwards is the
    /// scheduler's call. (Expired leases on `Running` tasks take the legal
    /// `Running -> Retryable -> Ready` walk via [`TaskStore::record_failure`]
    /// — the scheduler routes them.)
    pub fn reclaim_expired(&self, task_id: &Uuid) -> Result<TaskRecord, CoreError> {
        let expect = TaskState::Leased;
        let to = TaskState::Ready;
        if !expect.can_transition(&to) {
            return Err(CoreError::IllegalTransition { from: expect, to });
        }
        {
            let conn = self.lock()?;
            let updated = conn.execute(
                "UPDATE tasks SET state = ?2, attempt_count = attempt_count + 1,
                 lease_owner = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                 updated_at = ?3
                 WHERE id = ?1 AND state = ?4",
                rusqlite::params![
                    task_id.to_string(),
                    to.as_str(),
                    Utc::now().to_rfc3339(),
                    expect.as_str()
                ],
            );
            match updated {
                Ok(1) => {}
                Ok(_) => {
                    return Err(CoreError::NotFound(format!(
                        "task {task_id}: no longer leased (wanted reclaim from {expect})"
                    )))
                }
                Err(err) => return Err(map_sqlite_error(err)),
            }
        }
        self.task(task_id)
    }

    /// Record a successful execution in one explicit transaction, walking
    /// the legal success chain of the task lifecycle:
    /// `Running -> OutputReady -> ReviewPending -> Approved -> GitQueued ->
    /// Committed -> Done` (every hop a CAS; core's state machine has no
    /// shortcut to `Done`). Returns the final record.
    pub fn record_success(&self, task_id: &Uuid) -> Result<TaskRecord, CoreError> {
        const SUCCESS_WALK: [TaskState; 6] = [
            TaskState::OutputReady,
            TaskState::ReviewPending,
            TaskState::Approved,
            TaskState::GitQueued,
            TaskState::Committed,
            TaskState::Done,
        ];
        walk_states(self, task_id, TaskState::Running, &SUCCESS_WALK)
    }

    /// Record a failed execution: consumes the attempt
    /// (`attempt_count + 1`), parks `Running -> Retryable`, then either
    /// re-queues (`Retryable -> Ready`) or escalates
    /// (`Retryable -> Failed`) per the caller's retry decision. One
    /// transaction; returns the final record.
    pub fn record_failure(&self, task_id: &Uuid, requeue: bool) -> Result<TaskRecord, CoreError> {
        let final_state = if requeue {
            TaskState::Ready
        } else {
            TaskState::Failed
        };
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        let result = (|| -> Result<(), CoreError> {
            let now = Utc::now();
            let consumed = tx.execute(
                "UPDATE tasks SET attempt_count = attempt_count + 1, updated_at = ?2
                 WHERE id = ?1 AND state = ?3",
                rusqlite::params![
                    task_id.to_string(),
                    now.to_rfc3339(),
                    TaskState::Running.as_str()
                ],
            );
            match consumed {
                Ok(1) => {}
                Ok(_) => return Err(classify_cas_miss(&tx, task_id, TaskState::Retryable)),
                Err(err) => return Err(map_sqlite_error(err)),
            }
            cas_update_state(&tx, task_id, TaskState::Running, TaskState::Retryable, now)?;
            debug_assert!(TaskState::Retryable.can_transition(&final_state));
            cas_update_state(&tx, task_id, TaskState::Retryable, final_state, now)?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                let record = task_in_tx(&tx, task_id)?;
                tx.commit().map_err(map_sqlite_error)?;
                Ok(record)
            }
            Err(err) => {
                let _ = tx.rollback();
                Err(err)
            }
        }
    }
}

// ------------------------------------------------------------------ helpers

/// Run a chain of CAS state hops inside one explicit transaction (used by
/// the success walk). Every hop is legality-checked and CAS-guarded; any
/// miss rolls the whole chain back.
fn walk_states(
    store: &TaskStore,
    task_id: &Uuid,
    from: TaskState,
    chain: &[TaskState],
) -> Result<TaskRecord, CoreError> {
    let mut conn = store.lock()?;
    let tx = conn.transaction().map_err(map_sqlite_error)?;
    let result = (|| -> Result<(), CoreError> {
        let mut expect = from;
        for &to in chain {
            debug_assert!(
                expect.can_transition(&to),
                "state walk must stay on legal arcs ({expect} -> {to})"
            );
            cas_update_state(&tx, task_id, expect, to, Utc::now())?;
            expect = to;
        }
        Ok(())
    })();
    match result {
        Ok(()) => {
            let record = task_in_tx(&tx, task_id)?;
            tx.commit().map_err(map_sqlite_error)?;
            Ok(record)
        }
        Err(err) => {
            let _ = tx.rollback();
            Err(err)
        }
    }
}

/// `UPDATE tasks SET state/updated_at WHERE id AND state = expect` — the
/// CAS primitive. Arc legality is verified through core's
/// [`TaskState::can_transition`]; zero rows affected is classified into
/// NotFound / IllegalTransition-with-actual-state.
///
/// Lease columns persist while the worker may still hold the lease
/// (`Leased`, `Running` — heartbeats must keep working during execution)
/// and are released on every hop out of that pair.
fn cas_update_state(
    tx: &Transaction<'_>,
    task_id: &Uuid,
    expect: TaskState,
    to: TaskState,
    now: DateTime<Utc>,
) -> Result<(), CoreError> {
    if !expect.can_transition(&to) {
        return Err(CoreError::IllegalTransition { from: expect, to });
    }
    let updated = if matches!(to, TaskState::Leased | TaskState::Running) {
        tx.execute(
            "UPDATE tasks SET state = ?2, updated_at = ?3 WHERE id = ?1 AND state = ?4",
            rusqlite::params![
                task_id.to_string(),
                to.as_str(),
                now.to_rfc3339(),
                expect.as_str()
            ],
        )
    } else {
        tx.execute(
            "UPDATE tasks SET state = ?2, lease_owner = NULL, lease_expires_at = NULL,
             heartbeat_at = NULL, updated_at = ?3
             WHERE id = ?1 AND state = ?4",
            rusqlite::params![
                task_id.to_string(),
                to.as_str(),
                now.to_rfc3339(),
                expect.as_str()
            ],
        )
    }
    .map_err(map_sqlite_error)?;
    match updated {
        1 => Ok(()),
        _ => Err(classify_cas_miss(tx, task_id, to)),
    }
}

/// States in which a task's node spec may be retargeted: it is queued or
/// parked, so the next lease will read the new role. Executing states
/// (`Leased`, `Running`) and terminal states are refused.
fn retargetable(state: TaskState) -> bool {
    matches!(
        state,
        TaskState::Created
            | TaskState::Planned
            | TaskState::Ready
            | TaskState::Blocked
            | TaskState::Retryable
            | TaskState::HumanRequired
    )
}

/// Read a run row inside an open transaction.
fn run_in_tx(tx: &Transaction<'_>, run_id: &Uuid) -> Result<RunRecord, CoreError> {
    tx.query_row(
        "SELECT id, workflow_id, workflow_version, goal, status, created_at
         FROM runs WHERE id = ?1",
        rusqlite::params![run_id.to_string()],
        row_to_run,
    )
    .map_err(|err| not_found_or_sqlite(err, "run", run_id))
}

/// Read every task of a run inside an open transaction.
fn tasks_for_run_in_tx(tx: &Transaction<'_>, run_id: &Uuid) -> Result<Vec<TaskRecord>, CoreError> {
    let mut stmt = tx
        .prepare(&format!(
            "SELECT {TASK_COLUMNS} FROM tasks
             WHERE run_id = ?1 ORDER BY created_at ASC, id ASC"
        ))
        .map_err(map_sqlite_error)?;
    let rows = stmt
        .query_map(rusqlite::params![run_id.to_string()], row_to_task)
        .map_err(map_sqlite_error)?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite_error)
}

/// Read a task row inside an open transaction.
fn task_in_tx(tx: &Transaction<'_>, task_id: &Uuid) -> Result<TaskRecord, CoreError> {
    tx.query_row(
        &format!("SELECT {TASK_COLUMNS} FROM tasks WHERE id = ?1"),
        rusqlite::params![task_id.to_string()],
        row_to_task,
    )
    .map_err(|err| not_found_or_sqlite(err, "task", task_id))
}

/// After a CAS miss, re-read the row and report precisely: missing row ->
/// NotFound; present row -> IllegalTransition carrying the ACTUAL state.
fn classify_cas_miss(conn: &Connection, task_id: &Uuid, attempted_to: TaskState) -> CoreError {
    let actual = conn.query_row(
        "SELECT state FROM tasks WHERE id = ?1",
        rusqlite::params![task_id.to_string()],
        |row| row.get::<_, String>(0),
    );
    let actual = match actual {
        Ok(state) => Some(state),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(err) => {
            tracing::warn!(task_id = %task_id, error = %err, "CAS miss classification read failed");
            None
        }
    };
    match actual {
        Some(state) => match parse_enum::<TaskState>(&state) {
            Ok(from) => CoreError::IllegalTransition {
                from,
                to: attempted_to,
            },
            Err(err) => err,
        },
        None => CoreError::NotFound(format!("task {task_id}")),
    }
}

/// Translate a QueryReturnedNoRows miss into NotFound, other SQL errors
/// through the busy-aware mapper.
fn not_found_or_sqlite(err: rusqlite::Error, kind: &str, id: &Uuid) -> CoreError {
    match err {
        rusqlite::Error::QueryReturnedNoRows => CoreError::NotFound(format!("{kind} {id}")),
        other => map_sqlite_error(other),
    }
}

/// Priority wire string, mirroring agentos-core's serde form (`"p0"`..`"p3"`,
/// verified by core's own tests).
fn priority_wire(priority: &Priority) -> &'static str {
    match priority {
        Priority::P0 => "p0",
        Priority::P1 => "p1",
        Priority::P2 => "p2",
        Priority::P3 => "p3",
    }
}

/// Parse a TaskState/Priority/RunStatus wire string through its own serde
/// impl, so the column format is canon-by-construction.
fn parse_enum<T: serde::de::DeserializeOwned>(wire: &str) -> Result<T, CoreError> {
    serde_json::from_value(serde_json::Value::String(wire.to_owned()))
        .map_err(|err| CoreError::Serialization(format!("unknown wire value {wire:?}: {err}")))
}

fn row_to_run(row: &Row<'_>) -> Result<RunRecord, rusqlite::Error> {
    Ok(RunRecord {
        id: parse_uuid(&row.get::<_, String>("id")?).map_err(sql_conversion_failure)?,
        workflow_id: row.get("workflow_id")?,
        workflow_version: u32::try_from(row.get::<_, i64>("workflow_version")?)
            .map_err(int_conversion_failure)?,
        goal: row.get("goal")?,
        status: parse_enum(&row.get::<_, String>("status")?).map_err(sql_conversion_failure)?,
        created_at: parse_timestamp(&row.get::<_, String>("created_at")?)
            .map_err(sql_conversion_failure)?,
    })
}

fn row_to_task(row: &Row<'_>) -> Result<TaskRecord, rusqlite::Error> {
    Ok(TaskRecord {
        id: parse_uuid(&row.get::<_, String>("id")?).map_err(sql_conversion_failure)?,
        run_id: parse_uuid(&row.get::<_, String>("run_id")?).map_err(sql_conversion_failure)?,
        workflow_id: row.get("workflow_id")?,
        node_id: row.get("node_id")?,
        state: parse_enum(&row.get::<_, String>("state")?).map_err(sql_conversion_failure)?,
        priority: parse_enum(&row.get::<_, String>("priority")?).map_err(sql_conversion_failure)?,
        lease_owner: row.get("lease_owner")?,
        lease_expires_at: row
            .get::<_, Option<String>>("lease_expires_at")?
            .map(|ts| parse_timestamp(&ts))
            .transpose()
            .map_err(sql_conversion_failure)?,
        heartbeat_at: row
            .get::<_, Option<String>>("heartbeat_at")?
            .map(|ts| parse_timestamp(&ts))
            .transpose()
            .map_err(sql_conversion_failure)?,
        attempt_count: u32::try_from(row.get::<_, i64>("attempt_count")?)
            .map_err(int_conversion_failure)?,
        contract: serde_json::from_str(&row.get::<_, String>("contract")?)
            .map_err(sql_conversion_failure)?,
        node: serde_json::from_str(&row.get::<_, String>("node")?)
            .map_err(sql_conversion_failure)?,
        created_at: parse_timestamp(&row.get::<_, String>("created_at")?)
            .map_err(sql_conversion_failure)?,
        updated_at: parse_timestamp(&row.get::<_, String>("updated_at")?)
            .map_err(sql_conversion_failure)?,
    })
}

fn parse_uuid(wire: &str) -> Result<Uuid, uuid::Error> {
    wire.parse::<Uuid>()
}

fn parse_timestamp(wire: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(wire).map(|ts| ts.with_timezone(&Utc))
}

fn sql_conversion_failure(err: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
}

fn int_conversion_failure(err: std::num::TryFromIntError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Integer, Box::new(err))
}

// ------------------------------------------------- F-01 canon (private copy)

/// Open (creating if needed) the workflow database and bring it to the
/// current schema version, applying the F-01 canon in order: busy timeout
/// first, read-first journal-mode check, then versioned migrations in an
/// explicit transaction.
fn open_db(path: &Path) -> Result<Connection, CoreError> {
    let mut conn = Connection::open(path).map_err(map_sqlite_error)?;
    conn.busy_timeout(BUSY_TIMEOUT).map_err(map_sqlite_error)?;
    ensure_wal(&conn)?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(map_sqlite_error)?;
    migrate(&mut conn)?;
    Ok(conn)
}

/// Read `journal_mode`; if it is not already `wal`, set it, retrying a few
/// times because this pragma can return SQLITE_BUSY without honoring the
/// busy handler.
fn ensure_wal(conn: &Connection) -> Result<(), CoreError> {
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(map_sqlite_error)?;
    if mode.eq_ignore_ascii_case("wal") {
        return Ok(());
    }

    for attempt in 0..WAL_SET_ATTEMPTS {
        if attempt > 0 {
            std::thread::sleep(WAL_SET_RETRY_DELAY);
        }
        match conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0)) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => return Ok(()),
            Ok(mode) => {
                return Err(CoreError::Serialization(format!(
                    "sqlite: PRAGMA journal_mode=WAL did not take effect (reported {mode})"
                )));
            }
            Err(err) if is_sqlite_busy(&err) => continue,
            Err(err) => return Err(map_sqlite_error(err)),
        }
    }
    Err(CoreError::SqliteBusy)
}

/// Apply pending schema migrations, versioned via `PRAGMA user_version`,
/// inside one explicit transaction.
fn migrate(conn: &mut Connection) -> Result<(), CoreError> {
    let current: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(map_sqlite_error)?;
    if current > SCHEMA_VERSION {
        return Err(CoreError::Serialization(format!(
            "sqlite: database user_version {current} is newer than this build supports \
             (schema version {SCHEMA_VERSION}); upgrade first"
        )));
    }
    if current == SCHEMA_VERSION {
        return Ok(());
    }
    let tx = conn.transaction().map_err(map_sqlite_error)?;
    tx.execute_batch(MIGRATION_V1).map_err(map_sqlite_error)?;
    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))
        .map_err(map_sqlite_error)?;
    tx.commit().map_err(map_sqlite_error)?;
    Ok(())
}

/// Whether a rusqlite error is any flavour of SQLITE_BUSY (primary code 5,
/// including SQLITE_BUSY_RECOVERY and SQLITE_BUSY_SNAPSHOT).
fn is_sqlite_busy(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(ffi_err, _) => {
            ffi_err.code == rusqlite::ErrorCode::DatabaseBusy
                || (ffi_err.extended_code & 0xff) == rusqlite::ffi::SQLITE_BUSY
        }
        _ => false,
    }
}

/// Translate a rusqlite error into the shared CoreError taxonomy: SQLITE_BUSY
/// maps to the retryable [`CoreError::SqliteBusy`]; everything else surfaces
/// as non-retryable `Serialization("sqlite: …")` (agentos-core keeps a
/// deliberately minimal taxonomy — same deviation note as F-01).
fn map_sqlite_error(err: rusqlite::Error) -> CoreError {
    if is_sqlite_busy(&err) {
        CoreError::SqliteBusy
    } else {
        CoreError::Serialization(format!("sqlite: {err}"))
    }
}

// -------------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Budgets, NodeSpec, NodeType, RetryPolicy};

    fn one_node_spec() -> WorkflowSpec {
        WorkflowSpec {
            id: "smoke".to_owned(),
            version: 1,
            nodes: vec![NodeSpec {
                id: "only".to_owned(),
                node_type: NodeType::Run,
                depends_on: Vec::new(),
                agent_role: None,
                budgets: Budgets::default(),
                retry: RetryPolicy::default(),
            }],
        }
    }

    /// SQLITE_BUSY must surface as the retryable CoreError::SqliteBusy, and
    /// the identical operation must succeed once the contending write lock
    /// is released (F-01 canon: busy is transient).
    #[test]
    fn busy_maps_to_retryable_core_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("workflow.db");

        let store = TaskStore::open(&db_path).expect("store open");
        // Shorten the contender's busy timeout so the test fails fast
        // instead of waiting out the default 5000ms.
        {
            let conn = store.lock().expect("conn");
            conn.busy_timeout(Duration::from_millis(100))
                .expect("short busy_timeout");
        }

        let writer = open_db(&db_path).expect("writer open");
        writer
            .execute("BEGIN IMMEDIATE", [])
            .expect("BEGIN IMMEDIATE");

        let err = store
            .create_run(&one_node_spec(), "busy proof")
            .unwrap_err();
        assert_eq!(err, CoreError::SqliteBusy);
        assert!(err.is_retryable());

        writer.execute("COMMIT", []).expect("COMMIT");
        let run_id = store
            .create_run(&one_node_spec(), "busy proof")
            .expect("retry after lock release");
        let run = store.run(&run_id).expect("run after retry");
        assert_eq!(run.goal, "busy proof");
    }

    /// The busy mapper must treat extended SQLITE_BUSY codes (primary byte
    /// 5) as busy too.
    #[test]
    fn extended_busy_codes_map_to_sqlite_busy() {
        let busy = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::Unknown,
                extended_code: rusqlite::ffi::SQLITE_BUSY_SNAPSHOT,
            },
            None,
        );
        assert!(is_sqlite_busy(&busy));
        assert_eq!(map_sqlite_error(busy), CoreError::SqliteBusy);
    }

    /// The full row round-trip: every field of a task record survives the
    /// SQLite round trip, including enums, JSON payloads and RFC 3339
    /// lease timestamps.
    #[test]
    fn task_record_round_trips_all_fields() {
        let store = TaskStore::open_in_memory().expect("in-memory store");
        let run_id = store.create_run(&one_node_spec(), "round trip").unwrap();
        let tasks = store.tasks_for_run(&run_id).unwrap();
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];
        assert_eq!(task.state, TaskState::Ready);
        assert_eq!(task.priority, Priority::P2);
        assert_eq!(task.attempt_count, 0);
        assert_eq!(task.node.node_type, NodeType::Run);
        assert_eq!(task.contract.objective, "run `only` for goal: round trip");
        assert!(task.lease_owner.is_none());
        assert!(task.lease_expires_at.is_none());
        assert!(task.heartbeat_at.is_none());

        // Lease columns round-trip through RFC 3339 timestamps.
        let leased = store
            .grant_lease(&task.id, "worker-1", Duration::from_secs(30))
            .unwrap();
        assert_eq!(leased.state, TaskState::Leased);
        assert_eq!(leased.lease_owner.as_deref(), Some("worker-1"));
        let expires = leased.lease_expires_at.expect("expiry stamped");
        let beat = leased.heartbeat_at.expect("heartbeat stamped");
        assert!(expires > beat);
        let reloaded = store.task(&task.id).unwrap();
        assert_eq!(reloaded.lease_expires_at, Some(expires));
        assert_eq!(reloaded.heartbeat_at, Some(beat));
    }

    /// CAS misses report the ACTUAL state, and illegal arcs are rejected by
    /// core's can_transition before any SQL runs.
    #[test]
    fn cas_transition_reports_actual_state_on_miss() {
        let store = TaskStore::open_in_memory().expect("in-memory store");
        let run_id = store.create_run(&one_node_spec(), "cas").unwrap();
        let task = &store.tasks_for_run(&run_id).unwrap()[0];

        // Wrong expectation: task is Ready, CAS pretends it was Running.
        let err = store
            .cas_transition(&task.id, TaskState::Running, TaskState::OutputReady)
            .unwrap_err();
        assert_eq!(
            err,
            CoreError::IllegalTransition {
                from: TaskState::Ready,
                to: TaskState::OutputReady,
            }
        );

        // Illegal arc per core: Ready -> Done does not exist.
        let err = store
            .cas_transition(&task.id, TaskState::Ready, TaskState::Done)
            .unwrap_err();
        assert_eq!(
            err,
            CoreError::IllegalTransition {
                from: TaskState::Ready,
                to: TaskState::Done,
            }
        );

        // Missing task id: NotFound, not a transition error.
        let ghost = Uuid::now_v7();
        let err = store
            .cas_transition(&ghost, TaskState::Ready, TaskState::Leased)
            .unwrap_err();
        assert!(matches!(err, CoreError::NotFound(_)));
    }

    /// A CAS transition out of Leased clears all lease columns.
    #[test]
    fn cas_transition_out_of_leased_clears_lease() {
        let store = TaskStore::open_in_memory().expect("in-memory store");
        let run_id = store.create_run(&one_node_spec(), "lease clear").unwrap();
        let task_id = store.tasks_for_run(&run_id).unwrap()[0].id;
        store
            .grant_lease(&task_id, "w", Duration::from_secs(10))
            .unwrap();

        let record = store
            .cas_transition(&task_id, TaskState::Leased, TaskState::Ready)
            .unwrap();
        assert_eq!(record.state, TaskState::Ready);
        assert!(record.lease_owner.is_none());
        assert!(record.lease_expires_at.is_none());
        assert!(record.heartbeat_at.is_none());
    }

    /// The success walk reaches Done through exactly the legal chain and
    /// refuses to start from a task that is not Running.
    #[test]
    fn record_success_walks_the_legal_chain() {
        let store = TaskStore::open_in_memory().expect("in-memory store");
        let run_id = store.create_run(&one_node_spec(), "success").unwrap();
        let task_id = store.tasks_for_run(&run_id).unwrap()[0].id;

        // Not running yet: the walk must refuse.
        let err = store.record_success(&task_id).unwrap_err();
        assert!(matches!(err, CoreError::IllegalTransition { .. }));

        store
            .grant_lease(&task_id, "w", Duration::from_secs(10))
            .unwrap();
        store
            .cas_transition(&task_id, TaskState::Leased, TaskState::Running)
            .unwrap();
        let done = store.record_success(&task_id).unwrap();
        assert_eq!(done.state, TaskState::Done);
        assert_eq!(done.attempt_count, 0);
        assert!(done.lease_owner.is_none());
    }
}
