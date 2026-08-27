//! The agent registry: durable rows for agents and skills.
//!
//! The open helper is a **private copy of the F-01 SQLite canon**
//! (`agentos-daemon/src/db.rs`'s pattern, like every other crate):
//! busy timeout first, `journal_mode` read before set, versioned
//! migrations in a transaction, SQLITE_BUSY surfaced as retryable
//! [`CoreError::SqliteBusy`]. Unlike the daemon journal these tables are
//! mutable — the audit trail for mutations is the daemon's `agent.*`
//! journal events, not triggers here.
//!
//! One connection behind a `std::sync::Mutex`, short synchronous critical
//! sections only (the daemon calls into this from sync dispatch handlers;
//! no guard ever crosses an await).

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use agentos_core::CoreError;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, Row};

use crate::error::AgentsError;
use crate::record::{AgentEffort, AgentMode, AgentRecord};
use crate::seeds::{
    builtin_agents, builtin_skills, global_skill_sections, is_global_skill, render_skill_preamble,
};
use crate::skill::SkillRecord;

/// Busy-handler wait applied to every connection (F-01 canon).
const BUSY_TIMEOUT_MS: u64 = 5000;
/// Retries for the conditional WAL set (the busy handler does not apply to
/// this pragma).
const WAL_SET_ATTEMPTS: u32 = 5;
const WAL_SET_RETRY_DELAY_MS: u64 = 200;

/// Highest schema version this build understands.
const SCHEMA_VERSION: i64 = 1;

/// Migration v1: the mutable `agents` and `skills` tables.
const MIGRATION_V1: &str = r#"
CREATE TABLE IF NOT EXISTS skills (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT NOT NULL,
    body        TEXT NOT NULL,
    builtin     INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    description    TEXT NOT NULL,
    adapter_id     TEXT NOT NULL,
    model          TEXT,
    effort         TEXT,
    mode           TEXT NOT NULL,
    skills         TEXT NOT NULL,   -- JSON array of skill ids
    tool_allowlist TEXT NOT NULL,   -- JSON array
    tool_denylist  TEXT NOT NULL,   -- JSON array
    timeout_secs   INTEGER NOT NULL,
    builtin        INTEGER NOT NULL DEFAULT 0,
    enabled        INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agents_enabled_name ON agents (enabled, name);
"#;

/// The registry: agents + skills in one SQLite database.
#[derive(Debug)]
pub struct AgentRegistry {
    conn: Mutex<Connection>,
}

impl AgentRegistry {
    /// Open (creating if needed) the registry database at `path` and bring
    /// it to the current schema version (F-01 canon).
    pub fn open(path: &Path) -> Result<Self, AgentsError> {
        let mut conn = Connection::open(path).map_err(map_sqlite_error)?;
        conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS))
            .map_err(map_sqlite_error)?;
        ensure_wal(&conn)?;
        migrate(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open an in-memory registry (tests, examples).
    pub fn open_in_memory() -> Result<Self, AgentsError> {
        let mut conn = Connection::open_in_memory().map_err(map_sqlite_error)?;
        conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS))
            .map_err(map_sqlite_error)?;
        migrate(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Lock the connection (poisoned locks surface as errors, not panics).
    fn lock(&self) -> Result<MutexGuard<'_, Connection>, AgentsError> {
        self.conn.lock().map_err(|_| {
            AgentsError::Core(CoreError::Serialization(
                "registry mutex poisoned".to_owned(),
            ))
        })
    }

    // ------------------------------------------------------------- agents

    /// Insert a new agent. Validates shape and skill references; stamps
    /// `created_at`/`updated_at`. Returns the stored record.
    pub fn create_agent(&self, mut record: AgentRecord) -> Result<AgentRecord, AgentsError> {
        record.validate()?;
        let now = Utc::now();
        record.created_at = now;
        record.updated_at = now;
        let mut conn = self.lock()?;
        if agent_exists(&conn, &record.id)? {
            return Err(AgentsError::Duplicate(format!("agent {}", record.id)));
        }
        ensure_skills_exist(&conn, &record.skills)?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        insert_agent_tx(&tx, &record)?;
        tx.commit().map_err(map_sqlite_error)?;
        Ok(record)
    }

    /// Update an existing agent by id. Validates shape and skill
    /// references; preserves `created_at` and the `builtin` flag of the
    /// stored row (a builtin stays builtin; a user agent cannot promote
    /// itself to builtin).
    pub fn update_agent(&self, mut record: AgentRecord) -> Result<AgentRecord, AgentsError> {
        record.validate()?;
        let mut conn = self.lock()?;
        let stored: Option<(bool, DateTime<Utc>)> = conn
            .query_row(
                "SELECT builtin, created_at FROM agents WHERE id = ?1",
                [&record.id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)? != 0,
                        parse_rfc3339(&row.get::<_, String>(1)?),
                    ))
                },
            )
            .map(Some)
            .or_else(|err| match err {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(map_sqlite_error(other)),
            })?;
        let Some((builtin, created_at)) = stored else {
            return Err(AgentsError::NotFound(format!("agent {}", record.id)));
        };
        record.builtin = builtin;
        record.created_at = created_at;
        record.updated_at = Utc::now();
        ensure_skills_exist(&conn, &record.skills)?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        insert_agent_tx(&tx, &record)?;
        tx.commit().map_err(map_sqlite_error)?;
        Ok(record)
    }

    /// Delete an agent by id. Built-ins are refused (edit instead).
    pub fn delete_agent(&self, id: &str) -> Result<(), AgentsError> {
        let conn = self.lock()?;
        let builtin = conn
            .query_row("SELECT builtin FROM agents WHERE id = ?1", [id], |row| {
                row.get::<_, i64>(0)
            })
            .map(|v| v != 0)
            .map_err(|err| match err {
                rusqlite::Error::QueryReturnedNoRows => {
                    AgentsError::NotFound(format!("agent {id}"))
                }
                other => map_sqlite_error(other),
            })?;
        if builtin {
            return Err(AgentsError::BuiltinProtected(id.to_owned()));
        }
        conn.execute("DELETE FROM agents WHERE id = ?1", [id])
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    /// Fetch one agent by id.
    pub fn get_agent(&self, id: &str) -> Result<Option<AgentRecord>, AgentsError> {
        let conn = self.lock()?;
        agent_by_id(&conn, id)
    }

    /// All agents, ordered by name (built-ins first on ties).
    pub fn list_agents(&self) -> Result<Vec<AgentRecord>, AgentsError> {
        let conn = self.lock()?;
        let sql = format!("SELECT {AGENT_COLUMNS} FROM agents ORDER BY builtin DESC, name ASC");
        let mut stmt = conn.prepare(&sql).map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map([], agent_from_row)
            .map_err(map_sqlite_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)?;
        Ok(rows)
    }

    /// Resolve an id to its record when it exists **and is enabled** —
    /// the supervisor's routing lookup (disabled agents do not route).
    pub fn resolve(&self, id: &str) -> Result<Option<AgentRecord>, AgentsError> {
        Ok(self.get_agent(id)?.filter(|record| record.enabled))
    }

    /// Enabled agents, ordered by name — the orchestrator's worker roster
    /// and pool list.
    pub fn enabled_roster(&self) -> Result<Vec<AgentRecord>, AgentsError> {
        Ok(self
            .list_agents()?
            .into_iter()
            .filter(|a| a.enabled)
            .collect())
    }

    /// Flip an agent's enabled flag (routing/roster opt-in-out).
    pub fn set_agent_enabled(&self, id: &str, enabled: bool) -> Result<(), AgentsError> {
        let conn = self.lock()?;
        let changed = conn
            .execute(
                "UPDATE agents SET enabled = ?1, updated_at = ?2 WHERE id = ?3",
                rusqlite::params![i64::from(enabled), Utc::now().to_rfc3339(), id],
            )
            .map_err(map_sqlite_error)?;
        if changed == 0 {
            return Err(AgentsError::NotFound(format!("agent {id}")));
        }
        Ok(())
    }

    // ------------------------------------------------------------- skills

    /// Insert a new skill (validates shape; id must be free).
    pub fn create_skill(&self, mut skill: SkillRecord) -> Result<SkillRecord, AgentsError> {
        skill.validate()?;
        let now = Utc::now();
        skill.created_at = now;
        skill.updated_at = now;
        let conn = self.lock()?;
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM skills WHERE id = ?1)",
                [&skill.id],
                |row| row.get::<_, i64>(0),
            )
            .map(|v| v != 0)
            .map_err(map_sqlite_error)?;
        if exists {
            return Err(AgentsError::Duplicate(format!("skill {}", skill.id)));
        }
        conn.execute(
            "INSERT INTO skills (id, name, description, body, builtin, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 0, ?5, ?5)",
            rusqlite::params![
                skill.id,
                skill.name,
                skill.description,
                skill.body,
                now.to_rfc3339()
            ],
        )
        .map_err(map_sqlite_error)?;
        Ok(skill)
    }

    /// Update a skill's editable fields (preserves builtin flag and
    /// created_at; agents referencing it pick the new body up on their next
    /// session — preambles are composed at spawn time, deliberately).
    pub fn update_skill(&self, skill: SkillRecord) -> Result<SkillRecord, AgentsError> {
        skill.validate()?;
        let conn = self.lock()?;
        let changed = conn
            .execute(
                "UPDATE skills SET name = ?1, description = ?2, body = ?3, updated_at = ?4
                 WHERE id = ?5",
                rusqlite::params![
                    skill.name,
                    skill.description,
                    skill.body,
                    Utc::now().to_rfc3339(),
                    skill.id
                ],
            )
            .map_err(map_sqlite_error)?;
        if changed == 0 {
            return Err(AgentsError::NotFound(format!("skill {}", skill.id)));
        }
        Ok(skill)
    }

    /// Delete a skill. Built-ins are refused; a skill still referenced by
    /// any agent is refused (assign-then-delete would silently change an
    /// agent's preamble).
    pub fn delete_skill(&self, id: &str) -> Result<(), AgentsError> {
        let conn = self.lock()?;
        let builtin = conn
            .query_row("SELECT builtin FROM skills WHERE id = ?1", [id], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|err| match err {
                rusqlite::Error::QueryReturnedNoRows => {
                    AgentsError::NotFound(format!("skill {id}"))
                }
                other => map_sqlite_error(other),
            })?;
        if builtin != 0 {
            return Err(AgentsError::BuiltinProtected(id.to_owned()));
        }
        let agents = list_agent_ids_holding_skill(&conn, id)?;
        if !agents.is_empty() {
            return Err(AgentsError::Validation(format!(
                "skill {id} is still assigned to agents {agents:?}; unassign it first"
            )));
        }
        conn.execute("DELETE FROM skills WHERE id = ?1", [id])
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    /// All skills, ordered by name (built-ins first on ties).
    pub fn list_skills(&self) -> Result<Vec<SkillRecord>, AgentsError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, name, description, body, builtin, created_at, updated_at \
                      FROM skills ORDER BY builtin DESC, name ASC",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map([], skill_from_row)
            .map_err(map_sqlite_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)?;
        Ok(rows)
    }

    /// Fetch one skill by id.
    pub fn get_skill(&self, id: &str) -> Result<Option<SkillRecord>, AgentsError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, name, description, body, builtin, created_at, updated_at \
                      FROM skills WHERE id = ?1",
            )
            .map_err(map_sqlite_error)?;
        let mut rows = stmt
            .query_map([id], skill_from_row)
            .map_err(map_sqlite_error)?;
        match rows.next() {
            Some(row) => Ok(Some(row.map_err(map_sqlite_error)?)),
            None => Ok(None),
        }
    }

    // ------------------------------------------------------------- seeds

    /// Install the built-in skills and agents **idempotently**:
    /// insert-only-if-absent, so an edited built-in survives reseeds and
    /// upgrades. Returns the number of rows inserted.
    /// Insert the shipped skill bodies straight into the table.
    ///
    /// Production does not use this: the daemon syncs the skill library on
    /// disk instead, so there is exactly one store. It exists for callers
    /// that have no library — unit tests, and anyone embedding the registry
    /// without the daemon around it.
    pub fn seed_builtin_skills(&self) -> Result<usize, AgentsError> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        let mut inserted = 0;
        for skill in builtin_skills() {
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM skills WHERE id = ?1)",
                    [&skill.id],
                    |row| row.get::<_, i64>(0),
                )
                .map(|v| v != 0)
                .map_err(map_sqlite_error)?;
            if !exists {
                tx.execute(
                    "INSERT INTO skills (id, name, description, body, builtin, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, 1, ?5, ?5)",
                    rusqlite::params![
                        skill.id,
                        skill.name,
                        skill.description,
                        skill.body,
                        skill.created_at.to_rfc3339()
                    ],
                )
                .map_err(map_sqlite_error)?;
                inserted += 1;
            }
        }
        tx.commit().map_err(map_sqlite_error)?;
        Ok(inserted)
    }

    pub fn seed_builtins(&self) -> Result<usize, AgentsError> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(map_sqlite_error)?;
        // Skills are NOT seeded here. There is one skill store — the library
        // on disk — and the daemon exports the OS's own methods into it and
        // imports them back before this runs. Seeding them into the table too
        // would recreate the second, divergent store this system deliberately
        // does not have. Callers with no library (tests, embedders) call
        // [`AgentRegistry::seed_builtin_skills`] first.
        let mut inserted = 0;
        for agent in builtin_agents() {
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM agents WHERE id = ?1)",
                    [&agent.id],
                    |row| row.get::<_, i64>(0),
                )
                .map(|v| v != 0)
                .map_err(map_sqlite_error)?;
            if !exists {
                insert_agent_tx(&tx, &agent)?;
                inserted += 1;
            }
        }
        tx.commit().map_err(map_sqlite_error)?;
        Ok(inserted)
    }

    /// One-time compatibility migration for installations seeded before
    /// the canonical `mastermind` skill replaced the shortened command
    /// manifest. Deliberately narrow: user-customized skill sets are left
    /// untouched.
    pub fn migrate_orchestrator_mastermind_skill(&self) -> Result<bool, AgentsError> {
        let Some(mut record) = self.get_agent("orchestrator")? else {
            return Ok(false);
        };
        if record.skills != ["mastermind-commands".to_owned()] {
            return Ok(false);
        }
        record.skills = vec!["mastermind".to_owned()];
        self.update_agent(record)?;
        Ok(true)
    }

    // ---------------------------------------------------------- preamble

    /// Render an agent's skills into the prompt preamble prepended to its
    /// session objective. Caveman and Ponytail are application-level
    /// invariants and are always first, independent of the record's editable
    /// skill list.
    ///
    /// Composed at spawn time (not stored), so editing an ordinary skill body
    /// changes the next session without touching an agent row. Global skills
    /// are compiled in so a registry edit cannot weaken either invariant.
    pub fn preamble_for(&self, record: &AgentRecord) -> Result<String, AgentsError> {
        let mut sections = global_skill_sections();
        if !record.skills.is_empty() {
            let conn = self.lock()?;
            for id in &record.skills {
                // Global copies above are authoritative and must never be
                // duplicated by legacy or user-edited records.
                if is_global_skill(id) {
                    continue;
                }
                let skill = skill_by_id(&conn, id)?
                    .ok_or_else(|| AgentsError::NotFound(format!("skill {id}")))?;
                sections.push(format!(
                    "## Skill: {} ({})\n\n{}",
                    skill.name, skill.id, skill.body
                ));
            }
        }
        Ok(render_skill_preamble(&sections))
    }
}

// ------------------------------------------------------------- row plumbing

const AGENT_COLUMNS: &str = "id, name, description, adapter_id, model, effort, mode, \
                            skills, tool_allowlist, tool_denylist, timeout_secs, \
                            builtin, enabled, created_at, updated_at";

fn agent_from_row(row: &Row<'_>) -> rusqlite::Result<AgentRecord> {
    let skills_json: String = row.get(7)?;
    let allow_json: String = row.get(8)?;
    let deny_json: String = row.get(9)?;
    Ok(AgentRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        adapter_id: row.get(3)?,
        model: row.get(4)?,
        effort: row
            .get::<_, Option<String>>(5)?
            .and_then(|e| AgentEffort::parse(&e)),
        mode: AgentMode::parse(&row.get::<_, String>(6)?).unwrap_or(crate::record::AgentMode::Plan),
        skills: decode_json_list(&skills_json),
        tool_allowlist: decode_json_list(&allow_json),
        tool_denylist: decode_json_list(&deny_json),
        timeout_secs: row.get::<_, i64>(10)?.max(0) as u64,
        builtin: row.get::<_, i64>(11)? != 0,
        enabled: row.get::<_, i64>(12)? != 0,
        created_at: parse_rfc3339(&row.get::<_, String>(13)?),
        updated_at: parse_rfc3339(&row.get::<_, String>(14)?),
    })
}

fn skill_from_row(row: &Row<'_>) -> rusqlite::Result<SkillRecord> {
    Ok(SkillRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        body: row.get(3)?,
        builtin: row.get::<_, i64>(4)? != 0,
        created_at: parse_rfc3339(&row.get::<_, String>(5)?),
        updated_at: parse_rfc3339(&row.get::<_, String>(6)?),
    })
}

fn insert_agent_tx(
    tx: &rusqlite::Transaction<'_>,
    record: &AgentRecord,
) -> Result<(), AgentsError> {
    tx.execute(
        "INSERT OR REPLACE INTO agents (
            id, name, description, adapter_id, model, effort, mode, skills,
            tool_allowlist, tool_denylist, timeout_secs, builtin, enabled,
            created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        rusqlite::params![
            record.id,
            record.name,
            record.description,
            record.adapter_id,
            record.model,
            record.effort.map(|e| e.as_str()),
            record.mode.as_str(),
            encode_json_list(&record.skills),
            encode_json_list(&record.tool_allowlist),
            encode_json_list(&record.tool_denylist),
            record.timeout_secs as i64,
            i64::from(record.builtin),
            i64::from(record.enabled),
            record.created_at.to_rfc3339(),
            record.updated_at.to_rfc3339(),
        ],
    )
    .map_err(map_sqlite_error)?;
    Ok(())
}

fn agent_exists(conn: &Connection, id: &str) -> Result<bool, AgentsError> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM agents WHERE id = ?1)",
            [id],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v != 0)
        .map_err(map_sqlite_error)?;
    Ok(exists)
}

fn agent_by_id(conn: &Connection, id: &str) -> Result<Option<AgentRecord>, AgentsError> {
    let sql = format!("SELECT {AGENT_COLUMNS} FROM agents WHERE id = ?1");
    let mut stmt = conn.prepare(&sql).map_err(map_sqlite_error)?;
    let mut rows = stmt
        .query_map([id], agent_from_row)
        .map_err(map_sqlite_error)?;
    match rows.next() {
        Some(row) => Ok(Some(row.map_err(map_sqlite_error)?)),
        None => Ok(None),
    }
}

fn skill_by_id(conn: &Connection, id: &str) -> Result<Option<SkillRecord>, AgentsError> {
    let mut stmt = conn
        .prepare(
            "SELECT id, name, description, body, builtin, created_at, updated_at \
                  FROM skills WHERE id = ?1",
        )
        .map_err(map_sqlite_error)?;
    let mut rows = stmt
        .query_map([id], skill_from_row)
        .map_err(map_sqlite_error)?;
    match rows.next() {
        Some(row) => Ok(Some(row.map_err(map_sqlite_error)?)),
        None => Ok(None),
    }
}

/// Every agent id whose skills array contains `skill_id` (scan is fine at
/// registry scale; the JSON column is not indexable).
fn list_agent_ids_holding_skill(
    conn: &Connection,
    skill_id: &str,
) -> Result<Vec<String>, AgentsError> {
    let mut stmt = conn
        .prepare("SELECT id, skills FROM agents")
        .map_err(map_sqlite_error)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(map_sqlite_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite_error)?;
    Ok(rows
        .into_iter()
        .filter(|(_, skills_json)| decode_json_list(skills_json).iter().any(|s| s == skill_id))
        .map(|(id, _)| id)
        .collect())
}

/// Cross-row validation: every referenced skill id must exist. Unknown
/// skill ids would silently vanish from the preamble otherwise.
fn ensure_skills_exist(conn: &Connection, skills: &[String]) -> Result<(), AgentsError> {
    for id in skills {
        if skill_by_id(conn, id)?.is_none() {
            return Err(AgentsError::NotFound(format!(
                "skill {id} (assign existing skills only)"
            )));
        }
    }
    Ok(())
}

fn encode_json_list(list: &[String]) -> String {
    serde_json::to_string(list).unwrap_or_else(|_| "[]".to_owned())
}

fn decode_json_list(json: &str) -> Vec<String> {
    serde_json::from_str(json).unwrap_or_default()
}

fn parse_rfc3339(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

// ------------------------------------------------------------- sqlite canon

fn ensure_wal(conn: &Connection) -> Result<(), AgentsError> {
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(map_sqlite_error)?;
    if mode.eq_ignore_ascii_case("wal") {
        return Ok(());
    }
    for attempt in 0..WAL_SET_ATTEMPTS {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_millis(WAL_SET_RETRY_DELAY_MS));
        }
        match conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0)) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => return Ok(()),
            Ok(mode) => {
                return Err(AgentsError::Core(CoreError::Serialization(format!(
                    "sqlite: journal_mode=WAL did not take effect (reported {mode})"
                ))));
            }
            Err(err) if is_sqlite_busy(&err) => continue,
            Err(err) => return Err(map_sqlite_error(err)),
        }
    }
    Err(AgentsError::Core(CoreError::SqliteBusy))
}

fn migrate(conn: &mut Connection) -> Result<(), AgentsError> {
    let current: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(map_sqlite_error)?;
    if current > SCHEMA_VERSION {
        return Err(AgentsError::Core(CoreError::Serialization(format!(
            "sqlite: registry user_version {current} is newer than this build supports \
             (schema version {SCHEMA_VERSION}); upgrade first"
        ))));
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

fn is_sqlite_busy(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(ffi_err, _) => {
            ffi_err.code == rusqlite::ErrorCode::DatabaseBusy
                || (ffi_err.extended_code & 0xff) == rusqlite::ffi::SQLITE_BUSY
        }
        _ => false,
    }
}

fn map_sqlite_error(err: rusqlite::Error) -> AgentsError {
    if is_sqlite_busy(&err) {
        AgentsError::Core(CoreError::SqliteBusy)
    } else {
        AgentsError::Core(CoreError::Serialization(format!("sqlite: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{AgentMode, ADAPTER_ANTIGRAVITY_AGY};

    fn registry_with_seeds() -> AgentRegistry {
        let registry = AgentRegistry::open_in_memory().expect("open registry");
        registry.seed_builtin_skills().expect("seed skills");
        registry.seed_builtins().expect("seed");
        registry
    }

    fn custom_agent(id: &str) -> AgentRecord {
        AgentRecord {
            id: id.to_owned(),
            name: "Custom".to_owned(),
            description: "user-created".to_owned(),
            adapter_id: "mock".to_owned(),
            model: Some("mock-model-1".to_owned()),
            effort: None,
            mode: AgentMode::AcceptEdits,
            skills: vec!["tech-research".to_owned()],
            tool_allowlist: vec![],
            tool_denylist: vec!["WebFetch".to_owned()],
            timeout_secs: 300,
            builtin: false,
            enabled: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn crud_round_trip() {
        let registry = registry_with_seeds();
        let created = registry
            .create_agent(custom_agent("my-worker"))
            .expect("create");
        assert!(!created.builtin);

        let fetched = registry
            .get_agent("my-worker")
            .expect("read")
            .expect("present");
        assert_eq!(fetched.name, "Custom");
        assert_eq!(fetched.skills, vec!["tech-research".to_owned()]);
        assert_eq!(fetched.tool_denylist, vec!["WebFetch".to_owned()]);
        assert_eq!(fetched.mode, AgentMode::AcceptEdits);

        let mut edited = fetched.clone();
        edited.name = "Renamed".to_owned();
        edited.skills = vec![];
        let updated = registry.update_agent(edited).expect("update");
        assert_eq!(updated.name, "Renamed");
        assert!(updated.skills.is_empty());
        assert_eq!(
            updated.created_at, fetched.created_at,
            "created_at preserved"
        );

        registry.delete_agent("my-worker").expect("delete");
        assert!(registry.get_agent("my-worker").expect("read").is_none());
    }

    #[test]
    fn duplicate_ids_are_refused() {
        let registry = registry_with_seeds();
        registry
            .create_agent(custom_agent("dup"))
            .expect("first create");
        assert!(matches!(
            registry.create_agent(custom_agent("dup")),
            Err(AgentsError::Duplicate(_))
        ));
    }

    #[test]
    fn update_of_missing_agent_is_not_found() {
        let registry = registry_with_seeds();
        assert!(matches!(
            registry.update_agent(custom_agent("ghost")),
            Err(AgentsError::NotFound(_))
        ));
    }

    #[test]
    fn unknown_skill_reference_is_refused() {
        let registry = registry_with_seeds();
        let mut agent = custom_agent("bad-skills");
        agent.skills = vec!["does-not-exist".to_owned()];
        assert!(matches!(
            registry.create_agent(agent),
            Err(AgentsError::NotFound(_))
        ));
    }

    #[test]
    fn builtins_cannot_be_deleted_but_can_be_edited() {
        let registry = registry_with_seeds();
        assert!(matches!(
            registry.delete_agent("researcher"),
            Err(AgentsError::BuiltinProtected(_))
        ));
        let mut edited = registry.get_agent("researcher").expect("read").unwrap();
        edited.description = "edited by user".to_owned();
        let updated = registry.update_agent(edited).expect("builtin editable");
        assert!(updated.builtin, "builtin flag is preserved on update");
        assert_eq!(updated.description, "edited by user");
    }

    #[test]
    fn builtin_flag_cannot_be_forged_on_update() {
        let registry = registry_with_seeds();
        let created = registry
            .create_agent(custom_agent("wannabe"))
            .expect("create");
        let mut forged = created.clone();
        forged.builtin = true;
        let updated = registry.update_agent(forged).expect("update");
        assert!(!updated.builtin, "user agents stay user agents");
    }

    #[test]
    fn seeding_is_idempotent_and_preserves_edits() {
        let registry = registry_with_seeds();
        // Reseed inserts nothing the second time.
        registry.seed_builtin_skills().expect("seed skills");
        assert_eq!(registry.seed_builtins().expect("reseed"), 0);

        // An edited built-in keeps its edit across a reseed.
        let mut edited = registry.get_agent("orchestrator").expect("read").unwrap();
        edited.description = "my tuned orchestrator".to_owned();
        registry.update_agent(edited).expect("edit");
        registry.seed_builtin_skills().expect("seed skills");
        registry.seed_builtins().expect("reseed");
        let after = registry.get_agent("orchestrator").expect("read").unwrap();
        assert_eq!(after.description, "my tuned orchestrator");
    }

    #[test]
    fn resolve_respects_enabled_flag() {
        let registry = registry_with_seeds();
        assert!(registry.resolve("researcher").expect("resolve").is_some());
        registry
            .set_agent_enabled("researcher", false)
            .expect("disable");
        assert!(registry.resolve("researcher").expect("resolve").is_none());
        assert!(
            registry.get_agent("researcher").expect("read").is_some(),
            "disabled agents stay on disk"
        );
        let roster = registry.enabled_roster().expect("roster");
        assert!(roster.iter().all(|a| a.enabled));
        assert!(!roster.iter().any(|a| a.id == "researcher"));
    }

    #[test]
    fn preamble_renders_assigned_skill_bodies() {
        let registry = registry_with_seeds();
        let researcher = registry.get_agent("researcher").expect("read").unwrap();
        let preamble = registry.preamble_for(&researcher).expect("preamble");
        assert!(preamble.starts_with("# Assigned skills"));
        assert!(preamble.contains("/caveman"));
        assert!(preamble.contains("Technical substance exact"));
        assert!(preamble.contains("ACTIVE EVERY RESPONSE"));
        assert!(preamble.contains("Global skill: Ponytail"));
        assert!(preamble.contains("Standard library? Use it."));
        assert!(preamble.contains("## Skill: Tech Research (tech-research)"));
        assert!(preamble.contains("Bottom line"), "skill body present");
        assert!(preamble.ends_with("---\n\n"), "separator before objective");

        let orchestrator = registry.get_agent("orchestrator").expect("read").unwrap();
        let preamble = registry.preamble_for(&orchestrator).expect("preamble");
        assert!(preamble.contains("`create_task`"));
        assert!(preamble.contains("`close_goal`"));
    }

    #[test]
    fn global_skills_are_injected_without_assigned_skills() {
        let registry = registry_with_seeds();
        let mut agent = custom_agent("bare");
        agent.skills = vec![];
        let created = registry.create_agent(agent).expect("create");
        let preamble = registry.preamble_for(&created).expect("preamble");
        assert!(preamble.contains("Global skill: /caveman"));
        assert!(preamble.contains("Auto-Clarity"));
        assert!(preamble.contains("Global skill: Ponytail"));
        assert!(preamble.contains("minimum correct code"));
    }

    #[test]
    fn explicitly_listing_global_skills_does_not_duplicate_them() {
        let registry = registry_with_seeds();
        let mut agent = custom_agent("explicit-globals");
        agent.skills = vec![
            crate::seeds::CAVEMAN_SKILL_ID.to_owned(),
            crate::seeds::PONYTAIL_SKILL_ID.to_owned(),
        ];
        let created = registry.create_agent(agent).expect("create");
        let preamble = registry.preamble_for(&created).expect("preamble");
        assert_eq!(preamble.matches("Global skill: /caveman").count(), 1);
        assert_eq!(preamble.matches("Global skill: Ponytail").count(), 1);
    }

    #[test]
    fn global_skills_precede_role_skills_for_representative_agent_classes() {
        let registry = registry_with_seeds();
        for id in [
            "orchestrator",
            "spec-writer",
            "nextjs-dev",
            "general-worker-luna",
            "debugger",
            "debugger-sol-escalation",
            "code-reviewer",
        ] {
            let agent = registry.get_agent(id).expect("read").unwrap();
            let preamble = registry.preamble_for(&agent).expect("preamble");
            let caveman = preamble.find("Global skill: /caveman").unwrap();
            let ponytail = preamble.find("Global skill: Ponytail").unwrap();
            assert!(caveman < ponytail, "{id}: Caveman must precede Ponytail");
            if let Some(role_skill) = preamble.find("## Skill:") {
                assert!(
                    ponytail < role_skill,
                    "{id}: global skills must precede role skills"
                );
            }
        }
    }

    #[test]
    fn skill_crud_and_reference_guard() {
        let registry = registry_with_seeds();
        let skill = SkillRecord {
            id: "my-skill".to_owned(),
            name: "My Skill".to_owned(),
            description: "custom".to_owned(),
            body: "# Mine\n\nBe excellent.".to_owned(),
            builtin: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        registry.create_skill(skill.clone()).expect("create skill");
        assert!(matches!(
            registry.create_skill(skill.clone()),
            Err(AgentsError::Duplicate(_))
        ));

        // Referenced skill cannot be deleted.
        let mut holder = custom_agent("holder");
        holder.skills = vec!["my-skill".to_owned()];
        registry.create_agent(holder).expect("create holder");
        assert!(matches!(
            registry.delete_skill("my-skill"),
            Err(AgentsError::Validation(_))
        ));

        // Unreferenced, non-builtin skill deletes fine; builtins do not.
        let mut holder = registry.get_agent("holder").expect("read").unwrap();
        holder.skills = vec![];
        registry.update_agent(holder).expect("unassign");
        registry
            .delete_skill("my-skill")
            .expect("delete after unassign");
        assert!(matches!(
            registry.delete_skill("tech-research"),
            Err(AgentsError::BuiltinProtected(_))
        ));
    }

    #[test]
    fn records_survive_reopen_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("agents.db");
        {
            let registry = AgentRegistry::open(&path).expect("open");
            registry.seed_builtin_skills().expect("seed skills");
            registry.seed_builtins().expect("seed");
            registry
                .create_agent(custom_agent("persistent"))
                .expect("create");
        }
        let reopened = AgentRegistry::open(&path).expect("reopen");
        assert!(
            reopened.get_agent("persistent").expect("read").is_some(),
            "rows survive reopen"
        );
        reopened.seed_builtin_skills().expect("seed skills");
        assert_eq!(reopened.seed_builtins().expect("reseed"), 0);
    }

    #[test]
    fn camel_case_wire_round_trip_through_json() {
        let registry = registry_with_seeds();
        let researcher = registry.get_agent("researcher").expect("read").unwrap();
        let wire = serde_json::to_value(&researcher).expect("serialize");
        assert_eq!(wire["adapterId"], serde_json::json!("antigravity-agy"));
        assert_eq!(wire["model"], serde_json::json!("gemini-3.1-pro-high"));
        assert_eq!(wire["timeoutSecs"], serde_json::json!(900));
        let back: AgentRecord = serde_json::from_value(wire).expect("deserialize");
        assert_eq!(back, researcher);
    }

    /// The seeded creator runs sonnet via agy; the researcher runs
    /// gemini-3.1-pro-high via agy (user decisions 2026-08-22; exact catalog slug).
    #[test]
    fn seeds_match_the_user_decisions() {
        let registry = registry_with_seeds();
        let creator = registry.get_agent("agent-creator").expect("read").unwrap();
        assert_eq!(creator.adapter_id, ADAPTER_ANTIGRAVITY_AGY);
        assert_eq!(creator.model.as_deref(), Some("claude-sonnet-4-6"));
        assert_eq!(creator.mode, AgentMode::Plan);

        let researcher = registry.get_agent("researcher").expect("read").unwrap();
        assert_eq!(researcher.adapter_id, ADAPTER_ANTIGRAVITY_AGY);
        assert_eq!(researcher.model.as_deref(), Some("gemini-3.1-pro-high"));
        assert_eq!(researcher.mode, AgentMode::Plan);
    }

    /// The planning agent (mastermind Phases 1-4) runs on the top-tier
    /// model and must be able to write the documents it produces.
    #[test]
    fn spec_writer_plans_on_opus_and_can_write_docs() {
        let registry = registry_with_seeds();
        let spec = registry.get_agent("spec-writer").expect("read").unwrap();
        assert_eq!(spec.adapter_id, "claude-code");
        assert_eq!(spec.model.as_deref(), Some("claude-opus-5"));
        assert_eq!(spec.mode, AgentMode::AcceptEdits);
        assert!(spec.skills.contains(&"product-spec".to_owned()));

        let preamble = registry.preamble_for(&spec).expect("preamble");
        for phase in [
            "DISCOVERY.md",
            "PRD.md",
            "docs/features/",
            "IMPLEMENTATION_PLAN.md",
        ] {
            assert!(preamble.contains(phase), "spec skill must cover {phase}");
        }
    }

    #[test]
    fn requested_worker_pool_writes_on_claude_sonnet_5_and_holds_the_graph_rule() {
        let registry = registry_with_seeds();
        for id in [
            "database-engineer",
            "devops-deployer",
            "docs-writer",
            "flutter-dev",
            "nextjs-dev",
            "nodejs-dev",
            "python-specialist",
            "react-dev",
            "react-native-dev",
            "rust-specialist",
            "test-engineer",
            "typescript-specialist",
        ] {
            let coder = registry.get_agent(id).expect("read").unwrap();
            assert_eq!(coder.adapter_id, "claude-code", "{id} provider");
            assert_eq!(
                coder.model.as_deref(),
                Some("claude-sonnet-5"),
                "{id} model"
            );
            assert_eq!(coder.mode, AgentMode::AcceptEdits, "{id} must write files");
            assert!(
                coder.skills.contains(&"code-graph-discipline".to_owned()),
                "{id} must hold the strict graph rule"
            );
            assert!(coder.skills.contains(&"decision-protocol".to_owned()));
        }
    }

    /// The design phase is a proper agent (user request 2026-08-22,
    /// distilled from `~/.claude/agents/ui-designer.md` + mastermind
    /// Phases 6–7): the only built-in with write access, holding the
    /// product-design skill, on Gemini 3.1 Pro High via agy.
    #[test]
    fn ui_designer_is_the_write_capable_design_phase_agent() {
        let registry = registry_with_seeds();
        let designer = registry.get_agent("ui-designer").expect("read").unwrap();
        assert_eq!(designer.adapter_id, ADAPTER_ANTIGRAVITY_AGY);
        assert_eq!(designer.model.as_deref(), Some("gemini-3.1-pro-high"));
        assert_eq!(
            designer.mode,
            AgentMode::AcceptEdits,
            "designers ship artifacts"
        );
        assert_eq!(
            designer.skills,
            vec![
                "product-design".to_owned(),
                "code-graph-discipline".to_owned(),
                "decision-protocol".to_owned()
            ]
        );
        assert!(designer.timeout_secs >= 1800, "mockup runs are long");

        // Every other built-in stays read-only.
        for id in ["orchestrator", "agent-creator", "researcher"] {
            let record = registry.get_agent(id).expect("read").unwrap();
            assert_eq!(record.mode, AgentMode::Plan, "{id} stays plan mode");
        }

        // The skill body carries the load-bearing method rules.
        let skill = registry.get_skill("product-design").expect("read").unwrap();
        for rule in [
            "DESIGN.md",
            "tokens.css",
            "var(--*)",
            "INDEX.md",
            "4.5:1",
            "Brand register",
            "Product register",
            "Absolute bans",
            "AI slop test",
        ] {
            assert!(
                skill.body.contains(rule),
                "product-design must cover {rule}"
            );
        }
    }

    #[test]
    fn reviewers_and_debugger_match_the_requested_models_and_keep_their_modes() {
        let registry = registry_with_seeds();

        let reviewer = registry.get_agent("code-reviewer").expect("read").unwrap();
        assert_eq!(reviewer.adapter_id, "codex");
        assert_eq!(reviewer.model.as_deref(), Some("gpt-5.6-terra"));
        assert_eq!(reviewer.mode, AgentMode::Plan);

        let security = registry
            .get_agent("security-reviewer")
            .expect("read")
            .unwrap();
        assert_eq!(security.adapter_id, "claude-code");
        assert_eq!(security.model.as_deref(), Some("claude-sonnet-5"));
        assert_eq!(security.mode, AgentMode::Plan);

        let debugger = registry.get_agent("debugger").expect("read").unwrap();
        assert_eq!(debugger.adapter_id, "codex");
        assert_eq!(debugger.model.as_deref(), Some("gpt-5.6-terra"));
        assert_eq!(debugger.mode, AgentMode::AcceptEdits);

        let sol = registry
            .get_agent("debugger-sol-escalation")
            .expect("read")
            .unwrap();
        assert_eq!(sol.adapter_id, "codex");
        assert_eq!(sol.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(sol.mode, AgentMode::AcceptEdits);
        assert!(sol.description.contains("Escalation-only"));
        for denied in ["commit", "merge", "rebase", "push", "reset"] {
            assert!(
                sol.tool_denylist.iter().any(|rule| rule.contains(denied)),
                "Sol escalation must deny git {denied}"
            );
        }
    }
}
