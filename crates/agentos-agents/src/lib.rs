//! F-13: the dynamic agent registry — agents as *data*, not code.
//!
//! Every worker the OS can spawn is a row here: which provider adapter runs
//! it, which model, which skills, which tool permissions. The supervisor
//! resolves `agent_role` through the registry before falling back to the
//! static routing table; the orchestrator's worker roster and pool list come
//! from the enabled agents; the desktop Agents screen edits the same rows
//! over the daemon's `registry.*` WS methods.
//!
//! ## Skills are prompt preambles
//!
//! A skill is a named markdown document owned by this registry (the F-00
//! isolation canon keeps `~/.agents` out of worker sessions, so skills are
//! *injected*, not mounted). [`AgentRegistry::preamble_for`] renders an
//! agent's assigned skills into one block that the caller prepends to the
//! session objective. This is deliberately provider-agnostic: the F-03
//! claude adapter delivers the objective on stdin and the F-05 agy adapter
//! delivers it as the equals-form `--print=<objective>`, so both carry the
//! preamble without any adapter change.
//!
//! ## Mutations are registry rows, journal events are the audit trail
//!
//! The `agents`/`skills` tables are ordinary mutable tables (versioned
//! migrations, F-01 canon open helper — a private copy like every other
//! crate). The *audit* story lives one layer up: the daemon journals
//! `agent.created` / `agent.updated` / `agent.deleted` events for every
//! mutation it applies, so the append-only journal stays the record of who
//! changed what when, and this crate stays free of journal concerns.
//!
//! ## Built-ins
//!
//! [`seed_builtins`](crate::AgentRegistry::seed_builtins) installs the
//! mastermind trio idempotently (insert-if-absent, never overwriting an
//! edited row): the `orchestrator` (claude-opus-5, mastermind-commands
//! skill), the `agent-creator` (agy → claude-sonnet-4-6, interviews the
//! user and drafts agent definitions), and the `researcher` (agy →
//! gemini-3.1-pro-high, topic and tech-stack research). Built-ins can be edited
//! but not deleted.

pub mod error;
pub mod record;
pub mod registry;
pub mod seeds;
pub mod skill;

pub use error::AgentsError;
pub use record::{AgentEffort, AgentMode, AgentRecord, KNOWN_ADAPTERS};
pub use registry::AgentRegistry;
pub use seeds::{builtin_agents, builtin_skills};
pub use skill::SkillRecord;
