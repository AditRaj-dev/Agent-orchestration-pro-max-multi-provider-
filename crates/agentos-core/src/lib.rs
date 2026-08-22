//! # agentos-core
//!
//! Shared canon types for the Agent Engineering OS workspace: the append-only
//! [`Event`] journal model (PRD §18, F-00 §3 event rules), the [`TaskState`]
//! lifecycle machine (PRD §6.2), [`Priority`], and the [`CoreError`]
//! taxonomy.
//!
//! These types are canon: dependent crates (`agentos-daemon`,
//! `agentos-workflow`, `agentos-adapters`, `agentos-git`) must use them
//! rather than redefining local equivalents, so the event journal and the
//! task store stay mutually intelligible across every component.

#![forbid(unsafe_code)]

pub mod error;
pub mod event;
pub mod priority;
pub mod task;

pub use error::CoreError;
pub use event::{Event, EventType, EVENT_SCHEMA_VERSION};
pub use priority::Priority;
pub use task::TaskState;
