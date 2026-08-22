//! Task priority (PRD §18.1 Task entity: `priority`).

use serde::{Deserialize, Serialize};

/// Task priority, `P0` (most urgent) through `P3` (least urgent).
///
/// `Ord` is derived in declaration order, so `Priority::P0 < Priority::P3`:
/// ordering by priority ascending places the most urgent work first, which
/// is what a scheduler's ready-queue sort wants. Wire form is the lowercase
/// name (`"p0"`..`"p3"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Priority {
    /// Drop-everything critical.
    #[serde(rename = "p0")]
    P0,
    /// High urgency.
    #[serde(rename = "p1")]
    P1,
    /// Normal urgency.
    #[serde(rename = "p2")]
    P2,
    /// Background / best-effort.
    #[serde(rename = "p3")]
    P3,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn p0_is_most_urgent_and_sorts_first() {
        assert!(Priority::P0 < Priority::P1);
        assert!(Priority::P1 < Priority::P2);
        assert!(Priority::P2 < Priority::P3);
        assert_eq!(Priority::P0.min(Priority::P2), Priority::P0);

        let mut queue = vec![Priority::P3, Priority::P1, Priority::P0, Priority::P2];
        queue.sort();
        assert_eq!(
            queue,
            vec![Priority::P0, Priority::P1, Priority::P2, Priority::P3]
        );
    }

    #[test]
    fn priority_serializes_as_lowercase_names() {
        for (priority, wire) in [
            (Priority::P0, "p0"),
            (Priority::P1, "p1"),
            (Priority::P2, "p2"),
            (Priority::P3, "p3"),
        ] {
            assert_eq!(serde_json::to_value(priority).unwrap(), json!(wire));
            let parsed: Priority = serde_json::from_value(json!(wire)).unwrap();
            assert_eq!(parsed, priority);
        }
    }
}
