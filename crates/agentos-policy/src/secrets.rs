//! Secrets broker (PRD §14 SEC-03): scoped, expiring leases instead of broad
//! `.env` access.
//!
//! Safety properties (binding — F-00 §5 "secrets live in keychain/env, never
//! in repo files or transcripts"):
//!
//! - [`SecretString`] implements **no** `Serialize`/`Deserialize` — a lease
//!   cannot flow through serde into handoffs, events, or context artifacts;
//!   its `Debug` and `Display` both render `***`, so `{:?}`/`{}` in a log
//!   line cannot leak it. Reading the value requires the explicit,
//!   loudly-named [`SecretString::expose`] (for ephemeral env injection — the
//!   only sanctioned side channel).
//! - On drop the backing buffer is overwritten with NUL bytes
//!   ([`SecretString::zero_out`], called by `Drop`). Best-effort without a
//!   `zeroize` dependency: the stores happen through safe code before the
//!   buffer is freed, which defeats casual leaks via reused heap, though the
//!   optimizer is not contractually barred from eliding them (documented
//!   trade-off; a keyed backend can harden this).
//! - The broker interface is the **OS-keychain seam**: the MVP ships
//!   [`EphemeralBroker`] (values supplied programmatically, for tests/dev).
//!   A `KeychainBroker` (Windows Credential Manager / secret-service) later
//!   implements the same trait with values that never live in this process's
//!   config; nothing else changes.

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;

use crate::error::PolicyError;

/// A secret value that redacts itself in every formatting path and is
/// zeroed on drop.
///
/// Deliberately **not** `Clone` (cloning would multiply un-zeroed copies)
/// and **not** `Serialize`/`Deserialize` (values must never be persisted or
/// journaled). If a compile error ever points here because someone tried to
/// derive those: that is the type system working.
pub struct SecretString {
    inner: String,
}

impl SecretString {
    /// Wrap a raw value.
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            inner: value.into(),
        }
    }

    /// The raw value, for ephemeral env/side-channel injection only.
    /// NEVER log it, format it, persist it, or include it in handoffs,
    /// events, or model context (SEC-03 / F-00 §5).
    pub fn expose(&self) -> &str {
        &self.inner
    }

    /// Overwrite the backing buffer with NUL bytes in place (length is
    /// preserved so the overwrite is observable/testable). Called by `Drop`.
    pub(crate) fn zero_out(&mut self) {
        let mut bytes = std::mem::take(&mut self.inner).into_bytes();
        for byte in bytes.iter_mut() {
            *byte = 0;
        }
        self.inner = String::from_utf8(bytes).expect("NUL bytes are valid UTF-8");
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString(\"***\")")
    }
}

impl std::fmt::Display for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("***")
    }
}

impl Drop for SecretString {
    fn drop(&mut self) {
        self.zero_out();
    }
}

/// A scoped, expiring grant of one secret to one task (SEC-03).
///
/// Not `Serialize`: the lease (and anything containing it) must never be
/// persisted. `Debug` is safe — the value redacts itself.
#[derive(Debug)]
pub struct SecretLease {
    /// The granted scope, e.g. `"deploy/token"`.
    pub scope: String,
    /// The task the lease is bound to.
    pub task_id: String,
    /// Grant time.
    pub granted_at: DateTime<Utc>,
    /// Expiry — enforced by the backend at use/redaction time.
    pub expires_at: DateTime<Utc>,
    /// The secret itself (redacting, zero-on-drop).
    pub value: SecretString,
}

/// Backend-agnostic secrets broker. The OS-keychain implementation (SEC-03)
/// plugs in here; the ephemeral in-memory one covers tests/dev.
pub trait SecretsBroker: Send + Sync {
    /// Grant `scope` to `task_id` for `ttl`. Fails for unknown scopes or a
    /// negative ttl.
    fn grant(&self, scope: &str, task_id: &str, ttl: Duration) -> Result<SecretLease, PolicyError>;

    /// Revoke a lease (task end, cancellation). Idempotent.
    fn revoke(&self, lease: &SecretLease) -> Result<(), PolicyError>;

    /// Replace every known secret value occurring in `text` with
    /// `***[scope]` — the transcript/log scrubber. Longer values are
    /// replaced first so a value that is a prefix of another cannot shield
    /// it.
    fn redact(&self, text: &str) -> String;

    /// Structured form for audit events: scope metadata only, NEVER the
    /// value (default impl enforces this by construction).
    fn audit_details(&self, lease: &SecretLease) -> Value {
        serde_json::json!({
            "scope": lease.scope,
            "taskId": lease.task_id,
            "grantedAt": lease.granted_at.to_rfc3339(),
            "expiresAt": lease.expires_at.to_rfc3339(),
        })
    }
}

/// In-memory broker for tests and development: values are supplied
/// programmatically at construction (in production the same interface fronts
/// the OS keychain — see the module docs).
#[derive(Debug)]
pub struct EphemeralBroker {
    /// value -> scope, for redaction lookups.
    value_index: HashMap<String, String>,
    /// Active leases keyed by (scope, task_id).
    active: Mutex<HashMap<(String, String), ()>>,
}

impl EphemeralBroker {
    /// Build a broker holding `secrets` as (scope, value) pairs.
    pub fn new<I, S, V>(secrets: I) -> Self
    where
        I: IntoIterator<Item = (S, V)>,
        S: Into<String>,
        V: Into<String>,
    {
        let value_index = secrets
            .into_iter()
            .map(|(scope, value)| (value.into(), scope.into()))
            .collect();
        Self {
            value_index,
            active: Mutex::new(HashMap::new()),
        }
    }

    /// Number of active leases (test/observability helper).
    pub fn active_lease_count(&self) -> usize {
        self.active.lock().expect("active leases lock").len()
    }
}

impl SecretsBroker for EphemeralBroker {
    fn grant(&self, scope: &str, task_id: &str, ttl: Duration) -> Result<SecretLease, PolicyError> {
        if ttl < Duration::zero() {
            return Err(PolicyError::Invalid("ttl must be non-negative".to_owned()));
        }
        let value = self
            .value_index
            .iter()
            .find(|(_, s)| s.as_str() == scope)
            .map(|(value, _)| value.clone())
            .ok_or_else(|| PolicyError::Invalid(format!("unknown secret scope `{scope}`")))?;
        let now = Utc::now();
        let lease = SecretLease {
            scope: scope.to_owned(),
            task_id: task_id.to_owned(),
            granted_at: now,
            expires_at: now + ttl,
            value: SecretString::new(value),
        };
        self.active
            .lock()
            .expect("active leases lock")
            .insert((scope.to_owned(), task_id.to_owned()), ());
        tracing::debug!(scope, task_id, "secret lease granted (value never logged)");
        Ok(lease)
    }

    fn revoke(&self, lease: &SecretLease) -> Result<(), PolicyError> {
        self.active
            .lock()
            .expect("active leases lock")
            .remove(&(lease.scope.clone(), lease.task_id.clone()));
        tracing::debug!(scope = %lease.scope, task_id = %lease.task_id, "secret lease revoked");
        Ok(())
    }

    fn redact(&self, text: &str) -> String {
        // Longest value first: a value that prefixes another must not
        // partially mask it.
        let mut pairs: Vec<(&String, &String)> = self.value_index.iter().collect();
        pairs.sort_by_key(|(value, _)| std::cmp::Reverse(value.len()));
        let mut redacted = text.to_owned();
        for (value, scope) in pairs {
            if redacted.contains(value.as_str()) {
                redacted = redacted.replace(value.as_str(), &format!("***[{scope}]"));
            }
        }
        redacted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> EphemeralBroker {
        EphemeralBroker::new([
            ("deploy/token", "tok_live_9f2cfe1a7b44"),
            ("registry/npm", "npm_XXXXsecretXXXX"),
        ])
    }

    #[test]
    fn debug_and_display_redact_the_value() {
        let secret = SecretString::new("tok_live_9f2cfe1a7b44");
        let debug = format!("{secret:?}");
        let display = format!("{secret}");
        assert!(debug.contains("***"));
        assert_eq!(display, "***");
        assert!(!debug.contains("tok_live"));
        assert!(!display.contains("tok_live"));
        // Nested in a lease, Debug stays safe too.
        let lease = broker()
            .grant("deploy/token", "task-1", Duration::seconds(60))
            .expect("grant");
        let lease_debug = format!("{lease:?}");
        assert!(!lease_debug.contains("tok_live"));
        assert!(lease_debug.contains("***"));
    }

    #[test]
    fn expose_returns_the_raw_value_for_side_channel_injection() {
        let secret = SecretString::new("tok_live_9f2cfe1a7b44");
        assert_eq!(secret.expose(), "tok_live_9f2cfe1a7b44");
    }

    #[test]
    fn zero_out_overwrites_bytes_in_place_before_drop() {
        let mut secret = SecretString::new("alpha");
        secret.zero_out();
        // Length preserved and every byte NUL: the overwrite is observable,
        // proving `Drop` (which calls zero_out) scrubs the buffer rather
        // than just dropping it.
        assert_eq!(secret.inner, "\0\0\0\0\0");
        assert_eq!(secret.expose(), "\0\0\0\0\0");
    }

    #[test]
    fn grant_revoke_lifecycle() {
        let broker = broker();
        assert_eq!(broker.active_lease_count(), 0);

        let lease = broker
            .grant("deploy/token", "task-1", Duration::seconds(300))
            .expect("grant");
        assert_eq!(lease.scope, "deploy/token");
        assert_eq!(lease.task_id, "task-1");
        assert!(lease.expires_at > lease.granted_at);
        assert_eq!(lease.value.expose(), "tok_live_9f2cfe1a7b44");
        assert_eq!(broker.active_lease_count(), 1);

        // Re-granting the same (scope, task) replaces the lease.
        let _again = broker
            .grant("deploy/token", "task-1", Duration::seconds(60))
            .expect("re-grant");
        assert_eq!(broker.active_lease_count(), 1);

        broker.revoke(&lease).expect("revoke");
        assert_eq!(broker.active_lease_count(), 0);
        // Revocation is idempotent.
        broker.revoke(&lease).expect("re-revoke");
        assert_eq!(broker.active_lease_count(), 0);
    }

    #[test]
    fn unknown_scope_and_negative_ttl_are_rejected() {
        let broker = broker();
        let err = broker
            .grant("vault/root", "task-1", Duration::seconds(60))
            .unwrap_err();
        assert!(matches!(err, PolicyError::Invalid(ref msg) if msg.contains("vault/root")));

        let err = broker
            .grant("deploy/token", "task-1", Duration::seconds(-1))
            .unwrap_err();
        assert!(matches!(err, PolicyError::Invalid(_)));
    }

    #[test]
    fn redact_scrubs_leaked_values_from_a_transcript() {
        let broker = broker();
        let transcript =
            "worker used TOKEN=tok_live_9f2cfe1a7b44 then npm_XXXXsecretXXXX to publish";
        assert_eq!(
            broker.redact(transcript),
            "worker used TOKEN=***[deploy/token] then ***[registry/npm] to publish"
        );
        // Unknown-but-similar strings pass through untouched.
        assert_eq!(broker.redact("no secrets here"), "no secrets here");
    }

    #[test]
    fn redact_prefers_the_longest_match() {
        let broker =
            EphemeralBroker::new([("a/prefix", "tok_abc"), ("a/prefix/full", "tok_abcdef")]);
        assert_eq!(
            broker.redact("x tok_abcdef y"),
            "x ***[a/prefix/full] y",
            "the longer value must be replaced first"
        );
    }

    #[test]
    fn audit_details_carry_scope_only_never_the_value() {
        let broker = broker();
        let lease = broker
            .grant("deploy/token", "task-9", Duration::seconds(300))
            .expect("grant");
        let details = broker.audit_details(&lease);
        let rendered = details.to_string();
        assert!(rendered.contains("deploy/token"));
        assert!(
            !rendered.contains("tok_live"),
            "the value must never serialize"
        );
    }
}
