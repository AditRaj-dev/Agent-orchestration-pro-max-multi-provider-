//! F-03 integration tests — the frozen Claude fixture corpus (real observed
//! transcripts, `cli-fix-output/2026-08-22T06-30-51-908Z/fixtures`) driven
//! through the adapter's public surface. Everything here is offline: no
//! binary spawn, no network, no billable call.

use std::path::{Path, PathBuf};

use agentos_adapters::claude::{
    ClaudeAdapter, ClaudeInvocation, CLAUDE_FIXTURES_ENV, CLAUDE_SESSION_OVERHEAD_TOKENS,
};
use agentos_adapters::events::AdapterEvent;
use agentos_adapters::{RuntimeAdapter, SpawnSpec};
use uuid::Uuid;

/// Every T-battery transcript: T1 equals-form single disallow, T2 comma
/// denylist enforcement, T3 stdin prompt, T4 scoped allowlist (with a real
/// Bash tool use), T5a/T5b cost A/B (normal vs isolated home).
const FIXTURE_FILES: [&str; 6] = [
    "claude.T1-eq-single.stdout.jsonl",
    "claude.T2-eq-multi-enforce.stdout.jsonl",
    "claude.T3-stdin-prompt.stdout.jsonl",
    "claude.T4-allowlist.stdout.jsonl",
    "claude.T5a-cost-normal.stdout.jsonl",
    "claude.T5b-cost-isolated.stdout.jsonl",
];

/// The frozen corpus directory (env override, else the sibling
/// cli-fix-output tree). `None` → corpus unavailable on this machine;
/// corpus tests skip loudly rather than fail.
fn fixture_dir() -> Option<PathBuf> {
    if let Some(env_dir) = std::env::var_os(CLAUDE_FIXTURES_ENV) {
        let dir = PathBuf::from(env_dir);
        assert!(
            dir.is_dir(),
            "${} points at a missing directory",
            CLAUDE_FIXTURES_ENV
        );
        return Some(dir);
    }
    let default = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("cli-fix-output/2026-08-22T06-30-51-908Z/fixtures");
    default.is_dir().then_some(default)
}

fn spec(objective: &str, allowlist: Vec<&str>, denylist: Vec<&str>) -> SpawnSpec {
    SpawnSpec {
        task_id: Uuid::new_v4(),
        objective: objective.to_owned(),
        workspace: PathBuf::from("worktrees/task-claude"),
        allowed_paths: vec![],
        forbidden_paths: vec![],
        tool_allowlist: allowlist.into_iter().map(str::to_owned).collect(),
        tool_denylist: denylist.into_iter().map(str::to_owned).collect(),
        model: None,
        timeout_secs: 600,
        isolated_home: None,
    }
}

/// Full-corpus sweep: every observed run maps onto the same normalized
/// lifecycle and yields a resumable session id, real cost, and the two-model
/// per-model breakdown claude bills even on trivial runs.
#[test]
fn frozen_corpus_every_battery_run_maps_to_a_complete_lifecycle() {
    let Some(dir) = fixture_dir() else {
        eprintln!("claude fixture corpus unavailable; skipping corpus sweep");
        return;
    };
    assert!(
        !FIXTURE_FILES.is_empty(),
        "guard: the sweep must cover the whole battery"
    );

    for name in FIXTURE_FILES {
        let text = std::fs::read_to_string(dir.join(name)).expect(name);
        let mut reducer = agentos_adapters::claude::ClaudeStreamReducer::default();
        let mut events = Vec::new();
        for line in text.lines() {
            events.extend(reducer.push_line(line));
        }

        // Observed shape: init first, exactly one rate-limit notice inside
        // the successful run, the result's usage as the last stream event.
        assert_eq!(
            events.first().map(|event| event.kind()),
            Some("started"),
            "{name}: first event"
        );
        assert_eq!(events[1].kind(), "rate_limit", "{name}: second event");
        assert_eq!(
            events.last().expect("{name}: events").kind(),
            "usage_update",
            "{name}: last stream event"
        );

        // Digest: every battery run exited 0 with a result present.
        let terminal = reducer.finish(0);
        assert!(
            matches!(terminal, AdapterEvent::Finished { exit_code: 0, .. }),
            "{name}: {terminal:?}"
        );

        let session_id = reducer
            .session_id()
            .unwrap_or_else(|| panic!("{name}: no session id extracted for --resume"));
        assert_ne!(session_id, "unknown", "{name}");
        assert!(session_id.len() >= 8, "{name}: implausible id {session_id}");

        let result = reducer.result().expect("{name}: result payload");
        assert_eq!(result.is_error, Some(false), "{name}");
        assert_eq!(result.subtype.as_deref(), Some("success"), "{name}");
        assert!(result.result.is_some(), "{name}: final text");

        let usage = result.usage_snapshot();
        assert_eq!(usage.cost_usd, result.total_cost_usd, "{name}");
        assert!(
            usage.cost_usd.unwrap_or(0.0) > 0.0,
            "{name}: claude always bills"
        );
        assert_eq!(usage.per_model.len(), 2, "{name}: main + auxiliary model");
        assert!(
            usage
                .per_model
                .iter()
                .any(|row| row.model == "claude-sonnet-5"),
            "{name}: main model row missing"
        );
        assert_eq!(
            usage.session_overhead_tokens, CLAUDE_SESSION_OVERHEAD_TOKENS,
            "{name}"
        );
        assert!(
            result.subagent_stats.is_some(),
            "{name}: subagent_stats captured for depth guards"
        );
        assert!(result.permission_denials.is_empty(), "{name}");
    }
}

/// The adapter's rendered argv reproduces the battery's verified T2/T4 flag
/// shapes exactly (equals-form tool lists, acceptEdits, stream-json,
/// undocumented-but-working --max-turns), with the prompt on stdin instead
/// of a trailing positional (T3 default).
#[test]
fn invocation_reproduces_the_t2_and_t4_battery_argv() {
    let t4 = ClaudeInvocation::from_spec(&spec(
        "Run: echo PROBE_SHELL_TEST and reply DONE",
        vec!["Bash(echo:*)"],
        vec![],
    ))
    .with_max_turns(6)
    .args();
    assert_eq!(
        t4,
        vec![
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-mode",
            "acceptEdits",
            "--allowedTools=Bash(echo:*)",
            "--max-turns",
            "6",
        ]
    );

    let t2 = ClaudeInvocation::from_spec(&spec(
        "try to use bash",
        vec![],
        vec!["Bash", "WebFetch", "WebSearch"],
    ))
    .with_max_turns(6)
    .args();
    assert_eq!(
        t2,
        vec![
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-mode",
            "acceptEdits",
            "--disallowedTools=Bash,WebFetch,WebSearch",
            "--max-turns",
            "6",
        ]
    );
}

/// The public adapter surface answers discovery questions for free, without
/// touching the claude binary (detect/version probing is e2e-gated in the
/// module's ignored test).
#[tokio::test]
async fn claude_adapter_public_surface_is_free_and_offline() {
    let adapter: Box<dyn RuntimeAdapter> = Box::new(ClaudeAdapter::new());
    assert_eq!(adapter.id(), "claude-code");

    let caps = adapter.capabilities().await;
    assert!(caps.resume, "--resume verified (BANANA42)");
    assert!(!caps.structured_output, "no schema flag in v2.1.238 help");
}
