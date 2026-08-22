# F-02 — Runtime Adapter Layer

Status: implemented (this PR) · Crate: `crates/agentos-adapters` · Date: 2026-08-22
Canon sources (binding): `F-00-CONVENTIONS.md` §4 (provider-adapter canon table) + §5 (working agreements); `D:\OP\handoff.md` §3 + addenda (observed CLI contracts); PRD §8 (RT-01 adapter interface, RT-06 health/capability discovery).

Scope of this PR: the provider-independent `RuntimeAdapter` trait, the `AdapterEvent` streaming model, the `AdapterFailure` taxonomy + exit-code/final-event `Classifier`, and a credential-free `MockAdapter`. Concrete CLI adapters are deliberately NOT here — Claude Code lands in F-03, codex in F-04, agy + zcode in F-05/F-05b — but every type decision below is derived from those observed contracts, so the later adapters implement rather than redesign.

## 1. Trait design (PRD RT-01)

```rust
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    fn id(&self) -> &str;
    async fn detect(&self) -> RuntimeInfo;                     // free
    async fn auth_status(&self) -> AuthStatus;                 // free, never billable
    async fn capabilities(&self) -> Capabilities;              // RT-06 flag set
    async fn health(&self) -> HealthReport;                    // default: detect + auth + timestamp
    async fn start_session(&self, spec: SpawnSpec) -> Result<SessionHandle, AdapterError>;
    async fn shutdown(&self) -> Result<(), AdapterError>;
}
```

Decisions:

- **Object-safe + `async_trait`** — the supervisor holds `Box<dyn RuntimeAdapter>` pools; proven in `mock::tests::discovery_surfaces_are_canned_and_free`.
- **`health()` has a default impl** composing `detect()` + `auth_status()`. RT-06 requires health checks to never make billable calls; the free tier observed on this machine (claude auth-file existence, codex `login status` / `codex doctor`, agy `agy models`) maps exactly onto that composition.
- **Run failures are events, not `Result` errors.** `start_session` errors mean the adapter machinery failed (spawn refused, session dead); provider-run failures stream as `AdapterEvent::Failed(AdapterFailure)` so the daemon journals the full lifecycle (F-00 §3: append-only events, UI is a projection). PRD Appendix G's "auth failure distinct from task failure" is satisfied by the taxonomy below, not by distinct `Result` types.
- **`SpawnSpec.isolated_home`** carries the observed env-redirection isolation pattern (handoff §4.1 / codex C1: temp `USERPROFILE`/`HOME` with junctions to provider config dirs only). Field now, spawn-time enforcement with the concrete adapters.

### Session handle

```rust
#[derive(Clone)]
pub struct SessionHandle {
    pub fn session_id(&self) -> &str;
    pub fn events(&self) -> broadcast::Receiver<AdapterEvent>;
    pub async fn send_instruction(&self, text: String) -> Result<(), AdapterError>;
    pub async fn cancel(&self) -> Result<(), AdapterError>;
}
```

**Broadcast over mpsc, justified:** a session stream has multiple natural consumers in this architecture — the event journal (F-01), the budget ledger (F-06), and the live UI projection — all needing the same ordered feed, any of which may attach late. mpsc is single-consumer and would force an internal fan-out task per session; broadcast gives fan-out natively with per-consumer bounded buffers (consumers must be idempotent/lag-tolerant anyway, F-00 §3). Provider-specific send/cancel semantics live behind an internal `SessionBackend` trait object; the handle owns the shared channel.

Termination semantics: a completed session ends with a terminal event (`Finished`/`Failed`); the channel stays open for the handle's lifetime. `cancel()` is process-kill semantics — the stream goes quiet with **no** terminal event, exactly as a killed CLI behaves.

## 2. Event model (`AdapterEvent`)

Adjacently tagged serde enum (`{"type": "...", "data": ...}`, camelCase), forward-compatible for new variants.

| Variant | Observed origin (handoff) |
|---|---|
| `Started { session_id, model }` | claude init event (`type=system && subtype=init`); agy `init` (conversation_id, model, cwd, 58-tool inventory) |
| `TextDelta(String)` | claude stream-json deltas; agy `step_update.text_delta` |
| `ToolUse { tool, args_summary }` | claude `tool_use` events; agy tool steps. Summary only — large payloads go to content-addressed artifacts (F-00 §3) |
| `UsageUpdate(UsageSnapshot)` | claude `usage` + `modelUsage` at result; agy per-step and result usage |
| `RateLimit { provider_notice }` | claude `rate_limit_event`, observed inside *successful* runs at 0.86 seven-day utilization — captured verbatim for F-06 proactive backoff |
| `Finished { exit_code, final_result, structured }` | exit code + final-result-event presence (canon); claude `result.result`; agy `response` + `structured_output` (F-07 contracts) |
| `Failed(AdapterFailure)` | typed provider business errors (zcode `ProviderBusinessError`), classified |

`UsageSnapshot` carries the claude `modelUsage` canon as `per_model: Vec<ModelUsageRow>` (a trivial claude run billed two models — sonnet-5 main + haiku-4-5 aux, each with `costUSD`/`contextWindow`) plus `session_overhead_tokens`, the fixed per-session preamble ledger line (§4 below). `cost_usd` is `Option`: claude exposes `total_cost_usd` (the only cost field that exists); agy/codex are token-only — estimates are labeled only where estimates exist.

## 3. Failure taxonomy (the canon part)

`AdapterFailure` variants, each with its observed evidence:

| Variant | Evidence (handoff citation) | Retryable |
|---|---|---|
| `AuthFailure` | RT-04 reauthentication events; HTTP 401/403 surfaced through typed provider errors (claude `api_error_status`, zcode `responseStatus`) | no |
| `BillingFailure { provider_code }` | zcode `ProviderBusinessError [1113] 余额不足或无可用资源包` — observed through the BigModel key path (session 4) | no |
| `BotGateOrCaptcha { provider_code }` | zcode `[3007] captcha verify failed` on every headless attempt — provider anti-bot, explicitly not bypassable (session 4) | no |
| `PolicyDenial` | observable denial *events*: agy headless accept-edits denies shell with explicit "user denied permission"; claude `permission_denials` result field (§3.1) | no |
| `Transient` | claude `rate_limit_event` under utilization pressure; codex usage-limit until Sep 10 (§7.8: never misdiagnose as capability failure) | **yes** |
| `TaskFailure` | agy `status: ERROR` with the result event present and exit 2 — the model round-tripped and failed on its own merits (agy battery) | no |
| `SpawnFailure` | pre-model deaths: claude variadic-flag exit 1 with zero events (§3.2); codex exit 2 "unexpected argument" + session-init death exit 1 at ~7–9 s (§3.3, §7.3) | no (documented) |

**`is_retryable()` — the SpawnFailure choice.** Only `Transient` returns true. SpawnFailure was deliberately excluded: every observed pre-model death was deterministic in its environment (unsupported flag, config-pinned unsupported model, missing binary); an automatic retry re-hits the same wall and hides the root cause. Remediation is an environment fix followed by a *fresh* session, not a retry. `AdapterError::is_retryable()` delegates through `SessionFailed(AdapterFailure)`.

### Classifier (`error::Classifier`)

Binding rule (F-00 §4 / handoff §7.5): classify by **exit code + final-result-event presence** (plus typed provider codes and observable denial events) — **never stderr text** (codex stderr is wall-to-wall non-fatal skill-YAML noise; agy plan-mode SUCCESS emits scary-looking text).

```rust
Classifier::is_success(exit_code, saw_final_result_event)  // 0 && final-event
Classifier::classify(exit_code, saw_final_result_event, provider_code, denial_observed)
    -> AdapterFailure
```

Precedence ladder, strongest evidence first:

1. **Typed provider code** (zcode-style `providerCode` or an HTTP status surfaced by a typed error): `1113` → Billing, `3007` → BotGate, `401`/`403` → Auth, `429` → Transient. Unrecognized codes fall through and survive in the detail string.
2. **Observable denial event** → PolicyDenial.
3. **No final result event** → SpawnFailure (pre-model death; the run never completed a model round-trip).
4. **Final result present but non-zero exit** → TaskFailure (model tried and failed).

Known limitation, documented in code: exit-code-only classification cannot distinguish an auth wall from a spawn death when the provider types its errors neither as codes nor events (gemini-cli's `IneligibleTierError` class); adapters with richer signals override the ladder. The classifier covers the observed corpus of all four verified providers.

## 4. Overhead ledger

`UsageSnapshot.session_overhead_tokens` is the per-session preamble line the budget ledger (F-06) must carry (handoff §4.3): **~22k tokens claude** (T5: cacheRead 22115 with isolated home identical — core system prompt, *not* `~/.agents` skills) and **~37k agy** (37133→37136 with `--disable-slash-commands`, same verdict). The mock pins `MOCK_SESSION_OVERHEAD_TOKENS = 22_000` so ledger arithmetic is exercised against realistic numbers from day one.

## 5. MockAdapter usage

```rust
use agentos_adapters::mock::{MockAdapter, MockBehavior};
use agentos_adapters::{AdapterFailure, RuntimeAdapter, SpawnSpec};

// success script
let adapter = MockAdapter::new(MockBehavior::Success {
    turns: 2,
    files_changed: vec!["src/lib.rs".to_owned()],
});

// deterministic retry path: 2 transient failures (each preceded by a
// RateLimit notice, mirroring claude), then success
let adapter = MockAdapter::new(MockBehavior::FlakyThenSuccess { failures_before_success: 2 });

// classified failure injection
let adapter = MockAdapter::new(MockBehavior::FailWith(
    AdapterFailure::BillingFailure { provider_code: Some(1113), detail: "insufficient balance".into() },
));

let handle = adapter.start_session(spec).await?;
let mut events = handle.events();
while let Ok(event) = events.recv().await {
    if event.is_terminal() { break; }   // Finished or Failed
}
```

Properties: fully deterministic (no randomness, no wall-clock dependence in the script; token counts are arithmetic on the spec); discovery surfaces are canned (`Ready` / `Capabilities::full()`); attempt counting for `FlakyThenSuccess` is per adapter instance across `start_session` calls — exactly the path a supervisor retry loop takes; `shutdown()` cancels every live session. The script holds until the first subscriber attaches (broadcast has no replay), so a caller subscribing right after `start_session` always observes the full stream from `Started`.

## 6. Test evidence

Commands (custom target dir avoids lock contention with parallel builds):

```
CARGO_TARGET_DIR=target/adapters cargo test -p agentos-adapters
CARGO_TARGET_DIR=target/adapters cargo clippy -p agentos-adapters --all-targets -- -D warnings
```

25 unit tests + 1 doc-test, all passing; clippy clean under `-D warnings`; `cargo fmt --check` clean. Coverage:

- success lifecycle: exact event ordering (`started` → per-turn `text_delta`×2 + `tool_use` → `usage_update` → `finished`), completion packet (`filesChanged`, `tests: skipped`, summary text), overhead line = 22 000, per-model row
- `FlakyThenSuccess`: `started → rate_limit → failed(Transient)` twice, then `finished`; retryability asserted
- `FailWith`: `started → failed` with the exact failure; `SpawnFailure` variant skips `Started`; post-mortem `send_instruction` → `SessionNotActive`
- classifier: the observed-evidence mapping table (10 rows), typed-code preservation, unrecognized-code fall-through, defensive success input
- cancel/shutdown: no terminal events after cancellation; sessions inactive
- serde: camelCase round-trips for events, `UsageSnapshot`, `SpawnSpec`, `AuthStatus`
- object safety: discovery exercised through `Box<dyn RuntimeAdapter>`

## 7. Out of scope / next

- F-03 claude / F-04 codex / F-05+agy / F-05b zcode: concrete `RuntimeAdapter` impls against frozen probe fixtures (`D:\OP\cli-*-output\*` becomes the test corpus). They reuse this taxonomy and classifier unchanged.
- `isolated_home` spawn-time enforcement (junction technique) lands with the first concrete adapter.
- Daemon-side translation `AdapterEvent` → `agentos_core::Event` journal rows belongs to F-01's consumer, not this crate.
- zcode HTTP-status mapping beyond 401/403/429 extends as new typed codes are observed — never via stderr matching.
