# F-03 — Claude Code Adapter

Status: implemented (this PR) · Crate: `crates/agentos-adapters`, module `src/claude.rs` (+ `tests/claude_adapter.rs`) · Date: 2026-08-22
Canon sources (binding): `F-00-CONVENTIONS.md` §4 (claude canon rows) + §5; `D:\OP\handoff.md` §3.1 + ADDENDUM (T1–T5 battery, all observed on this machine, 2026-08-22); the frozen fixture corpus `cli-fix-output/2026-08-22T06-30-51-908Z/fixtures/` (real observed transcripts — the adapter test corpus per handoff §6.4); `claude.help.txt` from the same run; F-02 trait/taxonomy layer in this crate.

Scope: `ClaudeAdapter` implementing `RuntimeAdapter` for Claude Code v2.1.238 — discovery, capabilities, session spawn/drive, a pure argv builder, a pure stream-json parser, and the `SessionBackend` wiring (resume via `--resume`, tree-kill cancel). Claude Code is the reference provider: verified end-to-end by the T1–T5 battery plus the opus-5 orchestrator probe.

## 1. Design

```
ClaudeAdapter ──start_session(spec)──▶ claude.exe ──stdout──▶ ClaudeStreamReducer ──▶ AdapterEvent channel
    │        └──stdin: objective (T3)──┘                        │
    ├─ detect/auth/capabilities (free)                          └─ finish(exit_code) ─▶ Finished/Failed
    └─ shutdown ──cancel all──► taskkill /PID <pid> /T /F (tree kill), then child.kill
```

- **One headless run = one process**, `-p --output-format stream-json --verbose`. The prompt rides **stdin by default** (T3-verified) — immune to the variadic tool-list flags *and* to Windows command-line length limits (task contracts exceed them).
- **Pure core, thin shell.** `ClaudeInvocation::from_spec` + `args()`/`env_overrides()` (argv/env rendering) and `ClaudeStreamReducer` (event mapping) are pure and exhaustively unit-tested offline against the frozen corpus; only the process driver (`drive_headless_run`) touches the OS.
- **Classification** uses the F-02 `Classifier` with machine-reliable signals only: exit code + presence of the final `result` event, plus payload hooks (`is_error`, `api_error_status`, `permission_denials`) — never stderr, which is drained to debug logs only.
- **No credentials are read, stored, or harvested.** Auth is a file-existence check (`~/.claude/.credentials.json` present on the observed install); the adapter never opens the file.

## 2. Arg surface (builder)

Rendered by `ClaudeInvocation::args()` in this canonical order (stable; covered by an exact-vector test):

| Arg | Form | Rule / mapping |
|---|---|---|
| `-p` | first | headless print mode |
| `--output-format stream-json` | two args | NDJSON event stream (init event gives session id + model early) |
| `--verbose` | two args | kept for older-version compat (handoff §3.1) |
| `--permission-mode <mode>` | two args | see §4; omitted for `Default` |
| `--allowedTools=<a,b>` | **single argv element, equals form** | `spec.tool_allowlist`, comma-joined; **empty list omits the flag** (T4 canon) |
| `--disallowedTools=<a,b>` | **single argv element, equals form** | `spec.tool_denylist`, comma-joined; empty list omits the flag (T1/T2 canon) |
| `--model <id>` | two args | passthrough of `spec.model` (non-variadic; separate form is safe) |
| `--resume <session_id>` | two args | `.with_resume_session(id)`; used for follow-up instructions (BANANA42-verified) |
| `--max-turns <n>` | two args | `.with_max_turns(n)` hook — works but is **undocumented in v2.1.238 `--help`** (battery ran `--max-turns 6`); smoke-test on upgrades, never gate on help scans |
| `<objective>` | LAST, optional | only with `.with_argv_prompt()` (T1 shape); stdin remains the default |

Everything is passed as an argv vector via `tokio::process::Command` — no shell strings, no manual quoting. The child runs with cwd = `spec.workspace`, stdin piped exactly when the prompt rides it (EOF after write), stdout+stderr piped, `kill_on_drop(true)`.

**The variadic rule (§3.2 root cause, binding).** `--allowedTools`/`--disallowedTools` are variadic: the separate-value form (`--disallowedTools Bash <prompt>`) consumes the prompt as a second tool value, and the CLI dies pre-model (`Error: Input must be provided either through stdin or as a prompt argument`, exit 1, zero events). The adapter therefore *always* renders them as single equals-form elements, regardless of prompt delivery mode. An exact-vector test pins the rendered T2/T4 battery argv.

## 3. Event mapping (fixture → AdapterEvent)

`ClaudeStreamReducer::push_line`, one stdout line → 0..n events; the terminal event is deferred until the exit code is known:

| claude stream event | AdapterEvent |
|---|---|
| `type=system && subtype=init` (session_id, model, 378-tool inventory, `permissionMode`, `mcp_servers`, …) | `Started { session_id, model }`; the id is remembered as the `--resume` handle |
| `type=system` with any other subtype (`thinking_tokens` observed twice per run in T2/T4) | skipped: log + continue (multiple `system` events exist per run; only `subtype=init` is the init) |
| `type=rate_limit_event` (`rate_limit_info`: `seven_day`, utilization 0.85, `allowed_warning`, …) | `RateLimit { provider_notice: rate_limit_info }` — observed inside *successful* runs; telemetry for F-06 proactive backoff, never a failure by itself |
| `type=assistant` → `message.content[]` `text` blocks | `TextDelta(text)` (empty strings dropped) |
| `type=assistant` → `tool_use` block (`name`, `input`) | `ToolUse { tool: name, args_summary }` — compact ≤3-entry `key=value` summary, values truncated (F-00 §3: full payloads belong in artifacts) |
| `type=assistant` → `thinking` blocks | skipped (not assistant output) |
| `type=user` (tool_result) | skipped — no F-02 variant; the `ToolUse` event already carried the invocation |
| `type=result` (final object, §3.1 schema) | `UsageUpdate` **now** (see §6); the payload is stashed; `finish(exit_code)` later emits `Finished { exit_code, final_result: result.result, structured: None }` or the classified `Failed` |
| anything else (schema drift), blank/malformed lines | skipped: log + continue, never an error |

T4 fixture, end to end: `Started` → `RateLimit` → `ToolUse(Bash, "command=echo PROBE_SHELL_TEST…")` → `TextDelta("DONE")` → `UsageUpdate` → (`finish(0)`) `Finished{0, "DONE", None}` — asserted event-for-event in `stream_reducer_maps_t4_allowlist_fixture_lifecycle`.

## 4. Policy surface

- **Per-tool allow/deny is Claude's differentiator** (only provider with it; agy has none, codex has sandbox-only). `spec.tool_allowlist` → `--allowedTools=…`, `spec.tool_denylist` → `--disallowedTools=…`, both equals-form comma lists, both omitted when empty. Verified: T4 `--allowedTools=Bash(echo:*)` auto-approves scoped shell (bash_tool_use=1, output seen, no quoting mangling); T2 `--disallowedTools=Bash,WebFetch,WebSearch` enforced network-off (zero tool uses, graceful exit 0).
- **Permission-mode mapping (documented decision).** `SpawnSpec` carries no write-intent field, so `from_spec` pins `acceptEdits` — the battery's verified headless write mode (file writes verified exactly; T2/T3/T4 all ran it). Read-only/network-off policy is expressed via the denylist (T2-verified) rather than `plan` mode, which the battery never exercised. `ClaudePermissionMode` models all six documented choices (`acceptEdits | auto | bypassPermissions | manual | dontAsk | plan`) plus `Default` (= omit the flag; in headless `-p`, approval-gated tools surface as `permission_denials` rather than blocking); callers override with `.with_permission_mode(...)`. `bypassPermissions` is never adapter-selected.
- **`permission_denials` → `PolicyDenial`.** A non-empty list is an *observable structured denial event* and feeds the F-02 `Classifier`'s denial rung (empty on every battery run; shape unpinned, carried verbatim).

## 5. Failure classification

Machine-reliable signals only: exit code + presence of the final `result` event + payload hooks. Success = exit 0 **and** result present **and** `is_error == false` (claude's payload flag is an additional gate beyond the F-00 §4 conjunction). Failure ladder (F-02 `Classifier` precedence):

| Observed shape | Signals | Classified as |
|---|---|---|
| Variadic-flag misuse: exit 1, zero events, ~3s (§3.2) | exit ≠ 0, no result event | `SpawnFailure` |
| Exit 0 but no final result event (malformed run) | no result event | `SpawnFailure` |
| `is_error: true` in result (no stronger signal) | result present | `TaskFailure` (detail cites the payload, not stderr) |
| Result present but non-zero exit | exit ≠ 0, result present | `TaskFailure` |
| `api_error_status` 401/403 | typed status | `AuthFailure` |
| `api_error_status` 429 | typed status | `Transient` (the only retryable class) |
| `permission_denials` non-empty | observable denial event | `PolicyDenial` |
| Unknown `api_error_status` (e.g. 500) | typed code | ladder fall-through; code preserved in the detail string |
| Harness watchdog expiry | timeout_secs elapsed | `Transient` ("exceeded budget; process killed") |

Table-tested in `classification_table_for_claude_run_endings` + `classification_details_carry_the_claude_signals`.

## 6. Usage / ledger mapping

`ClaudeResult::usage_snapshot()` maps the §3.1 result schema onto F-02's `UsageSnapshot`:

- `input_tokens` ← `usage.input_tokens`; `output_tokens` ← `usage.output_tokens`; `thinking_tokens` ← `usage.output_tokens_details.thinking_tokens`; `cache_read_tokens` ← `usage.cache_read_input_tokens`.
- `total_tokens` ← input + output + cache_read + cache_creation (claude reports no total field; F-02 has no cache-creation slot, so it folds into the total only — documented deviation).
- `cost_usd` ← **`total_cost_usd` ONLY** (the field `cost_usd` does not exist — handoff §3.1).
- `per_model` ← the `modelUsage` **object keyed by model** (not an array): each row's `costUSD`/`contextWindow`; BTreeMap ordering keeps the vector deterministic. A trivial run touches **two models** — `claude-sonnet-5` main (1M ctx, 64k max out) + `claude-haiku-4-5-20251001` auxiliary — so the F-06 ledger is per-model from day one.
- `session_overhead_tokens` ← `CLAUDE_SESSION_OVERHEAD_TOKENS = 22_000` (F-00 §4 / claude T5: the ~22k cache-read preamble is the *core system prompt, not user skills* — isolated-home A/B moved cacheRead 22115 → 22115, cost Δ≈$0.0004).
- `server_tool_use` counts (`web_search_requests`/`web_fetch_requests`) are parsed on `ClaudeUsage` for SEC-02 post-hoc network-compliance telemetry.
- **`subagent_stats` is captured** on `ClaudeResult` (spawned/refused/killed/max_depth observed) for the supervisor's depth/budget guards, which must sit *above* claude v2's provider-internal subagents. F-02's `UsageSnapshot` has no slot for it — it is carried on the public result payload instead (deviation, §11).

Fixture-checked (T4): input 4, output 206, thinking 112, cache_read 73060, total 102359, cost $0.200528, two per-model rows ($0.000974 haiku @200k / $0.199554 sonnet-5 @1M).

## 7. Session lifecycle, instructions, cancel, timeout

- **spawn**: `start_session(spec)` builds the invocation, resolves the binary, spawns synchronously (machinery failures are `Result` errors), parks the child, and drives it on a spawned task. Prompt bytes are written to stdin, then the handle is flushed and dropped → the child sees EOF and starts its turn (T3).
- **session id**: learned from the init event (early) or the result event (fallback), remembered for `--resume`.
- **instructions (documented F-03 deviation, mirroring the agy sibling)**: headless `-p` runs consume stdin exactly once, so `send_instruction` delivers a **new headless run resuming the provider session** (`--resume <id>`, BANANA42-verified) with the instruction as its stdin prompt; events stream on the same session channel. `Finished` means *turn complete* — the session stays instructable until it `Failed` or is cancelled. Errors: `SessionNotActive` on dead sessions (finished-and-failed or cancelled), `Internal` while a run is active or before the first run's session id is known.
- **cancel**: process-kill semantics — the stream just ends, no terminal event. Windows: `taskkill /PID <pid> /T /F` tree-kill first (claude v2 spawns provider-internal subagents; a bare `TerminateProcess` would orphan them), then `child.kill()` reaps regardless.
- **timeout**: `spec.timeout_secs` harness watchdog (`0` disables). Expiry kills the tree and emits `Failed(Transient)` — retryable, since quota windows and hangs clear on their own.

## 8. Home isolation (env mapping only)

`spec.isolated_home` maps to child env overrides: `USERPROFILE` and `HOME` both point at the isolated home (`ClaudeInvocation::env_overrides()`, pure and tested). **Junction creation is the supervisor's job**: the isolated home must contain junctions to the provider config dirs that matter (auth), per the handoff §4.1 pattern; T5b proved an isolated home with copied `~/.claude` config runs identically (cacheRead 22115 = 22115). Consequence of T5: isolation is *optional for cost* (the preamble is the core prompt, not the 1278 `~/.agents` skills) but still useful for determinism.

## 9. Discovery (free, never billable)

- **detect**: `$AGENTOS_CLAUDE_BIN` → the observed canonical install `%USERPROFILE%\.local\bin\claude.exe` → PATH scan. Version via `claude --version` (free).
- **auth_status**: binary present + `~/.claude` dir + `~/.claude/.credentials.json` existence → `Ready`; dir/credentials missing → `NeedsLogin`; no binary → `Unknown`. Existence only — the file is never opened (RT-06 tier 0).
- **capabilities** (each with its basis): `filesystem_edit` ✅ (acceptEdits probe.txt), `shell` ✅ (T4 Bash on native Windows), `network` ✅ (WebFetch/WebSearch in the 378-tool init inventory; T2 denied them — which proves they exist), `resume` ✅ (`--resume` BANANA42), `long_context` ✅ (`modelUsage` contextWindow 1M), `mcp_client` ✅ (init `mcp_servers` with live connected entries), `multimodal` (model-family inference — **not exercised by the battery**), `structured_output` **false** (no schema flag in v2.1.238 help; battery never exercised one — flip only after a smoke test).
- `health()` is the F-02 default composition of detect + auth.

## 10. Test evidence (fresh output, same session as this PR)

Binding commands (custom target dir avoids lock contention):

```
$ CARGO_TARGET_DIR=target/f03 cargo test -p agentos-adapters claude
running 18 tests
test claude::tests::e2e_free_probes_version_and_auth_files ... ignored, live claude invocation (free probes only): set AGENTOS_CLAUDE_E2E=1 to run
test claude::tests::adapter_id_and_capability_surface_reflect_observed_claude ... ok
test claude::tests::arg_builder_argv_prompt_rides_last_behind_equals_form_flags ... ok
test claude::tests::arg_builder_full_vector_in_canonical_order ... ok
test claude::tests::arg_builder_headless_base_is_stdin_prompted_stream_json ... ok
test claude::tests::arg_builder_omits_empty_tool_lists ... ok
test claude::tests::arg_builder_passes_model_resume_and_undocumented_max_turns ... ok
test claude::tests::arg_builder_permission_mode_default_omits_the_flag ... ok
test claude::tests::arg_builder_renders_tool_lists_in_equals_form ... ok
test claude::tests::classification_details_carry_the_claude_signals ... ok
test claude::tests::classification_table_for_claude_run_endings ... ok
test claude::tests::env_overrides_redirect_home_only_when_isolated ... ok
test claude::tests::result_event_with_missing_fields_still_counts_as_final_event ... ok
test claude::tests::stream_reducer_maps_t3_stdin_fixture_lifecycle ... ok
test claude::tests::stream_reducer_maps_t4_allowlist_fixture_lifecycle ... ok
test claude::tests::stream_reducer_skips_unknown_and_malformed_lines ... ok
test claude::tests::tool_input_summarizer_is_compact_and_truncated ... ok
test claude::tests::usage_mapping_uses_total_cost_usd_and_per_model_rows ... ok

test result: ok. 17 passed; 0 failed; 1 ignored; 0 measured; 42 filtered out; finished in 0.00s
     Running tests\claude_adapter.rs (target\f03\debug\deps\claude_adapter-3ac66763b84482b2.exe)
test claude_adapter_public_surface_is_free_and_offline ... ok
test result: ok. 1 passed; 0 failed; 0 measured; 2 filtered out; finished in 0.00s
```

Two integration tests whose names lack the filter word (verified separately, full suite):

```
$ CARGO_TARGET_DIR=target/f03 cargo test -p agentos-adapters --test claude_adapter
running 3 tests
test claude_adapter_public_surface_is_free_and_offline ... ok
test invocation_reproduces_the_t2_and_t4_battery_argv ... ok
test frozen_corpus_every_battery_run_maps_to_a_complete_lifecycle ... ok
test result: ok. 3 passed; 0 failed; 0 measured; 0 filtered out; finished in 0.01s

$ CARGO_TARGET_DIR=target/f03 cargo clippy -p agentos-adapters --all-targets -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.52s
```

Full-crate regression: `58 passed; 0 failed; 2 ignored` (lib) — mock/agy/error modules untouched and green. `rustfmt --check` clean on both new files. No test makes a billable call or spawns the real binary; the only live invocation anywhere is the `#[ignore]`d free-probe test (`--version` + auth-file existence), doubly gated behind `AGENTOS_CLAUDE_E2E=1`.

Fixture-corpus tests resolve the corpus via `$AGENTOS_CLAUDE_FIXTURES` or the sibling `cli-fix-output/2026-08-22T06-30-51-908Z/fixtures` tree, and skip loudly (eprintln, never silently green on a wrong path) on machines without it — verified here that no skip notice fired, i.e. the real transcripts were consumed.

## 11. Deviations, assumptions, and open smoke items

1. **`Finished` = turn complete, session stays instructable** (agy-consistent deviation from the F-02 doc's stricter "terminal ends instructability"): the verified multi-turn mechanism on claude is `--resume`, not stdin reuse. Dead sessions (`Failed`/cancelled) reject instructions with `SessionNotActive`.
2. **`subagent_stats` is not an `AdapterEvent`/`UsageSnapshot` field** (F-02 surface is fixed): captured on the public `ClaudeResult` payload for F-06 depth guards; a future F-02 variant could promote it.
3. **`cache_creation` tokens fold into `total_tokens` only** — F-02's `UsageSnapshot` has no cache-creation slot.
4. **`structured_output: false`** pending a smoke test; claude may grow an output-schema surface (help shows none in v2.1.238). Likewise `multimodal: true` rests on model-family inference, not battery evidence.
5. **Auth check is file-existence only** and assumes the observed `~/.claude/.credentials.json` layout; if claude moves credentials into an OS keychain/credential store, `auth_status` needs a new free signal. `ANTHROPIC_API_KEY`-based auth is not considered (absent on this machine; canon is credential-file existence).
6. **`--max-turns` is undocumented in help** but battery-verified; re-verify on every CLI upgrade (the adapter exposes it as an explicit opt-in hook, never a default).
7. **T2 enforcement evidence is zero-executions, not a denial event** (handoff addendum note): the denylist held (no bash uses, graceful completion) but produced no `permission_denials` entry; the `PolicyDenial` path rests on the schema plus the classifier contract, not on an observed populated denial. First real denial observed in production should be diffed against the unpinned `permission_denials` shape.
8. **Resume runs are not in the frozen corpus** (BANANA42 predates the fixture freeze): the `--resume` + stdin-prompt combination is verified by handoff §3.1 evidence and the opus-5 session-4 probes, not by a fixture test.
