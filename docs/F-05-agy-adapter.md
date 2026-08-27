# F-05 — Antigravity (`agy`) Adapter

Status: implemented (this PR) · Crate: `crates/agentos-adapters`, module `src/agy.rs` · Date: 2026-08-22
Canon sources (binding): `F-00-CONVENTIONS.md` §4 (agy canon rows) + §5; `D:\OP\handoff.md` ADDENDUM "PIVOT — Antigravity CLI v1.1.18" (all findings observed on this machine, 9-call battery, 2026-08-22); F-02 trait/taxonomy layer in this crate.

Scope: `AgyAdapter` implementing `RuntimeAdapter` for the Antigravity CLI — discovery, capabilities, session spawn/drive, a pure argv builder, a pure json/stream-json parser, and the `SessionBackend` wiring (resume via `--conversation`, process-kill cancel). Per the pivot decision, agy replaces gemini-cli as the third MVP provider (Claude/Codex/agy).

## 1. Design

```
AgyAdapter ──start_session(spec)──▶ agy.exe ──stdout──▶ AgyStreamReducer ──▶ AdapterEvent channel
    │                                  ▲                        │
    ├─ detect/auth/capabilities (free)  │                        └─ finish(exit_code) ─▶ Finished/Failed
    └─ shutdown ──cancel all──► taskkill /T /F (tree kill)
```

- **One print run = one process.** The prompt rides `--print=<objective>` (equals form — the flag is variadic; see §2). stream-json is the default output mode; json is a per-invocation override.
- **Pure core, thin shell.** `AgyInvocation::from_spec` + `args()` (argv rendering) and `AgyStreamReducer` (event mapping) are pure and exhaustively unit-tested offline; only the process driver (`drive_print_run`) touches the OS.
- **Classification** uses the F-02 `Classifier` with machine-reliable signals only: exit code + presence of the final result event, plus payload-content hooks (the structured `error` field — never stderr, which is drained to debug logs only).
- **No credentials are read, stored, or harvested.** Auth lives with the agy CLI / Antigravity account; the adapter's free auth check is binary presence + state-dir existence (see §6).

## 2. Arg surface (builder)

Rendered by `AgyInvocation::args()` in this canonical order (stable; covered by an exact-vector test):

| Arg | Form | Rule / mapping |
|---|---|---|
| `--mode <plan\|accept-edits>` | two args | Write-intent mapping, see below |
| `--output-format <stream-json\|json>` | two args | `stream-json` default (init event gives conversation id + model early) |
| `--model <id>` | two args | passthrough of `SpawnSpec.model` |
| `--effort <low\|medium\|high>` | two args | passthrough hook — `SpawnSpec` has no effort field; callers chain `.with_effort()` |
| `--json-schema <file>` | two args | `.with_json_schema(path)`; agy accepts inline JSON or a file — the adapter always uses a **file** (schemas exceed comfortable argv lengths) |
| `--conversation <id>` | two args | `.with_conversation(id)`; used for resume (verified round-trip) |
| `--add-dir <dir>` | two args, **repeatable** | ALWAYS includes `spec.workspace`, then each `spec.allowed_paths` entry (deduped against the workspace) |
| `--print-timeout <secs>` | two args | `spec.timeout_secs + 30` slack when `timeout_secs > 0`; `0` omits the flag (CLI default 5m applies) |
| `--print=<objective>` | **single argv element, LAST** | equals form — `--print` is VARIADIC; the battery verified that equals form is the safe delivery (F-00 §4) |

Everything is passed as an argv vector via `tokio::process::Command` — no shell strings, no manual quoting. The child runs with cwd = `spec.workspace`, stdin null, stdout+stderr piped.

**Write-intent / mode mapping (documented decision).** `SpawnSpec` (F-02) carries no explicit write-intent field. F-05 derives it from `allowed_paths` emptiness:

- `allowed_paths` empty → no additional access targets declared → read-only/analysis run → `--mode plan` (VERIFIED read-only: write prompt → plan artifact, no file, SUCCESS).
- `allowed_paths` non-empty → declared write targets → `--mode accept-edits`, each entry granted via its own `--add-dir`.

The workspace is granted via `--add-dir` **in both cases**: `--add-dir` is the *access grant*, `--mode` is the *enforcement* — plan-mode reads of the workspace are unaffected, and in accept-edits the workspace is writable exactly because it was granted. Consequence for callers: a write task that only touches the workspace must still declare at least one `allowed_paths` entry (the workspace itself is accepted and deduplicated) to escape plan mode. When F-02 grows an explicit write-intent field, this proxy is replaced one-for-one.

## 3. Event mapping

**stream-json** (`AgyStreamReducer::push_line`, one line → 0..n events):

| agy event | AdapterEvent |
|---|---|
| `init` (`conversation_id`, `model`, `cwd`, 58-tool inventory, `permission_mode`) | `Started { session_id: conversation_id, model }`; the id is remembered for resume |
| `step_update` (`text_delta`, `state`, per-step `usage`) | `TextDelta(text_delta)` when non-empty + `UsageUpdate(usage)` when present — per-step usage is forwarded, not just the final one |
| `result` (payload identical to the json-mode object) | `UsageUpdate` (usage still present on ERROR runs) now; the terminal event is **deferred** until the process exit code is known |
| anything else (schema drift) | skipped: log + continue, never an error |

Envelope: the kind is read from `type` with `event` as fallback — the observed canon pins the event names but not the envelope field; both are accepted (fixtures use `type`).

**json mode** (`AgyStreamReducer::from_json_output` + `json_run_events`): json mode emits no `init`, so the adapter synthesizes a late `Started` from the result payload (model unknown → `None`), then `UsageUpdate`, then the terminal event. The object is located tolerantly (whole text → single line → first `{`…last `}` slice inside noise).

**Terminal decision** (`terminal_event(exit_code, result)`):

- `exit == 0 && status == SUCCESS` → `Finished { exit_code: 0, final_result: response, structured: structured_output }`.
- Otherwise → `Classifier::classify(exit, saw_final, provider_code, denial)` with:
  - `provider_code` — hook `provider_code_from_error` returns `None` today (no typed provider codes observed on agy v1.1.18, unlike zcode's `ProviderBusinessError [1113]/[3007]`); when one appears, map the textual signature there and the ladder yields Billing/BotGate/Auth/Transient automatically.
  - `denial` — the structured `error` field containing the observed `"user denied permission"` string (accept-edits denying shell headless) → `PolicyDenial`. This is machine-parsed payload content, not stderr scraping.
- The payload's `status` outranks the exit code: `ERROR` is never a success even at exit 0; `SUCCESS` at non-zero exit is still a failed run.

## 3a. LIVE envelope correction (2026-08-22)

F-05 shipped against **synthetic flat fixtures**; the real CLI **nests every payload under a key equal to its kind**, so the reducer parsed nothing on a real run — no text, no result, no conversation id, every agy session terminal-failing. Found by the first live agent-builder e2e; fixed and pinned against frozen real transcripts (`cli-agy-output/2026-08-22T17-44-06-000Z/fixtures`).

Real shapes:

- `{"event":"init","conversation_id":"<id>","init":{"model":...,"cwd":...,"tools":[...],"permission_mode":...}}` — id hoisted to the envelope, model nested.
- `{"event":"step_update","step_update":{conversation_id, step_index, state, step_type, text_delta?, usage?, tool_name?, tool_info?}}` — `step_type` is `user_input` | `checkpoint` | `agent_response` | `tool`; `state` runs `ACTIVE` → `DONE` | `ERROR`. Tool steps now map to one `ToolUse` per ACTIVE edge (previously agy emitted **no** tool events at all).
- `{"event":"result","result":{conversation_id,status,response,error,duration_seconds,num_turns,usage}}`.

The parser accepts the flat shape as a fallback, so older transcripts still reduce.

Two further corrections to the synthetic canon, both observed three times:

1. **`status: ERROR` does NOT imply exit 2** — every captured ERROR run exited **0**. The payload status is authoritative (F-00 §4); the terminal event must fail on it regardless of exit code.
2. **Quota exhaustion is its own class.** Observed verbatim: `Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 146h41m26s.` (gemini-3.1-pro-high, ERROR + exit 0). It now classifies as **Transient (retryable)** with the reset window preserved in the detail, instead of an opaque task failure.

## 3b. Plan-mode decisions (no asking tool)

agy's init inventory *does* list `ask_question`, `ask_permission` and
`ask_custom_permission` — but headless they are **auto-skipped**: no tool
step reaches the stream and the model simply reports that the question was
skipped (fixture `agy.A3-ask-question-skipped`). There is no asking surface
to lift a question out of. Plan-mode options therefore ride
in the answer text as a fenced JSON block, which the reducer turns into
`AdapterEvent::Decision` at `result` time (both the stream-json and plain
`--json` paths), before the terminal event:

```json
{"ask": {"question": "Which database?", "options": ["Postgres", "SQLite"], "multiSelect": false}}
```

Parsing lives in `agentos-adapters/src/decision.rs` (shared with F-03 claude
and, later, F-04 codex). It is tolerant by construction: a block that does
not parse, carries no options, or is still half-streamed is simply not a
decision — a malformed ask never fails a session. The emitting side is the
`decision-protocol` skill (F-13), held by every agy-backed built-in agent;
without that skill the model never emits the block and the feature is inert.

## 4. Virtualized filesystem and the `--add-dir` rule

CRITICAL observed semantic that differs from Claude/Codex: **in print mode, agy writes are virtualized** to `~/.gemini/antigravity-cli/brain/<conversation_id>/` (+ `.metadata.json` sidecar) and `~/.gemini/antigravity-cli/scratch/`. The cwd is NOT writable by default — a naive run "succeeds" while editing a shadow copy of the workspace.

Therefore the adapter **always passes `spec.workspace` via `--add-dir`** (plus each `allowed_paths` entry), regardless of mode. This was verified in the battery: `realfile.txt` appeared in the real directory with exit 0 only with `--add-dir`. The worktree prepared by F-09 must exist before `start_session` (it is the child's cwd).

## 5. Policy surface and the per-tool gap

agy's policy surface is **coarse**: `--mode accept-edits|plan` (plan = verified read-only), `--dangerously-skip-permissions` (never set by this adapter), `--sandbox` (untested in the battery, not set). **There are no per-tool allow/deny flag lists** — a real gap vs Claude's `--allowedTools=Bash(echo:*)` / `--disallowedTools=Bash,WebFetch` (T1–T4).

`Capabilities` is a boolean surface, so the gap is invisible there by design. Compensation is harness-side, per the handoff reconciliation queue (item 6):

1. **Mode selection** (this adapter): write intent gates accept-edits vs plan.
2. **`--add-dir` scoping** (this adapter): real-dir access exists only for declared paths.
3. **F-10 gating** (supervisor): `SpawnSpec.forbidden_paths`, `tool_allowlist`, and `tool_denylist` are enforced by the harness (path policy, post-hoc tool-use telemetry, approval gates) — agy cannot express them, so the harness must not delegate them.
4. agy's built-in subagents (`define_subagent`/`invoke_subagent`/`manage_subagents`) and browser suite sit *below* F-06 depth guards; the harness's guards must sit above provider-internal ones.

`--dangerously-skip-permissions` is deliberately never emitted.

## 6. Discovery (free, never billable)

| Method | Behavior |
|---|---|
| `id()` | `"antigravity-agy"` |
| `detect()` | resolve binary: `$AGENTOS_AGY_BIN` → `%LOCALAPPDATA%\agy\bin\agy.exe` (observed canonical install) → PATH scan (`agy.exe`/`agy`); version via the free `--version` probe (first stdout line) |
| `auth_status()` | binary present **and** `~/.gemini/antigravity-cli` state dir exists → `Ready`; otherwise `Unknown` (never `NeedsLogin` — unauthenticated cannot be distinguished without a call). The authoritative check is the **free** `agy models` call; it is a live invocation, so it lives only in the e2e-gated test |
| `capabilities()` | constant surface, every flag annotated with its observed basis (see `agy_capabilities()` doc); `health()` uses the F-02 default composition |

Capability basis: `filesystem_edit` (--add-dir verified), `shell` (`run_command` in the 58-tool inventory; policy-gated headless — observed denial), `network` (browser suite + web tools), `structured_output` (--json-schema verified), `resume` (--conversation verified, BANANA42), `multimodal` (gemini-3.x image input — model-family inference, not battery-exercised), `long_context` (gemini 1M-class catalog), `mcp_client` (`call_mcp_tool` observed in init inventory).

## 7. Session lifecycle, resume, cancel, timeout

- **Handle id** is adapter-internal (`agy-<n>`): the provider conversation id only exists after the first `init`/`result` event, so it cannot front `SessionHandle::session_id()`. It surfaces via `Started`/`Finished` payloads and is kept internally for resume.
- **`send_instruction` = a new print run resuming the conversation** (`--conversation <id>`, verified resume path): agy print runs are one-shot, so an instruction clones the first run's invocation template, swaps the objective, and spawns a follow-up run whose events (including its own terminal event) stream on the same session channel. This is a documented deviation from the F-02 "terminal = end of stream" reading: for agy, `Finished` means *turn complete*; the session stays instructable until it `Failed`s or is cancelled. Instructions are rejected with `SessionNotActive` after cancel/failure, with `Internal` while another run is active, and with `Internal` before the conversation id is learned.
- **`cancel`** is process-kill semantics — the stream goes quiet with **no** terminal event. On Windows the child is tree-killed via `taskkill /PID <pid> /T /F` first (handoff §7.7: agy spawns its own children — subagents, browser automation — that a bare `TerminateProcess` would orphan), then the direct kill reaps. `shutdown()` cancels every live session, idempotently.
- **Timeout** is owned by the harness: a watchdog at `spec.timeout_secs` kills the run and classifies it `Transient` (the only retryable class — timeouts clear on their own). The CLI receives `--print-timeout` = budget **+ 30 s slack** so the harness watchdog always fires first; letting the CLI die first would classify as an un-final-evented exit (`SpawnFailure`), misdiagnosing a timeout. `timeout_secs == 0` disables both and takes the CLI's 5 m default.

## 8. Usage / ledger mapping

agy reports tokens only — **no cost field exists** (OBS-04: label estimates only where they exist).

| `usage` field | `UsageSnapshot` field |
|---|---|
| `input_tokens` / `output_tokens` / `thinking_tokens` / `cache_read_tokens` | direct (each optional-tolerant) |
| `total_tokens` | direct; falls back to the field sum when absent |
| — (none exists) | `cost_usd: None` |
| — (single-model CLI, no breakdown) | `per_model: []` — even though the catalog spans three vendors, one print run bills exactly one model; per-model attribution belongs to the caller's model choice, not the payload |
| core agent prompt (fixed) | `session_overhead_tokens = 37_000` (`AGY_SESSION_OVERHEAD_TOKENS`; observed ~37k, `--disable-slash-commands` A/B: 37133→37136 — core prompt, not user skills) |

## 9. Model catalog — multi-provider routing implications

`agy models` (free) lists **three upstream vendors behind one CLI**: gemini-3.7/3.6/3.5-flash × (low/med/high effort), gemini-3.1-pro, claude-sonnet-4-6, claude-opus-4-6-thinking, gpt-oss-120b. Auth is the Antigravity account (no API key). Implications:

- The `--model` passthrough is the routing knob: an "agy worker pool" can in effect be a mixed Claude/Gemini/OSS pool (PRD Appendix A pool model).
- F-06 budget ledger: agy rows are token-only; cost estimates, when needed, must be labeled as estimates derived from the model in use.
- OR-01 orchestrator pool candidates observed through agy include claude-opus-4-6-thinking and gemini-3.1-pro(high).
- Routing across vendors on one account couples their quota/backoff state: F-06's provider-global semaphore must key on the *adapter*, not the upstream vendor, for agy workers.

## 10. Test evidence (fresh output, same session as this PR)

Commands (custom target dir avoids lock contention with parallel PRs):

```
CARGO_TARGET_DIR=target/f05 cargo test -p agentos-adapters agy
CARGO_TARGET_DIR=target/f05 cargo clippy -p agentos-adapters --all-targets -- -D warnings
```

Result: **16 passed; 0 failed; 1 ignored** (the e2e probe), clippy clean under `-D warnings`, `rustfmt --check` clean. Tail:

```
running 17 tests
test agy::tests::e2e_free_probes_version_and_models_catalog ... ignored, live agy invocation (free probes only): set AGENTOS_AGY_E2E=1 to run
test agy::tests::arg_builder_always_grants_workspace_via_add_dir ... ok
test agy::tests::arg_builder_maps_timeout_to_print_timeout_with_backstop_slack ... ok
test agy::tests::stream_reducer_learns_conversation_id_from_init_before_result ... ok
test agy::tests::arg_builder_maps_write_intent_to_mode_from_allowed_paths ... ok
test agy::tests::json_parser_tolerates_noise_around_the_result_object ... ok
test agy::tests::arg_builder_uses_equals_form_print_and_prefers_stream_json ... ok
test agy::tests::json_parser_happy_path_maps_success_result ... ok
test agy::tests::arg_builder_passes_model_effort_schema_and_conversation_flags ... ok
test agy::tests::timeout_maps_to_transient_failure ... ok
test agy::tests::usage_mapping_carries_agy_fields_and_overhead_constant ... ok
test agy::tests::json_parser_error_status_preserves_usage_and_classifies_task_failure ... ok
test agy::tests::classification_table_for_agy_run_endings ... ok
test agy::tests::arg_builder_renders_full_vector_in_canonical_order ... ok
test agy::tests::adapter_id_and_capability_surface_reflect_observed_agy ... ok
test agy::tests::stream_reducer_skips_unknown_and_malformed_lines ... ok
test agy::tests::stream_reducer_maps_init_step_and_result_events ... ok

test result: ok. 16 passed; 0 failed; 1 ignored; 0 measured; 25 filtered out; finished in 0.00s
```

**Test corpus policy (hard rule honored):** no frozen agy transcript corpus exists on disk, so every parser fixture is **synthetic** — built from the documented observed schema in the handoff ADDENDUM and clearly marked as synthetic in the test module. No test makes a billable call or spawns the real binary; the only live invocations anywhere are the free `--version` and `agy models` probes inside the `#[ignore]`d test, doubly gated behind `AGENTOS_AGY_E2E=1` (skipped by default). To run it: `AGENTOS_AGY_E2E=1 cargo test -p agentos-adapters agy -- --ignored`.

Coverage map: arg builder (equals-form `--print`, `--add-dir` always present + dedup, mode mapping, model/effort/schema/conversation flags, timeout+slack, full canonical vector), json parser (happy path, ERROR-with-usage, noise tolerance), stream parser (init/step/result mapping, unknown+malformed skipped, alt envelope, early conversation id), usage mapping (field sums, 37k overhead, cost `None`, `per_model` empty), classification table (7 observed endings incl. the agy ERROR shape and the denial event), timeout → Transient.

## 11. Deviations, assumptions, and open smoke items

Per the honesty boundary (handoff §8): everything below the observed battery is prior-based and must be smoke-tested on the first live run.

1. **`--print-timeout` unit** — the canon gives the default (5 m) but not the unit; the adapter passes integer seconds (300 = 5 m being the natural reading). Verify on first live run.
2. **stream envelope field** — kind read from `type` with `event` fallback; the observed canon pins event *names*, not the envelope key. Synthetic fixtures use `type`.
3. **`step_update` surface** — only `text_delta` + per-step `usage` are projected; `state` (and any tool-step sub-shape) is tolerated and skipped until a live transcript pins it. agy tool steps therefore do not yet map to `ToolUse`.
4. **`send_instruction` post-`Finished`** — deliberate deviation (§7): the session stays instructable after a turn completes, resuming via `--conversation`.
5. **Write-intent proxy** — `allowed_paths` emptiness until F-02 grows a real field (§2).
6. **`multimodal` / `long_context`** — model-family inference (gemini-3.x image input, 1M-class context), not battery-exercised.
7. **auth `Ready` heuristic** — state-dir existence implies prior use; the authoritative free check (`agy models`) is e2e-gated. `Unknown` is reported rather than guessing `NeedsLogin`.
8. **`--sandbox`** — present in the CLI, untested in the battery; the adapter does not set it (F-10 decides the sandbox model; harness/worktree isolation is the enforced boundary meanwhile).
9. **gemini-cli disposition** — F-05 was originally specced for Gemini CLI; the free tier is hard-dead (`IneligibleTierError`), so per the user pivot this adapter targets agy. RT-04's "detect signed-in Google account state" maps to agy auth; "quota errors" map to agy's `status`/`error` fields.
