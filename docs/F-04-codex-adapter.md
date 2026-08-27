# F-04 — Codex adapter

Status: implemented (`crates/agentos-adapters/src/codex.rs`), wired into the daemon's `AdapterSet` as adapter id `codex`.

Everything below is **observed on this machine, 2026-08-22**, with `codex-cli 0.149.0` on a ChatGPT account, default model `gpt-5.6-terra`. The transcripts are frozen in `cli-codex-output/2026-08-22T16-53-43-000Z/fixtures/` (12 probes + `digest.json`) and the parser tests run against them, so nothing here is inferred from documentation.

> Historical probe note: this account rejected `gpt-5.6-sol` during the 2026-08-22 fixture run, so those frozen transcripts use `gpt-5.6-terra`. The current desktop catalog exposes the full supported Codex surface — `gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`, `gpt-5.5`, and `gpt-5.4` — while final availability remains provider/account-specific at spawn time.

## 1. Surface

`codex exec --json` is the driven surface. The interactive TUI and the experimental `app-server` are out of scope.

| Concern | Decision |
|---|---|
| Prompt delivery | **stdin**: argv ends with `-`, the objective is written to the child's stdin and the pipe is closed (fixture P11). Removes argv quoting and leading-dash hazards entirely. |
| Working root | `--cd <workspace>` *and* the child's cwd on a fresh run. |
| Write grants | `--sandbox` + repeatable `--add-dir`. |
| Sandbox derivation | Empty `SpawnSpec::allowed_paths` → `read-only`; non-empty → `workspace-write`. `danger-full-access` is never derived — it must be requested explicitly. Mirrors the F-05 agy mapping. |
| Resume | `codex exec resume <thread_id>` — **preserves the thread id and the context** (P10a/P10b: BANANA42 round-trip). |
| Structured output | `--output-schema <file>` constrains the **final `agent_message`** itself; there is no separate structured item, so the adapter parses the last message (P7). |
| Timeout | `codex exec` has no timeout flag: the harness watchdog is the only budget, and a timeout classifies as `Transient` (retryable). |
| Isolation | `isolated_home` redirects `USERPROFILE`, `HOME`, **and `CODEX_HOME`** (`<home>/.codex`) — codex keeps auth/config there. |

### The resume flag trap

`codex exec resume` has a **narrower** flag set than `codex exec`: no `--sandbox`, no `--cd`, no `--add-dir` (verified — passing `--sandbox` exits 2 with "unexpected argument"). The adapter therefore renders resume runs as:

```
codex exec resume <thread_id> --json --skip-git-repo-check -c sandbox_mode="<mode>" [--model M] [--output-schema F] -
```

with the working root carried by the process cwd. A test pins this so nobody "tidies" the flags back in.

## 2. Event mapping

| codex stream | `AdapterEvent` |
|---|---|
| `thread.started{thread_id}` | `Started{session_id: thread_id, model: None}` — codex never reports the resolved model in-stream |
| `turn.started` | *(nothing)* |
| `item.started{command_execution}` | `ToolUse{tool: "command_execution", args_summary}` — started/completed are a pair, so only the start emits (no double-count) |
| `item.completed{agent_message}` | `TextDelta(text)` **plus** `decision::from_text(text)` |
| `item.completed{todo_list}` | `ToolUse{tool: "todo_list", args_summary: "<n> plan items"}` |
| `item.completed{error}` | *(nothing — non-fatal notices: hook-timeout clamping, skills-budget overflow; logged at debug)* |
| `turn.completed{usage}` | `UsageUpdate` |
| `turn.failed{error{message}}` / top-level `error{message}` | recorded; surfaces in the terminal `Failed` |
| *(process exit)* | `Finished` / `Failed` per §3 |

**All shell work — file writes included — arrives as `command_execution`.** codex writes through PowerShell `Set-Content` rather than a patch item; no `file_change` item appeared in any observed run.

### `todo_list` is not a decision

codex's plan surface is progress reporting, not a human gate. Mapping it to `AdapterEvent::Decision` would block every run on a click each time the model updates its plan. It maps to `ToolUse`, and a test asserts no `Decision` comes out of the plan fixture.

### Decisions ride the text protocol

`codex exec` exposes **no asking tool**, and it *rejects* `--ask-for-approval` outright (exit 2 — fixture P4); approvals exist only in the interactive TUI / app-server. So codex asks the way agy does (F-02 `Decision`, F-05 §3b): the model emits a fenced ```json `{"ask": {...}}` block, and `decision::from_text` lifts it out of the `agent_message`. Fixture P5 is a real turn doing exactly that, parsed under test. Emitting side: the `decision-protocol` skill (F-13).

## 3. Classification (F-00 §4)

Success is the conjunction **exit 0 + a `turn.completed` + no failure message**. Otherwise the shared `Classifier` ladder runs with the typed provider code parsed out of the failure payload — codex failure messages are themselves JSON (`{"type":"error","status":400,…}`), so 401/403 → `AuthFailure` and 429 → `Transient` come for free, no stderr reading.

| Observed death | Signal | Class |
|---|---|---|
| Rejected model (P9) | exit 1, `error` + `turn.failed`, embedded status 400 | `TaskFailure`, provider's words preserved in the detail |
| Unknown flag (P4) | exit 2, **no events at all** | `SpawnFailure` |
| Harness budget exceeded | watchdog fires, tree-kill | `Transient` (retryable) |
| Cancel | tree-kill, stream goes quiet | *no terminal event* (process-kill semantics, as F-05) |

stderr is drained for debug logging only — codex stderr is wall-to-wall skill-YAML load errors and hook noise, exactly the kind of text F-00 §4 forbids classifying on.

## 4. Usage

`turn.completed.usage` carries `input_tokens`, `cached_input_tokens`, `cache_write_input_tokens`, `output_tokens`, `reasoning_output_tokens`. **Token-only — no cost field**, so `cost_usd` stays `None` (OBS-04). The cached count is a *subset* of `input_tokens` (21 437 input / 11 008 cached observed), so `total_tokens = input + output`; adding cached would double-count.

`CODEX_SESSION_OVERHEAD_TOKENS = 21_000`: a trivial prompt ("Reply with exactly: PROBE_OK") billed 21 437 input tokens — the fixed instruction + skills preamble, not the user's words.

## 5. Capabilities

`filesystem_edit` (P6), `shell` (P2), `structured_output` (P7), `resume` (P10), `multimodal` (`-i/--image` on both exec and resume), `long_context`, `mcp_client` (`codex mcp`). **`network: false`** — no web/browser tool appeared in any observed exec run and `codex exec` exposes no search flag, so it is not claimed.

Per-tool allow/deny does not exist here (the surface is sandbox modes plus `.rules` execpolicy files) — the same gap agy has, and F-10's harness-side gating compensates.

## 6. Test evidence

- **14 offline tests** over the frozen real transcripts: arg builder (fresh/resume/isolation), event mapping per fixture, classification per death, structured output, typed-code lifting, and schema-drift tolerance.
- **1 live billable test**, `#[ignore]`d behind `AGENTOS_CODEX_E2E=1`: `e2e_session_streams_and_resumes` — starts a real session, asserts the streamed answer, then `send_instruction` (a resume run) and asserts the earlier token is recalled. Ran green in 25.2 s on 2026-08-22, proving spawn → stdin delivery → stream mapping → thread capture → resume → terminal event end to end.
- **1 free live probe**, also `#[ignore]`d: `detect()` reports the CLI version.

## 7. Known gaps

- **No approval/decision events.** Structural, not an omission: the surface does not exist in `codex exec`. Revisit only if the adapter is ever moved onto `app-server`.
- **Model is not reported in-stream**, so `Started.model` is `None` even when `--model` was passed; the invocation knows it, the stream does not.
- **`resume --last` picks the newest recorded session**, which may not be this session's thread (observed: a different `thread_id` came back). The adapter therefore always resumes by explicit thread id, never `--last`.
- **Skills-budget overflow on every run**: `Exceeded skills context budget. All skill descriptions were removed and ~1000 additional skills were not included`. A user-environment condition, not an adapter one, but it means codex sessions on this machine start with no skill descriptions visible to the model.
