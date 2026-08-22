# HANDOFF — Agent Engineering OS: BUILD PHASE

**Date:** 2026-08-22 · **From:** investigation/canonicalization session · **To:** build session
**Repo:** `D:\OP\agent-engineering-os` · **Working dir for everything else:** `D:\OP`

You are continuing the build of the **Agent Engineering OS** (local-first desktop orchestrator
for multi-agent coding teams: Rust daemon + Tauri desktop, SQLite WAL state, CLI-provider
workers). The investigation phase is CLOSED. Everything you need is on disk — no prior
conversation required.

## 0. Files that are canon (read in this order before writing code)

1. `agent-engineering-os/F-00-CONVENTIONS.md` — stack, repo layout, event rules (PRD §18.2),
   provider-adapter canon table, working agreements. EVERY decision there is binding.
2. `D:\OP\Agent_Engineering_OS_PRD_v1.0.md` — full PRD (2302 lines, complete — §1–§25 incl.
   appendices). §22.4 is the build order; §18 data model; §19 MCP surface; §21 NFRs.
3. `D:\OP\handoff.md` — investigation log with ALL observed CLI findings (read §3 + addenda
   when writing an adapter; skim otherwise).

## 1. Decisions already made (do not re-litigate)

| Decision | Value |
|---|---|
| Orchestrator model | `claude-opus-5` via Claude Code (`opus` alias ok; `opus-5` slug 404s) — verified |
| F-12 orchestrator shape | mastermind three-process pattern: opus-5 commands → cheap workers code → sonnet-class reviews; user gates; evidence-before-claims |
| Provider mix | Claude (F-03) ✅ · agy/Antigravity (F-05) ✅ · Codex (F-04) contract known, runtime smoke deferred to quota reset Sep 10 — build against `-m gpt-5.5` · zcode/GLM-5.3 (F-05b) headless verified through billing gate, awaits BigModel recharge · Mock adapter (F-02) from day one |
| Gemini CLI | REJECTED — free tier dead (IneligibleTierError). Antigravity `agy` replaces it (user decision) |
| Memory/interim state | memex (`D:\new\memex`, fixed for concurrency this session) is the dogfooding memory until the daemon journal replaces it |
| Toolchain | Rust 1.94.1, node 25.9.0, git — all present, Windows reference platform |

## 2. Exact repo state right now

```
agent-engineering-os/
├── F-00-CONVENTIONS.md          # written ✅
├── Cargo.toml                   # workspace ✅ (member: agentos-daemon)
├── crates/agentos-daemon/
│   ├── Cargo.toml               # ✅ (tokio, rusqlite bundled, serde, uuid v7, thiserror, tracing)
│   └── src/                     # EMPTY — no .rs files yet; cargo check will fail until F-01 lands
└── docs/HANDOFF-BUILD.md        # this file
```
Nothing committed to git yet (repo not `git init`-ed — do that as step 0 with the user's blessing).

## 3. Immediate next steps (in order)

1. `git init` the repo; initial commit of scaffold.
2. **F-01 daemon core skeleton**: `main.rs` (tokio runtime, graceful ctrl-c), `events.rs`
   (Event record per PRD §18.1 + append-only `events` table), SQLite open helper implementing
   the F-01 canon below. Target: `cargo check` green, one integration test appending events.
3. Then PRD §22.4 order: F-02 supervisor + `RuntimeAdapter` trait + `MockAdapter` → F-03
   Claude adapter (fullest observed contract — see canon table in F-00) → F-06 workflow engine.
4. F-docs from session 1 (F-00→F-11) were delivered in chat but are NOT on disk — regenerate
   per-PR as you build, from PRD + F-00 + handoff canon. F-12→F-16 unblocked (PRD ingested,
   opus-5 chosen).

## 4. F-01 SQLite canon (learned from memex race bug — binding)

- `busy_timeout` on EVERY connection; never re-issue `PRAGMA journal_mode` unconditionally
  (read it first — journal_mode returns SQLITE_BUSY without honoring busy_timeout);
  explicit transactions around multi-table writes; surface SQLITE_BUSY as retryable, not
  generic failure. Event log is append-only (PRD §18.2); UI state is a projection.

## 5. Working rules (hard-won, non-negotiable)

- Evidence before claims: no "done/passing" without fresh command output in the same message.
- Observed-truth beats priors and beats the PRD (F-00 §4 canon table is the distilled form).
- Health checks never make billable calls. Classify CLI runs by exit code + final result
  event, never stderr text. Variadic flags → equals form. Secrets never in repo/transcripts.
- One F-doc = one PR; each PR carries its smoke test.
- User gates between phases (mastermind pattern): never start phase N+1 unilaterally.

## 6. Open user actions (context, not blockers for F-01/F-02)

- Codex quota resets **Sep 10, 2:04 AM** → then rerun `cli-fix.js` codex battery with `-m gpt-5.5`.
- zcode: recharge BigModel key account → rerun `zcode --prompt='Reply with exactly ZCODE_OK...'`
  (config already wired: `~/.zcode/cli/config.json`, `builtin:bigmodel/GLM-5.3`).
- Probe scripts + frozen fixture transcripts live in `D:\OP\cli-*-output\*/` — freeze as the
  adapter test corpus when F-03/F-04/F-05 land.
