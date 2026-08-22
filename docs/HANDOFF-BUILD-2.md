# HANDOFF — Agent Engineering OS: BUILD PHASE 2 (post-backend-MVP)

**Date:** 2026-08-22 · **From:** build session 2 (waves 0–3) · **To:** next build session
**Repo:** `D:\OP\agent-engineering-os` (git on `main`, tree clean) · **Scratch dir:** `D:\OP`

You are continuing the build of the **Agent Engineering OS** (local-first desktop orchestrator
for multi-agent coding teams: Rust daemon + Tauri desktop, SQLite WAL state, CLI-provider
workers). The investigation phase and the backend-MVP build phase are CLOSED. Nine F-slices are
implemented, tested, and committed. No prior conversation required — everything is on disk.

## 0. Canon files (read in this order before writing code)

1. `agent-engineering-os/F-00-CONVENTIONS.md` — stack, layout, event rules, provider canon. BINDING.
2. `docs/HANDOFF-BUILD.md` — previous-phase handoff (SQLite canon §4, working rules §5). Still binding.
3. `D:\OP\Agent_Engineering_OS_PRD_v1.0.md` — full PRD (2303 lines). §22.4 build order; §23.3 release gates.
4. `D:\OP\handoff.md` — observed CLI ground truth (Claude/agy/zcode contracts). Read when touching adapters.
5. `docs/F-*.md` — per-crate design docs written by the build agents. Read the relevant one before
   modifying its crate.

## 1. Exact repo state

Workspace: **8 crates, 255 tests passing, clippy `-D warnings` clean** (verified 2026-08-22,
fresh `cargo test --workspace` run). Git history (oldest → newest):

```
1bf9483 scaffold: workspace + 5 crates, F-00 conventions, build handoff
9226a88 core: shared canon types — events, task lifecycle, error taxonomy
9c351b0 F-01: daemon core — SQLite WAL journal, append-only events, binary
e888caa F-02: RuntimeAdapter trait, event/failure taxonomy, MockAdapter
5ca9317 F-09: git manager — worktrees, mutation queue, agent ledger, ownership
69d6866 adapters: pre-wire claude/agy modules + tokio process feature
f572ca0 F-06: workflow engine — DAG validation, durable store, lease scheduler, budgets
14852f9 F-03: Claude Code adapter — fixture-tested parser, equals-form args, stdin prompts
7aed4d9 F-05: Antigravity agy adapter — stream/json parser, --add-dir workspaces, policy-gap docs
e440125 scaffold: pre-wire runtime/context/policy crates + blake3 dep
1f90784 F-07: runtime supervisor — contracts, handoff packets, e2e wiring, usage ledger
cb7359e F-08: context system — blake3 file index, context graph, role packs, invalidation
8899bc1 F-10: policy engine — permissions, fingerprint-bound approvals, secrets interface, audit
```

### Crate map

| Crate | F-doc | What it does | Tests |
|---|---|---|---|
| `agentos-core` | — | Event/EventType (18 canon names + open `Other`), TaskState + `can_transition`, Priority, CoreError (`SqliteBusy` = only retryable) | 21 |
| `agentos-daemon` | F-01 | SQLite open helper (canon), append-only `events` journal (trigger-enforced), binary | 5 |
| `agentos-adapters` | F-02/F-03/F-05 | `RuntimeAdapter` trait, `AdapterEvent` stream, failure taxonomy + Classifier (exit-code + final-event, never stderr), `MockAdapter`; `claude.rs` (fixture-tested vs real T1–T5 transcripts), `agy.rs` (synthetic fixtures, `--add-dir` rule) | 58+ |
| `agentos-workflow` | F-06 | WorkflowSpec DAG + validation, durable task/run store (CAS transitions), lease scheduler with heartbeats/expiry/budgets, `TaskExecutor` trait, `WorkflowEngine` | 29 |
| `agentos-git` | F-09 | git-CLI wrapper (argv vectors), worktrees `agentos/<run8>/<task8>`, single-consumer mutation queue (stale-base, push-approval-gated), agent ledger, ownership map | 28 |
| `agentos-runtime` | F-07 | Full TaskContract (App. B) + HandoffPacket (App. C, refs-only), usage ledger (per-model + session overhead), Supervisor composing everything; **5 real e2e tests** (happy path ends in an actual commit sha + Appendix-E event sequence; flaky retry; crash recovery; budget escalation; ownership conflict) | 35 |
| `agentos-context` | F-08 | blake3 file index (mtime fast path + `verify_all`), provenance-enforced context graph, 4-state invalidation cascade, role pack compiler with strict budgets + manifests | 23 |
| `agentos-policy` | F-10 | Fail-closed PermissionSets, compile→SpawnConstraints, fingerprint-bound approval gates (mutation/expiry invalidate), secrets interface (redact+zeroize, keychain seam), tamper-proof audit + export | 52 |

Only ignored tests: two env-gated free e2e probes (`AGENTOS_CLAUDE_E2E=1`, `AGENTOS_AGY_E2E=1`).
**Tests never make billable calls** — parsers are tested against frozen/synthetic fixtures.

## 2. Decisions already made (do not re-litigate)

| Decision | Value |
|---|---|
| Orchestrator model | `claude-opus-5` via Claude Code (alias `opus` ok, `opus-5` 404s) — verified |
| Orchestrator shape (F-12) | mastermind three-tier: opus-5 commands (never writes code) → cheap workers code → sonnet-class reviews; user gates between phases |
| Providers | Claude ✅ · agy ✅ · codex contract known, runtime smoke Sep 10 (`-m gpt-5.5`) · zcode F-05b blocked on BigModel recharge · Mock ✅ |
| Gemini CLI | REJECTED (free tier dead). agy replaces it |
| Event serde | camelCase JSON keys; EventType dotted snake_case; TaskState snake_case; unknown → `Other` (total parse) |
| Adapter failure classification | exit code + final-event presence + typed provider codes (zcode 1113/3007) — never stderr text |
| Token accounting | bytes/4 approximation (documented, no false precision); session overhead constants 22k (claude) / 37k (agy) |

## 3. Build method that worked (reuse it)

- **Orchestrator (main session) writes no crate logic.** It: verifies toolchain, pre-wires new
  crates into the workspace (Cargo.tomls + placeholder lib.rs + members + any new workspace
  deps), commits the scaffold, then dispatches self-contained build agents and integrates.
- **Dispatch ≤3 agents in parallel** (concurrency limit; a 4th dispatch bounces with
  `user concurrency limit exceeded` — just retry it after a slot frees). Each agent gets: exact
  file scope (own crate + own F-doc ONLY, no root Cargo.toml, no git), canon reading list,
  deliverables, and required fresh verification output.
- **Each agent uses its own `CARGO_TARGET_DIR=target/<name>`** to avoid cargo lock contention.
- **Integration by orchestrator:** scoped `cargo test -p ...` over the touched crates, then
  workspace-wide test + clippy, then **one commit per F-slice** (path-scoped `git add`, never
  `-A` when a parallel session might be writing).
- Working rules (binding, from HANDOFF-BUILD §5): evidence before claims; observed-truth beats
  priors; no billable calls in tests; one F-doc = one PR; user gates between phases.

## 4. Known debts / seams (ordered by priority)

1. ~~**F-09 branch-name collision**~~ — FIXED: shorts now come from the UUID tail
   (`worktree.rs::short_hex`), F-07's byte-swap shim removed.
2. ~~**F-07 GitGate hardcodes `approved=true`**~~ — CLOSED: policy wired into the supervisor
   (approvals, permission compile, audit). Follow-ups left open: `Gate::GitCommit` in
   `agentos-policy` (commit approvals currently ride `GitPush`), one-time approval
   consumption, and `SecretsBroker` into the spawn env.
3. **Reviewer is a deterministic stub** in F-07 (approve iff no unresolved + tests pass). Real
   reviewer pool (sonnet-class via adapters) is F-13 territory.
4. `agentos-policy` keychain backend is an interface (EphemeralBroker only); OS-keychain impl pending.
   (Its other debts — `Gate::GitCommit`, one-time approval consumption — are closed.)
5. Host-level network enforcement (SEC-02) is policy-modeled, not proxy-enforced yet.
6. Context symbols are a line-scanner stub; tree-sitter is the documented seam.
7. Orchestrator (F-12, opus-5) not built yet — the Claude adapter (F-03) is its substrate.

## 5. Next steps (suggested order)

1. **Small PR:** F-09 branch-name fix (debt #1).
2. **F-10→F-07 wiring PR:** approvals + permission compile into the supervisor (debt #2). Proves
   PRD §23.3 gate end-to-end through the real git queue.
3. **F-12 orchestrator:** PlanOperation commands (create_task/add_dependency/assign_pool/
   request_review/escalate/close_goal) validated by the workflow engine; model = claude-opus-5
   through `ClaudeAdapter`; engine stays authoritative (invalid commands rejected with
   machine-readable reason; runs continue if orchestrator is down).
4. **F-11 desktop:** Tauri 2 + React/TS shell consuming the daemon journal/events (UX-01..UX-05).
   Biggest slice; consider splitting: (a) daemon IPC/WS surface, (b) UI.
5. **When quotas clear:** codex battery rerun (Sep 10, patch `-m gpt-5.5` into `D:\OP\cli-fix.js`)
   → F-04 adapter; zcode recharge → rerun probe → F-05b adapter (taxonomy already in F-02).

## 6. Open user actions (context, not blockers for 1–4)

- Codex quota resets **Sep 10, 2:04 AM** (or upgrade).
- zcode: recharge BigModel key → rerun `zcode --prompt='Reply with exactly ZCODE_OK...'`
  (config wired: `~/.zcode/cli/config.json`, `builtin:bigmodel/GLM-5.3`).
- Probe corpora frozen at `D:\OP\cli-*-output\*/` — the adapter test corpus (claude T1–T5 already
  consumed by F-03's tests).

## 7. Honesty boundary

Everything above reflects fresh command output from 2026-08-22 (test counts, clippy, git log).
Provider behavior claims trace to `D:\OP\handoff.md` observations, not priors. If a provider CLI
was updated since, re-run the relevant probe before trusting an adapter flag.
