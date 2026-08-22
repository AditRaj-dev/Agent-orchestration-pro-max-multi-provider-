# F-00 — Conventions, Layout & Canon

Status: LIVING DOCUMENT — every subsequent F-doc and PR attaches or references this.
Sources of truth, in order: (1) observed runtime canon (`../handoff.md` §3 + addenda),
(2) this document, (3) `../Agent_Engineering_OS_PRD_v1.0.md`. Priors from training data
are never a source of truth for CLI behavior — the probe scripts are the smoke tests.

## 1. Stack (PRD §20)

| Layer      | Choice                                             |
|------------|----------------------------------------------------|
| Daemon     | Rust + Tokio, named-pipe/UDS IPC + WebSocket       |
| Desktop    | Tauri 2 + React + TypeScript (client of daemon)    |
| State      | SQLite (WAL mode), content-addressed artifact dir  |
| Parsing    | tree-sitter (+ LSP where useful), rebuildable cache|
| Git        | gix/libgit2 + `git` CLI fallback                   |
| Isolation  | Git worktrees per write-task; Docker optional      |
| Secrets    | OS keychain only; never plaintext, never transcripts|
| MCP        | Interface-only; never the state store              |

## 2. Repository layout

```
agent-engineering-os/
├── F-00-CONVENTIONS.md        # this file
├── Cargo.toml                 # workspace
├── crates/
│   ├── agentos-daemon/        # F-01: supervision, event journal, RPC/WS, SQLite
│   ├── agentos-workflow/      # F-06: engine, scheduler, leases, budgets
│   ├── agentos-adapters/      # F-02..F-05: RuntimeAdapter trait + provider impls
│   └── agentos-git/           # F-09: worktrees, queue, ledger
├── apps/desktop/              # F-11: Tauri command center
├── docs/                      # F-docs, ADRs; PRD lives one level up (../)
└── fixtures/                  # frozen probe transcripts = adapter test corpus
```

## 3. Event rules (PRD §18.2 — binding)

- Append-only; corrections are new events, never mutations.
- Every event carries `run_id` where applicable and `trace_id` for correlation.
- Consumers are idempotent (retries may re-deliver).
- Large payloads → content-addressed artifacts; events hold refs + hashes.
- UI state is a projection, never a second source of truth.

## 4. Provider-adapter canon (observed, machine-specific — handoff §3 + addenda)

| Rule | Origin |
|---|---|
| Classify runs by exit code + presence of final result event — never stderr text | codex skill-noise, agy plan-mode SUCCESS |
| Variadic flags: equals form (`--flag=value`) or stdin-delivered prompts | claude T1/T3, agy `--print='…'` |
| Claude: `total_cost_usd` only; per-model rows in `modelUsage`; init = `type=system && subtype=init` | claude probe |
| Claude policy: `--allowedTools=Bash(echo:*)` / `--disallowedTools=Bash,WebFetch` (comma list, equals form) | claude T2/T4 |
| agy: writes are virtualized unless dir passed via `--add-dir`; resume via `--conversation <id>`; `--mode plan` = enforced read-only | agy battery |
| agy contract: `{conversation_id,status,response,usage{...},structured_output}`; stream events `init/step_update/result` | agy battery |
| codex: `--output-schema` takes a FILE path; requires git cwd or `--skip-git-repo-check`; stdin must be closed; model must be overridden on this machine (`-m gpt-5.5`; config default `gpt-5.6-sol` is rejected for ChatGPT auth) | codex C-series |
| zcode: typed `ProviderBusinessError{providerCode,…}` — billing(1113)/bot-gate(3007) distinguishable; config = `~/.zcode/cli/config.json`, ref `providerId/modelId` | zcode investigation |
| Fixed per-session preamble ≈ 22k tok (claude) / 37k tok (agy) — core prompt, not user skills; budget ledger carries a per-session overhead line | claude T5, agy A/B |
| Orchestrator model: `claude-opus-5` via Claude Code (`opus-5` 404s; alias `opus` ok) | opus probe |

## 5. Working agreements

- One F-doc = one PR. Each PR carries its smoke test (probe scripts are the CLI smoke tests).
- Windows is the reference platform; POSIX paths in code, junctions not symlinks for redirects.
- Health checks never make billable calls.
- Every "done" needs fresh command output in the same message (evidence before claims).
- Secrets (e.g. BigModel keys) live in keychain/env — never in repo files or transcripts.

## 6. Provider-mix status at scaffold time (2026-08-22)

| Provider | Adapter | Status |
|---|---|---|
| Claude Code v2.1.238 | F-03 | verified end-to-end (T1–T5 + opus-5) |
| Antigravity `agy` v1.1.18 | F-05 | verified end-to-end (9-call battery) |
| Codex v0.149.0 | F-04 | contract known; runtime smoke deferred to quota reset (Sep 10); build against `-m gpt-5.5` |
| zcode v0.16.3 (GLM-5.3) | F-05b | headless pipeline verified through billing gate; awaiting account recharge |
| Mock | F-02 | credential-free e2e from day one |
