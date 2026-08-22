# F-11 — Desktop command center (daemon WS surface + Tauri/React shell)

Status: CONTRACT — sections 2–5 are binding for both build halves; drift is a bug.
Split (per HANDOFF-BUILD-2 §5.4): **(a)** daemon WebSocket API in `agentos-daemon`,
**(b)** desktop app in `apps/desktop`. This document is the interface between them.

## 1. Scope and non-goals

In scope:

- (a) A read-only WebSocket JSON API over the F-01 journal: paged reads, gapless
  subscribe (replay + live tail), and **projections** (runs/tasks/agents folded from
  events — F-00 §3: "UI state is a projection, never a second source of truth").
- (b) A Tauri 2 + React/TS shell consuming only that API (UX-01..UX-05 surface,
  UX-06-lite inbox), dark command-center design per the frozen mockups in `D:\OP\images`.

Non-goals (documented seams, later slices): write-path methods (approve/resolve live in
the policy store, not the journal), full git diff plumbing (`git.diff` returns
`not_supported` in v1 — UX-05 renders event-carried attribution until then), daemon-hosted
supervisor/orchestrator runs, auth beyond loopback.

## 2. Transport, framing, addressing

- WebSocket (RFC 6455) at `127.0.0.1:8741` by default; `AGENTOS_WS_ADDR` overrides
  (`host:port`). Bind loopback only. No auth in v1 — loopback is the boundary; token
  handshake is the documented seam before any non-loopback bind.
- One JSON object per TEXT frame, UTF-8, no binary frames.
- Requests carry a client-chosen `id` (u64 or string); responses echo it. Frames with no
  `id` are notifications (server → client only).
- Concurrency: requests may arrive on interleaved subscriptions; each connection is
  handled independently; handlers are idempotent and never block on a slow subscriber
  (per-connection outbound buffer; a subscriber that falls irrecoverably behind gets
  `subscription.closed` with `reason:"slow_consumer"` and must resubscribe from a seq).

### 2.1 Requests

```json
{"id": 7, "method": "events.list", "params": {"afterSeq": 0, "limit": 200}}
```

### 2.2 Responses

```json
{"id": 7, "ok": true, "result": { /* per method */ }}
{"id": 7, "ok": false, "error": {"code": "invalid_params", "message": "limit must be <= 1000"}}
```

Error codes: `invalid_request` (unparseable frame / no method), `method_not_found`,
`invalid_params`, `internal_error`, `not_supported` (specified but not built, e.g.
`git.diff` v1). Malformed frames get `id: null` in the error response.

### 2.3 Notifications

```json
{"notification": "event", "subscriptionId": "sub-3", "seq": 41, "event": { /* §3.1 */ }}
{"notification": "subscription.closed", "subscriptionId": "sub-3", "reason": "unsubscribed"}
{"notification": "daemon.stopping"}
```

`daemon.stopping` is broadcast before shutdown; the server then closes with code 1001.

## 3. Methods

### 3.1 Event wire shape

The daemon-serialized `agentos_core::Event` (camelCase, exactly the columns
`crates/agentos-daemon/src/events.rs` stores) plus the journal `seq`:

```json
{"seq": 41, "id": "<uuid>", "eventType": "task.running", "occurredAt": "<rfc3339>",
 "runId": "<uuid>|null", "traceId": "<uuid>|null", "taskId": "<uuid>|null",
 "agentId": "<string>|null", "payload": {}, "payloadRef": null, "payloadHash": null,
 "schemaVersion": 1}
```

`eventType` strings: the 18 canon names in `agentos-core/src/event.rs` (dotted
snake_case; unknown strings arrive as `Other` and pass through verbatim) plus the
supervisor's `Other` vocabulary (`session.spawn`, `session.started`, `agent.tool_use`,
`agent.rate_limit`, `agent.spawn_failed`, `usage.updated`, `task.failed`,
`handoff.rejected`, `ownership.conflict`, `approval.required`, `approval.granted`,
`approval.denied`, `policy.denied`, `git.gate_failed`, `git.stale_base`, `run.failed`).

### 3.2 Method table

| Method | Params | Result |
|---|---|---|
| `ping` | `{}` | `{"pong": true, "serverTime": "<rfc3339>"}` — health check, never bills |
| `daemon.info` | `{}` | `{"version", "pid", "journalPath", "eventCount", "lastSeq", "startedAt"}` |
| `events.list` | `{afterSeq?=0, limit?=200}` (limit ≤ 1000) | `{"events": [§3.1], "lastSeq": N, "truncated": bool}` |
| `events.subscribe` | `{afterSeq?=0}` | `{"subscriptionId": "sub-N"}`; server replays `seq > afterSeq` as §2.3 `event` notifications **in seq order**, then live tail |
| `events.unsubscribe` | `{subscriptionId}` | `{"stopped": bool}` |
| `runs.list` | `{}` | `{"runs": [RunSummary]}` newest-run-first |
| `tasks.list` | `{runId?}` | `{"tasks": [TaskSummary]}`; omitted runId = all |
| `agents.list` | `{runId?}` | `{"agents": [AgentSummary]}` |
| `git.diff` | `{repo, base, head}` | v1: error `not_supported` (seam; UX-05 falls back to event attribution) |

Replay + tail is gapless and at-least-once per seq: replay reads `seq > afterSeq`; the
tail loop continues from the last delivered seq. Consumers are idempotent (F-00 §3).

Live tail mechanics: poll `events::tail(last_seq, batch)` every 250 ms per subscription
(WAL readers never block writers; external processes appending to the same journal are
the supported write path until the supervisor moves in-process). A cheap
`PRAGMA data_version` short-circuit is allowed but the poll interval is the contract.

Journal access: one shared connection behind a `std::sync::Mutex` (desktop scale; the
API is read-only) or per-task read connections — implementer's choice, documented in
the crate.

### 3.3 Projections (fold)

Folded server-side from the whole journal on demand (journal sizes are thousands of
rows, not millions; no incremental cache required in v1). All ids are strings; states
are the canonical snake_case wire strings (`agentos-core` TaskState, F-06 RunStatus).

```json
RunSummary  = {"runId", "status": "running|completed|failed", "workflowId": "|null",
               "taskCounts": {"total", "done", "failed", "active"},
               "startedAt", "endedAt": "|null", "firstSeq", "lastSeq", "eventCount"}
TaskSummary = {"taskId", "runId", "nodeId": "|null", "state": "<task_state>",
               "attempts": 0, "agentId": "|null", "dependsOn": ["nodeId"],
               "lastEventType", "lastEventAt", "commitSha": "|null",
               "budgetExceeded": false}
AgentSummary = {"agentId", "provider": "|null", "model": "|null",
                "status": "idle|planning|running|waiting|reviewing|blocked|failed|complete",
                "runId": "|null", "taskId": "|null", "lastEventType", "lastEventAt",
                "eventCount": 0,
                "usage": {"costUsd": 0.0, "tokensEstimate": 0}}
```

Fold table (event → effect; anything not listed leaves state unchanged and counts
toward `lastEventType`/`eventCount` — forward compatible by construction):

| Event | Effect |
|---|---|
| `run.created` | create RunSummary(running) |
| `workflow.started` | set run.workflowId (payload `workflowId` if present) |
| `run.completed` / `run.failed` | run status per F-06 derivation (any task failed → failed) |
| `task.created` | create TaskSummary(created); nodeId/taskId from event columns/payload |
| `task.ready` → `task.done` | task state machine exactly as named (`task.created`→created, `task.ready`→ready, `agent.leased`→leased, `task.running`→running, `task.output_ready`→output_ready, `review.requested`→review_pending, `review.approved`→approved, `git.queued`→git_queued, `git.committed`→committed (+commitSha from payload), `task.done`→done) |
| `task.failed` | task state failed |
| `approval.required` with payload `blocking: true` | task state human_required (parked, BUILD-3 semantics) |
| `agent.crashed` | task attempts++ (re-lease follows as `agent.leased`/`task.running`) |
| `budget.exceeded` | task + run budgetExceeded = true |
| `agent.leased` / `task.running` | AgentSummary: leased/running (agent from `agentId`) |
| `session.spawn` | agent status planning |
| `session.started` | agent status running |
| `agent.tool_use` | agent status running (activity ticker) |
| `agent.rate_limit` | agent status waiting |
| `agent.spawn_failed` | agent status failed |
| `review.requested` (reviewer agent) | agent status reviewing |
| `usage.updated` | roll payload cost/tokens into AgentSummary.usage |
| `run.completed` / task terminal | run/task terminal → agent status complete (idle when unset) |

`dependsOn` comes from the `workflow.started` payload node list when present
(`{nodes: [{id, dependsOn: [...]}]}`); omitted otherwise — the UI renders blocked
tasks from state alone when absent.

### 3.4 Demo seeding (dev/QA only)

`agentos-daemon demo-seed --db <path> --fixture <file.json>` appends the frozen event
list in `fixtures/demo-run.json` (the F-07 e2e happy-path sequence: spec → parallel
a/b → build → review → commit → run.completed, with realistic agent ids and usage
events). It **refuses the default journal path** and any path that already contains
events; payloads carry `"demo": true`. Purpose: UI development and acceptance without a
live supervisor and without any billable call. `serve` mode is unchanged by this.

## 4. Desktop app (apps/desktop)

- Tauri 2 shell (`apps/desktop/src-tauri`, excluded from the cargo workspace) +
  Vite + React 18 + TypeScript. The webview app is a **pure client of the daemon**:
  all state arrives over §2/§3; Tauri APIs are used only for chrome (window, folder
  picker). The app must also run in a plain browser against `ws://127.0.0.1:8741`
  (dev mode) — no Tauri-gated state paths.
- Connection: WS client with exponential reconnect (250 ms → 8 s cap), heartbeat
  `ping` every 15 s, and **resubscribe from `lastSeq`** after any reconnect (gapless
  catch-up per §3.2). Connection state is always visible (UX-01 acceptance).
- Store: normalized maps keyed by runId/taskId/agentId + a bounded event ring
  (last 2000); the fold mirrors §3.3 client-side so live notifications update
  projections identically to a fresh snapshot (UI state is a projection — same rule,
  both sides). High-frequency streams (`usage.updated`, `agent.tool_use`) coalesce
  per-agent at ≤ 4 Hz (UX-02: ten sessions stay usable).
- Views (left nav, mockup design language — dark slate `#0F1117`, panels `#161A23`,
  1px `#262B38` borders, 8px radius, accents `#7C5CFF`/`#4F8CFF`, Inter + ui-monospace
  for ids/sha/seq; badges: running `#4F8CFF`, complete `#3FB68B`, failed `#E5484D`,
  waiting `#F5A524`, idle `#6B7280`):
  - **Command Center** (default): agent cards grid (AgentSummary), run strip
    (RunSummary), live activity feed (event notifications, auto-scroll, pause on
    hover). Blocked tasks and their dependency reachable in ≤ 2 clicks (UX-02).
  - **Runs & Graph**: run list → DAG canvas rendered from TaskSummary/`dependsOn`
    (stable node ids = taskId; layered layout; status-colored borders; click →
    detail drawer with contract info from events). Rendered from engine-projected
    state, never from LLM narration (UX-04).
  - **Session**: per task/agent timeline (events filtered by taskId/agentId),
    payload inspector, usage panel, terminal-style stream (throttled).
  - **Review**: `git.committed`/`git.queued` events with task/agent attribution,
    commit sha, changed-file lists from payloads, review scores; full diffs are the
    `git.diff` seam (UX-05 v1 surface).
  - **Settings / project (UX-01 v1)**: daemon address, journal path, connection
    health, project folder picker (Tauri dialog, preference in localStorage).
    Daemon-owned project records are a seam (§1 non-goals).
  - Notification inbox (UX-06-lite): approvals/budget/conflict events surfaced with
    deep-links; quiet mode toggle.
- Tests (vitest): fold parity with §3.3 on synthetic frames, reconnect/resubscribe
  gapless catch-up (fake WS), throttle coalescing. No network in unit tests.
- Gates: `npm install && npm run build && npm test` green; `cargo check` green in
  `src-tauri`. `npm run tauri dev` manual smoke documented in the F-doc addendum.

## 5. Test obligations (both halves)

- No billable calls anywhere (F-00 §5). The daemon API is journal-only; the desktop
  app talks only to the daemon.
- Daemon: in-process server + tokio-tungstenite client e2e (ephemeral port) covering
  ping/info, list pagination, subscribe replay+tail (including events appended by an
  "external" second connection mid-subscription), unsubscribe, error frames, and the
  §3.3 fold against synthetic journals including unknown event types.
- Desktop: §4 unit gates; manual `tauri dev` smoke against `demo-seed` output
  screenshot attached to the F-doc addendum by the integrator.

## 6. Seams left open

1. `git.diff` (full UX-05 diffs) — needs agentos-git in the daemon; error
   `not_supported` in v1.
2. Write-path methods (approve/deny/steer) — belong to the policy store surface, not
   the journal; blocked on daemon-hosted supervisor.
3. Auth token for non-loopback binds.
4. Daemon-owned project records (UX-01 full form).
5. Incremental projection cache (journal-size driven; re-fold is cheap for now).

## 7. F-11a implementation addendum

Daemon half (a), built in `crates/agentos-daemon`: `src/server.rs` (WS API),
`src/projection.rs` (§3.3 fold), `src/seed.rs` (§3.4 demo seeding),
`fixtures/demo-run.json` (frozen corpus), plus small additions to
`src/events.rs`/`src/db.rs` and the `main.rs` serve/demo-seed wiring. No wire
deviations from §2–§3; the interpretation choices the contract left open:

- **Journal access** (§3.2 implementer's choice): one shared
  `rusqlite::Connection` behind a `std::sync::Mutex`. The API is read-only
  and desktop-scale, so the mutex beats a pool; guards are never held
  across an `.await` (every journal read is a synchronous block returning
  owned data). External writers appending through their own WAL connections
  are observed because every autocommit `SELECT` takes a fresh snapshot.
- **Seq-carrying reads**: `events::tail_with_seq` / `events::journal_stats`
  were added next to the F-01 primitives — §3.1 frames and the §3.3
  `firstSeq`/`lastSeq` fields need the journal `seq` that `tail` drops.
  `db::default_journal_path()` moved into the library so the seeder can
  refuse that path; `main` still honors `AGENTOS_DB` first.
- **Framing strictness**: `invalid_request` (always answered with
  `id: null`) covers unparseable frames, non-object frames, missing/empty
  `method`, a missing or non-number/string `id` (client→server frames must
  carry one — notifications are server→client only, §2), and a non-object
  `params`. `events.list` validates `afterSeq ≥ 0`, `limit` in `1..=1000`
  (0 and floats rejected); unknown params are ignored (forward compat).
- **Subscriptions**: per-subscription task, `events::tail(last_seq, 500)`
  per 250 ms tick; batches drain back-to-back so replay runs at full speed
  and the sleep only applies to idle polls. Delivery is gapless and
  at-least-once per seq (each frame advances `last_seq` only once queued);
  the response frame is enqueued before the task spawns, so the
  `subscriptionId` reply precedes replay notifications. Outbound frames go
  through a bounded 128-slot channel; a subscriber that fills it gets
  `subscription.closed` reason `slow_consumer` (a journal read failure
  closes with reason `journal_error` — non-contract reasons are free
  strings by §2.3's shape). Unsubscribe of an unknown/already-closed id
  answers `{"stopped": false}`.
- **Shutdown**: a `tokio::sync::watch` flips on ctrl-c; every connection's
  writer drains queued frames, sends `daemon.stopping`, then closes 1001;
  `serve()` returns only after all connections finish.
- **Fold interpretation** (documented in `src/projection.rs` module docs):
  `budget.exceeded` sets `budgetExceeded` on the task only — the §3.3
  `RunSummary` wire shape has no such field, and the exact-shape requirement
  outranks the fold-table prose; `agent.leased`/`task.running` both map the
  agent to `running` (the vocabulary has no `leased`); `run.completed`
  resolves through the F-06 derivation (`RunStatus::from_tasks`: any failed
  task → `failed`); `commitSha` reads the supervisor's payload key `sha`
  (`commitSha` accepted as fallback); `attempts` increments only on
  `agent.crashed` per the table (the supervisor's `attempt` payload field
  on lease events is not folded). `dependsOn` absorbs the §3.3 node-list
  shape `{nodes:[{id,dependsOn}]}` retroactively (workflow.started precedes
  task.created) and tolerates today's supervisor shape where `nodes` is a
  bare count. Agent `usage` sums `costUsd` when reported and takes
  `totalTokens`, else `inputTokens+outputTokens` (UsageSnapshot camelCase
  canon; codex/agy `costUsd: null` per F-00 §4).
- **Demo fixture** (`fixtures/demo-run.json`, 51 events over ~1m45s): the
  F-07 Appendix-E happy path — spec → parallel build-a (codex) / build-b
  (agy) → review (stub-reviewer@f-07) → git gate (`git_push`), with the
  adapter ids of the F-00 §6 provider mix, `usage.updated` snapshots in the
  real `UsageSnapshot` camelCase shape, and the git-gate approval flow as
  F-07 §5.2/§5.3 shape it: the human opens the request up front
  (`approval.required`, non-blocking, fingerprint + expiry) and the gate
  time sees `approval.granted` before `git.queued`. The blocking
  (park-in-`human_required`) approval variant is covered by fold tests on
  synthetic journals, not by the happy-path corpus.
- **demo-seed refusals** are checked before any append: the default journal
  path (compared case/separator-insensitively for Windows) and any journal
  with existing events (§3.4). The seeder stamps `"demo": true` into every
  payload even if the fixture forgot it. `main` hand-parses argv: no
  subcommand or `serve` serves; `demo-seed --db <path> --fixture <file>`
  seeds; both flags required, unknown flags are errors.
- **Dependencies**: `chrono` (already a workspace dep) for RFC 3339
  stamps; `futures-util` + `tokio-tungstenite` 0.26 were pre-wired by the
  scaffold and are used with default features (ws:// only — no TLS crates
  enter the lockfile).

Verification (Windows reference platform, `CARGO_TARGET_DIR=target/daemon-f11a`):
`cargo test -p agentos-daemon` — 32 tests green across lib (8),
`demo_seed` (5, incl. seeding + folding the frozen fixture),
`events_roundtrip` (2, F-01 regression), `projection_fold` (10),
`ws_e2e` (7: ping/info, pagination+truncated, error ladder, subscribe
replay→external-writer tail→resubscribe→unsubscribe, projections over WS,
`daemon.stopping`+close 1001, slow-consumer close). `cargo clippy
--all-targets -- -D warnings` clean; `cargo fmt` applied. External smoke:
the real binary serves `101` + `ping`/`daemon.info` round-trips on
`127.0.0.1:8799` from a hand-rolled WS client, and `demo-seed` appends 51
events / refuses both the default path and a reseed.

## 8. F-13 addendum: registry & chat-session methods (binding)

F-13 (`docs/F-13-agent-registry.md`) extends this contract's method table —
§2 framing/error codes apply unchanged. `dispatch` became `async` for the
adapter-facing arms; the §"Journal access" invariant survives (guards never
cross an await).

New methods: `registry.agents.list` / `registry.agents.create` /
`registry.agents.update` / `registry.agents.delete` /
`registry.agents.set-enabled` / `registry.skills.list` / `registry.catalog` /
`agent.session.start` / `agent.session.send` / `agent.session.cancel` — exact
params/results in F-13 §3. Error mapping: domain rejections (validation,
duplicate, not-found, builtin-protected, finished-session) → `invalid_params`;
storage/adapter failures → `internal_error`.

New journal event types (all round-trip via `EventType::Other`):
`agent.created` / `agent.updated` / `agent.deleted` (registry audit; payload
= the full record), `session.instruction` (chat follow-up; payload =
`{sessionId, message}`), `session.finished` (payload adds `finalResult`,
`chat: true` on chat sessions), `agent.session_failed` (`{sessionId, error}`),
`session.cancelled`, `agent.proposal` (`{sessionId, agent}` — a validated
draft, never a registration), `agent.proposal_invalid` (`{sessionId, reason}`).
Chat sessions set `agent_id` = the registry agent slug and share one
`trace_id` per conversation; they carry **no** `run_id`/`task_id` (no
fabricated projections). `session.spawn` payloads gain `model` and
`objectivePreview` (first 2 000 chars, preamble included) on both the
supervisor and chat paths.

The read-only promise of §1 is narrowed, deliberately and visibly: the
**registry** methods are the first write path in the daemon's own API surface
(the journal stays append-only and trigger-guarded; the registry's own tables
are mutable with the journal as their audit trail).
