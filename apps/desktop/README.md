# Agent Engineering OS — Desktop Command Center (F-11b)

The desktop command-center UI and Tauri 2 shell for the **Agent Engineering OS**. Built with React 18, TypeScript, and standard Web standards, communicating strictly with the local daemon WebSocket surface (`docs/F-11-desktop.md`).

---

## Architectural Principles & Contract Parity

- **Pure Client**: The UI maintains zero second-source-of-truth state. All state is a deterministic client-side projection folded directly from append-only journal events conforming to `docs/F-11-desktop.md` §3.3.
- **Strict Protocol Conformance**:
  - Transport: Loopback WebSocket (RFC 6455) at `ws://127.0.0.1:8741`.
  - Endpoint Precedence: `?ws=` URL query &gt; `localStorage` setting &gt; `AGENTOS_WS_ADDR` / `VITE_AGENTOS_WS_ADDR` env &gt; `ws://127.0.0.1:8741`.
  - Resilience: Exponential reconnect (250 ms → 8 s cap with jitter).
  - Heartbeat: 15-second `ping` RPC; if no response within 10 seconds, connection is marked dead and forcibly reconnected.
  - Gapless Catch-up: Automatically resubscribes with `afterSeq = lastDeliveredSeq` upon reconnection to prevent event loss.
  - Telemetry Coalescing: High-frequency telemetry streams (`usage.updated`, `agent.tool_use`) coalesce at $\le 4\text{ Hz}$ (250 ms batched flush) to ensure smooth rendering with 10+ concurrent agents.
  - Ring Buffer: Keeps latest 2000 events in memory.
- **Zero Heavy Runtime Frameworks**: Hand-rolled plain CSS, native state-based view switching, and SVG DAG canvas without third-party graph/UI kits.

---

## Views & UX Capabilities

1. **Multi-Agent Command Center (`UX-02`)**:
   - Status cards for agents (`running`, `planning`, `waiting`, `reviewing`, `blocked`, `failed`, `complete`).
   - Run history strip with pipeline progress bars.
   - Live activity feed with auto-scroll and hover pause.
   - Blocked task and dependency resolution in $\le 2$ clicks.
2. **Workflow Graph View (`UX-04`)**:
   - Topological layered DAG layout rendered from `TaskSummary` + `dependsOn` state.
   - SVG bezier curve connectors with directional arrows.
   - Task detail drawer with event history, payload hashes, and retry counters.
3. **Interactive Session View (`UX-03`)**:
   - Filterable terminal-style stream and conversation turn grouping.
   - Real-time token and USD cost gauges.
   - Raw JSON payload inspector.
4. **Diff & Review Workspace (`UX-05`)**:
   - Git & review event timeline with task/agent attribution and commit SHAs.
   - Changed-file candidate sets.
   - Seam documentation for daemon `git.diff`.
5. **Notification & Intervention Inbox (`UX-06-lite`)**:
   - Centralized inbox for human approvals, budget overages, file conflicts, and failures.
   - Quiet mode filter and local dismissal.
   - Direct deep-links to workflow graph and agent sessions.
6. **Project & Settings (`UX-01`)**:
   - Daemon address configuration and live latency ping tester.
   - Native directory picker via Tauri dialog plugin (`@tauri-apps/plugin-dialog`) with browser text fallback.
   - Daemon runtime diagnostics (PID, version, SQLite journal path, total events).

---

## Development Runbook

### 1. Install Dependencies
```bash
cd apps/desktop
npm install
```

### 2. Browser Mode (Dev)
```bash
npm run dev
```
Open `http://localhost:5173`. When the daemon is not running, the top bar displays the disconnected state honestly and initiates exponential reconnect polling.

### 3. Running with Seeded Daemon Fixture
To test against realistic multi-agent event data without LLM billing:
```bash
# In repo root:
cargo run -p agentos-daemon -- demo-seed --db target/demo.db --fixture fixtures/demo-run.json
AGENTOS_DB=target/demo.db cargo run -p agentos-daemon
```
Then in browser or desktop, point the app to `ws://127.0.0.1:8741`.

### 4. Desktop Mode (Tauri 2)
```bash
npm run tauri dev
```

---

## Verification & Build Gates

- **Typecheck & Vite Bundle**:
  ```bash
  npm run build
  ```
- **Vitest Unit Test Suite**:
  ```bash
  npm test
  ```
  Tests include:
  - Fold parity with F-11 §3.3 table on synthetic frames (happy path, crash-retry, human parking, total function on unknown event types, usage rollup).
  - WS client exponential backoff and gapless catch-up via fake WebSocket.
  - High-frequency event coalescing ($\le 4\text{ Hz}$).
- **Tauri Rust Compilation Check**:
  ```bash
  cd src-tauri
  cargo check
  ```

---

## Known Seams (F-11 §6)

1. **`git.diff`**: Returns `not_supported` in v1; UX-05 displays verified event-carried attribution and changed-file manifests until `agentos-git` is linked.
2. **Write-Path Methods**: User steering instructions and task intervention RPCs belong to the policy/supervisor surface.
3. **Project Records**: Daemon-owned project manifests are a future slice; UX-01 persists user preferences in localStorage.
4. **Branding Assets**: Placeholder PNG icons in `src-tauri/icons/` generated via `scripts/gen-icon.mjs` should be replaced with final brand icons.
