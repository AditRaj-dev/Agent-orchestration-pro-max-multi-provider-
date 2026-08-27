// F-11 Settings & Project Workspace View (UX-01 v1)
import { useEffect, useState } from "react";
import { applyTheme, systemTheme } from "../theme";
import { IconFolder, IconMoon, IconSettings, IconSun } from "../components/Icons";
import { getDaemonWsClient } from "../daemon/ws";
import { useDaemonConnection } from "../store/hooks";
import type { DaemonInfo, GitPushProfile, ViewType } from "../types";

export interface SettingsProps {
  onNavigate: (view: ViewType) => void;
  activeProjectPath: string;
  onSelectProject: (path: string) => void;
}

export function Settings({ activeProjectPath, onSelectProject }: SettingsProps) {
  const client = getDaemonWsClient();
  const { state: connState, meta } = useDaemonConnection();

  const [wsInput, setWsInput] = useState(() => client.getUrl());
  const [projectInput, setProjectInput] = useState(activeProjectPath);
  const [pingTesting, setPingTesting] = useState(false);
  const [pingResult, setPingResult] = useState<string | null>(null);
  const [savedSuccess, setSavedSuccess] = useState(false);

  const [daemonInfo, setDaemonInfo] = useState<DaemonInfo | null>(null);
  const [daemonInfoError, setDaemonInfoError] = useState<string | null>(null);
  const [pushProfile, setPushProfile] = useState<GitPushProfile | null>(null);
  const [pushProfileError, setPushProfileError] = useState<string | null>(null);
  const [pushRemote, setPushRemote] = useState("");
  const [savingPushRemote, setSavingPushRemote] = useState(false);

  // Fetch daemon.info on connect or load
  const fetchDaemonInfo = async () => {
    try {
      setDaemonInfoError(null);
      const info = await client.call<DaemonInfo>("daemon.info", {});
      setDaemonInfo(info);
    } catch (err: any) {
      setDaemonInfoError(err.message || String(err));
    }
  };

  useEffect(() => {
    if (connState === "connected") {
      fetchDaemonInfo();
    }
  }, [connState]);

  const fetchPushProfile = async (repo = activeProjectPath) => {
    if (!repo.trim() || connState !== "connected") return;
    try {
      setPushProfileError(null);
      const profile = await client.call<GitPushProfile>("git.push-targets", { repo });
      setPushProfile(profile);
      setPushRemote(profile.pushDefault || "");
    } catch (err: any) {
      setPushProfile(null);
      setPushProfileError(err.message || String(err));
    }
  };

  useEffect(() => {
    fetchPushProfile();
  }, [connState, activeProjectPath]);

  const handleSaveWsUrl = () => {
    client.setUrl(wsInput);
    setSavedSuccess(true);
    setTimeout(() => setSavedSuccess(false), 2500);
  };

  const handleTestPing = async () => {
    setPingTesting(true);
    setPingResult(null);
    const start = Date.now();
    try {
      const res = await client.call<{ pong: boolean; serverTime: string }>("ping", {});
      const elapsed = Date.now() - start;
      setPingResult(`Pong received in ${elapsed}ms (server time: ${res.serverTime})`);
    } catch (err: any) {
      setPingResult(`Ping failed: ${err.message}`);
    } finally {
      setPingTesting(false);
    }
  };

  // Tauri Folder Picker with Browser fallback
  const handleOpenFolderPicker = async () => {
    try {
      // Dynamic import to guard for browser vs Tauri runtime
      const dialog = await import("@tauri-apps/plugin-dialog");
      if (dialog && typeof dialog.open === "function") {
        const selected = await dialog.open({
          directory: true,
          multiple: false,
          title: "Select Project Git Repository or Folder",
        });
        if (selected && typeof selected === "string") {
          setProjectInput(selected);
          onSelectProject(selected);
          return;
        }
      }
    } catch (err) {
      console.warn("Tauri dialog plugin not available in browser mode, falling back to manual input", err);
    }
  };

  const handleSaveProject = () => {
    if (projectInput.trim()) {
      onSelectProject(projectInput.trim());
    }
  };

  const handleSavePushRemote = async () => {
    if (!pushRemote || !activeProjectPath.trim()) return;
    setSavingPushRemote(true);
    try {
      setPushProfileError(null);
      const profile = await client.call<GitPushProfile>("git.push-target.set", {
        repo: activeProjectPath,
        remote: pushRemote,
      });
      setPushProfile(profile);
      setPushRemote(profile.pushDefault || "");
    } catch (err: any) {
      setPushProfileError(err.message || String(err));
    } finally {
      setSavingPushRemote(false);
    }
  };

  const [themeMode, setThemeMode] = useState<"light" | "dark" | "system">(() => {
    if (typeof window !== "undefined" && window.localStorage) {
      const saved = window.localStorage.getItem("agentos_theme");
      if (saved === "dark" || saved === "light") return saved;
    }
    return "system";
  });

  const handleSetTheme = (mode: "light" | "dark" | "system") => {
    setThemeMode(mode);
    if (mode === "system") {
      localStorage.removeItem("agentos_theme");
      applyTheme(systemTheme());
    } else {
      localStorage.setItem("agentos_theme", mode);
      applyTheme(mode);
    }
  };

  return (
    <div className="view-body" style={{ maxWidth: "900px", margin: "0 auto", width: "100%" }}>
      {/* Title */}
      <div className="panel" style={{ padding: "12px 16px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          <IconSettings size={18} />
          <span style={{ fontWeight: 600, fontSize: "14px" }}>Project & Daemon Configuration</span>
        </div>
      </div>

      {/* Appearance & Design Theme */}
      <div className="panel">
        <div className="panel-header">
          <div className="panel-title">
            <IconSun size={16} />
            <span>Appearance & Design Theme</span>
          </div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
          <p style={{ fontSize: "12px", color: "var(--text-secondary)" }}>
            Select your preferred visual mode. Both themes follow the MiniMax Design System with DM Sans typography, pill buttons, and vibrant gradient accents.
          </p>

          <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
            <button
              className={`btn ${themeMode === "light" ? "btn-primary" : "btn-secondary"}`}
              onClick={() => handleSetTheme("light")}
              style={{ display: "inline-flex", alignItems: "center", gap: "8px" }}
            >
              <IconSun size={15} />
              <span>MiniMax Light (Canvas White)</span>
            </button>
            <button
              className={`btn ${themeMode === "dark" ? "btn-primary" : "btn-secondary"}`}
              onClick={() => handleSetTheme("dark")}
              style={{ display: "inline-flex", alignItems: "center", gap: "8px" }}
            >
              <IconMoon size={15} />
              <span>Obsidian Dark (Deep Carbon)</span>
            </button>
            <button
              className={`btn ${themeMode === "system" ? "btn-primary" : "btn-secondary"}`}
              onClick={() => handleSetTheme("system")}
              style={{ display: "inline-flex", alignItems: "center", gap: "8px" }}
            >
              <span>💻 Follow System</span>
            </button>
          </div>
        </div>
      </div>

      {/* Project Workspace Setting (UX-01) */}
      <div className="panel">
        <div className="panel-header">
          <div className="panel-title">
            <IconFolder size={16} />
            <span>Active Project Workspace</span>
          </div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
          <p style={{ fontSize: "12px", color: "var(--text-secondary)" }}>
            Select the local Git repository or workspace directory. Persisted locally in this desktop client.
          </p>

          <div style={{ display: "flex", gap: "10px" }}>
            <input
              type="text"
              className="input-text"
              style={{ flex: 1, fontFamily: "var(--font-mono)" }}
              value={projectInput}
              onChange={(e) => setProjectInput(e.target.value)}
              placeholder="e.g. D:\OP\agent-engineering-os"
            />
            <button className="btn-secondary" onClick={handleOpenFolderPicker}>
              Browse Folder...
            </button>
            <button className="btn-primary" onClick={handleSaveProject}>
              Save
            </button>
          </div>

          <div style={{ fontSize: "11px", color: "var(--text-muted)" }}>
            Active path: <code>{activeProjectPath || "No project selected"}</code>
          </div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-header">
          <div className="panel-title">Git Push Account</div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
          <p style={{ fontSize: "12px", color: "var(--text-secondary)" }}>
            Choose the Git remote used by default for pushes. Authentication stays in your SSH agent or OS credential manager; no credentials are stored here.
          </p>

          {pushProfile ? (
            pushProfile.remotes.length > 0 ? (
            <>
              <div style={{ display: "flex", gap: "10px" }}>
                <select
                  className="input-text"
                  style={{ flex: 1, fontFamily: "var(--font-mono)" }}
                  value={pushRemote}
                  onChange={(e) => setPushRemote(e.target.value)}
                >
                  <option value="" disabled>Select a push remote…</option>
                  {pushProfile.remotes.map((remote) => (
                    <option key={remote.name} value={remote.name}>
                      {remote.name} — {remote.pushUrl}
                    </option>
                  ))}
                </select>
                <button
                  className="btn-primary"
                  onClick={handleSavePushRemote}
                  disabled={!pushRemote || savingPushRemote}
                >
                  {savingPushRemote ? "Saving…" : "Use for Pushes"}
                </button>
              </div>
              <div style={{ fontSize: "11px", color: "var(--text-muted)" }}>
                Commit identity: <code>{pushProfile.identity.name || "not configured"}</code>
                {pushProfile.identity.email ? <> &lt;{pushProfile.identity.email}&gt;</> : null}
              </div>
            </>
            ) : (
              <div style={{ fontSize: "12px", color: "var(--text-muted)" }}>
                No Git remotes are configured for this project yet. Add a remote such as <code>origin</code>, then return here to choose its push account.
              </div>
            )
          ) : (
            <div style={{ fontSize: "12px", color: "var(--text-muted)" }}>
              {connState === "connected" ? "Loading Git remotes…" : "Connect to the daemon to inspect Git remotes."}
            </div>
          )}

          {pushProfileError && (
            <div style={{ fontSize: "12px", color: "var(--status-failed)" }}>{pushProfileError}</div>
          )}
        </div>
      </div>

      {/* WebSocket Daemon Connection */}
      <div className="panel">
        <div className="panel-header">
          <div className="panel-title">
            <span>Daemon Connection (WebSocket RPC)</span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <span className={`status-dot ${connState}`} />
            <span style={{ fontSize: "12px", textTransform: "capitalize", fontWeight: 600 }}>{connState}</span>
          </div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
          <div>
            <label style={{ display: "block", fontSize: "12px", color: "var(--text-secondary)", marginBottom: "4px" }}>
              WebSocket Endpoint Address:
            </label>
            <div style={{ display: "flex", gap: "10px" }}>
              <input
                type="text"
                className="input-text"
                style={{ flex: 1, fontFamily: "var(--font-mono)" }}
                value={wsInput}
                onChange={(e) => setWsInput(e.target.value)}
                placeholder="ws://127.0.0.1:8741"
              />
              <button className="btn-primary" onClick={handleSaveWsUrl}>
                {savedSuccess ? "Saved ✓" : "Apply & Connect"}
              </button>
              <button
                className="btn-secondary"
                onClick={handleTestPing}
                disabled={pingTesting || connState !== "connected"}
              >
                {pingTesting ? "Pinging..." : "Test Ping"}
              </button>
            </div>
            <p style={{ fontSize: "11px", color: "var(--text-muted)", marginTop: "4px" }}>
              Precedence: <code>?ws=</code> URL query &gt; Settings / localStorage &gt; <code>AGENTOS_WS_ADDR</code> env &gt; <code>ws://127.0.0.1:8741</code>
            </p>
          </div>

          {pingResult && (
            <div
              style={{
                backgroundColor: "var(--bg-input)",
                border: "1px solid var(--border-subtle)",
                borderRadius: "var(--radius-sm)",
                padding: "8px 12px",
                fontSize: "12px",
                fontFamily: "var(--font-mono)",
                color: pingResult.includes("failed") ? "var(--status-failed)" : "var(--accent-green)",
              }}
            >
              {pingResult}
            </div>
          )}

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px", fontSize: "12px" }}>
            <div>
              <span style={{ color: "var(--text-secondary)" }}>Round-trip Latency:</span>{" "}
              <strong>{meta.latencyMs !== null ? `${meta.latencyMs} ms` : "—"}</strong>
            </div>
            <div>
              <span style={{ color: "var(--text-secondary)" }}>Highest Journal Seq:</span>{" "}
              <strong style={{ fontFamily: "var(--font-mono)" }}>#{meta.lastSeq}</strong>
            </div>
            <div>
              <span style={{ color: "var(--text-secondary)" }}>Last Ping:</span>{" "}
              <span>{meta.lastPingAt ? new Date(meta.lastPingAt).toLocaleTimeString() : "—"}</span>
            </div>
            <div>
              <span style={{ color: "var(--text-secondary)" }}>Reconnect Retries:</span>{" "}
              <span>{meta.reconnectAttempts}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Daemon System Information */}
      <div className="panel">
        <div className="panel-header">
          <span className="panel-title">Daemon Runtime Metadata</span>
          {connState === "connected" && (
            <button className="btn-secondary" style={{ fontSize: "11px", padding: "2px 8px" }} onClick={fetchDaemonInfo}>
              Refresh Info
            </button>
          )}
        </div>

        {daemonInfo ? (
          <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px", fontSize: "12px" }}>
              <div>
                <span style={{ color: "var(--text-secondary)" }}>Daemon Version:</span>{" "}
                <strong>{daemonInfo.version}</strong>
              </div>
              <div>
                <span style={{ color: "var(--text-secondary)" }}>Process PID:</span>{" "}
                <strong style={{ fontFamily: "var(--font-mono)" }}>{daemonInfo.pid}</strong>
              </div>
              <div>
                <span style={{ color: "var(--text-secondary)" }}>Total Journal Events:</span>{" "}
                <strong style={{ fontFamily: "var(--font-mono)" }}>{daemonInfo.eventCount}</strong>
              </div>
              <div>
                <span style={{ color: "var(--text-secondary)" }}>Started At:</span>{" "}
                <span>{daemonInfo.startedAt}</span>
              </div>
            </div>

            <div style={{ fontSize: "12px" }}>
              <span style={{ color: "var(--text-secondary)" }}>Journal SQLite Database:</span>
              <div
                style={{
                  backgroundColor: "var(--bg-input)",
                  padding: "6px 10px",
                  borderRadius: "var(--radius-sm)",
                  fontFamily: "var(--font-mono)",
                  color: "var(--text-code)",
                  marginTop: "4px",
                  wordBreak: "break-all",
                }}
              >
                {daemonInfo.journalPath}
              </div>
            </div>
          </div>
        ) : (
          <div style={{ color: "var(--text-muted)", fontSize: "12px" }}>
            {connState === "connected"
              ? daemonInfoError
                ? `Error fetching daemon info: ${daemonInfoError}`
                : "Loading daemon info..."
              : "Connect to daemon to inspect runtime process details and journal path."}
          </div>
        )}
      </div>
    </div>
  );
}
