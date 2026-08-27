// F-11 Command Center Main Application Shell (docs/F-11-desktop.md §4)
import { useEffect, useState } from "react";
import {
  IconAgents,
  IconDashboard,
  IconGit,
  IconGraph,
  IconBot,
  IconInbox,
  IconMoon,
  IconSession,
  IconSettings,
  IconSun,
} from "./components/Icons";
import { LimitMeters } from "./components/LimitMeters";
import {
  useAgents,
  useDaemonConnection,
  useEventRate,
  useInboxEvents,
  useRuns,
} from "./store/hooks";
import "./styles/app.css";
import { applyTheme } from "./theme";
import type { ViewType } from "./types";
import { Agents } from "./views/Agents";
import { CommandCenter } from "./views/CommandCenter";
import { Inbox } from "./views/Inbox";
import { Mastermind } from "./views/Mastermind";
import { Review } from "./views/Review";
import { RunsGraph } from "./views/RunsGraph";
import { Session } from "./views/Session";
import { Settings } from "./views/Settings";

const STORAGE_KEY_PROJECT_PATH = "agentos_project_path";
const STORAGE_KEY_THEME = "agentos_theme";

export default function App() {
  const [currentView, setCurrentView] = useState<ViewType>("command_center");
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  // Dark / Light Theme Mode
  const [theme, setTheme] = useState<"light" | "dark">(() => {
    if (typeof window !== "undefined" && window.localStorage) {
      const saved = window.localStorage.getItem(STORAGE_KEY_THEME);
      if (saved === "dark" || saved === "light") return saved;
      if (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) {
        return "dark";
      }
    }
    return "light";
  });

  useEffect(() => {
    const cancel = applyTheme(theme);
    if (typeof window !== "undefined" && window.localStorage) {
      window.localStorage.setItem(STORAGE_KEY_THEME, theme);
    }
    return cancel;
  }, [theme]);

  const toggleTheme = () => {
    setTheme((prev) => (prev === "dark" ? "light" : "dark"));
  };

  // Deep-link state for navigation between views
  const [targetTaskId, setTargetTaskId] = useState<string | null>(null);
  const [targetAgentId, setTargetAgentId] = useState<string | null>(null);

  const [projectPath, setProjectPath] = useState<string>(() => {
    if (typeof window !== "undefined" && window.localStorage) {
      return window.localStorage.getItem(STORAGE_KEY_PROJECT_PATH) || "D:\\OP\\agent-engineering-os";
    }
    return "D:\\OP\\agent-engineering-os";
  });

  const runs = useRuns();
  const agents = useAgents();
  const { state: connState, meta } = useDaemonConnection();
  const eventRate = useEventRate();
  const inboxEvents = useInboxEvents();

  const totalCost = agents.reduce((sum, a) => sum + (a.usage?.costUsd || 0), 0);

  const handleNavigate = (
    view: ViewType,
    params?: { runId?: string; taskId?: string; agentId?: string }
  ) => {
    if (params?.runId) setSelectedRunId(params.runId);
    if (params?.taskId) setTargetTaskId(params.taskId);
    if (params?.agentId) setTargetAgentId(params.agentId);
    setCurrentView(view);
  };

  const handleSelectProject = (newPath: string) => {
    setProjectPath(newPath);
    if (typeof window !== "undefined" && window.localStorage) {
      window.localStorage.setItem(STORAGE_KEY_PROJECT_PATH, newPath);
    }
  };

  const getViewTitle = (view: ViewType): string => {
    switch (view) {
      case "command_center":
        return "Multi-Agent Command Center";
      case "runs_graph":
        return "Workflow & Task Graph";
      case "session":
        return "Interactive Agent Session";
      case "review":
        return "Diff & Review Workspace";
      case "inbox":
        return "Intervention & Notification Inbox";
      case "agents":
        return "Agent Registry & Creation";
      case "mastermind":
        return "Mastermind — Orchestrated Build";
      case "settings":
        return "Settings & Project Workspace";
    }
  };

  return (
    <div className="app-container">
      {/* Left Sidebar Nav */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <div className="brand-icon">OS</div>
          <div style={{ display: "flex", flexDirection: "column" }}>
            <span className="brand-title">Agent Engineering</span>
            <span style={{ fontSize: "10px", color: "var(--text-secondary)", letterSpacing: "0.5px" }}>
              COMMAND CENTER v1.0
            </span>
          </div>
        </div>

        <nav className="sidebar-nav">
          <button
            className={`nav-item ${currentView === "command_center" ? "active" : ""}`}
            onClick={() => setCurrentView("command_center")}
          >
            <IconDashboard size={18} />
            <span>Command Center</span>
          </button>

          <button
            className={`nav-item ${currentView === "runs_graph" ? "active" : ""}`}
            onClick={() => setCurrentView("runs_graph")}
          >
            <IconGraph size={18} />
            <span>Runs & Graph</span>
          </button>

          <button
            className={`nav-item ${currentView === "agents" ? "active" : ""}`}
            onClick={() => setCurrentView("agents")}
          >
            <IconAgents size={18} />
            <span>Agents</span>
          </button>

          <button
            className={`nav-item ${currentView === "mastermind" ? "active" : ""}`}
            onClick={() => setCurrentView("mastermind")}
          >
            <IconBot size={18} />
            <span>Mastermind</span>
          </button>

          <button
            className={`nav-item ${currentView === "session" ? "active" : ""}`}
            onClick={() => setCurrentView("session")}
          >
            <IconSession size={18} />
            <span>Session & Stream</span>
          </button>

          <button
            className={`nav-item ${currentView === "review" ? "active" : ""}`}
            onClick={() => setCurrentView("review")}
          >
            <IconGit size={18} />
            <span>Diff & Review</span>
          </button>

          <button
            className={`nav-item ${currentView === "inbox" ? "active" : ""}`}
            onClick={() => setCurrentView("inbox")}
          >
            <IconInbox size={18} />
            <span>Inbox</span>
            {inboxEvents.length > 0 && (
              <span className="nav-item-badge">{inboxEvents.length}</span>
            )}
          </button>

          <button
            className={`nav-item ${currentView === "settings" ? "active" : ""}`}
            onClick={() => setCurrentView("settings")}
          >
            <IconSettings size={18} />
            <span>Settings</span>
          </button>
        </nav>

        {/* Project Path Indicator */}
        <div className="sidebar-footer">
          <div style={{ fontSize: "10px", color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>
            Project Root
          </div>
          <div className="project-pill" title={projectPath}>
            <span>📁</span>
            <span>{projectPath.split(/[\\/]/).pop() || "project"}</span>
          </div>
        </div>
      </aside>

      {/* Main Workspace Wrapper */}
      <div className="main-wrapper">
        {/* Top App Header */}
        <header className="top-header">
          <div className="header-left">
            <h2 className="view-title">{getViewTitle(currentView)}</h2>

            {/* Run Selector */}
            <div className="run-selector-container">
              <select
                className="run-select"
                value={selectedRunId || ""}
                onChange={(e) => setSelectedRunId(e.target.value || null)}
              >
                <option value="">All Runs ({runs.length})</option>
                {runs.map((r) => (
                  <option key={r.runId} value={r.runId}>
                    Run {r.runId.slice(0, 8)} • {r.status}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="header-right">
            {/* Event Velocity Telemetry */}
            <div className="telemetry-chip" title="Journal event ingestion rate">
              <span>⚡</span>
              <span className="telemetry-value">{eventRate}</span>
              <span>ev/s</span>
            </div>

            {/* Provider rate-limit meters */}
            <LimitMeters />

            {/* Total Cost Telemetry */}
            <div className="telemetry-chip" title="Cumulative estimated LLM cost">
              <span>💲</span>
              <span className="telemetry-value">${totalCost.toFixed(3)}</span>
            </div>

            {/* Theme Mode Toggle (Light / Dark) */}
            <button
              className="btn-icon-circular"
              onClick={toggleTheme}
              title={`Switch to ${theme === "dark" ? "Light" : "Dark"} Mode`}
              style={{
                width: "30px",
                height: "30px",
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                cursor: "pointer",
                background: "var(--colors-surface)",
                border: "1px solid var(--colors-hairline)",
                borderRadius: "var(--rounded-full)",
                color: "var(--colors-ink)",
              }}
            >
              {theme === "dark" ? <IconSun size={15} /> : <IconMoon size={15} />}
            </button>

            {/* Daemon Connection State */}
            <div className="connection-indicator" title={`Daemon URL: ${meta.url}`}>
              <span className={`status-dot ${connState}`} />
              <span style={{ fontSize: "11px", color: "var(--text-primary)" }}>
                {connState === "connected"
                  ? `Connected ${meta.latencyMs !== null ? `(${meta.latencyMs}ms)` : ""}`
                  : connState === "reconnecting"
                  ? "Reconnecting..."
                  : connState === "connecting"
                  ? "Connecting..."
                  : "Disconnected"}
              </span>
            </div>
          </div>
        </header>

        {/* View Switcher Container */}
        <main style={{ flex: 1, overflow: "hidden", display: "flex" }}>
          {currentView === "command_center" && (
            <CommandCenter
              onNavigate={handleNavigate}
              selectedRunId={selectedRunId}
              onSelectRun={setSelectedRunId}
            />
          )}

          {currentView === "runs_graph" && (
            <RunsGraph
              onNavigate={handleNavigate}
              selectedRunId={selectedRunId}
              onSelectRun={setSelectedRunId}
              initialSelectedTaskId={targetTaskId}
            />
          )}

          {currentView === "agents" && <Agents />}

          {currentView === "mastermind" && (
            <Mastermind projectPath={projectPath} onSelectProject={handleSelectProject} />
          )}

          {currentView === "session" && (
            <Session
              onNavigate={handleNavigate}
              selectedAgentId={targetAgentId}
              selectedTaskId={targetTaskId}
            />
          )}

          {currentView === "review" && (
            <Review
              onNavigate={handleNavigate}
              selectedRunId={selectedRunId}
            />
          )}

          {currentView === "inbox" && (
            <Inbox
              onNavigate={handleNavigate}
            />
          )}

          {currentView === "settings" && (
            <Settings
              onNavigate={handleNavigate}
              activeProjectPath={projectPath}
              onSelectProject={handleSelectProject}
            />
          )}
        </main>
      </div>
    </div>
  );
}
