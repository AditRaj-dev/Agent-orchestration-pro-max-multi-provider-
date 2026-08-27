// Read and explore Mastermind phase deliverables in a tabbed browser-like workspace.
// The daemon serves them through `mastermind.artifact`, allowlisted against the
// session's own repository — this component never touches the filesystem directly.
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { marked } from "marked";
import {
  IconArrowLeft,
  IconArrowRight,
  IconClose,
  IconCode,
  IconCopy,
  IconExternalLink,
  IconFileText,
  IconList,
  IconPlus,
  IconRefresh,
  IconSearch,
  IconSidebar,
} from "./Icons";
import { getDaemonWsClient } from "../daemon/ws";

export interface ArtifactResponse {
  sessionId: string;
  path: string;
  content: string;
  bytes: number;
  truncated: boolean;
  modifiedAt: string | null;
  available: string[];
}

export interface ArtifactPreviewProps {
  sessionId: string;
  path: string;
  /** Popped out into its own OS window: no drag handle, full window browser layout. */
  standalone?: boolean;
  onClose?: () => void;
}

export interface TabItem {
  path: string;
  title: string;
  bytes?: number;
  modifiedAt?: string | null;
}

export type ViewMode = "preview" | "source" | "outline";

const WIDTH_KEY = "mastermind.artifactPreview.width";
const MIN_WIDTH = 380;
const MAX_WIDTH = 1100;
const DEFAULT_WIDTH = 680;

/** Elements that can execute or phone home, dropped wholesale. */
const FORBIDDEN_TAGS = new Set([
  "SCRIPT",
  "STYLE",
  "IFRAME",
  "FRAME",
  "OBJECT",
  "EMBED",
  "LINK",
  "META",
  "BASE",
  "FORM",
]);
const URL_ATTRS = new Set(["href", "src", "xlink:href", "action", "formaction"]);

/**
 * These documents are written by language models into the user's repository,
 * so their markdown is untrusted input at this boundary. Parse into a detached
 * node, strip anything executable, and only then hand the markup to React.
 */
export function sanitizeMarkup(html: string): string {
  const host = document.createElement("div");
  host.innerHTML = html;
  for (const element of Array.from(host.querySelectorAll("*"))) {
    if (FORBIDDEN_TAGS.has(element.tagName)) {
      element.remove();
      continue;
    }
    for (const attribute of Array.from(element.attributes)) {
      const name = attribute.name.toLowerCase();
      const value = attribute.value.replace(/[\s - ]/g, "").toLowerCase();
      const isScriptUrl =
        URL_ATTRS.has(name) &&
        (value.startsWith("javascript:") || value.startsWith("data:text/html"));
      if (name.startsWith("on") || isScriptUrl) {
        element.removeAttribute(attribute.name);
      }
    }
  }
  return host.innerHTML;
}

function storedWidth(): number {
  const raw = Number(window.localStorage.getItem(WIDTH_KEY));
  if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_WIDTH;
  return Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, raw));
}

function getFileName(filePath: string): string {
  const parts = filePath.split(/[\\/]/);
  return parts[parts.length - 1] || filePath;
}

interface HeadingItem {
  id: string;
  text: string;
  level: number;
}

function extractHeadings(markdown: string): HeadingItem[] {
  const lines = markdown.split("\n");
  const headings: HeadingItem[] = [];
  let index = 0;
  for (const line of lines) {
    const match = line.match(/^(#{1,6})\s+(.+)$/);
    if (match) {
      const level = match[1].length;
      const text = match[2].trim().replace(/[#*`_~]/g, "");
      const id = `heading-${index++}-${text.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`;
      headings.push({ id, text, level });
    }
  }
  return headings;
}

export function ArtifactPreview({
  sessionId,
  path: initialPath,
  standalone = false,
  onClose,
}: ArtifactPreviewProps) {
  // Tabs State
  const [tabs, setTabs] = useState<TabItem[]>(() => [
    { path: initialPath, title: getFileName(initialPath) },
  ]);
  const [activeTabPath, setActiveTabPath] = useState<string>(initialPath);

  // Cache of loaded artifacts by path
  const [artifactCache, setArtifactCache] = useState<Record<string, ArtifactResponse>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loadingPaths, setLoadingPaths] = useState<Record<string, boolean>>({});

  // Browser Navigation History
  const [history, setHistory] = useState<string[]>([initialPath]);
  const [historyIndex, setHistoryIndex] = useState<number>(0);

  // View state
  const [viewMode, setViewMode] = useState<ViewMode>("preview");
  const [showSidebar, setShowSidebar] = useState<boolean>(standalone);
  const [showFilePicker, setShowFilePicker] = useState<boolean>(false);
  const [fileFilter, setFileFilter] = useState<string>("");
  const [copied, setCopied] = useState<boolean>(false);

  // Window sizing & drag
  const [width, setWidth] = useState(() => (standalone ? 0 : storedWidth()));
  const dragging = useRef(false);
  const widthRef = useRef(width);

  // All known available deliverables from daemon responses
  const [availableFiles, setAvailableFiles] = useState<string[]>([initialPath]);

  const activeArtifact = artifactCache[activeTabPath] || null;
  const activeLoading = Boolean(loadingPaths[activeTabPath]);
  const activeError = errors[activeTabPath] || null;

  // Fetch an artifact from the daemon
  const fetchArtifact = useCallback(
    (targetPath: string, force = false) => {
      if (!force && artifactCache[targetPath]) return;

      setLoadingPaths((prev) => ({ ...prev, [targetPath]: true }));
      setErrors((prev) => {
        const next = { ...prev };
        delete next[targetPath];
        return next;
      });

      const client = getDaemonWsClient();
      client
        .call<ArtifactResponse>("mastermind.artifact", { sessionId, path: targetPath })
        .then((result) => {
          setArtifactCache((prev) => ({ ...prev, [targetPath]: result }));
          setLoadingPaths((prev) => ({ ...prev, [targetPath]: false }));

          // Update tab title / metadata
          setTabs((prev) =>
            prev.map((t) =>
              t.path === targetPath
                ? {
                    ...t,
                    title: getFileName(targetPath),
                    bytes: result.bytes,
                    modifiedAt: result.modifiedAt,
                  }
                : t,
            ),
          );

          if (result.available && result.available.length > 0) {
            setAvailableFiles((prev) => {
              const merged = Array.from(new Set([...prev, ...result.available]));
              return merged;
            });
          }
        })
        .catch((err: unknown) => {
          const message = err instanceof Error ? err.message : String(err);
          setErrors((prev) => ({ ...prev, [targetPath]: message }));
          setLoadingPaths((prev) => ({ ...prev, [targetPath]: false }));
        });
    },
    [sessionId, artifactCache],
  );

  // When active tab changes, ensure file is fetched
  useEffect(() => {
    fetchArtifact(activeTabPath);
  }, [activeTabPath, fetchArtifact]);

  // Open a file in a new tab or activate existing
  const openFile = useCallback(
    (filePath: string) => {
      setTabs((prev) => {
        const exists = prev.some((t) => t.path === filePath);
        if (!exists) {
          return [...prev, { path: filePath, title: getFileName(filePath) }];
        }
        return prev;
      });

      setActiveTabPath(filePath);
      setShowFilePicker(false);

      // Add to navigation history
      setHistory((prev) => {
        const next = prev.slice(0, historyIndex + 1);
        next.push(filePath);
        return next;
      });
      setHistoryIndex((prev) => prev + 1);
    },
    [historyIndex],
  );

  // Close tab
  const closeTab = useCallback(
    (tabPathToClose: string, e?: React.MouseEvent) => {
      e?.stopPropagation();

      const newTabs = tabs.filter((t) => t.path !== tabPathToClose);
      if (newTabs.length === 0) {
        onClose?.();
        return;
      }

      setTabs(newTabs);
      if (activeTabPath === tabPathToClose) {
        const closedIdx = tabs.findIndex((t) => t.path === tabPathToClose);
        const nextActiveIdx = Math.max(0, closedIdx - 1);
        setActiveTabPath(newTabs[nextActiveIdx]?.path || newTabs[0].path);
      }
    },
    [tabs, activeTabPath, onClose],
  );

  // History back / forward
  const goBack = useCallback(() => {
    if (historyIndex > 0) {
      const prevPath = history[historyIndex - 1];
      setHistoryIndex((i) => i - 1);
      setActiveTabPath(prevPath);
    }
  }, [history, historyIndex]);

  const goForward = useCallback(() => {
    if (historyIndex < history.length - 1) {
      const nextPath = history[historyIndex + 1];
      setHistoryIndex((i) => i + 1);
      setActiveTabPath(nextPath);
    }
  }, [history, historyIndex]);

  const refreshCurrent = useCallback(() => {
    fetchArtifact(activeTabPath, true);
  }, [activeTabPath, fetchArtifact]);

  // Keyboard shortcuts (Escape to close, Ctrl+W to close tab)
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !standalone && onClose) {
        onClose();
      }
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "w") {
        event.preventDefault();
        closeTab(activeTabPath);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [standalone, onClose, activeTabPath, closeTab]);

  // Drag resize
  useEffect(() => {
    if (standalone) return;
    const onMove = (event: PointerEvent) => {
      if (!dragging.current) return;
      const next = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, window.innerWidth - event.clientX));
      widthRef.current = next;
      setWidth(next);
    };
    const onUp = () => {
      if (!dragging.current) return;
      dragging.current = false;
      document.body.classList.remove("is-resizing");
      window.localStorage.setItem(WIDTH_KEY, String(widthRef.current));
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    return () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
  }, [standalone]);

  // Pop out to standalone OS window
  const popOut = useCallback(async () => {
    const { WebviewWindow } = await import("@tauri-apps/api/webviewWindow");
    const label = `artifact-${activeTabPath.replace(/[^a-zA-Z0-9]+/g, "-")}`;
    const existing = await WebviewWindow.getByLabel(label);
    if (existing) {
      await existing.setFocus();
    } else {
      new WebviewWindow(label, {
        url: `index.html?preview=${encodeURIComponent(activeTabPath)}&session=${encodeURIComponent(sessionId)}`,
        title: `Browser — ${getFileName(activeTabPath)}`,
        width: 1040,
        height: 860,
      });
    }
    onClose?.();
  }, [activeTabPath, sessionId, onClose]);

  const copyPath = useCallback(() => {
    navigator.clipboard.writeText(activeTabPath);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [activeTabPath]);

  // Body content rendering
  const renderedContent = useMemo(() => {
    if (!activeArtifact) return null;

    if (viewMode === "source" || !activeArtifact.path.endsWith(".md")) {
      const lines = activeArtifact.content.split("\n");
      return (
        <div className="browser-source-view">
          <div className="browser-line-numbers">
            {lines.map((_, i) => (
              <span key={i} className="line-num">
                {i + 1}
              </span>
            ))}
          </div>
          <pre className="browser-code-content">{activeArtifact.content}</pre>
        </div>
      );
    }

    if (viewMode === "outline") {
      const headings = extractHeadings(activeArtifact.content);
      return (
        <div className="browser-outline-view">
          <h4 className="outline-title">Document Structure & Headings</h4>
          {headings.length === 0 ? (
            <div className="empty-notice">No markdown headings found in this document.</div>
          ) : (
            <div className="outline-list">
              {headings.map((h, idx) => (
                <div
                  key={idx}
                  className={`outline-item level-${h.level}`}
                  onClick={() => setViewMode("preview")}
                >
                  <span className="outline-level-pill">H{h.level}</span>
                  <span className="outline-text">{h.text}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      );
    }

    // Markdown preview
    const html = sanitizeMarkup(
      marked.parse(activeArtifact.content, { async: false, gfm: true }) as string,
    );
    return <div className="artifact-preview-markdown" dangerouslySetInnerHTML={{ __html: html }} />;
  }, [activeArtifact, viewMode]);

  const headingsCount = useMemo(() => {
    if (!activeArtifact?.content) return 0;
    return extractHeadings(activeArtifact.content).length;
  }, [activeArtifact]);

  const filteredDeliverables = useMemo(() => {
    if (!fileFilter.trim()) return availableFiles;
    return availableFiles.filter((f) =>
      f.toLowerCase().includes(fileFilter.toLowerCase().trim()),
    );
  }, [availableFiles, fileFilter]);

  return (
    <aside
      className={standalone ? "artifact-preview-browser standalone" : "artifact-preview-browser"}
      style={standalone ? undefined : { width: `${width}px` }}
      aria-label={`Browser Preview of ${activeTabPath}`}
    >
      {!standalone && (
        <div
          className="artifact-preview-handle"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize browser preview"
          onPointerDown={(event) => {
            event.preventDefault();
            dragging.current = true;
            document.body.classList.add("is-resizing");
          }}
        />
      )}

      {/* Browser Tab Bar */}
      <div className="browser-tab-bar">
        <div className="browser-tabs-scroll">
          {tabs.map((tab) => {
            const isActive = tab.path === activeTabPath;
            return (
              <div
                key={tab.path}
                className={`browser-tab ${isActive ? "active" : ""}`}
                onClick={() => setActiveTabPath(tab.path)}
                title={tab.path}
              >
                <IconFileText size={14} />
                <span className="browser-tab-title">{tab.title}</span>
                <button
                  className="browser-tab-close"
                  onClick={(e) => closeTab(tab.path, e)}
                  title="Close tab (Ctrl+W)"
                  aria-label={`Close ${tab.title}`}
                >
                  <IconClose size={12} />
                </button>
              </div>
            );
          })}

          {/* New Tab Button */}
          <div className="browser-new-tab-wrapper">
            <button
              className="browser-new-tab-btn"
              onClick={() => setShowFilePicker(!showFilePicker)}
              title="Open deliverable file in new tab"
            >
              <IconPlus size={14} />
            </button>

            {/* Quick Open Deliverable Dropdown */}
            {showFilePicker && (
              <div className="browser-file-picker-dropdown">
                <div className="picker-search-box">
                  <IconSearch size={14} />
                  <input
                    type="text"
                    placeholder="Search session deliverables…"
                    autoFocus
                    value={fileFilter}
                    onChange={(e) => setFileFilter(e.target.value)}
                  />
                  {fileFilter && (
                    <button onClick={() => setFileFilter("")} className="btn-picker-clear">
                      <IconClose size={12} />
                    </button>
                  )}
                </div>
                <div className="picker-file-list">
                  {filteredDeliverables.length === 0 ? (
                    <div className="picker-empty">No matching files found</div>
                  ) : (
                    filteredDeliverables.map((f) => (
                      <div
                        key={f}
                        className={`picker-file-item ${f === activeTabPath ? "current" : ""}`}
                        onClick={() => openFile(f)}
                      >
                        <IconFileText size={14} />
                        <div className="picker-file-info">
                          <span className="picker-filename">{getFileName(f)}</span>
                          <span className="picker-filepath">{f}</span>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Window controls */}
        <div className="browser-window-controls">
          <button
            className={`btn-browser-util ${showSidebar ? "active" : ""}`}
            onClick={() => setShowSidebar(!showSidebar)}
            title="Toggle Deliverables Sidebar"
          >
            <IconSidebar size={15} />
          </button>
          {!standalone && (
            <button
              className="btn-browser-util"
              onClick={() => void popOut()}
              title="Pop out into full OS Window"
            >
              <IconExternalLink size={15} />
            </button>
          )}
          {!standalone && onClose && (
            <button
              className="btn-browser-util close"
              onClick={onClose}
              title="Close Preview (Esc)"
              aria-label="Close preview"
            >
              <IconClose size={15} />
            </button>
          )}
        </div>
      </div>

      {/* Browser Navigation & Omnibar Bar */}
      <div className="browser-address-bar-strip">
        <div className="browser-nav-buttons">
          <button
            className="btn-nav-arrow"
            disabled={historyIndex <= 0}
            onClick={goBack}
            title="Back"
          >
            <IconArrowLeft size={14} />
          </button>
          <button
            className="btn-nav-arrow"
            disabled={historyIndex >= history.length - 1}
            onClick={goForward}
            title="Forward"
          >
            <IconArrowRight size={14} />
          </button>
          <button
            className="btn-nav-arrow"
            onClick={refreshCurrent}
            title="Reload Deliverable (R)"
          >
            <IconRefresh size={14} />
          </button>
        </div>

        {/* Omnibar / Path URL Bar */}
        <div className="browser-omnibar" onClick={() => setShowFilePicker(!showFilePicker)}>
          <span className="browser-protocol-badge">deliverable://</span>
          <span className="browser-path-text">{activeTabPath}</span>
          <button
            className="btn-omnibar-copy"
            onClick={(e) => {
              e.stopPropagation();
              copyPath();
            }}
            title="Copy path"
          >
            <IconCopy size={13} />
            <span className="copy-label">{copied ? "Copied!" : "Copy"}</span>
          </button>
        </div>

        {/* Mode Switcher Tabs */}
        <div className="browser-view-modes">
          <button
            className={`mode-btn ${viewMode === "preview" ? "active" : ""}`}
            onClick={() => setViewMode("preview")}
            title="Rendered Markdown Preview"
          >
            <IconFileText size={13} />
            <span>Preview</span>
          </button>
          <button
            className={`mode-btn ${viewMode === "source" ? "active" : ""}`}
            onClick={() => setViewMode("source")}
            title="Raw Source View"
          >
            <IconCode size={13} />
            <span>Source</span>
          </button>
          <button
            className={`mode-btn ${viewMode === "outline" ? "active" : ""}`}
            onClick={() => setViewMode("outline")}
            title={`Document Outline (${headingsCount} headings)`}
          >
            <IconList size={13} />
            <span>Outline</span>
          </button>
        </div>
      </div>

      {/* Main Workspace: Optional Sidebar + Content */}
      <div className="browser-workspace">
        {/* Deliverables Explorer Sidebar */}
        {showSidebar && (
          <aside className="browser-sidebar">
            <div className="browser-sidebar-header">
              <span>Session Deliverables</span>
              <span className="deliverable-count-badge">{availableFiles.length}</span>
            </div>
            <div className="browser-sidebar-list">
              {availableFiles.map((file) => {
                const isSelected = file === activeTabPath;
                const isMarkdown = file.endsWith(".md");
                return (
                  <div
                    key={file}
                    className={`sidebar-file-item ${isSelected ? "selected" : ""}`}
                    onClick={() => openFile(file)}
                    title={file}
                  >
                    {isMarkdown ? <IconFileText size={14} /> : <IconCode size={14} />}
                    <div className="sidebar-file-details">
                      <span className="sidebar-filename">{getFileName(file)}</span>
                      <span className="sidebar-subpath">{file}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </aside>
        )}

        {/* File Content Body */}
        <main className="browser-content-viewport">
          {activeLoading && (
            <div className="browser-note loading">
              <span className="spinner-dot" />
              <span>Fetching {activeTabPath}…</span>
            </div>
          )}
          {activeError && (
            <div className="browser-note error">
              <strong>Failed to load artifact:</strong> {activeError}
            </div>
          )}
          {!activeLoading && !activeError && renderedContent}
        </main>
      </div>

      {/* Footer Status Bar */}
      <footer className="browser-status-bar">
        <div className="status-left">
          <span className="status-chip format">
            {activeTabPath.endsWith(".md") ? "Markdown" : "Plain Text"}
          </span>
          {activeArtifact && (
            <>
              <span className="status-chip">
                {(activeArtifact.bytes / 1024).toFixed(1)} KB
              </span>
              <span className="status-chip">
                {activeArtifact.content.split("\n").length} lines
              </span>
              <span className="status-chip">
                {activeArtifact.content.split(/\s+/).filter(Boolean).length} words
              </span>
            </>
          )}
        </div>
        <div className="status-right">
          {activeArtifact?.modifiedAt && (
            <span className="status-chip modified">
              Updated: {new Date(activeArtifact.modifiedAt).toLocaleTimeString()}
            </span>
          )}
          {activeArtifact?.truncated && (
            <span className="status-chip warning">Content truncated</span>
          )}
        </div>
      </footer>
    </aside>
  );
}
