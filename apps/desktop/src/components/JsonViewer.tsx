// Collapsible / formatted JSON viewer component
import { useMemo, useState } from "react";

export interface JsonViewerProps {
  data: any;
  initialExpanded?: boolean;
}

export function JsonViewer({ data, initialExpanded = true }: JsonViewerProps) {
  const [copied, setCopied] = useState(false);
  const [collapsed, setCollapsed] = useState(!initialExpanded);

  const jsonString = useMemo(() => {
    try {
      return JSON.stringify(data, null, 2);
    } catch {
      return String(data);
    }
  }, [data]);

  const handleCopy = () => {
    try {
      navigator.clipboard.writeText(jsonString);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // ignore
    }
  };

  return (
    <div
      style={{
        backgroundColor: "var(--colors-surface-soft)",
        border: "1px solid var(--colors-hairline)",
        borderRadius: "var(--rounded-md)",
        padding: "10px 14px",
        fontFamily: "var(--font-mono)",
        fontSize: "12px",
        position: "relative",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: collapsed ? 0 : "8px",
          borderBottom: collapsed ? "none" : "1px solid var(--colors-hairline-soft)",
          paddingBottom: collapsed ? 0 : "6px",
        }}
      >
        <button
          onClick={() => setCollapsed(!collapsed)}
          style={{
            background: "none",
            border: "none",
            color: "var(--colors-charcoal)",
            cursor: "pointer",
            fontSize: "12px",
            fontWeight: 600,
            display: "flex",
            alignItems: "center",
            gap: "6px",
          }}
        >
          <span style={{ fontSize: "9px" }}>{collapsed ? "▶" : "▼"}</span>
          <span>{collapsed ? "Expand payload" : "Payload JSON"}</span>
        </button>

        <button
          onClick={handleCopy}
          style={{
            background: copied ? "var(--colors-success-bg)" : "var(--colors-canvas)",
            border: `1px solid ${copied ? "var(--colors-success-border)" : "var(--colors-hairline)"}`,
            borderRadius: "var(--rounded-full)",
            color: copied ? "var(--colors-success-text)" : "var(--colors-slate)",
            fontSize: "11px",
            fontWeight: 600,
            padding: "2px 10px",
            cursor: "pointer",
            transition: "all 0.15s ease",
          }}
        >
          {copied ? "Copied!" : "Copy JSON"}
        </button>
      </div>

      {!collapsed && (
        <pre
          style={{
            margin: 0,
            overflowX: "auto",
            color: "var(--colors-ink)",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
            maxHeight: "350px",
            lineHeight: 1.5,
          }}
        >
          {jsonString}
        </pre>
      )}
    </div>
  );
}
