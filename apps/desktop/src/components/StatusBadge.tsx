// StatusBadge component for runs, tasks, and agents

export interface StatusBadgeProps {
  status: string;
  size?: "sm" | "md";
}

export function StatusBadge({ status, size = "md" }: StatusBadgeProps) {
  const norm = (status || "idle").toLowerCase().replace("-", "_");
  const isBlocked = norm === "blocked" || norm === "human_required";
  const isRunning = norm === "running" || norm === "leased";

  return (
    <span
      className={`status-badge ${norm} ${isBlocked ? "blocked" : ""}`}
      style={{
        fontSize: size === "sm" ? "10px" : "11px",
        padding: size === "sm" ? "1px 6px" : "2px 8px",
      }}
    >
      {isRunning && (
        <span
          style={{
            display: "inline-block",
            width: "6px",
            height: "6px",
            borderRadius: "50%",
            backgroundColor: "currentColor",
            animation: "pulse-dot 1.2s infinite ease-in-out",
          }}
        />
      )}
      {norm}
    </span>
  );
}
